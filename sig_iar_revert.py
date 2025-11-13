# -*- coding: utf-8 -*-
"""
core/sig_iar_revert.py

IAR（Intraday AVWAP Reversion，日内/周期内 AVWAP 均值回归）
只做现货多头（LONG）入库信号：
- source='iar_revert'
- direction='long'
- 严格按桶 t_ref（闭合K）
- 仅在“震荡/收敛 + BTC 上/平 + 无放量冲击 + 价格显著低于 AVWAP(按ATR计)”时触发
- RR 预估满足 rr_min 才入库（risk_cost 门后续还会再校核）
- TTL：2 根 bars

依赖：
- core.avwap: get_anchor_ts, compute_avwap_from_db
- core.indicators: get_last_two_rows, compute_atr_from_db, compute_vol_spike_ratio_from_db,
                   compute_bb_from_db, compute_rr
- core.btc_alignment: _infer_btc_trend（若不可用，BTC 条件退化为放行，但会记录 detail）
- core.timebox: now_local_str

与现有“三张牌”一致的行为：
- 失败/数据不足直接跳过，不抛异常
- UPSERT 到 signals（唯一键：t_ref,symbol,timeframe,source,direction）
"""

from __future__ import annotations
import sqlite3
import json
import math
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta

# 统一 now_ts
from core.timebox import now_local_str

# 指标/锚点
from .avwap import get_anchor_ts, compute_avwap_from_db
from .indicators import (
    get_last_two_rows,
    compute_atr_from_db,
    compute_vol_spike_ratio_from_db,
    compute_bb_from_db,
    compute_rr,
)

# BTC 趋势（内部函数，若不可用则兜底）
try:
    from .btc_alignment import _infer_btc_trend
except Exception:
    _infer_btc_trend = None  # 兜底：不可用时视为 'flat'


# ---------------------------------------------------------------------
# 配置访问 / 兜底
# ---------------------------------------------------------------------

def _core(cfg: Dict) -> Dict:
    return cfg.get("core", cfg) or {}

def _sig_cfg(cfg: Dict) -> Dict:
    """
    优先读取 core.signals_rules.iar_revert，其次 thresholds.signals.iar_revert
    """
    core = _core(cfg)
    rules = ((core.get("signals_rules") or {}).get("iar_revert") or
             (cfg.get("signals_rules") or {}).get("iar_revert") or {})
    thr = (((core.get("thresholds") or {}).get("signals") or {}).get("iar_revert") or
           ((cfg.get("thresholds") or {}).get("signals") or {}).get("iar_revert") or {})
    out = dict(thr)
    out.update(rules)
    return out

def _vol_gate_cfg(cfg: Dict) -> Dict:
    # 放量门全局配置（只取 lookback）
    return (_core(cfg).get("volume_spike") or {})

def _btc_cfg(cfg: Dict) -> Dict:
    return (_core(cfg).get("btc_alignment") or {})

def _allow_symbol(sym: str, restrict_list: Optional[List[str]]) -> bool:
    if not restrict_list:
        return True
    # 配置里既可能写 "BTC/USDT"，也可能写 "BTCUSDT"；保持严格匹配，避免误开
    return sym in restrict_list


# ---------------------------------------------------------------------
# 业务逻辑
# ---------------------------------------------------------------------

def _calc_expires_at(t_ref: str, timeframe: str, bars: int = 2) -> str:
    """
    TTL = bars * timeframe，兜底上限 2 天
    """
    s = str(t_ref)[:19].replace("T", " ")
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        dt = datetime.fromisoformat(s)
    tf = (timeframe or "").lower()
    if tf.endswith("h"):
        hours = int(tf[:-1] or 1) * bars
        dt2 = dt + timedelta(hours=hours)
    elif tf.endswith("m"):
        mins = int(tf[:-1] or 1) * bars
        dt2 = dt + timedelta(minutes=mins)
    else:
        # 默认按小时处理
        dt2 = dt + timedelta(hours=bars)
    # 上限 2 天（防脏）
    if dt2 - dt > timedelta(days=2):
        dt2 = dt + timedelta(days=2)
    return dt2.strftime("%Y-%m-%d %H:%M:%S")


def _bucketize_tref(ts: str, tf: str) -> str:
    """
    把 t_ref 向下落到周期边界（与现有三张牌一致）
    """
    s = str(ts)[:19].replace("T", " ")
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        dt = datetime.fromisoformat(s)
    tf = (tf or "").lower()
    if tf.endswith("h"):
        n = int(tf[:-1] or 1)
        hour = (dt.hour // n) * n
        dt = dt.replace(hour=hour, minute=0, second=0)
    elif tf.endswith("m"):
        n = int(tf[:-1] or 1)
        minute = (dt.minute // n) * n
        dt = dt.replace(minute=minute, second=0)
    else:
        # 默认当 1h
        dt = dt.replace(minute=0, second=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _btc_trend_ok(conn: sqlite3.Connection, cfg: Dict, t_ref: str) -> Tuple[bool, str]:
    """
    IAR 只在 BTC 'flat'（中性）或允许 'up' 时开放。默认允许 up。
    若 _infer_btc_trend 不可用，则视为 'flat' 并放行。
    """
    allow_up = bool(_sig_cfg(cfg).get("allow_btc_up", True))
    tf = str(_btc_cfg(cfg).get("timeframe", "1h"))
    bars = int(_btc_cfg(cfg).get("recent_bars", 6))
    if _infer_btc_trend is None:
        return True, "unknown"  # 退化放行
    trend = str(_infer_btc_trend(conn=conn, t_ref=t_ref, timeframe=tf, bars=bars) or "flat").lower()
    if trend in ("flat", "neutral"):
        return True, trend
    if trend == "up" and allow_up:
        return True, trend
    return False, trend


def detect_one(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    cfg: Dict,
) -> List[Dict[str, Any]]:
    """
    返回 0~1 个候选（仅做多）。
    全流程严格用桶化后的 t_ref_bucket。
    """
    t_ref_bucket = _bucketize_tref(t_ref, timeframe)
    out: List[Dict[str, Any]] = []

    # 限定白名单（可选）
    s_cfg = _sig_cfg(cfg)
    restrict = s_cfg.get("restrict_symbols") or []
    if not _allow_symbol(symbol, restrict):
        return out

    # 最近两根闭合K
    prev, last = get_last_two_rows(conn, symbol, timeframe, t_ref_bucket)
    if not last or not prev:
        return out

    close_now = float(last["close"])
    prev_low  = float(prev["low"])
    prev_high = float(prev["high"])  # 仅用于诊断
    t_prev    = str(prev["t"])

    # ATR
    atr = float(compute_atr_from_db(conn, symbol, timeframe, t_ref_bucket, length=int(s_cfg.get("atr_len", 14))))
    if not (atr > 0.0 and math.isfinite(atr)):
        return out

    # BB 宽度与分位（收敛/震荡判定）
    bb = compute_bb_from_db(
        conn=conn, symbol=symbol, timeframe=timeframe, t_ref=t_ref_bucket,
        length=int(s_cfg.get("bb_len", 20)), k=float(s_cfg.get("bb_k", 2.0)),
        lookback_for_pctile=int(s_cfg.get("bb_pctile_lookback", 120))
    ) or {}
    width_norm = bb.get("width_norm")
    width_pct  = bb.get("width_pctile")
    if not (isinstance(width_norm, (int, float)) and isinstance(width_pct, (int, float))):
        return out

    bb_width_norm_max  = float(s_cfg.get("bb_width_norm_max", 1.5))   # (BB宽度/ATR) 上限
    squeeze_pctile_max = float(s_cfg.get("squeeze_pctile_max", 0.30)) # 宽度分位上限
    if not (width_norm <= bb_width_norm_max and width_pct <= squeeze_pctile_max):
        return out

    # 放量冲击（IAR 需要“无明显放量”）
    lookback = int((_vol_gate_cfg(cfg).get("lookback_bars") or 24) or 24)
    vs = compute_vol_spike_ratio_from_db(conn, symbol, timeframe, t_ref_bucket, lookback=lookback) or {}
    vol_ratio = float(vs.get("ratio") or 0.0)
    vol_spike_cap = float(s_cfg.get("vol_spike_cap", 1.25))  # 超过该比值视为“趋势/单边”，不进 IAR
    if not (math.isfinite(vol_ratio) and vol_ratio < vol_spike_cap):
        return out

    # 计算 AVWAP（锚点）
    anchor_kind    = str(s_cfg.get("anchor", "weekly_open"))
    pivot_lookback = int(s_cfg.get("pivot_lookback", 50) or 50)
    try:
        anchor_ts_now = get_anchor_ts(conn=conn, symbol=symbol, timeframe=timeframe,
                                      t_ref=t_ref_bucket, anchor=anchor_kind, pivot_lookback=pivot_lookback)
        avwap_now = float(compute_avwap_from_db(conn, symbol, timeframe, anchor_ts_now, t_ref_bucket))
    except Exception:
        return out
    if not (math.isfinite(avwap_now) and avwap_now > 0.0):
        return out

    # 偏离度：价格显著低于 AVWAP（按 ATR 度量）
    dev_min_atr = float(s_cfg.get("dev_min_atr", 0.8))
    deviation_atr = float((avwap_now - close_now) / max(atr, 1e-12))
    if not (deviation_atr >= dev_min_atr and close_now < avwap_now):
        return out

    # （可选）ATR/close 的“低波动”附加约束
    atr_frac_max = float(s_cfg.get("atr_frac_max", 0.04))  # 例如 4%
    if not (atr / max(close_now, 1e-12) <= atr_frac_max):
        return out

    # BTC 趋势：仅上/平
    btc_ok, btc_trend = _btc_trend_ok(conn, cfg, t_ref_bucket)
    if not btc_ok:
        return out

    # 预估 RR 并构造候选
    take_mult = float(s_cfg.get("take_mult", 2.2))
    rr_min    = float(s_cfg.get("rr_min", 1.8))
    stop_k    = float(s_cfg.get("stop_k", 0.5))  # 止损相对 AVWAP 的缓冲 (k*ATR)

    entry  = close_now
    stop   = min(prev_low, avwap_now - stop_k * atr)
    target = entry + take_mult * atr

    rr_est = compute_rr(entry, stop, target, side="long")

    if not (rr_est >= rr_min and entry > stop and target > entry):
        return out

    detail = {
        "anchor": anchor_kind,
        "anchor_ts": anchor_ts_now,
        "avwap": avwap_now,
        "atr": atr,
        "deviation_atr": deviation_atr,
        "bb_width_norm": width_norm,
        "bb_width_pctile": width_pct,
        "vol_spike_ratio": vol_ratio,
        "btc_trend": btc_trend,
        "entry": entry,
        "stop": stop,
        "target": target,
        "t_prev": t_prev,
        "prev_high": prev_high,
        "prev_low": prev_low,
    }

    out.append({
        "t_ref": t_ref_bucket,
        "symbol": symbol,
        "timeframe": timeframe,
        "source": "iar_revert",
        "direction": "long",
        "score": None,
        "prob": None,
        "strength": None,
        "detail_json": json.dumps(detail, ensure_ascii=False),
        "status": "active",
        "expires_at": _calc_expires_at(t_ref_bucket, timeframe, bars=2),
    })

    return out


# ---------------------------------------------------------------------
# 扫描与入库（与三张牌保持一致）
# ---------------------------------------------------------------------

def run_for_bucket(
    conn: sqlite3.Connection,
    t_ref: str,
    cfg: Dict,
) -> Dict[str, int]:
    """
    对 core.tests × timeframes 全量扫描，并 UPSERT 到 signals。
    返回 {"inserted": x, "updated": y, "skipped": z}
    """
    core = _core(cfg)
    symbols = list(core.get("tests") or [])
    tfs = list(core.get("timeframes") or ["1h"])

    stats = dict(inserted=0, updated=0, skipped=0)

    # 统一 created_at
    now_ts = now_local_str(cfg.get("tz", "Asia/Shanghai"))

    for tf in tfs:
        for sym in symbols:
            try:
                cands = detect_one(conn, sym, tf, t_ref, cfg)
            except Exception:
                cands = []
            if not cands:
                stats["skipped"] += 1
                continue
            for c in cands:
                ins, upd = _upsert_signal(conn, c, now_ts)
                if ins:
                    stats["inserted"] += 1
                elif upd:
                    stats["updated"] += 1
                else:
                    stats["skipped"] += 1

    return stats


def _exists(conn: sqlite3.Connection, t_ref: str, symbol: str, timeframe: str, source: str, direction: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM signals
        WHERE t_ref=? AND symbol=? AND timeframe=? AND source=? AND direction=?
        LIMIT 1
        """,
        (t_ref, symbol, timeframe, source, direction),
    ).fetchone()
    return bool(row)


def _upsert_signal(conn: sqlite3.Connection, c: Dict[str, Any], now_ts: str) -> Tuple[bool, bool]:
    """
    返回 (inserted, updated)
    """
    if not _exists(conn, c["t_ref"], c["symbol"], c["timeframe"], c["source"], c["direction"]):
        conn.execute(
            """
            INSERT INTO signals
                (t_ref, symbol, timeframe, source, direction,
                 score, prob, strength, detail_json,
                 status, expires_at, created_at)
            VALUES
                (?, ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 UPPER(?), ?, ?)
            """,
            (
                c["t_ref"], c["symbol"], c["timeframe"], c["source"], c["direction"],
                c.get("score"), c.get("prob"), c.get("strength"), c.get("detail_json"),
                c.get("status", "active"), c.get("expires_at"),
                now_ts,
            ),
        )
        return True, False

    # 已存在 → 更新（detail_json/status/score/prob/strength/expires_at）
    conn.execute(
        """
        UPDATE signals
        SET score=?,
            prob=?,
            strength=?,
            detail_json=?,
            status=UPPER(?),
            expires_at=?
        WHERE t_ref=? AND symbol=? AND timeframe=? AND source=? AND direction=?
        """,
        (
            c.get("score"), c.get("prob"), c.get("strength"), c.get("detail_json"),
            c.get("status", "active"), c.get("expires_at"),
            c["t_ref"], c["symbol"], c["timeframe"], c["source"], c["direction"],
        ),
    )
    return False, True
