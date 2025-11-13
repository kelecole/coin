# -*- coding: utf-8 -*-
"""
core/sig_confluence_reclaim.py

AVWAP ∧ VPOC 共振回收（Confluence Reclaim）
- source='sig_confluence_reclaim'
- direction in {'long','short'}（对称）
- 严格按桶 t_ref（闭合K）
- 仅在 AVWAP 与 VPOC 距离很近（“中枢叠加”）的前提下，发生“收复/收回”时触发
- 体量过滤：避免极低/极高成交量（交给别的策略）
- RR 预估满足 rr_min 才入库（risk_cost 门后续仍会再校核）
- TTL：2 根 bars

依赖：
- core.avwap: get_anchor_ts, compute_avwap_from_db
- core.chip_structure: compute_triple_recent （取 VPOC/VAL/VAH）
- core.indicators: get_last_two_rows, compute_atr_from_db, compute_vol_spike_ratio_from_db, compute_rr
- core.btc_alignment: _infer_btc_trend（若不可用，BTC 条件退化为放行，但会记录 detail）
- core.timebox: now_local_str

行为与三张牌一致：
- 数据不足/异常 → 跳过，不抛异常
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

# 依赖模块
from .avwap import get_anchor_ts, compute_avwap_from_db
from .chip_structure import compute_triple_recent
from .indicators import (
    get_last_two_rows,
    compute_atr_from_db,
    compute_vol_spike_ratio_from_db,
    compute_rr,
)

# BTC 趋势（若不可用则兜底）
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
    优先读取 core.signals_rules.confluence_reclaim，其次 thresholds.signals.confluence_reclaim
    """
    core = _core(cfg)
    rules = ((core.get("signals_rules") or {}).get("confluence_reclaim") or
             (cfg.get("signals_rules") or {}).get("confluence_reclaim") or {})
    thr = (((core.get("thresholds") or {}).get("signals") or {}).get("confluence_reclaim") or
           ((cfg.get("thresholds") or {}).get("signals") or {}).get("confluence_reclaim") or {})
    out = dict(thr)
    out.update(rules)
    return out

def _vol_gate_cfg(cfg: Dict) -> Dict:
    # 放量门全局配置（只取 lookback）
    return (_core(cfg).get("volume_spike") or {})

def _chip_cfg(cfg: Dict) -> Dict:
    return ((_core(cfg).get("feature_engineering") or {}).get("chip_peak") or {})

def _btc_cfg(cfg: Dict) -> Dict:
    return (_core(cfg).get("btc_alignment") or {})

def _allow_short(cfg: Dict) -> bool:
    return bool(_core(cfg).get("allow_short", True))


# ---------------------------------------------------------------------
# 通用小工具
# ---------------------------------------------------------------------

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
    elif tf.endswith("d"):
        dt = dt.replace(hour=0, minute=0, second=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _calc_expires_at(t_ref: str, timeframe: str, bars: int = 2) -> str:
    """
    TTL = bars * timeframe，兜底上限 3 天
    """
    s = str(t_ref)[:19].replace("T", " ")
    try:
        base = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        base = datetime.fromisoformat(s)

    tf = (timeframe or "").lower()
    if tf.endswith("h"):
        secs = int(tf[:-1] or 1) * 3600
    elif tf.endswith("m"):
        secs = int(tf[:-1] or 1) * 60
    elif tf.endswith("d"):
        secs = int(tf[:-1] or 1) * 86400
    else:
        secs = 3600

    ttl_seconds = bars * secs
    cap_seconds = 3 * 86400
    expires = base + timedelta(seconds=min(ttl_seconds, cap_seconds))
    return expires.strftime("%Y-%m-%d %H:%M:%S")


def _btc_trend_ok(conn: sqlite3.Connection, cfg: Dict, t_ref: str, side: str) -> Tuple[bool, str]:
    """
    BTC 条件（long：flat/可选 up；short：flat/可选 down）
    """
    trend = "flat"
    allow_long_up  = bool(_sig_cfg(cfg).get("allow_btc_up_for_long", True))
    allow_short_dn = bool(_sig_cfg(cfg).get("allow_btc_down_for_short", True))
    tf_btc = str(_btc_cfg(cfg).get("timeframe", "1h"))
    bars   = int(_btc_cfg(cfg).get("recent_bars", 6))
    if _infer_btc_trend is not None:
        try:
            trend = str(_infer_btc_trend(conn=conn, t_ref=t_ref, timeframe=tf_btc, bars=bars) or "flat").lower()
        except Exception:
            trend = "flat"
    # 判定
    side = (side or "").lower()
    if side == "long":
        if trend in ("flat", "neutral"):
            return True, trend
        if trend == "up" and allow_long_up:
            return True, trend
        return False, trend
    else:  # short
        if trend in ("flat", "neutral"):
            return True, trend
        if trend == "down" and allow_short_dn:
            return True, trend
        return False, trend


# ---------------------------------------------------------------------
# 核心检测
# ---------------------------------------------------------------------

def detect_one(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    cfg: Dict,
) -> List[Dict[str, Any]]:
    """
    最多返回两条（long/short），常见为 0 或 1。
    全流程严格用桶化后的 t_ref_bucket。
    """
    out: List[Dict[str, Any]] = []
    t_ref_bucket = _bucketize_tref(t_ref, timeframe)

    # 最近两根闭合K
    prev, last = get_last_two_rows(conn, symbol, timeframe, t_ref_bucket)
    if not last or not prev:
        return out

    close_now  = float(last["close"])
    close_prev = float(prev["close"])
    prev_low   = float(prev["low"])
    prev_high  = float(prev["high"])
    t_prev     = str(prev["t"])

    # ATR
    s_cfg = _sig_cfg(cfg)
    atr_len = int(s_cfg.get("atr_len", 14))
    atr = float(compute_atr_from_db(conn, symbol, timeframe, t_ref_bucket, length=atr_len))
    if not (atr > 0.0 and math.isfinite(atr) and close_now > 0.0):
        return out

    # 体量过滤：避免极低/极高
    vol_cfg = _vol_gate_cfg(cfg)
    lookback = int(vol_cfg.get("lookback_bars", 24) or 24)
    vs = compute_vol_spike_ratio_from_db(conn, symbol, timeframe, t_ref_bucket, lookback=lookback) or {}
    vol_ratio = float(vs.get("ratio") or 0.0)
    vol_min   = float(s_cfg.get("vol_min", 0.80))
    vol_max   = float(s_cfg.get("vol_max", 1.60))
    if not (math.isfinite(vol_ratio) and vol_min <= vol_ratio <= vol_max):
        return out

    # 筹码三价带（近 N 根）
    lookback_bars = int(_chip_cfg(cfg).get("lookback", 90) or 90)
    tri_now = compute_triple_recent(conn, symbol, timeframe, t_ref_bucket, cfg, lookback_bars=lookback_bars)
    if not tri_now or not tri_now.get("ok"):
        return out
    vpoc_now = float(tri_now["vpoc"])
    val_now  = float(tri_now["val"])
    vah_now  = float(tri_now["vah"])

    # AVWAP（与 t_prev 同 anchor）
    anchor_kind    = str(s_cfg.get("anchor", "weekly_open"))
    pivot_lookback = int(s_cfg.get("pivot_lookback", 50) or 50)
    try:
        anchor_ts = get_anchor_ts(conn=conn, symbol=symbol, timeframe=timeframe,
                                  t_ref=t_ref_bucket, anchor=anchor_kind, pivot_lookback=pivot_lookback)
    except Exception:
        return out

    avwap_prev = float(compute_avwap_from_db(conn, symbol, timeframe, anchor_ts, t_prev))
    avwap_now  = float(compute_avwap_from_db(conn, symbol, timeframe, anchor_ts, t_ref_bucket))
    if not all(map(lambda x: math.isfinite(x), [avwap_prev, avwap_now, vpoc_now])):
        return out

    # Confluence：两锚点必须“靠得足够近”
    confl_dist_pct = float(s_cfg.get("confl_dist_pct", 0.30))  # 以 ATR 为单位
    confl_dist_atr = abs(avwap_now - vpoc_now) / max(atr, 1e-12)
    if not (confl_dist_atr <= confl_dist_pct):
        return out

    # ε：防擦边
    eps_atr = float(s_cfg.get("epsilon_atr", 0.05))
    eps_val = eps_atr * atr

    # 方向触发（long/short 对称）
    long_trigger  = (max(close_prev, prev_high) <= min(avwap_prev, vpoc_now, avwap_now) + eps_val) and \
                    (close_now >= max(avwap_now, vpoc_now) + eps_val)

    short_trigger = (min(close_prev, prev_low)  >= max(avwap_prev, vpoc_now, avwap_now) - eps_val) and \
                    (close_now <= min(avwap_now, vpoc_now) - eps_val)

    # BTC 对齐
    # 注意：BTC 判定使用 t_ref_bucket，方向相关
    # RR 参数
    rr_min    = float(s_cfg.get("rr_min", 2.0))
    take_mult = float(s_cfg.get("take_mult", 2.6))
    stop_k    = float(s_cfg.get("stop_k", 0.4))

    # LONG 候选
    if long_trigger:
        btc_ok, btc_trend = _btc_trend_ok(conn, cfg, t_ref_bucket, side="long")
        if btc_ok:
            entry  = close_now
            stop   = min(prev_low, min(avwap_now, vpoc_now) - stop_k * atr)
            target = entry + take_mult * atr
            rr_est = compute_rr(entry, stop, target, side="long")
            if rr_est >= rr_min and stop < entry:
                out.append({
                    "t_ref": t_ref_bucket,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "source": "sig_confluence_reclaim",
                    "direction": "long",
                    "score": None, "prob": None, "strength": None,
                    "detail_json": json.dumps({
                        "avwap": avwap_now, "vpoc": vpoc_now,
                        "confl_dist_atr": float(confl_dist_atr),
                        "epsilon_atr": float(eps_atr),
                        "atr": float(atr),
                        "vol_ratio": float(vol_ratio),
                        "btc_trend": btc_trend,
                        "entry": entry, "stop": stop, "target": target,
                        "val": val_now, "vah": vah_now,
                    }, ensure_ascii=False),
                    "status": "active",
                    "expires_at": _calc_expires_at(t_ref_bucket, timeframe, bars=2),
                })

    # SHORT 候选
    if short_trigger and _allow_short(cfg):
        btc_ok, btc_trend = _btc_trend_ok(conn, cfg, t_ref_bucket, side="short")
        if btc_ok:
            entry  = close_now
            stop   = max(prev_high, max(avwap_now, vpoc_now) + stop_k * atr)
            target = entry - take_mult * atr
            rr_est = compute_rr(entry, stop, target, side="short")
            if rr_est >= rr_min and stop > entry:
                out.append({
                    "t_ref": t_ref_bucket,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "source": "sig_confluence_reclaim",
                    "direction": "short",
                    "score": None, "prob": None, "strength": None,
                    "detail_json": json.dumps({
                        "avwap": avwap_now, "vpoc": vpoc_now,
                        "confl_dist_atr": float(confl_dist_atr),
                        "epsilon_atr": float(eps_atr),
                        "atr": float(atr),
                        "vol_ratio": float(vol_ratio),
                        "btc_trend": btc_trend,
                        "entry": entry, "stop": stop, "target": target,
                        "val": val_now, "vah": vah_now,
                    }, ensure_ascii=False),
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
