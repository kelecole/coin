# -*- coding: utf-8 -*-
"""
core/sig_vpoc_flip.py

VPOC 翻转（结构收复/失守 + 放量）→ 生成入库信号：
- source='sig_vpoc_flip'，direction in {'long','short'}
- 严格按桶 t_ref（闭合K）
- 仅当“穿越 VPOC”且出现放量时触发；RR 预估达标后入库
- TTL：1h → +24h；4h → +48h

依赖：
- core.chip_structure.compute_triple_recent   （取 VPOC/VAL/VAH）
- core.indicators：get_last_two_rows, compute_atr_from_db, compute_vol_spike_ratio_from_db, compute_rr
- core.timebox：now_local_str
- （可选）core.btc_alignment._infer_btc_trend（仅用于 detail 记录，不做硬门）

注意：
- 仅用闭合K（内部调用已严格 <= t_ref 的最近闭合K）
- rr 只是“预估RR”，真正的 risk_cost 门后续仍会复核
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
from .chip_structure import compute_triple_recent
from .indicators import (
    get_last_two_rows,
    compute_atr_from_db,
    compute_vol_spike_ratio_from_db,
    compute_rr,
)

try:
    from .btc_alignment import _infer_btc_trend
except Exception:
    _infer_btc_trend = None  # 不可用时仅在 detail 记 'unknown'


# ---------------------------------------------------------------------
# 配置访问 / 兜底（保持旧命名以兼容外部调用）
# ---------------------------------------------------------------------

def _get_core(cfg: Dict) -> Dict:
    return cfg.get("core", cfg) or {}

def _get_sig_cfg(cfg: Dict) -> Dict:
    """
    优先读取 core.signals_rules.vpoc_flip，其次 thresholds.signals.sig_vpoc_flip
    """
    core = _get_core(cfg)
    rules = ((core.get("signals_rules") or {}).get("vpoc_flip") or
             (cfg.get("signals_rules") or {}).get("vpoc_flip") or {})
    thr = (((core.get("thresholds") or {}).get("signal") or {}).get("sig_vpoc_flip") or
           ((cfg.get("thresholds") or {}).get("signal") or {}).get("sig_vpoc_flip") or {})
    out = dict(thr)
    out.update(rules)
    return out

def _get_vol_gate_cfg(cfg: Dict) -> Dict:
    # config.core.volume_spike
    return (_get_core(cfg).get("volume_spike") or {})

def _get_chip_cfg(cfg: Dict) -> Dict:
    # config.core.feature_engineering.chip_peak
    fe = (_get_core(cfg).get("feature_engineering") or {})
    return fe.get("chip_peak") or {}

def _allow_short(cfg: Dict) -> bool:
    return bool(_get_core(cfg).get("allow_short", True))

def _bucketize_tref(ts: str, tf: str) -> str:
    """把 t_ref 向下落到周期边界（与其它信号一致）"""
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

def _ttl_hours_for_tf(tf: str) -> int:
    tf = (tf or "").lower()
    if tf == "1h":
        return 24
    if tf == "4h":
        return 48
    # 其他周期给一个合理上限
    return 24

def _calc_expires_at_by_tf(t_ref: str, timeframe: str) -> str:
    s = str(t_ref)[:19].replace("T", " ")
    try:
        base = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        base = datetime.fromisoformat(s)
    hours = _ttl_hours_for_tf(timeframe)
    expires = base + timedelta(hours=hours)
    # 兜底上限 5 天，防脏
    if expires - base > timedelta(days=5):
        expires = base + timedelta(days=5)
    return expires.strftime("%Y-%m-%d %H:%M:%S")


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
    sig_cfg = _get_sig_cfg(cfg)
    atr_len = int(sig_cfg.get("atr_len", 14))
    atr = float(compute_atr_from_db(conn, symbol, timeframe, t_ref_bucket, length=atr_len))
    if not (atr > 0.0 and math.isfinite(atr) and close_now > 0.0):
        return out

    # 放量（VPOC 翻转要求“有量”）
    vol_cfg = _get_vol_gate_cfg(cfg)
    lookback = int(vol_cfg.get("lookback_bars", 24) or 24)
    vs = compute_vol_spike_ratio_from_db(conn, symbol, timeframe, t_ref_bucket, lookback=lookback) or {}
    vol_ratio = float(vs.get("ratio") or 0.0)
    vol_spike_min = float(sig_cfg.get("vol_spike_min", 1.6))
    if not (math.isfinite(vol_ratio) and vol_ratio >= vol_spike_min):
        return out

    # 筹码三价带（近 N 根）
    lookback_bars = int(_get_chip_cfg(cfg).get("lookback", 90) or 90)
    tri_now = compute_triple_recent(conn, symbol, timeframe, t_ref_bucket, cfg, lookback_bars=lookback_bars)
    if not tri_now or not tri_now.get("ok"):
        return out
    vpoc_now = float(tri_now["vpoc"])
    val_now  = float(tri_now.get("val", float('nan')))
    vah_now  = float(tri_now.get("vah", float('nan')))

    # 上一根的 VPOC 近似：用 t_prev 的三价带（避免同一根抖动）
    tri_prev = compute_triple_recent(conn, symbol, timeframe, t_prev, cfg, lookback_bars=lookback_bars)
    vpoc_prev = float(tri_prev["vpoc"]) if (tri_prev and tri_prev.get("ok")) else vpoc_now

    # 方向触发（long/short 对称）：穿越 VPOC
    long_trigger  = (close_prev <= vpoc_prev) and (close_now > vpoc_now)
    short_trigger = (close_prev >= vpoc_prev) and (close_now < vpoc_now)

    # BTC 趋势仅记录 detail（不做硬否决，硬门交给你已有 btc_alignment_gate）
    btc_trend = "unknown"
    if _infer_btc_trend is not None:
        try:
            btc_trend = str(_infer_btc_trend(conn=conn, t_ref=t_ref_bucket,
                                             timeframe=(_get_core(cfg).get("btc_ctx", {}) or {}).get("timeframe", "1h"),
                                             bars=(_get_core(cfg).get("btc_ctx", {}) or {}).get("recent_bars", 6)
                                            ) or "unknown").lower()
        except Exception:
            btc_trend = "unknown"

    # RR 参数
    rr_min    = float(sig_cfg.get("rr_min", 1.8))
    take_mult = float(sig_cfg.get("take_mult", 2.5))
    stop_buf  = float(sig_cfg.get("stop_buffer_atr", 0.2))

    # LONG 候选
    if long_trigger:
        entry  = close_now
        stop   = min(prev_low, vpoc_now - stop_buf * atr)
        target = entry + take_mult * atr
        rr_est = compute_rr(entry, stop, target, side="long")
        if rr_est >= rr_min and stop < entry:
            out.append({
                "t_ref": t_ref_bucket,
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "sig_vpoc_flip",
                "direction": "long",
                "score": None, "prob": None, "strength": None,
                "detail_json": json.dumps({
                    "vpoc": vpoc_now,
                    "vpoc_prev": vpoc_prev,
                    "val": val_now, "vah": vah_now,
                    "atr": float(atr),
                    "vol_ratio": float(vol_ratio),
                    "btc_trend": btc_trend,
                    "entry": entry, "stop": stop, "target": target,
                }, ensure_ascii=False),
                "status": "active",
                "expires_at": _calc_expires_at_by_tf(t_ref_bucket, timeframe),
            })

    # SHORT 候选
    if short_trigger and _allow_short(cfg):
        entry  = close_now
        stop   = max(prev_high, vpoc_now + stop_buf * atr)
        target = entry - take_mult * atr
        rr_est = compute_rr(entry, stop, target, side="short")
        if rr_est >= rr_min and stop > entry:
            out.append({
                "t_ref": t_ref_bucket,
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "sig_vpoc_flip",
                "direction": "short",
                "score": None, "prob": None, "strength": None,
                "detail_json": json.dumps({
                    "vpoc": vpoc_now,
                    "vpoc_prev": vpoc_prev,
                    "val": val_now, "vah": vah_now,
                    "atr": float(atr),
                    "vol_ratio": float(vol_ratio),
                    "btc_trend": btc_trend,
                    "entry": entry, "stop": stop, "target": target,
                }, ensure_ascii=False),
                "status": "active",
                "expires_at": _calc_expires_at_by_tf(t_ref_bucket, timeframe),
            })

    return out


# ---------------------------------------------------------------------
# 扫描与入库（与其它信号保持一致）
# ---------------------------------------------------------------------

def run_for_bucket(
    conn: sqlite3.Connection,
    t_ref: str,
    cfg: Dict,
) -> Dict[str, int]:
    core = _get_core(cfg)
    symbols = list(core.get("tests") or [])
    tfs = list(core.get("timeframes") or ["1h"])

    stats = dict(inserted=0, updated=0, skipped=0)
    now_ts = now_local_str(core.get("tz", "Asia/Shanghai"))

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
    """返回 (inserted, updated)"""
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

    # 已存在 → 更新
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
