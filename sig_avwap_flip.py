# -*- coding: utf-8 -*-
"""
core/sig_avwap_flip.py

AVWAP 翻转（夺回/跌回 + 放量） → 生成入库信号：
- source='sig_avwap_flip'，direction in {'long','short'}
- status='active'
- expires_at：1h → +24h；4h → +48h

触发：
  long : close[t] > AVWAP[t] 且 close[t-1] <= AVWAP[t-1] 且 vol_spike_ratio ≥ vol_spike_min_long
  short: close[t] < AVWAP[t] 且 close[t-1] >= AVWAP[t-1] 且 vol_spike_ratio ≥ vol_spike_min_short

止损/目标：
  long : stop = max(AVWAP[t], low[t-1])；target = entry + take_mult * ATR
  short: stop = min(AVWAP[t], high[t-1])；target = entry - take_mult * ATR
  仅当 RR >= rr_min 才入库（RR=|entry-stop| : |target-entry|）

配置读取：
  core.thresholds.signals.avwap_flip:
    { vol_spike_min: 1.5, rr_min: 1.8, take_mult: 2.5, anchor: "weekly_open", pivot_lookback: 50 }
  - 若提供 vol_spike_min_short，优先用；否则默认比 long 高 0.1
  - 锚点支持 "weekly_open" / "pivot_lookback"

依赖：
  - core.avwap: get_anchor_ts, compute_avwap_from_db, crossed_avwap
  - core.indicators: get_last_two_rows, compute_atr_from_db, compute_vol_spike_ratio_from_db, compute_rr
"""

from __future__ import annotations
import sqlite3
import json
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta

# [修改] 导入 timebox
from core.timebox import now_local_str

from .avwap import (
    get_anchor_ts,
    compute_avwap_from_db,
    crossed_avwap,
)
from .indicators import (
    get_last_two_rows,
    compute_atr_from_db,
    compute_vol_spike_ratio_from_db,
    compute_rr,
)

# ---------------------------------------------------------------------
# 配置读取 / 开关
# ---------------------------------------------------------------------

def _core(cfg: Dict) -> Dict:
    return cfg.get("core", cfg)

def _sig_cfg(cfg: dict) -> dict:
    core = cfg.get("core", cfg) or {}
    # 1) 新口径：signals_rules.avwap_flip
    rules = ((core.get("signals_rules") or {}).get("avwap_flip") or
             (cfg.get("signals_rules") or {}).get("avwap_flip") or {})
    # 2) 旧口径/兜底：thresholds.signals.avwap_flip
    thr = (((core.get("thresholds") or {}).get("signals") or {}).get("avwap_flip") or
           ((cfg.get("thresholds") or {}).get("signals") or {}).get("avwap_flip") or {})
    # 规则优先，阈值补全
    out = dict(thr)
    out.update(rules)
    return out

def _vol_gate_cfg(cfg: Dict) -> Dict:
    return (_core(cfg).get("volume_spike") or {})

def _allow_short(cfg: Dict) -> bool:
    return bool(_core(cfg).get("allow_short", True))

def _ttl_hours_for_tf(tf: str) -> int:
    tf = (tf or "").lower()
    if tf == "1h":
        return 24
    if tf == "4h":
        return 48
    return 24

def detect_one(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    cfg: Dict,
) -> List[Dict[str, Any]]:
    """
    ——变更点——
    1) t_ref 向下取整为桶时间 t_ref_bucket
    2) 之后所有取数与入库用 t_ref_bucket
    """
    from datetime import datetime

    def _bucketize_tref(ts: str, tf: str) -> str:
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

    t_ref_bucket = _bucketize_tref(t_ref, timeframe)

    out: List[Dict[str, Any]] = []

    prev, last = get_last_two_rows(conn, symbol, timeframe, t_ref_bucket)
    if not last or not prev:
        return out

    close_now = float(last["close"])
    close_prev = float(prev["close"])
    prev_low   = float(prev["low"])
    prev_high  = float(prev["high"])
    t_prev     = str(prev["t"])

    atr = float(compute_atr_from_db(conn, symbol, timeframe, t_ref_bucket, length=14))
    vs  = compute_vol_spike_ratio_from_db(conn, symbol, timeframe, t_ref_bucket, lookback=int((_core(cfg).get("volume_spike") or {}).get("lookback_bars", 24) or 24))
    vol_ratio = float(vs.get("ratio") or 0.0)
    if atr <= 0.0:
        return out

    s_cfg = (_core(cfg).get("thresholds") or {}).get("signals", {}).get("avwap_flip", {}) or {}
    anchor_kind    = str(s_cfg.get("anchor", "weekly_open"))
    pivot_lookback = int(s_cfg.get("pivot_lookback", 50) or 50)
    rr_min         = float(s_cfg.get("rr_min", 1.8))
    take_mult      = float(s_cfg.get("take_mult", 2.5))
    vol_spike_min_long  = float(s_cfg.get("vol_spike_min", 1.5))
    vol_spike_min_short = float(s_cfg.get("vol_spike_min_short", vol_spike_min_long + 0.1))

    try:
        anchor_ts_now = get_anchor_ts(conn=conn, symbol=symbol, timeframe=timeframe, t_ref=t_ref_bucket, anchor=anchor_kind, pivot_lookback=pivot_lookback)
    except Exception:
        return out

    avwap_prev = compute_avwap_from_db(conn, symbol, timeframe, anchor_ts_now, t_prev)
    avwap_now  = compute_avwap_from_db(conn, symbol, timeframe, anchor_ts_now, t_ref_bucket)
    if not (avwap_prev == avwap_prev and avwap_now == avwap_now):
        return out

    flip = crossed_avwap(close_prev, close_now, avwap_prev, avwap_now)

    if flip == "up" and vol_ratio >= vol_spike_min_long:
        entry  = close_now
        stop   = max(float(avwap_now), prev_low)
        target = entry + take_mult * atr
        rr_est = compute_rr(entry, stop, target, side="long")
        if rr_est >= rr_min and stop < entry:
            out.append({
                "t_ref": t_ref_bucket,
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "sig_avwap_flip",
                "direction": "long",
                "score": None, "prob": None, "strength": None,
                "detail_json": json.dumps({
                    "anchor": anchor_kind, "anchor_ts": anchor_ts_now,
                    "avwap_prev": avwap_prev, "avwap_now": avwap_now,
                    "volume_spike_ratio": vol_ratio, "atr": atr, "rr_est": rr_est,
                    "entry": entry, "stop": stop, "target": target,
                }, ensure_ascii=False),
                "status": "active",
                "expires_at": _calc_expires_at(t_ref_bucket, timeframe),
            })

    if flip == "down" and _allow_short(cfg) and vol_ratio >= vol_spike_min_short:
        entry  = close_now
        stop   = min(float(avwap_now), prev_high)
        target = entry - take_mult * atr
        rr_est = compute_rr(entry, stop, target, side="short")
        if rr_est >= rr_min and stop > entry:
            out.append({
                "t_ref": t_ref_bucket,
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "sig_avwap_flip",
                "direction": "short",
                "score": None, "prob": None, "strength": None,
                "detail_json": json.dumps({
                    "anchor": anchor_kind, "anchor_ts": anchor_ts_now,
                    "avwap_prev": avwap_prev, "avwap_now": avwap_now,
                    "volume_spike_ratio": vol_ratio, "atr": atr, "rr_est": rr_est,
                    "entry": entry, "stop": stop, "target": target,
                }, ensure_ascii=False),
                "status": "active",
                "expires_at": _calc_expires_at(t_ref_bucket, timeframe),
            })

    return out


def _calc_expires_at(t_ref: str, timeframe: str) -> str:
    """
    TTL = 3 bars * timeframe，兜底上限 3 天
    适用：sig_avwap_flip
    """
    from datetime import datetime, timedelta
    s = str(t_ref)[:19].replace("T", " ")
    try:
        base = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        base = datetime.fromisoformat(s)

    tf = (timeframe or "").strip().lower()
    if tf.endswith("h"):
        tf_seconds = int(tf[:-1] or 1) * 3600
    elif tf.endswith("m"):
        tf_seconds = int(tf[:-1] or 1) * 60
    elif tf.endswith("d"):
        tf_seconds = int(tf[:-1] or 1) * 86400
    else:
        tf_seconds = 3600

    bars = 3
    ttl_seconds = bars * tf_seconds
    cap_seconds = 3 * 86400

    expires = base + timedelta(seconds=min(ttl_seconds, cap_seconds))
    return expires.strftime("%Y-%m-%d %H:%M:%S")



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
    now_ts = now_local_str(cfg.get("tz","Asia/Shanghai"))

    for tf in tfs:
        for sym in symbols:
            cands = detect_one(conn, sym, tf, t_ref, cfg)
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

