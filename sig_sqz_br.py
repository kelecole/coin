# -*- coding: utf-8 -*-
"""
core/sig_sqz_br.py

收敛 → 放量破位（Squeeze Breakout）入库信号：
- source='sig_sqz_br'
- direction in {'long','short'}
- status='active'
- expires_at：1h→+24h，4h→+48h（可调）

定义：
- “收敛”判定： (BB宽度 / ATR) <= bb_width_norm_max  且  width_pctile <= squeeze_pctile_max
- “破位”判定：
    long : close[t] > BB_upper[t] 且 vol_spike_ratio >= vol_spike_min_long
    short: close[t] < BB_lower[t] 且 vol_spike_ratio >= vol_spike_min_short
- 止损：
    long : stop = min(low[t-1], squeeze_low)
    short: stop = max(high[t-1], squeeze_high)
- 目标：target = entry ± take_mult * ATR
- 预估RR：>= rr_min 才入库

依赖：
- core.indicators: compute_bb_from_db, compute_atr_from_db, compute_vol_spike_ratio_from_db,
                   get_last_two_rows, compute_rr
- signals 表具备唯一键 (t_ref, symbol, timeframe, source, direction)
"""

from __future__ import annotations
import sqlite3
import json
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta

# [修改] 导入 timebox
from core.timebox import now_local_str

from .indicators import (
    compute_bb_from_db,
    compute_atr_from_db,
    compute_vol_spike_ratio_from_db,
    get_last_two_rows,
    compute_rr,
)

# ---------------------------------------------------------------------
# 配置访问
# ---------------------------------------------------------------------

def _core(cfg: Dict) -> Dict:
    return cfg.get("core", cfg)

def _sig_cfg(cfg: dict) -> dict:
    core = cfg.get("core", cfg) or {}
    rules = ((core.get("signals_rules") or {}).get("sqz_br") or
             (cfg.get("signals_rules") or {}).get("sqz_br") or {})
    thr = (((core.get("thresholds") or {}).get("signals") or {}).get("sqz_br") or
           ((cfg.get("thresholds") or {}).get("signals") or {}).get("sqz_br") or {})
    out = dict(thr)
    out.update(rules)
    return out


def _vol_gate_cfg(cfg: Dict) -> Dict:
    # config.core.volume_spike
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


# ---------------------------------------------------------------------
# 判定：是否“收敛”
# ---------------------------------------------------------------------

def is_squeeze(
    bb_width_atr_norm: float,
    width_pctile: Optional[float],
    *,
    bb_width_norm_max: float = 1.2,
    pctile_max: float = 0.2,
) -> bool:
    """
    收敛判定：
      - bb_width_atr_norm = (BB_upper - BB_lower) / ATR
      - width_pctile      = 当前宽度在近样本中的分位（0~1）
    同时满足：bb_width_atr_norm <= bb_width_norm_max 且 width_pctile <= pctile_max
    """
    if bb_width_atr_norm is None or not (bb_width_atr_norm == bb_width_atr_norm):  # NaN
        return False
    if width_pctile is None or not (0.0 <= width_pctile <= 1.0):
        return False
    return (bb_width_atr_norm <= float(bb_width_norm_max)) and (width_pctile <= float(pctile_max))


# ---------------------------------------------------------------------
# 单票检测
# ---------------------------------------------------------------------







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


def detect_one(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    cfg: Dict,
) -> List[Dict[str, Any]]:
    """
    返回 0~1 个候选（多或空）。
    ——变更点——
    1) 先把 t_ref 向下取整到桶时间（1h→整点；4h→0/4/8/…）
    2) 后续所有取数与入库一律用桶后的 t_ref_bucket
    """
    from datetime import datetime

    def _bucketize_tref(ts: str, tf: str) -> str:
        # 统一把字符串裁成 'YYYY-MM-DD HH:MM:SS' 再向下取整
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

    # 最近两根（只用闭合K）
    prev, last = get_last_two_rows(conn, symbol, timeframe, t_ref_bucket)
    if not last or not prev:
        return out

    close_now = float(last["close"])
    prev_low  = float(prev["low"])
    prev_high = float(prev["high"])

    # 指标：BB + ATR + 放量比（都用桶后的时间）
    bb = compute_bb_from_db(conn, symbol, timeframe, t_ref_bucket, length=20, k=2.0, lookback_for_pctile=120)
    atr = float(compute_atr_from_db(conn, symbol, timeframe, t_ref_bucket, length=14))
    vs  = compute_vol_spike_ratio_from_db(conn, symbol, timeframe, t_ref_bucket, lookback=int((_core(cfg).get("volume_spike") or {}).get("lookback_bars", 24) or 24))

    if atr <= 0.0 or bb.get("upper") is None or bb.get("lower") is None:
        return out

    bb_upper = float(bb["upper"])
    bb_lower = float(bb["lower"])
    bb_mid   = float(bb["mid"]) if bb.get("mid") is not None else None
    width_pctile = bb.get("width_pctile")
    squeeze_low  = bb.get("squeeze_low")
    squeeze_high = bb.get("squeeze_high")

    bb_width_price = bb_upper - bb_lower
    bb_width_atr_norm = float(bb_width_price / max(atr, 1e-12))

    # 阈值
    s_cfg = (_core(cfg).get("thresholds") or {}).get("signals", {}).get("sqz_br", {}) or {}
    bb_width_norm_max    = float(s_cfg.get("bb_width_norm_max", 1.2))
    squeeze_pctile_max   = float(s_cfg.get("squeeze_pctile_max", 0.2))
    vol_spike_min_long   = float(s_cfg.get("vol_spike_min_long", 1.6))
    vol_spike_min_short  = float(s_cfg.get("vol_spike_min_short", 1.8))
    rr_min               = float(s_cfg.get("rr_min", 2.0))
    take_mult            = float(s_cfg.get("take_mult", 2.5))

    vol_ratio = float(vs.get("ratio") or 0.0)

    # 收敛判定
    if not is_squeeze(bb_width_atr_norm, width_pctile, bb_width_norm_max=bb_width_norm_max, pctile_max=squeeze_pctile_max):
        return out

    # 破位触发
    long_trigger  = (close_now > bb_upper) and (vol_ratio >= vol_spike_min_long)
    short_trigger = (close_now < bb_lower) and (vol_ratio >= vol_spike_min_short)

    # long
    if long_trigger:
        entry  = close_now
        stop_cands = []
        if squeeze_low is not None:
            try: stop_cands.append(float(squeeze_low))
            except: pass
        stop_cands.append(prev_low)
        stop = min([x for x in stop_cands if x == x])
        target = entry + take_mult * atr
        rr_est = compute_rr(entry, stop, target, side="long")

        if rr_est >= rr_min and stop < entry:
            out.append({
                "t_ref": t_ref_bucket,
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "sig_sqz_br",
                "direction": "long",
                "score": None,
                "prob": None,
                "strength": None,
                "detail_json": json.dumps({
                    "bb_upper": bb_upper,
                    "bb_lower": bb_lower,
                    "bb_mid": bb_mid,
                    "bb_width_atr_norm": bb_width_atr_norm,
                    "squeeze_pctile": width_pctile,
                    "squeeze_low": squeeze_low,
                    "squeeze_high": squeeze_high,
                    "volume_spike_ratio": vol_ratio,
                    "atr": atr,
                    "rr_est": rr_est,
                    "entry": entry,
                    "stop": stop,
                    "target": target,
                }, ensure_ascii=False),
                "status": "active",
                "expires_at": _calc_expires_at(t_ref_bucket, timeframe),
            })

    # short
    if short_trigger and _allow_short(cfg):
        entry  = close_now
        stop_cands = []
        if squeeze_high is not None:
            try: stop_cands.append(float(squeeze_high))
            except: pass
        stop_cands.append(prev_high)
        stop = max([x for x in stop_cands if x == x])
        target = entry - take_mult * atr
        rr_est = compute_rr(entry, stop, target, side="short")

        if rr_est >= rr_min and stop > entry:
            out.append({
                "t_ref": t_ref_bucket,
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "sig_sqz_br",
                "direction": "short",
                "score": None,
                "prob": None,
                "strength": None,
                "detail_json": json.dumps({
                    "bb_upper": bb_upper,
                    "bb_lower": bb_lower,
                    "bb_mid": bb_mid,
                    "bb_width_atr_norm": bb_width_atr_norm,
                    "squeeze_pctile": width_pctile,
                    "squeeze_low": squeeze_low,
                    "squeeze_high": squeeze_high,
                    "volume_spike_ratio": vol_ratio,
                    "atr": atr,
                    "rr_est": rr_est,
                    "entry": entry,
                    "stop": stop,
                    "target": target,
                }, ensure_ascii=False),
                "status": "active",
                "expires_at": _calc_expires_at(t_ref_bucket, timeframe),
            })

    return out


def _calc_expires_at(t_ref: str, timeframe: str) -> str:
    """
    TTL = 2 bars * timeframe，兜底上限 3 天
    适用：sig_sqz_br（收敛放量破位）
    """
    from datetime import datetime, timedelta
    s = str(t_ref)[:19].replace("T", " ")
    try:
        base = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        base = datetime.fromisoformat(s)

    # 解析 timeframe 时长
    tf = (timeframe or "").strip().lower()
    if tf.endswith("h"):
        tf_seconds = int(tf[:-1] or 1) * 3600
    elif tf.endswith("m"):
        tf_seconds = int(tf[:-1] or 1) * 60
    elif tf.endswith("d"):
        tf_seconds = int(tf[:-1] or 1) * 86400
    else:
        tf_seconds = 3600  # 合理兜底

    bars = 2
    ttl_seconds = bars * tf_seconds
    cap_seconds = 3 * 86400  # 3 天上限

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
