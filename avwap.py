# -*- coding: utf-8 -*-
"""
core/avwap.py

功能：
- get_anchor_ts(...): 计算 AVWAP 锚点时间（周开盘 / 枢轴拐点）
- compute_avwap_from_db(...): 从锚点到 t_ref（均为闭合K）计算锚定VWAP
- crossed_avwap(...): 给出 close 与 AVWAP 的翻越方向（up/down/None）

约束：
- 所有时间统一为 Asia/Shanghai 的 'YYYY-MM-DD HH:MM:SS'
- 一律只用 <= t_ref 的已收盘K；不会使用半根/未来K
- ohlcv 列名自适应（t/open/high/low/close/volume 常见别名）

依赖：
- sqlite3 / pandas / numpy
- from timezone_converter import TimezoneConverter   # 位于 core 上级目录
"""

from __future__ import annotations
import sqlite3
import logging
import re
from typing import Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from timezone_converter import TimezoneConverter
# 放在文件顶部 imports 之后
try:
    # 建议在 timebox 中提供这两个；若没有，也能走下面 fallback
    from core.timebox import to_local_str as _tb_to_local_str
except Exception:
    _tb_to_local_str = None

from datetime import timedelta
import pandas as pd

def _to_shanghai_str(x) -> str:
    """
    统一规范为 'YYYY-MM-DD HH:MM:SS'（Asia/Shanghai）。
    优先走 core.timebox；没有则用本地 fallback（保留你原有逻辑）。
    """
    if _tb_to_local_str is not None:
        try:
            return _tb_to_local_str(x, tz="Asia/Shanghai")
        except Exception:
            pass  # 回退到本地实现

    # ---- fallback：你原来的规范化逻辑（精简保留）----
    if hasattr(x, "to_pydatetime"):
        x = x.to_pydatetime()

    import pandas as pd, re
    from datetime import datetime as _dt
    TZC = pd.Timestamp.now(tz="Asia/Shanghai").tz

    if isinstance(x, (int, float)):
        return pd.Timestamp(x, unit="s", tz="UTC").tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(x, _dt):
        if x.tzinfo is None:
            # 视为已是上海本地
            return x.strftime("%Y-%m-%d %H:%M:%S")
        return pd.Timestamp(x).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")

    s = str(x or "").strip().replace("T", " ")
    if not s:
        return s
    if re.search(r"[Zz]|[+\-]\d{2}:\d{2}$", s):
        ts = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(ts):
            return s[:19]
        return ts.tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return s[:19]
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _floor_to_candle(t_ref: str, timeframe: str) -> str:
    """
    落到该 TF 最后收盘（<=t_ref）。不含未来/半根。
    """
    s = _to_shanghai_str(t_ref)
    dt = pd.to_datetime(s)

    tf = (timeframe or "").lower().strip()
    if tf.endswith("m"):
        m = int(tf[:-1]) if tf[:-1].isdigit() else 1
        floored = dt - pd.to_timedelta(dt.minute % m, unit="m")
        floored = floored.replace(second=0, microsecond=0)
    elif tf.endswith("h"):
        h = int(tf[:-1]) if tf[:-1].isdigit() else 1
        floored = dt.replace(minute=0, second=0, microsecond=0)
        floored = floored.replace(hour=(dt.hour // h) * h)
    elif tf.endswith("d"):
        floored = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"不支持的时间周期: {timeframe}")

    return floored.strftime("%Y-%m-%d %H:%M:%S")


def _last_closed_tref(t_ref: str, timeframe: str) -> str:
    """统一由 floor 函数给出 <= t_ref 的最后一根收盘K时间"""
    return _floor_to_candle(t_ref, timeframe)



# -----------------------------------------------------------------------------
# DB 工具：自适应列名 + 读取窗口
# -----------------------------------------------------------------------------

def _detect_ohlcv_cols(conn: sqlite3.Connection):
    rows = conn.execute("PRAGMA table_info('ohlcv');").fetchall()
    if not rows:
        raise RuntimeError("未找到表 ohlcv")

    cols = [r[1] for r in rows]
    lc = {c.lower(): c for c in cols}

    def pick(*cands):
        for c in cands:
            if c in lc:
                return lc[c]
        return None

    t_col = pick("t", "ts", "time", "timestamp", "datetime")
    o_col = pick("open", "o")
    h_col = pick("high", "h")
    l_col = pick("low", "l")
    c_col = pick("close", "c")
    v_col = pick("volume", "vol", "v")
    miss = [k for k, v in dict(t=t_col, open=o_col, high=h_col, low=l_col, close=c_col, volume=v_col).items() if v is None]
    if miss:
        raise RuntimeError(f"ohlcv 缺少列: {miss}; 已有={cols}")
    return dict(t=t_col, open=o_col, high=h_col, low=l_col, close=c_col, volume=v_col)


def _fetch_window_df(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    start_ts: str,
    end_ts: str,
) -> pd.DataFrame:
    """读取 [start_ts, end_ts] 的闭合K（升序），时间统一为上海文本。"""
    start_s = _last_closed_tref(start_ts, timeframe)
    end_s = _last_closed_tref(end_ts, timeframe)
    cols = _detect_ohlcv_cols(conn)
    sql = f"""
    SELECT {cols['t']}   AS t,
           {cols['open']}  AS open,
           {cols['high']}  AS high,
           {cols['low']}   AS low,
           {cols['close']} AS close,
           {cols['volume']} AS volume
    FROM ohlcv
    WHERE symbol=? AND timeframe=? AND {cols['t']} >= ? AND {cols['t']} <= ?
    ORDER BY {cols['t']} ASC
    """
    rows = conn.execute(sql, (symbol, timeframe, start_s, end_s)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["t", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
    df["t"] = df["t"].map(_to_shanghai_str)
    return df


def _fetch_last_n_bars_df(
    conn: sqlite3.Connection, symbol: str, timeframe: str, t_ref: str, n: int
) -> pd.DataFrame:
    """读取 <= t_ref 的最近 n 根K（升序）。"""
    if n <= 0:
        raise ValueError("n 必须为正整数")
    t_ref_closed = _last_closed_tref(t_ref, timeframe)
    cols = _detect_ohlcv_cols(conn)
    sql = f"""
    SELECT {cols['t']}   AS t,
           {cols['open']}  AS open,
           {cols['high']}  AS high,
           {cols['low']}   AS low,
           {cols['close']} AS close,
           {cols['volume']} AS volume
    FROM ohlcv
    WHERE symbol=? AND timeframe=? AND {cols['t']} <= ?
    ORDER BY {cols['t']} DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (symbol, timeframe, t_ref_closed, int(n))).fetchall()
    if not rows:
        return pd.DataFrame(columns=["t", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
    df["t"] = df["t"].map(_to_shanghai_str)
    return df.iloc[::-1].reset_index(drop=True)


# -----------------------------------------------------------------------------
# 锚点：weekly_open / pivot_lookback
# -----------------------------------------------------------------------------

def _week_start_local(t_ref_closed: str) -> str:
    """
    求自然周起始（周一 00:00:00，上海），返回文本。
    """
    dt = pd.to_datetime(_to_shanghai_str(t_ref_closed))
    monday = dt - timedelta(days=dt.weekday())
    start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.strftime("%Y-%m-%d %H:%M:%S")


def _find_first_bar_on_or_after(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    start_ts: str,
    end_ts: str,
) -> Optional[str]:
    """
    在 [start_ts, end_ts] 内找到第一根K的时间；若不存在返回 None。
    """
    cols = _detect_ohlcv_cols(conn)
    sql = f"""
    SELECT {cols['t']}
    FROM ohlcv
    WHERE symbol=? AND timeframe=? AND {cols['t']} >= ? AND {cols['t']} <= ?
    ORDER BY {cols['t']} ASC
    LIMIT 1
    """
    row = conn.execute(sql, (symbol, timeframe, start_ts, end_ts)).fetchone()
    return _to_shanghai_str(row[0]) if row else None


def _detect_recent_pivot_ts(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    lookback: int = 50,
    k: int = 2,
) -> Optional[str]:
    """
    基于最近 lookback 根内的摆动拐点（k 邻域）寻找“最近的” pivot 时间：
      - pivot high: high[i] > 邻域（左右各 k 根）
      - pivot low : low[i]  < 邻域
    返回最近（靠近 t_ref）的 pivot 的时间（优先取最后出现的高/低点之一）。
    若无 pivot，则返回窗口中的第一根时间。
    """
    n = max(lookback + k * 2 + 3, lookback + 5)
    df = _fetch_last_n_bars_df(conn, symbol, timeframe, t_ref, n=n)
    if df.empty or len(df) < (k * 2 + 3):
        return None

    highs = df["high"].to_numpy(dtype=float, copy=False)
    lows = df["low"].to_numpy(dtype=float, copy=False)

    pivots = []  # list of (idx, 'H'|'L')
    for i in range(k, len(df) - k):
        left_h = highs[i - k:i]
        right_h = highs[i + 1:i + 1 + k]
        left_l = lows[i - k:i]
        right_l = lows[i + 1:i + 1 + k]

        is_ph = highs[i] > np.max(left_h) and highs[i] > np.max(right_h)
        is_pl = lows[i] < np.min(left_l) and lows[i] < np.min(right_l)
        if is_ph:
            pivots.append((i, 'H'))
        if is_pl:
            pivots.append((i, 'L'))

    if not pivots:
        # 没有“显著拐点”，退化为窗口第一根
        return _to_shanghai_str(df.iloc[0]["t"])

    # 取最近一个（下标最大者）
    idx, _typ = pivots[-1]
    return _to_shanghai_str(df.iloc[idx]["t"])


def get_anchor_ts(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    anchor: str = "weekly_open",
    pivot_lookback: int = 50,
) -> str:
    """
    计算 AVWAP 锚点时间：
      - weekly_open: 自然周第一根闭合K（数据库中实际存在的第一根）
      - pivot_lookback: 最近 lookback 内的摆动拐点（无则窗口第一根）
    返回值为上海本地文本时间；若无法找到，抛出异常。
    """
    t_ref_closed = _last_closed_tref(t_ref, timeframe)

    if anchor == "weekly_open":
        week_start = _week_start_local(t_ref_closed)
        anc = _find_first_bar_on_or_after(conn, symbol, timeframe, week_start, t_ref_closed)
        if not anc:
            raise RuntimeError(f"[avwap] weekly_open 未找到首根K: {symbol} {timeframe} {week_start}~{t_ref_closed}")
        return anc

    elif anchor == "pivot_lookback":
        anc = _detect_recent_pivot_ts(conn, symbol, timeframe, t_ref_closed, lookback=pivot_lookback, k=2)
        if not anc:
            raise RuntimeError(f"[avwap] pivot_lookback 未找到锚点: {symbol} {timeframe} lookback={pivot_lookback}")
        return anc

    else:
        raise ValueError(f"未知锚点类型: {anchor}")


# -----------------------------------------------------------------------------
# 计算 AVWAP（锚定 VWAP）：HLC3 * Volume / sum(Volume)
# -----------------------------------------------------------------------------

def compute_avwap_from_db(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    anchor_ts: str,
    t_ref: str,
) -> float:
    """
    从 anchor_ts 到 t_ref（均按各自周期落到闭合边界）计算锚定 VWAP。
    价格使用典型价 HLC3；返回 float（若数据不足/成交量为0则返回 NaN）。
    """
    start_s = _last_closed_tref(anchor_ts, timeframe)
    end_s = _last_closed_tref(t_ref, timeframe)
    if start_s > end_s:
        logging.warning(f"[avwap] 锚点在 t_ref 之后: {start_s} > {end_s}")
        return float("nan")

    df = _fetch_window_df(conn, symbol, timeframe, start_s, end_s)
    if df.empty:
        logging.warning(f"[avwap] 无数据: {symbol} {timeframe} {start_s}~{end_s}")
        return float("nan")

    # 典型价 HLC3
    h = df["high"].astype(float).to_numpy(copy=False)
    l = df["low"].astype(float).to_numpy(copy=False)
    c = df["close"].astype(float).to_numpy(copy=False)
    v = df["volume"].astype(float).to_numpy(copy=False)

    tp = (h + l + c) / 3.0
    num = float(np.sum(tp * v))
    den = float(np.sum(v))
    if den <= 1e-12:
        logging.warning(f"[avwap] 成交量为0: {symbol} {timeframe} {start_s}~{end_s}")
        return float("nan")
    return num / den


# -----------------------------------------------------------------------------
# 翻越判定：up / down / None
# -----------------------------------------------------------------------------

def crossed_avwap(
    close_prev: float,
    close_now: float,
    avwap_prev: float,
    avwap_now: float,
    eps: float = 0.0,
) -> Optional[str]:
    """
    判定 close 是否对 AVWAP 发生“翻越”：
      - up:    close_prev <= avwap_prev 且 close_now > avwap_now + eps
      - down:  close_prev >= avwap_prev 且 close_now < avwap_now - eps
      - None:  其余情况或存在 NaN
    """
    vals = [close_prev, close_now, avwap_prev, avwap_now]
    if any(v is None for v in vals):
        return None
    try:
        cp, cn, ap, an = map(float, vals)
    except Exception:
        return None

    if (cp <= ap) and (cn > an + eps):
        return "up"
    if (cp >= ap) and (cn < an - eps):
        return "down"
    return None