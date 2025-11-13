# -*- coding: utf-8 -*-
"""
core/indicators.py

职责（只读DB，不连交易所）：
- 统一时间口径：全部使用 Asia/Shanghai 文本时间 'YYYY-MM-DD HH:MM:SS'
- 严格截断：仅使用 <= t_ref 的闭合K（按 timeframe 向下取整到已收盘）
- 通用指标：ATR(14)、布林带（含宽度分位）、自比较放量比
- 便捷打包：一次性拉齐三张牌常用字段
- 仅用历史K，不窥未来

依赖：
- sqlite3 / pandas / numpy
- from timezone_converter import TimezoneConverter   # 位于 core 的上级目录
"""

from __future__ import annotations
import sqlite3
import logging
import re
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# 直接复用你项目里的时区转换器（位于 core 上级目录）
from timezone_converter import TimezoneConverter

# 全局单例（避免重复构造）
_TZC = TimezoneConverter(local_timezone='Asia/Shanghai')


# =============================================================================
# 时区与时间工具（全部输出上海时区的 19 位字符串）
# =============================================================================

_TZ_SUFFIX_RE = re.compile(r'(Z|[+-]\d{2}:\d{2}|[+-]\d{4})$')

def _to_shanghai_str(x) -> str:
    """
    把各种时间类型规范为 'YYYY-MM-DD HH:MM:SS'（上海时区的本地时间文本）。

    规则（避免二次转时区）：
    - 数值(int/float)：视为 UTC epoch 秒 → TimezoneConverter → 上海文本
    - datetime 有 tz：按 tz → epoch(UTC) → TimezoneConverter → 上海文本
    - datetime 无 tz（naive）：视为“已是上海本地时间”，直接格式化
    - 字符串：
        * 若末尾带时区（Z/+08:00/+0800 等），按“有 tz”路径
        * 否则视为“已是上海本地时间”，只做规范化到 19 位
    """
    # pandas.Timestamp
    if hasattr(x, "to_pydatetime"):
        x = x.to_pydatetime()

    # epoch 秒
    if isinstance(x, (int, float)):
        s = _TZC.convert_to_local(float(x))
        return s[:19]

    # datetime
    if isinstance(x, datetime):
        if x.tzinfo is not None and x.tzinfo.utcoffset(x) is not None:
            epoch = x.timestamp()  # 含 tz 的安全转换
            s = _TZC.convert_to_local(epoch)
            return s[:19]
        # naive：视为已是上海本地
        return x.strftime("%Y-%m-%d %H:%M:%S")

    # 字符串
    s = str(x or "").strip()
    if not s:
        return s
    s_norm = s.replace("T", " ").strip()
    # 明确带 tz 的字符串：统一按 UTC 解析
    if _TZ_SUFFIX_RE.search(s_norm) or s_norm.endswith("Z"):
        ts = pd.to_datetime(s_norm, utc=True, errors="coerce")
        if pd.isna(ts):
            # 兜底：无法解析就直接截成 19 位
            return s_norm[:19]
        epoch = ts.value / 1e9  # ns → s
        return _TZC.convert_to_local(epoch)[:19]
    # naive 字符串：视为已是上海本地
    try:
        dt = pd.to_datetime(s_norm, errors="coerce")
        if pd.isna(dt):
            return s_norm[:19]
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s_norm[:19]


def _parse_timeframe_to_td(tf: str) -> timedelta:
    """'1m'/'1h'/'4h'/'1d' → timedelta。"""
    tf = (tf or "").strip().lower()
    unit = tf[-1:]
    val = int(tf[:-1]) if tf[:-1].isdigit() else 1
    if unit == "m":
        return timedelta(minutes=val)
    if unit == "h":
        return timedelta(hours=val)
    if unit == "d":
        return timedelta(days=val)
    raise ValueError(f"不支持的时间周期: {tf}")


def _floor_to_candle(t_ref: str, timeframe: str) -> str:
    """
    把任意 t_ref（可约等于“现在”）落到【这一周期最近的收盘时刻】（不含未收盘K）。
    例如：t_ref='2025-11-05 13:27:11', timeframe='4h' → '2025-11-05 12:00:00'
    """
    # 统一到本地时间，不做二次转时区
    s = _to_shanghai_str(t_ref)
    dt = pd.to_datetime(s)

    if timeframe.endswith("m"):
        minutes = int(timeframe[:-1]) if timeframe[:-1].isdigit() else 1
        floored = dt - timedelta(minutes=dt.minute % minutes,
                                 seconds=dt.second,
                                 microseconds=dt.microsecond)
    elif timeframe.endswith("h"):
        hours = int(timeframe[:-1]) if timeframe[:-1].isdigit() else 1
        floored_hour = (dt.hour // hours) * hours
        floored = dt.replace(hour=floored_hour, minute=0, second=0, microsecond=0)
    elif timeframe.endswith("d"):
        floored = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"不支持的时间周期: {timeframe}")

    return floored.strftime("%Y-%m-%d %H:%M:%S")


def _last_closed_tref(t_ref: str, timeframe: str) -> str:
    """
    计算“在 t_ref 时点，最晚一根已收盘K”的时间戳（上海时区文本）。
    若 t_ref 正好是收盘边界，返回它本身；若介于两根内，返回上一根收盘。
    """
    return _floor_to_candle(t_ref, timeframe)


# =============================================================================
# DB 读取：仅取 <= last_closed_tref 的闭合K；列名自适应
# =============================================================================

def _detect_ohlcv_cols(conn: sqlite3.Connection) -> Dict[str, str]:
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
        raise RuntimeError(f"ohlcv 表缺少必要列: {miss}; 已有={cols}")
    return dict(t=t_col, open=o_col, high=h_col, low=l_col, close=c_col, volume=v_col)


def fetch_last_n_bars_df(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    n: int,
) -> pd.DataFrame:
    """
    读取 <= last_closed_tref 的最近 n 根K（升序）。
    - 自动把 t_ref 落在上一根收盘边界（防未收盘）
    - 若不足 n 根，返回能取到的全部
    """
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
    WHERE symbol = ? AND timeframe = ? AND {cols['t']} <= ?
    ORDER BY {cols['t']} DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (symbol, timeframe, t_ref_closed, int(n))).fetchall()
    if not rows:
        return pd.DataFrame(columns=["t", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
    # 统一把 t 标准化为上海时区文本索引（便于上层一致处理）
    df["t"] = df["t"].map(_to_shanghai_str)
    # 升序
    df = df.iloc[::-1].reset_index(drop=True)
    return df


def fetch_window_df(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    start_ts: str,
    end_ts: str,
) -> pd.DataFrame:
    """
    读取 [start_ts, end_ts] 的闭合K（升序）。
    入参可为任意时间类型，内部统一到上海文本。
    """
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
    WHERE symbol = ? AND timeframe = ? AND {cols['t']} >= ? AND {cols['t']} <= ?
    ORDER BY {cols['t']} ASC
    """
    rows = conn.execute(sql, (symbol, timeframe, start_s, end_s)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["t", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume"])
    df["t"] = df["t"].map(_to_shanghai_str)
    return df


def get_last_close_from_db(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
) -> float:
    """
    返回 <= last_closed_tref 的最后一根K的收盘价。
    """
    tref = _last_closed_tref(t_ref, timeframe)
    cols = _detect_ohlcv_cols(conn)
    row = conn.execute(
        f"""
        SELECT {cols['close']}
        FROM ohlcv
        WHERE symbol=? AND timeframe=? AND {cols['t']} <= ?
        ORDER BY {cols['t']} DESC
        LIMIT 1
        """,
        (symbol, timeframe, tref),
    ).fetchone()
    if not row or row[0] is None:
        raise RuntimeError(f"找不到 {symbol} {timeframe} 在 {tref} 及以前的闭合K")
    px = float(row[0])
    if px <= 0:
        raise RuntimeError(f"价格非正: {px}")
    return px


def get_last_two_rows(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, float]]]:
    """
    返回最近两根闭合K（t-1,t），按时间升序：
      (prev_row, last_row)
    若不足两根，prev_row 为 None。
    """
    df = fetch_last_n_bars_df(conn, symbol, timeframe, t_ref, n=2)
    if df.empty:
        return None, None
    if len(df) == 1:
        return None, df.iloc[-1].to_dict()
    return df.iloc[-2].to_dict(), df.iloc[-1].to_dict()


# =============================================================================
# 指标：ATR / 布林带 / 放量比（自比较）
# =============================================================================

def compute_atr_from_db(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    length: int = 14,
) -> float:
    """
    ATR(length)；不足 length+1 根则返回 NaN。
    公式：TR = max(H-L, |H-C_prev|, |L-C_prev|)；ATR = SMA(TR, length)
    """
    need = max(length + 1, length + 2)
    df = fetch_last_n_bars_df(conn, symbol, timeframe, t_ref, n=need)
    if df.empty or len(df) < (length + 1):
        logging.warning(f"[ind] ATR 数据不足 {symbol} {timeframe}@{t_ref} len={len(df)} need>={length+1}")
        return float("nan")

    h = df["high"].to_numpy(dtype=float, copy=False)
    l = df["low"].to_numpy(dtype=float, copy=False)
    c = df["close"].to_numpy(dtype=float, copy=False)

    c_prev = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum.reduce([h - l, np.abs(h - c_prev), np.abs(l - c_prev)])
    atr = pd.Series(tr).rolling(length).mean().iloc[-1]
    return float(atr) if pd.notna(atr) else float("nan")


def compute_bb_from_db(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    length: int = 20,
    k: float = 2.0,
    lookback_for_pctile: int = 120,
) -> Dict[str, Optional[float]]:
    """
    布林带 + 宽度分位（按近 lookback_for_pctile 的滚动窗口统计）。
    返回：upper/lower/mid/width_norm/width_pctile + squeeze_low/high
    """
    need = max(length + lookback_for_pctile + 5, length + 5)
    df = fetch_last_n_bars_df(conn, symbol, timeframe, t_ref, n=need)
    if df.empty or len(df) < length:
        logging.warning(f"[ind] BB 数据不足 {symbol} {timeframe} len={len(df)} need>={length}")
        return dict(upper=None, lower=None, mid=None, width_norm=None, width_pctile=None,
                    squeeze_low=None, squeeze_high=None)

    closes = df["close"].astype(float)
    s = closes.rolling(length)
    mid = s.mean().iloc[-1]
    sd = s.std(ddof=0).iloc[-1]
    if not (pd.notna(mid) and pd.notna(sd)):
        return dict(upper=None, lower=None, mid=None, width_norm=None, width_pctile=None,
                    squeeze_low=None, squeeze_high=None)

    upper = mid + k * sd
    lower = mid - k * sd
    last_close = float(closes.iloc[-1])
    width_norm = (upper - lower) / max(last_close, 1e-12)

    # 形成宽度序列（窗口为 length），计算当前宽度在近 lookback_for_pctile 内的分位
    ww = (closes.rolling(length).std(ddof=0) * (2.0 * k)) / (closes + 1e-12)
    ww = ww.dropna()
    if len(ww) == 0:
        width_pctile = None
    else:
        ww_tail = ww.iloc[-min(len(ww), lookback_for_pctile):]
        cur_w = ww.iloc[-1]
        width_pctile = float((ww_tail <= cur_w).mean())

    window = df.iloc[-length:]
    squeeze_low = float(window["low"].min())
    squeeze_high = float(window["high"].max())

    return dict(
        upper=float(upper),
        lower=float(lower),
        mid=float(mid),
        width_norm=float(width_norm),
        width_pctile=width_pctile,
        squeeze_low=squeeze_low,
        squeeze_high=squeeze_high,
    )


def compute_vol_spike_ratio_from_db(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    lookback: int = 24,
) -> Dict[str, Optional[float]]:
    """
    自比较放量比：ratio = 当前K量 / 近 lookback 根均量（不含当前K）。
    返回：{ratio, cur_vol, base_avg, lookback}
    """
    if lookback < 1:
        raise ValueError("lookback 必须 >= 1")

    need = lookback + 1
    df = fetch_last_n_bars_df(conn, symbol, timeframe, t_ref, n=need)
    if df.empty or len(df) < need:
        logging.warning(f"[ind] volume_spike 数据不足 {symbol} {timeframe} need={need}, has={len(df)}")
        return dict(ratio=float("nan"), cur_vol=float("nan"), base_avg=float("nan"), lookback=float(lookback))

    vol = df["volume"].astype(float).to_numpy(copy=False)
    cur = vol[-1]
    base = float(np.mean(vol[:-1]))  # 不包含当前K
    ratio = float(cur / (base + 1e-12))
    return dict(ratio=ratio, cur_vol=float(cur), base_avg=float(base), lookback=float(lookback))


# =============================================================================
# 交易辅助：RR / 止损宽度 / 目标空间
# =============================================================================

def compute_rr(entry: float, stop: float, target: float, side: str) -> float:
    side_l = (side or "").lower()
    if side_l not in ("long", "short"):
        raise ValueError("side 必须是 'long' 或 'short'")
    if side_l == "long":
        risk = max(entry - stop, 0.0)
        reward = max(target - entry, 0.0)
    else:
        risk = max(stop - entry, 0.0)
        reward = max(entry - target, 0.0)
    return float(reward / risk) if risk > 1e-12 else 0.0


def stop_frac(entry: float, stop: float, side: str) -> float:
    side_l = (side or "").lower()
    if side_l == "long":
        return float(max(entry - stop, 0.0) / max(entry, 1e-12))
    elif side_l == "short":
        return float(max(stop - entry, 0.0) / max(entry, 1e-12))
    else:
        raise ValueError("side 必须是 'long' 或 'short'")


def reward_frac(entry: float, target: float, side: str) -> float:
    side_l = (side or "").lower()
    if side_l == "long":
        return float(max(target - entry, 0.0) / max(entry, 1e-12))
    elif side_l == "short":
        return float(max(entry - target, 0.0) / max(entry, 1e-12))
    else:
        raise ValueError("side 必须是 'long' 或 'short'")


# =============================================================================
# 便捷组合：一次拉齐三张牌常用字段
# =============================================================================

def gather_core_indicators(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    *,
    atr_len: int = 14,
    bb_len: int = 20,
    bb_k: float = 2.0,
    bb_pct_lookback: int = 120,
    vol_lookback: int = 24,
) -> Dict[str, Any]:
    """
    返回 dict（不足时给 None/NaN，由上层决定是否放行）：
      last_close, prev_close, prev_high, prev_low,
      atr,
      bb_upper, bb_lower, bb_mid, bb_width_norm, bb_width_pctile, squeeze_low, squeeze_high,
      volume_spike_ratio, volume_cur, volume_base_avg
    """
    prev, last = get_last_two_rows(conn, symbol, timeframe, t_ref)
    last_close = float(last["close"]) if last else None
    prev_close = float(prev["close"]) if prev else None
    prev_high = float(prev["high"]) if prev else None
    prev_low = float(prev["low"]) if prev else None

    atr = compute_atr_from_db(conn, symbol, timeframe, t_ref, length=atr_len)
    bb = compute_bb_from_db(conn, symbol, timeframe, t_ref, length=bb_len, k=bb_k, lookback_for_pctile=bb_pct_lookback)
    vs = compute_vol_spike_ratio_from_db(conn, symbol, timeframe, t_ref, lookback=vol_lookback)

    return dict(
        last_close=last_close,
        prev_close=prev_close,
        prev_high=prev_high,
        prev_low=prev_low,
        atr=atr,
        bb_upper=bb.get("upper"),
        bb_lower=bb.get("lower"),
        bb_mid=bb.get("mid"),
        bb_width_norm=bb.get("width_norm"),
        bb_width_pctile=bb.get("width_pctile"),
        squeeze_low=bb.get("squeeze_low"),
        squeeze_high=bb.get("squeeze_high"),
        volume_spike_ratio=vs.get("ratio"),
        volume_cur=vs.get("cur_vol"),
        volume_base_avg=vs.get("base_avg"),
    )