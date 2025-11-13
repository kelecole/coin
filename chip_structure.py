# -*- coding: utf-8 -*-
"""
core/chip_structure.py

功能：
- 从本地 SQLite ohlcv 表读取窗口数据（自入场以来至 t_ref）
- 计算筹码峰（价格分箱 × 成交量权重直方图）
- 计算三价带 (VPOC/VAL/VAH)，用于结构化失效判定

设计要点：
- 分箱方式：fixed | percentage
- 权重口径：full(默认，98/99分位winsorize) / raw / log
- K线内线性分摊：将每根K线的成交量按 price_split_count 个等距价格点分摊至直方图
- 三价带为“以 VPOC 为中心的连续区间”，满足价值区覆盖比例 value_area_pct（默认 0.70）
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Tuple, Optional, List
import math
import numpy as np

try:
    import pandas as pd  # 建议环境已有 pandas；若无可改为纯 numpy 实现
except Exception:  # 兜底：没有 pandas 也不报错，但 fetch_ohlcv_df 需要 pandas
    pd = None


# ---------------------------
# 公用小工具
# ---------------------------

def _winsorize(values: np.ndarray, p: float = 0.99) -> np.ndarray:
    """对数据做上分位剪裁（winsorize），用于 full 权重抑制极端量能。"""
    if values.size == 0:
        return values
    p = float(min(max(p, 0.50), 0.999))  # 合理边界
    cap = np.quantile(values, p)
    return np.clip(values, 0.0, cap)


def _detect_ohlcv_cols(conn: sqlite3.Connection) -> Dict[str, str]:
    """
    动态探测 ohlcv 表的列名，适配不同命名习惯。
    期望存在的列：时间、open/high/low/close/volume。
    """
    cur = conn.execute("PRAGMA table_info(ohlcv);")
    cols = [row[1] for row in cur.fetchall()]
    lc = {c.lower(): c for c in cols}  # 不区分大小写映射原名

    # 时间列优先级
    t_col = lc.get("t") or lc.get("ts") or lc.get("time") or lc.get("timestamp") or lc.get("t_ref")
    o_col = lc.get("open") or lc.get("o")
    h_col = lc.get("high") or lc.get("h")
    l_col = lc.get("low") or lc.get("l")
    c_col = lc.get("close") or lc.get("c")
    v_col = lc.get("volume") or lc.get("vol") or lc.get("v")

    missing = [k for k, v in dict(t=t_col, open=o_col, high=h_col, low=l_col, close=c_col, volume=v_col).items() if v is None]
    if missing:
        raise RuntimeError(f"ohlcv 表缺少必要列: {missing}，已检测列={cols}")

    return dict(t=t_col, open=o_col, high=h_col, low=l_col, close=c_col, volume=v_col)


def fetch_ohlcv_df(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    start_ts: str,
    end_ts: str,
) -> "pd.DataFrame":
    """
    从 ohlcv 表读取 [start_ts, end_ts] 区间内的数据（含端点），升序返回。
    start_ts/end_ts 需与DB内时间格式一致（一般 'YYYY-MM-DD HH:MM:SS'）。
    """
    if pd is None:
        raise RuntimeError("需要 pandas 支持，请在环境中安装 pandas。")

    cols = _detect_ohlcv_cols(conn)
    sql = f"""
    SELECT {cols['t']}   AS t,
           {cols['open']}  AS open,
           {cols['high']}  AS high,
           {cols['low']}   AS low,
           {cols['close']} AS close,
           {cols['volume']} AS volume
      FROM ohlcv
     WHERE symbol = ?
       AND timeframe = ?
       AND {cols['t']} >= ?
       AND {cols['t']} <= ?
     ORDER BY {cols['t']} ASC
    """
    df = pd.read_sql_query(sql, conn, params=[symbol, timeframe, start_ts, end_ts])
    # 基础清洗
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["low", "high", "close", "volume"]).reset_index(drop=True)
    return df


# ---------------------------
# 核心：筹码峰直方图
# ---------------------------

def build_volume_profile(
    df: "pd.DataFrame",
    *,
    bin_method: str = "fixed",          # "fixed" | "percentage"
    bin_count: int = 50,
    bin_percentage: float = 0.005,      # 当 method=percentage 时使用（以中位价的百分比做 bin 宽度）
    price_split_count: int = 10,        # 每根K线将成交量线性分摊到多少个等距价格点
    weight_mode: str = "full",          # "full" | "raw" | "log"
) -> Tuple[np.ndarray, np.ndarray]:
    """
    返回 (bin_edges, hist)，hist 为每个价格bin的体积权重。
    - 对每根K线：(low, high, volume) → 生成 price_split_count 个等距价格点，
      然后将本根的“权重后的 volume”平均分摊到这些点，再做加权直方图。
    - full：对全部 volume 做高分位剪裁（winsorize），抑制极端值；
      raw：直接使用原始 volume；
      log：使用 log1p(volume)。
    """
    if df is None or df.empty:
        return np.array([0.0, 1.0]), np.array([0.0])

    lows = df["low"].to_numpy(dtype=float, copy=False)
    highs = df["high"].to_numpy(dtype=float, copy=False)
    vols = df["volume"].to_numpy(dtype=float, copy=False)

    # 价格区间
    price_min = np.nanmin(lows)
    price_max = np.nanmax(highs)
    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max <= price_min:
        # 退化情况，给一个最小区间
        eps = 1e-8
        return np.array([price_min, price_min + eps]), np.array([0.0])

    # 权重转换
    vols_clean = np.nan_to_num(vols, nan=0.0, posinf=0.0, neginf=0.0)
    if weight_mode == "raw":
        w_vec = vols_clean
    elif weight_mode == "log":
        w_vec = np.log1p(np.maximum(vols_clean, 0.0))
    else:
        # full：winsorize，默认按 0.99 分位剪裁
        w_vec = _winsorize(np.maximum(vols_clean, 0.0), p=0.99)

    # 分箱设置
    if bin_method == "percentage":
        mid_price = float(np.nanmedian(df["close"].to_numpy(dtype=float, copy=False)))
        mid_price = max(mid_price, 1e-12)
        bin_width = max(bin_percentage * mid_price, 1e-12)
        est_bins = int(math.ceil((price_max - price_min) / bin_width))
        bin_count_eff = int(min(max(est_bins, 10), 400))  # 合理边界
    else:
        bin_count_eff = int(min(max(bin_count, 10), 400))

    bin_edges = np.linspace(price_min, price_max, num=bin_count_eff + 1, dtype=float)

    # 构造所有价格点与权重
    ps = int(min(max(price_split_count, 2), 200))
    prices_all: List[float] = []
    weights_all: List[float] = []

    # 遍历每根K线，线性摊分成交量到 ps 个点
    for lo, hi, w in zip(lows, highs, w_vec):
        if not np.isfinite(lo) or not np.isfinite(hi) or not np.isfinite(w):
            continue
        if hi <= lo or w <= 0.0:
            continue
        pts = np.linspace(lo, hi, num=ps, dtype=float)
        wp = float(w) / ps
        prices_all.append(pts)  # 延后拼接为一维
        weights_all.append(np.full(ps, wp, dtype=float))

    if not prices_all:
        return bin_edges, np.zeros(bin_count_eff, dtype=float)

    prices = np.concatenate(prices_all, axis=0)
    weights = np.concatenate(weights_all, axis=0)

    hist, _ = np.histogram(prices, bins=bin_edges, weights=weights)
    hist = hist.astype(float, copy=False)
    return bin_edges, hist


def compute_vp_triple(
    bin_edges: np.ndarray,
    hist: np.ndarray,
    *,
    value_area_pct: float = 0.70,
) -> Tuple[float, float, float]:
    """
    计算三价带 (VPOC, VAL, VAH)：
    - VPOC：最大柱对应的 bin 中心价
    - VAL/VAH：以 VPOC 为中心向两侧“连续扩张”，直至累计覆盖 value_area_pct 的体积
    """
    if hist.size == 0 or np.all(hist <= 0.0):
        # 退化：返回零
        return 0.0, 0.0, 0.0

    idx_vpoc = int(np.argmax(hist))
    # bin 中心
    vpoc = 0.5 * (bin_edges[idx_vpoc] + bin_edges[idx_vpoc + 1])

    total = float(hist.sum())
    target = float(min(max(value_area_pct, 0.50), 0.99)) * total

    left = right = idx_vpoc
    covered = float(hist[idx_vpoc])

    # 按“与 VPOC 相邻的更大柱优先”的规则向两侧扩张，保持连续性
    while covered < target and (left > 0 or right < hist.size - 1):
        left_w = hist[left - 1] if left > 0 else -1.0
        right_w = hist[right + 1] if right < hist.size - 1 else -1.0

        if right_w > left_w:
            right += 1
            covered += float(hist[right])
        else:
            left -= 1 if left > 0 else 0
            if left >= 0:
                covered += float(hist[left])

        # 防御：极端情况下跳出
        if left == 0 and right == hist.size - 1:
            break

    val = float(bin_edges[left])
    vah = float(bin_edges[right + 1])
    return vpoc, val, vah


# ---------------------------
# 对外主函数：按仓位窗口计算三价带
# ---------------------------

def compute_triple_since_entry(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    entry_time: str,
    t_ref: str,
    cfg: Dict,
) -> Dict:
    """
    基于“自入场以来至 t_ref”的 K 线窗口，计算 VPOC/VAL/VAH。
    返回字典包含：ok/vpoc/val/vah/value_area_pct/weight_mode/bin_method/bin_count/bin_percentage/price_split_count/window 等。
    """
    # 读取配置（兼容 cfg 或 cfg['core'] 两种挂载）
    core_cfg = cfg.get("core", cfg)
    fe_cfg = (core_cfg.get("feature_engineering") or {}).get("chip_peak", {})  # 无则用默认
    value_area_cfg = fe_cfg.get("value_area", {}) if isinstance(fe_cfg.get("value_area", {}), dict) else {}

    bin_method = str(fe_cfg.get("bin_method", "fixed")).lower()
    bin_count = int(fe_cfg.get("bin_count", 50))
    bin_percentage = float(fe_cfg.get("bin_percentage", 0.005))
    price_split_count = int(fe_cfg.get("price_split_count", 10))
    value_area_pct = float(value_area_cfg.get("pct", 0.70))
    weight_mode = str(value_area_cfg.get("weight_mode", "full")).lower()

    # 拉数据
    df = fetch_ohlcv_df(conn, symbol, timeframe, entry_time, t_ref)
    bars = int(df.shape[0]) if df is not None else 0

    if df is None or df.empty or bars < 2:
        return {
            "ok": False,
            "reason": "no_ohlcv_window",
            "symbol": symbol,
            "timeframe": timeframe,
            "window": {"start": entry_time, "end": t_ref, "bars": bars},
        }

    # 计算筹码峰直方图
    bin_edges, hist = build_volume_profile(
        df,
        bin_method=bin_method,
        bin_count=bin_count,
        bin_percentage=bin_percentage,
        price_split_count=price_split_count,
        weight_mode=weight_mode,
    )

    if hist.size == 0 or np.all(hist <= 0.0):
        return {
            "ok": False,
            "reason": "empty_hist",
            "symbol": symbol,
            "timeframe": timeframe,
            "window": {"start": entry_time, "end": t_ref, "bars": bars},
        }

    # 计算 VPOC/VAL/VAH
    vpoc, val, vah = compute_vp_triple(bin_edges, hist, value_area_pct=value_area_pct)

    # 输出
    out = {
        "ok": True,
        "symbol": symbol,
        "timeframe": timeframe,
        "vpoc": float(vpoc),
        "val": float(val),
        "vah": float(vah),
        "value_area_pct": float(value_area_pct),
        "weight_mode": weight_mode,
        "bin_method": bin_method,
        "bin_count": int(bin_count),
        "bin_percentage": float(bin_percentage),
        "price_split_count": int(price_split_count),
        "window": {
            "start": str(df["t"].iloc[0]),
            "end": str(df["t"].iloc[-1]),
            "bars": bars,
        },
    }
    return out


# ---------------------------
# 便捷方法：直接以最新窗口（最近 N 根）计算三价带
# ---------------------------

def compute_triple_recent(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    cfg: Dict,
    lookback_bars: int = 120,
) -> Dict:
    """
    备用口径：取最近 N 根（默认120）计算三价带。
    """
    if pd is None:
        raise RuntimeError("需要 pandas 支持，请在环境中安装 pandas。")

    # 找到 t_ref 往前的起点
    # 这里简单采用“取 <= t_ref 的最后 N 根”，避免依赖窗口函数
    core_cfg = cfg.get("core", cfg)
    cols = _detect_ohlcv_cols(conn)
    sql = f"""
    SELECT {cols['t']}   AS t,
           {cols['open']}  AS open,
           {cols['high']}  AS high,
           {cols['low']}   AS low,
           {cols['close']} AS close,
           {cols['volume']} AS volume
      FROM ohlcv
     WHERE symbol = ?
       AND timeframe = ?
       AND {cols['t']} <= ?
     ORDER BY {cols['t']} DESC
     LIMIT ?
    """
    df = pd.read_sql_query(sql, conn, params=[symbol, timeframe, t_ref, int(lookback_bars)])
    if df.empty:
        return {
            "ok": False,
            "reason": "no_recent",
            "symbol": symbol,
            "timeframe": timeframe,
            "window": {"start": None, "end": t_ref, "bars": 0},
        }
    df = df.iloc[::-1].reset_index(drop=True)  # 升序

    # 同样的直方图与三价带
    bin_method = str(core_cfg.get("feature_engineering", {}).get("chip_peak", {}).get("bin_method", "fixed")).lower()
    bin_count = int(core_cfg.get("feature_engineering", {}).get("chip_peak", {}).get("bin_count", 50))
    bin_percentage = float(core_cfg.get("feature_engineering", {}).get("chip_peak", {}).get("bin_percentage", 0.005))
    price_split_count = int(core_cfg.get("feature_engineering", {}).get("chip_peak", {}).get("price_split_count", 10))
    value_area_pct = float(core_cfg.get("feature_engineering", {}).get("chip_peak", {}).get("value_area", {}).get("pct", 0.70))
    weight_mode = str(core_cfg.get("feature_engineering", {}).get("chip_peak", {}).get("value_area", {}).get("weight_mode", "full")).lower()

    bin_edges, hist = build_volume_profile(
        df,
        bin_method=bin_method,
        bin_count=bin_count,
        bin_percentage=bin_percentage,
        price_split_count=price_split_count,
        weight_mode=weight_mode,
    )

    if hist.size == 0 or np.all(hist <= 0.0):
        return {
            "ok": False,
            "reason": "empty_hist",
            "symbol": symbol,
            "timeframe": timeframe,
            "window": {"start": str(df["t"].iloc[0]), "end": str(df["t"].iloc[-1]), "bars": int(df.shape[0])},
        }

    vpoc, val, vah = compute_vp_triple(bin_edges, hist, value_area_pct=value_area_pct)
    return {
        "ok": True,
        "symbol": symbol,
        "timeframe": timeframe,
        "vpoc": float(vpoc),
        "val": float(val),
        "vah": float(vah),
        "value_area_pct": float(value_area_pct),
        "weight_mode": weight_mode,
        "bin_method": bin_method,
        "bin_count": int(bin_count),
        "bin_percentage": float(bin_percentage),
        "price_split_count": int(price_split_count),
        "window": {
            "start": str(df["t"].iloc[0]),
            "end": str(df["t"].iloc[-1]),
            "bars": int(df.shape[0]),
        },
    }
