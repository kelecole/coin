#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
btc_alignment_job.py
按当前时间(或手动给的 t_ref)批量计算 BTC 对齐，写入 trading_signals_core.db 里的
btc_alignment_snapshot，供 run_open_cycle.py 直接查。

用法示例：
    # 1) 按当前整点跑（+08:00）
    python app/csmvp/btc_alignment_job.py \
        --config /www/wwwroot/Crypto-Signal/config.yml

    # 2) 指定时间跑
    python app/csmvp/btc_alignment_job.py \
        --config /www/wwwroot/Crypto-Signal/config.yml \
        --tref "2025-10-31 14:00:00"
"""
from typing import Dict, Any, List, Optional   
import argparse
# [修改] 移除 datetime
import sqlite3
import os
import yaml
import pandas as pd
import numpy as np

# [修改] 导入 timebox
from core.timebox import now_local_str, now_local_dt

BTC_SYMBOL = "BTC/USDT"

# 跟你老脚本一致的窗口
LOOKBACK_BARS = {
    "1h": 240,
    "4h": 240,
}
LAG_SCAN = {
    "1h": 6,
    "4h": 6,
}
FUND_Z_WINDOW_HOURS = 48
DOM_W = {"funding_rate": 0.35, "basis": 0.30, "long_ratio": 0.20, "oi": 0.15}

def _q(conn, sql: str, params=()):
    return pd.read_sql_query(sql, conn, params=params)

def _fetch_ohlcv(conn, symbol: str, timeframe: str, t_ref: str, n: int) -> pd.DataFrame:
    sql = """
    SELECT t, close, volume
    FROM ohlcv
    WHERE symbol=? AND timeframe=? AND t<=?
    ORDER BY t DESC
    LIMIT ?
    """
    df = _q(conn, sql, (symbol, timeframe, t_ref, n))
    if df.empty:
        return df
    return df.iloc[::-1].reset_index(drop=True)

def _funding_window(conn, t_ref: str) -> pd.DataFrame:
    sql = f"""
    SELECT t_ref, funding_rate, basis, oi, long_ratio
    FROM funding_snapshot
    WHERE symbol=? AND t_ref<=? AND t_ref>=datetime(?, '-{FUND_Z_WINDOW_HOURS} hours')
    ORDER BY t_ref ASC
    """
    return _q(conn, sql, (BTC_SYMBOL, t_ref, t_ref))

def _logret(x: pd.Series) -> pd.Series:
    return np.log(x).diff()

def _pearson(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) != len(b) or len(a) < 3:
        return np.nan
    if np.allclose(a, a[0]) or np.allclose(b, b[0]):
        return np.nan
    return float(np.corrcoef(a, b)[0, 1])

def _cross_corr_peak(a: np.ndarray, b: np.ndarray, L: int):
    best_lag, best_corr = None, None
    for lag in range(-L, L + 1):
        if lag < 0:
            a_s, b_s = a[-lag:], b[: len(b) + lag]
        elif lag > 0:
            a_s, b_s = a[: len(a) - lag], b[lag:]
        else:
            a_s, b_s = a, b
        if len(a_s) < 5:
            continue
        r = _pearson(a_s, b_s)
        if np.isnan(r):
            continue
        if best_corr is None or abs(r) > abs(best_corr):
            best_corr, best_lag = r, lag
    return best_lag, best_corr

def _ols_beta(y: np.ndarray, x: np.ndarray) -> float:
    if len(y) != len(x) or len(y) < 5:
        return np.nan
    X = np.column_stack([np.ones(len(x)), x])
    try:
        return float(np.linalg.lstsq(X, y, rcond=None)[0][1])
    except Exception:
        return np.nan

def _z(s: pd.Series) -> pd.Series:
    if len(s) < 5 or s.std(ddof=0) == 0:
        return pd.Series([np.nan] * len(s), index=s.index)
    return (s - s.mean()) / (s.std(ddof=0) + 1e-12)

def _tanh_clip(x: float) -> float:
    try:
        return float(np.tanh(x))
    except Exception:
        return np.nan

def _compute_dominance_adj(fwin: pd.DataFrame) -> float:
    if fwin.empty:
        return np.nan
    z_f = _z(fwin["funding_rate"])
    z_b = _z(-fwin["basis"])
    z_lr = (fwin["long_ratio"] - 0.5) / 0.5
    z_oi = _z(fwin["oi"])
    i = -1
    mix = (
        DOM_W["funding_rate"] * (0.0 if pd.isna(z_f.iloc[i]) else z_f.iloc[i])
        + DOM_W["basis"] * (0.0 if pd.isna(z_b.iloc[i]) else z_b.iloc[i])
        + DOM_W["long_ratio"] * (0.0 if pd.isna(z_lr.iloc[i]) else z_lr.iloc[i])
        + DOM_W["oi"] * (0.0 if pd.isna(z_oi.iloc[i]) else z_oi.iloc[i])
    )
    return _tanh_clip(mix)

def compute_one(conn, symbol: str, timeframe: str, t_ref: str, cfg: dict): # [修改] 增加 cfg
    n = LOOKBACK_BARS.get(timeframe)
    if not n:
        return None

    df_btc = _fetch_ohlcv(conn, BTC_SYMBOL, timeframe, t_ref, n)
    df_sym = _fetch_ohlcv(conn, symbol, timeframe, t_ref, n)
    if df_btc.empty or df_sym.empty or len(df_btc) < 20 or len(df_sym) < 20:
        return None

    df = df_btc[["t", "close"]].rename(columns={"close": "btc_close"}).merge(
        df_sym[["t", "close"]].rename(columns={"close": "sym_close"}),
        on="t",
        how="inner",
    )
    if len(df) < 20:
        return None

    r_btc = _logret(df["btc_close"]).dropna().values
    r_sym = _logret(df["sym_close"]).dropna().values
    m = min(len(r_btc), len(r_sym))
    r_btc, r_sym = r_btc[-m:], r_sym[-m:]

    align_corr = _pearson(r_sym, r_btc)
    lag, peak_corr = _cross_corr_peak(r_sym, r_btc, L=LAG_SCAN.get(timeframe, 6))
    beta = _ols_beta(r_sym, r_btc)

    fwin = _funding_window(conn, t_ref)
    dominance_adj = _compute_dominance_adj(fwin)

    # 新增字段
    lag_signif = 1 if abs(lag) > 0.5 else 0  # 假设滞后超过 0.5 是显著
    lag_window = abs(lag)  # 滞后窗口是滞后值的绝对值
    dom_src = "funding_rate"  # 假设资金面数据来源是 funding_rate
    window_bars = LOOKBACK_BARS.get(timeframe, 240)  # 根据时间框架设置窗口大小

    # [修改] 定义 now_ts
    now_ts = now_local_str(cfg.get("tz", "Asia/Shanghai"))
    cur = conn.cursor()
    # 插入数据，包含所有字段
    cur.execute(
        """
        INSERT INTO btc_alignment_snapshot
          (t_ref, symbol, timeframe,
           align_corr, peak_corr_lag, peak_corr_val,
           lag_signif, lag_window, beta, beta_ratio, dominance_adj, dom_src, window_bars, tf, calc_ok, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(symbol, timeframe, t_ref) DO UPDATE SET
          align_corr=excluded.align_corr,
          peak_corr_lag=excluded.peak_corr_lag,
          peak_corr_val=excluded.peak_corr_val,
          lag_signif=excluded.lag_signif,
          lag_window=excluded.lag_window,
          beta=excluded.beta,
          beta_ratio=excluded.beta_ratio,
          dominance_adj=excluded.dominance_adj,
          dom_src=excluded.dom_src,
          window_bars=excluded.window_bars,
          tf=excluded.tf,
          calc_ok=1,
          created_at=excluded.created_at
        """, # [修改] 替换 created_at
        (
            t_ref,
            symbol,
            timeframe,
            align_corr,
            lag,
            peak_corr,
            lag_signif,
            lag_window,
            beta,
            beta,  # 此处是 beta_ratio 还是 beta 需要具体确认
            dominance_adj,
            dom_src,
            window_bars,
            timeframe,
            now_ts, # [修改] 传入 now_ts
        ),
    )
    conn.commit()

    print(f"[OK] {symbol} {timeframe} @{t_ref} align={align_corr:.4f} lag={lag} peak={peak_corr:.4f} beta={beta:.4f}")

def _load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _default_tref(cfg: dict) -> str: # [修改] 增加 cfg
    # 取当前小时闭合K（+08:00）
    # [修改] 替换 datetime.now(...)
    now = now_local_dt(cfg.get("tz","Asia/Shanghai"))
    now = now.replace(minute=0, second=0, microsecond=0)
    return now.strftime("%Y-%m-%d %H:%M:%S")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--tref", help="比如 2025-10-31 14:00:00，不传就用当前整点")
    args = ap.parse_args()

    cfg_all = _load_cfg(args.config)
    core_cfg = cfg_all.get("core") or cfg_all.get("csmvp") or {}
    db_path = core_cfg.get("db_path") or "/www/wwwroot/Crypto-Signal/app/trading_signals_core.db"
    symbols: List[str] = core_cfg.get("tests") or core_cfg.get("symbols") or [BTC_SYMBOL]
    timeframes: List[str] = core_cfg.get("timeframes") or ["1h", "4h"]

    # [修改] 传入 cfg
    t_ref = args.tref or _default_tref(core_cfg)

    conn = sqlite3.connect(db_path)
    for s in symbols:
        for tf in timeframes:
            try:
                # [修改] 传入 cfg
                compute_one(conn, s, tf, t_ref, core_cfg)
            except Exception as e:
                print(f"[ERR] {s} {tf} @{t_ref}: {e}")
    conn.close()

if __name__ == "__main__":
    main()