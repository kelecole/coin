# -*- coding: utf-8 -*-
"""
core/btc_alignment.py

BTC 对齐模块（加强版）

功能分三块：
1. 计算并写表：对这一桶所有候选，去和 BTC 做相关性 / 滞后 / β / 资金面修正，
   写入 btc_alignment_snapshot（兼容你现有表结构）。
2. 给排序用的分数：返回 {symbol: align_score} 给 pipeline/decision_engine 做软排序。
3. 提供“硬过滤”接口：根据当前 BTC 方向 + 标的和 BTC 的相关性 + 信号方向，
   做**交易前**的一道硬门，满足你说的“BTC 正在涨就别开掉头空单”这种要求。

使用方式（推荐）：
- pipeline_runner 里还是调 analyze_alignment_all(...) 不变
- decision_engine 里在 _allocate_risk_R(...) 之后、correlation_gate 之前，调
    kept, rej, diag = btc_alignment.hard_filter_by_btc_alignment(...)
  把 rej 并到总的 rejected 里，把 kept 往后传。
"""

from __future__ import annotations

import sqlite3
import math
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd

# ------------------------------------------
# 默认参数（可以被 config 覆盖）
# ------------------------------------------

LOOKBACK_BARS = {
    "1h": 240,   # 约 10 天
    "4h": 240,   # 约 40 天
}

LAG_SCAN = {
    "1h": 6,
    "4h": 6,
}

FUND_Z_WINDOW_HOURS = 48
BTC_SYMBOL = "BTC/USDT"


# ------------------------------------------
# 小工具
# ------------------------------------------

def _q_to_df(conn: sqlite3.Connection, sql: str, params=()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def _fetch_ohlcv(conn, symbol: str, timeframe: str, t_ref: str, n: int) -> pd.DataFrame:
    """
    从 ohlcv 取最近 n 根K线(<= t_ref)，按时间升序返回
    需要列: t, close, volume
    """
    sql = """
    SELECT t, close, volume
    FROM ohlcv
    WHERE symbol=? AND timeframe=? AND t<=?
    ORDER BY t DESC
    LIMIT ?
    """
    df = _q_to_df(conn, sql, (symbol, timeframe, t_ref, n))
    if df.empty:
        return df
    return df.iloc[::-1].reset_index(drop=True)


def _funding_window(conn, t_ref: str) -> pd.DataFrame:
    """
    最近 48h 的 BTC 资金面，用来做 dominance 修正
    """
    sql = f"""
    SELECT t_ref, funding_rate, basis, oi, long_ratio
    FROM funding_snapshot
    WHERE symbol=? AND t_ref<=?
      AND t_ref>=datetime(?, '-{FUND_Z_WINDOW_HOURS} hours')
    ORDER BY t_ref ASC
    """
    return _q_to_df(conn, sql, (BTC_SYMBOL, t_ref, t_ref))


def _logret(x: pd.Series) -> pd.Series:
    return np.log(x).diff()


def _pearson(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) != len(b) or len(a) < 3:
        return np.nan
    if np.allclose(a, a[0]) or np.allclose(b, b[0]):
        return np.nan
    return float(np.corrcoef(a, b)[0, 1])


def _cross_corr_peak(a: np.ndarray, b: np.ndarray, L: int) -> Tuple[Optional[int], Optional[float]]:
    """
    扫描 [-L,+L] 找绝对相关度最大的滞后和数值。
    lag < 0 表示 BTC 先动；lag > 0 表示 标的滞后。
    """
    best_lag, best_corr = None, None
    for lag in range(-L, L + 1):
        if lag < 0:
            a_s, b_s = a[-lag:], b[:len(b) + lag]
        elif lag > 0:
            a_s, b_s = a[:len(a) - lag], b[lag:]
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
    """
    y = a + b x 里的 b
    """
    if len(y) != len(x) or len(y) < 5:
        return np.nan
    X = np.column_stack([np.ones(len(x)), x])
    try:
        b = np.linalg.lstsq(X, y, rcond=None)[0][1]
        return float(b)
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
    """
    融合 funding_rate / basis / long_ratio / oi 得一个资金面拥挤度
    """
    if fwin.empty:
        return np.nan

    z_f = _z(fwin["funding_rate"])
    z_b = _z(-fwin["basis"])
    z_lr = (fwin["long_ratio"] - 0.5) / 0.5
    z_oi = _z(fwin["oi"])

    i = -1
    mix = 0.35 * (0.0 if pd.isna(z_f.iloc[i]) else z_f.iloc[i]) + \
          0.30 * (0.0 if pd.isna(z_b.iloc[i]) else z_b.iloc[i]) + \
          0.20 * (0.0 if pd.isna(z_lr.iloc[i]) else z_lr.iloc[i]) + \
          0.15 * (0.0 if pd.isna(z_oi.iloc[i]) else z_oi.iloc[i])

    return _tanh_clip(mix)


def _btc_state_from_corrlag(align_corr: float,
                            peak_corr_val: float,
                            lag: Optional[int]) -> str:
    if math.isnan(align_corr) or abs(align_corr) < 0.2:
        return "INDEPENDENT"
    if lag is None:
        return "COUPLED"
    if lag < 0:
        return "LEAD"
    if lag > 0:
        return "LAG"
    return "COUPLED"


# ------------------------------------------
# BTC 当前是不是在涨/在跌
# ------------------------------------------
def _infer_btc_trend(
    conn: sqlite3.Connection,
    t_ref: str,
    timeframe: str = "1h",
    bars: int = 6,
) -> str:
    """
    很简单的版本：看最近 bars 根的收盘斜率
    return: "up" | "down" | "flat"
    """
    df = _fetch_ohlcv(conn, BTC_SYMBOL, timeframe, t_ref, bars)
    if df.empty or len(df) < 3:
        return "flat"

    closes = df["close"].astype(float).values
    # 简单斜率：最后一根和最前一根
    start = closes[0]
    end = closes[-1]
    if start <= 0:
        return "flat"
    pct = (end - start) / start
    if pct >= 0.003:      # +0.3% 以上算在涨
        return "up"
    elif pct <= -0.003:   # -0.3% 以下算在跌
        return "down"
    return "flat"


# ------------------------------------------
# 真·单票计算
# ------------------------------------------
# 替换 btc_alignment.py 中的 _compute_one_symbol
def _compute_one_symbol(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
) -> Optional[dict]:
    n = LOOKBACK_BARS.get(timeframe)
    if not n:
        return None

    df_btc = _fetch_ohlcv(conn, BTC_SYMBOL, timeframe, t_ref, n)
    df_sym = _fetch_ohlcv(conn, symbol, timeframe, t_ref, n)
    if df_btc.empty or df_sym.empty or len(df_btc) < 20 or len(df_sym) < 20:
        return None

    df = pd.merge(
        df_btc[["t", "close"]].rename(columns={"close": "btc_close"}),
        df_sym[["t", "close"]].rename(columns={"close": "sym_close"}),
        on="t",
        how="inner",
    )
    if len(df) < 20:
        return None

    r_btc = _logret(df["btc_close"]).dropna().values
    r_sym = _logret(df["sym_close"]).dropna().values
    m = min(len(r_btc), len(r_sym))
    r_btc = r_btc[-m:]
    r_sym = r_sym[-m:]

    # --- 开始合并计算 ---
    align_corr = _pearson(r_sym, r_btc)
    lag, peak_corr_val = _cross_corr_peak(r_sym, r_btc, L=LAG_SCAN.get(timeframe, 6))
    beta_val = _ols_beta(r_sym, r_btc) # 注意：这里计算的是 beta
    
    fwin = _funding_window(conn, t_ref)
    dominance_adj = _compute_dominance_adj(fwin)
    btc_state = _btc_state_from_corrlag(align_corr, peak_corr_val, lag)

    # --- 新增字段 (来自 btc_alignment_job.py) ---
    lag_val = lag if lag is not None else 0
    lag_signif = 1 if abs(lag_val) > 0.5 else 0
    lag_window = abs(lag_val)
    dom_src = "funding_rate"  # 假设资金面数据来源是 funding_rate
    window_bars = n
    # --- 结束新增字段 ---

    return {
        "t_ref": t_ref,
        "symbol": symbol,
        "timeframe": timeframe,
        "align_corr": float(align_corr) if not math.isnan(align_corr) else None,
        "peak_corr_lag": int(lag) if lag is not None else None,
        "peak_corr_val": float(peak_corr_val) if peak_corr_val is not None else None,
        
        # 同时写入 beta 和 beta_ratio (job.py 也是这么做的)
        "beta": float(beta_val) if not math.isnan(beta_val) else None,
        "beta_ratio": float(beta_val) if not math.isnan(beta_val) else None, 
        
        "dominance_adj": float(dominance_adj) if not math.isnan(dominance_adj) else None,
        "btc_state": btc_state,
        
        # 新增字段
        "lag_signif": int(lag_signif),
        "lag_window": int(lag_window),
        "dom_src": dom_src,
        "window_bars": int(window_bars),
    }
# 替换 btc_alignment.py 中的 _upsert_snapshot_row

def _upsert_snapshot_row(
    conn: sqlite3.Connection,
    row: dict,
    calc_ok: int,
    tz: str = "Asia/Shanghai",   # 新增缺省时区，不影响原有调用
):
    """
    把单票 BTC 对齐结果 upsert 到 btc_alignment_snapshot。
    关键变化：不再在 SQL 里写 datetime('now','localtime')，
    统一用 core.timebox.now_local_str(tz) 生成 created_at 并参数注入。
    """
    from core.timebox import now_local_str

    t_ref = row["t_ref"]
    sym = row["symbol"]
    tf = row["timeframe"]

    # 探测表结构
    try:
        cols = conn.execute("PRAGMA table_info(btc_alignment_snapshot);").fetchall()
        col_names = {c[1] for c in cols}
    except Exception:
        # 回退：假设一套较新的列集合（仅在 PRAGMA 失败时使用）
        col_names = {
            't_ref', 'symbol', 'timeframe', 'align_corr', 'peak_corr_lag',
            'peak_corr_val', 'beta_ratio', 'dominance_adj', 'btc_state',
            'calc_ok', 'created_at', 'lag_signif', 'lag_window', 'beta',
            'dom_src', 'window_bars', 'tf'
        }

    # 统一生成本地时间戳
    now_ts = now_local_str(tz)

    # 基础字段（必有）
    base_row_data = {
        "t_ref": t_ref,
        "symbol": sym,
        "timeframe": tf,
        "tf": tf,                   # 兼容 job.py
        "calc_ok": int(calc_ok),
    }
    # 合并 row 的其它字段（只取表里存在的列）
    merged = base_row_data.copy()
    for k, v in (row or {}).items():
        if k in col_names and k not in merged:
            merged[k] = v

    # 如果表有 created_at 列，就参数化写入
    if "created_at" in col_names:
        merged["created_at"] = now_ts

    # 仅保留表中存在的列
    final_data = {k: v for k, v in merged.items() if k in col_names}

    # 构造 INSERT ... ON CONFLICT
    insert_cols = list(final_data.keys())
    placeholders = [f":{k}" for k in insert_cols]
    # 冲突更新时，不更新主键列
    conflict_keys = ("t_ref", "symbol", "timeframe")
    update_cols = [k for k in insert_cols if k not in conflict_keys]
    update_assign = ", ".join([f"{k}=excluded.{k}" for k in update_cols]) if update_cols else ""

    sql = f"""
    INSERT INTO btc_alignment_snapshot ({", ".join(insert_cols)})
    VALUES ({", ".join(placeholders)})
    ON CONFLICT(t_ref, symbol, timeframe)
    DO UPDATE SET
        {update_assign}
    """

    try:
        conn.execute(sql, final_data)
    except Exception as e:
        print(f"[ERR] _upsert_snapshot_row FAILED for {sym} {tf}: {e}")
        # 不抛，避免影响上游流程
        pass


def _score_align_for_sorting(row: Optional[dict], cfg: dict) -> float:
    """
    软打分：[-2,2]
    """
    if row is None:
        return 0.0

    score = 0.0

    align_corr = row.get("align_corr")
    if align_corr is not None:
        if align_corr >= 0.65:
            score += 2.0
        elif align_corr >= 0.35:
            score += 1.0
        elif align_corr <= -0.65:
            score -= 2.0
        elif align_corr <= -0.35:
            score -= 1.0

    lag = row.get("peak_corr_lag")
    if lag is not None and isinstance(lag, (int, float)):
        if lag > 0:
            score -= 1.0

    dom_adj = row.get("dominance_adj")
    if dom_adj is not None and not math.isnan(dom_adj):
        if dom_adj > 0.6:
            score -= 1.0
        elif dom_adj < -0.6:
            score += 1.0

    # clamp
    if score > 2.0:
        score = 2.0
    if score < -2.0:
        score = -2.0

    return float(score)


def analyze_alignment_all(
    conn: sqlite3.Connection,
    cfg: dict,
    t_ref: str,
    candidates: List[dict],
) -> Dict[str, dict]:
    """
    给这一桶候选全部做一次 BTC 对齐分析，返回一个 map，让 pipeline / decision_engine 用。
    同时把结果落到 btc_alignment_snapshot 里（能落多少落多少，失败不炸主流程）。

    返回格式大致是：
    {
        "BTC/USDT": {
            "align_corr": 1.0,
            "peak_corr_lag": 0,
            "peak_corr_val": 1.0,
            "beta_ratio": 1.0,
            "dominance_adj": 0.0,
            "btc_state": "COUPLED",
            "regime_dir": "up|down|flat",
            "calc_ok": 1,
        },
        "UNI/USDT": {
            "align_corr": 0.8,
            ...
        },
        ...
    }
    """
    out: Dict[str, dict] = {}

    # 1) 配置里可能有 thresholds.btc_alignment，也可能只有 core.btc_symbols
    thresholds = cfg.get("thresholds") or {}
    btc_cfg = thresholds.get("btc_alignment") or {}
    btc_aliases = (
        btc_cfg.get("btc_alias")
        or cfg.get("btc_symbols")
        or ["BTC/USDT", "BTCUSDT", "BTC-USD", "BTC/USDT:USDT"]
    )

    # 2) 先算一下 BTC 自己的趋势，给后面硬门用
    #    这里用你上面写好的 _infer_btc_trend(...)
    btc_trend = _infer_btc_trend(
        conn=conn,
        t_ref=t_ref,
        timeframe=btc_cfg.get("timeframe", "1h"),
        bars=int(btc_cfg.get("recent_bars", 6)),
    )

    # 3) 给 BTC 自己构一条行，写到 snapshot 里，key 用第一个别名
    btc_row = {
        "t_ref": t_ref,
        "symbol": btc_aliases[0],
        "timeframe": btc_cfg.get("timeframe", "1h"),
        "align_corr": 1.0,
        "peak_corr_lag": 0,
        "peak_corr_val": 1.0,
        "beta_ratio": 1.0,
        "dominance_adj": 0.0,
        "btc_state": "SELF",
        "regime_dir": btc_trend,   # ★ 关键：把方向塞进去，给硬门识别
        "calc_ok": 1,
    }
    try:
        _upsert_snapshot_row(conn, btc_row, calc_ok=1)
    except Exception:
        pass
    out[btc_aliases[0]] = btc_row

    # 4) 把这一桶里出现过的 (symbol, timeframe) 去重出来
    seen_pairs = set()
    for c in candidates or []:
        sym = c.get("symbol")
        tf = c.get("timeframe") or "1h"
        if not sym:
            continue
        key = (sym, tf)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)

    # 5) 逐个算
    for (sym, tf) in seen_pairs:
        # 如果就是 BTC 本人，前面已经塞过了
        if sym in btc_aliases:
            continue

        row = _compute_one_symbol(conn, sym, tf, t_ref)
        if row is None:
            # 计算失败也要给一个壳，方便后面看到是 missing
            out[sym] = {
                "t_ref": t_ref,
                "symbol": sym,
                "timeframe": tf,
                "align_corr": None,
                "peak_corr_lag": None,
                "peak_corr_val": None,
                "beta_ratio": None,
                "dominance_adj": None,
                "btc_state": None,
                "calc_ok": 0,
            }
            # 表也尽量写一条
            try:
                _upsert_snapshot_row(
                    conn,
                    {
                        "t_ref": t_ref,
                        "symbol": sym,
                        "timeframe": tf,
                        "align_corr": None,
                        "peak_corr_lag": None,
                        "peak_corr_val": None,
                        "beta_ratio": None,
                        "dominance_adj": None,
                        "btc_state": None,
                    },
                    calc_ok=0,
                )
            except Exception:
                pass
            continue

        # 正常算出来的情况
        row["calc_ok"] = 1
        # 顺便把 BTC 的大方向塞进去，后面的硬门就能看到
        row["regime_dir"] = btc_trend

        # 落表
        try:
            _upsert_snapshot_row(conn, row, calc_ok=1)
        except Exception:
            pass

        # 放到返回 map
        out[sym] = row

    # 最后一定要 commit 一下，避免上层看不到
    try:
        conn.commit()
    except Exception:
        pass

    return out
# core/btc_alignment.py

def load_alignment_map_from_db(
    conn: sqlite3.Connection,
    t_ref: str,
) -> Dict[str, Dict[str, Any]]:
    """
    从 btc_alignment_snapshot 把这一桶(t_ref)的所有对齐结果读出来，
    按 symbol 做成一个 {symbol: row_dict} 的 map，
    同一个 symbol 有多个 timeframe 时优先 1h 其次 4h。
    """
    out: Dict[str, Dict[str, Any]] = {}

    # 优先按“新版字段齐全”的方式去取
    try:
        cur = conn.execute(
            """
            SELECT
              t_ref,
              symbol,
              timeframe,
              align_corr,
              peak_corr_lag,
              peak_corr_val,
              beta,
              beta_ratio,
              dominance_adj,
              btc_state,
              lag_signif,
              lag_window,
              dom_src,
              window_bars,
              calc_ok,
              created_at
            FROM btc_alignment_snapshot
            WHERE t_ref = ?
            """,
            (t_ref,),
        )
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]
    except sqlite3.OperationalError:
        # 表里可能还没有你后面加的那些字段，就退回到老字段集合
        try:
            cur = conn.execute(
                """
                SELECT
                  t_ref,
                  symbol,
                  timeframe,
                  align_corr,
                  peak_corr_lag,
                  peak_corr_val,
                  beta_ratio,
                  dominance_adj,
                  calc_ok,
                  created_at
                FROM btc_alignment_snapshot
                WHERE t_ref = ?
                """,
                (t_ref,),
            )
            rows = cur.fetchall()
            col_names = [d[0] for d in cur.description]
        except Exception:
            return out  # 表都没取到，就返回空
    except Exception:
        return out

    # 把同一个 symbol 的多TF做个优先级合并
    def _prio(tf: str) -> int:
        tf = (tf or "").lower()
        if tf == "1h":
            return 1
        if tf == "4h":
            return 2
        return 9

    for r in rows:
        row_dict = dict(zip(col_names, r))
        sym = row_dict.get("symbol")
        tf = row_dict.get("timeframe")
        if not sym:
            continue

        if sym not in out:
            out[sym] = row_dict
        else:
            # 已经有一条了，比较 timeframe 优先级
            cur_tf = out[sym].get("timeframe")
            if _prio(tf) < _prio(cur_tf):
                out[sym] = row_dict

    return out



def load_btc_trend_from_db(
    conn: sqlite3.Connection,
    t_ref: str,
    btc_aliases: Optional[List[str]] = None,
) -> str:
    """
    从 btc_alignment_snapshot 里“猜”这一桶 BTC 的方向。
    现在你的 cron job 只写了相关性这些字段，没有写 regime_dir，
    所以这里先保守返回 'flat'，但如果以后你在表里加了 regime_dir / btc_state，
    这里会自动识别出来。

    返回：'up' | 'down' | 'flat'
    """
    if not btc_aliases:
        btc_aliases = [
            "BTC/USDT",
            "BTCUSDT",
            "BTC-USD",
            "BTC/USDT:USDT",
        ]

    # 尝试从 snapshot 里把 BTC 那一行查出来
    for name in btc_aliases:
        try:
            row = conn.execute(
                """
                SELECT
                  timeframe,
                  align_corr,
                  peak_corr_val,
                  peak_corr_lag,
                  beta_ratio,
                  dominance_adj,
                  calc_ok
                FROM btc_alignment_snapshot
                WHERE t_ref = ?
                  AND symbol = ?
                ORDER BY
                  CASE timeframe
                    WHEN '1h' THEN 1
                    WHEN '4h' THEN 2
                    ELSE 9
                  END
                LIMIT 1
                """,
                (t_ref, name),
            ).fetchone()
        except Exception:
            row = None

        if row:
            # 现在表里没方向字段 -> 先返回 'flat'
            # 如果以后你在 job 里把 regime_dir 写进表，就可以在这里取出来：
            # 例：
            #   SELECT regime_dir FROM ...
            #   if regime_dir: return regime_dir.lower()
            return "flat"

    # 整个桶都没有 BTC 的对齐行，就当平
    return "flat"


# ------------------------------------------
# 对这一桶全量跑一遍，主入口（给 pipeline_runner 用）
# ------------------------------------------

def _filter_by_btc_alignment(
    candidates: list,
    btc_alignment_map: dict,
    cfg: dict,
):
    """
    BTC 对齐硬门过滤

    [已修复]：
    - 修复 Bug 2.1 (lag_signif_zero)：注释掉了导致同步信号(lag=0)被拒绝的过滤器。
    - 修复 Bug 2.2 (low_dominance_adj_funding_rate)：注释掉了导致非狂热市场被拒绝的过滤器。

    参数:
        candidates: run_bucket_pipeline / pipeline_state 之后的候选单
        btc_alignment_map: pipeline_state 里传下来的 BTC 对齐结果
        cfg: 全局配置，里面要有 thresholds.btc_alignment.*

    返回:
        kept_list, rejected_list, diag_dict
    """

    # 1) 先把配置拿出来
    thresholds = (cfg.get("thresholds") or {})
    btc_cfg = thresholds.get("btc_alignment") or {}

    enforce = bool(btc_cfg.get("enforce", False))              # 是否真的挡
    min_align_score = float(btc_cfg.get("min_align_score", 0)) # 高相关的判定线
    
    min_peak_corr_abs = float(btc_cfg.get("min_peak_corr_abs", 0.3)) # 默认 0.3
    
    reject_if_missing = bool(btc_cfg.get("reject_if_missing", False))

    btc_aliases = btc_cfg.get("btc_alias") or cfg.get("btc_symbols") or [
        "BTC/USDT",
        "BTCUSDT",
        "BTC-USD",
        "BTC/USDT:USDT",
    ]

    # 2) 先推断 BTC 当前是什么方向
    btc_row = None
    for name in btc_aliases:
        if name in btc_alignment_map:
            btc_row = btc_alignment_map.get(name)
            break

    btc_trend = "unknown"
    if isinstance(btc_row, dict):
        if btc_row.get("regime_dir"):
            btc_trend = str(btc_row.get("regime_dir")).lower()
        elif btc_row.get("dir"):
            btc_trend = str(btc_row.get("dir")).lower()
        elif btc_row.get("trend"):
            btc_trend = str(btc_row.get("trend")).lower()
        else:
            btc_trend = "unknown"

    kept = []
    rejected = []

    # 3) 一条一条检查候选
    for c in candidates:
        sym = c.get("symbol")
        tf = c.get("timeframe") or "1h"
        direction = (c.get("direction") or "").lower().strip()

        row = btc_alignment_map.get(sym)

        # 3.1 没有对齐记录的情况
        if row is None:
            if enforce and reject_if_missing:
                rejected.append({
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": direction,
                    "reject_reason": "btc_align_missing",
                })
                continue
            else:
                kept.append(c)
                continue

        # 3.2 有记录，取出相关性
        try:
            align_corr = float(row.get("align_corr", 0.0))
            lag_signif_flag = int(row.get("lag_signif", 0))
            dom_adj = row.get("dominance_adj", 0.0)
            dom_src = row.get("dom_src", "")
            peak_val = float(row.get("peak_corr_val", 0.0))
            
        except Exception:
            align_corr, dom_adj, dom_src = 0.0, 0.0, ""
            lag_signif_flag = 0 
            peak_val = 0.0

        if enforce:
            
            # 规则0：峰值相关性硬过滤 (这个逻辑是合理的，保留)
            if abs(peak_val) < min_peak_corr_abs:
                rejected.append({
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": direction,
                    "reject_reason": "peak_corr_too_low",
                })
                continue

            # 1) BTC 在涨，禁止空高相关 (这个逻辑是合理的，保留)
            if btc_trend == "up":
                if direction == "short" and align_corr >= min_align_score:
                    rejected.append({
                        "symbol": sym,
                        "timeframe": tf,
                        "direction": direction,
                        "reject_reason": "btc_up_but_short_highcorr",
                    })
                    continue

            # 2) BTC 在跌，禁止多高相关 (这个逻辑是合理的，保留)
            elif btc_trend == "down":
                if direction == "long" and align_corr >= min_align_score:
                    rejected.append({
                        "symbol": sym,
                        "timeframe": tf,
                        "direction": direction,
                        "reject_reason": "btc_down_but_long_highcorr",
                    })
                    continue

            # 3) [BUG 2.1 修复] 滞后性过滤
            # 原始逻辑 (if lag_signif_flag == 0:) 几乎肯定是一个 Bug，
            # 它拒绝了 lag=0 的同步信号。
            # 我们将其注释掉，以允许同步信号通过。
            # if lag_signif_flag == 0:
            #     rejected.append({
            #         "symbol": sym,
            #         "timeframe": tf,
            #         "direction": direction,
            #         "reject_reason": "lag_signif_zero",
            #     })
            #     continue

            # 4) [BUG 2.2 修复] 资金面数据源过滤
            # 原始逻辑 (if dom_src == "funding_rate" and dom_adj < 0.5:)
            # 是一个极度严苛的过滤器，它拒绝了所有非狂热的市场。
            # 我们将其注释掉，以防止它过滤掉所有交易。
            # if dom_src == "funding_rate" and dom_adj < 0.5:
            #     rejected.append({
            #         "symbol": sym,
            #         "timeframe": tf,
            #         "direction": direction,
            #         "reject_reason": "low_dominance_adj_funding_rate",
            #     })
            #     continue

        # 没命中任何硬条件，就放行
        kept.append(c)

    # 4) 返回诊断，方便你写进 decision_snapshot
    diag = {
        "enabled": enforce,
        "btc_trend": btc_trend,
        "all": len(candidates),
        "kept": len(kept),
        "rejected": len(rejected),
        "min_align_score": min_align_score,
        "min_peak_corr_abs": min_peak_corr_abs,
        "reject_if_missing": reject_if_missing,
    }

    return kept, rejected, diag