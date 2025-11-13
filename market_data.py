# -*- coding: utf-8 -*-
"""
core/market_data.py

从旧的 run_bucket_pipeline.py 抽取而来：
- compute_market_ctx()  ← 原 _compute_market_ctx
- fetch_last_price()    ← 原 _fetch_last_price
- _get_exchange()       ← 原内部帮助函数

保持行为不变：严格只看 t_ref 及以前的闭合K，实时价优先、DB价回退。
"""

import sqlite3
from typing import Optional, Dict, Any, List
import math
import logging
import pandas as pd  # <--- 确保这一行存在


#
# --- 将这个新函数完整粘贴到 market_data.py ---
#

def fetch_ohlcv_series(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    lookback_bars: int
) -> pd.DataFrame:
    """
    [新] 从 'ohlcv' 表获取K线序列 (按时间升序)。
    
    这是 liquidity_filter.py 依赖的函数，用于获取
    成交量自比较所需的数据。
    """
    try:
        # SQL 查询：
        # 1. 从正确的 'ohlcv' 表查询
        # 2. 按 symbol, timeframe 和 t <= t_ref 过滤
        # 3. 按时间倒序 (DESC) 排序，以便 LIMIT 能取到最新的 N 条
        sql = """
        SELECT t, open, high, low, close, volume
        FROM ohlcv
        WHERE symbol = ?
          AND timeframe = ?
          AND t <= ?
        ORDER BY t DESC
        LIMIT ?
        """
        params = (symbol, timeframe, t_ref, lookback_bars)
        
        # 使用 pandas 读取数据
        df = pd.read_sql_query(sql, conn, params=params)
        
        if df.empty:
            return pd.DataFrame()
            
        # 关键：liquidity_filter 期望数据是按时间升序的
        # (iloc[-1] 是当前K线, iloc[:-1] 是历史K线)
        # 因为我们是 DESC 查询的，所以这里必须反转 (iloc[::-1])
        df = df.iloc[::-1].reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"[market_data] ERROR in fetch_ohlcv_series for {symbol} {timeframe}: {e}")
        # 出错时返回空 DataFrame
        return pd.DataFrame()

def compute_market_ctx(conn: sqlite3.Connection, t_ref: str, cfg: dict) -> dict:
    """
    市场上下文（BTC 是否在“放量下杀”）
    *** 实战修正版 ***
    - 严格只看 t_ref 以及之前已经闭合的K线
    - 禁止去交易所拿当前最新价，防止用到 t_ref 之后的未来信息
    - 如果数据不够，视为盲飞 => 直接当危险盘，禁止开多

    参数:
        conn : sqlite3.Connection
        t_ref: 本桶时间 (闭合K线时间, 'YYYY-MM-DD HH:MM:SS', Asia/Shanghai口径)
        cfg  : 全局配置dict（外层传进来的 cfg）

    期望在 cfg 里找到 (优先 csmvp.btc_ctx，如果没有就走 cfg.btc_ctx):
        btc_ctx.timeframe            e.g. "1h"
        btc_ctx.recent_bars          e.g. 6
        btc_ctx.hist_bars            e.g. 24
        btc_ctx.drop_pct_threshold   e.g. -0.03   # 近期价格跌幅到达这个阈值视为“下杀”
        btc_ctx.selloff_vol_ratio    e.g. 1.2     # 近期均量 / 历史均量 量能放大到这个倍数算放量

    返回 dict:
        {
          "panic_selloff": bool,   # True 代表“这是烂盘/高危环境”，后续应该禁止开多
          "is_down": bool|None,    # 价格是否触发下杀阈值
          "is_heavy": bool|None,   # 成交量是否显著放大
          "pct_change_recent": float|None,  # 近期跌幅
          "vol_ratio": float|None,          # 近期量能 / 历史量能
          "symbol": str|None,               # 使用到的BTC符号
          "timeframe": str,
          "recent_bars": int,
          "hist_bars": int,
          "bars_used": int,
          "reason": str|None               # 例如 "insufficient_btc_history"
        }

    设计原则:
    - 我们假设 ohlcv 表里只保存“已经闭合完成的K线”，也就是不会有半根未收完的bar。
    - 我们强制 SELECT ... WHERE t <= t_ref，保证不看 t_ref 之后的任何信息。
    - 如果拿不到足够的历史K线，就直接 panic_selloff=True（把市场当危险盘），
      这样系统在“眼睛是瞎的”情况下不会贸然开多。
    """
    # ==== 1) 读配置 ====
    C = cfg.get("csmvp") or cfg or {}
    btc_cfg = dict(C.get("btc_ctx") or {})

    tf        = str(btc_cfg.get("timeframe", "1h")).lower()
    n_recent  = int(btc_cfg.get("recent_bars", 6))
    n_hist    = int(btc_cfg.get("hist_bars", 24))
    drop_thr  = float(btc_cfg.get("drop_pct_threshold", -0.03))
    vol_thr   = float(btc_cfg.get("selloff_vol_ratio", 1.2))

    # BTC交易对别名，按优先级尝试
    btc_aliases = list(C.get("btc_symbols") or [
        "BTC/USDT",
        "BTCUSDT",
        "BTC-USD",
        "BTC/USDT:USDT",
    ])

    need_total = n_recent + n_hist + 2  # 多拿点缓冲，避免边界不够

    def _fetch_bars_db_cutoff(sym: str, tf_local: str, need: int, cutoff_tref: str):
        """
        只从本地sqlite拿 <= t_ref 的闭合K线。

        返回 [(close, volume), ...]，时间顺序为老->新。
        """
        try:
            rows = conn.execute(
                """
                SELECT close, volume
                FROM ohlcv
                WHERE symbol=? AND timeframe=? AND t <= ?
                ORDER BY t DESC
                LIMIT ?
                """,
                (sym, tf_local, cutoff_tref, need + 5)
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            return []

        seq = []
        # rows 现在是最新在前，所以我们翻转成老->新
        for close_v, vol_v in rows[::-1]:
            try:
                c = float(close_v)
                v = float(vol_v if vol_v is not None else 0.0)
                if (c > 0) and math.isfinite(c) and math.isfinite(v):
                    seq.append((c, v))
            except Exception:
                # 非法数据点就跳过
                continue

        # 只保留最后 need 根（老->新）
        if len(seq) > need:
            seq = seq[-need:]
        return seq

    # ==== 2) 找到第一个能拿到足够历史的BTC符号 ====
    bars = []
    used_symbol = None
    for sym in btc_aliases:
        candidate = _fetch_bars_db_cutoff(sym, tf, need_total, t_ref)
        # 至少需要 n_recent + n_hist + 1 根，才能做后面的比较
        if len(candidate) >= (n_recent + n_hist + 1):
            bars = candidate
            used_symbol = sym
            break

    # ==== 3) 如果没拿到足够K线 —— 盲飞保护 ====
    if (used_symbol is None) or (len(bars) < (n_recent + n_hist + 1)):
        out = {
            "panic_selloff": True,        # 没眼睛=当成危险盘，禁止开多
            "is_down": None,
            "is_heavy": None,
            "pct_change_recent": None,
            "vol_ratio": None,
            "symbol": used_symbol,
            "timeframe": tf,
            "recent_bars": n_recent,
            "hist_bars": n_hist,
            "bars_used": len(bars),
            "reason": "insufficient_btc_history",
        }
        try:
            _dbg("market_ctx_insufficient", ctx=out)  # 未定义也没关系，try/except 会吃掉
        except Exception:
            pass
        return out

    # ==== 4) 正常计算 ====
    # bars: [(close, vol)]，老->新
    closes = [c for (c, v) in bars]
    vols   = [v for (c, v) in bars]

    # 跌幅：最近 n_recent 根 vs n_recent 根前的一根
    if len(closes) >= (n_recent + 1):
        close_now  = closes[-1]
        close_prev = closes[-(n_recent + 1)]
        if close_prev and close_prev > 0:
            pct_change_recent = (close_now / close_prev) - 1.0
        else:
            pct_change_recent = None
    else:
        pct_change_recent = None

    # 成交量放大：近期均量 vs 更早一段均量
    recent_vols = vols[-n_recent:] if len(vols) >= n_recent else []
    start_hist  = max(0, len(vols) - n_recent - n_hist)
    end_hist    = max(0, len(vols) - n_recent)
    hist_vols   = vols[start_hist:end_hist]

    def _safe_mean(arr):
        arr2 = [x for x in arr if (x is not None and x >= 0 and math.isfinite(x))]
        return (sum(arr2) / len(arr2)) if arr2 else None

    avg_recent = _safe_mean(recent_vols)
    avg_hist   = _safe_mean(hist_vols)

    if (avg_recent is not None) and (avg_hist is not None) and (avg_hist > 0):
        vol_ratio = avg_recent / avg_hist
    else:
        vol_ratio = None

    # 是否“下杀”
    is_down = False
    if (pct_change_recent is not None) and math.isfinite(pct_change_recent):
        if pct_change_recent <= drop_thr:
            is_down = True

    # 是否“放量”
    is_heavy = False
    if (vol_ratio is not None) and math.isfinite(vol_ratio):
        if vol_ratio >= vol_thr:
            is_heavy = True

    panic_selloff = bool(is_down and is_heavy)

    out = {
        "panic_selloff": panic_selloff,
        "is_down": is_down,
        "is_heavy": is_heavy,
        "pct_change_recent": (
            None if pct_change_recent is None else float(pct_change_recent)
        ),
        "vol_ratio": (
            None if vol_ratio is None else float(vol_ratio)
        ),
        "symbol": used_symbol,
        "timeframe": tf,
        "recent_bars": n_recent,
        "hist_bars": n_hist,
        "bars_used": len(bars),
        "reason": None,
    }

    try:
        _dbg("market_ctx", ctx=out)
    except Exception:
        pass

    return out


def _get_exchange(cfg: dict):
    """
    返回一个 ccxt 交易所实例，用于拿实时 ticker / 盘口价格。

    约定（与旧逻辑一致）:
    - cfg["exchange"]            : 交易所名称字符串，比如 "binance", "okx", "bybit"...（默认 "binance"）
    - cfg["api_keys"][<name>]    : (可选) API key/secret/password
    - cfg["exchange_params"]     : (可选) 额外透传给 ccxt 构造器的 dict

    失败直接抛异常，由上游回退 DB 价。
    """
    import ccxt
    import logging
    if not isinstance(cfg, dict):
        logging.error("[probe] _get_exchange bad cfg: type=%s preview=%r",
                    type(cfg).__name__, str(cfg)[:200])
        raise RuntimeError("_get_exchange: cfg 不是 dict")
    try:
        logging.debug("[probe] _get_exchange ok: exchange=%r keys=%s",
                    cfg.get("exchange"),
                    list(cfg.keys())[:10])
    except Exception:
        pass
   
    if not isinstance(cfg, dict):
        raise RuntimeError("_get_exchange: cfg 不是 dict")

    ex_name = (cfg.get("exchange") or "binance")
    if not isinstance(ex_name, str) or not ex_name.strip():
        ex_name = "binance"
    ex_name = ex_name.strip()

    ex_cls = getattr(ccxt, ex_name, None)
    if ex_cls is None:
        raise RuntimeError(f"_get_exchange: 不支持的交易所 '{ex_name}' (ccxt里找不到同名类)")

    keys_all = cfg.get("api_keys") or {}
    keys_one = {}
    if isinstance(keys_all, dict):
        maybe_keys = keys_all.get(ex_name)
        if isinstance(maybe_keys, dict):
            keys_one = {
                k: v for k, v in maybe_keys.items()
                if k in ("apiKey", "secret", "password", "uid")
                and v is not None
            }

    extra_params = cfg.get("exchange_params") or {}
    if not isinstance(extra_params, dict):
        extra_params = {}

    base_params = {
        "enableRateLimit": True,
    }
    base_params.update(extra_params)
    base_params.update(keys_one)

    try:
        ex = ex_cls(base_params)
        try:
            ex.load_markets(reload=False)
        except Exception as e_load:
            logging.debug(f"_get_exchange: load_markets 警告: {e_load}")
        return ex
    except Exception as e:
        raise RuntimeError(f"_get_exchange: 实例化 {ex_name} 失败: {e}")


def fetch_last_price(conn: sqlite3.Connection,
                     symbol: str,
                     timeframe: str,
                     t_ref: str,
                     cfg: dict) -> float:
    """
    获取用于风险评估的入场价（修正版）

    新逻辑：
      1) 优先用实时 ticker 价格（真实可成交附近价）
      2) 失败 → 回退 DB 最近一根闭合K(t<=t_ref)的收盘价
      3) 还失败 → 抛异常

    返回:
        float, 当前可交易价（用于RR、止损距离等）
    """
    cur = conn.cursor()

    # --- 1) 实时价 ---
    ex = None
    try:
        ex = _get_exchange(cfg)
    except Exception as e:
        logging.warning(f"[price] _get_exchange() 失败，将回退DB价格: {e}")

    if ex is not None:
        try:
            ticker = ex.fetch_ticker(symbol)
            px_list = []
            if isinstance(ticker, dict):
                for key in ("last", "close", "bid", "ask"):
                    v = ticker.get(key)
                    if v is not None:
                        try:
                            v = float(v)
                            if v > 0:
                                px_list.append(v)
                        except Exception:
                            pass
            if px_list:
                live_px = None
                # 优先顺序：last > close > (bid+ask)/2
                if ticker.get("last"):
                    live_px = float(ticker["last"])
                elif ticker.get("close"):
                    live_px = float(ticker["close"])
                else:
                    bid = ticker.get("bid")
                    ask = ticker.get("ask")
                    if bid is not None and ask is not None:
                        try:
                            b = float(bid)
                            a = float(ask)
                            if b > 0 and a > 0:
                                live_px = (b + a) / 2.0
                        except Exception:
                            live_px = None

                if live_px is not None and live_px > 0:
                    logging.info(f"[price] using LIVE ticker {symbol} ~ {live_px}")
                    return live_px

        except Exception as e:
            logging.warning(f"[price] fetch_ticker({symbol}) 失败，将回退DB价格: {e}")

    # --- 2) 回退：DB 最近闭合K收盘价 (t<=t_ref) ---
    row = cur.execute(
        """
        SELECT close
        FROM ohlcv
        WHERE symbol = ?
          AND timeframe = ?
          AND t <= ?
        ORDER BY t DESC
        LIMIT 1
        """,
        (symbol, timeframe, t_ref)
    ).fetchone()

    if row and row[0] is not None:
        px = float(row[0])
        if px > 0:
            logging.info(f"[price] fallback DB close {symbol} {t_ref} -> {px}")
            return px

    # --- 3) 实在没价格 ---
    raise RuntimeError(f"_fetch_last_price: 无法获得 {symbol} 的有效价格(实时/DB都失败)")
