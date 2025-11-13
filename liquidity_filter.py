# -*- coding: utf-8 -*-
"""
core/liquidity_filter.py

[✅ 高性能修复版]

本模块负责流动性过滤。
原文件存在 3 个致命 Bug：
1. NameError (调用未定义函数)
2. API 滥用 (N+1 查询导致 IP 被封禁)
3. AttributeError (返回值类型不匹配)

此修复版重写了核心逻辑：
1. 采用高性能的批量 API 调用：无论有多少信号，都只发起 2 次 API 请求。
2. 修复了所有崩溃 Bug。
3. 从 cfg 读取静态阈值，替换了原先损坏的“动态阈值”逻辑。
"""

import requests
import numpy as np
from statistics import mean, stdev
import time
from typing import List, Dict, Any, Set

BINANCE_API_BASE = "https://api.binance.com/api/v3"

# -----------------------------------------------------------------
# ✅ 修复后的高性能函数
# -----------------------------------------------------------------

_TICKER_CACHE = {"data": None, "timestamp": 0}
_BOOK_TICKER_CACHE = {"data": None, "timestamp": 0}
_CACHE_TTL_SECONDS = 30 # 缓存 30 秒，防止在同一分钟内重复调用


# -----------------------------------------------------------------
# 第 1/2 块代码：添加到 core/liquidity_filter.py
# -----------------------------------------------------------------
import pandas as pd
from . import market_data
import sqlite3
from typing import List, Dict, Any, Tuple


def _fetch_volume_series(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    t_ref: str,
    lookback_bars: int
) -> pd.DataFrame:
    """
    [新] 辅助函数: 使用 market_data 获取 OHLCV 序列。
    [依赖: market_data.py (需要有 fetch_ohlcv_series)]
    """
    try:
        # 依赖 market_data.py 中的 fetch_ohlcv_series
        df = market_data.fetch_ohlcv_series(
            conn, symbol, timeframe, t_ref, lookback_bars
        )
        
        # 我们需要 'close' 和 'volume' 来计算美元成交额
        if 'close' in df.columns and 'volume' in df.columns:
            df['vol_usd'] = df['close'] * df['volume']
            return df[['vol_usd']]
        return pd.DataFrame()
    except Exception as e:
        print(f"[liquidity_filter] _fetch_volume_series error: {e}")
        return pd.DataFrame()

def filter_by_volume_spike(conn, candidates, t_ref, cfg):
    """
    体量放量门（自比较）：
    - 保持原有行为：返回 (kept, rejected, diag)
    - 仅新增 diag["per_symbol"]：记录每个标的的数值证据，便于上层落盘 decision_evidence
      per_symbol[symbol] = {
         "turnover_usd_curr": <float or None>,
         "turnover_usd_avgN": <float or None>,
         "spike_ratio": <float or None>,
         "lookback_bars": <int>,
         "min_spike_ratio": <float>,
         "source": {"table":"ohlcv","fields":["close","volume"],"calc":"close*volume","t_ref":t_ref,"window": f"<=t_ref last {lookback_bars}"},
         "pass": true/false/None,
         "reason": <string or None>
      }
    """
    vol_cfg = (cfg.get("thresholds") or {}).get("volume_spike", {})
    enforce = bool(vol_cfg.get("enforce", False))
    min_spike_ratio = float(vol_cfg.get("min_spike_ratio", 1.5))
    lookback_bars = int(vol_cfg.get("lookback_bars", 24))
    debug = bool(vol_cfg.get("debug", False))  # 保留原调试开关

    if not enforce:
        diag = {
            "note": "volume spike check disabled",
            "enforce": False,
            "kept": len(candidates),
            "rejected": 0,
            "per_symbol": {}  # 新增（禁用时为空）
        }
        return candidates, [], diag

    kept, rejected = [], []
    per_symbol: Dict[str, Dict[str, Any]] = {}

    for c in candidates:
        sym = c.get("symbol"); tf = c.get("timeframe") or "1h"
        direc = c.get("direction", "?")
        df = _fetch_volume_series(conn, sym, tf, t_ref, lookback_bars + 1)

        # 统一先准备 per_symbol 的基础结构
        per_symbol.setdefault(sym, {
            "turnover_usd_curr": None,
            "turnover_usd_avgN": None,
            "spike_ratio": None,
            "lookback_bars": lookback_bars,
            "min_spike_ratio": min_spike_ratio,
            "source": {
                "table": "ohlcv",
                "fields": ["close", "volume"],
                "calc": "close*volume",
                "t_ref": t_ref,
                "window": f"<=t_ref last {lookback_bars}"
            },
            "pass": None,
            "reason": None
        })

        if df.empty or len(df) < 3:
            # 原逻辑：数据缺失直接拒
            rejected.append({"symbol": sym, "timeframe": tf, "direction": direc,
                             "reject_reason": "volume_data_unavailable"})
            per_symbol[sym].update({"pass": False, "reason": "volume_data_unavailable"})
            if debug:
                print(f"[vol-spike][{t_ref}] {sym} {tf} NA -> REJECT volume_data_unavailable")
            continue

        try:
            cur = float(df.iloc[-1]["vol_usd"])
            avg = float(df.iloc[:-1]["vol_usd"].mean())
            per_symbol[sym]["turnover_usd_curr"] = cur
            per_symbol[sym]["turnover_usd_avgN"] = avg
        except Exception:
            rejected.append({"symbol": sym, "timeframe": tf, "direction": direc,
                             "reject_reason": "volume_calc_error"})
            per_symbol[sym].update({"pass": False, "reason": "volume_calc_error"})
            if debug:
                print(f"[vol-spike][{t_ref}] {sym} {tf} ERR -> REJECT volume_calc_error")
            continue

        if not (avg > 0):
            # 原逻辑：均值无效则放行
            kept.append(c)
            per_symbol[sym].update({"pass": True, "reason": "no_avg_baseline"})
            if debug:
                print(f"[vol-spike][{t_ref}] {sym} {tf} cur={cur:.2f} avg=0 -> PASS (no-avg)")
            continue

        ratio = cur / avg
        per_symbol[sym]["spike_ratio"] = ratio

        if ratio < min_spike_ratio:
            reason_txt = f"volume_spike_too_low ({ratio:.2f} < {min_spike_ratio})"
            rejected.append({"symbol": sym, "timeframe": tf, "direction": direc,
                             "reject_reason": reason_txt})
            per_symbol[sym].update({"pass": False, "reason": reason_txt})
            if debug:
                print(f"[vol-spike][{t_ref}] {sym} {tf} cur={cur:.2f} avg={avg:.2f} "
                      f"ratio={ratio:.2f} < {min_spike_ratio} -> REJECT")
        else:
            c["volume_spike_ratio"] = ratio  # 保持原有副作用（上层可能读取）
            kept.append(c)
            per_symbol[sym].update({"pass": True, "reason": "ok"})
            if debug:
                print(f"[vol-spike][{t_ref}] {sym} {tf} cur={cur:.2f} avg={avg:.2f} "
                      f"ratio={ratio:.2f} >= {min_spike_ratio} -> PASS")

    diag = {
        "enforce": enforce,
        "min_spike_ratio": min_spike_ratio,
        "lookback_bars": lookback_bars,
        "kept": len(kept),
        "rejected": len(rejected),
        "per_symbol": per_symbol  # 新增：每个标的的证据（数值/阈值/来源/结论/原因）
    }
    return kept, rejected, diag


def _get_all_tickers(cfg: dict) -> Dict[str, Dict[str, Any]]:
    """
    (高性能)
    获取所有U本位合约的 24h Ticker 数据（包含成交额）。
    使用内存缓存，确保在 30 秒内只调用一次 API。
    """
    global _TICKER_CACHE
    now = time.time()
    
    # 检查缓存是否有效
    if _TICKER_CACHE["data"] and (now - _TICKER_CACHE["timestamp"] < _CACHE_TTL_SECONDS):
        return _TICKER_CACHE["data"]

    try:
        # 币安 U本位合约 Ticker API
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        tickers = response.json()
        
        # 将 list 转换为 dict，方便快速查找
        ticker_map = {d["symbol"]: d for d in tickers if "symbol" in d}
        
        _TICKER_CACHE["data"] = ticker_map
        _TICKER_CACHE["timestamp"] = now
        return ticker_map
        
    except Exception as e:
        print(f"[liquidity_filter] 严重错误: _get_all_tickers API 请求失败: {e}")
        # API 失败时，返回一个空字典，并依赖旧缓存（如果有）
        return _TICKER_CACHE["data"] if _TICKER_CACHE["data"] else {}

def _get_all_book_tickers(cfg: dict) -> Dict[str, Dict[str, Any]]:
    """
    (高性能)
    获取所有U本位合约的实时买卖盘（Book Ticker）。
    使用内存缓存，确保在 30 秒内只调用一次 API。
    """
    global _BOOK_TICKER_CACHE
    now = time.time()

    if _BOOK_TICKER_CACHE["data"] and (now - _BOOK_TICKER_CACHE["timestamp"] < _CACHE_TTL_SECONDS):
        return _BOOK_TICKER_CACHE["data"]

    try:
        # 币安 U本位合约 Book Ticker API
        url = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        books = response.json()
        
        book_map = {d["symbol"]: d for d in books if "symbol" in d}
        
        _BOOK_TICKER_CACHE["data"] = book_map
        _BOOK_TICKER_CACHE["timestamp"] = now
        return book_map
        
    except Exception as e:
        print(f"[liquidity_filter] 严重错误: _get_all_book_tickers API 请求失败: {e}")
        return _BOOK_TICKER_CACHE["data"] if _BOOK_TICKER_CACHE["data"] else {}

from typing import List, Any, Dict, Set, Tuple, Union

def prefilter_candidates_with_liquidity_filter(
    candidates: List[Any],
    cfg: dict,
    return_details: bool = False,
) -> Union[Set[str], Tuple[Set[str], Dict[str, Dict[str, Any]]]]:
    """
    高性能流动性预过滤（不强制用固定 24h 成交额）

    策略：
      1) 能拉到交易所 ticker/book 的先留下来
      2) 若配置了 max_bid_ask_spread_pct，则做点差筛选
      3) 若配置了 min_24h_volume_usd，则做绝对量筛选
      4) 放量自比较留到后续（决策层第二刀）

    返回：
      - return_details=False: Set[str]            （始终为 set）
      - return_details=True : (Set[str], details)（details 为 dict）
    """
    liq_cfg = (
        cfg.get("core", {}).get("liquidity")
        or cfg.get("liquidity")
        or {}
    )

    MIN_VOL_USD = liq_cfg.get("min_24h_volume_usd", None)          # None 表示禁用
    MAX_SPREAD_PCT = liq_cfg.get("max_bid_ask_spread_pct", None)   # None 表示禁用

    if MIN_VOL_USD is None and MAX_SPREAD_PCT is None:
        print("[liquidity_filter] 开始过滤: ticker/book 存在性检查 (无固定24h量、无固定点差)")
    else:
        print(
            "[liquidity_filter] 开始过滤: "
            f"min_vol={'disabled' if MIN_VOL_USD is None else f'{MIN_VOL_USD/1_000_000:.1f}M'}, "
            f"max_spread={'disabled' if MAX_SPREAD_PCT is None else f'{MAX_SPREAD_PCT:.3f}%'}"
        )

    # ---- 拉取交易所快照（ticker/book）----
    try:
        all_tickers = _get_all_tickers(cfg)          # 期望: dict[api_sym] -> {..., 'quoteVolume'/'volume': str/float}
        all_books   = _get_all_book_tickers(cfg)     # 期望: dict[api_sym] -> {'bidPrice': str, 'askPrice': str}
    except Exception as e:
        print(f"[liquidity_filter] API error: {e}, skip filter (pass-through).")
        passed_all = {
            str(c.get("symbol") if isinstance(c, dict) else c)
            for c in candidates if (c and (c.get("symbol") if isinstance(c, dict) else c))
        }
        if return_details:
            details = {
                s: {
                    "api_symbol": str(s).replace("/", "").replace("-", "").upper(),
                    "vol_24h_usd": None,
                    "spread_pct": None,
                    "reason": "api_error_skip",
                } for s in passed_all
            }
            return passed_all, details
        return passed_all

    if not all_tickers or not all_books:
        print("[liquidity_filter] empty api result, skip filter (pass-through).")
        passed_all = {
            str(c.get("symbol") if isinstance(c, dict) else c)
            for c in candidates if (c and (c.get("symbol") if isinstance(c, dict) else c))
        }
        if return_details:
            details = {
                s: {
                    "api_symbol": str(s).replace("/", "").replace("-", "").upper(),
                    "vol_24h_usd": None,
                    "spread_pct": None,
                    "reason": "empty_api_skip",
                } for s in passed_all
            }
            return passed_all, details
        return passed_all

    passed_set: Set[str] = set()
    details: Dict[str, Dict[str, Any]] = {}
    rejected_reasons: Dict[str, List[str]] = {}

    for c in candidates:
        raw_sym = c.get("symbol") if isinstance(c, dict) else c
        if not raw_sym:
            continue
        raw_sym = str(raw_sym).strip()
        api_sym = raw_sym.replace("/", "").replace("-", "").upper()

        ticker_data = all_tickers.get(api_sym)
        if not ticker_data:
            # 连 ticker 都没有，视为不可交易
            rejected_reasons.setdefault("Ticker_Not_Found", []).append(api_sym)
            details[raw_sym] = {
                "api_symbol": api_sym,
                "vol_24h_usd": None,
                "spread_pct": None,
                "reason": "Ticker_Not_Found",
            }
            continue

        # 24h 成交额（优先使用 quoteVolume；没有则回退 volume）
        vol_24h_usd = ticker_data.get("quoteVolume") or ticker_data.get("volume") or None
        if isinstance(vol_24h_usd, str):
            try:
                vol_24h_usd = float(vol_24h_usd.replace(",", ""))
            except Exception:
                vol_24h_usd = None

        # 点差
        book_data = all_books.get(api_sym, {})
        bid = book_data.get("bidPrice")
        ask = book_data.get("askPrice")
        spread_pct = None
        if bid is not None and ask is not None:
            try:
                bid_f = float(bid)
                ask_f = float(ask)
                if bid_f > 0:
                    spread_pct = (ask_f - bid_f) / bid_f * 100.0
            except Exception:
                spread_pct = None

        # 1) 点差检查（仅当配置了阈值且可算出点差）
        if (MAX_SPREAD_PCT is not None) and (spread_pct is not None) and (spread_pct > MAX_SPREAD_PCT):
            rejected_reasons.setdefault("High_Spread", []).append(api_sym)
            details[raw_sym] = {
                "api_symbol": api_sym,
                "vol_24h_usd": vol_24h_usd,
                "spread_pct": spread_pct,
                "reason": "High_Spread",
            }
            continue

        # 2) 绝对量检查（仅当配置了阈值且有成交额数字）
        if (MIN_VOL_USD is not None) and (vol_24h_usd is not None) and (vol_24h_usd < MIN_VOL_USD):
            rejected_reasons.setdefault("Low_Volume_24h", []).append(api_sym)
            details[raw_sym] = {
                "api_symbol": api_sym,
                "vol_24h_usd": vol_24h_usd,
                "spread_pct": spread_pct,
                "reason": f"Low_Volume_24h ({vol_24h_usd:.0f} < {MIN_VOL_USD:.0f})",
            }
            continue

        # 通过第一刀
        passed_set.add(raw_sym)
        details[raw_sym] = {
            "api_symbol": api_sym,
            "vol_24h_usd": vol_24h_usd,
            "spread_pct": spread_pct,
            "reason": "ok",
        }

    # 日志：被挡原因与通过数量
    if rejected_reasons:
        blocked_n = sum(len(v) for v in rejected_reasons.values())
        print(f"[liquidity_filter] 已过滤 {blocked_n} 个标的:")
        for reason, syms in rejected_reasons.items():
            preview = ", ".join(syms[:5])
            suffix = "..." if len(syms) > 5 else ""
            print(f"  - {reason}: {preview}{suffix}")

    print(f"[liquidity_filter] 允许 {len(passed_set)} 个标的通过。")

    if return_details:
        return passed_set, details
    return passed_set
