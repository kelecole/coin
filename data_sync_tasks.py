#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
data_sync_tasks.py
- 只做两件事（针对 BTC）：
  1) 回填 OHLCV（1h/4h，最近 N 根）
  2) 增量补齐最新闭合K

修复点：
- min_candles 与 max_candles 自动协调，避免“回填 240 根 < 最小 990 根”被判无效
- 先 load_markets，再根据 btc_symbols 候选从交易所的真实市场里挑可用的符号（比如 Binance 上不会使用 BTC/USD）

运行示例：
  python data_sync_tasks.py --config /www/wwwroot/Crypto-Signal/config.yml --mode backfill --log INFO
  python data_sync_tasks.py --config /www/wwwroot/Crypto-Signal/config.yml --mode increment --log INFO
  python data_sync_tasks.py --config /www/wwwroot/Crypto-Signal/config.yml --mode all --log INFO
"""

import os
import sys
import argparse
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import time
import numpy as np
import pandas as pd
import ccxt
from core.timebox import now_local_str

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)




# 你现成工具（用户已提供）
from timezone_converter import TimezoneConverter

# -------------------------
# 全局 CONFIG/Exchange 句柄
# -------------------------
CONFIG: Dict[str, Any] = {}
EXCHANGE: Optional[ccxt.Exchange] = None

# -------------------------------
# Funding / Basis / OI / Long-ratio for BTC (Binance Futures)
# -------------------------------
import requests
import math
from datetime import datetime, timedelta
import pytz

BINANCE_FAPI = "https://fapi.binance.com"
BTC_PERP_SYMBOL = "BTCUSDT"           # Binance 合约符号
BTC_PAIR_HUMAN  = "BTC/USDT"          # 我们表里统一的写法
FUNDING_SOURCE  = "binance_futures"

def connect_db(cfg_root: dict) -> sqlite3.Connection:
    """
    从配置里取数据库路径优先级：
    1) core.db_path  ← 现在主配置用这个
    2) csmvp.db_path ← 兼容老版本
    都没有就报错。
    """
    # 先看新版结构
    core_cfg = cfg_root.get("core", {}) or {}
    db_path = core_cfg.get("db_path")

    # 兼容老的 csmvp:
    if not db_path:
        csmvp = cfg_root.get("csmvp", {}) or {}
        db_path = csmvp.get("db_path")

    if not db_path:
        raise FileNotFoundError("配置里没有找到 core.db_path 也没有 csmvp.db_path")

    # 防一手路径写错/目录不存在
    db_dir = os.path.dirname(db_path) or "."
    if not os.path.exists(db_dir):
        raise FileNotFoundError(f"数据库目录不存在: {db_dir} (db_path={db_path})")

    return get_conn(db_path)

def _symbols_from_config(cfg_root: dict) -> list[str]:
    """
    从配置里取交易对列表，优先 core.tests，其次 csmvp.tests，最后兜底 BTC/USDT
    """
    core_cfg = (cfg_root.get("core") or {})
    syms = core_cfg.get("tests")

    if not syms:
        csmvp_cfg = (cfg_root.get("csmvp") or {})
        syms = csmvp_cfg.get("tests")

    if not syms:
        syms = ["BTC/USDT"]

    out, seen = [], set()
    for s in syms:
        s = (s or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    if not out:
        out = ["BTC/USDT"]
    return out



# 顶部 imports 附近增加
try:
    from core.avwap import _last_closed_tref as _bucket_close   # 已经统一到 timebox 口径
except Exception:
    _bucket_close = None

from datetime import datetime

def floor_to_bucket_str(ts: datetime | str, timeframe: str = "1h", tz: str = "Asia/Shanghai") -> str:
    """
    返回该 ts 在 timeframe 下的最后收盘桶 'YYYY-MM-DD HH:MM:SS'（Asia/Shanghai）。
    """
    if _bucket_close is not None:
        return _bucket_close(
            ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts),
            timeframe
        )
    # 兜底（不建议长期使用）
    from core.avwap import _floor_to_candle
    return _floor_to_candle(
        ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts),
        timeframe
    )


def _binance_premium_index(symbol: str = BTC_PERP_SYMBOL) -> dict:
    """
    资金费/基差 相关：premiumIndex 接口。
    返回 dict: { 'funding_rate': float, 'markPrice': float, 'indexPrice': float, 'ts': int }
    """
    url = f"{BINANCE_FAPI}/fapi/v1/premiumIndex"
    r = requests.get(url, params={"symbol": symbol}, timeout=8)
    r.raise_for_status()
    d = r.json()
    funding_rate = float(d.get("lastFundingRate", 0.0))
    mark_price   = float(d.get("markPrice", 0.0))
    index_price  = float(d.get("indexPrice", 0.0))
    ts           = int(d.get("time", 0))
    return {
        "funding_rate": funding_rate,
        "markPrice": mark_price,
        "indexPrice": index_price,
        "ts": ts
    }

def _binance_open_interest(symbol: str = BTC_PERP_SYMBOL) -> float:
    """
    当前未平仓量（张数换成 USDT 规模：接口已经是张数? 该端点返回数量，直接存）
    """
    url = f"{BINANCE_FAPI}/fapi/v1/openInterest"
    r = requests.get(url, params={"symbol": symbol}, timeout=8)
    r.raise_for_status()
    d = r.json()
    # 返回字段: {"openInterest":"12345.6789", "symbol":"BTCUSDT", "time":...}
    return float(d.get("openInterest", 0.0))

def _binance_long_ratio(symbol: str = BTC_PERP_SYMBOL, period: str = "1h") -> float:
    """
    顶级账户多空比（账户口径）。返回 long_ratio ∈ [0,1]，如 0.689 表示 68.9% 多头。
    使用 topLongShortAccountRatio 接口，取最近一条。
    """
    url = f"{BINANCE_FAPI}/futures/data/topLongShortAccountRatio"
    r = requests.get(url, params={"symbol": symbol, "period": period, "limit": 1}, timeout=8)
    r.raise_for_status()
    arr = r.json()
    if not arr:
        return None
    # 字段例子：{"symbol":"BTCUSDT","longShortRatio":"1.22",...}
    lsr = float(arr[0].get("longShortRatio", 0.0))
    # longShortRatio = Long / Short
    if not math.isfinite(lsr) or lsr <= 0:
        return None
    long_ratio = lsr / (1.0 + lsr)
    return max(0.0, min(1.0, long_ratio))

def upsert_funding_snapshot_row(conn, t_ref: str, symbol: str, funding_rate: float,
                                basis: float, basis_delta: float, oi: float,
                                long_ratio: float, timeframe: str = "1h", source: str = FUNDING_SOURCE) -> int:
    """
    把资金面一行写入 funding_snapshot（存在则覆盖）。返回 1（受影响行数不易取，这里返回逻辑成功 1）
    """
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        INSERT INTO funding_snapshot
          (t_ref, symbol, timeframe, funding_rate, basis, basis_delta, oi, long_ratio, source, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(t_ref, symbol)
        DO UPDATE SET
          timeframe=excluded.timeframe,
          funding_rate=excluded.funding_rate,
          basis=excluded.basis,
          basis_delta=excluded.basis_delta,
          oi=excluded.oi,
          long_ratio=excluded.long_ratio,
          source=excluded.source,
          created_at=excluded.created_at
    """, (t_ref, symbol, timeframe, funding_rate, basis, basis_delta, oi, long_ratio, source, now))
    conn.commit()
    return 1


def sync_btc_funding_snapshot(
    conn,
    cfg_root: dict,
    mode: str = "increment",
    lookback_hours: int = 48,
) -> int:
    """
    写入 BTC 的资金面快照（Asia/Shanghai，严格落到 1h 收盘桶）。
    - mode='increment': 只写当前最后收盘的 1h 桶
    - mode='backfill'/'all': 回填最近 lookback_hours 个 1h 桶（含当前最后收盘桶）
    返回成功 upsert 的条数（尝试次数）。
    """
    # 延迟导入，避免循环依赖
    try:
        from core.timebox import now_local_dt
    except Exception:
        now_local_dt = None
    try:
        from core.avwap import _last_closed_tref as _bucket_1h
    except Exception:
        _bucket_1h = None

    from datetime import timedelta, datetime
    import math

    tz = (
        cfg_root.get("tz")
        or cfg_root.get("core", {}).get("tz")
        or cfg_root.get("csmvp", {}).get("tz")
        or "Asia/Shanghai"
    )

    def _now_local():
        if now_local_dt:
            return now_local_dt(tz)
        # 兜底：无 timebox 时，用系统本地时间（不推荐长期使用）
        return datetime.now()

    def _last_closed_1h(ts) -> str:
        """
        返回 ts 所在时刻的最后收盘 1h 桶（'YYYY-MM-DD HH:00:00'，Asia/Shanghai）
        """
        if _bucket_1h is not None:
            s = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts)
            return _bucket_1h(s, "1h")
        # 兜底：手工 floor 到小时
        if not isinstance(ts, datetime):
            ts = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S")
        floored = ts.replace(minute=0, second=0, microsecond=0)
        return floored.strftime("%Y-%m-%d %H:%M:%S")

    def _to_float(x):
        try:
            if x is None or (isinstance(x, str) and x.strip() == ""):
                return None
            return float(x)
        except Exception:
            return None

    # 常量与外部函数，按你原文件已有定义使用
    # BTC_PERP_SYMBOL, BTC_PAIR_HUMAN, FUNDING_SOURCE
    # _binance_premium_index, _binance_open_interest, _binance_long_ratio
    # upsert_funding_snapshot_row

    def write_one(bucket_dt: datetime) -> int:
        t_bucket = _last_closed_1h(bucket_dt)  # 统一口径：最后收盘 1h 桶

        # 1) 拉取即时数据（视为该桶的代表值）
        try:
            pidx = _binance_premium_index(BTC_PERP_SYMBOL)
        except Exception as e:
            print(f"[funding] premiumIndex 拉取失败: {e}")
            return 0

        funding_rate = _to_float(pidx.get("funding_rate"))
        mark_price   = _to_float(pidx.get("markPrice"))
        index_price  = _to_float(pidx.get("indexPrice"))
        basis = None
        if mark_price is not None and index_price is not None:
            basis = mark_price - index_price

        # 2) 读取上一个桶，计算 basis_delta（按 1h 桶过滤，避免串到其它 TF）
        cur = conn.cursor()
        cur.execute(
            """
            SELECT basis
            FROM funding_snapshot
            WHERE symbol=? AND timeframe='1h' AND t_ref < ?
            ORDER BY t_ref DESC
            LIMIT 1
            """,
            (BTC_PAIR_HUMAN, t_bucket),
        )
        row = cur.fetchone()
        prev_basis = _to_float(row[0]) if row and row[0] is not None else None
        basis_delta = (basis - prev_basis) if (basis is not None and prev_basis is not None) else None

        # 3) OI / 顶级账户多空比
        try:
            oi = _to_float(_binance_open_interest(BTC_PERP_SYMBOL))
        except Exception as e:
            print(f"[funding] openInterest 拉取失败: {e}")
            oi = None

        try:
            long_ratio = _to_float(_binance_long_ratio(BTC_PERP_SYMBOL, "1h"))
        except Exception as e:
            print(f"[funding] longRatio 拉取失败: {e}")
            long_ratio = None

        # 4) 落表（upsert）
        upsert_funding_snapshot_row(
            conn=conn,
            t_ref=t_bucket,
            symbol=BTC_PAIR_HUMAN,
            funding_rate=funding_rate,
            basis=basis,
            basis_delta=basis_delta,
            oi=oi,
            long_ratio=long_ratio,
            timeframe="1h",
            source=FUNDING_SOURCE,
        )
        return 1

    n = 0
    now_loc = _now_local()

    if mode in ("all", "backfill"):
        # 回填最近 lookback_hours 个整点（含当前最后收盘桶）
        # 从最老到最新，保证时间顺序一致
        processed = set()
        for h in range(lookback_hours, -1, -1):
            dt = now_loc - timedelta(hours=h)
            t_bucket = _last_closed_1h(dt)
            if t_bucket in processed:
                continue
            processed.add(t_bucket)
            try:
                n += write_one(dt)
            except Exception as e:
                print(f"[funding] write_one({t_bucket}) 失败: {e}")
    else:
        # 仅当前最后收盘桶
        try:
            n += write_one(now_loc)
        except Exception as e:
            print(f"[funding] write_one(current) 失败: {e}")

    return n

# -------------------------
# 工具：时间粒度 -> timedelta
# -------------------------
def get_time_delta(tf: str) -> timedelta:
    tf = tf.lower().strip()
    if tf.endswith("m"):
        return timedelta(minutes=int(tf[:-1]))
    if tf.endswith("h"):
        return timedelta(hours=int(tf[:-1]))
    if tf.endswith("d"):
        return timedelta(days=int(tf[:-1]))
    raise ValueError(f"Unsupported timeframe: {tf}")


# -------------------------
# 工具：ATR 计算（简单版）
# -------------------------
def compute_atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)

    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(length, min_periods=1).mean()
    return atr


# -------------------------
# 读取配置
# -------------------------
def load_config(path: str) -> Dict[str, Any]:
    import yaml
    # 允许传入 csmvp/config.yml 或项目根 config.yml
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到配置文件：{path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg or {}


# -------------------------
# 连接数据库
# -------------------------
def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


# -------------------------
# 确保 schema（基础索引）
# -------------------------
def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # 你现成的 ohlcv 表这里不创建，只保证索引存在
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_tf_t ON ohlcv(symbol, timeframe, t);")
    except Exception as e:
        logging.warning(f"创建 ohlcv 索引失败（可能表不存在）：{e}")
    conn.commit()


# -------------------------
# CCXT 交易所实例
# -------------------------
def get_exchange_from_config(cfg_root: Dict[str, Any]) -> ccxt.Exchange:
    # 默认使用 binance
    ex_cfg = cfg_root.get("data_source", {}).get("exchange", "binance")
    if ex_cfg.lower() == "binance":
        ex = ccxt.binance({"enableRateLimit": True, "timeout": 15000})
    else:
        # 也可扩展 okx、bybit 等
        ex = getattr(ccxt, ex_cfg)({"enableRateLimit": True, "timeout": 15000})
    return ex


# -------------------------
# 根据别名挑当前交易所真实上市的 BTC 符号
# -------------------------
def pick_listed_symbol(exchange: ccxt.Exchange, candidates: List[str]) -> Optional[str]:
    try:
        markets = exchange.load_markets()
    except Exception as e:
        logging.error(f"load_markets 失败：{e}")
        markets = {}
    for s in candidates:
        if s in markets:
            return s
    return None

# 建议位置：data_sync_tasks.py 或新建 policy.py
import sqlite3
from core.timebox import now_local_str

def upsert_threshold_policy(
    conn: sqlite3.Connection,
    *,
    name: str,
    timeframe: str,
    regime: str,
    value: float,
    scope: str = "global",         # 'global' | 'symbol_group' | 'symbol'
    symbol: str | None = None,     # scope!='global' 时可传具体 symbol 或分组名
    q: float | None = None,        # 采用的分位数（可选）
    lookback_days: int | None = None,
    sample_size: int | None = None,
    valid_from: str | None = None, # 不传则用当前本地时间
    valid_to: str | None = None    # 可为 None 表示长期有效
) -> int:
    """
    将一条阈值策略写入 threshold_policy（存在则覆盖）。
    返回受影响的行数（1 表示成功 UPSERT）。
    """
    sym = (symbol or "").strip()
    now_ts = now_local_str("Asia/Shanghai")
    vf = valid_from or now_ts

    sql = """
    INSERT INTO threshold_policy
      (name, timeframe, regime, scope, symbol, value, q, lookback_days, sample_size, valid_from, valid_to, updated_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(name, timeframe, regime, scope, symbol)
    DO UPDATE SET
      value = excluded.value,
      q = excluded.q,
      lookback_days = excluded.lookback_days,
      sample_size = excluded.sample_size,
      valid_from = excluded.valid_from,
      valid_to = excluded.valid_to,
      updated_at = excluded.updated_at
    ;
    """
    cur = conn.cursor()
    cur.execute(sql, (name, timeframe, regime, scope, sym, float(value),
                      q, lookback_days, sample_size, vf, valid_to, now_ts))
    conn.commit()
    return cur.rowcount

# -------------------------
# 用户提供的 fetch_ohlcv（做了两点改动）：
#  1) min_candles = min(cfg_min, max_candles)
#  2) 动态使用全局 CONFIG 和 EXCHANGE
# -------------------------
def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 1000, max_candles: int = None) -> Optional[pd.DataFrame]:
    """
    从交易所获取 OHLCV（包含：最早日期锚点、未收盘K剔除、Asia/Shanghai 时区、ATR 计算等）
    """
    global CONFIG, EXCHANGE

    fetch_params = CONFIG.get('data_source', {}).get('fetch_params', {})
    if max_candles is None:
        max_candles = fetch_params.get('max_candles_to_fetch', 30000)
    # 关键修复：min_candles 与 max_candles 协调，避免误判
    min_candles_cfg = fetch_params.get('min_candles_for_processing', 990)
    min_candles = min(int(min_candles_cfg), int(max_candles))

    limit = fetch_params.get('ohlcv_fetch_limit', 1000)
    api_retries = fetch_params.get('api_retries', 3)
    api_retry_delay = fetch_params.get('api_retry_delay_seconds', 2)

    logging.info(f"开始获取数据: {symbol}, {timeframe}, limit={limit}, max_candles={max_candles}, min_candles={min_candles}")
    try:
        all_ohlcv = []
        earliest_date = pd.to_datetime("2017-08-17T00:00:00Z")  # binance 开站日
        earliest_timestamp_ms = int(earliest_date.timestamp() * 1000)

        current_time_ms = int(datetime.utcnow().timestamp() * 1000)
        delta_ms = get_time_delta(timeframe).total_seconds() * 1000
        calculated_since = current_time_ms - int(max_candles * delta_ms)
        since = max(calculated_since, earliest_timestamp_ms)

        logging.info(f"修正后的初始 since 时间戳: {pd.to_datetime(since, unit='ms')}")
        consecutive_empty_attempts = 0
        max_consecutive_empty = 3

        while len(all_ohlcv) < max_candles:
            ohlcv = None
            for attempt in range(api_retries):
                try:
                    logging.debug(f"请求数据: since={pd.to_datetime(since, unit='ms')}, attempt={attempt+1}")
                    ohlcv = EXCHANGE.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
                    logging.debug(f"请求返回 {len(ohlcv)} 根 K 线")
                    break
                except Exception as e:
                    logging.warning(f"获取 OHLCV 数据失败 (尝试 {attempt+1}/{api_retries}): {e}")
                    if attempt < api_retries - 1:
                        time.sleep(api_retry_delay)
                    else:
                        logging.error(f"达到最大重试次数，视为空数据")
                        ohlcv = []

            if not ohlcv:
                consecutive_empty_attempts += 1
                logging.warning(f"API 返回空数据 for {symbol}, {timeframe}, 连续空数据尝试 {consecutive_empty_attempts}/{max_consecutive_empty}, 当前K线数量: {len(all_ohlcv)}")
                if consecutive_empty_attempts >= max_consecutive_empty:
                    if len(all_ohlcv) >= min_candles:
                        logging.info(f"连续空数据达到 {max_consecutive_empty} 次，但已收集足够数据，退出循环")
                        break
                    else:
                        logging.warning(f"连续空数据达到 {max_consecutive_empty} 次，且K线数量不足，跳过此交易对")
                        return None
                continue

            consecutive_empty_attempts = 0
            existing_timestamps = {c[0] for c in all_ohlcv}
            new_ohlcv = [c for c in ohlcv if c[0] not in existing_timestamps]
            if not new_ohlcv:
                logging.warning(f"API未返回新数据，可能已到达历史数据末尾。")
                break

            all_ohlcv.extend(new_ohlcv)
            all_ohlcv.sort(key=lambda x: x[0])
            logging.info(f"累计获取 {len(all_ohlcv)} 根不重复的 K 线")
            since = all_ohlcv[-1][0] + 1
            if len(all_ohlcv) >= max_candles:
                logging.info(f"已达到或超过最大 K 线数量 {max_candles}，退出循环")
                break

        if len(all_ohlcv) < min_candles:
            logging.warning(f"获取的 K 线数量 {len(all_ohlcv)} 小于最小要求 {min_candles}，跳过此交易对")
            return None

        timezone_converter = TimezoneConverter(local_timezone='Asia/Shanghai')
        data = []
        for candle in all_ohlcv:
            ts = candle[0] / 1000
            local_time_str = timezone_converter.convert_to_local(ts)
            local_time = pd.to_datetime(local_time_str, format='%Y-%m-%d %H:%M:%S')
            data.append([local_time] + candle[1:])

        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        if df.empty:
            logging.error(f"构建 DataFrame 失败，数据为空 for {symbol}, {timeframe}")
            return None
        df.set_index('timestamp', inplace=True)

        now_local = pd.to_datetime(timezone_converter.convert_to_local(datetime.utcnow().timestamp()), format='%Y-%m-%d %H:%M:%S')
        delta = get_time_delta(timeframe)
        last_candle_open = df.index[-1]
        expected_close_time = last_candle_open + delta
        if now_local < expected_close_time:
            df = df.iloc[:-1]
            logging.info(f"删除未完成 K 线，剩余 {len(df)} 根 K 线 for {symbol}, {timeframe}")
            if df.empty:
                logging.info(f"{symbol} {timeframe}: 删除未完成 K 线后无有效数据")
                return None

        required_columns = ['open', 'high', 'low', 'close', 'volume']
        if df[required_columns].isnull().values.any() or not np.isfinite(df[required_columns].values).all():
            logging.error(f"数据包含 NaN 或无穷大值 for {symbol}, {timeframe}")
            return None

        df['ATR'] = compute_atr(df, length=14)
        logging.info(f"成功获取并处理 {len(df)} 根 K 线 for {symbol}, {timeframe}")
        return df

    except Exception as e:
        logging.error(f"获取或处理 OHLCV 数据时发生严重错误 ({symbol}, {timeframe}): {e}", exc_info=True)
        return None


# -------------------------
# Upsert OHLCV 到表 ohlcv
#   表结构约定：t TEXT, symbol TEXT, timeframe TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL
# -------------------------
def upsert_ohlcv_df(conn, symbol: str, timeframe: str, df) -> int:
    """
    把 df(包含 t/open/high/low/close/volume) Upsert 到 ohlcv。
    - 分片写入 + commit，减少长事务占锁
    - 简易重试，缓解 `database is locked`
    需要：import time, sqlite3
    """
    import time, sqlite3
    if df is None or df.empty:
        return 0

    rows = [
        (symbol, timeframe, *r)
        for r in df[['t','open','high','low','close','volume']].itertuples(index=False, name=None)
    ]

    # 保底：确保表存在（与原建表一致即可）
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ohlcv (
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      t TEXT NOT NULL,
      open REAL, high REAL, low REAL, close REAL, volume REAL,
      created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
      UNIQUE(symbol, timeframe, t)
    );
    """)

    sql = """
    INSERT INTO ohlcv(symbol,timeframe,t,open,high,low,close,volume,created_at)
    VALUES(?,?,?,?,?,?,?,?,datetime('now','localtime'))
    ON CONFLICT(symbol,timeframe,t) DO UPDATE SET
    open=excluded.open,
    high=excluded.high,
    low=excluded.low,
    close=excluded.close,
    volume=excluded.volume,
    created_at=datetime('now','localtime');
    """

    cur = conn.cursor()
    total, CHUNK = 0, 500
    for i in range(0, len(rows), CHUNK):
        part = rows[i:i+CHUNK]
        for attempt in range(3):
            try:
                cur.executemany(sql, part)
                conn.commit()  # 快速提交释放写锁
                total += len(part)
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise
    cur.close()
    return total


def sync_symbols_ohlcv(conn, cfg_root: dict, mode: str = "increment") -> None:
    """
    把配置里的币种+周期的K线拉下来写入 ohlcv 表
    优先 core.tests / core.timeframes
    """
    import logging
    # 初始化全局 CONFIG/EXCHANGE（给 fetch_ohlcv 用）
    global CONFIG, EXCHANGE
    CONFIG = cfg_root or {}
    try:
        EXCHANGE = get_exchange_from_config(CONFIG)
    except Exception as e:
        logging.error(f"init exchange error: {e}")
        return

    core_cfg = (cfg_root.get("core") or {})
    csmvp_cfg = (cfg_root.get("csmvp") or {})

    symbols = core_cfg.get("tests") or csmvp_cfg.get("tests") or ["BTC/USDT"]
    timeframes = core_cfg.get("timeframes") or csmvp_cfg.get("timeframes") or ["1h", "4h"]

    # 可选的抓取参数
    fetch_params = (cfg_root.get("data_source") or {}).get("fetch_params", {})
    if mode == "backfill":
        max_candles = int(fetch_params.get("max_candles_to_fetch", 240))
    else:
        max_candles = int(fetch_params.get("increment_candles", 120))
    limit = int(fetch_params.get("ohlcv_fetch_limit", 1000))

    # 去重
    seen, symbols_clean = set(), []
    for s in symbols:
        s = (s or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        symbols_clean.append(s)

    for sym in symbols_clean:
        for tf in timeframes:
            logging.info(f"[ohlcv] {mode} 拉取 {sym} {tf} max_candles={max_candles}")
            try:
                df = fetch_ohlcv(sym, tf, limit=limit, max_candles=max_candles)
            except Exception as e:
                logging.warning(f"[ohlcv] 抓取失败：{sym} {tf} -> {e}")
                continue

            if df is None or df.empty:
                logging.warning(f"[ohlcv] 无数据：{sym} {tf}")
                continue

            dff = df[["open", "high", "low", "close", "volume"]].copy()
            dff["t"] = dff.index.strftime("%Y-%m-%d %H:%M:%S")
            dff = dff[["t", "open", "high", "low", "close", "volume"]]

            try:
                n = upsert_ohlcv_df(conn, sym, tf, dff)
                logging.info(f"[ohlcv] 写入 ohlcv：{sym} {tf} 共 {n} 行")
            except Exception as e:
                logging.error(f"[ohlcv] 入库失败：{sym} {tf} -> {e}")


def sync_symbols_ohlcv_ref(conn, cfg_root, mode: str = "increment") -> None:
    """
    把 config.yml -> csmvp.tests 的币按 1h/4h 同步到 ohlcv。
    口径：复用 candle_raw_extractor.fetch_ohlcv（去未收盘K、Asia/Shanghai）。
    """
    import logging
    symbols = _symbols_from_config(cfg_root)
    timeframes = (cfg_root.get("csmvp", {}) or {}).get("timeframes", ["1h", "4h"])

    fetch_params = (cfg_root.get("data_source", {}) or {}).get("fetch_params", {})
    if mode == "backfill":
        max_candles = int(fetch_params.get("max_candles_to_fetch", 240))
    else:
        max_candles = int(fetch_params.get("increment_candles", 120))
    limit = int(fetch_params.get("ohlcv_fetch_limit", 1000))

    for s in symbols:
        for tf in timeframes:
            logging.info(f"[ohlcv] {mode} 拉取 {s} {tf} max_candles={max_candles}")
            # 用参考程序的抓数口径（内部已做未收盘剔除与 +08:00）
            df = fetch_ohlcv(s, tf, limit=limit, max_candles=max_candles)
            if df is None or df.empty:
                logging.warning(f"[ohlcv] 无数据：{s} {tf}")
                continue
            dff = df[['open','high','low','close','volume']].copy()
            dff['t'] = dff.index.strftime("%Y-%m-%d %H:%M:%S")
            dff = dff[['t','open','high','low','close','volume']]
            n = upsert_ohlcv_df(conn, s, tf, dff)
            logging.info(f"[ohlcv] 写入 ohlcv：{s} {tf} 共 {n} 行")


# -------------------------
# 回填 BTC OHLCV（仅 BTC）
# -------------------------
def backfill_btc_ohlcv(conn: sqlite3.Connection, cfg_root: Dict[str, Any]) -> None:
    csmvp = cfg_root.get("csmvp", {})
    btc_aliases = csmvp.get("btc_symbols", ["BTC/USDT", "BTCUSDT", "BTC-USD"])
    timeframes = csmvp.get("timeframes", ["1h", "4h"])

    symbol_on_exchange = pick_listed_symbol(EXCHANGE, btc_aliases)
    if not symbol_on_exchange:
        logging.error(f"在交易所未找到可用的 BTC 符号，候选：{btc_aliases}")
        return

    fetch_params = cfg_root.get('data_source', {}).get('fetch_params', {})
    max_candles = int(fetch_params.get('max_candles_to_fetch', 240))
    limit = int(fetch_params.get('ohlcv_fetch_limit', 1000))

    for tf in timeframes:
        df = fetch_ohlcv(symbol_on_exchange, tf, limit=limit, max_candles=max_candles)
        if df is None or df.empty:
            logging.warning(f"[backfill] 无数据：{symbol_on_exchange} {tf}")
            continue

        # 关键信息：转成带 't' 列的 DataFrame 再 upsert
        dff = df[['open','high','low','close','volume']].copy()
        dff['t'] = df.index.strftime('%Y-%m-%d %H:%M:%S')
        dff = dff[['t','open','high','low','close','volume']]

        try:
            n = upsert_ohlcv_df(conn, symbol_on_exchange, tf, dff)
            logging.info(f"[backfill] 写入 ohlcv：{symbol_on_exchange} {tf} 共 {n} 行")
        except Exception as e:
            logging.error(f"[backfill] 入库失败：{symbol_on_exchange} {tf} -> {e}")



# -------------------------
# 增量补齐（只补最近若干根；这里直接拉一段并 upsert）
# -------------------------
def increment_btc_ohlcv(conn: sqlite3.Connection, cfg_root: Dict[str, Any]) -> None:
    csmvp = cfg_root.get("csmvp", {})
    btc_aliases = csmvp.get("btc_symbols", ["BTC/USDT", "BTCUSDT", "BTC-USD"])
    timeframes = csmvp.get("timeframes", ["1h", "4h"])

    symbol_on_exchange = pick_listed_symbol(EXCHANGE, btc_aliases)
    if not symbol_on_exchange:
        logging.error(f"在交易所未找到可用的 BTC 符号，候选：{btc_aliases}")
        return

    fetch_params = cfg_root.get('data_source', {}).get('fetch_params', {})
    max_candles = int(fetch_params.get('increment_candles', 120))
    limit = int(fetch_params.get('ohlcv_fetch_limit', 1000))

    for tf in timeframes:
        df = fetch_ohlcv(symbol_on_exchange, tf, limit=limit, max_candles=max_candles)
        if df is None or df.empty:
            logging.warning(f"[increment] 无数据：{symbol_on_exchange} {tf}")
            continue

        # 同步口径：带 't' 列后 upsert
        dff = df[['open','high','low','close','volume']].copy()
        dff['t'] = df.index.strftime('%Y-%m-%d %H:%M:%S')
        dff = dff[['t','open','high','low','close','volume']]

        try:
            n = upsert_ohlcv_df(conn, symbol_on_exchange, tf, dff)
            logging.info(f"[increment] 写入 ohlcv：{symbol_on_exchange} {tf} 共 {n} 行")
        except Exception as e:
            logging.error(f"[increment] 入库失败：{symbol_on_exchange} {tf} -> {e}")




def increment_ohlcv_once(conn, cfg_root):
    """仅写‘上一根闭合K’（1h/4h），统一复用 fetch_ohlcv 口径。"""
    import logging
    global CONFIG, EXCHANGE
    CONFIG = cfg_root or {}
    try:
        EXCHANGE = get_exchange_from_config(CONFIG)
    except Exception as e:
        logging.error(f"[ohlcv] init exchange error: {e}")
        return

    symbol = "BTC/USDT"
    for tf in ["1h", "4h"]:
        df = fetch_ohlcv(symbol, tf, limit=3, max_candles=3)
        if df is None or df.empty:
            logging.warning(f"[ohlcv] 无数据：{symbol} {tf}")
            continue
        last = df.iloc[-1:]
        dff = last[["open","high","low","close","volume"]].copy()
        dff["t"] = last.index.strftime("%Y-%m-%d %H:%M:%S")
        dff = dff[["t","open","high","low","close","volume"]]
        try:
            n = upsert_ohlcv_df(conn, symbol, tf, dff)
            logging.info(f"[ohlcv] increment once: {symbol} {tf} -> {n} 行")
        except Exception as e:
            logging.error(f"[ohlcv] 入库失败：{symbol} {tf} -> {e}")



# -------------------------
# 主入口
# -------------------------
def main():
    import argparse, os, sys, logging, sqlite3, yaml

    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="config.yml 路径")
    ap.add_argument("--mode", required=True,
                    choices=["backfill", "increment", "ohlcv", "funding"])
    ap.add_argument("--lookback", default="240")
    ap.add_argument("--log", default="INFO")
    args = ap.parse_args()

    # 读配置
    with open(args.config, "r", encoding="utf-8") as f:
        cfg_root = yaml.safe_load(f) or {}

    core_cfg = (cfg_root.get("core") or {})
    csmvp_cfg = (cfg_root.get("csmvp") or {})

    log_dir = core_cfg.get("log_dir") or csmvp_cfg.get("log_dir") or "/www/wwwroot/Crypto-Signal/app/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "data_sync_tasks.log")

    logging.basicConfig(
        level=getattr(logging, args.log),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # ★ DB 路径：先 core，再 csmvp，最后你的那条绝对路径
    db_path = (
        core_cfg.get("db_path")
        or csmvp_cfg.get("db_path")
        or "/www/wwwroot/Crypto-Signal/app/trading_signals_core.db"
    )

    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute("PRAGMA busy_timeout=60000;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception as e:
        logging.warning(f"设置 PRAGMA 失败: {e}")

    logging.info(f"DB => {db_path} | mode={args.mode}")

    try:
        if args.mode == "backfill":
            sync_symbols_ohlcv(conn, cfg_root, mode="backfill")
            hours = int(args.lookback)
            sync_btc_funding_snapshot(conn, cfg_root, mode="backfill", lookback_hours=hours)
            logging.info("backfill 完成")
        elif args.mode == "increment":
            sync_symbols_ohlcv(conn, cfg_root, mode="increment")
            sync_btc_funding_snapshot(conn, cfg_root, mode="increment")
            logging.info("increment 完成")
        elif args.mode == "ohlcv":
            sync_symbols_ohlcv(conn, cfg_root, mode="increment")
            logging.info("ohlcv 完成")
        elif args.mode == "funding":
            hours = int(args.lookback)
            sync_btc_funding_snapshot(conn, cfg_root, mode="backfill", lookback_hours=hours)
            logging.info("funding 完成")
    except Exception as e:
        logging.exception(f"运行失败: {e}")
        raise
    finally:
        try:
            conn.commit()
        except Exception:
            pass
        conn.close()



if __name__ == "__main__":
    main()


