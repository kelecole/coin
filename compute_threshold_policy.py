# core/compute_threshold_policy.py
# 最小可用：放量阈值 / 广度阈值 / (可选) max_stop_frac 写入 threshold_policy
from __future__ import annotations
import argparse, os, sqlite3, math, sys
from datetime import timedelta
from typing import Dict, List, Tuple, Optional

# 依赖：pandas / numpy / pyyaml 已在你的环境中常用
import numpy as np
import pandas as pd

try:
    import yaml
except Exception:
    yaml = None

# 统一时间口径
from .timebox import now_local_str, now_local_dt

# 优先使用你已有的 upsert；没有则用兜底
try:
    from .data_sync_tasks import upsert_threshold_policy as _upsert_threshold_policy
except Exception:
    def _upsert_threshold_policy(
        conn: sqlite3.Connection, *,
        name: str,
        timeframe: str,
        regime: str,
        value: float,
        scope: str = "global",
        symbol: str | None = None,
        q: float | None = None,
        lookback_days: int | None = None,
        sample_size: int | None = None,
        valid_from: str | None = None,
        valid_to: str | None = None
    ) -> int:
        """兜底版 upsert（与之前给你的实现一致）。"""
        cur = conn.cursor()
        now_ts = now_local_str("Asia/Shanghai")
        vf = valid_from or now_ts
        sym = (symbol or "").strip()
        sql = """
        INSERT INTO threshold_policy
          (name, timeframe, regime, scope, symbol, value, q, lookback_days, sample_size, valid_from, valid_to, updated_at)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name, timeframe, regime, scope, symbol)
        DO UPDATE SET
          value=excluded.value,
          q=excluded.q,
          lookback_days=excluded.lookback_days,
          sample_size=excluded.sample_size,
          valid_from=excluded.valid_from,
          valid_to=excluded.valid_to,
          updated_at=excluded.updated_at
        """
        cur.execute(sql, (name, timeframe, regime, scope, sym, float(value),
                          q, lookback_days, sample_size, vf, valid_to, now_ts))
        conn.commit()
        return cur.rowcount


def log(*a): print("[policy]", *a)

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return bool(row)

def load_cfg(path: Optional[str]) -> dict:
    if not path or not yaml:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        log(f"load_cfg failed: {e}")
        return {}

def get_tz(cfg: dict) -> str:
    return (cfg.get("tz")
            or cfg.get("csmvp", {}).get("tz")
            or cfg.get("core", {}).get("tz")
            or "Asia/Shanghai")

# ---------- 计算：放量阈值 ----------
def compute_volume_spike_thresholds(
    conn: sqlite3.Connection,
    *, timeframe_list: List[str],
    start_ts: str,
    lookback_N: int,
    q: float
) -> Dict[str, Tuple[float, int]]:
    """
    返回 {tf: (thr_value, sample_size)}
    vol_spike = vol[t] / avg(vol[t-N:t-1])
    """
    out: Dict[str, Tuple[float, int]] = {}
    for tf in timeframe_list:
        sql = """
        SELECT symbol, t, volume
        FROM ohlcv
        WHERE timeframe=? AND t>=?
        ORDER BY symbol, t
        """
        df = pd.read_sql_query(sql, conn, params=(tf, start_ts))
        if df.empty:
            log(f"volume: no data for tf={tf}")
            out[tf] = (np.nan, 0)
            continue

        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df = df.dropna(subset=["volume"])
        if df.empty:
            out[tf] = (np.nan, 0); continue

        df["t"] = pd.to_datetime(df["t"])
        df = df.sort_values(["symbol", "t"]).reset_index(drop=True)

        # 计算 N 期均量（不含当前 bar）
        df["vol_avg_N"] = (df.groupby("symbol")["volume"]
                             .rolling(lookback_N, min_periods=lookback_N)
                             .mean()
                             .shift(1)
                             .reset_index(level=0, drop=True))
        df["vol_spike"] = df["volume"] / df["vol_avg_N"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["vol_spike"])

        # 去极端：自适应 winsorize（上 99.5% 截断）
        if not df.empty:
            cap = df["vol_spike"].quantile(0.995)
            df.loc[df["vol_spike"] > cap, "vol_spike"] = cap

        if df.empty:
            out[tf] = (np.nan, 0)
        else:
            thr = float(df["vol_spike"].quantile(q))
            out[tf] = (thr, int(df.shape[0]))
            log(f"volume tf={tf} q={q:.3f} -> thr={thr:.4f} samples={df.shape[0]}")
    return out

# ---------- 计算：广度阈值 ----------
def compute_breadth_thresholds(
    conn: sqlite3.Connection,
    *, timeframe_list: List[str],
    start_ts: str,
    q_long: float,
    q_short: float
) -> Optional[Dict[str, Tuple[float, float, int]]]:
    """
    返回 {tf: (breadth_long_min, breadth_short_max, sample_size)} 或 None
    更宽容的列名识别：
      1) 直接比例列：breadth / breadth_ratio / pct_above_ma / pct_above_sma / pct_above
      2) 计数列：adv/dec | advancers/decliners | advancing/declining | adv_count/dec_count
         -> breadth = adv / (adv + dec)
    """
    def _tbl_exists(c: sqlite3.Connection, t: str) -> bool:
        return bool(c.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)
        ).fetchone())

    if not _tbl_exists(conn, "breadth_snapshot"):
        log("breadth_snapshot not found; skip breadth thresholds.")
        return None

    df = pd.read_sql_query(
        "SELECT * FROM breadth_snapshot WHERE t_ref>=? ORDER BY t_ref",
        conn, params=(start_ts,)
    )
    if df.empty:
        log("breadth_snapshot empty in lookback.")
        return None

    cols = {c.lower(): c for c in df.columns}

    # 1) 直接比例列
    direct_candidates = [
        "breadth", "breadth_ratio",
        "pct_above_ma", "pct_above_sma", "pct_above"
    ]
    breadth_series = None
    for key in direct_candidates:
        if key in cols:
            s = pd.to_numeric(df[cols[key]], errors="coerce")
            breadth_series = s
            break

    # 2) 计数列
    if breadth_series is None:
        # 可能的多种命名
        adv_candidates = ["adv", "advancers", "advancing", "adv_count", "n_adv", "num_adv"]
        dec_candidates = ["dec", "decliners", "declining", "dec_count", "n_dec", "num_dec"]

        adv_col = next((cols[k] for k in adv_candidates if k in cols), None)
        dec_col = next((cols[k] for k in dec_candidates if k in cols), None)

        if adv_col and dec_col:
            adv = pd.to_numeric(df[adv_col], errors="coerce")
            dec = pd.to_numeric(df[dec_col], errors="coerce")
            total = adv.add(dec, fill_value=0.0)
            with np.errstate(divide='ignore', invalid='ignore'):
                breadth_series = (adv / total).clip(0, 1)
        else:
            log("breadth_snapshot schema not recognized (no ratio or adv/dec counts); skip.")
            return None

    # 清洗
    s = breadth_series.replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        log("breadth: no valid numeric values; skip.")
        return None

    long_min = float(s.quantile(q_long))
    short_max = float(s.quantile(q_short))
    n = int(s.shape[0])
    log(f"breadth q_long={q_long:.2f}->{long_min:.3f}, q_short={q_short:.2f}->{short_max:.3f}, n={n}")

    return {tf: (long_min, short_max, n) for tf in timeframe_list}


# ---------- （可选）max_stop_frac：基于 ATR/Close 分位 + 安全余量 ----------
def compute_max_stop_frac(
    conn: sqlite3.Connection,
    *, timeframe_list: List[str],
    start_ts: str,
    atr_n: int,
    stop_mult: float,
    q_stop: float,
    safety_pad: float = 1.10
) -> Dict[str, Tuple[float, int]]:
    """
    估算 max_stop_frac ≈ stop_mult * ATR_n / Close 的分位 * 安全余量
    返回 {tf: (max_stop_frac, sample_size)}
    """
    out: Dict[str, Tuple[float, int]] = {}
    for tf in timeframe_list:
        df = pd.read_sql_query(
            "SELECT symbol, t, open, high, low, close FROM ohlcv WHERE timeframe=? AND t>=? ORDER BY symbol, t",
            conn, params=(tf, start_ts)
        )
        if df.empty:
            out[tf] = (np.nan, 0); continue
        df = df.dropna(subset=["high", "low", "close"])
        df["t"] = pd.to_datetime(df["t"])
        df = df.sort_values(["symbol", "t"]).reset_index(drop=True)

        def _atr(group: pd.DataFrame) -> pd.Series:
            h, l, c = group["high"].values, group["low"].values, group["close"].values
            prev_c = np.r_[np.nan, c[:-1]]
            tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(prev_c - l)))
            atr = pd.Series(tr).rolling(atr_n, min_periods=atr_n).mean()
            return atr

        df["ATR"] = df.groupby("symbol", group_keys=False).apply(_atr).reset_index(drop=True)
        df["stop_frac_est"] = (stop_mult * df["ATR"]) / df["close"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["stop_frac_est"])
        if df.empty:
            out[tf] = (np.nan, 0); continue
        max_stop = float(df["stop_frac_est"].quantile(q_stop) * safety_pad)
        out[tf] = (max_stop, int(df.shape[0]))
        log(f"stop_frac tf={tf} q={q_stop:.2f} -> {max_stop:.4f} (pad x{safety_pad:.2f}) n={df.shape[0]}")
    return out

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Compute and write dynamic thresholds to threshold_policy.")
    ap.add_argument("--config", type=str, default="/www/wwwroot/Crypto-Signal/config.yml")
    ap.add_argument("--db", type=str, default="/www/wwwroot/Crypto-Signal/app/trading_signals_core.db")
    ap.add_argument("--timeframes", type=str, default="1h,4h", help="comma-separated, e.g. 1h,4h,1d")
    ap.add_argument("--lookback-days", type=int, default=180)
    ap.add_argument("--vol-lookback-n", type=int, default=20, help="rolling N for average volume")
    ap.add_argument("--q-vol", type=float, default=0.80, help="quantile for volume_spike threshold")
    ap.add_argument("--q-breadth-long", type=float, default=0.50)
    ap.add_argument("--q-breadth-short", type=float, default=0.40)
    ap.add_argument("--compute-stop", action="store_true", help="also compute max_stop_frac")
    ap.add_argument("--atr-n", type=int, default=14)
    ap.add_argument("--stop-mult", type=float, default=1.5)
    ap.add_argument("--q-stop", type=float, default=0.85)
    ap.add_argument("--weekly-only", action="store_true", help="only write on weekly schedule (Mon 05:00±10m)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    tz = get_tz(cfg)
    now_ts = now_local_dt(tz)
    start_dt = now_ts - timedelta(days=int(args.lookback_days))
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")

    if args.weekly_only:
        # 周一 05:00 ± 10min 才执行，其余直接退出（幂等、方便你串在现有 cron 后）
        in_window = (now_ts.weekday() == 0) and (now_ts.hour == 5) and (0 <= now_ts.minute <= 10)
        if not in_window:
            log(f"weekly-only guard: now={now_ts} not in Mon 05:00±10m, exit.")
            return 0

    tf_list = [s.strip() for s in args.timeframes.split(",") if s.strip()]
    assert tf_list, "timeframes empty."

    assert os.path.exists(args.db), f"DB not found: {args.db}"
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    # 放量阈值
    vol_map = compute_volume_spike_thresholds(
        conn, timeframe_list=tf_list, start_ts=start_ts,
        lookback_N=int(args.vol_lookback_n), q=float(args.q_vol)
    )

    # 广度阈值（可选）
    br_map = compute_breadth_thresholds(
        conn, timeframe_list=tf_list, start_ts=start_ts,
        q_long=float(args.q_breadth_long), q_short=float(args.q_breadth_short)
    )

    # 可选：stop 分位
    stop_map = {}
    if args.compute_stop:
        stop_map = compute_max_stop_frac(
            conn, timeframe_list=tf_list, start_ts=start_ts,
            atr_n=int(args.atr_n), stop_mult=float(args.stop_mult),
            q_stop=float(args.q_stop)
        )

    # 写表
    total_writes = 0
    if args.dry_run:
        log("[dry-run] thresholds computed. Skip writing.")
    else:
        vf = now_ts.strftime("%Y-%m-%d %H:%M:%S")
        # regime 用 'any'（层1最小实现）；后续可按 regime 切片分别写
        regime = "any"

        for tf, (thr, n) in vol_map.items():
            if not (thr > 0 and math.isfinite(thr) and n > 0):
                continue
            total_writes += _upsert_threshold_policy(
                conn, name="volume_spike_thr", timeframe=tf, regime=regime,
                value=float(thr), q=float(args.q_vol),
                lookback_days=int(args.lookback_days), sample_size=int(n),
                valid_from=vf, valid_to=None
            )

        if br_map:
            for tf, (long_min, short_max, n) in br_map.items():
                if long_min > 0:
                    total_writes += _upsert_threshold_policy(
                        conn, name="breadth_long_min", timeframe=tf, regime=regime,
                        value=float(long_min), q=float(args.q_breadth_long),
                        lookback_days=int(args.lookback_days), sample_size=int(n),
                        valid_from=vf, valid_to=None
                    )
                if short_max > 0:
                    total_writes += _upsert_threshold_policy(
                        conn, name="breadth_short_max", timeframe=tf, regime=regime,
                        value=float(short_max), q=float(args.q_breadth_short),
                        lookback_days=int(args.lookback_days), sample_size=int(n),
                        valid_from=vf, valid_to=None
                    )

        if stop_map:
            for tf, (mx, n) in stop_map.items():
                if mx > 0 and math.isfinite(mx) and n > 0:
                    total_writes += _upsert_threshold_policy(
                        conn, name="max_stop_frac", timeframe=tf, regime=regime,
                        value=float(mx), q=float(args.q_stop),
                        lookback_days=int(args.lookback_days), sample_size=int(n),
                        valid_from=vf, valid_to=None
                    )

    conn.close()
    log(f"done. writes={total_writes}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
