# -*- coding: utf-8 -*-
"""
export_trade_journey_plus.py  (drop-in 替换/并行使用均可)

用法（默认配置即可，不用选时间）：
  python export_trade_journey_plus.py
    - 默认 DB: /www/wwwroot/Crypto-Signal/app/trading_signals_core.db
    - 默认导出最近 48 小时（opened_at >= now-48h）
    - 默认输出到：<DB同目录>/excel/trade_journey_plus_<ts>.xlsx

可选参数：
  --db /path/to/trading_signals_core.db
  --out /path/to/out.xlsx
  --last_hours 72
  --pre 250 --post_hours 24 --atr_n 14 --tsl_k 2.5 --tsl_base Hc
  --tz Asia/Shanghai

新增/增强点：
- positions_summary_plus：合并 signal(同向优先/回退)、risk_cost/liquidity/correlation/btc_alignment、并回填 entry_ATR、路径R轨迹与 MFE/MAE。
- signals_recent：窗口内信号（含 source/score/prob/strength/detail_json），用于“信号→决策”溯源。
- bucket_decisions（原样） + rejections（仅拒单） + exit_actions（如表存在）。
- decision_evidence / decision_outcome（如表存在）。
- paths / paths_features：逐bar导出 close / R_now_path / MFE_R_path / MAE_R_path。
- gate_summary：按主拒因聚合（risk_cost_* / liquidity_* / btc_align_* / volume_spike_* 等）。
- schema_counts：库内每张表的行数与最近时间戳，帮助识别“大库/小表”的稀疏情况。

只做导出增补，不改库结构；缺表自动跳过。
"""

import os, sys, json, argparse, sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 你的工程内已经有 timebox 模块；无则可用本地兜底
try:
    from timebox import now_local_str, now_local_dt
except Exception:
    from datetime import timezone
    def now_local_dt(tz_name="Asia/Shanghai"):
        return datetime.now()
    def now_local_str(tz_name="Asia/Shanghai"):
        return now_local_dt(tz_name).strftime("%Y-%m-%d %H:%M:%S")

DEFAULT_DB = "/www/wwwroot/Crypto-Signal/app/trading_signals_core.db"

# ----------------------- 工具函数 -----------------------
def log(msg, tz: str = "Asia/Shanghai"):
    ts = now_local_str(tz)
    print(f"[export] {ts} {msg}")

def q(conn, sql, params=()):
    conn.row_factory = sqlite3.Row
    return pd.read_sql_query(sql, conn, params=params)

def table_exists(conn, name):
    return q(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).shape[0] > 0

def ensure_datetime(s):
    return pd.to_datetime(s, errors="coerce")

def calc_atr(df, n=14):
    """Wilder ATR：TR 的 RMA（用 EMA 近似）"""
    if df.empty:
        return df.assign(ATR=np.nan)
    d = df.copy()
    d["prev_close"] = d["close"].shift(1)
    d["tr1"] = d["high"] - d["low"]
    d["tr2"] = (d["high"] - d["prev_close"]).abs()
    d["tr3"] = (d["low"] - d["prev_close"]).abs()
    d["TR"]  = d[["tr1","tr2","tr3"]].max(axis=1)
    d["ATR"] = d["TR"].ewm(alpha=1/n, adjust=False).mean()
    return d

def chandelier_tsl(df, n=14, k=2.5, base="Hc", direction="long"):
    d = df.copy()
    d = calc_atr(d, n=n)
    if str(direction).lower() == "long":
        d["base_line"] = (d["close"] if base=="Hc" else d["high"]).rolling(n, min_periods=1).max()
        d["tsl_sim"]   = d["base_line"] - k * d["ATR"]
    else:
        d["base_line"] = (d["close"] if base=="Hc" else d["low"]).rolling(n, min_periods=1).min()
        d["tsl_sim"]   = d["base_line"] + k * d["ATR"]
    return d

def safe_merge(left, right, on, how="left", prefix="r_"):
    if right is None or right.empty:
        return left
    dup_cols = [c for c in right.columns if c in left.columns and c not in on]
    r = right.rename(columns={c: f"{prefix}{c}" for c in dup_cols})
    return left.merge(r, on=on, how=how)

def parse_reasons_json(s):
    try:
        arr = json.loads(s) if isinstance(s, str) else (s if s is not None else [])
        if not isinstance(arr, list):
            return [], None, None
        reasons = [str(x) for x in arr]
        primary = next((r.split("=",1)[1] for r in reasons if r.startswith("reason=")), None)
        verdict = "approved" if "approved" in reasons else ("rejected" if "rejected" in reasons else None)
        return reasons, primary, verdict
    except Exception:
        return [], None, None

def unique_on(df, keys, order_cols):
    if df is None or df.empty:
        return df
    cols = [c for c in order_cols if c in df.columns]
    if cols:
        df = df.sort_values(cols)
    return df.drop_duplicates(subset=[k for k in keys if k in df.columns], keep="last")

def table_counts(conn):
    # 所有普通表
    t = q(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
    out = []
    for name in t["name"].tolist():
        try:
            cnt = q(conn, f"SELECT COUNT(1) AS n FROM '{name}'")["n"].iloc[0]
        except Exception:
            cnt = None
        last_t = None
        for k in ["t","t_ref","opened_at","created_at","closed_at","timestamp","time","dt"]:
            if k in q(conn, f"PRAGMA table_info('{name}')")["name"].tolist():
                try:
                    last_t = q(conn, f"SELECT MAX({k}) AS last FROM '{name}'")["last"].iloc[0]
                    break
                except Exception:
                    pass
        out.append({"table": name, "rows": cnt, "last_ts_col": (k if last_t is not None else ""), "last_ts": last_t})
    return pd.DataFrame(out).sort_values(["rows","table"], ascending=[False, True])

# ----------------------- 导出主流程 -----------------------
def build_export(args):
    assert os.path.exists(args.db), f"数据库不存在: {args.db}"
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    # 统一时区口径（读 config.yml 失败则回退 Asia/Shanghai）
    tz_name = args.tz or "Asia/Shanghai"

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    required = ["positions_virtual","decision_snapshot","signals","ohlcv"]
    for t in required:
        assert table_exists(conn, t), f"缺少核心表: {t}"

    optional_names = [
        "risk_cost_snapshot","correlation_snapshot","liquidity_snapshot",
        "btc_alignment_snapshot","exit_log","decision_evidence","decision_outcome",
        "orders","orders_filled","positions_physical"
    ]
    optional = {t: table_exists(conn, t) for t in optional_names}
    log(f"可选表: {optional}")

    # ---- 时间窗口 ----
    where_time, params = "", []
    if getattr(args, "start", None):
        where_time += " AND pv.opened_at >= ?"; params.append(args.start)
    if getattr(args, "end", None):
        where_time += " AND pv.opened_at <= ?"; params.append(args.end)
    if (not getattr(args,"start",None)) and (not getattr(args,"end",None)) and getattr(args,"last_hours",None):
        start_dt = now_local_dt(tz_name) - timedelta(hours=args.last_hours)
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        where_time += " AND pv.opened_at >= ?"; params.append(start_str)
        log(f"时间窗口: 最近 {args.last_hours} 小时 (opened_at >= {start_str})")

    # ---- positions ----
    pos = q(conn, f"""
    SELECT pv.id AS position_id, pv.t_ref, pv.symbol, pv.timeframe, pv.direction, pv.status,
           pv.opened_at, pv.closed_at, pv.entry_price, pv.exit_price, pv.qty, pv.notional_usd,
           pv.pnl_usd, pv.exit_reason, pv.mfe_R,
           pv.stop_price, pv.be_price, pv.tsl_price, pv.tp_stage, pv.be_armed,
           pv.strategy_version,
           pv.r0_px_dist, pv.r0_R_usd
    FROM positions_virtual pv
    WHERE 1=1 {where_time}
    ORDER BY pv.opened_at ASC
    """, tuple(params))

    pos["opened_at_dt"] = ensure_datetime(pos["opened_at"])
    pos["closed_at_dt"] = ensure_datetime(pos["closed_at"])
    pos["holding_minutes"] = (pos["closed_at_dt"] - pos["opened_at_dt"]).dt.total_seconds() / 60.0

    # ---- decisions（每 key 取最新） ----
    dec = q(conn, """
    SELECT ds.*
    FROM decision_snapshot ds
    JOIN (
      SELECT t_ref, symbol, timeframe, MAX(id) AS max_id
      FROM decision_snapshot
      GROUP BY t_ref, symbol, timeframe
    ) z ON z.max_id = ds.id
    """)
    if "reasons_json" in dec.columns:
        tmp = dec["reasons_json"].apply(parse_reasons_json)
        dec["reason_list"]   = tmp.apply(lambda x: x[0])
        dec["primary_reason"]= tmp.apply(lambda x: x[1])
        dec["verdict"]       = tmp.apply(lambda x: x[2])

    # ---- signals（窗口内） ----
    sig = q(conn, "SELECT id, t_ref, symbol, timeframe, source, direction, score, prob, strength, detail_json, created_at FROM signals")
    sig["_key"] = sig["t_ref"] + "|" + sig["symbol"] + "|" + sig["timeframe"]
    pos["_key"] = pos["t_ref"] + "|" + pos["symbol"] + "|" + pos["timeframe"]
    sig["created_at"] = ensure_datetime(sig["created_at"])

    # 仅导出窗口内的信号，若无 start/end 则按 last_hours
    if params:
        # 用 opened_at 窗口近似筛信号（也可扩展为 t_ref/created_at 双窗口）
        t_min = pos["opened_at_dt"].min()
        t_max = pos["opened_at_dt"].max() if pos["opened_at_dt"].notna().any() else None
        if pd.notna(t_min):
            sig = sig[sig["created_at"] >= (t_min - pd.Timedelta(hours=24))]
        if pd.notna(t_max):
            sig = sig[sig["created_at"] <= (t_max + pd.Timedelta(hours=24))]

    # 同向优先 + 回退 合并到 positions
    same_dir = (sig.sort_values(["_key","direction","created_at"], ascending=[True, True, False])
                  .drop_duplicates(subset=["_key","direction"], keep="first"))
    m0 = pos.merge(same_dir.add_prefix("sd_"),
                   left_on=["_key","direction"],
                   right_on=["sd__key","sd_direction"],
                   how="left")
    fallback = (sig.sort_values(["_key","created_at"], ascending=[True, False])
                  .drop_duplicates(subset=["_key"], keep="first")
                  .add_prefix("fb_"))
    m1 = m0.merge(fallback, left_on="_key", right_on="fb__key", how="left")
    for col in ["id","source","direction","score","prob","strength","detail_json","created_at"]:
        m1[f"signal_{col}"] = m1[f"sd_{col}"].where(m1[f"sd_{col}"].notna(), m1[f"fb_{col}"])

    # ---- gate snapshots 合并（唯一化后 merge） ----
    rc   = q(conn, "SELECT * FROM risk_cost_snapshot")         if table_exists(conn,"risk_cost_snapshot")       else None
    corr = q(conn, "SELECT * FROM correlation_snapshot")       if table_exists(conn,"correlation_snapshot")      else None
    liq  = q(conn, "SELECT * FROM liquidity_snapshot")         if table_exists(conn,"liquidity_snapshot")        else None
    btc  = q(conn, "SELECT * FROM btc_alignment_snapshot")     if table_exists(conn,"btc_alignment_snapshot")    else None

    m2 = m1
    if rc is not None and not rc.empty:
        rc_u = unique_on(rc, keys=["t_ref","symbol","timeframe"], order_cols=["t_ref","symbol","timeframe","id"])
        m2 = safe_merge(m2, rc_u, on=["t_ref","symbol","timeframe"], how="left", prefix="rc_")
    if corr is not None and not corr.empty:
        corr_u = unique_on(corr, keys=["t_ref","symbol","timeframe"], order_cols=["t_ref","symbol","timeframe","id"])
        m2 = safe_merge(m2, corr_u, on=["t_ref","symbol","timeframe"], how="left", prefix="corr_")
    if liq is not None and not liq.empty:
        if "timeframe" in liq.columns:
            liq_u = unique_on(liq, keys=["t_ref","symbol","timeframe"], order_cols=["t_ref","symbol","timeframe","id"])
            m2 = safe_merge(m2, liq_u, on=["t_ref","symbol","timeframe"], how="left", prefix="liq_")
        else:
            liq_u = unique_on(liq, keys=["t_ref","symbol"], order_cols=["t_ref","symbol","id"])
            m2 = safe_merge(m2, liq_u, on=["t_ref","symbol"], how="left", prefix="liq_")
    if btc is not None and not btc.empty:
        if "is_global" in btc.columns:
            btc_g = btc[btc["is_global"] == 1].copy()
            if btc_g.empty:
                btc_g = btc.copy()
        else:
            btc_g = unique_on(btc, keys=["t_ref"], order_cols=["t_ref","id"])
        btc_u = unique_on(btc_g, keys=["t_ref"], order_cols=["t_ref","id"])
        m2 = safe_merge(m2, btc_u, on=["t_ref"], how="left", prefix="btc_")

    # ---- position 维度去重 ----
    if "position_id" in m2.columns:
        m3 = (m2.sort_values(["position_id"]).drop_duplicates(subset=["position_id"], keep="last")).reset_index(drop=True)
    else:
        m3 = m2.reset_index(drop=True)

    # ---- 计算 entry ATR + 路径R轨迹 ----
    end_latest = q(conn, "SELECT MAX(t) AS max_t FROM ohlcv;")["max_t"].iloc[0]
    end_latest_dt = ensure_datetime(end_latest) if end_latest else now_local_dt(tz_name)

    paths_rows, bar_features_rows = [], []
    for i, r in m3.iterrows():
        sym, tf = r["symbol"], r["timeframe"]
        if pd.isna(r["opened_at_dt"]):
            continue
        rows_pre = q(conn, """
            SELECT symbol, timeframe, t, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol=? AND timeframe=? AND t<=?
            ORDER BY t DESC
            LIMIT ?
        """, (sym, tf, r["opened_at"], args.pre))
        rows_pre = rows_pre.sort_values("t", ascending=True).reset_index(drop=True)
        rows_pre["t_dt"] = ensure_datetime(rows_pre["t"])
        if rows_pre.empty:
            continue
        sim = chandelier_tsl(rows_pre.rename(columns=str),
                             n=args.atr_n, k=args.tsl_k, base=args.tsl_base,
                             direction=str(r["direction"]).lower())
        idx = sim.index[sim["t"] <= r["opened_at"]].max() if (sim["t"] <= r["opened_at"]).any() else None
        entry_atr = float(sim.loc[idx, "ATR"]) if idx is not None else np.nan
        m3.loc[i, "entry_atr14"] = entry_atr
        m3.loc[i, "R_unit"] = entry_atr

        t_end = r["closed_at_dt"] if pd.notna(r["closed_at_dt"]) else end_latest_dt
        t_end_ext = (t_end + timedelta(hours=args.post_hours)).strftime("%Y-%m-%d %H:%M:%S")
        rows_fwd = q(conn, """
            SELECT symbol, timeframe, t, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol=? AND timeframe=? AND t>=? AND t<=?
            ORDER BY t ASC
        """, (sym, tf, r["opened_at"], t_end_ext))
        if rows_fwd.empty:
            continue
        rows_fwd.insert(0, "position_id", r["position_id"])
        paths_rows.append(rows_fwd)

        entry_px = r["entry_price"]

        # 优先锁定分母：r0_px_dist；缺失/无效时回退到 entry_atr（与旧导出口径兼容）
        r0 = r["r0_px_dist"] if "r0_px_dist" in r.index else np.nan
        den = np.nan
        if pd.notna(r0):
            try:
                if float(r0) > 0:
                    den = float(r0)
            except Exception:
                den = np.nan
        if pd.isna(den) or den == 0:
            den = entry_atr if (entry_atr and not np.isnan(entry_atr) and entry_atr != 0) else np.nan

        def r_now_locked(px, entry, den, direction):
            if pd.isna(den) or den == 0 or pd.isna(px) or pd.isna(entry):
                return np.nan
            return (entry - px)/den if str(direction).lower()=="short" else (px - entry)/den

        rn = rows_fwd["close"].apply(lambda px: r_now_locked(px, entry_px, den, r["direction"]))
        rows_fwd["R_now_path"] = rn
        rows_fwd["MFE_R_path"] = rn.cummax()
        rows_fwd["MAE_R_path"] = rn.cummin()


        mfe_idx = rows_fwd["MFE_R_path"].idxmax() if rows_fwd["MFE_R_path"].notna().any() else None
        mae_idx = rows_fwd["MAE_R_path"].idxmin() if rows_fwd["MAE_R_path"].notna().any() else None
        if mfe_idx is not None:
            m3.loc[i, "mfe_R_recomputed"] = float(rows_fwd.loc[mfe_idx, "MFE_R_path"])
            m3.loc[i, "time_to_mfe_min"]  = (ensure_datetime(rows_fwd.loc[mfe_idx, "t"]) - r["opened_at_dt"]).total_seconds()/60.0
        if mae_idx is not None:
            m3.loc[i, "mae_R_recomputed"] = float(rows_fwd.loc[mae_idx, "MAE_R_path"])
            m3.loc[i, "time_to_mae_min"]  = (ensure_datetime(rows_fwd.loc[mae_idx, "t"]) - r["opened_at_dt"]).total_seconds()/60.0

        bar_features_rows.append(rows_fwd[["position_id","t","close","R_now_path","MFE_R_path","MAE_R_path"]])

    df_paths = pd.concat(paths_rows, ignore_index=True) if paths_rows else pd.DataFrame(
        columns=["position_id","symbol","timeframe","t","open","high","low","close","volume"]
    )
    df_path_feats = pd.concat(bar_features_rows, ignore_index=True) if bar_features_rows else pd.DataFrame(
        columns=["position_id","t","close","R_now_path","MFE_R_path","MAE_R_path"]
    )

    # ---- 拒单清单 ----
    rej = dec.copy()
    if "final" in rej.columns:
        rej = rej[rej["final"].astype(str).str.lower().isin(["skip","rejected"])]
    elif "verdict" in rej.columns:
        rej = rej[rej["verdict"]=="rejected"]

    # ---- 退出动作流水 ----
    exit_actions = q(conn, "SELECT * FROM exit_log ORDER BY position_id, t_ref;") if table_exists(conn,"exit_log") else \
                   pd.DataFrame(columns=["position_id","t_ref","action","qty","price","note"])

    # ---- 其它审计表（如存在则直出） ----
    extra_frames = {}
    for t in ["decision_evidence","decision_outcome"]:
        if table_exists(conn, t):
            extra_frames[t] = q(conn, f"SELECT * FROM {t}")

    # ---- gate_summary（按主拒因聚合） ----
    gate_summary = pd.DataFrame(columns=["primary_reason","n"])
    if "primary_reason" in dec.columns:
        gate_summary = (dec[~dec["primary_reason"].isna()]
                        .groupby("primary_reason").size().sort_values(ascending=False)
                        .rename("n").reset_index())

    # ---- signals_recent：窗口内信号（便于“信号→决策”溯源） ----
    signals_recent = sig.sort_values(["created_at"]).reset_index(drop=True)

    # ---- schema_counts：每张表的行数与最近时间戳 ----
    schema_count_df = table_counts(conn)

    # ---- meta ----
    gen_at = now_local_str(tz_name)
    meta = pd.DataFrame([{
        "generated_at": gen_at,
        "db_path": args.db,
        "last_hours": (args.last_hours if (not getattr(args,"start",None) and not getattr(args,"end",None)) else ""),
        "start": getattr(args,"start",None) or "",
        "end": getattr(args,"end",None) or "",
        "pre": args.pre, "post_hours": args.post_hours,
        "atr_n": args.atr_n, "tsl_k": args.tsl_k, "tsl_base": args.tsl_base,
        "tz": tz_name,
    }])

    # ---- 导出 Excel ----
    with pd.ExcelWriter(args.out, engine="xlsxwriter") as writer:
        meta.to_excel(writer, sheet_name="meta", index=False)
        m3.to_excel(writer, sheet_name="positions_summary_plus", index=False)

        # 方便你切分观察
        if "status" in m3.columns:
            m3[m3["status"]=="OPEN"].to_excel(writer, sheet_name="positions_open", index=False)
            m3[m3["status"]=="CLOSED"].to_excel(writer, sheet_name="positions_closed", index=False)

        dec.to_excel(writer, sheet_name="bucket_decisions", index=False)
        rej.to_excel(writer, sheet_name="rejections", index=False)
        exit_actions.to_excel(writer, sheet_name="exit_actions", index=False)
        df_paths.to_excel(writer, sheet_name="paths", index=False)
        df_path_feats.to_excel(writer, sheet_name="paths_features", index=False)
        signals_recent.to_excel(writer, sheet_name="signals_recent", index=False)
        gate_summary.to_excel(writer, sheet_name="gate_summary", index=False)
        schema_count_df.to_excel(writer, sheet_name="schema_counts", index=False)

        for name, frame in extra_frames.items():
            # Excel sheet name ≤31 字符
            frame.to_excel(writer, sheet_name=name[:31], index=False)

    log(f"OK -> {args.out}")
    return args.out

def main():
    parser = argparse.ArgumentParser(description="导出 AI 训练友好的交易旅程 Excel（增强版）")
    parser.add_argument("--db", required=False, default=DEFAULT_DB)
    parser.add_argument("--out", required=False, default=None)
    parser.add_argument("--start", required=False, default=None)
    parser.add_argument("--end", required=False, default=None)
    parser.add_argument("--last_hours", type=int, default=48)
    parser.add_argument("--pre", type=int, default=250)
    parser.add_argument("--post_hours", type=int, default=24)
    parser.add_argument("--atr_n", type=int, default=14)
    parser.add_argument("--tsl_k", type=float, default=2.5)
    parser.add_argument("--tsl_base", choices=["Hc","H"], default="Hc")
    parser.add_argument("--tz", default="Asia/Shanghai")

    args = parser.parse_args()
    if not args.out:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.join(os.path.dirname(args.db) or ".", "excel")
        os.makedirs(base_dir, exist_ok=True)
        args.out = os.path.join(base_dir, f"trade_journey_plus_{ts}.xlsx")

    try:
        out = build_export(args)
        print(out)
    except Exception as e:
        log(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
