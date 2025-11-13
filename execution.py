# -*- coding: utf-8 -*-
"""
core/execution.py

把 decision_engine.build_trade_plan(...) 产出的 approved 列表
正式落到 SQLite 的 positions_virtual 表，作为影子持仓记录。

现在版本：适配你现有 DB 的真实表结构，并扩展支持审计列
(strategy_version, setup_tag, entry_thesis, validation_level, risk_R_alloc)。

这允许我们后面：
- 回撤归因到具体 strategy_version
- 复盘哪种 setup / entry_thesis 在送钱或在杀人
- 用 validation_level 做 setup 失效止损
- 用 risk_R_alloc 来衡量风控敞口

我们不下真单，只是记账。
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Any, List
# [已移除] from datetime import datetime
from .timebox import now_local_str # [新增] 统一时间管理

from . import market_data


def now_db_str(cfg=None):
    """
    [修改]
    获取统一的数据库时间字符串 (YYYY-MM-DD HH:MM:SS)
    """
    tz = (cfg or {}).get("tz","Asia/Shanghai") if isinstance(cfg, dict) else "Asia/Shanghai"
    return now_local_str(tz)


def _normalize_direction(direction: str) -> str:
    d = (direction or "").strip().lower()
    if d in ("long", "short"):
        return d
    return ""

def _ensure_positions_virtual_schema(conn: sqlite3.Connection) -> None:
    """
    幂等：确保 positions_virtual 必要列存在。只做 ADD COLUMN，不改你已有逻辑。
    """
    cur = conn.execute("PRAGMA table_info(positions_virtual)")
    cols = {r[1] for r in cur.fetchall()}

    def _ensure_col(name: str, ddl_type: str, default_clause: str = None):
        if name not in cols:
            sql = f"ALTER TABLE positions_virtual ADD COLUMN {name} {ddl_type}"
            if default_clause:
                sql += f" {default_clause}"
            conn.execute(sql)
            cols.add(name)

    # —— 你已有的列（保持原样）——
    _ensure_col("entry_atr",       "REAL")
    _ensure_col("base_R_usd",      "REAL")
    _ensure_col("risk_R",          "REAL")
    _ensure_col("stop_price",      "REAL")
    _ensure_col("be_price",        "REAL")
    _ensure_col("be_armed",        "INTEGER", "NOT NULL DEFAULT 0")
    _ensure_col("tsl_price",       "REAL")
    _ensure_col("tsl_method",      "TEXT")
    _ensure_col("tsl_k",           "REAL")
    _ensure_col("tsl_armed_at_tp", "INTEGER", "NOT NULL DEFAULT 0")
    _ensure_col("tp_stage",        "INTEGER", "NOT NULL DEFAULT 0")
    _ensure_col("mfe_R",           "REAL",    "NOT NULL DEFAULT 0.0")

    # —— R 分母锁定的一次性字段 —— 
    _ensure_col("r0_stop_price", "REAL")
    _ensure_col("r0_px_dist",    "REAL")
    _ensure_col("r0_atr",        "REAL")
    _ensure_col("r0_R_usd",      "REAL")
    _ensure_col("r0_defined_at", "TEXT")

    # —— 审计：每次计算 R_den 的来源与时间 —— 仅新增两列
    _ensure_col("r_den_src_last",   "TEXT")
    _ensure_col("r_den_checked_at", "TEXT")




def _generate_client_order_id(t_ref: str, symbol: str, timeframe: str,
                              direction: str, opened_at: str) -> str:
    """
    唯一 client_order_id。只要 t_ref+symbol+方向+时间 不重复，基本稳定。
    如果重复（比如重复跑同一桶），INSERT 时会撞 UNIQUE，我们会捕错并跳过。
    """
    return f"{t_ref}|{symbol}|{timeframe}|{direction}|{opened_at}"


def _pick_notional_usd(order: dict, cfg: dict) -> float:
    """
    名义仓位规模。
    优先级：
      1) order["notional_usd"]（如果 decision_engine 已经给了）
      2) cfg["thresholds"]["risk_gate"]["notional_cap_usd"]
      3) cfg["risk_cost"]["max_size_usd"]
      4) 1000.0 兜底
    """
    v = order.get("notional_usd")
    if v is not None:
        try:
            v = float(v)
            if v > 0:
                return v
        except Exception:
            pass

    rg = (cfg.get("thresholds") or {}).get("risk_gate") or {}
    v = rg.get("notional_cap_usd")
    if v is not None:
        try:
            v = float(v)
            if v > 0:
                return v
        except Exception:
            pass

    rc = cfg.get("risk_cost") or {}
    v = rc.get("max_size_usd")
    if v is not None:
        try:
            v = float(v)
            if v > 0:
                return v
        except Exception:
            pass

    return 1000.0

def _insert_position_row(conn, row: dict, cfg: dict = None) -> int:
    """
    插入 positions_virtual（影子持仓）一行，并补齐“R 基线”字段。
    - 若 cfg 可用：用 _compute_initial_r_baseline() 计算初始止损/ATR/stop_frac
    - 若 cfg 不可用：使用保守默认参数兜底（k_sl=1.5, atr_len=14, donchian_n=10）
    - 自动为缺失的新增列执行 ALTER TABLE（逐列判断后再加）
    - 动态按现有列集合生成 INSERT，避免列不存在报错

    返回：新行 id
    """
    import sqlite3
    import math

    # -------- 读取调用方给到的必需字段 --------
    symbol        = row.get("symbol")
    timeframe     = row.get("timeframe") or "1h"
    t_ref         = row.get("t_ref")
    direction     = (row.get("direction") or row.get("side") or "").lower()
    entry_price   = float(row.get("entry_price") or 0.0)
    qty           = float(row.get("qty") or 0.0)
    notional_usd  = float(row.get("notional_usd") or 0.0)

    if not symbol or entry_price <= 0.0 or qty <= 0.0 or notional_usd <= 0.0:
        raise ValueError("insert_position_row: bad args (symbol/entry_price/qty/notional_usd)")

    # -------- 确保必要列存在（幂等） --------
    cur = conn.execute("PRAGMA table_info(positions_virtual)")
    cols = {r[1] for r in cur.fetchall()}

    def _ensure_col(name: str, ddl_type: str, default_clause: str = None):
        if name not in cols:
            sql = f"ALTER TABLE positions_virtual ADD COLUMN {name} {ddl_type}"
            if default_clause:
                sql += f" {default_clause}"
            conn.execute(sql)
            cols.add(name)

    # 已有运行路径用到的列（若缺则补）
    _ensure_col("entry_atr",       "REAL")
    _ensure_col("base_R_usd",      "REAL")
    _ensure_col("risk_R",          "REAL")
    _ensure_col("stop_price",      "REAL")
    _ensure_col("be_price",        "REAL")
    _ensure_col("be_armed",        "INTEGER", "NOT NULL DEFAULT 0")
    _ensure_col("tsl_price",       "REAL")
    _ensure_col("tsl_method",      "TEXT")
    _ensure_col("tsl_k",           "REAL")
    _ensure_col("tsl_armed_at_tp", "INTEGER", "NOT NULL DEFAULT 0")
    _ensure_col("tp_stage",        "INTEGER", "NOT NULL DEFAULT 0")
    _ensure_col("mfe_R",           "REAL",    "NOT NULL DEFAULT 0.0")

    # —— 新增：R 分母锁定用五列（若缺则补）——
    _ensure_col("r0_stop_price", "REAL")
    _ensure_col("r0_px_dist",    "REAL")
    _ensure_col("r0_atr",        "REAL")
    _ensure_col("r0_R_usd",      "REAL")
    _ensure_col("r0_defined_at", "TEXT")

    # -------- 计算 R 基线：优先走配置化的计算器；否则用兜底口径 --------
    def _fallback_baseline():
        # 兜底用：atr_len=14, k_sl=1.5, donchian_n=10
        import numpy as np
        row_t = conn.execute(
            "SELECT t FROM ohlcv WHERE symbol=? AND timeframe=? AND t<=? ORDER BY t DESC LIMIT 1",
            (symbol, timeframe, str(t_ref)),
        ).fetchone()
        if not row_t:
            tref_eff = str(t_ref)
        else:
            tref_eff = row_t[0]

        k_sl = 1.5

        rows = conn.execute(
            "SELECT high,low,close FROM ohlcv WHERE symbol=? AND timeframe=? AND t<=? ORDER BY t DESC LIMIT 128",
            (symbol, timeframe, tref_eff),
        ).fetchall()
        if not rows or len(rows) < 20:
            # 极端兜底：用入场价 ±1.5*ATR≈入场价的 1%
            entry_atr = max(entry_price * 0.01, 1e-12)
            stop_price = entry_price - k_sl * entry_atr if direction != "short" else entry_price + k_sl * entry_atr
            stop_frac = abs(stop_price - entry_price) / entry_price
            return {
                "ok": True, "t_ref": str(t_ref), "entry_atr": float(entry_atr),
                "stop_price": float(stop_price), "stop_frac": float(stop_frac),
                "regime": "trending", "k_sl_used": float(k_sl),
                "atr_len_used": 14, "struct_anchor": None, "method": "fallback-lite"
            }

        highs = [float(r[0]) for r in rows][::-1]
        lows  = [float(r[1]) for r in rows][::-1]
        close = [float(r[2]) for r in rows][::-1]

        def _atr(h, l, c, n=14):
            h, l, c = map(np.asarray, (h, l, c))
            tr = np.maximum(h[1:], c[:-1]) - np.minimum(l[1:], c[:-1])
            alpha = 1.0 / n
            a = tr.astype(float)
            for i in range(1, a.shape[0]):
                a[i] = alpha * tr[i] + (1 - alpha) * a[i-1]
            return float(a[-1])

        entry_atr = max(_atr(highs, lows, close, 14), 1e-12)
        ndc = 10
        dc_hi = float(np.nanmax(highs[-ndc:]))
        dc_lo = float(np.nanmin(lows[-ndc:]))

        if direction == "short":
            atr_stop = entry_price + k_sl * entry_atr
            struct_stop = dc_hi
            stop_price = max(struct_stop, atr_stop)
            stop_frac = max(0.0, (stop_price - entry_price) / entry_price)
        else:
            atr_stop = entry_price - k_sl * entry_atr
            struct_stop = dc_lo
            stop_price = min(struct_stop, atr_stop)
            stop_frac = max(0.0, (entry_price - stop_price) / entry_price)

        return {
            "ok": True,
            "t_ref": str(t_ref),
            "entry_atr": float(entry_atr),
            "stop_price": float(stop_price),
            "stop_frac": float(stop_frac),
            "regime": "trending",
            "k_sl_used": float(k_sl),
            "atr_len_used": 14,
            "struct_anchor": float(dc_lo if direction != "short" else dc_hi),
            "method": "fallback",
        }

    baseline = None
    if cfg is not None:
        try:
            baseline = _compute_initial_r_baseline(
                conn=conn,
                symbol=symbol,
                timeframe=timeframe,
                t_ref=str(t_ref),
                entry_price=float(entry_price),
                direction=direction,
                cfg=cfg,
            )
        except Exception:
            baseline = None
    if not baseline or not baseline.get("ok"):
        baseline = _fallback_baseline()

    entry_atr  = float(baseline.get("entry_atr") or 0.0)
    stop_price = float(baseline.get("stop_price") or 0.0)
    stop_frac  = float(baseline.get("stop_frac") or 0.0)

    # base_R_usd：按初始止损宽度（相对入场价）估算
    base_R_usd = float(notional_usd * stop_frac)

    # risk_R：优先使用决策阶段的 risk_R_alloc；否则 1.0 兜底
    risk_R = row.get("risk_R")
    if risk_R is None:
        risk_R = row.get("risk_R_alloc")
    try:
        risk_R = float(risk_R) if risk_R is not None else 1.0
    except Exception:
        risk_R = 1.0

    # 初始状态字段
    be_armed = 0
    be_price = None
    tsl_price = None
    tsl_method = None
    tsl_k = None
    tsl_armed_at_tp = 0
    tp_stage = 0
    mfe_R = 0.0

    # —— R 分母锁定：一次性写入 r0_*（不可变统计口径）——
    r0_stop_price = stop_price
    r0_px_dist    = float(abs(entry_price - r0_stop_price))
    r0_atr        = entry_atr
    r0_R_usd      = base_R_usd
    r0_defined_at = now_db_str(cfg)

    # -------- 组装 INSERT（仅包含表里真实存在的列）--------
    values = dict(row)  # 先放入调用方已有字段
    # 强制覆盖/新增我们要写的字段
    values.update({
        "entry_atr": entry_atr,
        "base_R_usd": base_R_usd,
        "risk_R": risk_R,
        "stop_price": stop_price,
        "be_price": be_price,
        "be_armed": be_armed,
        "tsl_price": tsl_price,
        "tsl_method": tsl_method,
        "tsl_k": tsl_k,
        "tsl_armed_at_tp": tsl_armed_at_tp,
        "tp_stage": tp_stage,
        "mfe_R": mfe_R,

        # R0 锁定分母（只写一次，插入即定）
        "r0_stop_price": r0_stop_price,
        "r0_px_dist":    r0_px_dist,
        "r0_atr":        r0_atr,
        "r0_R_usd":      r0_R_usd,
        "r0_defined_at": r0_defined_at,
    })

    # 过滤掉表里不存在的键（避免列缺失报错）
    insert_cols = [k for k in values.keys() if k in cols]
    placeholders = ",".join(["?"] * len(insert_cols))
    sql = f"INSERT INTO positions_virtual ({','.join(insert_cols)}) VALUES ({placeholders})"
    args = [values[k] for k in insert_cols]

    conn.execute(sql, args)

    # 拿到新 id
    new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # —— 最小必要调试 ——（一行）
    try:
        print(f"[open_insert] id={new_id} {symbol} {timeframe} px={entry_price:.6f} "
              f"ATR={entry_atr:.6f} stop={stop_price:.6f} "
              f"R_base=${base_R_usd:.2f} risk_R={risk_R:.2f} tp_stage=0")
    except Exception:
        pass

    return int(new_id)



def _already_have_position(conn: sqlite3.Connection, symbol: str, direction: str) -> bool:
    """
    返回 True 表示我们已经持有这个 symbol 的这个方向的仓位
    (status='OPEN' 或 'ACTIVE')，不允许再加同方向新票。

    目的：
    - 防止同一个币不断加码（典型灾难：DYDX/USDT 连开6张 long）
    - 我们的纪律是：一票进场，没走就砍；不是叠仓赌命
    """
    if not symbol or not direction:
        return False  # 缺字段就不要卡死流程，由后面逻辑决定 skip

    row = conn.execute(
        """
        SELECT 1
        FROM positions_virtual
        WHERE symbol = ?
          AND lower(direction) = lower(?)
          AND status IN ('OPEN','ACTIVE')
        LIMIT 1;
        """,
        (symbol, direction),
    ).fetchone()

    return row is not None


import sqlite3
from typing import Dict, Any, List

from core import market_data  # 你原来就有

# -------------------- 新增/补全的工具函数 --------------------
def _compute_initial_r_baseline(
    conn,
    symbol: str,
    timeframe: str,
    t_ref: str,
    entry_price: float,
    direction: str,
    cfg: dict,
) -> dict:
    """
    目的：开仓瞬间确定“R 基线”，供后续 BE/TP/TSL 使用（不要用后续收紧过的止损来当 R 分母）。
    返回仅包含“价格口径”和“分母口径”，不依赖 notional；base_R_usd 由上层用 notional_usd * stop_frac 推出。

    返回:
      {
        "ok": True/False,
        "t_ref": <str>,                # 实际对齐到的收盘时间
        "entry_atr": <float>,          # 按 regime 选用的 ATR（Wilder近似）
        "stop_price": <float>,         # 初始硬止损（结构/ATR 二择一，遵从“min(多)/max(空)”规则）
        "stop_frac": <float>,          # 相对入场价的止损距离占比；多=(E-S)/E，空=(S-E)/E
        "regime": "trending|ranging",  # 用广度+BTC 对齐粗判
        "k_sl_used": <float>,          # 使用的 k_sl
        "atr_len_used": <int>,         # 使用的 ATR 长度
        "struct_anchor": <float|None>, # 结构锚点（Donchian低/高）
        "method": "atr_only|atr_vs_struct"
      }
    失败时: {"ok": False, "reason": "..."}
    """
    import numpy as np

    try:
        direction = (direction or "").lower()
        timeframe = timeframe or "1h"

        if entry_price is None or float(entry_price) <= 0:
            return {"ok": False, "reason": "bad_entry_price"}

        # ---- 读取配置 ----
        core_cfg = cfg.get("core", cfg)
        exit_cfg = core_cfg.get("exit", {}) or {}
        sl_cfg = exit_cfg.get("sl", {}) or {}
        tsl_cfg = exit_cfg.get("tsl", {}) or {}

        # ATR 长度（与 TSL 的 len 对齐，趋势/震荡两套）
        if isinstance(tsl_cfg.get("atr_len"), dict):
            atr_len_tr = int(tsl_cfg["atr_len"].get("trending", 14))
            atr_len_rg = int(tsl_cfg["atr_len"].get("ranging", 10))
        else:
            atr_len_tr = int(tsl_cfg.get("atr_len") or 14)
            atr_len_rg = max(5, int(0.7 * atr_len_tr))

        # k_sl（趋势/震荡两套）
        if isinstance(sl_cfg.get("atr_mult"), dict):
            k_tr = float(sl_cfg["atr_mult"].get("trending", 1.5))
            k_rg = float(sl_cfg["atr_mult"].get("ranging", 1.8))
        else:
            k_tr = k_rg = float(sl_cfg.get("atr_mult", 1.5))

        use_struct = bool(sl_cfg.get("use_structure", True))

        # ---- 锚定 t_ref：若传入不存在该根，回退到最近 <= t_ref 的一根 ----
        row_t = conn.execute(
            "SELECT t FROM ohlcv WHERE symbol=? AND timeframe=? AND t<=? ORDER BY t DESC LIMIT 1",
            (symbol, timeframe, str(t_ref)),
        ).fetchone()
        if not row_t:
            return {"ok": False, "reason": "no_tref"}
        t_ref_eff = row_t[0]

        # ---- 拉窗口（尽量长，确保统计稳定）----
        lookback = max(200, atr_len_tr * 6, atr_len_rg * 8)
        rows = conn.execute(
            """
            SELECT t, open, high, low, close
            FROM ohlcv
            WHERE symbol=? AND timeframe=? AND t<=?
            ORDER BY t DESC
            LIMIT ?
            """,
            (symbol, timeframe, t_ref_eff, lookback),
        ).fetchall()
        if not rows or len(rows) < max(atr_len_rg + 2, 5):
            return {"ok": False, "reason": "not_enough_bars"}
        # 升序
        rows = rows[::-1]
        highs = np.array([float(r[2]) for r in rows], dtype=float)
        lows  = np.array([float(r[3]) for r in rows], dtype=float)
        closes= np.array([float(r[4]) for r in rows], dtype=float)

        # ---- 计算 ATR（Wilder 近似）----
        def _atr_wilder(h, l, c, n: int) -> float:
            if n <= 1 or len(c) < 2:
                return float("nan")
            pc = np.roll(c, 1); pc[0] = c[0]
            tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
            alpha = 2.0 / (n + 1.0)
            atr = tr.astype(float)
            for i in range(1, atr.shape[0]):
                atr[i] = alpha * tr[i] + (1 - alpha) * atr[i-1]
            return float(atr[-1])

        # ---- 粗判 regime（与 exit_ctx 一致口径）----
        br = conn.execute(
            "SELECT ad_ratio FROM breadth_snapshot WHERE t_ref<=? ORDER BY t_ref DESC LIMIT 1",
            (t_ref_eff,),
        ).fetchone()
        ad_ratio = float(br[0]) if br and br[0] is not None else None

        ba = conn.execute(
            """
            SELECT align_corr, peak_corr_val
            FROM btc_alignment_snapshot
            WHERE symbol=? AND timeframe=? AND t_ref<=?
            ORDER BY t_ref DESC LIMIT 1
            """,
            (symbol, timeframe, t_ref_eff),
        ).fetchone()
        align_corr = float(ba[0]) if ba and ba[0] is not None else None
        peak_corr  = float(ba[1]) if ba and ba[1] is not None else None

        # 判别
        regime = "trending"
        if ad_ratio is None or align_corr is None:
            regime = "trending"  # 缺数据偏向给趋势更多空间
        else:
            if direction == "short":
                regime = "trending" if (ad_ratio < 0.95 and abs(align_corr) >= 0.30) else "ranging"
            else:
                regime = "trending" if (ad_ratio > 1.05 and abs(align_corr) >= 0.30) else "ranging"

        atr_len_use = atr_len_tr if regime == "trending" else atr_len_rg
        k_sl = k_tr if regime == "trending" else k_rg
        entry_atr = _atr_wilder(highs, lows, closes, atr_len_use)
        entry_atr = max(entry_atr, 1e-12)

        # ---- 结构锚点：Donchian 近似（入场前 N 根的极值）----
        # 使用与 ranging 口径一致的 n（更敏感），但不少于 10
        n_dc = max(10, atr_len_rg)
        dc_high = float(np.nanmax(highs[-n_dc:]))
        dc_low  = float(np.nanmin(lows[-n_dc:]))

        # ---- 候选止损（ATR 与 结构）----
        if direction == "short":
            atr_stop = float(entry_price + k_sl * entry_atr)
            struct_stop = float(dc_high) if use_struct else atr_stop
            # 规则：空单取 “max(结构上沿, ATR 上沿)”（更靠上 = 更宽），与用户口径“短反之”一致
            stop_price = max(struct_stop, atr_stop)
            stop_frac = max(0.0, (stop_price - entry_price) / entry_price)
        else:
            atr_stop = float(entry_price - k_sl * entry_atr)
            struct_stop = float(dc_low) if use_struct else atr_stop
            # 规则：多单取 “min(结构下沿, ATR 下沿)”（更靠下 = 更宽），与用户口径一致
            stop_price = min(struct_stop, atr_stop)
            stop_frac = max(0.0, (entry_price - stop_price) / entry_price)

        # 避免异常：stop_frac 极值裁剪
        if not np.isfinite(stop_frac) or stop_frac <= 0:
            stop_frac = float(abs(k_sl * entry_atr) / max(entry_price, 1e-12))
            if direction == "short":
                stop_price = entry_price * (1.0 + stop_frac)
            else:
                stop_price = entry_price * (1.0 - stop_frac)

        # —— 最小必要调试 ——（一行）
        try:
            print(f"[open_baseline] {symbol} {timeframe} t={t_ref_eff} px={float(entry_price):.6f} "
                  f"ATR={entry_atr:.6f} k={k_sl:.2f} reg={regime} "
                  f"dc={dc_low:.6f}-{dc_high:.6f} stop={stop_price:.6f} frac={stop_frac:.5f} "
                  f"mode={'atr_vs_struct' if use_struct else 'atr_only'}")
        except Exception:
            pass

        return {
            "ok": True,
            "t_ref": str(t_ref_eff),
            "entry_atr": float(entry_atr),
            "stop_price": float(stop_price),
            "stop_frac": float(stop_frac),
            "regime": regime,
            "k_sl_used": float(k_sl),
            "atr_len_used": int(atr_len_use),
            "struct_anchor": float(dc_low if direction != "short" else dc_high) if use_struct else None,
            "method": "atr_vs_struct" if use_struct else "atr_only",
        }

    except Exception as e:
        print(f"[open_baseline][ERR] {e}")
        return {"ok": False, "reason": f"exception:{e}"}



def _ensure_orders_schema(conn: sqlite3.Connection) -> None:
    """
    positions_virtual 有外键指向 orders(client_order_id)，
    所以我们在开仓前必须保证 orders 存在。
    这里建的是最小字段集，跟你库里那张对得上。
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders(
          client_order_id TEXT PRIMARY KEY,
          decision_id     INTEGER,
          t_ref           TEXT NOT NULL,
          symbol          TEXT NOT NULL,
          timeframe       TEXT NOT NULL,
          direction       TEXT,
          price           REAL,
          qty             REAL,
          notional_usd    REAL,
          position_scale  REAL DEFAULT 1.0,
          status          TEXT NOT NULL,
          reject_reason   TEXT,
          created_at      TEXT NOT NULL,
          is_shadow       INTEGER DEFAULT 1,
          source          TEXT,
          FOREIGN KEY(decision_id)
            REFERENCES decision_snapshot(id)
            ON DELETE SET NULL
            ON UPDATE CASCADE
        )
        """
    )


def _already_have_position_sym_tf(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    direction: str,
) -> bool:
    """
    按 symbol+timeframe+direction 去重。
    这样同一币 1h/4h 可以各开一条，但同一币同一周期同一方向只开一条。
    """
    cur = conn.execute(
        """
        SELECT 1
        FROM positions_virtual
        WHERE symbol = ?
          AND timeframe = ?
          AND direction = ?
          AND status IN ('OPEN','ACTIVE')
        LIMIT 1
        """,
        (symbol, timeframe, direction),
    )
    return cur.fetchone() is not None


# -------------------- 这是你要的主函数（替换你原来的） --------------------

def open_positions_for_plan(
    conn: sqlite3.Connection,
    cfg: dict,
    trade_plan: dict,
) -> Dict[str, Any]:
    """
    把 decision_engine.build_trade_plan(...) 输出的 trade_plan['approved']
    真正落到 orders + positions_virtual 里，作为影子持仓。

    修改点：
    - 去重从 “同symbol同方向” → “同symbol同timeframe同方向”
    - 插 positions_virtual 之前先插 orders，解决 FOREIGN KEY constraint
    - 开仓时写入 R 基线：entry_atr / stop_price / base_R_usd / risk_R
    - 初始化退出侧字段：be_armed/be_price/tsl_* / tp_stage / mfe_R
    - 若配置 exit.sl.use_structure=true，使用筹码峰三价带对 ATR 止损做收紧
    """
    from core import market_data
    from core.chip_structure import compute_triple_recent
    import math
    import logging
    if not isinstance(cfg, dict):
        logging.error("[probe] execution.open_positions_for_plan got NON-DICT cfg: type=%s repr=%r",
                    type(cfg).__name__, str(cfg)[:200])
        raise TypeError("cfg must be dict")

    try:
        logging.debug(
            "[probe] execution.open_positions_for_plan start t_ref=%s approved_n=%s cfg.keys=%s",
            trade_plan.get("t_ref"),
            len(trade_plan.get("approved") or []),
            list(cfg.keys())[:12] if isinstance(cfg, dict) else None
        )
    except Exception:
        pass

    # 1) 确保两张表结构都在
    _ensure_positions_virtual_schema(conn)
    _ensure_orders_schema(conn)

    approved: List[dict] = list(trade_plan.get("approved") or [])
    opened_list: List[dict] = []
    skipped_list: List[dict] = []

    # [修改] 使用 now_db_str(cfg)
    now_ts = now_db_str(cfg)
    plan_t_ref = trade_plan.get("t_ref")
    t_ref_str = str(plan_t_ref or now_ts)

    # 全局策略版本兜底
    global_strat_ver = str(cfg.get("strategy_version", "unknown"))

    # 配置读取
    core_cfg = cfg.get("core", cfg)
    exit_cfg = core_cfg.get("exit", {}) or {}
    sl_cfg = exit_cfg.get("sl", {}) or {}
    use_structure = bool(sl_cfg.get("use_structure", False))

    for order in approved:
        symbol = order.get("symbol")
        timeframe = order.get("timeframe") or "1h"
        direction = _normalize_direction(order.get("direction"))

        # === 基本字段校验 ===
        if not symbol or not timeframe or direction not in ("long", "short"):
            skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": "invalid_fields",
            })
            print("[run_open_cycle] SKIP invalid_fields", symbol, direction, timeframe)
            continue

        # === 新的去重：按 symbol+timeframe+direction ===
        if _already_have_position_sym_tf(conn, symbol, timeframe, direction):
            skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": "duplicate_active_position_sym_tf",
            })
            print("[run_open_cycle] SKIP duplicate_active_position_sym_tf", symbol, timeframe, direction)
            continue

        # === 入场价：先用 trade_plan 的，没有就去行情表拿 ===
        entry_px = order.get("entry_price")
        if entry_px is None:
            try:
                logging.debug(
                    "[probe] execution.price_call sym=%s tf=%s t_ref=%s cfg_type=%s",
                    symbol, timeframe, t_ref_str, type(cfg).__name__
                )
                entry_px = market_data.fetch_last_price(
                    conn=conn,
                    symbol=symbol,
                    timeframe=timeframe,
                    t_ref=t_ref_str,
                    cfg=cfg,
                )
            except Exception:
                entry_px = None

        if entry_px is None:
            skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": "price_unavailable",
            })
            print("[run_open_cycle] SKIP price_unavailable", symbol, direction)
            continue

        try:
            entry_px_f = float(entry_px)
            if not (entry_px_f > 0):
                raise ValueError("non-positive price")
        except Exception:
            skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": "bad_price_value",
            })
            print("[run_open_cycle] SKIP bad_price_value", symbol, direction, entry_px)
            continue

        # === 资金规模（USD）和数量 ===
        notional_usd = _pick_notional_usd(order, cfg)
        try:
            notional_usd_f = float(notional_usd)
            qty_f = notional_usd_f / entry_px_f if notional_usd_f > 0 else 0.0
        except Exception:
            notional_usd_f = 0.0
            qty_f = 0.0

        if qty_f <= 0.0:
            skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": "invalid_size",
            })
            print("[run_open_cycle] SKIP invalid_size", symbol, direction, notional_usd)
            continue

        # === position_scale ===
        position_scale_f = 1.0
        try:
            position_scale_f = float(order.get("position_scale", 1.0))
        except Exception:
            pass

        # === 幂等 client_order_id ===
        coid = _generate_client_order_id(
            t_ref=t_ref_str,
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            opened_at=now_ts,
        )

        # === 单票策略版本 ===
        strat_ver = str(order.get("strategy_version") or global_strat_ver or "unknown")

        # --------------------------------------------------
        # 开仓前：计算 R 基线（ATR 止损），并可选用结构位收紧
        # --------------------------------------------------
        baseline = _compute_initial_r_baseline(
            conn=conn,
            symbol=symbol,
            timeframe=timeframe,
            t_ref=t_ref_str,
            entry_price=entry_px_f,
            direction=direction,
            cfg=cfg,
        )
        entry_atr = float(baseline.get("entry_atr", 0.0))
        atr_stop_price = float(baseline.get("stop_price", entry_px_f))

        # 用筹码峰三价带对止损做一次“只收紧不放宽”的结构化校准（可选）
        final_stop_price = atr_stop_price
        if use_structure:
            try:
                triple = compute_triple_recent(
                    conn=conn,
                    symbol=symbol,
                    timeframe=timeframe,
                    t_ref=t_ref_str,
                    cfg=cfg,
                    lookback_bars=120,  # 默认 120 根
                )
                if triple.get("ok"):
                    val = float(triple.get("val", 0.0))
                    vah = float(triple.get("vah", 0.0))
                    if direction == "long" and val > 0.0:
                        # long：取更严格的止损（更靠近入场且在下方）
                        candidate = min(atr_stop_price, val)
                        # 确保在入场价之下
                        if candidate < entry_px_f:
                            final_stop_price = candidate
                    elif direction == "short" and vah > 0.0:
                        # short：取更严格的止损（更靠近入场且在上方）
                        candidate = max(atr_stop_price, vah)
                        # 确保在入场价之上
                        if candidate > entry_px_f:
                            final_stop_price = candidate
            except Exception as e:
                # 出问题就忽略结构位，仍以 ATR 为准
                print("[run_open_cycle] structure tighten failed:", symbol, timeframe, str(e))

        # 计算风险分数（按“相对入场价”的宽度）
        stop_frac = abs(final_stop_price - entry_px_f) / entry_px_f
        stop_frac = float(max(0.0, stop_frac))
        base_R_usd = float(notional_usd_f * stop_frac)

        # 分配到此单的 risk_R（来自上游 decision_engine 分配结果）
        try:
            risk_R_val = float(order.get("risk_R_alloc")) if order.get("risk_R_alloc") is not None else None
        except Exception:
            risk_R_val = None

        # --------------------------------------------------
        # 先插 orders（满足 positions_virtual 的外键）
        # --------------------------------------------------
        try:
            # 尝试找到对应的 decision_snapshot.id 方便审计
            decision_id = None
            cur = conn.execute(
                """
                SELECT id
                FROM decision_snapshot
                WHERE t_ref = ? AND symbol = ? AND timeframe = ?
                """,
                (t_ref_str, symbol, timeframe),
            )
            row = cur.fetchone()
            if row:
                decision_id = row[0]

            # [修改] SQL： 'NEW', datetime('now','localtime'), 1, ?  ->  'NEW', ?, 1, ?
            # [修改] PARAMS： 增加 now_ts 作为 created_at 的参数
            conn.execute(
                """
                INSERT OR IGNORE INTO orders(
                    client_order_id,
                    decision_id,
                    t_ref,
                    symbol,
                    timeframe,
                    direction,
                    price,
                    qty,
                    notional_usd,
                    position_scale,
                    status,
                    created_at,
                    is_shadow,
                    source
                ) VALUES (
                    ?,?,?,?,?,?,?,?,?,?,'NEW',?,1,?
                )
                """,
                (
                    coid,
                    decision_id,
                    t_ref_str,
                    symbol,
                    timeframe,
                    direction,
                    entry_px_f,
                    qty_f,
                    notional_usd_f,
                    position_scale_f,
                    now_ts, # [修改] 对应 created_at 的 '?'
                    order.get("source") or "shadow",
                ),
            )
        except sqlite3.IntegrityError as e:
            skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": f"integrity_error_orders:{str(e)}",
            })
            print("[run_open_cycle] SKIP integrity_error (orders)", symbol, direction, e)
            continue

        # --------------------------------------------------
        # 再插 positions_virtual（含 R 基线 与 退出字段初始化）
        # --------------------------------------------------
        row_to_insert = {
            "client_order_id": coid,
            "t_ref": t_ref_str,
            "symbol": symbol,
            "timeframe": timeframe,
            "direction": direction,
            "entry_price": entry_px_f,
            "qty": qty_f,
            "notional_usd": notional_usd_f,
            "position_scale": position_scale_f,
            "status": "OPEN",
            "opened_at": now_ts, # [修改] 这里也使用 now_ts
            "closed_at": None,
            "exit_reason": None,
            "exit_price": None,
            "pnl_usd": 0.0,
            "is_shadow": 1,
            "source": order.get("source") or "shadow",
            "strategy_version": strat_ver,
            "setup_tag": order.get("setup_tag"),
            "entry_thesis": order.get("entry_thesis"),
            "validation_level": order.get("validation_level"),
            "risk_R_alloc": order.get("risk_R_alloc"),

            # ---- 新增：R 基线 & 退出侧字段 ----
            "entry_atr": entry_atr,
            "base_R_usd": base_R_usd,
            "risk_R": risk_R_val,
            "stop_price": final_stop_price,

            "be_price": None,
            "be_armed": 0,

            "tsl_price": None,
            "tsl_method": None,
            "tsl_k": None,
            "tsl_armed_at_tp": 0,

            "tp_stage": 0,
            "mfe_R": 0.0,
        }

        try:
            # [修改] _insert_position_row 现在依赖 cfg 来计算 R 基线
            _insert_position_row(conn, row_to_insert, cfg)
        except sqlite3.IntegrityError as e:
            skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": f"integrity_error:{str(e)}",
            })
            print("[run_open_cycle] SKIP integrity_error (positions_virtual)", symbol, direction, e)
            continue
        except Exception as e_ins:
             skipped_list.append({
                "symbol": symbol,
                "direction": direction,
                "reason": f"insert_pos_fail:{str(e_ins)}",
            })
             print(f"[run_open_cycle] SKIP _insert_position_row failed: {e_ins}")
             continue


        # === 成功开仓，记一笔 ===
        opened_list.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "direction": direction,
                "entry_price": entry_px_f,
                "qty": qty_f,
                "notional_usd": notional_usd_f,
                "client_order_id": coid,
                "strategy_version": strat_ver,
                "entry_atr": entry_atr,
                "stop_price": final_stop_price,
                "base_R_usd": base_R_usd,
                "risk_R": risk_R_val,
            }
        )

    return {
        "opened": opened_list,
        "skipped": skipped_list,
    }