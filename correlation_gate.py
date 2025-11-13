# -*- coding: utf-8 -*-
"""
core/correlation_gate.py

这个模块负责“相关性门” (correlation gate)：

1. 看当前账户已有持仓 (positions_virtual status='OPEN')，
   生成一个组合画像 portfolio_snapshot，用于当作基准。
   - 以后真实版会用这个组合去算每个候选票的相关性 rho_*。
   - 现在简化：如果没有持仓，就走 when_no_portfolio 策略。

2. 对本桶候选单逐个给出决策:
   - decision: "pass" / "scale" / "block"
   - position_scale: 1.0 / 0.5 / 0.25 / 0.0 ...
   - scale_note: 文案（低相关 / 中高相关 / 极端相关 等）
   - rho_star: 相关性强度评分 (占位，后续用真实的)
   - rho_1h / rho_4h / rho_1d / samples_* 也是站位，先写0、96之类
   - reason/status/portfolio_json 方便dashboard审计

   并把这些写入 correlation_snapshot 表(INSERT)

3. 聚合这些单票决策，产出整桶的 gate 结果，给 decision_engine 用：
   - corr_action: pass / scale / block
   - corr_scale: 这桶整体建议的缩放系数
   - corr_est: 我们估计的“拥挤度分数” (用rho_star替代)
   - correlation_block_reason: 如果整桶直接封单

后续要升级的部分（还没实现）：
   - 真正计算 rho_1h/rho_4h/rho_1d：rolling收益序列, Pearson相关
   - tf_weights 加权出 rho_star
   - hysteresis (迟滞): 与上一桶决定对比，避免来回抖
   - 集中度惩罚 / clamp_scale
   - basis='portfolio' / 'btc' 区分
"""

from __future__ import annotations
import sqlite3
import json
# [修改] 移除 datetime, 导入 timebox
from core.timebox import now_local_str
from typing import Any, Dict, List, Tuple, Optional


# ---------------------------------------------------------------------
# 内部: 读取当前持仓，构建 portfolio_snapshot
# ---------------------------------------------------------------------

def _get_open_positions(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """
    拉当前OPEN仓位，用于构建组合权重。
    需要表 positions_virtual 至少有:
        symbol, direction('long'/'short'), qty, entry_price, status
    """
    rows = conn.execute(
        """
        SELECT symbol, direction, qty, entry_price
        FROM positions_virtual
        WHERE status='OPEN'
        """
    ).fetchall()
    return rows or []


def _estimate_last_price(conn: sqlite3.Connection, symbol: str) -> Optional[float]:
    """
    估这个symbol的最新价，给组合估值用。
    简化版: 去 ohlcv_1h (或类似) 里拿最近一条 close。
    如果拿不到，就None。
    """
    # 尝试几个常见的K线表，取最新 close
    candidate_tables = [
        "ohlcv_1h", "ohlcv_4h", "ohlcv",
        "candles_1h", "candles",
    ]
    for tname in candidate_tables:
        # 试着探测列名字 time/ts/open_time/t
        for tcol in ("time", "ts", "open_time", "t"):
            try:
                row = conn.execute(
                    f"""
                    SELECT close
                    FROM {tname}
                    WHERE symbol=?
                    ORDER BY {tcol} DESC
                    LIMIT 1
                    """,
                    (symbol,)
                ).fetchone()
            except sqlite3.OperationalError:
                # 表不存在 或 列不存在
                continue
            if row and row[0] is not None:
                try:
                    return float(row[0])
                except (TypeError, ValueError):
                    return None
    return None


def _build_portfolio_snapshot(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    返回:
    {
      "weights": { "BTC/USDT": 0.4, "ETH/USDT": 0.6, ... },   # 绝对名义占比
      "net_dir": { "BTC/USDT": "long", "ETH/USDT": "short", ... },
      "total_abs_usd": 12345.67,
      "n_positions": 2
    }

    逻辑:
    - 估算每个OPEN仓的名义美元规模: abs(qty * last_price)
    - total_abs_usd = 全部abs名义规模求和
    - weight = abs_pos_usd / total_abs_usd
      (即使是short, 这里权重还是正的; 方向单独写在 net_dir)
    - 如果没有持仓, total_abs_usd = 0, 返回空
    """
    rows = _get_open_positions(conn)

    comps = []
    total_abs_usd = 0.0
    details = []
    for r in rows:
        sym = r["symbol"]
        direction = (r["direction"] or "").lower()
        qty = float(r["qty"] or 0.0)
        entry_px = float(r["entry_price"] or 0.0)

        # 尝试拿一个最新价，拿不到就fallback entry_px
        last_px = _estimate_last_price(conn, sym)
        if last_px is None or last_px <= 0:
            last_px = entry_px if entry_px > 0 else 0.0

        notional_usd = abs(qty * last_px)
        total_abs_usd += notional_usd

        details.append({
            "symbol": sym,
            "direction": direction,
            "qty": qty,
            "last_px": last_px,
            "abs_notional_usd": notional_usd,
        })

    weights_map = {}
    dir_map = {}
    if total_abs_usd > 0:
        for d in details:
            w = d["abs_notional_usd"] / total_abs_usd if total_abs_usd > 0 else 0.0
            weights_map[d["symbol"]] = w
            dir_map[d["symbol"]] = d["direction"]
    else:
        # 空组合
        weights_map = {}
        dir_map = {}

    snap = {
        "weights": weights_map,
        "net_dir": dir_map,
        "total_abs_usd": total_abs_usd,
        "n_positions": len(details),
    }
    return snap


# ---------------------------------------------------------------------
# 内部: 根据当前组合 / cfg，给一个候选票生成决策 + snapshot row
# ---------------------------------------------------------------------

def _decide_for_candidate_no_portfolio(
    cg_cfg: dict,
    symbol: str,
    timeframe: str,
    portfolio_snap: Dict[str, Any],
) -> Dict[str, Any]:
    """
    当账户目前是空仓 (没有组合) 的情况下怎么判这票？
    策略由 cfg["thresholds"]["correlation_gate"]["when_no_portfolio"] 决定。
    常见值: "pass" / "block" / "scale_half"
    我们默认 "pass".

    返回结构:
    {
      "decision": "pass",
      "position_scale": 1.0,
      "scale_note": "低相关",
      "rho_basis": "portfolio",
      "rho_1h": 0.0,
      "rho_4h": 0.0,
      "rho_1d": 0.0,
      "rho_star": 0.0,
      "samples_1h": 96,
      "samples_4h": 96,
      "samples_1d": 0,
      "calc_ok": 1,
      "status": "ok",
      "reason": "no_portfolio_pass",
    }
    """
    mode = str(cg_cfg.get("when_no_portfolio", "pass")).lower()

    if mode == "block":
        decision = "block"
        scale = 0.0
        note = "无组合封单"
        reason = "no_portfolio_block"
    elif mode in ("scale_half", "scale_halfway", "half"):
        decision = "scale"
        scale = 0.5
        note = "无组合半仓"
        reason = "no_portfolio_scale"
    else:
        # 默认 pass
        decision = "pass"
        scale = 1.0
        note = "低相关"
        reason = "no_portfolio_pass"

    out = {
        "symbol": symbol,
        "timeframe": timeframe,
        "rho_1h": 0.0,
        "rho_4h": 0.0,
        "rho_1d": 0.0,
        "rho_star": 0.0,
        "rho_basis": "portfolio",
        "decision": decision,
        "position_scale": scale,
        "scale_note": note,
        "samples_1h": 96,
        "samples_4h": 96,
        "samples_1d": 0,
        "calc_ok": 1,
        "status": "ok",
        "reason": reason,
        "portfolio_json": json.dumps(portfolio_snap, ensure_ascii=False),
    }
    return out

# [修改] 增加 now_ts 参数
def _insert_snapshot_row(conn, t_ref, row, now_ts: str):
    """
    把这一票的相关性决策写进 correlation_snapshot 表。

    该表在库里有唯一约束 (t_ref, symbol, timeframe)。
    我们现在允许多次跑同一桶，所以必须幂等：
    - 如果已存在 -> 更新
    - 如果不存在 -> 插入

    我们用 SQLite 的 INSERT ... ON CONFLICT DO UPDATE。
    """

    conn.execute(
        """
        INSERT INTO correlation_snapshot(
            t_ref,
            symbol,
            timeframe,
            rho_1h,
            rho_4h,
            rho_1d,
            rho_star,
            rho_basis,
            decision,
            position_scale,
            scale_note,
            samples_1h,
            samples_4h,
            samples_1d,
            calc_ok,
            status,
            reason,
            portfolio_json,
            created_at
        )
        VALUES (
            :t_ref,
            :symbol,
            :timeframe,
            :rho_1h,
            :rho_4h,
            :rho_1d,
            :rho_star,
            :rho_basis,
            :decision,
            :position_scale,
            :scale_note,
            :samples_1h,
            :samples_4h,
            :samples_1d,
            :calc_ok,
            :status,
            :reason,
            :portfolio_json,
            :created_at
        )
        ON CONFLICT(t_ref, symbol, timeframe)
        DO UPDATE SET
            rho_1h         = excluded.rho_1h,
            rho_4h         = excluded.rho_4h,
            rho_1d         = excluded.rho_1d,
            rho_star       = excluded.rho_star,
            rho_basis      = excluded.rho_basis,
            decision       = excluded.decision,
            position_scale = excluded.position_scale,
            scale_note     = excluded.scale_note,
            samples_1h     = excluded.samples_1h,
            samples_4h     = excluded.samples_4h,
            samples_1d     = excluded.samples_1d,
            calc_ok        = excluded.calc_ok,
            status         = excluded.status,
            reason         = excluded.reason,
            portfolio_json = excluded.portfolio_json,
            created_at     = excluded.created_at
        ;
        """,
        {
            "t_ref":           t_ref,
            "symbol":          row.get("symbol"),
            "timeframe":       row.get("timeframe"),
            "rho_1h":          row.get("rho_1h"),
            "rho_4h":          row.get("rho_4h"),
            "rho_1d":          row.get("rho_1d"),
            "rho_star":        row.get("rho_star"),
            "rho_basis":       row.get("rho_basis"),
            "decision":        row.get("decision"),
            "position_scale":  row.get("position_scale"),
            "scale_note":      row.get("scale_note"),
            "samples_1h":      row.get("samples_1h"),
            "samples_4h":      row.get("samples_4h"),
            "samples_1d":      row.get("samples_1d"),
            "calc_ok":         row.get("calc_ok", 1),
            "status":          row.get("status", "ok"),
            "reason":          row.get("reason"),
            "portfolio_json":  row.get("portfolio_json"),
            "created_at":      now_ts, # [修改] 使用 now_ts
        },
    )


import numpy as np
import pandas as pd
from typing import List

def calculate_rho(prices_a: List[float], prices_b: List[float]) -> float:
    """
    计算两个标的（价格序列）的皮尔逊相关系数（rho）
    :param prices_a: 标的A的历史价格列表
    :param prices_b: 标的B的历史价格列表
    :return: 皮尔逊相关系数（rho）
    """
    if len(prices_a) != len(prices_b):
        raise ValueError("两个标的的价格数据长度不一致")
    
    # 转为pandas的Series进行计算
    series_a = pd.Series(prices_a)
    series_b = pd.Series(prices_b)
    
    # 计算皮尔逊相关系数
    correlation = series_a.corr(series_b)
    
    return correlation

def calculate_position_scale(correlation: float) -> float:
    """
    根据相关性（rho）动态计算仓位规模。
    :param correlation: 标的间的相关性（rho）
    :return: 动态调整后的仓位比例
    """
    if correlation < 0.5:
        return 1.0  # 正常仓位（100%）
    elif 0.5 <= correlation < 0.8:
        return 0.5  # 减仓至 50%
    else:
        return 0.25  # 高相关性，进一步减仓至 25%



# ---------------------------------------------------------------------
# 核心入口：对一桶候选跑相关性门，并返回汇总结果
# ---------------------------------------------------------------------

def run_correlation_gate(
    conn: sqlite3.Connection,
    cfg: dict,
    t_ref: str,
    candidates_after_risk: List[dict],
) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    """
    相关性门（升级版接口，逐票决策 + 审计友好）

    功能：
      - 对每个候选票，计算它跟当前组合的相关性 (rho)。
      - 根据 |rho_star|（1h/4h/1d 里最大绝对相关）决定：
          pass  -> 保持原仓位
          scale -> 半仓 (position_scale_corrgate = 0.5)
          block -> 直接封单
      - 把每个票的决策写进 correlation_snapshot（审计）
      - 返回:
          approved_after_corr   : 可以继续下单的票（含 scale 后的 notional_usd/qty）
          corr_rejected_list    : 被 block 的票 [{symbol,timeframe,direction,reject_reason}]
          corr_info           : 汇总信息，给 risk_summary/snapshot 用

    返回值说明:
      approved_after_corr: List[dict]
        - 来自 candidates_after_risk
        - 对于 pass/scale 的票，我们会把 notional_usd / qty 按 position_scale_corrgate 缩放后再返回
        - 我们也会把 position_scale_corrgate 写回该票，方便后面落库
        - [补丁] 增加 evidence_correlation 字段

      corr_rejected_list: List[dict]
        - 每个元素：
          {
            "symbol": str,
            "timeframe": str,
            "direction": "long" | "short",
            "reject_reason": "correlation_block"
          }
        - [补丁] 增加 evidence_correlation 字段

      corr_info: Dict[str,Any]
        {
          "corr_est": worst_rho_star,         # 桶里最危险（最高绝对相关）的rho_star
          "corr_action": "pass"|"scale"|"block",
          "corr_scale": min_scale_used,       # 最低缩放倍数（没缩即1.0）
          "correlation_block_reason": Optional[str]
        }

    启用/阈值：
      cfg["thresholds"]["correlation_gate"] 支持：
        enable (bool)
        abs_corr_block_threshold (默认0.90)
        abs_corr_scale_threshold (默认0.75)

    依赖的内部工具函数（保持你现有实现）：
      - _build_portfolio_snapshot(conn)
      - _decide_for_candidate_no_portfolio(...)
      - _insert_snapshot_row(conn, t_ref, row)

    注意：
      1. 我们不再用“有一票被block就整桶清零”的做法。
         而是逐票决定谁能上，谁半仓，谁直接拒。
      2. correlation_block 的拒单会传回 decision_engine，进 decision_snapshot(final='skip')。
    """

    # [修改] 定义 now_ts
    tz_name = cfg.get("tz","Asia/Shanghai")
    now_ts = now_local_str(tz_name) # 假定 now_local_str 在此作用域可用

    cg_cfg = ((cfg.get("thresholds") or {}).get("correlation_gate") or {})
    enable_flag = bool(cg_cfg.get("enable", False))

    import math
    from typing import Optional

    # ------- 小工具1: 从DB拿某symbol某周期的close序列（升序） -------
    def _fetch_close_series_for_tf(
        conn_i: sqlite3.Connection,
        symbol: str,
        timeframe: str,
        t_ref_i: str,
        limit: int = 96,
    ) -> List[float]:
        # 容错：不同部署下 K 线表名/时间列名可能不一致
        table_map = {
            "1h":  ["ohlcv_1h", "candles_1h", "ohlcv", "candles"],
            "4h":  ["ohlcv_4h", "candles_4h", "ohlcv", "candles"],
            "1d":  ["ohlcv_1d", "ohlcv_24h", "candles_1d", "ohlcv", "candles"],
        }
        tfs = timeframe.lower().strip()
        tbl_candidates = table_map.get(tfs, ["ohlcv", "candles"])

        closes: List[float] = []
        for tname in tbl_candidates:
            if closes:
                break
            for tcol in ("time", "ts", "open_time", "t", "t_ref"):
                try:
                    rows = conn_i.execute(
                        f"""
                        SELECT close
                        FROM {tname}
                        WHERE symbol=?
                          AND {tcol} <= ?
                        ORDER BY {tcol} DESC
                        LIMIT ?
                        """,
                        (symbol, t_ref_i, limit),
                    ).fetchall()
                except Exception:
                    rows = []

                if not rows:
                    continue

                tmp: List[float] = []
                for r in rows:
                    v = r[0]
                    if v is None:
                        continue
                    try:
                        vv = float(v)
                        if vv > 0:
                            tmp.append(vv)
                    except Exception:
                        pass

                if len(tmp) >= 2:
                    tmp.reverse()  # 我们想要升序
                    closes = tmp
                    break

        return closes

    # ------- 小工具2: 价格序列 -> 简单收益序列 r_t = close_t / close_{t-1} - 1 -------
    def _series_to_returns(closes: List[float]) -> List[float]:
        rets: List[float] = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            cur = closes[i]
            if prev and prev != 0:
                rets.append((cur / prev) - 1.0)
        return rets

    # ------- 小工具3: 用当前组合快照构建组合收益序列 -------
    # 组合逻辑：
    #   portfolio_snap["weights"][sym] = 权重 (按美元notional占比之类)
    #   portfolio_snap["net_dir"][sym] = "long"/"short"
    #   如果当前仓位是short，则把该symbol的收益序列取反
    def _build_portfolio_ret_series(
        conn_i: sqlite3.Connection,
        portfolio_snap: Dict[str, Any],
        timeframe: str,
        t_ref_i: str,
        lookback: int = 96,
    ) -> List[float]:
        weights = dict(portfolio_snap.get("weights") or {})
        dirs = dict(portfolio_snap.get("net_dir") or {})

        per_symbol: Dict[str, Tuple[float, List[float]]] = {}
        min_len: Optional[int] = None

        for psym, w in weights.items():
            closes = _fetch_close_series_for_tf(conn_i, psym, timeframe, t_ref_i, lookback + 1)
            if len(closes) < 2:
                continue
            sym_rets = _series_to_returns(closes)

            if str(dirs.get(psym, "long")).lower().strip() == "short":
                sym_rets = [(-1.0 * r) for r in sym_rets]

            per_symbol[psym] = (float(w), sym_rets)
            if min_len is None or len(sym_rets) < min_len:
                min_len = len(sym_rets)

        if not per_symbol or min_len is None or min_len < 2:
            return []

        port_ret: List[float] = []
        for idx in range(min_len):
            acc = 0.0
            for (w, rets) in per_symbol.values():
                acc += w * rets[idx]
            port_ret.append(acc)

        return port_ret

    # ------- 小工具4: Pearson 相关系数 -------
    # 样本太少(<5) 或某边方差≈0 -> 返回 None
    def _pearson_corr(a: List[float], b: List[float]) -> Optional[float]:
        n = min(len(a), len(b))
        if n < 5:
            return None

        xa = a[-n:]
        xb = b[-n:]

        mean_a = sum(xa) / n
        mean_b = sum(xb) / n

        var_a = sum((x - mean_a) * (x - mean_a) for x in xa)
        var_b = sum((x - mean_b) * (x - mean_b) for x in xb)
        if var_a <= 0 or var_b <= 0:
            return None

        cov = sum((xa[i] - mean_a) * (xb[i] - mean_b) for i in range(n))
        denom = math.sqrt(var_a * var_b)
        if denom <= 0:
            return None
        return cov / denom

    # ------- 小工具5: |rho_star| -> pass/scale/block 判定 -------
    def _decide_scale_from_rho(rho_abs: Optional[float]) -> Tuple[str, float, str, str]:
        block_thr = float(cg_cfg.get("abs_corr_block_threshold", 0.90))
        half_thr = float(cg_cfg.get("abs_corr_scale_threshold", 0.75))

        # 数据不足 -> 保守半仓
        if rho_abs is None:
            return ("scale", 0.5, "insufficient_data_half", "insufficient_data")

        if rho_abs >= block_thr:
            return ("block", 0.0, "high_corr_block", "high_corr_block")

        if rho_abs >= half_thr:
            return ("scale", 0.5, "high_corr_scale", "high_corr_scale")

        return ("pass", 1.0, "low_corr_pass", "low_corr_pass")

    # -------------------------------------------------
    # 1. 组合快照（你现有的函数，保持不动）
    # -------------------------------------------------
    portfolio_snap = _build_portfolio_snapshot(conn) # 假定 _build_portfolio_snapshot 可用
    portfolio_is_empty = (portfolio_snap.get("total_abs_usd", 0.0) <= 0.0)

    # 我们会把每个candidate的计算结果放到 snap_rows，后面既要写DB也要汇总
    snap_rows: List[Dict[str, Any]] = []

    # -------------------------------------------------
    # 2A. 如果还没持仓 -> 用“when_no_portfolio”逻辑
    #     （你原本就有 _decide_for_candidate_no_portfolio）
    # -------------------------------------------------
    if portfolio_is_empty:
        for c in candidates_after_risk:
            sym = c.get("symbol", "")
            tf = c.get("timeframe", "")

            row = _decide_for_candidate_no_portfolio( # 假定 _decide_for_candidate_no_portfolio 可用
                cg_cfg=cg_cfg,
                symbol=sym,
                timeframe=tf,
                portfolio_snap=portfolio_snap,
            )
            # _decide_for_candidate_no_portfolio() 需要确保返回字段与我们后面要求一致：
            #   decision / position_scale / rho_* / rho_star / reason / etc.
            # 如果你之前版本的返回里没有 direction/timeframe，可以在这里补：
            row.setdefault("symbol", sym)
            row.setdefault("timeframe", tf)
            row.setdefault("rho_1h", 0.0)
            row.setdefault("rho_4h", 0.0)
            row.setdefault("rho_1d", 0.0)
            row.setdefault("rho_star", 0.0)
            row.setdefault("rho_basis", "nopos")
            row.setdefault("decision", "pass" if enable_flag else "pass")
            row.setdefault("position_scale", 1.0)
            row.setdefault("calc_ok", 1)
            row.setdefault("status", "ok")
            row.setdefault("reason", row.get("reason", "nopos"))
            row.setdefault("portfolio_json", json.dumps(portfolio_snap, ensure_ascii=False))
            row.setdefault("samples_1h", 0)
            row.setdefault("samples_4h", 0)
            row.setdefault("samples_1d", 0)

            snap_rows.append(row)

    # -------------------------------------------------
    # 2B. 如果组合非空 -> 真正走相关性计算
    # -------------------------------------------------
    else:
        # 先缓存组合的收益序列，避免重复计算
        portfolio_ret_cache: Dict[str, List[float]] = {}

        def _get_portfolio_ret(tf_name: str, lookback: int) -> List[float]:
            key = f"{tf_name}:{lookback}"
            if key not in portfolio_ret_cache:
                portfolio_ret_cache[key] = _build_portfolio_ret_series(
                    conn_i=conn,
                    portfolio_snap=portfolio_snap,
                    timeframe=tf_name,
                    t_ref_i=t_ref,
                    lookback=lookback,
                )
            return portfolio_ret_cache[key]

        for c in candidates_after_risk:
            sym = c.get("symbol", "")
            tf = c.get("timeframe", "")

            # 候选票自己的价格->收益序列
            closes_1h = _fetch_close_series_for_tf(conn, sym, "1h", t_ref, limit=96 + 1)
            rets_1h = _series_to_returns(closes_1h)

            closes_4h = _fetch_close_series_for_tf(conn, sym, "4h", t_ref, limit=96 + 1)
            rets_4h = _series_to_returns(closes_4h)

            closes_1d = _fetch_close_series_for_tf(conn, sym, "1d", t_ref, limit=30 + 1)
            rets_1d = _series_to_returns(closes_1d)

            # 组合的收益序列
            port_1h = _get_portfolio_ret("1h", 96)
            port_4h = _get_portfolio_ret("4h", 96)
            port_1d = _get_portfolio_ret("1d", 30)

            # Pearson 相关
            rho_1h = _pearson_corr(rets_1h, port_1h)
            rho_4h = _pearson_corr(rets_4h, port_4h)
            rho_1d = _pearson_corr(rets_1d, port_1d)

            rho_abs_list = [abs(r) for r in (rho_1h, rho_4h, rho_1d) if r is not None]
            if rho_abs_list:
                rho_star_val = max(rho_abs_list)
                calc_ok = 1
                status = "ok"
                reason_calc = None
            else:
                rho_star_val = 0.0
                calc_ok = 0
                status = "neutral"
                reason_calc = "insufficient_data"

            decision, scale_val, note, reason_rule = _decide_scale_from_rho(
                rho_star_val if rho_abs_list else None
            )
            final_reason = reason_calc if reason_calc else reason_rule

            row = {
                "symbol": sym,
                "timeframe": tf,
                "rho_1h": rho_1h if rho_1h is not None else 0.0,
                "rho_4h": rho_4h if rho_4h is not None else 0.0,
                "rho_1d": rho_1d if rho_1d is not None else 0.0,
                "rho_star": rho_star_val,
                "rho_basis": "portfolio",
                "decision": decision,        # 'pass' | 'scale' | 'block'
                "position_scale": scale_val,     # 1.0 / 0.5 / 0.0
                "scale_note": note,          # 描述
                "samples_1h": len(rets_1h),
                "samples_4h": len(rets_4h),
                "samples_1d": len(rets_1d),
                "calc_ok": calc_ok,
                "status": status,
                "reason": final_reason,
                "portfolio_json": json.dumps(portfolio_snap, ensure_ascii=False),
            }
            snap_rows.append(row)

    # 如果相关性门根本没开 (enable=False)，我们还是要有 snap_rows，但我们不缩不挡
    # 注意：上面 row.setdefault("decision"... ) 在空仓分支已经保证有 decision/position_scale
    if not enable_flag:
        for row in snap_rows:
            row["decision"] = "pass"
            row["position_scale"] = 1.0
            row.setdefault("rho_star", 0.0)

    # -------------------------------------------------
    # 3. 写 correlation_snapshot
    #    你已有的 _insert_snapshot_row 做 upsert: (t_ref, symbol, timeframe)
    # -------------------------------------------------
    # 3. 写 correlation_snapshot 之后
    for row in snap_rows:
        _insert_snapshot_row(conn, t_ref, row, now_ts)

    # ← 加上这段
    try:
        conn.commit()
    except Exception as e:
        print(f"[correlation_gate] commit failed: {e}")


    # -------------------------------------------------
    # 4. 汇总 & 生成输出
    #    我们不再“全桶封杀”，而是逐票封杀
    # -------------------------------------------------
    approved_after_corr: List[dict] = []
    corr_rejected_list: List[dict] = []

    worst_rho = 0.0       # 桶里最大的 rho_star（绝对相关最高）
    min_scale_used = 1.0  # 桶里最小的缩放系数，越小表示我们越保守
    any_scaled = False
    any_blocked = False

    # ------- [补丁 A] 插入阈值定义 -------
    # 阈值（仅用于 evidence 展示；判定仍沿用 _decide_scale_from_rho）
    thr_block = float(cg_cfg.get("abs_corr_block_threshold", 0.90))
    thr_scale = float(cg_cfg.get("abs_corr_scale_threshold", 0.75))
    # ------------------------------------

    for idx, c in enumerate(candidates_after_risk):
        row = snap_rows[idx]
        sym = c.get("symbol", "")
        tf = c.get("timeframe") or "1h"
        direc = (c.get("direction") or "").lower().strip()

        rho_star_val = float(row.get("rho_star", 0.0) or 0.0)
        if rho_star_val > worst_rho:
            worst_rho = rho_star_val

        scale_i = float(row.get("position_scale", 1.0) or 1.0)
        if scale_i < min_scale_used:
            min_scale_used = scale_i

        decision_i = str(row.get("decision", "")).lower().strip()

        # ------- [补丁 B] 插入 ev 证据对象 -------
        # 证据打点（仅新增字段，不影响原逻辑）
        ev = {
            "rho_1h": row.get("rho_1h"),
            "rho_4h": row.get("rho_4h"),
            "rho_1d": row.get("rho_1d"),
            "rho_star": row.get("rho_star"),
            "basis": row.get("rho_basis"),
            "decision": row.get("decision"),
            "position_scale": row.get("position_scale"),
            "samples_1h": row.get("samples_1h"),
            "samples_4h": row.get("samples_4h"),
            "samples_1d": row.get("samples_1d"),
            "thresholds": {
                "abs_corr_scale_threshold": thr_scale,
                "abs_corr_block_threshold": thr_block,
            },
            "source": {
                "table": "correlation_snapshot",
                "t_ref": t_ref,
            },
        }
        # ----------------------------------------

        # ------- [补丁 C] 替换 block 分支 -------
        if decision_i == "block" or scale_i <= 0.0:
            any_blocked = True
            item = {
                "symbol": sym,
                "timeframe": tf,
                "direction": direc,
                "reject_reason": "correlation_block",
            }
            item["evidence_correlation"] = ev  # ★仅新增字段
            corr_rejected_list.append(item)
            continue
        # ----------------------------------------

        if scale_i < 0.999:
            any_scaled = True

        # 对通过/半仓的票，按相关性scale缩仓（notional_usd / qty 调整）
        new_c = dict(c)

        if "notional_usd" in new_c and new_c["notional_usd"] is not None:
            try:
                new_c["notional_usd"] = float(new_c["notional_usd"]) * scale_i
            except (TypeError, ValueError):
                pass

        if "qty" in new_c and new_c["qty"] is not None:
            try:
                new_c["qty"] = float(new_c["qty"]) * scale_i
            except (TypeError, ValueError):
                pass

        # ------- [补丁 D] 插入 evidence 字段 -------
        new_c["evidence_correlation"] = ev
        # ------------------------------------------
        new_c["position_scale_corrgate"] = scale_i
        approved_after_corr.append(new_c)

    # corr_info 是“桶级总结”，给 risk_summary / snapshot 用
    if any_blocked:
        corr_action = "block"
        corr_block_reason = "correlation_gate_block"
    elif any_scaled:
        corr_action = "scale"
        corr_block_reason = None
    else:
        corr_action = "pass"
        corr_block_reason = None

    corr_info: Dict[str, Any] = {
        "corr_est": worst_rho,
        "corr_action": corr_action,  # "pass" | "scale" | "block"
        "corr_scale": min_scale_used,
    }
    if corr_block_reason:
        corr_info["correlation_block_reason"] = corr_block_reason

    return approved_after_corr, corr_rejected_list, corr_info