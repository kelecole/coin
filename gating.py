from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Tuple
import math
import json
# [修改] 移除 datetime, 导入 timebox
from core.timebox import now_local_str

# ---------------------------------------------------------------------
# Step 1: 把候选信号整理成简单结构
# ---------------------------------------------------------------------
from .liquidity_filter import prefilter_candidates_with_liquidity_filter



def prefilter_candidates_for_breadth(
    candidates: List[Dict[str, Any]],
    cfg: dict,
) -> List[Dict[str, Any]]:
    """
    输入：run_pipeline 里 after_cd 的候选信号
        每条至少有：
          symbol, timeframe, direction, prob, score, strength, source
          
    输出：一个 list，元素形如：
        {
          "symbol": "DYDX/USDT",
          "direction": "long" / "short",
          "timeframe": "1h",
          "score": <float or None>,
          "prob": <float or None>,
          "strength": <float or None>,
          "source": <str>
        }
    """
    out: List[Dict[str, Any]] = []

    # 从配置中获取市场广度要求
    min_universe = cfg.get("thresholds", {}).get("breadth", {}).get("min_universe", 3)
    
    # 获取所有候选标的的符号
    symbols = [c.get("symbol") for c in candidates]

    # 先做流动性过滤
    valid_symbols = prefilter_candidates_with_liquidity_filter(symbols, cfg)


    for c in candidates:
        sym = c.get("symbol")
        if sym not in valid_symbols:
            continue  # 如果不在有效标的列表中，则跳过

        direct = c.get("direction")
        tf = c.get("timeframe")
        out.append({
            "symbol": sym,
            "direction": direct,
            "timeframe": tf,
            "score": _to_float_or_none(c.get("score")),
            "prob": _to_float_or_none(c.get("prob")),
            "strength": _to_float_or_none(c.get("strength")),
            "source": c.get("source"),
        })

    return out

def _to_float_or_none(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None

# ---------------------------------------------------------------------
# Step 2: 计算广度 & 决定是否整桶阻断
# ---------------------------------------------------------------------


def compute_breadth_gate(
    conn: sqlite3.Connection,
    cfg: dict,
    t_ref: str,
    breadth_input: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    返回:
      breadth_gate: {"gate": "pass"|"block", "reason": str}
      breadth_info: dict，包含我们要落表和要喂 decision_engine 的信息

    同时会把一个宽度快照写入 breadth_snapshot 表，字段对齐你现有库：
        CREATE TABLE breadth_snapshot(
          t_ref TEXT PRIMARY KEY,
          universe_size INTEGER NOT NULL,
          advancers INTEGER NOT NULL,
          decliners INTEGER NOT NULL,
          ad_ratio REAL NOT NULL,
          new_high INTEGER,
          new_low INTEGER,
          notes TEXT,
          created_at TEXT NOT NULL
        );
    """
    thr_b = ((cfg.get("thresholds") or {}).get("breadth") or {})
    enforce = bool(thr_b.get("enforce", True))

    min_universe = _safe_float(thr_b.get("min_universe"), 3.0)
    no_dom_lo = _safe_float(thr_b.get("no_dominant_ratio_low"), 0.80)
    no_dom_hi = _safe_float(thr_b.get("no_dominant_ratio_high"), 1.25)

    knife_adv_ratio = None
    knife_cfg = thr_b.get("knife_block_long", {}) or {}
    if isinstance(knife_cfg, dict):
        knife_adv_ratio = _safe_float(knife_cfg.get("adv_vs_dec_ratio"), None)

    # ---- 先进行流动性过滤
    symbols = [row.get("symbol") for row in breadth_input]
    valid_symbols = prefilter_candidates_with_liquidity_filter(symbols, cfg)


    # 过滤掉不符合流动性要求的标的
    breadth_input = [row for row in breadth_input if row.get("symbol") in valid_symbols]

    # ---- 统计
    symbols_seen = set()
    advancers = 0
    decliners = 0

    for row in breadth_input:
        sym = row.get("symbol")
        direction = (row.get("direction") or "").lower()
        if sym:
            symbols_seen.add(sym)
        if direction == "long":
            advancers += 1
        elif direction == "short":
            decliners += 1

    universe_size = len(symbols_seen)
    if decliners <= 0:
        ad_ratio = float(advancers) if advancers > 0 else 0.0
    else:
        ad_ratio = advancers / float(decliners)

    # bias: 市场主线倾向
    if advancers > decliners:
        bias = "long"
    elif decliners > advancers:
        bias = "short"
    else:
        bias = "neutral"

    # panic_selloff: 先固定 False（BTC杀跌检测逻辑后面单独接）
    panic_selloff = False

    # ---- 广度门判断
    gate = "pass"
    reason = "ok"

    if enforce:
        # 1) 样本太少，不开
        if universe_size < min_universe:
            gate = "block"
            reason = "too_thin_universe"
        else:
            # 2) 多空互撕，无主线，不开
            if ad_ratio >= no_dom_lo and ad_ratio <= no_dom_hi:
                gate = "block"
                reason = "no_dominant_trend"

    # ---- breadth_info 输出
    breadth_info: Dict[str, Any] = {
        "t_ref": t_ref,
        "universe_size": universe_size,
        "advancers": advancers,
        "decliners": decliners,
        "ad_ratio": ad_ratio,
        "bias": bias,
        "panic_selloff": panic_selloff,
    }

    # ---- 写 breadth_snapshot
    _upsert_breadth_snapshot(
        conn=conn,
        t_ref=t_ref,
        universe_size=universe_size,
        advancers=advancers,
        decliners=decliners,
        ad_ratio=ad_ratio,
        bias=bias,
        panic_selloff=panic_selloff,
        cfg=cfg # [修改] 传入 cfg
    )

    breadth_gate = {
        "gate": gate,        # "pass" or "block"
        "reason": reason,    # why
    }

    return breadth_gate, breadth_info

def _safe_float(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _upsert_breadth_snapshot(
    conn: sqlite3.Connection,
    t_ref: str,
    universe_size: int,
    advancers: int,
    decliners: int,
    ad_ratio: float,
    bias: str,
    panic_selloff: bool,
    cfg: dict # [修改] 增加 cfg 参数
) -> None:
    """
    往 breadth_snapshot 写/更新一行。
    表结构是现网的：
        t_ref TEXT PRIMARY KEY
        universe_size INTEGER
        advancers INTEGER
        decliners INTEGER
        ad_ratio REAL
        new_high INTEGER
        new_low INTEGER
        notes TEXT
        created_at TEXT
    我们不动 new_high/new_low，先塞 NULL，
    notes 就放个JSON（bias / panic_selloff / ts）
    """
    # [修改] 使用 now_local_str
    now_ts = now_local_str(cfg.get("tz","Asia/Shanghai"))
    
    notes_obj = {
        "bias": bias,
        "panic_selloff": panic_selloff,
        "ts": now_ts, # [修改] 使用 now_ts
    }
    notes_json = json.dumps(notes_obj, ensure_ascii=False)

    # INSERT OR REPLACE 可以用，因为 breadth_snapshot.t_ref 是 PRIMARY KEY
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO breadth_snapshot(
                t_ref,
                universe_size,
                advancers,
                decliners,
                ad_ratio,
                new_high,
                new_low,
                notes,
                created_at
            ) VALUES (
                ?, ?, ?, ?, ?, NULL, NULL, ?, ?
            )
            """, # [修改] created_at 改为参数 ?
            (
                t_ref,
                int(universe_size),
                int(advancers),
                int(decliners),
                float(ad_ratio),
                notes_json,
                now_ts, # [修改] 传入 now_ts
            ),
        )
    except Exception:
        # 最后兜底，不让主流程死
        pass