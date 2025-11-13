# -*- coding: utf-8 -*-
"""
core/pipeline_runner.py

目标：把“TTL 口径吃信号”改为“逐桶按 (t_ref,timeframe) 吃信号”，并天然具备补桶能力。
提供：
- list_backlog_buckets(...)  列出需要处理的 (t_ref,timeframe)
- iter_buckets(...)          逐桶产出 pipeline_state（阈值/冷却/广度/BTC 对齐 全按该桶 t_ref）
- run_pipeline(...)          兼容旧入口：只返回“下一个待处理桶”的 pipeline_state

说明：
- 不在这里落单，也不写心跳；这些仍由 run_open_cycle.py 调用 decision_engine / execution / ops_heartbeat 完成
- BTC 对齐使用 analyze_alignment_all(...)，结果与该桶 t_ref 绑定，并写入 btc_alignment_snapshot
"""

from __future__ import annotations
import sqlite3, time
from typing import Any, Dict, List, Tuple, Iterable, Optional

# --- imports ---
try:
    from core.timebox import now_local_dt, now_local_str
    from core.avwap import _last_closed_tref
    from core import btc_alignment, gating
except Exception:
    from .timebox import now_local_dt, now_local_str
    from .avwap import _last_closed_tref
    from . import btc_alignment, gating


# -------------------------------
# 基础：当前墙上小时 t_ref（向下取整）
# -------------------------------
def _current_hour_t_ref(cfg: dict) -> str:
    tz = cfg.get("tz", "Asia/Shanghai")
    now = now_local_dt(tz)
    now_floor = now.replace(minute=0, second=0, microsecond=0)
    return now_floor.strftime("%Y-%m-%d %H:%M:%S")


# -------------------------------
# 工具：按 (t_ref,timeframe) 精确取候选
# -------------------------------
def _load_signals_strict_bucket(
    conn: sqlite3.Connection,
    t_ref: str,
    timeframe: str,
) -> List[Dict[str, Any]]:
    """
    严格按 (t_ref, timeframe) 取该桶候选信号。
    - status 口径：大小写无关，兼容 NEW/ACTIVE
    - timeframe 比较：忽略大小写
    - 保持既有排序：score DESC, created_at ASC
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, t_ref, symbol, timeframe, source, direction,
               score, prob, strength, detail_json, status, expires_at, created_at
        FROM signals
        WHERE UPPER(status) IN ('NEW','ACTIVE')
          AND t_ref = ?
          AND LOWER(timeframe) = LOWER(?)
        ORDER BY score DESC, created_at ASC
        """,
        (t_ref, timeframe),
    ).fetchall()
    return [dict(r) for r in rows]


# -------------------------------
# 阈值过滤（最小实现：尊重 cfg.thresholds.signal）
# -------------------------------
def _apply_signal_thresholds(
    candidates: List[Dict[str, Any]],
    cfg: dict,
) -> List[Dict[str, Any]]:
    thr = (cfg.get("thresholds") or {}).get("signal") or {}
    min_score = thr.get("min_score", None)
    min_prob = thr.get("min_prob", None)
    min_strength = thr.get("min_strength", None)

    def ok_num(v, mn):
        if mn is None: 
            return True
        try:
            return float(v) >= float(mn)
        except Exception:
            return False

    out: List[Dict[str, Any]] = []
    for c in candidates:
        if not ok_num(c.get("score"), min_score):
            continue
        if not ok_num(c.get("prob"), min_prob):
            continue
        if not ok_num(c.get("strength"), min_strength):
            continue
        out.append(c)
    return out


# -------------------------------
# 冷却 + 冲突过滤（最小可用版）
# -------------------------------
def _cooldown_and_conflict_filter(
    conn: sqlite3.Connection,
    candidates: List[Dict[str, Any]],
    cfg: dict,
    t_ref: str,
) -> List[Dict[str, Any]]:
    # 1) 同票多空冲突：保留分数更高的
    best: Dict[str, Dict[str, Any]] = {}
    for c in candidates:
        sym = c.get("symbol")
        if not sym:
            continue
        prev = best.get(sym)
        if prev is None or (float(c.get("score") or 0) > float(prev.get("score") or 0)):
            best[sym] = c
    dedup = list(best.values())

    # 2) 冷却：已有 OPEN 仓位 / 刚平仓不久
    cooldown_cfg = (cfg.get("cooldown") or {})
    hold_min = int(cooldown_cfg.get("min_hold_minutes", 0))

    open_syms = set()
    recent_closed = set()
    try:
        # OPEN
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM positions_virtual WHERE status='OPEN';"
        ).fetchall()
        for r in rows:
            open_syms.add(r[0])
    except Exception:
        pass

    try:
        if hold_min > 0:
            rows = conn.execute(
                """
                SELECT DISTINCT symbol FROM positions_virtual
                WHERE status='CLOSED' AND closed_at >= datetime(?, ?)
                """,
                (t_ref, f"-{hold_min} minutes"),
            ).fetchall()
            for r in rows:
                recent_closed.add(r[0])
    except Exception:
        pass

    out: List[Dict[str, Any]] = []
    for c in dedup:
        sym = c.get("symbol")
        if sym in open_syms:
            continue
        if sym in recent_closed:
            continue
        out.append(c)
    return out


# -------------------------------
# 广度 + 主方向（会写 breadth_snapshot@t_ref）
# -------------------------------
def _compute_breadth_and_bias(
    conn: sqlite3.Connection,
    cfg: dict,
    t_ref: str,
    filtered_signals: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
    # gating 需要简化的输入（symbol/direction/timeframe）
    simple = []
    for c in filtered_signals:
        simple.append(
            {
                "symbol": c.get("symbol"),
                "direction": (c.get("direction") or "").lower(),
                "timeframe": (c.get("timeframe") or "").lower(),
                "score": c.get("score"),
                "prob": c.get("prob"),
            }
        )
    breadth_gate, breadth_info = gating.compute_breadth_gate(
        conn=conn, cfg=cfg, t_ref=t_ref, breadth_input=simple
    )
    reason = (breadth_gate.get("reason") or "").lower()
    panic_flag = ("panic" in reason) or ("knife" in reason)
    return breadth_gate, breadth_info, panic_flag


# -------------------------------
# BTC 对齐（按该桶即时计算并落表）
# -------------------------------
def _compute_btc_alignment_map(
    conn: sqlite3.Connection,
    cfg: dict,
    t_ref: str,
    candidates: List[Dict[str, Any]],
) -> Dict[str, dict]:
    try:
        return btc_alignment.analyze_alignment_all(
            conn=conn, cfg=cfg, t_ref=t_ref, candidates=candidates
        )
    except Exception as e:
        print(f"[pipeline] BTC alignment analyze failed: {e}")
        return {}


def _distinct_timeframes_from_signals(conn: sqlite3.Connection) -> List[str]:
    """
    从 signals 枚举可用 timeframe。
    - 修复：status 大小写无关（NEW/ACTIVE 都算）
    - 返回已去重的 timeframe 列表（原样大小写）
    """
    conn.row_factory = None
    rows = conn.execute(
        """
        SELECT DISTINCT timeframe
        FROM signals
        WHERE UPPER(status) IN ('NEW','ACTIVE')
        """
    ).fetchall()
    return [r[0] for r in rows]


def run_pipeline_batch(
    conn: sqlite3.Connection,
    cfg: dict,
    max_buckets_per_run: int = 6,
) -> List[Dict[str, Any]]:
    """
    一次取最多 N 个待处理桶（从最旧开始），用于入口脚本循环消费。
    不落单、不写心跳——只负责产出逐桶的 pipeline_state。
    """
    out: List[Dict[str, Any]] = []
    n = 0
    for state in iter_buckets(conn, cfg):
        out.append(state)
        n += 1
        if n >= int(max_buckets_per_run):
            break
    return out


# -------------------------------
# 关键：列 backlog（需要“补/跑”的桶）
# 规则：
#   - 从 signals(status='active') 提取 (t_ref,timeframe)
#   - t_ref 必须 <= 该 timeframe 的“最后一根已收盘K”
#   - 过滤掉已经存在 decision_snapshot 的桶
#   - 限定回溯窗口（pipeline.backfill_hours，默认 24h）
# -------------------------------
def list_backlog_buckets(
    conn: sqlite3.Connection,
    cfg: dict,
) -> List[Tuple[str, str]]:
    """
    逐桶待处理列表（含补桶 2.0）：
    - 近 backfill_hours 内，signals 出现过的 (t_ref,timeframe)；
    - 仅使用闭合K（由上层调用保证，如必要可在此加限制）；
    - 若该桶尚未决策 -> 进入 backlog；
    - 若该桶已决策，但后来有“更晚写入”的同桶信号(created_at > 决策的 decided_at) -> 进入 backlog（二次处理）。
    - 额外优化：若该桶存在 __BUCKET_NOOP__ 快照，且没有“晚于 NOOP 的新信号”，则直接跳过以减少 I/O。
    - 大小写口径统一：status/timeframe 忽略大小写。
    """
    tz = cfg.get("tz", "Asia/Shanghai")
    asof = now_local_str(tz)
    pipe_cfg = (cfg.get("pipeline") or {})
    backfill_hours = int(pipe_cfg.get("backfill_hours", 24))
    reprocess_late = bool(pipe_cfg.get("reprocess_late", True))

    # 1) 候选桶：近 backfill_hours 内 signals 出现过的 (t_ref, timeframe)
    #    同时拿到该桶“最新信号创建时间”用于和 NOOP/已决策时间比较
    cand_rows = conn.execute(
        """
        SELECT
            t_ref,
            LOWER(timeframe) AS timeframe,
            MAX(COALESCE(created_at, t_ref)) AS last_sig_created_at
        FROM signals
        WHERE UPPER(status) IN ('NEW','ACTIVE')
          AND t_ref >= datetime(?, ?)
        GROUP BY t_ref, LOWER(timeframe)
        ORDER BY t_ref ASC
        """,
        (asof, f"-{backfill_hours} hours"),
    ).fetchall()
    # 规范化
    candidates: List[Tuple[str, str]] = [(r["t_ref"], r["timeframe"]) for r in cand_rows]
    sig_latest_map: Dict[Tuple[str, str], str] = {
        (r["t_ref"], r["timeframe"]): (r["last_sig_created_at"] or r["t_ref"])
        for r in cand_rows
    }

    if not candidates:
        return []

    # 2) 已决策桶 -> 以 MAX(created_at) 作为 decided_at
    dec_rows = conn.execute(
        """
        SELECT t_ref, LOWER(timeframe) AS timeframe,
               MAX(COALESCE(created_at, t_ref)) AS decided_at
        FROM decision_snapshot
        GROUP BY t_ref, LOWER(timeframe)
        """
    ).fetchall()
    decided_map: Dict[Tuple[str, str], str] = {
        (r["t_ref"], r["timeframe"]): (r["decided_at"] or r["t_ref"])
        for r in dec_rows
    }

    # 3) NOOP 桶（__BUCKET_NOOP__） -> 取该 NOOP 的 created_at
    noop_rows = conn.execute(
        """
        SELECT t_ref, LOWER(timeframe) AS timeframe,
               MAX(COALESCE(created_at, t_ref)) AS noop_at
        FROM decision_snapshot
        WHERE symbol='__BUCKET_NOOP__'
        GROUP BY t_ref, LOWER(timeframe)
        """
    ).fetchall()
    noop_map: Dict[Tuple[str, str], str] = {
        (r["t_ref"], r["timeframe"]): (r["noop_at"] or r["t_ref"])
        for r in noop_rows
    }

    # 4) 组装 backlog
    backlog: List[Tuple[str, str]] = []
    backlog_set: Set[Tuple[str, str]] = set()

    for (t_ref, tf) in candidates:
        key = (t_ref, tf)
        last_sig_at = sig_latest_map.get(key, t_ref)

        # 4.1) 若该桶存在 NOOP，且没有“晚于 NOOP 的新信号”，直接跳过
        noop_at = noop_map.get(key)
        if noop_at is not None:
            # reprocess_late 为 True 时，只有“有更晚信号”才重入；否则一律跳过
            if (not reprocess_late) or (last_sig_at <= noop_at):
                # 无需处理此桶
                continue
            # 有更晚信号，允许继续按“晚到信号重处理”逻辑走

        decided_at = decided_map.get(key)
        if decided_at is None:
            # 从未决策，直接进入 backlog
            if key not in backlog_set:
                backlog.append(key)
                backlog_set.add(key)
            continue

        # 已决策：仅当存在“晚到信号”才重新进入 backlog
        if reprocess_late and last_sig_at > decided_at:
            if key not in backlog_set:
                backlog.append(key)
                backlog_set.add(key)

    # 5) 结果按时间升序（t_ref ASC, timeframe 排序稳定）
    backlog.sort(key=lambda x: (x[0], x[1]))
    return backlog




# -------------------------------
# 逐桶产出 pipeline_state（阈值/冷却/广度/BTC对齐）
# -------------------------------
def iter_buckets(
    conn: sqlite3.Connection,
    cfg: dict,
) -> Iterable[Dict[str, Any]]:
    backlog = list_backlog_buckets(conn, cfg)
    if not backlog:
        # 兜底：当前小时任何 TF 有信号就跑；没有则直接返回
        cur_tref = _current_hour_t_ref(cfg)
        rows = conn.execute(
            """
            SELECT DISTINCT timeframe
            FROM signals
            WHERE UPPER(status) IN ('NEW','ACTIVE')
              AND t_ref = ?
            """,
            (cur_tref,),
        ).fetchall()
        tfs = [(cur_tref, (r[0] or "").lower()) for r in rows]
        backlog = tfs
    for (t_ref, tf) in backlog:
        print(f"[pipeline] 逐桶处理 t_ref={t_ref} tf={tf}")

        raw = _load_signals_strict_bucket(conn, t_ref, tf)
        after_thr = _apply_signal_thresholds(raw, cfg)
        after_cd = _cooldown_and_conflict_filter(conn, after_thr, cfg, t_ref)

        breadth_gate, breadth_info, panic_flag = _compute_breadth_and_bias(
            conn, cfg, t_ref, after_cd
        )
        btc_map = _compute_btc_alignment_map(conn, cfg, t_ref, after_cd)

        yield {
            "t_ref": t_ref,
            "timeframe": tf,
            "candidates_ready": after_cd,
            "breadth_info": breadth_info,
            "breadth_gate": breadth_gate,
            "market_ctx": {"panic_selloff": bool(panic_flag)},
            "btc_alignment": btc_map,
        }


# -------------------------------
# 兼容旧入口：只返回“下一个待处理桶”的 pipeline_state
# -------------------------------
def run_pipeline(conn: sqlite3.Connection, cfg: dict) -> Dict[str, Any]:
    for state in iter_buckets(conn, cfg):
        # 只取第一个（最旧的 backlog 桶）；其余由调用方决定是否继续消费
        return state
    # 没有可跑的，返回空状态
    cur = _current_hour_t_ref(cfg)
    return {
        "t_ref": cur,
        "timeframe": None,
        "candidates_ready": [],
        "breadth_info": {},
        "breadth_gate": {"gate": "pass", "reason": "no_candidates"},
        "market_ctx": {"panic_selloff": False},
        "btc_alignment": {},
    }
