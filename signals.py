# -*- coding: utf-8 -*-
"""
core/signals.py

职责（从旧 run_bucket_pipeline.py 拆出来）：
1. 从 DB 的 signals 表里，拉出本桶 t_ref 的原始候选，并根据配置里的阈值做基本筛选。
   -> _collect_candidates_with_thresholds()

2. 安全过滤：
   - 冷却过滤（同一币刚平仓没过冷却窗口/还在持仓，就别再上）：
     -> _cooldown_filter_candidates()
   - 多周期/多方向冲突过滤（同一个 symbol 既有 long 又有 short，整票丢掉）：
     -> _reject_multi_tf_conflict()

3. 阈值解释工具：
   - _thr_choose_value_from_candidate()
   - _eval_threshold_for_decision()

注意：
- 我们不在这里决定仓位大小，不在这里做风险花费，不在这里做市场广度/大盘状态。
- 我们也不在这里下单，纯粹是「拿出这一桶能考虑的候选」。
- t_ref 是这一桶的时间戳（字符串，形如 "YYYY-MM-DD HH:MM:SS"）。

后续流程（pipeline_runner）会按顺序调用：
    candidates, thr_map = _collect_candidates_with_thresholds(conn, t_ref, cfg)
    conflict_info = _reject_multi_tf_conflict(candidates)
    cool_info = _cooldown_filter_candidates(conn, t_ref, cfg, conflict_info["kept"])

然后才会进 gating / decision_engine 等后续步骤。
"""
def _collect_candidates_with_thresholds(conn, t_ref: str, cfg: dict):
    """
    读取 signals/thresholds，产出 “已过线候选” 的 list[dict]，并返回阈值映射。

    统一返回形态：每个候选都是 dict，至少包含：
        {
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "source": "...",
            "prob": <float|None>,
            "score": <float|None>,
            "strength": <float|None>,
            "direction": "long"|"short"|...,
            "strategy_version": "v1.0.0"
        }

    返回:
        candidates: list[dict]
        thr_map:    dict[source -> rule]
    """
    import sqlite3

    # 策略版本
    strategy_version = str(cfg.get("strategy_version", "v1.0.0"))

    # 阈值配置
    thr_cfg = (cfg.get("thresholds") or {}).get("signal") or {}

    # 优先级：candidates.include_sources > thresholds.signal.keys() > 默认源
    # 新增 sig_confluence_reclaim；保留三张牌与 IAR
    default_sources = [
        "chip_llm", "candle_llm", "triangle_raw",
        "sig_vpoc_flip", "sig_avwap_flip", "sig_sqz_br",
        "sig_iar_revert", "sig_confluence_reclaim",
    ]
    cfg_sources = (cfg.get("candidates") or {}).get("include_sources") or []
    thr_keys = list(thr_cfg.keys()) if isinstance(thr_cfg, dict) else []

    def _uniq_keep_order(seq):
        seen = set()
        out = []
        for x in seq:
            if x is None:
                continue
            s = str(x).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out

    include_sources = _uniq_keep_order(cfg_sources or thr_keys or default_sources)

    # 查询信号（动态占位）
    q = f"""
    SELECT t_ref, symbol, timeframe, source, direction,
           prob, score, strength, status, expires_at, created_at
    FROM signals
    WHERE t_ref = ?
      AND source IN ({",".join(["?"]*len(include_sources))})
      AND UPPER(status) IN ('NEW','ACTIVE')
    """
    rows = conn.execute(q, (t_ref, *include_sources)).fetchall()

    # 阈值判定
    def _passed_threshold(row):
        src = row.get("source")
        rule = (thr_cfg.get(src) or {})
        metric = (rule.get("metric") or "score").lower()
        minv = rule.get("min")
        if minv is None:
            return True

        if metric == "prob":
            val = row.get("prob")
        elif metric == "score":
            val = row.get("score")
        elif metric == "strength":
            val = row.get("strength")
        else:
            val = next((row.get(k) for k in ("prob", "score", "strength") if row.get(k) is not None), None)

        try:
            v = 0.0 if (val is None) else float(val)
            return v >= float(minv)
        except Exception:
            return False

    # 收集通过阈值的候选
    candidates, before = [], 0
    for r in rows:
        if isinstance(r, sqlite3.Row):
            d = dict(r)
        else:
            d = {
                "t_ref": r[0],
                "symbol": r[1],
                "timeframe": r[2],
                "source": r[3],
                "direction": r[4],
                "prob": r[5],
                "score": r[6],
                "strength": r[7],
                "status": r[8],
                "expires_at": r[9],
                "created_at": r[10],
            }

        d.setdefault("timeframe", "1h")
        before += 1
        if _passed_threshold(d):
            d["strategy_version"] = strategy_version
            candidates.append(d)

    try:
        print(f"[candidates] after_thr n={len(candidates)} (before_thr={before}); sources={include_sources}")
    except Exception:
        pass

    thr_map = {s: (thr_cfg.get(s) or {}) for s in include_sources}
    return candidates, thr_map




def _thr_choose_value_from_candidate(c: dict, prefer_metric: str):
    """
    在候选 c 中按 prefer_metric 优先取值；
    若该字段是 None，则回退到 prob->score->strength 的顺序。

    返回 (value, used_metric)
    - 如果所有字段都拿不到，返回 (None, None)
    """
    prefer_metric = (prefer_metric or "prob").lower()
    order = [prefer_metric] + [
        m for m in ("prob", "score", "strength") if m != prefer_metric
    ]
    for m in order:
        v = c.get(m)
        if v is not None:
            try:
                return float(v), m
            except Exception:
                pass
    return None, None


def _eval_threshold_for_decision(c: dict, cfg: dict) -> dict:
    """
    阈值门判定（带兜底）：
      - 优先用 config 的 metric（比如 "prob" 或 "score"）
      - 如果那个字段是 None，就自动回退到 prob->score->strength
      - 如果最后还是 None 且 min<=0，就当 0.0 来判断
        （这样 0 也能过一个非常宽松的门）
      - 给出 whether 通过

    返回：
        {
            "metric":      <str>   # 用的哪种度量
            "min":         <float> # cfg里要求的最小值
            "value":       <float|None> # 实际拿到的值
            "used_metric": <str|None>   # 最终用哪列
            "passed":      <bool>
        }
    """
    src = c.get("source")
    thr_cfg = ((cfg or {}).get("thresholds") or {}).get("signal") or {}
    metric = (thr_cfg.get(src, {}).get("metric") or "prob").lower()
    minv = float(thr_cfg.get(src, {}).get("min") or 0.0)

    val, used = _thr_choose_value_from_candidate(c, metric)
    if val is None and minv <= 0.0:
        # 如果这个 signal 源的阈值是 <=0，那即使拿不到值也给它 0.0
        val, used = 0.0, metric

    passed = (val is not None) and (val >= minv)
    return {
        "metric": metric,
        "min": minv,
        "value": val,
        "used_metric": used,
        "passed": bool(passed),
    }


def _cooldown_filter_candidates(conn, t_ref: str, cfg: dict, candidates: list[dict]) -> dict:
    """
    冷却期过滤（修正版，统一看真实仓位表 positions 和影子仓位表 positions_virtual）：

    目标：
    - 一个 symbol 如果还在持仓（status=OPEN/ACTIVE），就别重复加仓（除非 cfg.cooldown.block_if_open=False）
    - 一个 symbol 刚刚才平仓，但距离现在还没过冷却窗口（min_hold_minutes），就别立刻重复开
      -> 防止来回打脸、追涨杀跌

    配置（cfg["cooldown"]）预期字段：
        cooldown.block_if_open: bool，True=有持仓就不准再开（默认 True）
        cooldown.min_hold_minutes: float，平仓后至少休息多少分钟再允许重进（默认 120 分钟）

    输入：
        conn         sqlite3.Connection
        t_ref        当前桶时间 "YYYY-MM-DD HH:MM:SS"
        cfg          全局配置 dict
        candidates   候选列表，每个元素形如：
                        {
                            "symbol": "BTC/USDT",
                            "timeframe": "1h",
                            "direction": "long",
                            "source": "chip_llm",
                            ...
                        }

    返回：
        {
            "kept": [cand, cand, ...],      # 允许继续往后走风控/下单的候选
            "blocked": {
                "BTC/USDT": {
                    "reason": "already_open"
                },
                "SOL/USDT": {
                    "reason": "cooldown_not_elapsed",
                    "last_closed_at": "2025-10-26 14:30:00",
                    "age_min": 37.5,
                    "need_min": 120
                }
            }
        }

    注意：
    - 如果两个表都不存在（既无 positions 也无 positions_virtual），则不拦任何币，直接放行。
    - 只有 block_if_open=True 时才会因为“还在持仓”而直接拦。
    """
    import datetime

    # ---- 读取冷却相关配置，带默认值，防止 cfg 里没写 ----
    cd_cfg = (cfg or {}).get("cooldown", {}) or {}
    block_if_open = bool(cd_cfg.get("block_if_open", True))
    cooldown_minutes = cd_cfg.get("min_hold_minutes", 120)
    if cooldown_minutes is None:
        cooldown_minutes = 120
    try:
        cooldown_minutes = float(cooldown_minutes)
    except Exception:
        cooldown_minutes = 120.0

    kept = []
    blocked = {}

    # 没候选就直接空返回
    if not candidates:
        return {"kept": kept, "blocked": blocked}

    # 抽取本轮涉及到的 symbol 列表
    syms = []
    for c in candidates:
        s = (c.get("symbol") or "").strip()
        if s:
            syms.append(s)
    syms = sorted(set(syms))
    if not syms:
        return {"kept": list(candidates), "blocked": {}}

    cur = conn.cursor()

    # 小工具：表是否存在
    def _table_exists(table_name: str) -> bool:
        row = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return bool(row)

    has_positions_tbl = _table_exists("positions")
    has_pv_tbl = _table_exists("positions_virtual")

    # 如果两个表都没有，系统根本没法知道有没有开仓/冷却，直接全放行
    if not has_positions_tbl and not has_pv_tbl:
        return {"kept": list(candidates), "blocked": {}}

    # 统一把 t_ref 解析成 datetime，后面算“上次平仓到现在过了几分钟”
    def _parse_ts(ts_str: str):
        try:
            return datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    tref_dt = _parse_ts(t_ref)

    # ------------------------------------------------------------------
    # 1) 找出所有仍在持仓状态的 symbol
    #    status 视为还在场上: 'OPEN', 'ACTIVE'
    # ------------------------------------------------------------------
    open_status_syms = set()

    if block_if_open:
        q_marks = ",".join(["?"] * len(syms))

        if has_positions_tbl:
            rows_pos = (
                cur.execute(
                    f"""
                SELECT DISTINCT symbol
                FROM positions
                WHERE UPPER(status) IN ('OPEN','ACTIVE')
                  AND symbol IN ({q_marks})
                """,
                    syms,
                ).fetchall()
                or []
            )
            for r in rows_pos:
                sym = r[0]
                if sym:
                    open_status_syms.add(sym)

        if has_pv_tbl:
            rows_pv = (
                cur.execute(
                    f"""
                SELECT DISTINCT symbol
                FROM positions_virtual
                WHERE UPPER(status) IN ('OPEN','ACTIVE')
                  AND symbol IN ({q_marks})
                """,
                    syms,
                ).fetchall()
                or []
            )
            for r in rows_pv:
                sym = r[0]
                if sym:
                    open_status_syms.add(sym)

    # ------------------------------------------------------------------
    # 2) 找出每个 symbol 最近一次平仓时间（看 positions 和 positions_virtual 两边）
    # ------------------------------------------------------------------
    last_close_map: dict[str, str] = {}

    if has_positions_tbl:
        q_marks = ",".join(["?"] * len(syms))
        rows_last_close_pos = (
            cur.execute(
                f"""
            SELECT symbol, MAX(closed_at)
            FROM positions
            WHERE UPPER(status) IN ('CLOSED','EXITED','DONE')
              AND closed_at IS NOT NULL
              AND symbol IN ({q_marks})
            GROUP BY symbol
            """,
                syms,
            ).fetchall()
            or []
        )
        for sym, ts in rows_last_close_pos:
            if sym and ts:
                last_close_map[sym] = ts

    if has_pv_tbl:
        q_marks = ",".join(["?"] * len(syms))
        rows_last_close_pv = (
            cur.execute(
                f"""
            SELECT symbol, MAX(closed_at)
            FROM positions_virtual
            WHERE UPPER(status) IN ('CLOSED','EXITED','DONE')
              AND closed_at IS NOT NULL
              AND symbol IN ({q_marks})
            GROUP BY symbol
            """,
                syms,
            ).fetchall()
            or []
        )
        for sym, ts in rows_last_close_pv:
            if sym and ts:
                # 如果 positions_virtual 也有更晚的平仓时间，就覆盖
                prev = last_close_map.get(sym)
                if (not prev) or (ts > prev):
                    last_close_map[sym] = ts

    # ------------------------------------------------------------------
    # 3) 对每个候选票做判断
    # ------------------------------------------------------------------
    for c in candidates:
        sym = (c.get("symbol") or "").strip()
        if not sym:
            continue

        # (a) 还在持仓，就直接拦
        if block_if_open and sym in open_status_syms:
            blocked[sym] = {"reason": "already_open"}
            continue

        # (b) 如果刚刚才平过仓，检查冷却分钟
        lc_ts = last_close_map.get(sym)
        if lc_ts and tref_dt:
            lc_dt = _parse_ts(lc_ts)
            if lc_dt:
                age_min = (tref_dt - lc_dt).total_seconds() / 60.0
                if age_min < cooldown_minutes:
                    blocked[sym] = {
                        "reason": "cooldown_not_elapsed",
                        "last_closed_at": lc_ts,
                        "age_min": round(age_min, 2),
                        "need_min": cooldown_minutes,
                    }
                    continue

        # (c) 通过
        kept.append(c)

    return {
        "kept": kept,
        "blocked": blocked,
    }


def _reject_multi_tf_conflict(candidates):
    """
    多周期方向冲突硬杀门：

    目的：
    - 如果同一个 symbol 在本桶出现了同时 long 和 short，
      -> 这个 symbol 整体视为冲突，不允许交易。
    - 我们不做“降权”这种软处理，是直接整票拉黑。

    输入：
        candidates: list[dict]  形如:
            {
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "direction": "long" / "short" / None / "",
                "source": "chip_llm",
                ...
            }

    返回：
        {
            "kept": [cand, cand, ...],   # 允许后续继续走风控/下单的
            "conflicted": {
                "BTC/USDT": ["long", "short"],
                "DOGE/USDT": ["long", "short"],
                ...
            }
        }

    行为：
    - direction 只认 "long" / "short"，其他值忽略。
    - 如果某 symbol 只出现 long，OK。
      只出现 short，OK。
      同时出现 long & short -> 这个 symbol 的所有候选都丢掉。
    """
    # 1) 收集每个 symbol 出现过的方向集合
    dir_map = {}  # sym -> set(["long","short"])
    for c in candidates or []:
        sym = c.get("symbol")
        if not sym:
            continue
        d = (c.get("direction") or "").lower()
        if d not in ("long", "short"):
            continue
        if sym not in dir_map:
            dir_map[sym] = set()
        dir_map[sym].add(d)

    # 2) 找出冲突的 symbol
    conflict_syms = {sym for sym, dirs in dir_map.items() if len(dirs) > 1}

    # 3) 过滤
    kept = []
    conflicted_detail = {}  # sym -> sorted list of dirs
    for c in candidates or []:
        sym = c.get("symbol")
        if sym in conflict_syms:
            # 该 symbol 被整票否决
            conflicted_detail[sym] = sorted(list(dir_map.get(sym, [])))
            # 不加入 kept
            continue
        kept.append(c)

    return {
        "kept": kept,
        "conflicted": conflicted_detail,
    }
