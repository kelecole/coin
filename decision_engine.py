from __future__ import annotations

"""
core/decision_engine.py

生成“下单计划书”，但不真正落单。

风控闸门顺序：
1. 广度门（bucket-level，市场有没有主线，散沙就整桶不开）
2. panic/bias 过滤（极端行情下砍方向，暂时保守）
3. R 分配（max_R_per_bucket / base_R_per_trade 等）
4. 相关性门（correlation_gate）：检查组合集中度，可能 scale 半仓/四分之一仓，甚至整桶 block
5. dd_kill 在 run_open_cycle 里最后挡住实际落单

最终返回：
{
    "t_ref": ...,
    "approved": [...],     # 计划要下的单（含 qty / notional_usd / position_scale_corrgate）
    "rejected": {...},     # symbol -> reason
    "risk_summary": {...},
    "breadth_info": {...},
    "market_ctx": {...},
}
"""
import json
import sqlite3
from typing import List, Dict, Any
from core.btc_alignment import _filter_by_btc_alignment as btc_alignment_hard_filter
from core.timebox import now_local_str

def _plan_evictions(
    conn,
    cfg: dict,
    t_ref: str,
    approved: list,
) -> tuple[list, dict]:
    """
    组合上限 + 更强替换更弱 的规划器（不执行退出，只生成计划）。
    返回: (approved_after, eviction_plan)
      - approved_after: 可能被截断/重排后的待开仓列表
      - eviction_plan: {
            "cap": { "open_now": int, "cap": int },
            "pairs": [  # 新仓与被替换老仓的一一对应计划
                {
                  "candidate": {symbol, timeframe, direction, source, score?},
                  "victim":    {id, symbol, timeframe, direction, R_now, mfe_R, tp_stage, be_armed},
                  "reason":    "edge_margin_ok|portfolio_full|... ",
                },
                ...
            ],
            "skipped_candidates": [ ... 被丢弃的新候选（没有足够优势） ... ],
            "victim_pool_snapshot": [ ... 排序后的可淘汰池快照，便于审计 ... ]
        }

    规则（与 config.yml 中 core.eviction 对齐）：
      - 只有在 当前已开仓 >= portfolio_open_cap 时才触发淘汰逻辑
      - 保护：
          * 入场未满 min_hold_minutes 不淘汰
          * protect_be=true 时，已 be_armed 不淘汰
          * protect_tp1=true 时，tp_stage>=1 不淘汰
      - 选“最弱”：
          * 先按是否保护（未保护优先）
          * 再按 R_now 升序（亏损更大优先）
          * 再按 mfe_R 升序（历史最好表现差的优先）
          * 再按 opened_at 较新者优先（同劣时优先换掉更“新且差”的，避免老盈利单被替换）
      - 新信号必须比“最弱”至少高出 min_edge_margin 才允许替换
    """
    import math
    from datetime import datetime

    # ---------- 读取配置 ----------
    core = (cfg.get("core") or {})
    limits = (core.get("limits") or {})
    eviction_cfg = (core.get("eviction") or {})
    if not eviction_cfg or not eviction_cfg.get("enable", False):
        return approved, {"cap": None, "pairs": [], "skipped_candidates": [], "victim_pool_snapshot": []}

    cap_total = int(limits.get("portfolio_open_cap") or limits.get("bucket_open_cap") or 7)
    min_hold_minutes = int(eviction_cfg.get("min_hold_minutes", 60))
    protect_be = bool(eviction_cfg.get("protect_be", True))
    protect_tp1 = bool(eviction_cfg.get("protect_tp1", True))
    hysteresis_R = float(eviction_cfg.get("hysteresis_R", 0.15))  # 对负R的回滞，不至于来回抖
    min_edge_margin = float(eviction_cfg.get("min_edge_margin", 0.20))

    exit_cfg = (core.get("exit") or (cfg.get("exit") or {}))
    sl_cfg = (exit_cfg.get("sl") or {})
    kmap = (sl_cfg.get("atr_mult") or {"trending": 1.8, "ranging": 2.0})

    # ---------- 工具函数 ----------
    def _now_local_str(tz_str="Asia/Shanghai"):
        try:
            import pytz
            tz = pytz.timezone(tz_str)
            return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _latest_px_atr(conn_local, symbol, timeframe):
        # 取最近一根 close 与 ATR14（取不到则返回 None）
        row = conn_local.execute(
            "SELECT close, ATR14 FROM ohlcv WHERE symbol=? AND timeframe=? ORDER BY t DESC LIMIT 1;",
            (symbol, timeframe),
        ).fetchone()
        if not row: return None, None
        try:
            return float(row[0]), (float(row[1]) if row[1] is not None else None)
        except Exception:
            return None, None

    def _eff_stop(side, sp, tsl):
        vals = [v for v in (sp, tsl) if v not in (None, "")]
        if not vals: return None
        vals = list(map(float, vals))
        return max(vals) if side == "long" else min(vals)

    def _risk_unit_unified(entry_px, stop_px, entry_atr, atr_now, regime):
        k_sl = float(kmap.get("trending" if (regime or "ranging") == "trending" else "ranging", 1.8))
        cands = []
        if stop_px is not None: cands.append(abs(float(entry_px) - float(stop_px)))
        if entry_atr is not None: cands.append(float(entry_atr) * k_sl)
        if atr_now is not None: cands.append(float(atr_now) * k_sl)
        return max(cands) if cands else 0.0

    def _pos_R_now(conn_local, p: dict):
        # 计算当前 R_now（保守分母）
        px_now, atr_now = _latest_px_atr(conn_local, p["symbol"], p["timeframe"])
        if px_now is None:
            return 0.0  # 兜底
        den = _risk_unit_unified(
            entry_px=float(p.get("entry_price") or 0.0),
            stop_px=_eff_stop(p["direction"], p.get("stop_price"), p.get("tsl_price")),
            entry_atr=(p.get("entry_atr")),
            atr_now=atr_now,
            regime=p.get("regime") or "ranging",
        )
        if den <= 0: return 0.0
        num = (px_now - float(p["entry_price"])) if p["direction"] == "long" else (float(p["entry_price"]) - px_now)
        return num / den

    def _candidate_edge(c: dict) -> float:
        # 候选信号的“优势分”（简化口径：score/prob/strength 取最大；王牌信号 +0.2）
        base = 0.0
        for k in ("score", "prob", "strength"):
            v = c.get(k)
            try:
                if v is not None: base = max(base, float(v))
            except Exception:
                pass
        src = (c.get("source") or "").lower()
        if src in ("sig_vpoc_flip", "sig_avwap_flip", "sig_sqz_br"):
            base += 0.20
        return float(max(0.0, min(base, 1.5)))  # 适当截断

    # ---------- 读取当前组合 ----------
    rows = conn.execute("""
        SELECT id, symbol, timeframe, direction, entry_price, stop_price, tsl_price,
               entry_atr, be_armed, tp_stage, mfe_R, regime, created_at, status
        FROM positions_virtual
        WHERE status IN ('OPEN','ACTIVE')
        ORDER BY id ASC;
    """).fetchall()
    open_positions = []
    for r in rows or []:
        try:
            d = {
                "id": int(r[0]),
                "symbol": str(r[1]),
                "timeframe": str(r[2]),
                "direction": str(r[3]).lower(),
                "entry_price": float(r[4] or 0.0),
                "stop_price": (None if r[5] is None else float(r[5])),
                "tsl_price": (None if r[6] is None else float(r[6])),
                "entry_atr": (None if r[7] is None else float(r[7])),
                "be_armed": int(r[8] or 0),
                "tp_stage": int(r[9] or 0),
                "mfe_R": float(r[10] or 0.0),
                "regime": (r[11] or "ranging"),
                "created_at": (r[12] or _now_local_str(core.get("tz", "Asia/Shanghai"))),
                "status": str(r[13] or "OPEN"),
            }
            # 计算 R_now
            d["R_now"] = _pos_R_now(conn, d)
            # 持仓时长
            try:
                from datetime import datetime as _dt
                dur = (_dt.strptime(_now_local_str(core.get("tz", "Asia/Shanghai")), "%Y-%m-%d %H:%M:%S")
                       - _dt.strptime(str(d["created_at"]), "%Y-%m-%d %H:%M:%S")).total_seconds() / 60.0
            except Exception:
                dur = 1e9  # 解析失败就视为很久
            d["held_minutes"] = float(dur)
            open_positions.append(d)
        except Exception:
            continue

    open_now = len(open_positions)
    if open_now < cap_total or not approved:
        # 没满仓或没有候选，直接返回
        return approved, {"cap": {"open_now": open_now, "cap": cap_total}, "pairs": [], "skipped_candidates": [], "victim_pool_snapshot": []}

    # ---------- 构建“可淘汰池” ----------
    victim_pool = []
    for p in open_positions:
        prot = 0
        if protect_be and int(p["be_armed"] or 0) == 1:
            prot = 1
        if protect_tp1 and int(p["tp_stage"] or 0) >= 1:
            prot = 1
        if p["held_minutes"] < float(min_hold_minutes):
            prot = 1
        p["_protected"] = prot

        # 给出“弱度”打分：保护的视为极优（排到后面），未保护的按 R_now 升序、mfe_R 升序
        # 同等时优先淘汰“更新且表现差”的，避免老单被频繁换掉
    victim_pool_sorted = sorted(
        victim_pool + open_positions,  # victim_pool 为空时就是 open_positions
        key=lambda x: (
            int(x.get("_protected", 0)),                 # 0(可淘汰) 在前
            float(x.get("R_now", 0.0)),                  # R_now 越小越差
            float(x.get("mfe_R", 0.0)),                  # 历史最好越小越差
            -float(x.get("held_minutes", 0.0)),          # 更“新”的（held短）在前
        )
    )

    # 只保留“未保护”的作为可淘汰池
    victim_pool_sorted = [p for p in victim_pool_sorted if int(p.get("_protected", 0)) == 0]

    victim_snapshot = [
        {
            "id": p["id"], "symbol": p["symbol"], "tf": p["timeframe"],
            "R_now": p["R_now"], "mfe_R": p["mfe_R"],
            "tp_stage": p["tp_stage"], "be_armed": p["be_armed"],
            "held_m": round(p["held_minutes"], 1)
        } for p in victim_pool_sorted
    ]

    if not victim_pool_sorted:
        # 没有可淘汰对象：组合已满，直接截断新候选
        keep_n = max(0, cap_total - open_now)
        return approved[:keep_n], {
            "cap": {"open_now": open_now, "cap": cap_total},
            "pairs": [],
            "skipped_candidates": approved[keep_n:],
            "victim_pool_snapshot": victim_snapshot,
        }

    # ---------- 候选排序：强的在前 ----------
    cand_sorted = sorted(list(approved), key=lambda c: (-_candidate_edge(c), c.get("symbol", "")))

    pairs = []
    skipped = []
    victims_iter = iter(victim_pool_sorted)

    # 需要空位数量
    need_slots = max(0, open_now + len(cand_sorted) - cap_total)

    for c in cand_sorted:
        if need_slots <= 0:
            # 不需要再挤出空位，保留即可
            pairs.extend([])  # no-op
            continue

        try:
            v = next(victims_iter)
        except StopIteration:
            # 没受害者了，后面的候选丢弃
            skipped.append(c); continue

        cand_edge = _candidate_edge(c)
        # “最弱者”的边际：以 -R_now 为负面强度，再加上回滞
        victim_edge = max(0.0, -float(v.get("R_now", 0.0)) - hysteresis_R)

        if (cand_edge - victim_edge) >= min_edge_margin:
            pairs.append({
                "candidate": {
                    "symbol": c.get("symbol"), "timeframe": c.get("timeframe"),
                    "direction": c.get("direction"), "source": c.get("source"),
                    "score": c.get("score"), "prob": c.get("prob"), "strength": c.get("strength"),
                },
                "victim": {
                    "id": v["id"], "symbol": v["symbol"], "timeframe": v["timeframe"],
                    "direction": v["direction"], "R_now": v["R_now"], "mfe_R": v["mfe_R"],
                    "tp_stage": v["tp_stage"], "be_armed": v["be_armed"],
                },
                "reason": "portfolio_full|edge_margin_ok"
            })
            need_slots -= 1
        else:
            # 优势不够，丢弃该候选
            skipped.append(c)

    # 重新计算最终可开仓列表：去掉被丢弃的候选
    approved_after = [c for c in cand_sorted if c not in skipped]

    return approved_after, {
        "cap": {"open_now": open_now, "cap": cap_total},
        "pairs": pairs,
        "skipped_candidates": skipped,
        "victim_pool_snapshot": victim_snapshot,
    }


def _write_decision_snapshot_batch(
    conn: sqlite3.Connection,
    t_ref: str,
    approved: List[Dict[str, Any]],
    rejected: Dict[str, str],
    cfg: dict,
    risk_summary: Dict[str, Any],
):
    """
    把这桶的最终裁决写进 decision_snapshot 表（保持原语义）。
    增量：按规范把 evidence_* 同步写入 decision_evidence（final/protection/liquidity/correlation/btc_align/可选 breadth/risk_cost）。
    对于 rejected（当前为 Dict[symbol->reason] 且缺 timeframe），维持现状：不落表。
    """
    import json
    from typing import Optional
    from core.timebox import now_local_str

    now_ts = now_local_str(cfg.get("tz", "Asia/Shanghai"))
    t_now = now_ts 
    cur = conn.cursor()

    strat_ver = str(cfg.get("strategy_version", "dev"))
    dd_kill_active = bool(risk_summary.get("dd_kill_active", False))
    panic_selloff = bool(risk_summary.get("panic_selloff", False))

    # ---------- helpers ----------
    def _nz(v):
        try:
            return float(v)
        except Exception:
            return 0.0

    def _clear_evidence(snapshot_id: int):
        try:
            cur.execute("DELETE FROM decision_evidence WHERE snapshot_id=?;", (snapshot_id,))
        except Exception:
            # 兼容不同表结构
            pass

    def _insert_evidence(
        snapshot_id: int,
        sym: str,
        tf: str,
        gate: str,
        passed_val: Optional[int],
        data_obj: Optional[dict],
        thr_obj: Optional[dict],
        src_obj: Optional[dict],
        notes: Optional[str],
    ):
        try:
            cur.execute(
                """
                INSERT INTO decision_evidence(
                    snapshot_id, t_ref, symbol, timeframe, gate, pass,
                    data_json, threshold_json, source_json, notes, created_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    snapshot_id, t_ref, sym, tf, gate,
                    (int(passed_val) if passed_val is not None else None),
                    json.dumps(data_obj, ensure_ascii=False) if data_obj is not None else None,
                    json.dumps(thr_obj, ensure_ascii=False) if thr_obj is not None else None,
                    json.dumps(src_obj, ensure_ascii=False) if src_obj is not None else None,
                    notes,
                    now_ts,
                ),
            )
        except Exception as e:
            print(f"[decision_engine] evidence insert failed gate={gate} {sym} {tf}: {e}")

    def _table_exists(name: str) -> bool:
        try:
            r = cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
                (name,),
            ).fetchone()
            return bool(r)
        except Exception:
            return False

    def _fetch_equity_row(tref: str):
        try:
            row = cur.execute(
                """
                SELECT drawdown_pct, dd_kill, dd_kill_flag
                FROM equity_snapshot
                WHERE t_ref <= ?
                ORDER BY t_ref DESC
                LIMIT 1
                """,
                (tref,),
            ).fetchone()
            if row:
                return {
                    "drawdown_pct": row[0],
                    "dd_kill": bool(row[1]) if row[1] is not None else None,
                }
        except Exception:
            pass
        return None

    def _fetch_btc_align(sym: str, tf: str, tref: str):
        """
        读取 btc_alignment_snapshot（<= t_ref 最近一条）。
        兼容两版列名：
        - 新：align_corr, peak_corr_val
        - 旧：align_score, peak_corr_abs
        统一输出：
        align_score  -> 对齐到 float(align_corr)
        peak_corr_abs-> 对齐到 abs(float(peak_corr_val))
        """
        # 内部依赖：_table_exists / cur 都与原文件一致
        if not _table_exists("btc_alignment_snapshot"):
            return None
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(btc_alignment_snapshot);").fetchall()]

            # 优先用“新列名”
            if ("align_corr" in cols) and ("peak_corr_val" in cols):
                row = cur.execute(
                    """
                    SELECT align_corr, peak_corr_val, t_ref
                    FROM btc_alignment_snapshot
                    WHERE symbol=? AND timeframe=? AND t_ref<=?
                    ORDER BY t_ref DESC
                    LIMIT 1
                    """,
                    (sym, tf, tref),
                ).fetchone()
                if not row:
                    return {"missing": True}
                align_corr_val = (float(row[0]) if row[0] is not None else None)
                peak_corr_val  = (float(row[1]) if row[1] is not None else None)
                return {
                    "align_score": align_corr_val,
                    "peak_corr_abs": (abs(peak_corr_val) if peak_corr_val is not None else None),
                    "t_ref_checked": row[2],
                    "missing": False,
                }

            # 兼容“旧列名”
            row = cur.execute(
                """
                SELECT align_score, peak_corr_abs, t_ref
                FROM btc_alignment_snapshot
                WHERE symbol=? AND timeframe=? AND t_ref<=?
                ORDER BY t_ref DESC
                LIMIT 1
                """,
                (sym, tf, tref),
            ).fetchone()
            if not row:
                return {"missing": True}
            return {
                "align_score": (float(row[0]) if row[0] is not None else None),
                "peak_corr_abs": (float(row[1]) if row[1] is not None else None),
                "t_ref_checked": row[2],
                "missing": False,
            }
        except Exception as e:
            print(f"[decision_engine] fetch btc_align failed {sym} {tf}: {e}")
            return None


    def _fetch_breadth_row(tref: str):
        # 可选：如果你有 breadth_snapshot 就写一条证据，没表则跳过
        if not _table_exists("breadth_snapshot"):
            return None
        try:
            cols = [d[1] for d in cur.execute("PRAGMA table_info(breadth_snapshot);").fetchall()]
            row = cur.execute(
                "SELECT * FROM breadth_snapshot WHERE t_ref=? LIMIT 1;",
                (tref,),
            ).fetchone()
            if not row:
                return None
            data = {}
            for k in ("advancers", "decliners", "ratio", "universe", "adv_vol", "dec_vol"):
                if k in cols:
                    idx = cols.index(k)
                    data[k] = row[idx]
            return data or None
        except Exception:
            return None

    # ---------- 1) 通过票：写 decision_snapshot + 证据 ----------
    for a in approved:
        sym = a.get("symbol")
        tf = a.get("timeframe")
        if not sym or not tf:
            continue

        prob = a.get("prob")
        score = a.get("score")
        strength = a.get("strength")
        synth_score = _nz(prob) * 2.0 + _nz(score) * 1.0 + _nz(strength) * 0.5
        position_scale = a.get("position_scale", 1.0)

        reasons = ["approved", f"strategy_version={strat_ver}"]
        if dd_kill_active:
            reasons.append("dd_kill_active=True")
        if panic_selloff:
            reasons.append("panic_selloff=True")

        # 写 decision_snapshot（幂等 upsert）
        try:
            cur.execute(
                """
                INSERT INTO decision_snapshot(
                    t_ref, symbol, timeframe, source,
                    final, score, reasons_json,
                    position_scale, synth_score, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(t_ref, symbol, timeframe)
                DO UPDATE SET
                    final=excluded.final,
                    score=excluded.score,
                    reasons_json=excluded.reasons_json,
                    position_scale=excluded.position_scale,
                    synth_score=excluded.synth_score,
                    created_at=excluded.created_at
                """,
                (
                    t_ref,
                    sym,
                    tf,
                    a.get("source"),
                    "open",
                    _nz(score),
                    json.dumps(reasons, ensure_ascii=False),
                    float(position_scale),
                    synth_score,
                    now_ts,
                ),
            )
        except Exception as e:
            print(f"[decision_engine] snapshot(upsert) failed {sym} {tf}: {e}")
            continue

        # 取回 snapshot_id
        sid_row = cur.execute(
            "SELECT id FROM decision_snapshot WHERE t_ref=? AND symbol=? AND timeframe=? LIMIT 1;",
            (t_ref, sym, tf),
        ).fetchone()
        if not sid_row or sid_row[0] is None:
            continue
        snapshot_id = int(sid_row[0])

        # 清理旧证据（避免重复）
        _clear_evidence(snapshot_id)

        # final=open
        _insert_evidence(
            snapshot_id, sym, tf, "final", 1,
            {"decision": "open"},
            None,
            {"table": "decision_snapshot"},
            None,
        )

        # protection（panic/dd_kill + drawdown）
        eq = _fetch_equity_row(t_ref)
        prot_data = {"panic_selloff": panic_selloff, "dd_kill": dd_kill_active}
        if isinstance(eq, dict):
            if eq.get("drawdown_pct") is not None:
                prot_data["drawdown_pct"] = eq["drawdown_pct"]
            if eq.get("dd_kill") is not None:
                prot_data["dd_kill"] = eq["dd_kill"]
        _insert_evidence(
            snapshot_id, sym, tf, "protection", 1,
            prot_data,
            None,
            {"table": "equity_snapshot"},
            None,
        )

        # 可选 breadth（如有表/字段）
        br = _fetch_breadth_row(t_ref)
        if isinstance(br, dict) and br:
            _insert_evidence(
                snapshot_id, sym, tf, "breadth", None,
                br,
                None,
                {"table": "breadth_snapshot"},
                None,
            )

        # liquidity（来自 evidence_liquidity）
        ev_l = a.get("evidence_liquidity")
        if isinstance(ev_l, dict):
            try:
                data_obj = {
                    "turnover_usd_curr": ev_l.get("turnover_usd_curr"),
                    "turnover_usd_avgN": ev_l.get("turnover_usd_avgN"),
                    "spike_ratio": ev_l.get("spike_ratio"),
                    "window_N": ev_l.get("lookback_bars"),
                }
                liq_thr = {}
                try:
                    if ev_l.get("min_spike_ratio") is not None:
                        liq_thr["min_spike_ratio"] = ev_l.get("min_spike_ratio")
                except Exception:
                    pass
                try:
                    liq_cfg = (cfg.get("thresholds") or {}).get("liquidity") or {}
                    if "min_avg_usd" in liq_cfg:
                        liq_thr["min_avg_usd"] = float(liq_cfg.get("min_avg_usd"))
                except Exception:
                    pass
                src_obj = {
                    "table": "ohlcv",
                    "cols": ["quote_volume|value_usd", "close"],
                    "fallback": "base*close",
                    "lookup": "<= t_ref last N",
                }
                passed_flag = ev_l.get("pass")
                pval = 1 if passed_flag is True else (0 if passed_flag is False else None)
                _insert_evidence(
                    snapshot_id, sym, tf, "liquidity", pval,
                    data_obj, (liq_thr or None), src_obj, ev_l.get("reason"),
                )
            except Exception as e:
                print(f"[decision_engine] write liquidity evidence failed: {sym} {tf} {e}")

        # correlation（来自 evidence_correlation）
        ev_c = a.get("evidence_correlation")
        if isinstance(ev_c, dict):
            try:
                # 统一到你的规范键名
                data_obj = {
                    "corr_est": ev_c.get("rho_star"),
                    "action": ev_c.get("decision"),
                    "scale_ratio": ev_c.get("position_scale"),
                }
                thr = ev_c.get("thresholds") or {}
                thr_obj = {
                    "scale": thr.get("abs_corr_scale_threshold"),
                    "block": thr.get("abs_corr_block_threshold"),
                }
                src_obj = {"table": "correlation_snapshot"}
                notes = None
                d = ev_c.get("decision")
                if isinstance(d, str):
                    notes = d.lower().strip()
                _insert_evidence(
                    snapshot_id, sym, tf, "correlation", 1,
                    data_obj, thr_obj, src_obj, notes,
                )
            except Exception as e:
                print(f"[decision_engine] write correlation evidence failed: {sym} {tf} {e}")

        # btc_align（查表，按规范写）
        try:
            ba = _fetch_btc_align(sym, tf, t_ref)
            if isinstance(ba, dict):
                thr_cfg = (cfg.get("thresholds") or {}).get("btc_align") or {}
                thr_obj = {
                    "min_align_score": thr_cfg.get("min_align_score"),
                    "min_peak_corr_abs": thr_cfg.get("min_peak_corr_abs"),
                    "reject_if_missing": bool(thr_cfg.get("reject_if_missing", False)),
                }
                data_obj = {
                    "align_score": ba.get("align_score"),
                    "peak_corr_abs": ba.get("peak_corr_abs"),
                    "t_ref_checked": ba.get("t_ref_checked"),
                    "missing": bool(ba.get("missing", False)),
                }
                src_obj = {
                    "table": "btc_alignment_snapshot",
                    "cols": ["align_score", "peak_corr_abs", "t_ref"],
                    "lookup": "<= t_ref nearest",
                }
                # 通过票落 pass=1；若缺失也记录 missing=true 但不报错
                _insert_evidence(
                    snapshot_id, sym, tf, "btc_align", 1,
                    data_obj, thr_obj, src_obj, None,
                )
        except Exception as e:
            print(f"[decision_engine] write btc_align evidence failed: {sym} {tf} {e}")

        # risk_cost（如上游已挂 evidence_risk_cost 就写）
        ev_rc = a.get("evidence_risk_cost")
        if isinstance(ev_rc, dict):
            try:
                data_obj = {
                    "rr_actual": ev_rc.get("rr_actual"),
                    "basis": ev_rc.get("basis", "ATR"),
                    "max_stop_frac": ev_rc.get("max_stop_frac"),
                }
                thr_cfg = (cfg.get("thresholds") or {}).get("risk_cost") or {}
                thr_obj = {
                    "rr_min": thr_cfg.get("rr_min"),
                }
                src_obj = {"module": "decision_engine._filter_by_risk_cost"}
                _insert_evidence(
                    snapshot_id, sym, tf, "risk_cost", ev_rc.get("pass"),
                    data_obj, thr_obj, src_obj, ev_rc.get("reason"),
                )
            except Exception as e:
                print(f"[decision_engine] write risk_cost evidence failed: {sym} {tf} {e}")

    # ---------- 2) 拒单：保持现状（当前缺 timeframe），不写库 ----------
    for _sym, _reason in (rejected or {}).items():
        pass

    try:
        conn.commit()
    except Exception as e:
        print(f"[decision_engine] commit snapshot/evidence failed: {e}")





from typing import Dict, Any, List, Tuple


# ---------------------------------------------------------
# 工具：从 breadth_info 推导 bias
# ---------------------------------------------------------
def _infer_bias_from_breadth(breadth_info: dict) -> str:
    adv = float(breadth_info.get("advancers", 0.0))
    dec = float(breadth_info.get("decliners", 0.0))

    if adv > dec * 1.2:
        return "long"
    if dec > adv * 1.2:
        return "short"
    return "neutral"





# ---------------------------------------------------------
# 工具：按强度排序
# ---------------------------------------------------------
def _rank_candidates(cands: List[dict]) -> List[dict]:
    def _score(c: dict) -> float:
        for k in ("prob", "score", "strength"):
            v = c.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return 0.0

    return sorted(cands, key=_score, reverse=True)


# ---------------------------------------------------------
# 工具：分配 R / sizing
# ---------------------------------------------------------
# 替换 decision_engine.py 中的 _allocate_risk_R
def _allocate_risk_R(
    ranked: List[dict],
    cfg: dict,
    bias: str,
    btc_alignment_map: dict,
) -> Tuple[List[dict], List[dict], float, float, float]:
    """
    给候选分配风险额度R、基础资金规模、position_scale，并执行桶级R硬上限。
    同时：
      - 按 bias 做轻微方向偏好
      - 按 BTC 对齐做轻微放大/缩小
      - ★ 新增：按 Beta 调整仓位风险 ★
      - 按 allow_short 决定要不要直接砍空单
      - 不再把拿不到的价格写成 0.0，留给执行层去补
    """

    # 1) 基础配置
    risk_cfg = cfg.get("risk", {}) or {}
    risk_cost = cfg.get("risk_cost", {}) or {}

    max_bucket_R = float(risk_cfg.get("max_R_per_bucket", 3.0))
    base_R = float(risk_cfg.get("base_R_per_trade", 1.0))
    weak_R = float(risk_cfg.get("weak_R_per_trade", 0.5))
    strong_cut = float(risk_cfg.get("strong_signal_cutoff", 0.7))

    starting_equity = float(risk_cfg.get("starting_equity_usd", 10000.0))
    max_size_usd = float(risk_cost.get("max_size_usd", 2000.0))

    # 2) bias 规范
    bias_norm = (bias or "neutral").lower().strip()

    # 3) allow_short 多来源读取：root / core / trading / csmvp
    allow_short = cfg.get("allow_short", None)
    if allow_short is None:
        allow_short = (cfg.get("core") or {}).get("allow_short", None)
    if allow_short is None:
        allow_short = (cfg.get("trading") or {}).get("allow_short", None)
    if allow_short is None:
        allow_short = (cfg.get("csmvp") or {}).get("allow_short", None)
    if allow_short is None:
        # 全都没配，就当成不允许开空
        allow_short = False

    approved: List[dict] = []
    rejected: List[dict] = []
    used_R = 0.0
    strategy_version = str(cfg.get("strategy_version", "v1.0.0"))

    def _nz_float(v, default=0.0):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    for c in ranked:
        sym = c.get("symbol", "?")
        tf = c.get("timeframe") or "1h"
        direc = (c.get("direction") or "").lower().strip()  # "long"/"short"

        # -------- A) 全局没开空就砍空 --------
        if direc == "short" and not allow_short:
            rejected.append({
                "symbol": sym,
                "timeframe": tf,
                "direction": direc,
                "reject_reason": "short_disabled",
            })
            continue

        # -------- B) 信号强度 -> this_R --------
        strength_val = 0.0
        for k in ("prob", "score", "strength"):
            if c.get(k) is not None:
                try:
                    strength_val = float(c[k])
                    break
                except (TypeError, ValueError):
                    pass

        this_R = base_R if strength_val >= strong_cut else weak_R

        # -------- C) 桶级R硬上限 --------
        if used_R + this_R > max_bucket_R:
            rejected.append({
                "symbol": sym,
                "timeframe": tf,
                "direction": direc,
                "reject_reason": "risk_cap_exceeded",
            })
            continue

        # -------- D) baseline notional --------
        raw_notional = starting_equity * 0.15 * this_R
        baseline_notional = min(max_size_usd, raw_notional)

        # -------- E) position_scale 初始值 --------
        pos_scale = 1.0

        # E1: 顺 bias 方向稍微放大
        if bias_norm in ("long", "short") and direc == bias_norm:
            if strength_val >= strong_cut:
                pos_scale *= 1.25

        # E2: BTC 对齐映射，注意这里可能是 float，也可能是 dict
        align_raw = btc_alignment_map.get(sym)
        if isinstance(align_raw, dict):
            align_score = _nz_float(align_raw.get("align_score"), None)
            # ↓↓↓ 新增：获取 beta 值 ↓↓↓
            beta_val = _nz_float(align_raw.get("beta"), None) 
        else:
            align_score = _nz_float(align_raw, None)
            # ↓↓↓ 新增：旧格式下 beta 为 None ↓↓↓
            beta_val = None

        if align_score is not None:
            # 限 [-2, 2]
            if align_score > 2.0:
                align_score = 2.0
            elif align_score < -2.0:
                align_score = 2.0
            pos_scale *= (1.0 + 0.1 * align_score)

        # ↓↓↓ 新增：Beta 风险调整 (E3) ↓↓↓
        # 我们对高 beta (高风险) 标的进行仓位缩减
        if beta_val is not None and beta_val > 0.5:
            # 示例：beta=2.0, 缩放 1/1.5 = 0.66倍
            # 示例：beta=1.5, 缩放 1/1.25 = 0.8倍
            # 示例：beta=0.8, 缩放 1/0.9 = 1.11倍
            # (避免 beta 接近 0 导致仓位爆炸)
            beta_adj_factor = 1.0 / ( (beta_val - 1.0) * 0.5 + 1.0 )
            if beta_adj_factor > 1.5: beta_adj_factor = 1.5 # 放大上限
            if beta_adj_factor < 0.5: beta_adj_factor = 0.5 # 缩小下限
            pos_scale *= beta_adj_factor
        # ↑↑↑ 结束新增 Beta 调整 ↑↑↑
            
        # E4: 再夹一下
        if pos_scale < 0.0:
            pos_scale = 0.0
        if pos_scale > 2.0:
            pos_scale = 2.0

        # -------- F) 应用到下单资金 --------
        final_notional_usd = baseline_notional * pos_scale

        # -------- G) 入场价：不要再写 0 --------
        entry_price = None
        for pk in ("entry_price", "price", "last_price", "ref_price"):
            if c.get(pk) is not None:
                try:
                    v = float(c[pk])
                    if v > 0:
                        entry_price = v
                        break
                except (TypeError, ValueError):
                    pass
        # 留 None，执行层自己去 market_data 里补

        qty = 0.0
        if entry_price is not None and entry_price > 0:
            qty = final_notional_usd / entry_price

        enriched = dict(c)
        enriched["notional_usd"] = final_notional_usd
        enriched["position_scale"] = pos_scale
        enriched["entry_price"] = entry_price
        enriched["qty"] = qty
        enriched["risk_R_alloc"] = this_R
        enriched["strategy_version"] = strategy_version

        approved.append(enriched)
        used_R += this_R

    return approved, rejected, used_R, max_bucket_R, base_R



def _filter_by_market_context(
    candidates_ready: list,
    market_ctx: dict,
    bias: str,
    btc_alignment_map: dict,  # <-- ★ 在这里添加第 4 个参数
):
    """
    市场环境二次过滤。

    [✅ 修复] 
    修复了 'str' object has no attribute 'get' 崩溃 Bug。
    'rejected' 的返回类型从 Dict[str, str] 修改为 List[Dict[str, Any]]，
    与其他风控模块保持一致。

    目标：
    1. panic_selloff == True 时，禁止做多 (long)。
    2. 非 panic 时，按照大盘主线 bias 做方向一致性过滤。

    返回:
      kept: list[dict]   -> 还能继续走后续R分配/相关性门的票
      rejected: list[dict] -> { "symbol": "...", "timeframe": "...", "direction": "...", "reject_reason": "..." }
    """

    kept = []
    rejected = []  # [✅ 修复] 从 {} 修改为 []

    panic_flag = bool(market_ctx.get("panic_selloff", False))

    for c in candidates_ready:
        sym = c.get("symbol", "?")
        direc = (c.get("direction") or "").lower().strip()  # 'long'/'short'
        tf = c.get("timeframe") or "1h"  # [✅ 修复] 获取 timeframe 供 rejected 列表使用

        # --- 1) Panic 场景：严禁接多头
        if panic_flag:
            if direc == "long":
                # [✅ 修复] 从 rejected[sym] = "..." 修改为 .append(dict)
                rejected.append({
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": direc,
                    "reject_reason": "panic_block_long"
                })
                continue
            else:
                # short 在panic可以保留（允许顺势防御/做空）
                kept.append(c)
                continue

        # --- 2) 非 panic 场景：按 bias 方向筛
        b = (bias or "neutral").lower().strip()

        if b == "long":
            if direc == "short":
                # [✅ 修复] 从 rejected[sym] = "..." 修改为 .append(dict)
                rejected.append({
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": direc,
                    "reject_reason": "against_bias"
                })
                continue
            else:
                kept.append(c)
                continue

        if b == "short":
            if direc == "long":
                # [✅ 修复] 从 rejected[sym] = "..." 修改为 .append(dict)
                rejected.append({
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": direc,
                    "reject_reason": "against_bias"
                })
                continue
            else:
                kept.append(c)
                continue

        # b == 'neutral' or unknown
        kept.append(c)

    return kept, rejected
def _apply_liquidity_gate(
    conn,
    cfg: dict,
    t_ref: str,
    candidates: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    流动性门（两段式）：
    1) API 层面预过滤（能不能交易 + 点差 + 可选24h量）——core/liquidity_filter.prefilter_...
    2) K线层面放量检查（和自己过去N根比）——core/liquidity_filter.filter_by_volume_spike(...)

    返回：
        kept:     能继续往下（risk_cost / correlation）的票
        rejected: 在任一段被挡掉的票，带 reject_reason
    """
    from core import liquidity_filter
    from core.timebox import now_local_str              # [新增]
    now_ts = now_local_str(cfg.get("tz", "Asia/Shanghai"))  # [新增]

    # 收集 symbol，保持你原来列表结构
    symbols = []
    for c in candidates:
        sym = c.get("symbol")
        if sym and sym not in symbols:
            symbols.append(sym)

    print(
        f"[decision_engine] liquidity gate @ {t_ref}: "
        f"incoming={len(candidates)}, symbols={len(symbols)}"
    )

    # ---------- 第一段：API 流动性 ----------
    try:
        passed_syms, liq_details = liquidity_filter.prefilter_candidates_with_liquidity_filter(
            symbols,
            cfg,
            return_details=True,
        )
    except Exception as e:
        # 不要因为外部API挂了把所有票全砍掉，直接放行
        print(f"[decision_engine] liquidity prefilter failed, skip. err={e}")
        return candidates, []

    kept_after_basic: list[dict] = []
    rejected_all: list[dict] = []
    cur = conn.cursor()

    for c in candidates:
        sym = c.get("symbol")
        tf = c.get("timeframe") or "1h"
        direction = (c.get("direction") or "").lower().strip()
        if not sym:
            continue

        det = liq_details.get(sym, {})
        vol_24h = det.get("vol_24h_usd")
        spread_pct = det.get("spread_pct")
        reason = det.get("reason", "ok")

        if sym in passed_syms:
            kept_after_basic.append(c)
            # 写通过记录
            try:
                cur.execute(
                    """
                    INSERT INTO liquidity_snapshot (
                        t_ref, symbol, passed,
                        vol_24h_usd, spread_pct, reason, created_at
                    )
                    VALUES (?, ?, 1, ?, ?, ?, ?)
                    ON CONFLICT(t_ref, symbol) DO UPDATE SET
                        passed=excluded.passed,
                        vol_24h_usd=excluded.vol_24h_usd,
                        spread_pct=excluded.spread_pct,
                        reason=excluded.reason
                    ;
                    """,
                    (t_ref, sym, vol_24h, spread_pct, reason, now_ts),
                )
            except Exception as e2:
                print(f"[decision_engine] liquidity snapshot(pass) failed: {e2}")
        else:
            # 第一段被挡
            rejected_all.append(
                {
                    "symbol": sym,
                    "timeframe": tf,
                    "direction": direction,
                    "reject_reason": reason,
                }
            )
            try:
                cur.execute(
                    """
                    INSERT INTO liquidity_snapshot (
                        t_ref, symbol, passed,
                        vol_24h_usd, spread_pct, reason, created_at
                    )
                    VALUES (?, ?, 0, ?, ?, ?, ?)
                    ON CONFLICT(t_ref, symbol) DO UPDATE SET
                        passed=excluded.passed,
                        vol_24h_usd=excluded.vol_24h_usd,
                        spread_pct=excluded.spread_pct,
                        reason=excluded.reason
                    ;
                    """,
                    (t_ref, sym, vol_24h, spread_pct, reason, now_ts),
                )
            except Exception as e2:
                print(f"[decision_engine] liquidity snapshot(reject-basic) failed: {e2}")

    try:
        conn.commit()
    except Exception as e:
        print(f"[decision_engine] liquidity commit (basic) failed: {e}")

    # 如果第一段就全死了，直接返回
    if not kept_after_basic:
        print("[decision_engine] liquidity gate: no symbols left after basic check")
        return [], rejected_all

    # ---------- 第二段：K线放量检查（和自己比） ----------
    try:
        kept_final, rejected_volume, diag = liquidity_filter.filter_by_volume_spike(
            conn=conn,
            candidates=kept_after_basic,
            t_ref=t_ref,
            cfg=cfg,
        )
        print(f"[decision_engine] volume-spike diag: {diag}")

        # 把数值证据挂到项上（仅新增字段，不影响原逻辑）
        try:
            per = (diag or {}).get("per_symbol") or {}
            if isinstance(per, dict):
                for c in kept_final:
                    s = c.get("symbol")
                    if s and s in per:
                        c["evidence_liquidity"] = per[s]
                for r in rejected_volume:
                    s = r.get("symbol")
                    if s and s in per:
                        r["evidence_liquidity"] = per[s]
        except Exception as e:
            print(f"[decision_engine] attach liquidity evidence failed: {e}")

        # 你的原有调试打印保持不动
        vol_cfg = (cfg.get("thresholds") or {}).get("volume_spike", {})
        if vol_cfg.get("debug"):
            for c in kept_final:
                if "volume_spike_ratio" in c:
                    print(
                        f"[liquidity_filter] PASS {c['symbol']} {c['timeframe']}: "
                        f"ratio={c['volume_spike_ratio']:.2f} "
                        f"(min={vol_cfg.get('min_spike_ratio', 1.5)})"
                    )
            for r in rejected_volume:
                print(
                    f"[liquidity_filter] REJECT {r['symbol']} {r['timeframe']}: "
                    f"{r.get('reject_reason')}"
                )

    except Exception as e:
        # 放量这一段不能影响整体流程，出错就忽略
        print(f"[decision_engine] volume-spike check failed, keep basic result. err={e}")
        return kept_after_basic, rejected_all

    # 把第二段被挡的也写进表
    for r in rejected_volume:
        sym = r.get("symbol")
        if not sym:
            continue
        tf = r.get("timeframe") or "1h"
        direction = (r.get("direction") or "").lower().strip()
        rej_reason = r.get("reject_reason") or "volume_spike_reject"
        rejected_all.append(
            {
                "symbol": sym,
                "timeframe": tf,
                "direction": direction,
                "reject_reason": rej_reason,
            }
        )
        try:
            cur.execute(
                """
                INSERT INTO liquidity_snapshot (
                    t_ref, symbol, passed,
                    vol_24h_usd, spread_pct, reason, created_at
                )
                VALUES (?, ?, 0, NULL, NULL, ?, ?)
                ON CONFLICT(t_ref, symbol) DO UPDATE SET
                    passed=excluded.passed,
                    reason=excluded.reason
                ;
                """,
                (t_ref, sym, rej_reason, now_ts),
            )
        except Exception as e2:
            print(f"[decision_engine] liquidity snapshot(reject-volume) failed: {e2}")

    try:
        conn.commit()
    except Exception as e:
        print(f"[decision_engine] liquidity commit (volume) failed: {e}")

    print(
        f"[decision_engine] liquidity gate final: kept={len(kept_final)}, "
        f"rejected_basic={len(rejected_all) - len(rejected_volume)}, "
        f"rejected_volume={len(rejected_volume)}"
    )

    return kept_final, rejected_all

def _apply_risk_cost_gate(
    conn,
    cfg: dict,
    t_ref: str,
    candidates: list[dict],
) -> tuple[list[dict], list[dict]]:
    from core.timebox import now_local_str
    now_ts = now_local_str(cfg.get("tz", "Asia/Shanghai"))
    """
    risk_cost 门（结构+成本）
    顺序：在流动性门之后、分 R 之前。
    本版默认禁止“ATR 回退”，只有提供了结构化 stop/take 的候选才参与评估。
    如需放开，可在 config.yml 下：
      core.risk_cost.fallback_policy: allow
    """

    import sqlite3
    from core import market_data  # 复用 _get_exchange / fetch_last_price

    rc_cfg = (
        cfg.get("core", {}).get("risk_cost")
        or cfg.get("risk_cost")
        or {}
    )
    enabled = bool(rc_cfg.get("enable", True))

    # ★ 新增：禁止/允许回退（默认禁止）
    fallback_policy = str(rc_cfg.get("fallback_policy", "reject")).lower()  # "reject" | "allow"

    max_stop_frac    = float(rc_cfg.get("max_stop_frac", 0.03))
    min_reward_frac  = float(rc_cfg.get("min_reward_frac", 0.01))
    rr_min           = float(rc_cfg.get("rr_min", 2.0))          # 毛RR下限
    rr_min_net       = float(rc_cfg.get("rr_min_net", rr_min))   # 净RR下限（默认等于 rr_min）
    stop_mult        = float(rc_cfg.get("stop_mult", 1.5))       # ATR * 1.5 当止损（仅在允许回退时使用）
    take_mult        = float(rc_cfg.get("take_mult", 2.5))       # ATR * 2.5 当目标（仅在允许回退时使用）
    atr_lookback     = int(rc_cfg.get("atr_lookback", 14))

    fees_cfg = (
        cfg.get("core", {}).get("fees")
        or cfg.get("fees")
        or {}
    )
    fee_bps        = float(fees_cfg.get("fee_bps", 0.0))       # 单边费率(bps)
    slippage_bps   = float(fees_cfg.get("slippage_bps", 0.0))  # 单边滑点(bps)

    print(
        f"[decision_engine] risk_cost gate @ {t_ref}: "
        f"incoming={len(candidates)}, enabled={enabled}, "
        f"max_stop_frac={max_stop_frac}, rr_min={rr_min}, rr_min_net={rr_min_net}, "
        f"fee_bps={fee_bps}, slippage_bps={slippage_bps}, fallback_policy={fallback_policy}"
    )
    if not enabled:
        return candidates, []

    cur = conn.cursor()

    def _fetch_last_close_from_db(symbol: str, timeframe: str, tref: str) -> float | None:
        try:
            row = cur.execute(
                """
                SELECT close FROM ohlcv
                WHERE symbol=? AND timeframe=? AND t<=?
                ORDER BY t DESC LIMIT 1
                """,
                (symbol, timeframe, tref),
            ).fetchone()
            if row and row[0] is not None:
                px = float(row[0])
                if px > 0:
                    return px
        except Exception:
            pass
        return None

    def _estimate_atr14(symbol: str, timeframe: str, tref: str, lookback: int = 14) -> float:
        # 仅在 fallback_policy=allow 时才会被调用
        try:
            rows = cur.execute(
                """
                SELECT t, open, high, low, close
                FROM ohlcv
                WHERE symbol=? AND timeframe=? AND t<=?
                ORDER BY t DESC
                LIMIT ?
                """,
                (symbol, timeframe, tref, max(lookback + 1, 15)),
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            # 可选回退表（保持与原版一致）
            for tn in ("ohlcv_raw", "ohlcv_cache"):
                try:
                    rs = cur.execute(
                        f"""
                        SELECT t, open, high, low, close
                        FROM {tn}
                        WHERE symbol=? AND timeframe=? AND t<=?
                        ORDER BY t DESC
                        LIMIT ?
                        """,
                        (symbol, timeframe, tref, max(lookback + 1, 15)),
                    ).fetchall()
                except Exception:
                    rs = []
                if rs:
                    rows = rs
                    break

        if not rows:
            return 0.0

        trs = []
        prev_close = None
        for r in rows[::-1]:  # 从老到新
            _h = float(r[2]); _l = float(r[3]); _c = float(r[4])
            if prev_close is None:
                tr = _h - _l
            else:
                tr = max(_h - _l, abs(_h - prev_close), abs(prev_close - _l))
            trs.append(tr)
            prev_close = _c
        return (sum(trs) / len(trs)) if trs else 0.0

    def _try_fetch_spread_frac(symbol: str) -> float:
        """
        取 (ask - bid) / price 的近似点差占比；失败返回 0
        """
        try:
            ex = market_data._get_exchange((cfg.get("core") or cfg))  # 兼容 core.exchange
            tk = ex.fetch_ticker(symbol)
            bid = tk.get("bid"); ask = tk.get("ask")
            if bid and ask and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
                if mid > 0:
                    return (ask - bid) / mid
        except Exception:
            pass
        return 0.0

    kept: list[dict] = []
    rejected: list[dict] = []

    for c in candidates:
        sym = c.get("symbol"); tf = c.get("timeframe")
        if not sym or not tf:
            reason = "risk_cost_bad_symbol_tf"
            rejected.append({"symbol": sym, "timeframe": tf, "reason": reason, "reject_reason": reason})
            try:
                cur.execute(
                    """
                    INSERT INTO risk_cost_snapshot (
                        t_ref, symbol, timeframe, passed,
                        max_stop_frac, rr_min_req, reason, created_at
                    )
                    VALUES (?, ?, ?, 0, ?, ?, ?, ?)
                    ON CONFLICT(t_ref, symbol, timeframe) DO UPDATE SET
                        passed=excluded.passed,
                        reason=excluded.reason
                    """,
                    (t_ref, sym, tf, max_stop_frac, rr_min, reason, now_ts),
                )
            except Exception:
                pass
            continue

        # 1) 入场价：候选 -> 实时 -> DB
        entry_price = None
        if c.get("entry_price"):
            try:
                entry_price = float(c.get("entry_price"))
            except (TypeError, ValueError):
                entry_price = None
        if not entry_price or entry_price <= 0:
            import logging
            logging.debug("[probe] risk_cost.fetch_last_price args: sym=%s tf=%s t_ref=%s cfg_type=%s",
                          sym, tf, t_ref, type(cfg).__name__)
            try:
                entry_price = market_data.fetch_last_price(
                    conn=conn, symbol=sym, timeframe=tf, t_ref=t_ref, cfg=(cfg.get("core") or cfg)
                )
            except Exception as e:
                logging.error("[probe] risk_cost.fetch_last_price raised: %s", e)
                entry_price = None
        if not entry_price or entry_price <= 0:
            entry_price = _fetch_last_close_from_db(sym, tf, t_ref)
        if not entry_price or entry_price <= 0:
            reason = "risk_cost_no_price"
            rejected.append({"symbol": sym, "timeframe": tf, "reason": reason, "reject_reason": reason})
            try:
                cur.execute(
                    """
                    INSERT INTO risk_cost_snapshot (
                        t_ref, symbol, timeframe, passed,
                        max_stop_frac, rr_min_req, reason, created_at
                    )
                    VALUES (?, ?, ?, 0, ?, ?, ?, ?)
                    ON CONFLICT(t_ref, symbol, timeframe) DO UPDATE SET
                        passed=excluded.passed,
                        reason=excluded.reason
                    """,
                    (t_ref, sym, tf, max_stop_frac, rr_min, reason, now_ts),
                )
            except Exception:
                pass
            continue

        # 2) 止损/目标：强制需要“结构化”价位；不允许则直接拒单
        stop_price  = c.get("stop_loss")   or c.get("stop_price")
        take_price  = c.get("take_profit") or c.get("target_price")

        # 标记缺失
        missing_stop = (stop_price is None or (isinstance(stop_price, str) and not stop_price))
        missing_take = (take_price is None or (isinstance(take_price, str) and not take_price))

        # 尝试转 float
        if not missing_stop:
            try:
                stop_price = float(stop_price)
            except (TypeError, ValueError):
                missing_stop = True
        if not missing_take:
            try:
                take_price = float(take_price)
            except (TypeError, ValueError):
                missing_take = True

        if fallback_policy == "reject" and (missing_stop or missing_take):
            miss = []
            if missing_stop: miss.append("stop")
            if missing_take: miss.append("take")
            rej_reason = f"risk_cost_requires_structured_levels(missing={','.join(miss)})"
            rejected.append({"symbol": sym, "timeframe": tf, "reason": rej_reason, "reject_reason": rej_reason})
            try:
                cur.execute(
                    """
                    INSERT INTO risk_cost_snapshot (
                        t_ref, symbol, timeframe, passed,
                        stop_frac, rr_est, max_stop_frac, rr_min_req, reason, created_at
                    )
                    VALUES (?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?)
                    ON CONFLICT(t_ref, symbol, timeframe) DO UPDATE SET
                        passed=excluded.passed,
                        reason=excluded.reason
                    """,
                    (t_ref, sym, tf, max_stop_frac, rr_min, rej_reason, now_ts),
                )
            except Exception:
                pass
            continue

        # 允许回退时，才用 ATR 推断缺失的 stop/take
        stop_frac = None
        reward_frac = None
        rr_est = None

        dir_side = str(c.get("direction") or "").lower()

        if missing_stop and fallback_policy == "allow":
            atr = _estimate_atr14(sym, tf, t_ref, atr_lookback) or 0.0
            if atr <= 0:
                stop_frac = 1e6
            else:
                stop_dist = stop_mult * atr
                stop_frac = stop_dist / entry_price if entry_price > 0 else 1e6
        else:
            # 使用结构 stop
            stop_dist = (entry_price - stop_price) if dir_side == "long" else (stop_price - entry_price)
            stop_frac = stop_dist / entry_price if entry_price > 0 else None

        if missing_take and fallback_policy == "allow":
            atr = _estimate_atr14(sym, tf, t_ref, atr_lookback) or 0.0
            if atr > 0:
                reward_dist = take_mult * atr
                reward_frac = reward_dist / entry_price if entry_price > 0 else 0.0
            else:
                reward_frac = 0.0
        else:
            # 使用结构 target
            reward_dist = (take_price - entry_price) if dir_side == "long" else (entry_price - take_price)
            reward_frac = reward_dist / entry_price if entry_price > 0 else 0.0

        if stop_frac and stop_frac > 0:
            rr_est = (reward_frac / stop_frac) if reward_frac is not None else 0.0
        else:
            rr_est = 0.0

        # 3) 成本折算成 R，得到净 RR
        spread_frac = _try_fetch_spread_frac(sym)            # e.g. 0.0008 = 0.08%
        fee_frac    = 2.0 * (fee_bps / 10000.0)             # 双边手续费
        slip_frac   = (slippage_bps / 10000.0)              # 单边滑点（保守）
        total_cost_frac = max(0.0, fee_frac + slip_frac + spread_frac)

        cost_R   = (total_cost_frac / stop_frac) if (stop_frac and stop_frac > 0) else float("inf")
        rr_net   = rr_est - cost_R

        # 4) 判定
        rej_reason = None
        if stop_frac is None or stop_frac <= 0:
            rej_reason = "risk_cost_bad_stop"
        elif stop_frac > max_stop_frac:
            rej_reason = f"risk_cost_stop_too_wide({stop_frac:.4f}>{max_stop_frac:.4f})"
        elif reward_frac is None or reward_frac < min_reward_frac:
            rej_reason = f"risk_cost_reward_too_small({(reward_frac or 0):.4f}<{min_reward_frac:.4f})"
        elif rr_est is None or rr_est < rr_min:
            rej_reason = f"risk_cost_rr_too_low({(rr_est or 0):.2f}<{rr_min:.2f})"
        elif rr_net < rr_min_net:
            rej_reason = (
                f"risk_cost_net_rr_too_low(net={rr_net:.2f}<{rr_min_net:.2f}; "
                f"gross={rr_est:.2f}, costR={cost_R:.2f}, "
                f"fee_bps={fee_bps:.1f}, slip_bps={slippage_bps:.1f}, spread={spread_frac*100:.3f}%)"
            )

        # 5) 记录 + 输出
        if rej_reason is None:
            kept.append(c)
            c["evidence_risk_cost"] = {
                "pass": 1,
                "rr_actual": rr_est,
                "rr_net": rr_net,
                "max_stop_frac": max_stop_frac,
                "basis": "STRUCTURED",
                "reason": "ok",
                "fee_bps": fee_bps,
                "slippage_bps": slippage_bps,
                "spread_frac": spread_frac,
                "cost_R": cost_R,
                "rr_min": rr_min,
                "rr_min_net": rr_min_net,
            }
            try:
                cur.execute(
                    """
                    INSERT INTO risk_cost_snapshot (
                        t_ref, symbol, timeframe, passed,
                        stop_frac, rr_est, max_stop_frac, rr_min_req, reason, created_at
                    )
                    VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(t_ref, symbol, timeframe) DO UPDATE SET
                        passed=excluded.passed,
                        stop_frac=excluded.stop_frac,
                        rr_est=excluded.rr_est,
                        reason=excluded.reason
                    ;
                    """,
                    (
                        t_ref, sym, tf,
                        float(stop_frac) if stop_frac is not None else None,
                        float(rr_est) if rr_est is not None else None,
                        max_stop_frac,
                        rr_min,
                        "ok",
                        now_ts,
                    ),
                )
            except Exception as e2:
                print(f"[decision_engine] risk_cost snapshot(pass) failed: {e2}")
        else:
            rej_item = {"symbol": sym, "timeframe": tf, "reason": rej_reason, "reject_reason": rej_reason}
            rejected.append(rej_item)
            c["evidence_risk_cost"] = {
                "pass": 0,
                "rr_actual": rr_est,
                "rr_net": rr_net,
                "max_stop_frac": max_stop_frac,
                "basis": "STRUCTURED" if fallback_policy == "reject" else "MIXED",
                "reason": rej_reason,
                "fee_bps": fee_bps,
                "slippage_bps": slippage_bps,
                "spread_frac": spread_frac,
                "cost_R": cost_R,
                "rr_min": rr_min,
                "rr_min_net": rr_min_net,
            }
            try:
                cur.execute(
                    """
                    INSERT INTO risk_cost_snapshot (
                        t_ref, symbol, timeframe, passed,
                        stop_frac, rr_est, max_stop_frac, rr_min_req, reason, created_at
                    )
                    VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(t_ref, symbol, timeframe) DO UPDATE SET
                        passed=excluded.passed,
                        stop_frac=excluded.stop_frac,
                        rr_est=excluded.rr_est,
                        reason=excluded.reason
                    ;
                    """,
                    (
                        t_ref, sym, tf,
                        float(stop_frac) if (stop_frac is not None and stop_frac != float("inf")) else None,
                        float(rr_est) if rr_est is not None else None,
                        max_stop_frac,
                        rr_min,
                        rej_reason,
                        now_ts,
                    ),
                )
            except Exception as e2:
                print(f"[decision_engine] risk_cost snapshot(reject) failed: {e2}")

    try:
        conn.commit()
    except Exception as e:
        print(f"[decision_engine] risk_cost commit failed: {e}")

    print(f"[decision_engine] risk_cost gate result: kept={len(kept)}, rejected={len(rejected)}")
    return kept, rejected


def _attach_struct_levels_by_swings(
    conn,
    cfg: dict,
    t_ref: str,
    candidates: list[dict],
) -> list[dict]:
    """
    给“缺 stop/take”的候选，按最近摆动结构补齐结构化止损/目标。
    - 不写库，只在内存补字段；
    - 已有 stop/take 的不动；
    - 用近N根K的摆动低点/高点作为结构stop；take 用 rr_min * 风险距离推算。
    """
    cur = conn.cursor()

    # 配置
    conf = (cfg.get("core", {}).get("struct_levels")
            or cfg.get("struct_levels")
            or {})
    swing_lookback = int(conf.get("swing_lookback_bars", 12))
    left = int(conf.get("swing_left", 2))
    right = int(conf.get("swing_right", 2))
    buffer_bps = float(conf.get("buffer_bps", 10.0))     # 止损缓冲，默认10bps
    buffer_frac = buffer_bps / 10000.0

    rc_cfg = (cfg.get("core", {}).get("risk_cost")
              or cfg.get("risk_cost")
              or {})
    rr_min = float(rc_cfg.get("rr_min", 2.0))             # 用 rr_min 推算目标

    def _last_close(sym: str, tf: str, tref: str) -> float | None:
        row = cur.execute(
            "SELECT close FROM ohlcv WHERE symbol=? AND timeframe=? AND t<=? "
            "ORDER BY t DESC LIMIT 1",
            (sym, tf, tref),
        ).fetchone()
        try:
            return float(row[0]) if row and row[0] is not None else None
        except Exception:
            return None

    def _load_ohlcv(sym: str, tf: str, tref: str, n: int):
        rows = cur.execute(
            "SELECT t, open, high, low, close FROM ohlcv "
            "WHERE symbol=? AND timeframe=? AND t<=? "
            "ORDER BY t DESC LIMIT ?",
            (sym, tf, tref, max(n, left + right + 3)),
        ).fetchall()
        return rows[::-1] if rows else []

    def _recent_pivot(rows, kind: str):
        # rows: oldest->newest；返回最近一个摆动点 (idx, price)
        if not rows:
            return None
        highs = [float(r[2]) for r in rows]
        lows  = [float(r[3]) for r in rows]
        pivots = []
        for i in range(left, len(rows) - right):
            if kind == "low":
                window = lows[i - left:i + right + 1]
                if lows[i] == min(window):
                    pivots.append((i, lows[i]))
            else:
                window = highs[i - left:i + right + 1]
                if highs[i] == max(window):
                    pivots.append((i, highs[i]))
        return pivots[-1] if pivots else None

    out: list[dict] = []
    for c in candidates:
        # 已有结构则不动
        stop = c.get("stop_loss") or c.get("stop_price")
        take = c.get("take_profit") or c.get("target_price")
        if stop and take:
            out.append(c)
            continue

        sym = c.get("symbol"); tf = c.get("timeframe")
        side = str(c.get("direction") or "").lower()
        if not sym or not tf or side not in ("long", "short"):
            out.append(c)
            continue

        rows = _load_ohlcv(sym, tf, t_ref, swing_lookback + left + right + 5)
        if len(rows) < left + right + 3:
            out.append(c)
            continue

        # 入场价：优先候选，其次DB最新close
        entry = c.get("entry_price")
        try:
            entry = float(entry) if entry is not None else None
        except Exception:
            entry = None
        if not entry:
            entry = _last_close(sym, tf, t_ref)
        if not entry:
            out.append(c)
            continue

        # 补 stop
        if not stop:
            if side == "long":
                piv = _recent_pivot(rows, "low")
                if piv:
                    stop_val = piv[1] * (1.0 - buffer_frac)
                    c["stop_loss"] = stop_val
                    print(f"[struct] {t_ref} {sym} {tf} LONG stop<-pivot_low={piv[1]:.6f} buf={buffer_bps}bps -> {stop_val:.6f}")
            else:
                piv = _recent_pivot(rows, "high")
                if piv:
                    stop_val = piv[1] * (1.0 + buffer_frac)
                    c["stop_loss"] = stop_val
                    print(f"[struct] {t_ref} {sym} {tf} SHORT stop<-pivot_high={piv[1]:.6f} buf={buffer_bps}bps -> {stop_val:.6f}")

        # 补 take（基于 rr_min * 风险距离）
        take = c.get("take_profit") or c.get("target_price")
        stop = c.get("stop_loss") or c.get("stop_price")
        try:
            stop = float(stop) if stop is not None else None
        except Exception:
            stop = None
        if (take is None) and (stop is not None):
            if side == "long":
                risk = entry - stop
                if risk > 0:
                    c["take_profit"] = entry + rr_min * risk
                    print(f"[struct] {t_ref} {sym} {tf} LONG take=entry+rr_min*risk -> {c['take_profit']:.6f}")
            else:
                risk = stop - entry
                if risk > 0:
                    c["take_profit"] = entry - rr_min * risk
                    print(f"[struct] {t_ref} {sym} {tf} SHORT take=entry-rr_min*risk -> {c['take_profit']:.6f}")

        out.append(c)

    return out



# ---------------------------------------------------------
# 主入口（带 BTC 对齐硬门，流动性门，risk_cost 门，兼容返回2值/3值的版本）
# ---------------------------------------------------------
# ---------------------------------------------------------
# 主入口（带 BTC 对齐硬门，流动性门，risk_cost 门，兼容返回2值/3值的版本）
# ---------------------------------------------------------

def build_trade_plan(
    conn,
    cfg: dict,
    pipeline_state: dict,
) -> dict:
    """
    输出 trade_plan，不动 positions_virtual。
    run_open_cycle.py 会拿这个去真正落单。
    """
    from core import correlation_gate
    import json
    t_now = now_local_str(cfg.get("tz", "Asia/Shanghai"))  # [保持] 本地时间戳

    # ----------------- 内部：写 decision_snapshot -----------------
    def _write_decision_snapshot_batch_local(
        conn_local,
        t_ref_local,
        approved_local,
        rejected_local,
        cfg_local,
        risk_summary_local,
    ):
        """
        作用：
          1) 写 decision_snapshot（保持原语义）
          2) 删除并重建本次快照对应的 decision_evidence（最小证据：final/protection + 显式拒因）
          3) 若条目上带有 evidence_liquidity，则追加 gate='liquidity' 的数值证据
          4) 返回 snapshot_id_map：{(symbol,timeframe): snapshot_id}
        """
        import json

        strat_ver = str(cfg_local.get("strategy_version", "dev"))

        dd_kill_active_flag = bool(risk_summary_local.get("dd_kill_active", False))
        panic_flag_local = bool(risk_summary_local.get("panic_selloff", False))
        breadth_block_reason = risk_summary_local.get("breadth_block_reason")
        corr_block_reason = risk_summary_local.get("correlation_block_reason")
        btc_align_diag = risk_summary_local.get("btc_align_diag")
        liquidity_block_reason = risk_summary_local.get("liquidity_block_reason")
        risk_cost_block_reason = risk_summary_local.get("risk_cost_block_reason")

        cur = conn_local.cursor()
        snapshot_id_map = {}
        cleared_ids = set()

        def _nz(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        def _reason_gate(reason_text: str) -> str:
            if not reason_text:
                return "final"
            r = reason_text.lower()
            if r.startswith("risk_cost"):
                return "risk_cost"
            if r.startswith("liquidity"):
                return "liquidity"
            if r.startswith("panic") or r.startswith("dd_kill") or "panic_selloff" in r:
                return "protection"
            if r.startswith("breadth"):
                return "breadth"
            if r.startswith("corr") or r.startswith("correlation"):
                return "correlation"
            if r.startswith("btc_align"):
                return "btc_align"
            return "final"

        def _insert_evidence_row(snapshot_id, sym, tf, gate, passed, data_obj, thr_obj, src_obj, notes):
            # 同一 snapshot_id 在首次插入前清理旧记录
            if snapshot_id not in cleared_ids:
                try:
                    cur.execute("DELETE FROM decision_evidence WHERE snapshot_id=?;", (snapshot_id,))
                except Exception:
                    pass
                cleared_ids.add(snapshot_id)
            try:
                cur.execute(
                    """
                    INSERT INTO decision_evidence(
                        snapshot_id, t_ref, symbol, timeframe, gate, pass,
                        data_json, threshold_json, source_json, notes, created_at
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        snapshot_id, t_ref_local, sym, tf, gate,
                        (int(passed) if passed is not None else None),
                        json.dumps(data_obj, ensure_ascii=False) if data_obj is not None else None,
                        json.dumps(thr_obj, ensure_ascii=False) if thr_obj is not None else None,
                        json.dumps(src_obj, ensure_ascii=False) if src_obj is not None else None,
                        notes,
                        t_now,
                    ),
                )
            except Exception as e:
                print(f"[decision_engine] evidence insert failed gate={gate} {sym} {tf}: {e}")

        # ---- 写通过的 ----
        for a in approved_local:
            sym = a.get("symbol")
            tf = a.get("timeframe")
            if not sym or not tf:
                print("[decision_engine] snapshot SKIP (approved missing sym/tf):", a)
                continue

            prob = a.get("prob")
            score = a.get("score")
            strength = a.get("strength")
            synth_score = _nz(prob) * 2.0 + _nz(score) * 1.0 + _nz(strength) * 0.5

            rs = ["approved", f"strategy_version={strat_ver}"]
            if dd_kill_active_flag:
                rs.append("dd_kill_active=True")
            if panic_flag_local:
                rs.append("panic_selloff=True")
            if breadth_block_reason:
                rs.append(f"breadth_block_reason={breadth_block_reason}")
            if corr_block_reason:
                rs.append(f"correlation_block_reason={corr_block_reason}")
            if liquidity_block_reason:
                rs.append(f"liquidity_block_reason={liquidity_block_reason}")
            if risk_cost_block_reason:
                rs.append(f"risk_cost_block_reason={risk_cost_block_reason}")
            if btc_align_diag:
                rs.append(f"btc_align_diag={json.dumps(btc_align_diag, ensure_ascii=False)}")

            position_scale_val = a.get("position_scale", 1.0)

            reasons_obj = {
                "final": "open",
                "strategy_version": strat_ver,
                "gates": {
                    "breadth": breadth_block_reason or None,
                    "correlation": corr_block_reason or None,
                    "liquidity": liquidity_block_reason or None,
                    "risk_cost": risk_cost_block_reason or None,
                },
                "flags": {
                    "dd_kill_active": dd_kill_active_flag,
                    "panic_selloff": panic_flag_local,
                },
                "btc_align_diag": btc_align_diag or None,
            }

            print(
                "[decision_engine] snapshot WRITE",
                t_ref_local,
                sym,
                tf,
                {"final": "open", "scale": position_scale_val, "synth_score": synth_score, "reasons": rs},
            )

            cur.execute(
                """
                INSERT INTO decision_snapshot(
                    t_ref, symbol, timeframe, source,
                    final, score, reasons_json,
                    position_scale, synth_score, reasons_obj, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(t_ref, symbol, timeframe)
                DO UPDATE SET
                    final=excluded.final,
                    score=excluded.score,
                    reasons_json=excluded.reasons_json,
                    position_scale=excluded.position_scale,
                    synth_score=excluded.synth_score,
                    reasons_obj=excluded.reasons_obj
                """,
                (
                    t_ref_local,
                    sym,
                    tf,
                    a.get("source"),
                    "open",
                    _nz(score),
                    json.dumps(rs, ensure_ascii=False),
                    float(position_scale_val),
                    synth_score,
                    json.dumps(reasons_obj, ensure_ascii=False),
                    t_now,
                ),
            )

            # 取回 id 并写最小证据
            row = cur.execute(
                "SELECT id FROM decision_snapshot WHERE t_ref=? AND symbol=? AND timeframe=? LIMIT 1;",
                (t_ref_local, sym, tf),
            ).fetchone()
            if row and row[0] is not None:
                sid = int(row[0])
                snapshot_id_map[(sym, tf)] = sid
                _insert_evidence_row(
                    sid, sym, tf, "final", 1,
                    {"decision": "open"},
                    None,
                    {"table": "decision_snapshot"},
                    None,
                )
                _insert_evidence_row(
                    sid, sym, tf, "protection", 1,
                    {"panic_selloff": panic_flag_local, "dd_kill_active": dd_kill_active_flag},
                    None,
                    {"table": "equity_snapshot", "source": "risk_summary"},
                    None,
                )
                # ★ 若该条目携带了 liquidity 数值证据，则写 gate='liquidity'
                ev_l = a.get("evidence_liquidity")
                if isinstance(ev_l, dict):
                    try:
                        data_obj = {
                            "turnover_usd_curr": ev_l.get("turnover_usd_curr"),
                            "turnover_usd_avgN": ev_l.get("turnover_usd_avgN"),
                            "spike_ratio": ev_l.get("spike_ratio"),
                            "window_N": ev_l.get("lookback_bars"),
                        }
                        thr_obj = {
                            "min_spike_ratio": ev_l.get("min_spike_ratio"),
                        }
                        src_obj = ev_l.get("source") if isinstance(ev_l.get("source"), dict) else {"source": "liquidity_filter.filter_by_volume_spike"}
                        passed_flag = ev_l.get("pass")
                        if passed_flag is True:
                            pval = 1
                        elif passed_flag is False:
                            pval = 0
                        else:
                            pval = None
                        _insert_evidence_row(
                            sid, sym, tf, "liquidity", pval,
                            data_obj, thr_obj, src_obj, ev_l.get("reason"),
                        )
                    except Exception as e:
                        print(f"[decision_engine] write liquidity evidence (approved) failed: {e}")

        # ---- 写拒绝的 ----
        for r in rejected_local:
            sym = r.get("symbol")
            tf = r.get("timeframe")
            if not sym or not tf:
                print("[decision_engine] snapshot SKIP (rejected missing sym/tf):", r)
                continue

            rej_reason = r.get("reject_reason") or r.get("reason") or "rejected"

            rs = [
                "rejected",
                f"reason={rej_reason}",
                f"strategy_version={strat_ver}",
            ]
            if dd_kill_active_flag:
                rs.append("dd_kill_active=True")
            if panic_flag_local:
                rs.append("panic_selloff=True")
            if breadth_block_reason:
                rs.append(f"breadth_block_reason={breadth_block_reason}")
            if corr_block_reason:
                rs.append(f"correlation_block_reason={corr_block_reason}")
            if liquidity_block_reason:
                rs.append(f"liquidity_block_reason={liquidity_block_reason}")
            if risk_cost_block_reason:
                rs.append(f"risk_cost_block_reason={risk_cost_block_reason}")
            if btc_align_diag:
                rs.append(f"btc_align_diag={json.dumps(btc_align_diag, ensure_ascii=False)}")

            reason_code = (rej_reason or "").split(" ", 1)[0].split("(")[0]
            reasons_obj = {
                "final": "skip",
                "reason_code": reason_code,
                "reason_text": rej_reason,
                "strategy_version": strat_ver,
                "gates": {
                    "breadth": breadth_block_reason or None,
                    "correlation": corr_block_reason or None,
                    "liquidity": liquidity_block_reason or None,
                    "risk_cost": risk_cost_block_reason or None,
                },
                "flags": {
                    "dd_kill_active": dd_kill_active_flag,
                    "panic_selloff": panic_flag_local,
                },
                "btc_align_diag": btc_align_diag or None,
            }

            print(
                "[decision_engine] snapshot WRITE",
                t_ref_local,
                sym,
                tf,
                {"final": "skip", "reasons": rs},
            )

            cur.execute(
                """
                INSERT INTO decision_snapshot(
                    t_ref, symbol, timeframe, source,
                    final, score, reasons_json,
                    position_scale, synth_score, reasons_obj, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(t_ref, symbol, timeframe)
                DO UPDATE SET
                    final=excluded.final,
                    score=excluded.score,
                    reasons_json=excluded.reasons_json,
                    position_scale=excluded.position_scale,
                    synth_score=excluded.synth_score,
                    reasons_obj=excluded.reasons_obj
                """,
                (
                    t_ref_local,
                    sym,
                    tf,
                    None,
                    "skip",
                    0.0,
                    json.dumps(rs, ensure_ascii=False),
                    0.0,
                    None,
                    json.dumps(reasons_obj, ensure_ascii=False),
                    t_now,
                ),
            )

            # 取回 id 并写最小证据 + gate 映射；若有 liquidity 证据也一并写入
            row = cur.execute(
                "SELECT id FROM decision_snapshot WHERE t_ref=? AND symbol=? AND timeframe=? LIMIT 1;",
                (t_ref_local, sym, tf),
            ).fetchone()
            if row and row[0] is not None:
                sid = int(row[0])
                snapshot_id_map[(sym, tf)] = sid
                _insert_evidence_row(
                    sid, sym, tf, "final", 0,
                    {"decision": "skip", "reason": rej_reason},
                    None,
                    {"table": "decision_snapshot"},
                    None,
                )
                gate = _reason_gate(rej_reason)
                _insert_evidence_row(
                    sid, sym, tf, gate, 0,
                    {"reason": rej_reason},
                    None,
                    {"source": "decision_engine.reasons"},
                    None,
                )
                _insert_evidence_row(
                    sid, sym, tf, "protection", None,
                    {"panic_selloff": panic_flag_local, "dd_kill_active": dd_kill_active_flag},
                    None,
                    {"table": "equity_snapshot", "source": "risk_summary"},
                    None,
                )
                ev_l = r.get("evidence_liquidity")
                if isinstance(ev_l, dict):
                    try:
                        data_obj = {
                            "turnover_usd_curr": ev_l.get("turnover_usd_curr"),
                            "turnover_usd_avgN": ev_l.get("turnover_usd_avgN"),
                            "spike_ratio": ev_l.get("spike_ratio"),
                            "window_N": ev_l.get("lookback_bars"),
                        }
                        thr_obj = {
                            "min_spike_ratio": ev_l.get("min_spike_ratio"),
                        }
                        src_obj = ev_l.get("source") if isinstance(ev_l.get("source"), dict) else {"source": "liquidity_filter.filter_by_volume_spike"}
                        passed_flag = ev_l.get("pass")
                        if passed_flag is True:
                            pval = 1
                        elif passed_flag is False:
                            pval = 0
                        else:
                            pval = None
                        _insert_evidence_row(
                            sid, sym, tf, "liquidity", pval,
                            data_obj, thr_obj, src_obj, ev_l.get("reason"),
                        )
                    except Exception as e:
                        print(f"[decision_engine] write liquidity evidence (rejected) failed: {e}")

        conn_local.commit()
        return snapshot_id_map

    # ----------------- 主流程 -----------------
    t_ref = pipeline_state.get("t_ref")
    timeframe = pipeline_state.get("timeframe") or "1h"
    candidates_ready = list(pipeline_state.get("candidates_ready") or [])
    breadth_info = pipeline_state.get("breadth_info") or {}
    market_ctx = pipeline_state.get("market_ctx") or {}
    breadth_gate = pipeline_state.get("breadth_gate") or {}
    btc_alignment_map = pipeline_state.get("btc_alignment") or {}
    dd_kill_flag = bool(pipeline_state.get("dd_kill", False))

    # 0) ★ 空桶占位，防止同桶反复处理
    if not candidates_ready:
        try:
            cur = conn.cursor()
            strat_ver = str(cfg.get("strategy_version", "dev"))
            reasons = ["noop", "reason=no_candidates", f"strategy_version={strat_ver}"]
            reasons_obj = {
                "final": "noop",
                "reason_code": "no_candidates",
                "strategy_version": strat_ver,
            }
            cur.execute(
                """
                INSERT INTO decision_snapshot(
                    t_ref, symbol, timeframe, source,
                    final, score, reasons_json,
                    position_scale, synth_score, reasons_obj, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(t_ref, symbol, timeframe)
                DO UPDATE SET
                    final=excluded.final,
                    reasons_json=excluded.reasons_json,
                    reasons_obj=excluded.reasons_obj
                """,
                (
                    t_ref,
                    "__BUCKET_NOOP__",
                    timeframe,
                    None,
                    "noop",
                    0.0,
                    json.dumps(reasons, ensure_ascii=False),
                    0.0,
                    None,
                    json.dumps(reasons_obj, ensure_ascii=False),
                    t_now,
                ),
            )
            conn.commit()
            print(f"[decision_engine] noop snapshot WRITE {t_ref} {timeframe} __BUCKET_NOOP__")
        except Exception as e:
            print("[decision_engine] noop snapshot write failed:", e)

        bias_val0 = _infer_bias_from_breadth(breadth_info)
        risk_summary0 = {
            "bias": bias_val0,
            "panic_selloff": bool(market_ctx.get("panic_selloff", False)),
            "max_R_per_bucket": float(cfg["risk"].get("max_R_per_bucket", 3.0)),
            "base_R_per_trade": float(cfg["risk"].get("base_R_per_trade", 1.0)),
            "total_R_planned": 0.0,
            "num_final": 0,
            "dd_kill_active": False,
        }
        return {
            "t_ref": t_ref,
            "approved": [],
            "rejected": [],
            "risk_summary": risk_summary0,
            "breadth_info": breadth_info,
            "market_ctx": market_ctx,
            "evictions": [],
        }

    # 0.5) 熔断直接全拒
    if dd_kill_flag:
        rejected_all = []
        for c in candidates_ready:
            rejected_all.append(
                {
                    "symbol": c.get("symbol", "?"),
                    "timeframe": c.get("timeframe") or "1h",
                    "direction": (c.get("direction") or "").lower().strip(),
                    "reject_reason": "dd_kill_active",
                }
            )

        risk_summary = {
            "bias": _infer_bias_from_breadth(breadth_info),
            "panic_selloff": bool(market_ctx.get("panic_selloff", False)),
            "max_R_per_bucket": float(cfg["risk"].get("max_R_per_bucket", 3.0)),
            "base_R_per_trade": float(cfg["risk"].get("base_R_per_trade", 1.0)),
            "total_R_planned": 0.0,
            "num_final": 0,
            "dd_kill_active": True,
        }

        out = {
            "t_ref": t_ref,
            "approved": [],
            "rejected": rejected_all,
            "risk_summary": risk_summary,
            "breadth_info": breadth_info,
            "market_ctx": market_ctx,
            "evictions": [],
        }

        try:
            _write_decision_snapshot_batch_local(
                conn_local=conn,
                t_ref_local=t_ref,
                approved_local=[],
                rejected_local=rejected_all,
                cfg_local=cfg,
                risk_summary_local=risk_summary,
            )
        except Exception as e:
            print("[decision_engine] snapshot write failed(dd_kill):", e)

        print(
            "[decision_engine] plan summary:",
            {
                "t_ref": t_ref,
                "approved_n": 0,
                "used_R": 0.0,
                "panic_selloff": risk_summary["panic_selloff"],
                "bias": risk_summary["bias"],
                "dd_kill_active": True,
            },
        )
        return out

    # 1) 广度一刀切
    if str(breadth_gate.get("gate", "pass")).lower() == "block":
        used_R = 0.0
        max_R_bucket = float(cfg["risk"].get("max_R_per_bucket", 3.0))
        base_R = float(cfg["risk"].get("base_R_per_trade", 1.0))

        bias_val = _infer_bias_from_breadth(breadth_info)
        panic_flag = bool(market_ctx.get("panic_selloff", False))
        block_reason = breadth_gate.get("reason", "breadth_block")

        rejected_all = []
        for c in candidates_ready:
            rejected_all.append(
                {
                    "symbol": c.get("symbol", "?"),
                    "timeframe": c.get("timeframe") or "1h",
                    "direction": (c.get("direction") or "").lower().strip(),
                    "reject_reason": block_reason,
                }
            )

        risk_summary = {
            "bias": bias_val,
            "panic_selloff": panic_flag,
            "max_R_per_bucket": max_R_bucket,
            "base_R_per_trade": base_R,
            "total_R_planned": used_R,
            "num_final": 0,
            "breadth_block_reason": block_reason,
            "dd_kill_active": False,
        }

        out = {
            "t_ref": t_ref,
            "approved": [],
            "rejected": rejected_all,
            "risk_summary": risk_summary,
            "breadth_info": breadth_info,
            "market_ctx": market_ctx,
            "evictions": [],
        }

        try:
            _write_decision_snapshot_batch_local(
                conn_local=conn,
                t_ref_local=t_ref,
                approved_local=[],
                rejected_local=rejected_all,
                cfg_local=cfg,
                risk_summary_local=risk_summary,
            )
        except Exception as e:
            print("[decision_engine] snapshot write failed(breadth_block):", e)

        print(
            "[decision_engine] plan summary:",
            {
                "t_ref": t_ref,
                "approved_n": 0,
                "used_R": used_R,
                "panic_selloff": panic_flag,
                "bias": bias_val,
                "breadth_block_reason": block_reason,
            },
        )
        return out

    # 2) 市场情绪过滤
    bias_val = _infer_bias_from_breadth(breadth_info)
    stage2_kept, stage2_rejected_list = _filter_by_market_context(
        candidates_ready,
        market_ctx,
        bias_val,
        btc_alignment_map,
    )

    # 2.5) BTC 对齐硬门
    btc_res = btc_alignment_hard_filter(stage2_kept, btc_alignment_map, cfg)
    if isinstance(btc_res, tuple) and len(btc_res) == 2:
        stage2b_kept, stage2b_rejected_list = btc_res
        btc_diag = {
            "enabled": bool(
                cfg.get("thresholds", {}).get("btc_alignment", {}).get("enforce", False)
            ),
            "all": len(stage2_kept),
            "kept": len(stage2b_kept),
            "rejected": len(stage2b_rejected_list),
        }
    else:
        stage2b_kept, stage2b_rejected_list, btc_diag = btc_res

    print("[decision_engine] btc_align_hard", btc_diag)

    if not stage2b_kept:
        rejected_all = []
        rejected_all.extend(stage2_rejected_list)
        rejected_all.extend(stage2b_rejected_list)

        max_R_bucket = float(cfg["risk"].get("max_R_per_bucket", 3.0))
        base_R = float(cfg["risk"].get("base_R_per_trade", 1.0))

        risk_summary = {
            "bias": bias_val,
            "panic_selloff": bool(market_ctx.get("panic_selloff", False)),
            "max_R_per_bucket": max_R_bucket,
            "base_R_per_trade": base_R,
            "total_R_planned": 0.0,
            "num_final": 0,
            "dd_kill_active": False,
            "btc_align_diag": btc_diag,
        }

        out = {
            "t_ref": t_ref,
            "approved": [],
            "rejected": rejected_all,
            "risk_summary": risk_summary,
            "breadth_info": breadth_info,
            "market_ctx": market_ctx,
            "evictions": [],
        }

        try:
            _write_decision_snapshot_batch_local(
                conn_local=conn,
                t_ref_local=t_ref,
                approved_local=[],
                rejected_local=rejected_all,
                cfg_local=cfg,
                risk_summary_local=risk_summary,
            )
        except Exception as e:
            print("[decision_engine] snapshot write failed(btc_align):", e)

        print(
            "[decision_engine] plan summary:",
            {
                "t_ref": t_ref,
                "approved_n": 0,
                "used_R": 0.0,
                "panic_selloff": risk_summary["panic_selloff"],
                "bias": bias_val,
                "btc_align_diag": btc_diag,
            },
        )
        return out

    # 3) 流动性门
    try:
        stage3_kept, stage3_rejected = _apply_liquidity_gate(
            conn=conn,
            cfg=cfg,
            t_ref=t_ref,
            candidates=stage2b_kept,
        )
    except Exception as e:
        print("[decision_engine] liquidity gate failed, skip:", e)
        stage3_kept = stage2b_kept
        stage3_rejected = []

    if not stage3_kept:
        rejected_all = []
        rejected_all.extend(stage2_rejected_list)
        rejected_all.extend(stage2b_rejected_list)
        rejected_all.extend(stage3_rejected)

        max_R_bucket = float(cfg["risk"].get("max_R_per_bucket", 3.0))
        base_R = float(cfg["risk"].get("base_R_per_trade", 1.0))

        risk_summary = {
            "bias": bias_val,
            "panic_selloff": bool(market_ctx.get("panic_selloff", False)),
            "max_R_per_bucket": max_R_bucket,
            "base_R_per_trade": base_R,
            "total_R_planned": 0.0,
            "num_final": 0,
            "dd_kill_active": False,
            "btc_align_diag": btc_diag,
            "liquidity_block_reason": "liquidity_reject",
        }

        out = {
            "t_ref": t_ref,
            "approved": [],
            "rejected": rejected_all,
            "risk_summary": risk_summary,
            "breadth_info": breadth_info,
            "market_ctx": market_ctx,
            "evictions": [],
        }

        try:
            _write_decision_snapshot_batch_local(
                conn_local=conn,
                t_ref_local=t_ref,
                approved_local=[],
                rejected_local=rejected_all,
                cfg_local=cfg,
                risk_summary_local=risk_summary,
            )
        except Exception as e:
            print("[decision_engine] snapshot write failed(liquidity_block):", e)

        print(
            "[decision_engine] plan summary:",
            {
                "t_ref": t_ref,
                "approved_n": 0,
                "used_R": 0.0,
                "panic_selloff": risk_summary["panic_selloff"],
                "bias": bias_val,
                "btc_align_diag": btc_diag,
                "liquidity_block_reason": "liquidity_reject",
            },
        )
        return out

    # 3.x) 到这一步才分 R
    ranked = _rank_candidates(stage3_kept)
    approved_stage3, riskrej_list, used_R, max_R_bucket, base_R = _allocate_risk_R(
        ranked,
        cfg,
        bias_val,
        btc_alignment_map,
    )

    # 4) struct -> risk_cost 门（risk_cost 改为 fail-closed）
    try:
        # 为缺 stop/take 的候选补齐结构位（摆动高低点），已有结构不动
        candidates_struct = _attach_struct_levels_by_swings(
            conn=conn,
            cfg=cfg,
            t_ref=t_ref,
            candidates=approved_stage3,
        )
    except Exception as e:
        print("[decision_engine] struct-level attach failed, fallback without attach:", e)
        candidates_struct = approved_stage3

    try:
        stage4_kept, stage4_rejected = _apply_risk_cost_gate(
            conn=conn,
            cfg=cfg,
            t_ref=t_ref,
            candidates=candidates_struct,
        )
    except Exception as e:
        print("[decision_engine] risk_cost gate crashed -> fail-closed:", e)
        stage4_kept = []
        stage4_rejected = [
            {
                "symbol": c.get("symbol", "?"),
                "timeframe": c.get("timeframe") or "1h",
                "direction": (c.get("direction") or "").lower().strip(),
                "reject_reason": "risk_cost_internal_error",
            }
            for c in (candidates_struct or [])
        ]

    # 合并拒单到现在
    rejected_all = []
    rejected_all.extend(stage2_rejected_list)
    rejected_all.extend(stage2b_rejected_list)
    rejected_all.extend(stage3_rejected)
    rejected_all.extend(riskrej_list)
    rejected_all.extend(stage4_rejected)

    # 如果 risk_cost 把所有都挡掉了
    if not stage4_kept:
        risk_summary = {
            "bias": bias_val,
            "panic_selloff": bool(market_ctx.get("panic_selloff", False)),
            "max_R_per_bucket": max_R_bucket,
            "base_R_per_trade": base_R,
            "total_R_planned": 0.0,
            "num_final": 0,
            "dd_kill_active": False,
            "btc_align_diag": btc_diag,
            "risk_cost_block_reason": "risk_cost_reject",
        }

        out = {
            "t_ref": t_ref,
            "approved": [],
            "rejected": rejected_all,
            "risk_summary": risk_summary,
            "breadth_info": breadth_info,
            "market_ctx": market_ctx,
            "evictions": [],
        }

        try:
            _write_decision_snapshot_batch_local(
                conn_local=conn,
                t_ref_local=t_ref,
                approved_local=[],
                rejected_local=rejected_all,
                cfg_local=cfg,
                risk_summary_local=risk_summary,
            )
        except Exception as e:
            print("[decision_engine] snapshot write failed(risk_cost_block):", e)

        print(
            "[decision_engine] plan summary:",
            {
                "t_ref": t_ref,
                "approved_n": 0,
                "used_R": 0.0,
                "panic_selloff": risk_summary["panic_selloff"],
                "bias": bias_val,
                "btc_align_diag": btc_diag,
                "risk_cost_block_reason": "risk_cost_reject",
            },
        )
        return out

    # 5) 相关性门
    approved_stage4, corr_rejected_list, corr_info = correlation_gate.run_correlation_gate(
        conn=conn,
        cfg=cfg,
        t_ref=t_ref,
        candidates_after_risk=stage4_kept,
    )
    rejected_all.extend(corr_rejected_list)

    corr_block_reason = corr_info.get("correlation_block_reason")

    # 5.5) 组合上限/淘汰计划（仅规划，不执行）
    evictions_list = []
    try:
        ev_res = _plan_evictions(conn, cfg, t_ref, approved_stage4)
        # 兼容返回 2 值/3 值
        if isinstance(ev_res, tuple):
            if len(ev_res) == 3:
                approved_stage4, evictions_list, _ = ev_res
            elif len(ev_res) == 2:
                approved_stage4, maybe_plan = ev_res
                if isinstance(maybe_plan, dict):
                    evictions_list = list(maybe_plan.get("pairs") or [])
                elif isinstance(maybe_plan, list):
                    evictions_list = maybe_plan
    except Exception as e:
        print("[decision_engine] eviction planning failed:", e)
        evictions_list = []

    # 6) 最终补字段
    strat_ver = str(cfg.get("strategy_version", "dev"))
    for a in approved_stage4:
        a["strategy_version"] = strat_ver
        a["t_ref"] = t_ref

    # 7) 汇总
    panic_flag = bool(market_ctx.get("panic_selloff", False))
    effective_used_R = used_R if len(approved_stage4) > 0 else 0.0
    risk_summary = {
        "bias": bias_val,
        "panic_selloff": panic_flag,
        "max_R_per_bucket": max_R_bucket,
        "base_R_per_trade": base_R,
        "total_R_planned": effective_used_R,
        "num_final": len(approved_stage4),
        "dd_kill_active": False,
        "btc_align_diag": btc_diag,
    }
    if corr_block_reason:
        risk_summary["correlation_block_reason"] = corr_block_reason
    risk_summary["corr_est"] = corr_info.get("corr_est")
    risk_summary["corr_action"] = corr_info.get("corr_action")
    risk_summary["corr_scale"] = corr_info.get("corr_scale")

    out = {
        "t_ref": t_ref,
        "approved": approved_stage4,
        "rejected": rejected_all,
        "risk_summary": risk_summary,
        "breadth_info": breadth_info,
        "market_ctx": market_ctx,
        "evictions": evictions_list,
    }

    # 8) snapshot
    try:
        _write_decision_snapshot_batch_local(
            conn_local=conn,
            t_ref_local=t_ref,
            approved_local=approved_stage4,
            rejected_local=rejected_all,
            cfg_local=cfg,
            risk_summary_local=risk_summary,
        )
    except Exception as e:
        print("[decision_engine] snapshot write failed(main):", e)

    # 9) debug
    dbg = {
        "t_ref": t_ref,
        "approved_n": len(approved_stage4),
        "used_R": effective_used_R,
        "panic_selloff": panic_flag,
        "bias": bias_val,
        "btc_align_diag": btc_diag,
    }
    if corr_block_reason:
        dbg["correlation_block_reason"] = corr_block_reason
    print("[decision_engine] plan summary:", dbg)

    return out
