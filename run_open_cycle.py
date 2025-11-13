# -*- coding: utf-8 -*-

import sys
import traceback
from typing import Dict, Any

from core.config_loader import load_cfg, get_db_path
from core import pipeline_runner, decision_engine, execution, risk_monitor, db_access
# --- notify (safe import) ---
try:
    from notification import simple_notify
except Exception:
    simple_notify = None  # 不阻断流程
from core.notification_logger import log_notification
def _execute_evictions(conn, cfg: dict, t_ref: str, evictions: list) -> dict:
    """
    执行“淘汰腾挪”（Evictions）：
      1) 为每个需淘汰的持仓写入 exit_log(action='EVICT') 事件（带上 reason/reasons_obj）
      2) 按当前价（优先取 ohlcv<=t_ref 的 close）全量平掉该持仓（positions_virtual → CLOSED/qty=0 等）
      3) 返回执行结果汇总，供上层日志/通知使用

    兼容性：自动探测 exit_log / positions_virtual / ohlcv 的列，尽量动态落库；拿不到 close 就用 entry_price 兜底。
    适配：支持两种输入形态
      a) 扁平：{"position_id"/"id", "symbol", "timeframe", "reason"...}
      b) pairs：{"candidate": {...}, "victim": {"id","symbol","timeframe",...}, "reason": "..."}
    """
    import json
    from datetime import datetime

    def _now_str():
        tz = (cfg.get("tz") or cfg.get("core", {}).get("tz") or "Asia/Shanghai")
        try:
            import pytz
            return datetime.now(pytz.timezone(tz)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _cols(table):
        try:
            cur = conn.execute(f"PRAGMA table_info({table});")
            return {r[1] for r in cur.fetchall()}
        except Exception:
            return set()

    def _first_col(cols: set, cands: list, default=None):
        for c in cands:
            if c in cols:
                return c
        return default

    def _fetch_pos(pid: int):
        sql = "SELECT * FROM positions_virtual WHERE id=?"
        try:
            row = conn.execute(sql, (pid,)).fetchone()
            return dict(row) if row else None
        except Exception:
            return None

    def _last_close(sym: str, tf: str, t_ref_local: str):
        cols = _cols("ohlcv")
        if not cols:
            return None
        t_col = _first_col(cols, ["t_ref", "t", "ts"])
        if not t_col:
            return None
        sym_col = _first_col(cols, ["symbol", "pair", "ticker"], "symbol")
        tf_col  = _first_col(cols, ["timeframe", "tf"], "timeframe")
        px_col  = _first_col(cols, ["close", "c"], "close")
        try:
            sql = f"""
                SELECT {px_col} AS close_px
                FROM ohlcv
                WHERE {sym_col}=? AND {tf_col}=? AND {t_col}<=?
                ORDER BY {t_col} DESC
                LIMIT 1
            """
            r = conn.execute(sql, (sym, tf, t_ref_local)).fetchone()
            return float(r[0]) if r and r[0] is not None else None
        except Exception:
            return None

    def _insert_exit_log(pid: int, sym: str, tf: str, px: float, qty: float, reason_text: str, reasons_obj: dict):
        cols = _cols("exit_log")
        if not cols:
            return False
        base = {
            "t_ref": t_ref,
            "position_id": pid,
            "action": "EVICT",
            "reason_text": reason_text or "evict",
            "reasons_obj": json.dumps(reasons_obj, ensure_ascii=False) if reasons_obj is not None else None,
            "price": px,
            "qty": qty,
            "pnl_usd": None,
            "created_at": _now_str(),
        }
        fields = [c for c in ["t_ref","position_id","action","reason_text","reasons_obj","price","qty","pnl_usd","created_at"] if c in cols]
        sql = f"INSERT INTO exit_log({','.join(fields)}) VALUES ({','.join(['?']*len(fields))});"
        vals = [base.get(k) for k in fields]
        try:
            conn.execute(sql, vals)
            return True
        except Exception as e:
            print(f"[evict] exit_log insert failed pid={pid}: {e}")
            return False

    def _compute_pnl_usd(pos: dict, exit_px: float):
        try:
            entry_px = float(pos.get("entry_price"))
            qty      = float(pos.get("qty"))
            side     = (pos.get("direction") or pos.get("side") or "").lower()
            if side == "long":
                pnl = (exit_px - entry_px) * qty
            elif side == "short":
                pnl = (entry_px - exit_px) * qty
            else:
                return None
            return float(pnl)
        except Exception:
            return None

    def _close_position(pid: int, pos: dict, exit_px: float, reason_text: str):
        cols = _cols("positions_virtual")
        if not cols:
            return False
        updates = {}
        if "status" in cols:       updates["status"] = "CLOSED"
        if "qty" in cols:          updates["qty"] = 0.0
        if "exit_reason" in cols:  updates["exit_reason"] = reason_text or "evict"
        if "exit_price" in cols and exit_px is not None: updates["exit_price"] = float(exit_px)
        if "updated_at" in cols:   updates["updated_at"] = _now_str()
        if "closed_at" in cols:    updates["closed_at"]  = t_ref

        if not updates:
            return False

        sets = ", ".join([f"{k}=?" for k in updates.keys()])
        vals = list(updates.values()) + [pid]
        sql  = f"UPDATE positions_virtual SET {sets} WHERE id=?"
        try:
            conn.execute(sql, vals)
            return True
        except Exception as e:
            print(f"[evict] close position failed pid={pid}: {e}")
            return False

    # === 新增：归一化，兼容 pairs / 扁平 ===
    def _normalize(item: dict) -> dict:
        if not isinstance(item, dict):
            return {}
        if "victim" in item and isinstance(item["victim"], dict):
            v = item["victim"]
            out = {
                "position_id": v.get("id") or v.get("position_id"),
                "symbol": v.get("symbol"),
                "timeframe": v.get("timeframe") or v.get("tf") or "1h",
                "reason": item.get("reason") or item.get("reason_text") or "evict_by_policy",
                "rule": item.get("rule"),
                "rank": item.get("rank"),
                "metric": item.get("metric"),
                "metric_value": item.get("metric_value"),
                "bars_held": item.get("bars_held"),
            }
            return out
        # 扁平就原样返回
        return dict(item)

    results = {"ok": [], "skip": [], "error": []}

    for raw in (evictions or []):
        item = _normalize(raw)

        try:
            pid    = int(item.get("position_id") or item.get("id"))
            reason = item.get("reason") or item.get("reason_text") or "evict_by_policy"
            sym    = item.get("symbol")
            tf     = item.get("timeframe") or item.get("tf") or "1h"
        except Exception:
            results["error"].append({"item": raw, "error": "invalid item"})
            continue

        pos = _fetch_pos(pid)
        if not pos:
            results["skip"].append({"pid": pid, "reason": "position_not_found"})
            continue
        if str(pos.get("status") or "OPEN").upper() != "OPEN":
            results["skip"].append({"pid": pid, "reason": f"status_is_{pos.get('status')}"})
            continue

        # 价格与数量
        exit_px = _last_close(sym or pos.get("symbol"), tf or pos.get("timeframe"), t_ref) or pos.get("entry_price")
        try: exit_px = float(exit_px) if exit_px is not None else None
        except Exception: exit_px = None
        try: qty = float(pos.get("qty") or 0.0)
        except Exception: qty = 0.0

        # exit_log
        robj = {
            "label_cn": "风控-组合腾挪",
            "mode": "evict",
            "evict_rule": item.get("rule") or None,
            "rank": item.get("rank") or None,
            "metric": item.get("metric") or None,
            "metric_value": item.get("metric_value") or None,
            "bars_held": item.get("bars_held") or None,
        }
        ok1 = _insert_exit_log(pid, sym or pos.get("symbol"), tf, exit_px, qty, reason, robj)

        # 计算 pnl_usd（若可计算则回填）
        pnl = _compute_pnl_usd(pos, exit_px) if exit_px is not None else None
        if pnl is not None:
            try:
                conn.execute(
                    "UPDATE exit_log SET pnl_usd=? WHERE position_id=? AND action='EVICT' AND t_ref=?;",
                    (float(pnl), pid, t_ref),
                )
            except Exception:
                pass

        # 关闭持仓
        ok2 = _close_position(pid, pos, exit_px, reason)

        if ok1 and ok2:
            results["ok"].append({"pid": pid, "symbol": sym or pos.get("symbol"), "tf": tf, "price": exit_px, "qty": qty})
        elif ok1 and not ok2:
            results["error"].append({"pid": pid, "error": "exit_log_ok_but_close_failed"})
        elif not ok1 and ok2:
            results["error"].append({"pid": pid, "error": "close_ok_but_exit_log_failed"})
        else:
            results["error"].append({"pid": pid, "error": "both_failed"})

    try:
        conn.commit()
    except Exception:
        pass

    return results


def main() -> None:
    import time
    from core import ops_heartbeat
    from core.config_loader import load_cfg, get_db_path
    from core import db_access, pipeline_runner, decision_engine, execution, risk_monitor

    cfg = load_cfg()
    db_path = get_db_path(cfg)
    strategy_version = str(cfg.get("strategy_version", "unknown"))

    # 一次最多处理几个桶（可在 config.yml 配置）
    max_buckets = int((cfg.get("pipeline") or {}).get("max_buckets_per_run", 6))

    all_results = []

    with db_access.db_conn(db_path) as conn:
        # ——关键：批量取桶（从最旧开始），若无该API就回退到 iter_buckets 限流——
        try:
            states = pipeline_runner.run_pipeline_batch(conn=conn, cfg=cfg, max_buckets_per_run=max_buckets)
        except AttributeError:
            states = []
            for _state in pipeline_runner.iter_buckets(conn=conn, cfg=cfg):
                states.append(_state)
                if len(states) >= max_buckets:
                    break

        for pipe_state in states:
            t0 = time.time()
            t_ref = str(pipe_state.get("t_ref"))

            # 1) 生成计划（逐桶）
            plan = decision_engine.build_trade_plan(conn, cfg, pipe_state)
            _execute_evictions(conn, cfg, plan["t_ref"], plan.get("evictions", []))
            # 2) 风控快照 / 熔断
            rm = risk_monitor.check_drawdown_and_update_snapshot(conn, cfg, t_ref=t_ref)
            dd_kill = bool(rm.get("dd_kill", False))

            risk_summary = dict(plan.get("risk_summary", {}))
            risk_summary["dd_kill_active"] = dd_kill

            # 3) 执行（逐桶）
            if dd_kill:
                open_res = {"opened": [], "skipped": [{"symbol": "(all)", "reason": "dd_kill_active"}]}
            else:
                open_res = execution.open_positions_for_plan(conn=conn, cfg=cfg, trade_plan=plan)

            result = {
                "t_ref": plan.get("t_ref"),
                "timeframe": pipe_state.get("timeframe"),
                "opened": open_res.get("opened"),
                "skipped": open_res.get("skipped"),
                "risk_summary": risk_summary,
                "strategy_version": strategy_version,
            }
            print("[open_cycle] bucket result:", result)
            all_results.append(result)

            # 4) 心跳（逐桶）
            t1 = time.time()
            try:
                opened_n = len(result.get("opened") or [])
                skipped_n = len(result.get("skipped") or [])
                used_R = (
                    result.get("risk_summary", {}).get("used_R")
                    or result.get("risk_summary", {}).get("total_R_planned")
                    or 0.0
                )
                if opened_n == 0:
                    used_R = 0.0

                ops_heartbeat.write(
                    conn=conn,
                    job="open_cycle",
                    t_ref=str(result.get("t_ref") or ""),
                    started_at=t0,
                    finished_at=t1,
                    cfg=cfg,
                    status="ok",
                    opened_n=opened_n,
                    skipped_n=skipped_n,
                    used_R=float(used_R),
                    dd_kill_flag=bool(dd_kill),
                    payload={"timeframe": result.get("timeframe")},
                )
            except Exception as e:
                print("[open_cycle][heartbeat][WARN]", e)

    print("[open_cycle] all bucket results:", all_results)



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[open_cycle] FATAL exception:", e, file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
