# -*- coding: utf-8 -*-
"""
run_exit_cycle.py

平仓/止损/止盈循环的入口脚本。

流程：
1. 读取配置 (load_cfg)
2. 打开影子账户数据库 (db_conn)
3. exit_daemon.run_exit_cycle(conn, cfg)
   - 扫描 positions_virtual 里 status='OPEN'/'ACTIVE' 的仓位
   - 用 exit_logic / 市场数据 / dd_kill 等规则判断哪些要平
   - 回写这些仓位：status='CLOSED', closed_at, exit_reason, exit_price, pnl_usd
4. 打印本轮平掉了哪些仓位

重要：
- 这个脚本只是在影子账本 (trading_signals_core.db) 上操作。
- 不会下真单，不会碰线上老库。
- db_conn() 会自动 commit / rollback。
"""

import sys, traceback
from typing import Dict, Any
from core import db_access
from core.config_loader import load_cfg, get_db_path
from core import exit_daemon

try:
    from notification import simple_notify
except Exception:
    simple_notify = None

def _send_exit_notifications(conn, cfg, window_minutes: int = 10):
    """
    读取最近 window_minutes 分钟内 exit_log 的真实出场事件，
    用 reason_text / reasons_obj 渲染后推送；附带当下收益率（%）。
    无事件返回 False。
    """
    import json

    def _pl_pct(entry_px, cur_px, direction, pnl_usd=None, notional_usd=None):
        try:
            if entry_px and cur_px and (entry_px > 0):
                if str(direction).lower() == "short":
                    return (entry_px / cur_px - 1.0) * 100.0
                return (cur_px / entry_px - 1.0) * 100.0
        except Exception:
            pass
        try:
            if pnl_usd is not None and notional_usd:
                return float(pnl_usd) / float(notional_usd) * 100.0
        except Exception:
            pass
        return None

    cur = conn.execute(
        """
        SELECT
          e.id, e.t_ref, e.position_id, e.action, e.reason_text, e.reasons_obj,
          e.price, e.qty, e.pnl_usd,
          p.symbol, p.timeframe, p.direction, p.entry_price, p.notional_usd
        FROM exit_log e
        JOIN positions_virtual p ON p.id = e.position_id
        WHERE e.t_ref >= datetime('now','localtime', ?)
        ORDER BY e.id DESC
        """,
        (f"-{int(window_minutes)} minutes",)
    )
    rows = cur.fetchall()
    if not rows:
        return False

    lines = []
    for r in rows:
        (_id, t_ref, pid, action, reason_text, reasons_obj,
         price, qty, pnl_usd, symbol, tf, direction, entry_price, notional_usd) = r

        label_cn = None; share_pct = None
        try:
            if reasons_obj:
                obj = json.loads(reasons_obj)
                label_cn = obj.get("label_cn")
                share_pct = obj.get("share_pct")
        except Exception:
            pass

        plp = _pl_pct(entry_price, price, direction, pnl_usd, notional_usd)
        pl_str = (f"  P/L:{plp:+.2f}%" if isinstance(plp, (int,float)) else "")

        if reason_text and "】" in reason_text:
            msg = f"{t_ref} | {symbol} {tf}\n{reason_text}\n@{float(price):g} × {float(qty):g}{pl_str}"
        else:
            tag = label_cn or action
            pct = f"{share_pct:.0f}%" if isinstance(share_pct, (int, float)) else ""
            msg = f"{t_ref} | 【{tag} {pct}】{symbol} {tf} @{float(price):g} × {float(qty):g}{pl_str}"
        lines.append(msg)

    if not lines:
        return False

    title = "📤 出场事件"
    body  = "\n\n".join(lines)
    try:
        _notify_to_all(cfg, title, body)
    except Exception as e:
        print(f"[notify][exit] error: {e}")
        return False
    return True



def _notify_to_all(cfg: dict, title: str, body: str) -> None:
    """
    统一的通知出口：优先走 notification.simple_notify；失败则打印兜底。
    """
    try:
        # 优先使用已导入的 simple_notify
        from notification import simple_notify as _n  # 若上面已导入，这里也能命中
        _n(f"{title}\n\n{body}")
        return
    except Exception:
        pass
    try:
        # 其他可能存在的后端（可选）
        from notification import notify as _n
        _n(f"{title}\n\n{body}")
        return
    except Exception:
        pass
    # 最后兜底：stdout
    print(f"[notify]\n{title}\n\n{body}")

def _emit_notifications_for_exit_report(conn, report: dict) -> None:
    """
    发送出场通知（事件卡片 + 汇总 + 逐笔）
    关键修复：优先按 created_at 最近窗口取 exit_log（避免 t_ref 与桶时间错位导致捞空）
    并在所有卡片中追加 P/L 百分比。
    """
    import logging, json
    from datetime import datetime

    # 选择通知后端（有就用；都没有就 print 兜底）
    notifier = None
    backend_name = ""
    try:
        from notification import simple_notify as _fn  # type: ignore
        notifier = _fn; backend_name = "notification.simple_notify"
    except Exception:
        pass
    if notifier is None:
        try:
            from notification import notify as _fn  # type: ignore
            notifier = _fn; backend_name = "notification.notify"
        except Exception:
            pass
    if notifier is None:
        try:
            from notification import push as _fn  # type: ignore
            notifier = _fn; backend_name = "notification.push"
        except Exception:
            pass
    if notifier is None:
        try:
            import notification as _m  # type: ignore
            notifier = getattr(_m, "simple_notify", None) or getattr(_m, "notify", None) or getattr(_m, "push", None)
            if notifier:
                backend_name = f"notification.{getattr(notifier, '__name__', 'callable')}"
        except Exception:
            pass
    try:
        logging.info(f"[notify] backend={backend_name or 'None'}")
    except Exception:
        pass

    def _sn(text: str) -> None:
        if notifier:
            try:
                notifier(text); return
            except Exception as e:
                try: logging.error("notify failed: %s", e)
                except Exception: pass
        print(f"[notify] {text}")

    def _q(sql: str, args: tuple = ()) -> list[dict]:
        try:
            cur = conn.execute(sql, args)
            rows = cur.fetchall() or []
            return [dict(r) for r in rows]
        except Exception as e:
            try: logging.error("[notify][_q] %s", e)
            except Exception: pass
            return []

    def _label_cn_from_action(action: str) -> str:
        m = {
            "TP": "止盈-分批",
            "TSL": "止损-移动止损",
            "STRUCT": "止损-结构破位",
            "CTX": "风控-冲突减仓",
            "TIME": "风控-时间出场",
            "EVICT": "风控-组合腾挪",
        }
        return m.get((action or "").upper(), action or "?")

    def _pl_pct(entry_px, cur_px, direction, pnl_usd=None, notional_usd=None):
        try:
            if entry_px and cur_px and (entry_px > 0):
                if str(direction).lower() == "short":
                    return (entry_px / cur_px - 1.0) * 100.0
                return (cur_px / entry_px - 1.0) * 100.0
        except Exception:
            pass
        try:
            if pnl_usd is not None and notional_usd:
                return float(pnl_usd) / float(notional_usd) * 100.0
        except Exception:
            pass
        return None

    t_ref = report.get("t_ref") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dd_kill = bool(report.get("dd_kill", False))
    equity_snapshot = report.get("equity_snapshot") or {}

    # 1) 熔断
    if dd_kill:
        parts = [
            "🛑 触发账户熔断 (dd_kill)",
            f"时间: {t_ref}",
            f"权益: {equity_snapshot.get('equity_usd')}  峰值: {equity_snapshot.get('peak_equity_usd')}",
            f"回撤: {equity_snapshot.get('drawdown_pct')}",
        ]
        _sn("\n".join(parts))

    # 2) 事件卡片：优先按 created_at 最近10分钟；若捞空，再按 t_ref 兜底
    evt_rows = _q(
        """
        SELECT
            e.id, e.t_ref, e.created_at, e.position_id, e.action, e.reason_text, e.reasons_obj,
            e.price, e.qty, e.pnl_usd,
            p.symbol, p.timeframe, p.direction, p.entry_price, p.notional_usd
        FROM exit_log e
        JOIN positions_virtual p ON p.id = e.position_id
        WHERE e.created_at >= datetime('now','localtime','-10 minutes')
        ORDER BY e.id ASC;
        """
    )
    if not evt_rows:
        evt_rows = _q(
            """
            SELECT
                e.id, e.t_ref, e.created_at, e.position_id, e.action, e.reason_text, e.reasons_obj,
                e.price, e.qty, e.pnl_usd,
                p.symbol, p.timeframe, p.direction, p.entry_price, p.notional_usd
            FROM exit_log e
            JOIN positions_virtual p ON p.id = e.position_id
            WHERE e.t_ref >= datetime('now','localtime','-10 minutes')
            ORDER BY e.id ASC;
            """
        )

    for r in (evt_rows or []):
        action     = (r.get("action") or "").upper()
        symbol     = r.get("symbol")
        timeframe  = r.get("timeframe")
        price      = float(r.get("price") or 0.0)
        share_pct  = None; label_cn = None; extra_tail = ""
        reason_text = r.get("reason_text") or ""
        try:
            robj = json.loads(r.get("reasons_obj") or "{}")
            label_cn  = robj.get("label_cn")
            try: share_pct = float(robj.get("share_pct")) if robj.get("share_pct") is not None else None
            except Exception: share_pct = None
            if action == "TP":
                tp_stage = robj.get("tp_stage")
                R_now    = robj.get("R_now")
                target_R = robj.get("target_R")
                if tp_stage is not None and R_now is not None and target_R is not None:
                    extra_tail = f"  TP{int(tp_stage)} R={float(R_now):.2f}/{float(target_R):.2f}"
            elif action == "CTX":
                mode   = robj.get("mode")
                streak = robj.get("streak")
                if mode and streak is not None:
                    extra_tail = f"  {mode} streak={int(streak)}"
            elif action == "TIME":
                bars = robj.get("bars_since_entry")
                mfe  = robj.get("mfe_R")
                if bars is not None and mfe is not None:
                    extra_tail = f"  bars={int(bars)} MFE={float(mfe):.2f}"
        except Exception:
            pass

        if not label_cn:
            label_cn = _label_cn_from_action(action)

        # 收益率（按事件时刻价格）
        plp = _pl_pct(r.get("entry_price"), r.get("price"), r.get("direction"), r.get("pnl_usd"), r.get("notional_usd"))
        pl_part = (f"  P/L:{plp:+.2f}%" if isinstance(plp, (int,float)) else "")

        if reason_text and reason_text.startswith("【"):
            card = f"{reason_text}  {symbol} {timeframe} @{price:.6f}{pl_part}"
        elif reason_text:
            pct_part = (f" {share_pct:.0f}%" if share_pct is not None else "")
            card = f"【{label_cn}{pct_part}】{symbol} {timeframe} @{price:.6f}{pl_part}  {reason_text}"
        else:
            pct_part = (f" {share_pct:.0f}%" if share_pct is not None else "")
            head = f"【{label_cn}{pct_part}】"
            card = f"{head}{symbol} {timeframe} @{price:.6f}{pl_part}{extra_tail}"

        _sn(card)

    # 3) 平仓汇总
    closed_positions = list(report.get("closed") or [])
    if closed_positions:
        lines = []
        for i, p in enumerate(closed_positions, 1):
            try:
                ep  = float(p.get("entry_price") or 0.0)
                xp  = float(p.get("exit_price") or 0.0)
                pnl = float(p.get("pnl_usd") or 0.0)
                side= (p.get("side") or p.get("direction") or "").upper()
                # 尝试用价格计算收益率，失败则回落到 pnl/notional
                plp = None
                try:
                    if ep > 0 and xp > 0:
                        plp = (ep/xp - 1.0)*100.0 if side == "SHORT" else (xp/ep - 1.0)*100.0
                except Exception:
                    pass
                if plp is None:
                    try:
                        notional = float(p.get("notional_usd") or 0.0)
                        if notional > 0:
                            plp = pnl / notional * 100.0
                    except Exception:
                        plp = None

                pct_str = f"  P/L:{plp:+.2f}%" if isinstance(plp, (int,float)) else ""
                lines.append(f"{i}. {p.get('symbol')} {side:<5} 原因:{p.get('exit_reason') or '-'}  入:{ep:.6f} 出:{xp:.6f}  PnL:{pnl:.2f} USD{pct_str}")
            except Exception:
                lines.append(f"{i}. {p.get('symbol')} 原因:{p.get('exit_reason') or '-'}")
        _sn("\n".join(["📦 平仓汇总", f"时间: {t_ref}", f"数量: {len(closed_positions)}", "——", "\n".join(lines)]))

    # 4) 逐笔详情（链路）
    def _fetch_exit_chain(position_id: int) -> dict:
        logs = _q("""
            SELECT action, reason_text, reasons_obj, price, created_at
            FROM exit_log
            WHERE position_id=?
            ORDER BY created_at ASC;
        """, (position_id,))
        chain_parts, tp_parts = [], []
        final_ctx_or_struct = None
        import json as _json
        for r in (logs or []):
            act = (r.get("action") or "").upper()
            if act == "TP":
                px = r.get("price"); robj = _json.loads(r.get("reasons_obj") or "{}")
                stg = robj.get("tp_stage"); shr = robj.get("share_pct")
                if stg is not None and shr is not None:
                    tp_parts.append(f"TP{int(stg)}({float(shr):.0f}%)@{px}")
                else:
                    tp_parts.append(f"TP@{px}")
                chain_parts.append(f"TP{int(stg) if stg is not None else ''}".strip())
            elif act in ("TSL", "TIME"):
                px = r.get("price"); chain_parts.append(f"{act}@{px}")
            elif act in ("CTX", "STRUCT", "EVICT"):
                final_ctx_or_struct = r.get("reason_text") or ""
                chain_parts.append(act)
            else:
                chain_parts.append(act or "?")
        return {
            "chain": "→".join([c for c in chain_parts if c]) if chain_parts else None,
            "tp_detail": ", ".join(tp_parts) if tp_parts else None,
            "final_reason_text": final_ctx_or_struct,
        }

    for p in (closed_positions or []):
        try:
            detail = _fetch_exit_chain(int(p.get("id") or p.get("rowid")))
            ep  = float(p.get("entry_price") or 0.0)
            xp  = float(p.get("exit_price") or 0.0)
            pnl = float(p.get("pnl_usd") or 0.0)
            side= (p.get("side") or p.get("direction") or "").upper()

            # 百分比
            plp = None
            try:
                if ep > 0 and xp > 0:
                    plp = (ep/xp - 1.0)*100.0 if side == "SHORT" else (xp/ep - 1.0)*100.0
            except Exception:
                pass
            if plp is None:
                try:
                    notional = float(p.get("notional_usd") or 0.0)
                    if notional > 0:
                        plp = pnl / notional * 100.0
                except Exception:
                    plp = None
            pctline = f"收益率: {plp:+.2f}%" if isinstance(plp, (int,float)) else None

            parts = [
                "✅ 平仓执行",
                f"时间: {t_ref}",
                f"ID: {p.get('id') or p.get('rowid')}",
                f"交易对: {p.get('symbol')}  方向: {side}",
                f"入场: {ep:.6f}  出场: {xp:.6f}",
                f"最终原因: {p.get('exit_reason') or '-'}",
            ]
            extra = []
            if detail.get("chain"):       extra.append(f"触发链: {detail['chain']}")
            if detail.get("tp_detail"):   extra.append(f"TP明细: {detail['tp_detail']}")
            if detail.get("final_reason_text"): extra.append(f"结构/上下文: {detail['final_reason_text']}")
            if pctline: extra.append(pctline)
            if extra: parts.extend(extra)
            parts.append(f"实盈亏(USD): {pnl:.2f}")
            _sn("\n".join(parts))
        except Exception as e:
            try: logging.error("逐笔平仓通知发送失败: %s", e)
            except Exception: pass




def main() -> None:
    import time
    from typing import Dict, Any
    from datetime import datetime

    # 这些 import 都放在函数内，避免模块间循环
    try:
        from core.config_loader import load_cfg, get_db_path
    except Exception:
        # 兼容你旧版的 load_cfg/get_db_path 导入路径
        from config_loader import load_cfg, get_db_path

    try:
        from core import db_access, exit_daemon, ops_heartbeat, risk_monitor
    except Exception:
        # 兼容旧工程结构
        import db_access, exit_daemon, ops_heartbeat, risk_monitor

    # ---- 本地时间戳工具（不依赖全局工具，防止 NameError）----
    def _now_local_str(_cfg: Dict[str, Any]) -> str:
        tz_str = (_cfg.get("core") or {}).get("tz", "Asia/Shanghai")
        try:
            import pytz
            tz = pytz.timezone(tz_str)
            return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cfg: Dict[str, Any] = load_cfg()
    db_path: str = get_db_path(cfg)

    with db_access.db_conn(db_path) as conn:
        t0 = time.time()

        # 1) 执行平仓/风控主流程
        report: Dict[str, Any] = exit_daemon.run_exit_cycle(conn=conn, cfg=cfg)
        print("[exit_cycle] report:")
        print(report)

        # 2) 推送通知（优先使用“真实出场事件卡片”；没有则退回原汇总）
        sent = False
        try:
            # 你若已按我给的实现添加了 _send_exit_notifications，则会生效；
            # 没有该函数就会触发 NameError 被捕获并回退。
            sent = bool(_send_exit_notifications(conn, cfg, window_minutes=10))
        except NameError:
            pass
        except Exception as e:
            print("[notify][exit-events][WARN]", e)

        if not sent:
            try:
                _emit_notifications_for_exit_report(conn, report)
            except NameError:
                # 没有旧的汇总推送也没关系
                pass
            except Exception as e:
                print("[notify][summary][WARN]", e)

        # 3) 写心跳（含本轮耗时/关闭数量/熔断标记）
        t1 = time.time()
        try:
            # t_ref：优先用 report 自带；没有就用本地时间
            t_ref = str(report.get("t_ref") or _now_local_str(cfg))
            closed_n = len(report.get("closed") or [])
            dd_kill_flag = False
            try:
                dd_kill_flag = bool(risk_monitor.get_dd_kill_flag(conn))
            except Exception:
                # 旧版 risk_monitor 可能没有该函数
                dd_kill_flag = False

            # payload 里放一点轻量信息，避免把整个 report 塞进去
            payload = {"kept_n": len(report.get("kept") or []),
                       "errors_n": len(report.get("errors") or [])}

            ops_heartbeat.write(
                conn,
                job="exit_cycle",
                t_ref=t_ref,
                started_at=t0,
                finished_at=t1,
                status="ok",
                closed_n=closed_n,
                dd_kill_flag=dd_kill_flag,
                payload=payload,
                cfg=cfg,
            )
        except Exception as e:
            print("[exit_cycle][heartbeat][WARN]", e)


if __name__ == "__main__":
    main()


