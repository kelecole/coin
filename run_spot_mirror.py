
# -*- coding: utf-8 -*-
"""
run_spot_mirror.py  (with console logs)

镜像型提醒脚本（只转述“已发生”的开/平仓事实，不加任何新逻辑）。
本版增加了**可读日志**与 --verbose 开关：
- 总是打印：DB 路径、时间窗口、抓取数量、合并/去重统计、通知后端、完成状态；
- --verbose：打印每类事件样本（前3条）与关键中间步骤。
"""

from __future__ import annotations
import sys, os, sqlite3, json, traceback, argparse, re
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, timedelta

# ---- 可选依赖（优先使用你的现有模块） ----
try:
    from core.config_loader import load_cfg, get_db_path  # type: ignore
except Exception:
    load_cfg = None
    get_db_path = None

try:
    from core import db_access  # type: ignore
except Exception:
    db_access = None

try:
    from core import ops_heartbeat  # type: ignore
except Exception:
    ops_heartbeat = None

# ---- YAML 兜底 ----
try:
    import yaml  # type: ignore
except Exception:
    yaml = None


DEFAULT_DB_PATH = "/www/wwwroot/Crypto-Signal/app/trading_signals_core.db"
VERBOSE = False  # 由 CLI --verbose 控制


def _log(msg: str) -> None:
    try:
        print(msg)
    except Exception:
        pass


def _dbg(msg: str) -> None:
    if VERBOSE:
        _log("[dbg] " + msg)


# ------------------------------
# 通知后端探测
# ------------------------------
def _resolve_notifier():
    """
    返回 (notifier_callable, backend_name)
    """
    notifier = None
    backend_name = ""

    # 1) 显式 from ... import ...
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

    # 2) import 模块 + 属性探测（兜底）
    if notifier is None:
        try:
            import notification as _m  # type: ignore
            notifier = getattr(_m, "simple_notify", None) or getattr(_m, "notify", None) or getattr(_m, "push", None)
            if notifier:
                backend_name = f"notification.{getattr(notifier, '__name__', 'callable')}"
        except Exception:
            pass
    if notifier is None:
        try:
            import simple_notify as _m  # type: ignore
            notifier = getattr(_m, "notify", None) or getattr(_m, "simple_notify", None) or getattr(_m, "push", None)
            if notifier:
                backend_name = f"simple_notify.{getattr(notifier, '__name__', 'callable')}"
        except Exception:
            pass
    return notifier, backend_name or "None"


def _notify(text: str) -> None:
    notifier, backend = _resolve_notifier()
    _dbg(f"[notify_backend]={backend}")
    if notifier:
        try:
            notifier(text); return
        except Exception as e:
            _log(f"[spot_mirror][notify][WARN] {e}")
    _log("[notify] " + text)  # 兜底：stdout


# ------------------------------
# DB 工具
# ------------------------------
def _connect_db_fallback(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _db_conn(db_path: str):
    """
    返回一个可用的上下文管理器。优先使用 core.db_access.db_conn。
    """
    if db_access and hasattr(db_access, "db_conn"):
        return db_access.db_conn(db_path)  # type: ignore
    # 兜底：简易 contextmanager
    class _Ctx:
        def __init__(self, path: str):
            self.path = path
            self.conn: Optional[sqlite3.Connection] = None
        def __enter__(self):
            self.conn = _connect_db_fallback(self.path)
            return self.conn
        def __exit__(self, exc_type, exc, tb):
            try:
                if exc is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
            finally:
                self.conn.close()
    return _Ctx(db_path)


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS spot_mirror_snapshot(
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      t_ref       TEXT NOT NULL,
      event_key   TEXT NOT NULL UNIQUE,   -- 'order:<coid>' | 'exit:<exit_id>' | fallback key
      event_type  TEXT NOT NULL,          -- BUY | TRIM | SELL | INFO_SHORT
      position_id INTEGER,
      order_id    TEXT,
      symbol      TEXT,
      timeframe   TEXT,
      text        TEXT,
      created_at  TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    );
    """)


def _load_cursor(conn: sqlite3.Connection) -> Optional[str]:
    """
    从 ops_heartbeat(job='spot_mirror') 读取最近一次 finished_at 作为窗口起点。
    若表或字段不存在，返回 None。
    """
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ops_heartbeat';")
        if not cur.fetchone():
            _dbg("ops_heartbeat not found; first run assumed")
            return None
        cur = conn.execute("""
            SELECT finished_at FROM ops_heartbeat
            WHERE job='spot_mirror'
            ORDER BY id DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception as e:
        _dbg(f"load_cursor failed: {e}")
    return None


def _time_window(now: datetime, cursor_iso: Optional[str], grace_sec: int) -> Tuple[str, str]:
    """
    形成 (start_iso, end_iso] 扫描区间。若无 cursor，用 now-65min。
    """
    end = now + timedelta(seconds=max(0, int(grace_sec)))
    if cursor_iso:
        start = datetime.strptime(cursor_iso, "%Y-%m-%d %H:%M:%S")
    else:
        start = now - timedelta(minutes=65)
    if start >= end:
        start = end - timedelta(minutes=65)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------
# 读取事件
# ------------------------------
def _select_new_orders(conn: sqlite3.Connection, start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    q = """
    SELECT
      o.client_order_id AS order_id,
      o.decision_id,
      o.t_ref,
      o.symbol,
      o.timeframe,
      o.direction,
      o.price       AS entry_price,
      o.qty,
      o.notional_usd,
      o.status,
      o.created_at
    FROM orders o
    WHERE o.created_at > ? AND o.created_at <= ?
    ORDER BY o.created_at ASC, o.client_order_id ASC;
    """
    try:
        cur = conn.execute(q, (start_iso, end_iso))
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        _log(f"[spot_mirror][WARN] select orders failed: {e}")
        rows = []
    return rows


def _select_exit_events(conn: sqlite3.Connection, start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    q1 = """
    SELECT
      e.id          AS exit_id,
      e.position_id,
      e.t_ref,
      e.action,        -- 'TP','SL','TSL','CTX','TIME','STRUCT','MANUAL','CLOSE'
      e.qty,
      e.price,
      e.pnl_usd,
      e.reason_text,
      e.created_at
    FROM exit_log e
    WHERE e.created_at > ? AND e.created_at <= ?
    ORDER BY e.created_at ASC, e.id ASC;
    """
    try:
        for r in conn.execute(q1, (start_iso, end_iso)).fetchall():
            events.append(dict(r))
    except Exception as e:
        _log(f"[spot_mirror][WARN] select exit_log failed: {e}")

    q2 = """
    SELECT
      p.id          AS position_id,
      p.symbol,
      p.timeframe,
      p.direction,
      p.entry_price,
      p.exit_price,
      p.exit_reason,
      p.closed_at
    FROM positions_virtual p
    WHERE p.status='CLOSED' AND p.closed_at IS NOT NULL
      AND p.closed_at > ? AND p.closed_at <= ?
    ORDER BY p.closed_at ASC, p.id ASC;
    """
    try:
        for r in conn.execute(q2, (start_iso, end_iso)).fetchall():
            d = dict(r); d["closed_event"] = True; events.append(d)
    except Exception as e:
        _log(f"[spot_mirror][WARN] select positions_virtual(CLOSED) failed: {e}")

    return events


# ------------------------------
# 合并事件 => 标准事件
# ------------------------------
def _fetch_position_core(conn: sqlite3.Connection, position_id: int) -> Dict[str, Any]:
    q = """
    SELECT id, client_order_id, symbol, timeframe, direction, entry_price, status,
           exit_price, exit_reason, pnl_usd, opened_at, closed_at, t_ref
    FROM positions_virtual WHERE id=?;
    """
    r = conn.execute(q, (position_id,)).fetchone()
    return dict(r) if r else {}


def _fetch_decision_context(conn: sqlite3.Connection, decision_id: Optional[int]) -> Dict[str, Any]:
    if not decision_id:
        return {}
    q = """
    SELECT id, t_ref, symbol, timeframe, source, final, reasons_json, created_at
    FROM decision_snapshot WHERE id=?;
    """
    r = conn.execute(q, (int(decision_id),)).fetchone()
    return dict(r) if r else {}


def _fetch_trigger_chain(conn: sqlite3.Connection, position_id: int) -> Tuple[str, str]:
    q = """
    SELECT action, price, reason_text, created_at
    FROM exit_log
    WHERE position_id=?
    ORDER BY created_at ASC, id ASC;
    """
    parts_short: List[str] = []
    parts_long: List[str] = []
    for r in conn.execute(q, (position_id,)).fetchall():
        action = str(r["action"])
        price  = r["price"]
        reason = r["reason_text"] or ""
        if action == "TP":
            m = re.search(r"tp_stage\s*=\s*(\d+)", reason)
            stage = m.group(1) if m else "?"
            parts_short.append(f"TP{stage}")
            parts_long.append(f"TP{stage}@{price} | {reason}".strip())
        else:
            parts_short.append(action)
            txt = f"{action}@{price}" if price is not None else action
            if reason:
                txt += f" | {reason}"
            parts_long.append(txt)
    return "→".join(parts_short), " ; ".join(parts_long)


def _coalesce_events(conn: sqlite3.Connection,
                     orders: List[Dict[str, Any]],
                     exits:  List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # 订单 → BUY / INFO_SHORT
    for o in orders:
        ev_key = f"order:{o['order_id']}"
        ev_type = "BUY" if (o.get("direction") or "").lower() == "long" else "INFO_SHORT"
        ctx = _fetch_decision_context(conn, o.get("decision_id"))
        out.append({
            "event_key": ev_key,
            "event_type": ev_type,
            "symbol": o.get("symbol"),
            "timeframe": o.get("timeframe"),
            "direction": o.get("direction"),
            "order_id": o.get("order_id"),
            "position_id": None,
            "entry_price": o.get("entry_price"),
            "qty": o.get("qty"),
            "notional_usd": o.get("notional_usd"),
            "created_at": o.get("created_at"),
            "t_ref": o.get("t_ref"),
            "status": o.get("status"),
            "decision_ctx": ctx,
        })

    # 退出事件按 position 分组
    by_pos: Dict[int, Dict[str, Any]] = {}
    for e in exits:
        position_id = e.get("position_id")
        if not position_id:
            if e.get("closed_event"):
                position_id = e.get("position_id")
            else:
                continue
        position_id = int(position_id)
        arr = by_pos.setdefault(position_id, {"events": [], "closed_event": None})
        if e.get("closed_event"):
            arr["closed_event"] = e
        else:
            arr["events"].append(e)

    for pid, pack in by_pos.items():
        core = _fetch_position_core(conn, pid)
        if not core:
            continue
        is_closed = str(core.get("status", "")).upper() == "CLOSED" or (pack.get("closed_event") is not None)
        short_chain, long_chain = _fetch_trigger_chain(conn, pid)

        if is_closed:
            if pack["events"]:
                ev_key = f"exit:{pack['events'][-1]['exit_id']}"
            else:
                closed_at = core.get("closed_at") or (pack["closed_event"].get("closed_at") if pack.get("closed_event") else "")
                ev_key = f"exit:{pid}@{closed_at}"
            out.append({
                "event_key": ev_key,
                "event_type": "SELL",
                "symbol": core.get("symbol"),
                "timeframe": core.get("timeframe"),
                "direction": core.get("direction"),
                "order_id": core.get("client_order_id"),
                "position_id": pid,
                "entry_price": core.get("entry_price"),
                "exit_price": core.get("exit_price"),
                "exit_reason": core.get("exit_reason"),
                "pnl_usd": core.get("pnl_usd"),
                "trigger_chain": short_chain,
                "trigger_chain_detail": long_chain,
                "created_at": core.get("closed_at") or "",
                "t_ref": core.get("t_ref"),
            })
            continue

        if pack["events"]:
            has_tp = any((e.get("action") == "TP") for e in pack["events"])
            has_close_like = any((e.get("action") in ("TSL","CTX","STRUCT","TIME","SL","MANUAL","CLOSE")) for e in pack["events"])
            if has_tp and not has_close_like:
                last_tp = [e for e in pack["events"] if e.get("action") == "TP"][-1]
                ev_key = f"exit:{last_tp['exit_id']}"
                out.append({
                    "event_key": ev_key,
                    "event_type": "TRIM",
                    "symbol": core.get("symbol"),
                    "timeframe": core.get("timeframe"),
                    "direction": core.get("direction"),
                    "order_id": core.get("client_order_id"),
                    "position_id": pid,
                    "entry_price": core.get("entry_price"),
                    "trim_price": last_tp.get("price"),
                    "pnl_usd": last_tp.get("pnl_usd"),
                    "tp_reason": last_tp.get("reason_text"),
                    "trigger_chain": short_chain,
                    "trigger_chain_detail": long_chain,
                    "created_at": last_tp.get("created_at"),
                    "t_ref": last_tp.get("t_ref"),
                })

    return out


# ------------------------------
# 去重、渲染、落库
# ------------------------------
def _ensure_spot_table(conn: sqlite3.Connection) -> None:
    _ensure_tables(conn)


def _dedupe(conn: sqlite3.Connection, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not events:
        return []
    _ensure_spot_table(conn)
    cur = conn.execute("SELECT event_key FROM spot_mirror_snapshot;")
    sent = set([r[0] for r in cur.fetchall()])
    fresh = [ev for ev in events if ev.get("event_key") not in sent]
    return fresh


def _render_summary(events: List[Dict[str, Any]], t_ref: str) -> Optional[str]:
    if not events:
        return None
    kinds = {}
    for ev in events:
        kinds[ev["event_type"]] = kinds.get(ev["event_type"], 0) + 1
    parts = [f"{k}:{v}" for k, v in sorted(kinds.items())]
    lines = [ "📦 现货镜像——本桶汇总",
              f"时间: {t_ref}",
              f"事件: {', '.join(parts)}",
              "——"]
    for i, ev in enumerate(events, 1):
        sym = ev.get("symbol") or "-"
        tf = ev.get("timeframe") or "-"
        et = ev.get("event_type")
        if et == "BUY":
            ent = ev.get("entry_price")
            lines.append(f"{i}. [BUY]  {sym} {tf} 入:{ent}")
        elif et == "TRIM":
            trim = ev.get("trim_price")
            lines.append(f"{i}. [TRIM] {sym} {tf} 减:{trim}  链:{ev.get('trigger_chain')}")
        elif et == "SELL":
            xp = ev.get("exit_price")
            rsn = ev.get("exit_reason") or "-"
            lines.append(f"{i}. [SELL] {sym} {tf} 出:{xp} 原因:{rsn}  链:{ev.get('trigger_chain')}")
        elif et == "INFO_SHORT":
            lines.append(f"{i}. [INFO] {sym} {tf} 系统开空(现货侧建议减持/观望)")
    return "\n".join(lines)


def _render_per_event(conn: sqlite3.Connection, ev: Dict[str, Any]) -> str:
    et = ev.get("event_type")
    sym = ev.get("symbol") or "-"
    tf = ev.get("timeframe") or "-"
    if et == "BUY":
        ctx = ev.get("decision_ctx") or {}
        rs = ctx.get("reasons_json") or ""
        kw = []
        if isinstance(rs, str) and rs:
            for k in ("vol", "risk_cost", "btc_align", "corr"):
                if k in rs and len(kw) < 2:
                    kw.append(k)
        msg = (
            "🟢 买入提醒（镜像）\n"
            f"时间: {ev.get('created_at')}\n"
            f"交易对: {sym}  周期: {tf}  方向: {ev.get('direction')}\n"
            f"入场: {ev.get('entry_price')}  金额(USD): {ev.get('notional_usd')}\n"
            f"状态: {ev.get('status')}\n"
            + (f"依据: {', '.join(kw)}\n" if kw else "")
            + f"单号: {ev.get('order_id')}"
        )
        return msg

    if et == "TRIM":
        short_chain = ev.get("trigger_chain") or "-"
        long_chain  = ev.get("trigger_chain_detail") or ""
        return (
            "🟡 减仓提醒（镜像）\n"
            f"时间: {ev.get('created_at')}\n"
            f"交易对: {sym}  周期: {tf}  方向: {ev.get('direction')}\n"
            f"减仓价: {ev.get('trim_price')}  触发链: {short_chain}\n"
            + (f"明细: {long_chain}\n" if long_chain else "")
            + f"仓位ID: {ev.get('position_id')}"
        )

    if et == "SELL":
        short_chain = ev.get("trigger_chain") or "-"
        long_chain  = ev.get("trigger_chain_detail") or ""
        pnl = ev.get("pnl_usd")
        return (
            "🔴 卖出提醒（镜像）\n"
            f"时间: {ev.get('created_at')}\n"
            f"交易对: {sym}  周期: {tf}  方向: {ev.get('direction')}\n"
            f"出场: {ev.get('exit_price')}  原因: {ev.get('exit_reason') or '-'}\n"
            f"触发链: {short_chain}\n"
            + (f"明细: {long_chain}\n" if long_chain else "")
            + (f"实盈亏(USD): {pnl:.2f}\n" if isinstance(pnl, (int, float)) else "")
            + f"仓位ID: {ev.get('position_id')}"
        )

    if et == "INFO_SHORT":
        return (
            "🔵 系统开空（镜像提示）\n"
            f"时间: {ev.get('created_at')}\n"
            f"交易对: {sym}  周期: {tf}\n"
            "说明: 系统在合约侧开空。现货侧建议减持或观望。"
        )

    return f"[未知事件] {json.dumps(ev, ensure_ascii=False)}"


def _persist_sent(conn: sqlite3.Connection, t_ref: str, events: List[Dict[str, Any]], texts: List[str]) -> None:
    if not events:
        return
    _ensure_tables(conn)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for ev, txt in zip(events, texts):
        conn.execute("""
        INSERT OR IGNORE INTO spot_mirror_snapshot
            (t_ref, event_key, event_type, position_id, order_id, symbol, timeframe, text, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            t_ref,
            ev.get("event_key"),
            ev.get("event_type"),
            ev.get("position_id"),
            ev.get("order_id"),
            ev.get("symbol"),
            ev.get("timeframe"),
            txt,
            now,
        ))


# ------------------------------
# 配置加载
# ------------------------------
def _load_config_cli() -> Dict[str, Any]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="/www/wwwroot/Crypto-Signal/config.yml")
    parser.add_argument("--verbose", "-v", action="store_true", help="打印详细调试日志")
    args = parser.parse_args()
    return {"config_path": args.config, "verbose": bool(args.verbose)}


def _load_cfg(db_default: str = DEFAULT_DB_PATH) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
    """
    优先使用 core.config_loader；失败则 YAML 兜底。
    返回 (cfg, db_path, cli)
    """
    cli = _load_config_cli()
    cfg = {}
    db_path = db_default

    # 1) core.config_loader
    if load_cfg and get_db_path:
        try:
            cfg = load_cfg()
            db_path = get_db_path(cfg)
            return cfg, db_path, cli
        except Exception:
            pass

    # 2) YAML 兜底
    path = cli.get("config_path") or "/www/wwwroot/Crypto-Signal/config.yml"
    try:
        if yaml is None:
            raise RuntimeError("PyYAML not available")
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        cfg = raw
    except Exception:
        cfg = {}

    candidates = [
        "/www/wwwroot/Crypto-Signal/app/trading_signals_core.db",
        "/www/wwwroot/Crypto-Signal/trading_signals_core.db",
        "./trading_signals_core.db",
    ]
    for c in candidates:
        if os.path.exists(c):
            db_path = c; break
    return cfg, db_path, cli


# ------------------------------
# 主流程
# ------------------------------
def main() -> None:
    global VERBOSE
    started_ts = datetime.now().timestamp()

    cfg, db_path, cli = _load_cfg()
    VERBOSE = bool(cli.get("verbose", False))

    _log(f"[spot_mirror] DB: {db_path}")

    with _db_conn(db_path) as conn:
        # 心跳表准备（若模块存在）
        if ops_heartbeat and hasattr(ops_heartbeat, "ensure"):
            try:
                ops_heartbeat.ensure(conn)  # type: ignore
            except Exception as e:
                _dbg(f"heartbeat.ensure failed: {e}")

        # 时间窗口
        cursor_iso = _load_cursor(conn)
        grace_sec = 120
        try:
            grace_sec = int(
                (((cfg or {}).get("core") or {}).get("spot_mirror") or {}).get("window_grace_sec", 120)
            )
        except Exception:
            pass
        now = datetime.now()
        start_iso, end_iso = _time_window(now, cursor_iso, grace_sec)
        _log(f"[spot_mirror] window: ({start_iso} -> {end_iso}]  cursor={cursor_iso or '<none>'}")

        # 拉事件
        orders = _select_new_orders(conn, start_iso, end_iso)
        exits  = _select_exit_events(conn, start_iso, end_iso)
        _log(f"[spot_mirror] fetched: orders={len(orders)}  exit_events={len(exits)}")

        if VERBOSE:
            if orders:
                _dbg("orders sample: " + json.dumps(orders[:3], ensure_ascii=False))
            if exits:
                _dbg("exits sample: " + json.dumps(exits[:3], ensure_ascii=False))

        # 合并与标准化
        events = _coalesce_events(conn, orders, exits)

        # 事件类型统计
        kinds = {}
        for ev in events:
            kinds[ev["event_type"]] = kinds.get(ev["event_type"], 0) + 1
        types_line = ", ".join([f"{k}:{v}" for k, v in sorted(kinds.items())]) or "none"
        _log(f"[spot_mirror] coalesced events={len(events)} ({types_line})")

        # 去重
        fresh = _dedupe(conn, events)
        _log(f"[spot_mirror] fresh={len(fresh)} (deduped {len(events)-len(fresh)})")

        # 预探测通知后端（即便无事件也打印）
        notifier, backend = _resolve_notifier()
        _log(f"[spot_mirror] notify_backend={backend}")

        # 渲染 & 推送
        t_ref = now.strftime("%Y-%m-%d %H:%M:%S")
        texts: List[str] = []

        # 汇总（仅当 fresh>0 且配置允许）
        send_summary = True
        try:
            send_summary = bool(
                (((cfg or {}).get("core") or {}).get("spot_mirror") or {}).get("send_summary", True)
            )
        except Exception:
            pass

        if fresh and send_summary:
            summ = _render_summary(fresh, t_ref)
            if summ:
                _notify(summ)
        else:
            _log("[spot_mirror] nothing to notify (no fresh events)") if not fresh else None

        # 逐笔
        for ev in fresh:
            txt = _render_per_event(conn, ev)
            texts.append(txt)
            _notify(txt)

        # 落库（仅新事件）
        _persist_sent(conn, t_ref, fresh, texts)

        # 写心跳（窗口、计数）
        finished_ts = datetime.now().timestamp()
        if ops_heartbeat and hasattr(ops_heartbeat, "write"):
            try:
                ops_heartbeat.write(  # type: ignore
                    conn,
                    job="spot_mirror",
                    t_ref=t_ref,
                    started_at=started_ts,
                    finished_at=finished_ts,
                    status="ok",
                    msg=f"orders={len(orders)}, exits={len(exits)}, fresh={len(fresh)}",
                    opened_n=len([e for e in fresh if e.get('event_type')=='BUY']),
                    closed_n=len([e for e in fresh if e.get('event_type')=='SELL']),
                    skipped_n=len(events) - len(fresh),
                    payload={"window": [start_iso, end_iso]},
                    cfg=cfg,  # ← 新增
                )
                _dbg("heartbeat.write ok")
            except Exception as e:
                _log(f"[spot_mirror][heartbeat][WARN] {e}")

    _log("[spot_mirror] done.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print("[spot_mirror] FATAL:", e, file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
