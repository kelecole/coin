"""
risk_monitor.py

核心职责：
- 估当前权益 equity_usd = 起始资金 + 所有OPEN仓位的未实现PnL
- 维护峰值 peak_equity_usd（equity_peak 表）
- 算回撤 drawdown_pct
- 根据 cfg["risk"]["dd_kill_drawdown_pct"] 决定是否触发熔断 dd_kill
- 把这些写入 equity_snapshot (按 t_ref 作为主键幂等 Upsert)

***对外唯一推荐调用接口***
    check_drawdown_and_update_snapshot(conn, cfg, t_ref=None)

    这个函数会：
    1. 计算 equity / peak / drawdown_pct / dd_kill
    2. upsert 到 equity_snapshot
    3. 返回这些值（dict）

兼容旧代码：
    check_drawdown_and_equity(...) 只是别名，未来会删。
"""



from __future__ import annotations
import sqlite3
from typing import Dict, Any, Optional, List, Tuple
# [修改] 替换 datetime 导入
from core.timebox import now_local_str, now_local_dt

try:
    from core import market_data
except Exception:
    market_data = None


# ----------------------------------------------------------------------
# util: 检查并升级表结构
# ----------------------------------------------------------------------

def _table_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    for r in rows:
        # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
        if r[1] == col:
            return True
    return False

def _sum_realized_pnl(conn: sqlite3.Connection, t_ref: Optional[str]) -> float:
    """
    累计已实现盈亏（按 t_ref 截止到本桶时间）。exit_log 是权威来源。
    """
    try:
        if t_ref:
            row = conn.execute(
                "SELECT COALESCE(SUM(pnl_usd), 0.0) FROM exit_log WHERE t_ref <= ?",
                (t_ref,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COALESCE(SUM(pnl_usd), 0.0) FROM exit_log"
            ).fetchone()
        return float(row[0] or 0.0)
    except Exception:
        return 0.0


def _ensure_tables(conn: sqlite3.Connection) -> None:
    """
    我们需要的结构：
    equity_peak(id=1, peak_equity_usd, updated_at)

    equity_snapshot(
        t_ref PRIMARY KEY,
        equity_usd,
        peak_equity_usd,
        drawdown_pct,
        dd_kill INTEGER NOT NULL,
        strategy_version TEXT NULL,
        created_at TEXT NOT NULL,
        dd_kill_flag INTEGER NULL
    )

    你的库现在基本长这样，只是有的列可能没加，或者是旧结构。
    我们这里做两件事：
    1. CREATE TABLE IF NOT EXISTS （防止初次运行报表不存在）
    2. 针对 equity_snapshot，缺的列就补列
       （如果列已存在，ALTER TABLE 会报 duplicate column name，直接忽略即可）
    """

    # 1. equity_peak
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equity_peak (
            id INTEGER PRIMARY KEY CHECK (id=1),
            peak_equity_usd REAL NOT NULL,
            updated_at TEXT NOT NULL
        );
    """)

    # 2. equity_snapshot (基本骨架，尽量贴近你现有库)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equity_snapshot (
            t_ref TEXT PRIMARY KEY,
            equity_usd REAL NOT NULL,
            peak_equity_usd REAL NOT NULL,
            drawdown_pct REAL NOT NULL,
            dd_kill INTEGER NOT NULL,
            strategy_version TEXT,
            created_at TEXT NOT NULL
        );
    """)

    # 2a. 如果老表里还没有 dd_kill_flag，补上
    if not _table_has_column(conn, "equity_snapshot", "dd_kill_flag"):
        try:
            conn.execute("ALTER TABLE equity_snapshot ADD COLUMN dd_kill_flag INTEGER;")
        except sqlite3.OperationalError:
            # already exists or cannot add -> ignore
            pass

    # 2b. 如果老表里没有 strategy_version（罕见，但为了容错）
    if not _table_has_column(conn, "equity_snapshot", "strategy_version"):
        try:
            conn.execute("ALTER TABLE equity_snapshot ADD COLUMN strategy_version TEXT;")
        except sqlite3.OperationalError:
            pass

    # 2c. 如果老表里没有 created_at（极旧版本）
    if not _table_has_column(conn, "equity_snapshot", "created_at"):
        try:
            conn.execute("ALTER TABLE equity_snapshot ADD COLUMN created_at TEXT;")
        except sqlite3.OperationalError:
            pass

    # 2d. 防御：保证列 drawdown_pct 存在
    if not _table_has_column(conn, "equity_snapshot", "drawdown_pct"):
        try:
            conn.execute("ALTER TABLE equity_snapshot ADD COLUMN drawdown_pct REAL;")
        except sqlite3.OperationalError:
            pass

    # 2e. 防御：保证列 dd_kill 存在
    if not _table_has_column(conn, "equity_snapshot", "dd_kill"):
        try:
            conn.execute("ALTER TABLE equity_snapshot ADD COLUMN dd_kill INTEGER;")
        except sqlite3.OperationalError:
            pass

    # 注意：我们不去删列，不去改 NOT NULL 约束
    # 我们后面写 snapshot 时会同时写 dd_kill 和 dd_kill_flag，保证兼容你现有 NOT NULL 约束


# ----------------------------------------------------------------------
# 推测当前桶的 t_ref
# ----------------------------------------------------------------------

def _guess_current_t_ref(conn: sqlite3.Connection, cfg: dict) -> str:
    row = conn.execute(
        "SELECT t_ref FROM signals ORDER BY t_ref DESC LIMIT 1"
    ).fetchone()

    # 兼容 sqlite3.Row / tuple
    if row:
        try:
            v = row["t_ref"]  # 若 row_factory=sqlite3.Row
        except Exception:
            v = row[0] if len(row) > 0 else None  # 默认 tuple
        if (v or "").strip():
            return str(v)

    # signals 为空或无法读取：按配置时区回落到整分
    tz = cfg.get("tz", "Asia/Shanghai")
    return now_local_dt(tz).replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:00")


# ----------------------------------------------------------------------
# 最新价格获取
# ----------------------------------------------------------------------

def _get_last_price(conn: sqlite3.Connection, cfg: dict, symbol: str) -> Optional[float]:
    """
    获取 symbol 最新价。
    1. 优先 core.market_data.get_last_price / get_ticker_price
    2. 兜底去 ohlcv* 表里按时间最新的 close
    如果拿不到，返回 None
    """

    # 1) 尝试 core.market_data 内部报价函数
    if market_data is not None:
        for fn_name in ("get_last_price", "get_ticker_price"):
            if hasattr(market_data, fn_name):
                try:
                    fn = getattr(market_data, fn_name)
                    px = fn(conn=conn, cfg=cfg, symbol=symbol)
                    if px is not None:
                        return float(px)
                except Exception:
                    pass

    # 2) 尝试本地K线表
    try:
        tbls = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name LIKE 'ohlcv%';
        """).fetchall()
        cand_tables = [r["name"] for r in tbls] if tbls else []

        preferred = []
        for t in ("ohlcv_1h", "ohlcv_4h", "ohlcv", "candles_1h", "candles"):
            if t in cand_tables:
                preferred.append(t)
        for t in cand_tables:
            if t not in preferred:
                preferred.append(t)

        for tname in preferred:
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
                except Exception:
                    continue
                if row and row[0] is not None:
                    return float(row[0])
    except Exception:
        pass

    return None


# ----------------------------------------------------------------------
# 计算当前权益
# ----------------------------------------------------------------------
def _estimate_equity_from_positions(
    conn: sqlite3.Connection,
    cfg: dict,
) -> Tuple[float, List[Dict[str, Any]]]:
    """
    返回 (equity_usd, details_list)

    修正后定义：
      equity_usd = starting_equity
                 + realized_pnl (来自 exit_log)
                 + unrealized_pnl (来自当前 OPEN 仓)

      unrealized_pnl:
        long:  (last - entry) * qty
        short: (entry - last) * qty
    """

    starting_equity = float(cfg["risk"].get("starting_equity_usd", 10000))
    # [修改] 传入 cfg
    t_ref_now = _guess_current_t_ref(conn, cfg)

    # 1) 已实现盈亏（按本桶时间之前）
    realized_total = _sum_realized_pnl(conn, t_ref_now)

    # 2) 未实现盈亏（当前 OPEN 仓）
    rows = conn.execute("""
        SELECT symbol, direction, qty, entry_price
        FROM positions_virtual
        WHERE status='OPEN';
    """).fetchall()

    unrealized_total = 0.0
    details: List[Dict[str, Any]] = []

    for r in rows or []:
        symbol = r["symbol"]
        direction = (r["direction"] or "").lower()
        qty = float(r["qty"] or 0.0)
        entry = float(r["entry_price"] or 0.0)

        last_px = _get_last_price(conn, cfg, symbol)
        if last_px is None or qty == 0.0:
            pnl = 0.0
        else:
            if direction == "long":
                pnl = (float(last_px) - entry) * qty
            elif direction == "short":
                pnl = (entry - float(last_px)) * qty
            else:
                pnl = 0.0

        unrealized_total += pnl
        details.append({
            "symbol": symbol,
            "direction": direction,
            "qty": qty,
            "entry_price": entry,
            "last_price": last_px,
            "unrealized_pnl": pnl,
        })

    equity = starting_equity + realized_total + unrealized_total
    return equity, details



# ----------------------------------------------------------------------
# 峰值管理
# ----------------------------------------------------------------------

def _read_peak(conn: sqlite3.Connection) -> Optional[float]:
    row = conn.execute(
        "SELECT peak_equity_usd FROM equity_peak WHERE id=1"
    ).fetchone()
    if row:
        return float(row[0])
    return None


def _upsert_peak(conn: sqlite3.Connection, new_peak: float, cfg: dict) -> None: # [修改] 增加 cfg
    # [修改] 定义 now_ts
    now_ts = now_local_str(cfg.get("tz","Asia/Shanghai"))
    cur = conn.execute(
        # [修改] 替换 datetime('now','localtime')
        "UPDATE equity_peak SET peak_equity_usd=?, updated_at=? WHERE id=1",
        (float(new_peak), now_ts), # [修改]
    )
    if cur.rowcount is None or cur.rowcount == 0:
        conn.execute(
            # [修改] 替换 datetime('now','localtime')
            "INSERT INTO equity_peak (id, peak_equity_usd, updated_at) VALUES (1, ?, ?)",
            (float(new_peak), now_ts), # [修改]
        )


# ----------------------------------------------------------------------
# 主逻辑：计算回撤、写 snapshot、返回熔断状态
# ----------------------------------------------------------------------

def check_drawdown_and_update_snapshot(
    conn: sqlite3.Connection,
    cfg: dict,
    t_ref: Optional[str] = None,
) -> Dict[str, Any]:
    """
    返回:
    {
        "t_ref": <str>,
        "equity_usd": <float>,
        "peak_equity_usd": <float>,
        "drawdown_pct": <float>,   # 比例，0.027=回撤2.7%
        "dd_kill": <bool>,
    }

    并把这轮检查写进 equity_snapshot。
    """

    # 0. 表结构对齐（包含 dd_kill / dd_kill_flag）
    _ensure_tables(conn)

    # [修改] 定义 now_ts
    now_ts = now_local_str(cfg.get("tz","Asia/Shanghai"))

    # 1. 当前权益
    equity_usd, _details = _estimate_equity_from_positions(conn, cfg)

    # 2. 峰值处理
    peak = _read_peak(conn)
    if peak is None:
        peak = equity_usd
        _upsert_peak(conn, peak, cfg) # [修改] 传入 cfg

    if equity_usd > peak:
        # 创新高，更新peak，回撤置0
        peak = equity_usd
        _upsert_peak(conn, peak, cfg) # [修改] 传入 cfg
        drawdown_frac = 0.0
    else:
        if peak > 0:
            drawdown_frac = (peak - equity_usd) / peak
        else:
            drawdown_frac = 0.0

    # 3. 熔断判断
    kill_thresh = float(cfg["risk"]["dd_kill_drawdown_pct"])  # 例如 0.03 表示3%
    dd_kill_bool = (drawdown_frac >= kill_thresh)
    dd_kill_int = 1 if dd_kill_bool else 0  # 这个会同时写到 dd_kill 和 dd_kill_flag

    # 4. 确定 t_ref（用这桶时间当主键）
    # [修改] 传入 cfg
    t_ref_final = t_ref or _guess_current_t_ref(conn, cfg)

    # 5. UPDATE 现有行；如果没有再 INSERT
    cur = conn.execute(
        """
        UPDATE equity_snapshot
        SET equity_usd=?,
            peak_equity_usd=?,
            drawdown_pct=?,
            dd_kill=?,
            dd_kill_flag=?,
            created_at=?
        WHERE t_ref=?;
        """, # [修改] created_at
        (
            float(equity_usd),
            float(peak),
            float(drawdown_frac),
            dd_kill_int,
            dd_kill_int,
            now_ts, # [修改] 传入 now_ts
            t_ref_final,
        ),
    )

    if cur.rowcount is None or cur.rowcount == 0:
        # 旧库要求 dd_kill NOT NULL, created_at NOT NULL
        conn.execute(
            """
            INSERT INTO equity_snapshot
                (t_ref,
                 equity_usd,
                 peak_equity_usd,
                 drawdown_pct,
                 dd_kill,
                 strategy_version,
                 created_at,
                 dd_kill_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """, # [修改] created_at
            (
                t_ref_final,
                float(equity_usd),
                float(peak),
                float(drawdown_frac),
                dd_kill_int,
                str(cfg.get("strategy_version", "dev")),
                now_ts, # [修改] 传入 now_ts
                dd_kill_int,
            ),
        )

    return {
        "t_ref": t_ref_final,
        "equity_usd": float(equity_usd),
        "peak_equity_usd": float(peak),
        "drawdown_pct": float(drawdown_frac),
        "dd_kill": bool(dd_kill_bool),
    }


# 给 run_open_cycle.py / run_exit_cycle.py 用的兼容旧名
def check_drawdown_and_equity(
    conn: sqlite3.Connection,
    cfg: dict,
    t_ref: Optional[str] = None,
) -> Dict[str, Any]:
    return check_drawdown_and_update_snapshot(conn, cfg, t_ref=t_ref)

def get_dd_kill_flag(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT t_ref, dd_kill_flag, drawdown_pct
        FROM equity_snapshot
        ORDER BY t_ref DESC
        LIMIT 1;
        """
    ).fetchone()

    if not row:
        print("[risk_monitor] no equity_snapshot rows yet, dd_kill=False (default)")
        return False

    # row 可能是 Row，也可能是 tuple，稳一点处理
    try:
        t_ref = row["t_ref"]
    except Exception:
        t_ref = row[0]

    try:
        dd_flag_val = row["dd_kill_flag"]
    except Exception:
        dd_flag_val = row[1]

    try:
        dd_drawdown = row["drawdown_pct"]
    except Exception:
        # 如果 row 是 tuple，drawdown_pct 应该是 index=2
        try:
            dd_drawdown = row[2]
        except Exception:
            dd_drawdown = None

    dd_bool = False
    try:
        dd_bool = bool(int(dd_flag_val))
    except Exception:
        dd_bool = False

    print(f"[risk_monitor] latest equity_snapshot t_ref={t_ref} drawdown={dd_drawdown} dd_kill_flag={dd_bool}")
    return dd_bool