# -*- coding: utf-8 -*-
"""
db_clear.py — 清理 trading_signals_core.db 的过程/资金类数据，保留上游入库与基线数据。

- 保留：ohlcv、signals、btc_alignment_snapshot、以及 chip*/vpoc*/volume_profile* 等筹码/筹码峰类表
- 清理：positions_virtual、orders、exit_log、equity_snapshot、decision_snapshot、
       correlation_snapshot、liquidity_snapshot、risk_cost_snapshot、pnl_*、ops_heartbeat、fuse_log 等
- 额外规则：除了已知清单，还会清理所有 *snapshot / *_cache / *_queue / *_tmp（但跳过保留白名单）

CLI:
  --db /path/to/trading_signals_core.db  指定数据库路径（默认见 DEFAULT_DB_PATH）
  --dry-run      仅预演（不执行删除）
  --yes          跳过 “YES” 确认
  --vacuum       清理完成后 VACUUM
  --reset-seq    重置被清理表的 AUTOINCREMENT 计数
  --keep T       额外保留某表（可多次）
  --also T       额外指定清理某表（可多次）
"""

import argparse
import contextlib
import datetime
import os
import re
import shutil
import sqlite3
import sys

DEFAULT_DB_PATH = "/www/wwwroot/Crypto-Signal/app/trading_signals_core.db"

# ---- 白名单：明确保留（上游入库 & 基线） ----
KEEP_EXACT = {
    "ohlcv",
    "signals",                 # 原始信号：保留
    "btc_alignment_snapshot",  # BTC 对齐：保留
}

# 筹码/筹码峰类：按前缀保留（按你实际库命名可增删）
KEEP_PATTERNS = [
    r"^chip",               # chip_* / chip_structure_* ...
    r"^vpoc",               # vpoc_* ...
    r"^volume_profile",     # volume_profile_* ...
]

# ---- 已知过程/资金类候选：存在即清 ----
WIPE_CANDIDATES = {
    "positions_virtual",
    "orders",
    "exit_log",
    "equity_snapshot",
    "equity_peak",
    "decision_snapshot",
    "correlation_snapshot",
    "liquidity_snapshot",
    "risk_cost_snapshot",
    "pnl_snapshot",
    "pnl_snapshots",
    "ops_heartbeat",
    "fuse_log",
    "breadth_snapshot",
    # 常见缓存/队列
    "signal_queue",
    "signal_cache",
    "calc_cache",
    "decision_evidence",
}

def _should_keep(name: str) -> bool:
    if name in KEEP_EXACT:
        return True
    return any(re.match(p, name) for p in KEEP_PATTERNS)

def _compute_wipe_set(conn: sqlite3.Connection, also_wipe: set, extra_keep: set):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = {r[0] for r in cur.fetchall()}

    # 1) 已知清单
    wipe = {t for t in WIPE_CANDIDATES if t in all_tables}

    # 2) 规则追加：*snapshot / *_cache / *_queue / *_tmp
    for t in all_tables:
        if t in wipe:
            continue
        if _should_keep(t):
            continue
        if ("snapshot" in t and t != "btc_alignment_snapshot"):
            wipe.add(t)
        if t.endswith("_cache") or t.endswith("_queue") or t.endswith("_tmp"):
            wipe.add(t)

    # 3) 用户显式追加清理
    wipe |= {t for t in also_wipe if t in all_tables}

    # 4) 去掉显式或规则保留
    wipe = {t for t in wipe if not _should_keep(t)}
    wipe -= extra_keep  # --keep 优先级最高

    keep = all_tables - wipe
    return all_tables, wipe, keep

@contextlib.contextmanager
def db_conn(path: str):
    conn = None
    try:
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def _backup(db_path: str) -> str:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = f"{db_path}.bak.{ts}"
    shutil.copy2(db_path, bak)
    return bak

def main():
    ap = argparse.ArgumentParser(description="清理过程/资金类数据，保留上游入库与基线数据。")
    ap.add_argument("--db", default=DEFAULT_DB_PATH, help="数据库路径")
    ap.add_argument("--dry-run", action="store_true", help="仅预演")
    ap.add_argument("--yes", action="store_true", help="跳过确认")
    ap.add_argument("--vacuum", action="store_true", help="清理后 VACUUM")
    ap.add_argument("--reset-seq", action="store_true", help="重置 AUTOINCREMENT 计数")
    ap.add_argument("--keep", action="append", default=[], help="额外保留表（可多次）")
    ap.add_argument("--also", action="append", default=[], help="额外清理表（可多次）")
    args = ap.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"[ERR] DB 不存在: {db_path}", file=sys.stderr)
        sys.exit(2)

    extra_keep = set(args.keep or [])
    also_wipe = set(args.also or [])

    with db_conn(db_path) as conn:
        all_tables, wipe_set, keep_set = _compute_wipe_set(conn, also_wipe, extra_keep)

        print("="*60)
        print(f"DB: {db_path}")
        print("将清理（DELETE FROM）以下表：")
        for t in sorted(wipe_set):
            print(f"  - {t}")
        print("\n将保留以下表：")
        for t in sorted(keep_set):
            print(f"  - {t}")
        print("="*60)

        if args.dry_run:
            return

        if not args.yes:
            ans = input("请输入 YES 确认执行：").strip()
            if ans != "YES":
                print("已取消。")
                return

        bak = _backup(db_path)
        print(f"[OK] 备份完成: {bak}")

        failed = []
        total_deleted = 0
        for t in sorted(wipe_set):
            try:
                cur = conn.execute(f'DELETE FROM "{t}";')
                total_deleted += (cur.rowcount or 0)
                if args.reset_seq:
                    try:
                        conn.execute('DELETE FROM sqlite_sequence WHERE name=?', (t,))
                    except sqlite3.Error:
                        pass
                print(f"[✓] 清空 {t} — 删除行数: {cur.rowcount if cur.rowcount is not None else 'unknown'}")
            except sqlite3.Error as e:
                print(f"[✗] 清理 {t} 失败: {e}")
                failed.append((t, str(e)))

        if args.vacuum:
            try:
                conn.execute("VACUUM;")
                print("[✓] VACUUM 完成")
            except sqlite3.Error as e:
                print(f"[✗] VACUUM 失败: {e}")

        print("="*60)
        print(f"完成。清理表数: {len(wipe_set)}，估计删除总行数: {total_deleted}")
        if failed:
            print("以下表清理失败：")
            for t, msg in failed:
                print(f"  - {t}: {msg}")

if __name__ == "__main__":
    main()
