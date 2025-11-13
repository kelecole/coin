# -*- coding: utf-8 -*-
"""
db_clear_all.py — 【高危】清理数据库中所有表的数据。

- 功能：默认 DELETE FROM 数据库中所有的用户表。
- 保留：仅保留通过 --keep 参数显式指定的表。
- 注意：此操作只会删除数据，不会 DROP 表（Schema 保留）。

CLI:
  --db /path/to/db   指定数据库路径
  --dry-run          仅预演（不执行删除）
  --yes              跳过 “YES” 确认
  --vacuum           清理完成后 VACUUM (推荐，重建数据库文件以释放空间)
  --reset-seq        重置所有被清理表的 AUTOINCREMENT 计数
  --keep T           例外保留某表（可多次使用）
"""

import argparse
import contextlib
import datetime
import os
import shutil
import sqlite3
import sys

# 默认数据库路径（如需常用可修改此处）
DEFAULT_DB_PATH = "/www/wwwroot/Crypto-Signal/app/trading_signals_core.db"

def _compute_wipe_set(conn: sqlite3.Connection, extra_keep: set):
    """
    计算需要清理的表集合。
    逻辑：默认清理所有非系统表，除非在 extra_keep 中指定。
    """
    # 获取所有用户表（排除 sqlite_ 开头的系统表）
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    all_tables = {r[0] for r in cur.fetchall()}

    # 计算保留集：仅包含用户显式指定的表
    keep_set = {t for t in extra_keep if t in all_tables}

    # 计算清理集：所有表 - 保留集
    wipe_set = all_tables - keep_set

    return all_tables, wipe_set, keep_set

@contextlib.contextmanager
def db_conn(path: str):
    conn = None
    try:
        conn = sqlite3.connect(path)
        # 启用 WAL 模式以提高并发性能（可选，视你环境而定）
        conn.execute("PRAGMA journal_mode=WAL;")
        # 开启外键约束，确保级联删除（如果你的 Schema 设计了级联）
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
    bak = f"{db_path}.bak_all_{ts}"
    shutil.copy2(db_path, bak)
    return bak

def main():
    ap = argparse.ArgumentParser(description="【高危】清理数据库中所有表的数据（保留 Schema）。")
    ap.add_argument("--db", default=DEFAULT_DB_PATH, help="数据库路径")
    ap.add_argument("--dry-run", action="store_true", help="仅预演，列出将要清理的表")
    ap.add_argument("--yes", action="store_true", help="跳过二次确认")
    ap.add_argument("--vacuum", action="store_true", help="清理后执行 VACUUM 以减小文件体积")
    ap.add_argument("--reset-seq", action="store_true", help="重置被清理表的自增 ID (sqlite_sequence)")
    ap.add_argument("--keep", action="append", default=[], help="例外保留的表名（可多次使用）")
    
    args = ap.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"[ERR] DB 不存在: {db_path}", file=sys.stderr)
        sys.exit(2)

    extra_keep = set(args.keep or [])

    with db_conn(db_path) as conn:
        all_tables, wipe_set, keep_set = _compute_wipe_set(conn, extra_keep)

        print("="*60)
        print(f"【全库清理模式】目标 DB: {db_path}")
        print(f"总表数: {len(all_tables)}")
        print("-"*60)
        
        if wipe_set:
            print(f"即将清空（DELETE）以下 {len(wipe_set)} 张表：")
            for t in sorted(wipe_set):
                print(f"  [DEL] {t}")
        else:
            print("没有需要清理的表。")

        if keep_set:
            print(f"\n例外保留以下 {len(keep_set)} 张表：")
            for t in sorted(keep_set):
                print(f"  [KEEP] {t}")
        print("="*60)

        if not wipe_set:
            print("无需执行任何操作。")
            return

        if args.dry_run:
            print("[INFO] Dry-run 模式结束，未修改任何数据。")
            return

        # 危险操作二次确认
        if not args.yes:
            print("警告：此操作将不可逆地删除上述所有表中的数据！")
            ans = input("请仔细核对，输入 YES (全大写) 确认执行：").strip()
            if ans != "YES":
                print("操作已取消。")
                return

        # 执行备份
        try:
            bak = _backup(db_path)
            print(f"[OK] 已自动备份至: {bak}")
        except Exception as e:
            print(f"[ERR] 备份失败，终止操作: {e}")
            sys.exit(1)

        # 执行清理
        print("开始清理...")
        failed = []
        total_deleted_rows = 0
        
        # 禁用外键约束可能提高大批量删除速度，但需确保数据一致性不在意
        # 这里为了安全默认保持开启，如果遇到互锁问题可考虑临时关闭
        # conn.execute("PRAGMA foreign_keys=OFF;") 

        for t in sorted(wipe_set):
            try:
                # 使用 TRUNCATE 的替代方案（SQLite 不支持 TRUNCATE，DELETE 不带 WHERE 通常有优化）
                cur = conn.execute(f'DELETE FROM "{t}";')
                rows = cur.rowcount
                total_deleted_rows += (rows if rows is not None else 0)

                if args.reset_seq:
                    # 尝试重置自增序列
                    try:
                        conn.execute('DELETE FROM sqlite_sequence WHERE name=?', (t,))
                    except sqlite3.Error:
                        pass # 某些表可能没有自增列，忽略错误

                print(f"  [✓] 已清空: {t:<30} (删除约 {rows} 行)")
            except sqlite3.Error as e:
                print(f"  [✗] 清理失败: {t:<30} 错误: {e}")
                failed.append((t, str(e)))

        # VACUUM
        if args.vacuum:
            print("\n正在执行 VACUUM (可能需要一些时间)...")
            try:
                conn.execute("VACUUM;")
                print("[✓] VACUUM 完成，数据库已压缩。")
            except sqlite3.Error as e:
                print(f"[✗] VACUUM 失败: {e}")

        print("="*60)
        print(f"操作完成。")
        print(f"清理表数: {len(wipe_set) - len(failed)} / {len(wipe_set)}")
        print(f"累计删除行数: ~{total_deleted_rows}")
        if failed:
            print("\n以下表清理失败，请检查占用或权限：")
            for t, msg in failed:
                print(f"  - {t}: {msg}")

if __name__ == "__main__":
    main()