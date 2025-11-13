# -*- coding: utf-8 -*-
"""
core/db_access.py

统一管理 SQLite 连接和事务边界。

特性：
- 提供 with db_conn(db_path) as conn:
    * 打开连接
    * 关闭 foreign key 约束（影子模式允许我们插入审计仓位，不被历史外键关系卡死）
    * 开启 WAL / 合理的同步模式
    * with 块正常结束 -> commit()
    * 异常 -> rollback()

注意：这只影响当前 shadow 系统用的复制库 trading_signals_core.db。
不会回写你线上老库。
"""
import sqlite3
import contextlib
from typing import Iterator


def _connect(db_path: str) -> sqlite3.Connection:
    """
    建立到 SQLite 的连接，开启 row_factory=sqlite3.Row，返回 conn。
    这里假定 db_path 是已经算好的最终路径。
    不在这里再去搞 cfg，不在这里再去 get_db_path。
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # WAL / 性能 / 外键这些可以按你原本文件里保持，如果原来有就保留
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextlib.contextmanager
def db_conn(db_path: str) -> Iterator[sqlite3.Connection]:
    """
    用法：
        with db_conn(db_path) as conn:
            ...操作conn...
    结束时自动 commit / close
    """
    conn = _connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
