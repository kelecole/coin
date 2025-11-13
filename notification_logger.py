# -*- coding: utf-8 -*-
"""
core/notification_logger.py

提供一个中心化的函数，用于将所有出站的通知（开仓、平仓、错误等）
记录到数据库的 notification_log 表中。
"""

import sqlite3
import logging
from typing import Optional

try:
    # 假设在 core 包内运行
    from .timebox import now_local_str
except ImportError:
    # 兼容在 app/ 目录运行
    try:
        from core.timebox import now_local_str
    except ImportError:
        # 最后的兜底
        from datetime import datetime
        def now_local_str(tz_name="Asia/Shanghai"):
            # 这是一个简化的兜底，不处理时区
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_notification(
    conn: sqlite3.Connection,
    event_type: str,
    body: str,
    *,
    title: Optional[str] = None,
    position_id: Optional[int] = None,
    symbol: Optional[str] = None,
    status: str = "SENT",
    backend: str = "ALL"
) -> None:
    """
    将一条通知消息写入 notification_log 表。
    这是一个“尽力而为”的操作，失败时不应抛出异常使主程序崩溃。
    """
    try:
        # 确保表存在 (幂等检查)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            event_type TEXT,
            position_id INTEGER,
            symbol TEXT,
            title TEXT,
            body TEXT NOT NULL,
            status TEXT DEFAULT 'SENT',
            backend TEXT,
            FOREIGN KEY(position_id) REFERENCES positions_virtual(id)
        );""")
        
        # 使用 core.timebox 获取带时区的当前时间
        ts = now_local_str() 
        
        conn.execute(
            """
            INSERT INTO notification_log 
            (timestamp, event_type, position_id, symbol, title, body, status, backend)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, event_type, position_id, symbol, title, body, status, backend)
        )
        # 注意：这里的 conn.commit() 依赖于调用方 (run_open_cycle/run_exit_cycle) 
        # 在with db_access.db_conn() 块结束时自动提交。
        # 如果是独立使用，需要手动 conn.commit()。
        
    except Exception as e:
        # 关键：捕获所有异常，只打印日志，不让主程序崩溃
        logging.warning(f"[notification_logger] 无法记录通知到数据库: {e}")
        # 如果 logging 未配置，打印到 stderr
        print(f"[notification_logger] 无法记录通知到数据库: {e}")