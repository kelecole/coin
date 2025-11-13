# app/core/timebox.py
from datetime import datetime
from typing import Optional

def now_local_str(tz_name: str = "Asia/Shanghai") -> str:
    """
    返回指定时区的当前时间：'YYYY-MM-DD HH:MM:SS'（文本）
    """
    try:
        from zoneinfo import ZoneInfo  # Py>=3.9
        tz = ZoneInfo(tz_name)
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        import pytz
        tz = pytz.timezone(tz_name)
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def now_local_dt(tz_name: str = "Asia/Shanghai") -> datetime:
    """
    返回 tz-aware datetime（便于再做舍入/加减）
    """
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        return datetime.now(tz)
    except Exception:
        import pytz
        tz = pytz.timezone(tz_name)
        return datetime.now(tz)

def epoch_to_local_str(sec: float, tz_name: str = "Asia/Shanghai") -> str:
    """
    epoch 秒 → 指定时区文本
    """
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        return datetime.fromtimestamp(sec, tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        import pytz, timezone as _tz
        tz = pytz.timezone(tz_name)
        return datetime.fromtimestamp(sec, tz).strftime("%Y-%m-%d %H:%M:%S")

def minute_floor_local_str(tz_name: str = "Asia/Shanghai") -> str:
    """
    当前分钟对齐到 :00（文本）
    """
    dt = now_local_dt(tz_name).replace(second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:00")
