# -*- coding: utf-8 -*-
import json, time, socket, os, sqlite3
# [修改] 替换 datetime 导入
from core.timebox import now_local_str, epoch_to_local_str

_DDL = """
CREATE TABLE IF NOT EXISTS ops_heartbeat (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  job           TEXT    NOT NULL,
  t_ref         TEXT    NOT NULL,
  started_at    TEXT    NOT NULL,
  finished_at   TEXT    NOT NULL,
  duration_ms   INTEGER,
  status        TEXT    NOT NULL,           -- ok|error|skip
  hostname      TEXT,
  pid           INTEGER,
  msg           TEXT,
  opened_n      INTEGER,
  closed_n      INTEGER,
  skipped_n     INTEGER,
  used_R        REAL,
  dd_kill_flag  INTEGER,                    -- 0/1
  payload_json  TEXT,
  created_at    TEXT    NOT NULL,
  UNIQUE(job, t_ref, started_at)
);
"""

def ensure(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL)

# [修改] 删除了全局的、不带时区的 _ts 函数
# def _ts(sec: float) -> str:
#     return datetime.fromtimestamp(sec).strftime("%Y-%m-%d %H:%M:%S")

def write(conn: sqlite3.Connection, *,
          job: str,
          t_ref: str,
          started_at: float,
          finished_at: float,
          cfg: dict | None = None,  # ← 改为可选
          status: str = "ok",
          msg: str = "",
          opened_n: int | None = None,
          closed_n: int | None = None,
          skipped_n: int | None = None,
          used_R: float | None = None,
          dd_kill_flag: bool | None = None,
          payload: dict | None = None) -> None:
    """
    兼容老/新结构的心跳写入：
    - 优先 INSERT；
    - 若命中老库的 UNIQUE(t_ref) 约束，则对该 t_ref 做 UPDATE 合并；
    - 只写“表里确实存在”的列（有列就写），自动兼容 module/ended_at/error/detail_json 等老字段。
    """
    try:
        # 新库会建表；老库则什么都不做
        try:
            ensure(conn)  # 已存在则无副作用
        except Exception:
            pass

        # 表实际列集合
        cols_info = conn.execute("PRAGMA table_info(ops_heartbeat);").fetchall()
        have = {r[1] for r in cols_info}  # r[1] 是列名

        # [修改] 统一获取 tz_name
        tz_name = (cfg or {}).get("tz", "Asia/Shanghai")  # ← 加兜底
        # [修改] _ts 函数使用 epoch_to_local_str 并捕获异常
        def _ts(sec: float) -> str:
            try:
                # 使用带时区的转换
                return epoch_to_local_str(sec, tz_name)
            except Exception:
                # 兜底：使用 now_local_str (近似原 datetime.now() 逻辑，但使用 timebox)
                return now_local_str(tz_name)

        duration_ms = int(max(0.0, float(finished_at) - float(started_at)) * 1000)
        
        # [修改] created_at 使用 now_local_str
        created_at_ts = now_local_str(tz_name)

        # 统一候选键值；仅当列存在才会被写入
        kv = {
            # 新结构
            "job": str(job),
            "t_ref": str(t_ref or ""),
            "started_at": _ts(started_at),
            "finished_at": _ts(finished_at),
            "duration_ms": duration_ms,
            "status": str(status),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "msg": str(msg or ""),
            "opened_n": opened_n,
            "closed_n": closed_n,
            "skipped_n": skipped_n,
            "used_R": None if used_R is None else float(used_R),
            "dd_kill_flag": None if dd_kill_flag is None else int(bool(dd_kill_flag)),
            "payload_json": None if payload is None else json.dumps(payload, ensure_ascii=False),
            "created_at": created_at_ts, # [修改]

            # 兼容老结构映射（有列就写）
            "module": str(job),  # 老库 NOT NULL 的 module：写成 job
            "error": None if status == "ok" else str(msg or ""),
            "detail_json": None if payload is None else json.dumps(payload, ensure_ascii=False),
            "ended_at": _ts(finished_at),
        }

        # ---------- 尝试 INSERT ----------
        ins_cols = [c for c in kv.keys() if c in have]
        ins_vals = [kv[c] for c in ins_cols]
        sql_ins = f"INSERT INTO ops_heartbeat({','.join(ins_cols)}) VALUES({','.join('?' for _ in ins_cols)})"
        try:
            conn.execute(sql_ins, ins_vals)
            conn.commit()
            return
        except sqlite3.IntegrityError as e:
            # 命中老库 UNIQUE(t_ref) 的冲突，则转 UPDATE 合并
            if "ops_heartbeat.t_ref" not in str(e):
                raise

        # ---------- 冲突改 UPDATE（按 t_ref 定位） ----------
        # 不要覆盖这些关键列
        dont_touch = {"t_ref", "job", "module", "started_at"}

        upd_cols = [c for c in ins_cols if c not in dont_touch]
        if not upd_cols:
            # 没啥可更新的，直接返回
            return

        sql_upd = "UPDATE ops_heartbeat SET " + ", ".join([f"{c}=?" for c in upd_cols]) + " WHERE t_ref=?"
        upd_vals = [kv[c] for c in upd_cols] + [kv["t_ref"]]
        conn.execute(sql_upd, upd_vals)
        conn.commit()

    except Exception as e:
        # 不让心跳失败影响主流程
        try:
            print(f"[ops_heartbeat][WARN] write failed: {e}")
        except Exception:
            pass