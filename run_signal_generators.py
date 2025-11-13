# -*- coding: utf-8 -*-
"""
run_signal_generators.py

职责：
- 在每个桶闭合后（例如 09:00、13:00 等），统一调度“信号模块”：
    1) sig_vpoc_flip.run_for_bucket
    2) sig_avwap_flip.run_for_bucket
    3) sig_confluence_reclaim.run_for_bucket   ← 新增：AVWAP ∧ VPOC 共振回收（多空对称）
    4) sig_iar_revert.run_for_bucket           ← IAR（日内 AVWAP 回归，现货长多）
    5) sig_sqz_br.run_for_bucket               ← 压缩后放量破位
- 任何一步报错仅记录，不影响后续与 open_cycle。
- 统一读取 config.yml，使用 core.db_path 连接 SQLite。
- t_ref 默认用“当前上海时间”的 19 位字符串，也可通过 --t_ref 指定。

用法：
    python run_signal_generators.py
    python run_signal_generators.py --config /path/to/config.yml --t_ref "2025-11-05 13:00:00"
"""

from __future__ import annotations
import argparse
import logging
import os
import sys
import sqlite3
from typing import Dict, Any

# 依赖 PyYAML
try:
    import yaml
except Exception as e:
    print("[FATAL] 需要安装 pyyaml：pip install pyyaml", file=sys.stderr)
    raise

# 你的时区工具（位于 core 的上级目录）
from timezone_converter import TimezoneConverter


# -----------------------------------------------------------------------------
# 生成器安全加载
# -----------------------------------------------------------------------------
def _safe_import_generators():
    """
    返回 {模块名: 模块或 None}
    缺失不抛异常，仅记录 warning。
    """
    mods = {}
    for name in ("sig_vpoc_flip", "sig_avwap_flip", "sig_confluence_reclaim", "sig_iar_revert", "sig_sqz_br"):
        try:
            mods[name] = __import__(f"core.{name}", fromlist=[name])
        except Exception as e:
            logging.warning(f"[runner] 模块缺失或导入失败，已跳过：core.{name} ({e})")
            mods[name] = None
    return mods


# -----------------------------------------------------------------------------
# 基础工具
# -----------------------------------------------------------------------------
_TZC = TimezoneConverter(local_timezone="Asia/Shanghai")

def _now_shanghai_str() -> str:
    """当前时刻（UTC 秒）→ 上海 'YYYY-MM-DD HH:MM:SS' 文本。"""
    import time
    return _TZC.convert_to_local(time.time())[:19]


def _load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise RuntimeError("配置文件格式错误：根对象不是字典")
    return cfg


def _resolve_config_path(arg_path: str | None) -> str:
    """
    按优先级解析配置路径：
    1) 显式 --config
    2) 环境变量 CS_CONFIG
    3) 当前工作目录 ./config.yml
    4) 脚本同级  ./config.yml
    5) 脚本上一级 ../config.yml
    6) 硬编码 /www/wwwroot/Crypto-Signal/config.yml
    找到即返回；否则抛错并打印尝试过的路径。
    """
    candidates = []
    if arg_path:
        candidates.append(arg_path)

    env_path = os.getenv("CS_CONFIG")
    if env_path:
        candidates.append(env_path)

    here = os.path.dirname(os.path.abspath(__file__))
    candidates += [
        os.path.join(os.getcwd(), "config.yml"),
        os.path.join(here, "config.yml"),
        os.path.normpath(os.path.join(here, "..", "config.yml")),
        "/www/wwwroot/Crypto-Signal/config.yml",
    ]

    for p in candidates:
        if p and os.path.exists(p):
            return p

    raise FileNotFoundError(
        "[runner] 找不到 config.yml，尝试过以下路径：\n" + "\n".join(candidates)
    )


def _get_db_path(cfg: Dict[str, Any]) -> str:
    core = cfg.get("core", cfg)
    db_path = core.get("db_path")
    if not db_path:
        raise RuntimeError("config.core.db_path 未设置")
    return db_path


def _ensure_logger(verbosity: int = 1):
    level = logging.INFO if verbosity <= 1 else logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _run_one(name: str, mod, conn: sqlite3.Connection, t_ref: str, cfg: Dict[str, Any]) -> Dict[str, int]:
    """
    执行单个生成器的 run_for_bucket；异常不崩，返回空统计。
    """
    stats = {"inserted": 0, "updated": 0, "skipped": 0}
    if mod is None:
        logging.info(f"[runner] 跳过 {name}（模块未加载）")
        return stats

    if not hasattr(mod, "run_for_bucket"):
        logging.warning(f"[runner] 跳过 {name}（缺少 run_for_bucket）")
        return stats

    try:
        logging.info(f"[runner] 开始 {name}.run_for_bucket(t_ref={t_ref})")
        res = mod.run_for_bucket(conn, t_ref, cfg)
        if isinstance(res, dict):
            stats.update({k: int(res.get(k, 0)) for k in stats.keys()})
        conn.commit()
        logging.info(f"[runner] 完成 {name}: {stats}")
    except Exception as e:
        logging.exception(f"[runner] {name} 发生异常，已跳过：{e}")
        try:
            conn.rollback()
        except Exception:
            pass
    return stats


# -----------------------------------------------------------------------------
# 主流程
# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="信号生成器统一调度")
    ap.add_argument("--config", default=None, help="配置文件路径（留空则自动探测）")
    ap.add_argument("--t_ref", default=None, help="桶时间（上海本地 'YYYY-MM-DD HH:MM:SS'），默认=当前上海时间")
    ap.add_argument("-v", "--verbose", action="count", default=0, help="重复 -v 提升日志级别")
    args = ap.parse_args()

    _ensure_logger(args.verbose)

    # 读取配置（自动解析上一级 ../config.yml 等）
    try:
        config_path = _resolve_config_path(args.config)
        logging.info(f"[runner] 使用配置：{config_path}")
        cfg = _load_config(config_path)
    except Exception as e:
        logging.exception(f"[runner] 读取配置失败：{e}")
        return

    # 取 DB 路径
    try:
        db_path = _get_db_path(cfg)
        logging.info(f"[runner] 使用数据库：{db_path}")
    except Exception as e:
        logging.exception(f"[runner] 配置缺失：{e}")
        return

    # 计算桶时间
    t_ref = args.t_ref or _now_shanghai_str()
    logging.info(f"[runner] 桶时间 t_ref（上海本地）：{t_ref}")

    # 连接数据库
    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        logging.exception(f"[runner] 无法连接数据库：{db_path} ({e})")
        return

    # 动态加载生成器
    mods = _safe_import_generators()

    # 固定顺序执行：VPOC → AVWAP → Confluence → IAR → Squeeze
    total = {"inserted": 0, "updated": 0, "skipped": 0}
    for name in ("sig_vpoc_flip", "sig_avwap_flip", "sig_confluence_reclaim", "sig_iar_revert", "sig_sqz_br"):
        stats = _run_one(name, mods.get(name), conn, t_ref, cfg)
        for k in total:
            total[k] += int(stats.get(k, 0))

    try:
        conn.close()
    except Exception:
        pass

    logging.info(f"[runner] 统计汇总：inserted={total['inserted']}, updated={total['updated']}, skipped={total['skipped']}")
    logging.info("[runner] 信号生成完成（异常不影响后续 open_cycle）")


if __name__ == "__main__":
    main()
