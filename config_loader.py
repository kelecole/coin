# -*- coding: utf-8 -*-
"""
core/config_loader.py

读取 config.yml，并标准化成系统内部统一用的 cfg。
- 支持从显式 path / CWD / 父目录搜索
- 透传 core.* 常用字段
- 合并 core.exit 与 根级 exit（core 优先，根级兜底），并透传 exit.partials / exit.tsl
- 提供 load_config(path=None, refresh=False) 与 load_cfg()（兼容老代码）
"""

import os
import yaml
from typing import Optional, Dict, Any

_CFG_CACHE: Optional[Dict[str, Any]] = None
_CFG_PATH_CACHE: Optional[str] = None


def load_raw_config(path: Optional[str] = None) -> dict:
    """
    搜索顺序：
      1) 显式传入 path
      2) CWD/config.yml
      3) 父目录/config.yml
    命中后打印路径并返回 raw dict
    """
    candidate_paths = []
    if path:
        candidate_paths.append(path)

    cwd = os.getcwd()
    candidate_paths.append(os.path.join(cwd, "config.yml"))
    parent_dir = os.path.dirname(cwd)
    candidate_paths.append(os.path.join(parent_dir, "config.yml"))

    tried = []
    for p in candidate_paths:
        if not p or p in tried:
            continue
        tried.append(p)
        if os.path.isfile(p):
            with open(p, "r", encoding="utf-8") as f:
                print(f"[config_loader] using config: {p}")
                return yaml.safe_load(f)

    raise FileNotFoundError(f"config.yml not found. tried: {tried}")


def _normalize_cfg(raw: dict) -> dict:
    """
    把 config.yml 的结构（strategy_version + core.* + 根级 exit 兜底）组装为统一 cfg。
    关键输出：
      - strategy_version, tz, db_path, log_dir, mode
      - risk.*（含 dd_kill_drawdown_pct）
      - exit.max_stale_minutes / exit.min_pnl_to_consider
      - 透传 exit.partials / exit.tsl（用于 TSL/分批武装）
      - cooldown.*, candidates.include_sources, timeframes
      - thresholds / btc_ctx / direction_policy / fuses / risk_cost / fees
      - allow_short, limits, symbols, tests, btc_symbols
    """
    core = (raw or {}).get("core", {}) or {}

    out: Dict[str, Any] = {}

    # --- 基础元信息 ---
    out["strategy_version"] = raw.get("strategy_version", "dev")
    out["tz"] = core.get("tz", "Asia/Shanghai")
    out["db_path"] = core.get("db_path")
    out["log_dir"] = core.get("log_dir")
    out["mode"] = core.get("mode", "shadow")

    # 允许做空（从 core 或根级兜底）
    out["allow_short"] = bool(core.get("allow_short", raw.get("allow_short", False)))

    # ----- 账户权益 / 熔断线 -----
    account = core.get("account", {}) or {}
    fuses = core.get("fuses", {}) or {}

    starting_equity = account.get("equity_usd", 10000)

    # 老配置里常用百分比（3.0 表示 3%）
    max_daily_loss_pct = fuses.get("max_daily_loss_pct", 3.0)
    try:
        dd_kill_frac = float(max_daily_loss_pct) / 100.0
    except Exception:
        dd_kill_frac = 0.03  # 兜底 3%

    out["risk"] = {
        "starting_equity_usd": float(starting_equity),
        "dd_kill_drawdown_pct": dd_kill_frac,
        "max_R_per_bucket": 3.0,
        "base_R_per_trade": 1.0,
        "weak_R_per_trade": 0.5,
        "strong_signal_cutoff": 0.7,
    }

    # ----- 退出规则（合并 core.exit 与 根级 exit，core 优先）-----
    exit_core = core.get("exit", {}) or {}
    exit_root = raw.get("exit", {}) or {}
    exit_all = {**exit_root, **exit_core}  # 浅合并即可，core 覆盖 root

    out["exit"] = {
        "max_stale_minutes": exit_all.get("max_stale_minutes", 120),
        "min_pnl_to_consider": exit_all.get("min_pnl_to_consider", 0.005),
    }
    # 透传子结构（给 exit_daemon/TSL 使用）
    if isinstance(exit_all.get("partials"), dict):
        out["exit"]["partials"] = exit_all["partials"]
    if isinstance(exit_all.get("tsl"), dict):
        out["exit"]["tsl"] = exit_all["tsl"]

    # ----- 冷却规则 -----
    cooldown_core = core.get("cooldown", {}) or {}
    out["cooldown"] = {
        "block_if_open": bool(cooldown_core.get("block_if_open", True)),
        "min_hold_minutes": float(cooldown_core.get("min_hold_minutes", 120)),
    }

    # ----- 候选信号源 / 时间框架 -----
    cand_cfg = core.get("candidates", {}) or {}
    out["candidates"] = {
        "include_sources": cand_cfg.get(
            "include_sources",
            ["chip_llm", "candle_llm", "triangle_raw"],
        )
    }
    out["timeframes"] = core.get("timeframes", raw.get("timeframes", ["1h"]))

    # ----- 透传的大结构 -----
    out["thresholds"] = core.get("thresholds", {}) or {}
    out["btc_ctx"] = core.get("btc_ctx", {}) or {}
    out["direction_policy"] = core.get("direction_policy", {}) or {}
    out["fuses"] = fuses
    out["risk_cost"] = core.get("risk_cost", {}) or {}
    out["fees"] = core.get("fees", {}) or {}

    # 其余保持，以免后续模块需要
    out["limits"] = core.get("limits", {}) or {}
    out["symbols"] = core.get("symbols")
    out["tests"] = core.get("tests", [])
    out["btc_symbols"] = core.get("btc_symbols", [])

    return out


def load_config(path: Optional[str] = None, refresh: bool = False) -> dict:
    """
    统一入口（支持外界传 path；默认带缓存，refresh=True 强制重载）
    """
    global _CFG_CACHE, _CFG_PATH_CACHE
    if refresh or _CFG_CACHE is None or (path and path != _CFG_PATH_CACHE):
        raw = load_raw_config(path)
        _CFG_CACHE = _normalize_cfg(raw)
        _CFG_PATH_CACHE = path
    return _CFG_CACHE


# 兼容老代码（无参缓存版）
def load_cfg() -> dict:
    return load_config(path=None, refresh=False)


def get_db_path(cfg: dict) -> str:
    dbp = cfg.get("db_path")
    if not dbp:
        raise RuntimeError("db_path missing in config.yml under core.db_path")
    return dbp
