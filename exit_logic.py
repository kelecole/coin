# -*- coding: utf-8 -*-
"""
exit_logic.py

单仓位出场判定逻辑，不负责真正平仓，不写数据库。

职责：
    decide_exit_for_position(...)
    -> 返回这个持仓当前是否应该被强平（should_exit=True/False）以及原因。

原因（互斥，优先级从高到低）：
1. dd_kill              整体风控熔断：账户回撤过线，所有仓必须离场
2. context_invalidated  [新] 市场环境失效 (BTC对齐失败、广度门关闭等)
3. setup_invalidated    交易的前提被打脸（价格触及止损）
4. no_followthrough     进场后应该走的方向一直没走，资金被卡住没正向表达

非常重要：
- 这里不直接改 positions_virtual，不更新 status 不计算 realized_pnl_usd
- 我们只是“法官”，负责说这仓要不要死、为什么
- 真正执行平仓+写回DB的是 exit_daemon
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Any, Optional
from datetime import datetime

# [修复 1] 导入 btc_alignment 模块，以便调用与 decision_engine 相同的硬过滤门
try:
    from . import btc_alignment
except ImportError:
    # 兼容性处理，以防模块路径问题
    print("[WARN] exit_logic.py 无法导入 btc_alignment，市场环境检查将跳过")
    btc_alignment = None


# ---------- 小工具 ----------

# ---------- 小工具 ----------
def _parse_ts(ts_str: Optional[str]) -> Optional[datetime]:
    """把 'YYYY-MM-DD HH:MM:SS' 解析成 datetime。失败就返回 None。"""
    if not ts_str:
        return None
    try:
        # 修正这里：原来是 "%Y-%m-%d %H:%M:S"，缺少秒两位的 %S
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None



def _calc_unrealized_pnl_pct(entry_price: float, current_price: float, side: str) -> Optional[float]:
    """
    粗略浮动收益率（方向化）:
        long:  (cur/entry - 1)
        short: (entry/cur - 1)

    返回小数，比如 +0.05 = +5%
    如果数据不齐或出问题，返回 None
    """
    try:
        ep = float(entry_price)
        cp = float(current_price)
        if ep <= 0 or cp <= 0:
            return None
    except Exception:
        return None

    s = (side or "").lower()
    if s == "long":
        return (cp / ep) - 1.0
    elif s == "short":
        return (ep / cp) - 1.0
    else:
        return None


# ---------- [新增] 规则 2: context_invalidated (市场环境失效) ----------

def _load_latest_btc_alignment(conn: sqlite3.Connection, symbol: str, t_ref: str) -> Dict[str, Any]:
    """
    (工具函数) - 从 btc_alignment_snapshot 加载最新的数据。
    t_ref 应该是当前时间，但我们用 t_ref <= now 来获取最近的一条记录。
    """
    try:
        # 1. 尝试加载 t_ref (例如 '10:00:00') 的数据
        row = conn.execute(
            """
            SELECT * FROM btc_alignment_snapshot
            WHERE symbol = ? AND t_ref = ?
            LIMIT 1
            """,
            (symbol, t_ref)
        ).fetchone()

        if row:
            return dict(row)

        # 2. 如果 t_ref 没匹配上，加载小于 t_ref 的最新一条
        row = conn.execute(
            """
            SELECT * FROM btc_alignment_snapshot
            WHERE symbol = ? AND t_ref <= ?
            ORDER BY t_ref DESC
            LIMIT 1
            """,
            (symbol, t_ref)
        ).fetchone()
        
        if row:
            return dict(row)
            
    except Exception as e:
        print(f"[exit_logic] _load_latest_btc_alignment failed: {e}")
        
    # 3. 如果数据库中完全没有（或查询失败），返回一个模拟的 "missing" 状态
    # 这将触发 'btc_align_missing' 拒绝理由（与你的截图一致）
    return {"symbol": symbol, "calc_ok": 0, "reject_reason": "btc_align_missing"}


def _should_kill_for_context_invalidated(
    conn: sqlite3.Connection,
    pos_row: Dict[str, Any],
    now_ts: str,
    cfg: dict
) -> Dict[str, Any]:
    """
    [新] 规则 2: 市场环境失效。
    检查该持仓是否还能通过当前(now_ts)的 BTC 对齐硬过滤。
    如果通不过，说明持仓前提已失效，应立即平仓。
    """
    
    # 如果 btc_alignment 模块没有成功导入，跳过此检查
    if btc_alignment is None:
        return {"triggered": False, "reason": None, "detail": {"note": "btc_alignment module missing, check skipped"}}

    symbol = pos_row.get("symbol")
    direction = (pos_row.get("side") or "").lower()
    
    # 1. 我们需要一个 't_ref' (整点时间) 来查询 snapshot
    # 我们简单地把 now_ts (例如 "2025-11-01 10:01:23") 向下取整到 "2025-11-01 10:00:00"
    try:
        t_ref_dt = datetime.strptime(now_ts, "%Y-%m-%d %H:%M:%S").replace(minute=0, second=0, microsecond=0)
        t_ref_str = t_ref_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        t_ref_str = now_ts # 回退

    # 2. 加载最新的 BTC 对齐数据 (包括 BTC 自己的)
    btc_aliases = cfg.get("btc_symbols") or cfg.get("core", {}).get("btc_symbols") or ["BTC/USDT"]
    btc_row = _load_latest_btc_alignment(conn, btc_aliases[0], t_ref_str)
    sym_row = _load_latest_btc_alignment(conn, symbol, t_ref_str)

    # 模拟 decision_engine 使用的 btc_alignment_map
    btc_alignment_map = {
        symbol: sym_row,
        btc_aliases[0]: btc_row
    }
    
    # 3. 模拟一个只包含当前仓位的 "candidates" 列表
    candidate_to_check = {
        "symbol": symbol,
        "timeframe": pos_row.get("timeframe", "1h"),
        "direction": direction,
    }

    # 4. 调用与 decision_engine 完全相同的 BTC 硬过滤逻辑
    #    (注意：我们使用的是 btc_alignment._filter_by_btc_alignment)
    try:
        kept, rejected, diag = btc_alignment._filter_by_btc_alignment(
            candidates=[candidate_to_check],
            btc_alignment_map=btc_alignment_map,
            cfg=cfg
        )
    except Exception as e:
        # 防御性编程：如果过滤函数出错，我们不杀仓
        print(f"[ERROR] exit_logic btc_alignment._filter_by_btc_alignment failed: {e}")
        return {"triggered": False, "reason": None, "detail": {"note": "btc_alignment filter failed"}}
    
    # 5. 分析结果
    if len(rejected) > 0:
        # 被拒绝了！说明市场环境已失效
        # (这会捕获到 'btc_align_missing', 'btc_up_but_short_highcorr' 等所有理由)
        reason = rejected[0].get("reject_reason", "context_invalidated")
        
        return {
            "triggered": True,
            "reason": reason, 
            "detail": {"note": "Context check failed (e.g., BTC Alignment)", "diag": diag},
        }

    # 通过了检查，允许继续持有
    return {"triggered": False, "reason": None, "detail": {"note": "context_still_ok"}}


# ---------- 规则 3: setup_invalidated (原规则 2) ----------

def _should_kill_for_setup_invalidated(
    pos_row: Dict[str, Any],
    cfg: dict,
    current_price: float,
) -> Dict[str, Any]:
    """
    规则 3: setup_invalidated (价格止损):
    - 当初进场有个 setup_tag，比如 "breakout_long" (看多突破) 或 "breakdown_short" (看空破位)。
    - 我们还存了 validation_level（关键价 / 结构线）。
    - 如果价格已经回到关键线反方向，那 setup 死了，不允许嘴硬改故事。
      直接平仓。
    """

    side = (pos_row.get("side") or "").lower()
    setup_tag = (pos_row.get("setup_tag") or "").lower()

    # validation_level 是“这单为什么还合理”的关键价位
    validation_level = pos_row.get("validation_level")
    try:
        validation_level = float(validation_level) if validation_level is not None else None
    except Exception:
        validation_level = None

    try:
        cp = float(current_price)
        if cp <= 0:
            cp = None
    except Exception:
        cp = None

    # 基础数据不齐 -> 我们不贸然判死刑
    if (cp is None) or (validation_level is None) or (side not in ("long", "short")):
        return {
            "triggered": False,
            "reason": None,
            "detail": {"note": "insufficient_validation_info"},
        }

    # 多头突破单必须守住关键线
    if "breakout" in setup_tag and side == "long":
        if cp < validation_level:
            return {
                "triggered": True,
                "reason": "setup_invalidated",
                "detail": {
                    "current_price": cp,
                    "validation_level": validation_level,
                    "setup_tag": setup_tag,
                    "note": "long_breakout_failed",
                },
            }

    # 空头破位单必须维持在破位线下
    if "breakdown" in setup_tag and side == "short":
        if cp > validation_level:
            return {
                "triggered": True,
                "reason": "setup_invalidated",
                "detail": {
                    "current_price": cp,
                    "validation_level": validation_level,
                    "setup_tag": setup_tag,
                    "note": "short_breakdown_failed",
                },
            }

    # 其他情况暂时不强杀
    return {
        "triggered": False,
        "reason": None,
        "detail": {
            "current_price": cp,
            "validation_level": validation_level,
            "setup_tag": setup_tag,
            "note": "setup_still_ok_or_unknown_tag",
        },
    }


# ---------- 规则 4: no_followthrough (原规则 3) ----------

def _should_kill_for_no_followthrough(
    pos_row: Dict[str, Any],
    now_ts: str,
    cfg: dict,
    current_price: float,
) -> Dict[str, Any]:
    """
    规则 4: no_followthrough (时间止损):
    - 给仓位一个观察窗口，比如 2 小时。
    - 如果过了这个窗口，它还是没给我们正向推动（比如至少 +0.5%/+1% 的浮盈），
      那就是尸体仓位，占用风险预算不产出，应该砍。
    - 我们并不等它亏很多才砍；没动也要砍。
    """

    max_stale_min = cfg.get("exit", {}).get("max_stale_minutes", 120)
    try:
        max_stale_min = float(max_stale_min)
    except Exception:
        max_stale_min = 120.0

    min_pnl_to_consider = cfg.get("exit", {}).get("min_pnl_to_consider", 0.005)
    try:
        min_pnl_to_consider = float(min_pnl_to_consider)
    except Exception:
        min_pnl_to_consider = 0.005

    opened_at_ts = _parse_ts(pos_row.get("opened_at"))
    now_dt = _parse_ts(now_ts)
    if (opened_at_ts is None) or (now_dt is None):
        # 时间信息缺失 -> 我们不敢杀
        return {
            "triggered": False,
            "reason": None,
            "detail": {"note": "missing timestamps"},
        }

    age_min = (now_dt - opened_at_ts).total_seconds() / 60.0

    pnl_pct = _calc_unrealized_pnl_pct(
        pos_row.get("entry_price"),
        current_price,
        pos_row.get("side", ""),
    )

    # 时间不够久，不杀
    if age_min < max_stale_min:
        return {
            "triggered": False,
            "reason": None,
            "detail": {
                "age_min": round(age_min, 2),
                "need_min": max_stale_min,
                "pnl_pct": pnl_pct,
            },
        }

    # 时间够久: 如果它至少给了我们正向浮盈 >= min_pnl_to_consider，就算它“有表现”
    if pnl_pct is not None and pnl_pct >= min_pnl_to_consider:
        return {
            "triggered": False,
            "reason": None,
            "detail": {
                "age_min": round(age_min, 2),
                "need_min": max_stale_min,
                "pnl_pct": pnl_pct,
                "note": "followthrough_ok",
            },
        }

    # 否则就是没跟上、没表现，砍掉
    return {
        "triggered": True,
        "reason": "no_followthrough",
        "detail": {
            "age_min": round(age_min, 2),
            "need_min": max_stale_min,
            "pnl_pct": pnl_pct,
            "note": "stale_position_should_exit",
        },
    }



# ---------- 主入口：decide_exit_for_position (已修改) ----------
def decide_exit_for_position(
    conn: sqlite3.Connection,
    pos_row: Dict[str, Any],
    now_ts: str,
    cfg: dict,
    dd_kill_flag: bool,
    current_price: float,
) -> Dict[str, Any]:
    """
    为单一持仓决定：现在是否必须平仓。

    输入：
        conn            sqlite3.Connection
        pos_row         一条未平仓位，通常来自 positions_virtual WHERE status IN ('OPEN','ACTIVE')
        now_ts          当前时间 "YYYY-MM-DD HH:MM:SS" (例如 '10:01:23')
        cfg             全局配置 (来自 config_loader)
        dd_kill_flag    True/False，账户是否已触发 dd_kill 熔断
        current_price   最新成交价/可平仓价

    返回：
        {
          "should_exit": True/False,
          "reason": "dd_kill" | "context_invalidated" | "setup_invalidated" | "no_followthrough" | None,
          "detail": {...},
          "strategy_version": strategy_version
        }
    """

    # 获取策略版本 (这个版本是开仓时的版本，我们保留它)
    strategy_version = str(pos_row.get("strategy_version") or cfg.get("strategy_version", "v1.0.0"))

    # --- [修复 2] 检查优先级已调整 ---

    # 1. 全局熔断优先，直接全杀
    if dd_kill_flag:
        return {
            "should_exit": True,
            "reason": "dd_kill",
            "detail": {
                "note": "account-level drawdown kill switch",
            },
            "strategy_version": strategy_version
        }

    # 2. [新] 市场环境 (BTC对齐等) 失效
    #    (例如 ICP 在 09:00 通过了对齐, 但在 10:00 对齐数据丢失)
    context_res = _should_kill_for_context_invalidated(
        conn,
        pos_row,
        now_ts, # 传入 '10:01:23'
        cfg
    )
    if context_res["triggered"]:
        return {
            "should_exit": True,
            "reason": context_res.get("reason", "context_invalidated"),
            "detail": context_res.get("detail", {}),
            "strategy_version": strategy_version 
        }

    # 3. setup 被打脸（关键价位丢了）
    invalid_res = _should_kill_for_setup_invalidated(
        pos_row,
        cfg,
        current_price,
    )
    if invalid_res["triggered"]:
        return {
            "should_exit": True,
            "reason": "setup_invalidated",
            "detail": invalid_res["detail"],
            "strategy_version": strategy_version
        }

    # 4. 没有推进（卡在仓位里浪费风险额度）
    nft_res = _should_kill_for_no_followthrough(
        pos_row,
        now_ts,
        cfg,
        current_price,
    )
    if nft_res["triggered"]:
        return {
            "should_exit": True,
            "reason": "no_followthrough",
            "detail": nft_res["detail"],
            "strategy_version": strategy_version
        }

    # 5. 允许继续持有
    return {
        "should_exit": False,
        "reason": None,
        "detail": {
            "note": "position_ok_to_keep",
        },
        "strategy_version": strategy_version
    }
