# -*- coding: utf-8 -*-
"""
core/exit_daemon.py

[时区已修复]

职责：
- 真正执行平仓/关仓（对 positions_virtual 写回）
- 这是风控出口线，必须优先于新的开仓逻辑上线，防止盈利吐回去。
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Any, List
from datetime import datetime
import pytz  # <-- [修复] 导入 pytz 库

# 假设这些模块在同一 core 目录下
from . import market_data
from . import exit_logic
from . import risk_monitor


def _risk_unit_unified(entry_px: float,
                       stop_px: float | None,
                       entry_atr: float | None,
                       atr_now: float | None,
                       regime: str,
                       sl_cfg: dict) -> float:
    """
    R 分母统一口径：
      max( |entry - stop|, k_sl*entry_ATR, k_sl*ATR_now )
    regime: 'trending' 或 'ranging'
    sl_cfg: exit.sl 配置块（含 atr_mult）
    """
    try:
        kmap = (sl_cfg.get("atr_mult") or {"trending": 1.8, "ranging": 2.0})
    except Exception:
        kmap = {"trending": 1.8, "ranging": 2.0}
    k_sl = float(kmap.get("trending" if regime == "trending" else "ranging", 1.8))

    cands = []
    if stop_px is not None:
        cands.append(abs(float(entry_px) - float(stop_px)))
    if entry_atr is not None:
        cands.append(float(entry_atr) * k_sl)
    if atr_now is not None:
        cands.append(float(atr_now) * k_sl)
    return max(cands) if cands else 0.0


def _fmt_exit_reason(action: str, share_pct: float | None = None, extra: str | None = None) -> str:
    """
    统一的中文标签：
      TP     -> 【止盈-分批 xx%】
      TSL    -> 【止损-移动止损 100%】
      STRUCT -> 【止损-结构破位 100%】
      CTX    -> 【风控-冲突减仓 xx%/100%】
      TIME   -> 【风控-时间出场 100%】
    """
    label_map = {
        "TP": "止盈-分批",
        "TSL": "止损-移动止损",
        "STRUCT": "止损-结构破位",
        "CTX": "风控-冲突减仓",
        "TIME": "风控-时间出场",
    }
    head = f"【{label_map.get(action, action)}】" if share_pct is None else f"【{label_map.get(action, action)} {share_pct:.0f}%】"
    return head + (f" {extra}" if extra else "")



def _now_ts(cfg: dict) -> str:
    """
    使用 core/timebox 的统一时间封装，返回 'YYYY-MM-DD HH:MM:SS' 文本。
    仅更换时间获取方式，不改任何业务逻辑。
    """
    try:
        # 优先从 core 包内导入，兼容不同调用路径
        from . import timebox
    except Exception:
        import timebox  # 若同级导入失败，退回普通导入（保持兼容）

    tz_name = (
        cfg.get("core", {}).get("tz")      # 优先 core.tz
        or cfg.get("tz")                   # 兼容早期 cfg.tz
        or "Asia/Shanghai"                 # 兜底
    )
    return timebox.now_local_str(tz_name)



def _calc_realized_pnl_usd(
    entry_price: float,
    exit_price: float,
    side: str,
    notional_usd: float | None,
) -> float:
    """
    简单估算仓位的盈亏美元值。
    """
    try:
        ep = float(entry_price)
        xp = float(exit_price)
        if ep <= 0 or xp <= 0:
            return 0.0
    except Exception:
        return 0.0

    s = (side or "").lower()
    if s == "long":
        pct = (xp / ep) - 1.0
    elif s == "short":
        pct = (ep / xp) - 1.0
    else:
        pct = 0.0

    if notional_usd is None:
        return pct  # 退化: 返回比例
    try:
        nu = float(notional_usd)
    except Exception:
        nu = 0.0

    return pct * nu


def _fetch_open_positions(conn):
    """
    拉出还在开的仓位 (status='OPEN'/'ACTIVE')。
    """
    rows = conn.execute(
        """
        SELECT
            id                AS rowid,
            symbol            AS symbol,
            direction         AS side, -- 兼容
            timeframe         AS timeframe, -- exit_logic 需要
            entry_price       AS entry_price,
            opened_at         AS opened_at,
            status            AS status,
            setup_tag         AS setup_tag,
            validation_level  AS validation_level,
            strategy_version  AS strategy_version,
            notional_usd      AS notional_usd,
            closed_at         AS closed_at,
            exit_reason       AS exit_reason,
            pnl_usd           AS realized_pnl_usd,
            exit_price        AS exit_price
        FROM positions_virtual
        WHERE UPPER(status) IN ('OPEN','ACTIVE')
        ORDER BY opened_at ASC
        """
    ).fetchall()

    out = []
    for r in rows:
        out.append(dict(r))
    return out


def _close_position_row(
    conn: sqlite3.Connection,
    rowid: int,
    exit_reason: str,
    realized_pnl_usd: float,
    exit_price: float,
    now_ts_str: str  # <-- [修复] 传入准确的平仓时间戳
) -> None:
    """
    真正把这条仓位关掉（soft close，标记 status='CLOSED'，写 closed_at 等）。
    """
    # [修复] 不再调用 _now_ts()，使用传入的 now_ts_str
    # closed_ts = _now_ts() 

    conn.execute(
        """
        UPDATE positions_virtual
        SET status='CLOSED',
            closed_at=?,
            exit_reason=?,
            exit_price=?,
            pnl_usd=? 
        WHERE rowid=?
        """,
        (
            now_ts_str,  # <-- [修复] 使用传入的正确时间戳
            exit_reason,
            float(exit_price),
            float(realized_pnl_usd),
            rowid,
        )
    )

def _update_mfe_and_break_even(conn: sqlite3.Connection, pos: dict, ctx: dict, cfg: dict) -> dict:
    """
    统一R分母 + MFE更新 + BE武装/收紧（单调）。
    变更点（最小必要）：
      - R_den 优先使用 r0_px_dist（锁定分母）；若缺失/无效，先尝试从 DB 取 positions_virtual.r0_px_dist；
        再回退到原三候选：max(|entry-eff_stop|, k_sl*entry_ATR, k_sl*ATR_now)。
      - 将本次 R_den 来源与时间写回：positions_virtual.r_den_src_last / r_den_checked_at（审计）
    其它逻辑保持不变。
    """
    try:
        pid  = int(pos.get("rowid") or pos.get("id") or 0)
        if pid <= 0:
            return {"ok": False, "reason": "bad_pid"}

        side = (pos.get("direction") or pos.get("side") or "").lower().strip()
        if side not in ("long", "short"):
            return {"ok": False, "reason": "bad_direction"}

        # ---- 现价 / 上下文 ----
        px_now  = float(ctx.get("px_now"))
        regime  = (ctx.get("regime") or pos.get("regime") or "ranging").lower()

        atr_now = ctx.get("atr_now")
        if atr_now is None: atr_now = ctx.get("ATR14")
        if atr_now is None: atr_now = ctx.get("atr")
        atr_now = float(atr_now) if atr_now is not None else None

        entry = float(pos.get("entry_price") or 0.0)

        row = conn.execute(
            """SELECT stop_price, tsl_price, entry_atr, mfe_R, be_armed, be_price, timeframe, tp_stage
               FROM positions_virtual WHERE id=?""",
            (pid,),
        ).fetchone()
        if not row:
            return {"ok": False, "reason": "pos_missing"}

        stop_price_db = row[0]
        tsl_price_db  = row[1]
        entry_atr_db  = row[2]
        mfe_R_db      = float(row[3] or 0.0)
        be_armed_db   = int(row[4] or 0)
        be_price_db   = row[5]
        timeframe_db  = row[6] or (pos.get("timeframe") or "?")
        tp_stage_db   = int(row[7] or 0)

        # 兼容 entry_atr/atr_entry（旧路径）
        if entry_atr_db is None:
            if pos.get("entry_atr") is not None:
                entry_atr_db = float(pos["entry_atr"])
            elif pos.get("atr_entry") is not None:
                entry_atr_db = float(pos["atr_entry"])

        exit_cfg = (cfg.get("exit") or {})
        sl_cfg   = (exit_cfg.get("sl") or {})
        be_cfg   = (exit_cfg.get("break_even") or {})
        fees_cfg = (cfg.get("core") or {}).get("fees") or {}

        arm_at_R        = float(be_cfg.get("arm_at_R", 0.5))
        include_fees    = bool(be_cfg.get("include_fees", True))
        force_after_tp1 = bool(be_cfg.get("force_after_tp1", False))

        fee_bps   = float(fees_cfg.get("fee_bps", 7.0))
        slp_bps   = float(fees_cfg.get("slippage_bps", 8.0))
        cost_frac = (fee_bps + slp_bps) / 10000.0 if include_fees else 0.0

        # ---- 有效止损价（方向聚合，保持原口径）----
        def _eff_stop_long(sp, tsl):
            vals = [v for v in (sp, tsl) if v not in (None, "")]
            return max(map(float, vals)) if vals else None
        def _eff_stop_short(sp, tsl):
            vals = [v for v in (sp, tsl) if v not in (None, "")]
            return min(map(float, vals)) if vals else None

        eff_stop = _eff_stop_long(stop_price_db, tsl_price_db) if side == "long" \
                   else _eff_stop_short(stop_price_db, tsl_price_db)

        # ---- k_sl（保持原映射）----
        kmap = (sl_cfg.get("atr_mult") or {"trending": 1.8, "ranging": 2.0})
        k_sl = float(kmap.get("trending" if regime == "trending" else "ranging", 1.8))

        # ---- R 分母（优先 r0_px_dist；ctx -> DB -> 回退）----
        use_r0 = False
        src_label = "fallback"

        r0_px_dist = ctx.get("r0_px_dist")
        ctx_r0_valid = False
        if r0_px_dist is not None:
            try:
                r0_px_dist = float(r0_px_dist)
                if r0_px_dist > 0:
                    ctx_r0_valid = True
            except Exception:
                r0_px_dist = None

        if (r0_px_dist is None) or not (r0_px_dist > 0):
            try:
                row_r0 = conn.execute(
                    "SELECT r0_px_dist FROM positions_virtual WHERE id=?", (pid,)
                ).fetchone()
                if row_r0 is not None and row_r0[0] is not None and float(row_r0[0]) > 0:
                    r0_px_dist = float(row_r0[0])
                    src_label = "r0_db"
            except Exception:
                r0_px_dist = None

        R_den = 0.0
        if r0_px_dist is not None and r0_px_dist > 0:
            R_den = r0_px_dist
            use_r0 = True
            if ctx_r0_valid:
                src_label = "r0_ctx"
            elif src_label != "r0_db":
                src_label = "r0_db"
        else:
            dist_candidates = []
            if eff_stop is not None:
                dist_candidates.append(abs(entry - float(eff_stop)))
            if entry_atr_db is not None:
                dist_candidates.append(float(entry_atr_db) * k_sl)
            if atr_now is not None:
                dist_candidates.append(float(atr_now) * k_sl)
            R_den = max(dist_candidates) if dist_candidates else 0.0
            src_label = "fallback"

        # —— 审计写回：来源 + 时间（不中断主流程）——
        try:
            conn.execute(
                "UPDATE positions_virtual SET r_den_src_last=?, r_den_checked_at=? WHERE id=?;",
                (src_label, _now_ts(cfg), pid)
            )
        except Exception:
            pass

        # ---- 调试输出（可开关）----
        if (cfg.get("exit") or {}).get("debug_r", False):
            try:
                eff_stop_dbg = float(eff_stop) if eff_stop is not None else None
                comp_stop = abs(entry - eff_stop_dbg) if eff_stop_dbg is not None else None
                comp_eatr = (float(entry_atr_db) * k_sl) if entry_atr_db is not None else None
                comp_anow = (float(atr_now) * k_sl) if atr_now is not None else None
                print(
                    f"[Rden] pid={pid} {side} tf={timeframe_db} "
                    f"src={src_label} r0_px_dist={r0_px_dist} "
                    f"eff_stop={eff_stop_dbg} |entry-stop|={comp_stop} "
                    f"k*entryATR={comp_eatr} k*ATRnow={comp_anow} k={k_sl}  R_den={R_den}"
                )
            except Exception:
                pass

        # ---- R_now / MFE ----
        if R_den <= 0:
            R_now = 0.0
        else:
            R_now = ((px_now - entry) if side == "long" else (entry - px_now)) / R_den
        new_mfe_R = max(mfe_R_db, R_now)

        # ---- 保本价（含/不含成本）----
        be_target = entry * (1.0 + cost_frac) if side == "long" else entry * (1.0 - cost_frac)

        # ---- 武装 / 收紧 ----
        armed_now = False
        tightened = False

        if not be_armed_db:
            need_arm = (R_now >= arm_at_R) or (force_after_tp1 and tp_stage_db >= 1)
            if need_arm:
                new_stop = be_target
                if eff_stop is not None:
                    new_stop = max(float(eff_stop), float(be_target)) if side == "long" \
                               else min(float(eff_stop), float(be_target))
                conn.execute(
                    "UPDATE positions_virtual "
                    "SET be_price=?, stop_price=?, mfe_R=?, be_armed=?, updated_at=? "
                    "WHERE id=? AND status IN ('OPEN','ACTIVE');",
                    (float(be_target), float(new_stop), float(new_mfe_R), int(1), _now_ts(cfg), pid)
                )
                armed_now = True
                tightened = True
                try:
                    print(f"[BE][arm] pid={pid} {side} R_now={R_now:.4f} mfe->{new_mfe_R:.4f} stop->{new_stop:.6f}")
                except Exception:
                    pass
            else:
                conn.execute(
                    "UPDATE positions_virtual SET mfe_R=?, updated_at=? "
                    "WHERE id=? AND status IN ('OPEN','ACTIVE');",
                    (float(new_mfe_R), _now_ts(cfg), pid)
                )
        else:
            if eff_stop is None or \
               (side == "long" and float(be_target) > float(eff_stop)) or \
               (side == "short" and float(be_target) < float(eff_stop)):
                new_stop = float(be_target) if eff_stop is None else \
                           (max(float(eff_stop), float(be_target)) if side == "long"
                            else min(float(eff_stop), float(be_target)))
                conn.execute(
                    "UPDATE positions_virtual SET stop_price=?, mfe_R=?, updated_at=? "
                    "WHERE id=? AND status IN ('OPEN','ACTIVE');",
                    (float(new_stop), float(new_mfe_R), _now_ts(cfg), pid)
                )
                tightened = True
                try:
                    print(f"[BE][tighten] pid={pid} {side} R_now={R_now:.4f} mfe->{new_mfe_R:.4f} stop->{new_stop:.6f}")
                except Exception:
                    pass
            else:
                conn.execute(
                    "UPDATE positions_virtual SET mfe_R=?, updated_at=? "
                    "WHERE id=? AND status IN ('OPEN','ACTIVE');",
                    (float(new_mfe_R), _now_ts(cfg), pid)
                )

        try:
            print(f"[mfe] pid={pid} dir={side} tf={timeframe_db} R_now={R_now:.4f} new_mfe_R={new_mfe_R:.4f} "
                  f"be_armed={1 if (be_armed_db or armed_now) else 0}")
        except Exception:
            pass

        return {
            "ok": True,
            "R_now": float(R_now),
            "mfe_R": float(new_mfe_R),
            "be_armed": int(1 if (be_armed_db or armed_now) else 0),
            "be_price": float(be_target),
            "tightened": bool(tightened),
        }

    except Exception as e:
        print(f"[BE] error: {e}")
        return {"ok": False, "error": str(e)}



def _backfill_r_baseline_for_open_positions(conn: sqlite3.Connection, cfg: dict) -> dict:
    """
    目标（保留你原有逻辑，并新增 r0_* 只写空回填）：
    - 仅对 OPEN 且 (entry_atr 或 stop_price 缺失) 或 任一 r0_* 缺失 的老仓，补齐“R基线”
    - 计算 entry_atr（用入场时刻之前的 N 根K线估算ATR）
    - 计算初始 stop_price（结构位 与 ATR 止损 二取其紧；多=更低者，空=更高者）
    - 若已 BE 武装(be_armed=1)，与 be_price 做单调合并（不放宽）
    - 新增：为 r0_* 五列（r0_stop_price, r0_px_dist, r0_atr, r0_R_usd, r0_defined_at）
            做“只写空位”的一次性回填；已有值不改

    备注：
    - 尽量使用入场 t_ref 之前的K线，提升“入场刻度”的一致性
    - ATR长度：优先用 exit.tsl.atr_len.ranging，缺省 14
    - k_sl：用 exit.sl.atr_mult.{trending|ranging}；回填阶段未知 regime，取 ranging 档（更稳健）
    - 结构位：若 chip_structure 可用则参考，否则跳过（只用 ATR 止损）
    """
    try:
        # 取配置
        core = cfg.get("core", {})
        exit_cfg = core.get("exit", cfg.get("exit", {}))

        # ATR长度（回填默认用 ranging 档）；k_sl 用 sl.atr_mult.ranging
        atr_len_cfg = exit_cfg.get("tsl", {}).get("atr_len", {"trending": 14, "ranging": 10})
        atr_len = int(atr_len_cfg.get("ranging", 14) or 14)
        k_sl_cfg = exit_cfg.get("sl", {}).get("atr_mult", {"trending": 1.5, "ranging": 1.8})
        k_sl = float(k_sl_cfg.get("ranging", 1.8) or 1.8)

        # ---- 取需要回填的 OPEN 仓位 ----
        rows = conn.execute(
            """
            SELECT
              id, symbol, timeframe, direction, entry_price, t_ref,
              entry_atr, stop_price, be_armed, be_price,
              notional_usd,
              r0_stop_price, r0_px_dist, r0_atr, r0_R_usd, r0_defined_at
            FROM positions_virtual
            WHERE status='OPEN'
              AND (
                    entry_atr IS NULL OR stop_price IS NULL
                 OR r0_stop_price IS NULL OR r0_px_dist IS NULL
                 OR r0_atr IS NULL OR r0_R_usd IS NULL OR r0_defined_at IS NULL
              )
            ORDER BY opened_at ASC
            """
        ).fetchall()

        if not rows:
            print("[backfill] no open positions requiring baseline.")
            return {"ok": True, "updated": 0}

        updated = 0

        # 可选：结构位辅助（若模块在则用；失败不阻断）
        def _struct_stop_optional(symbol: str, timeframe: str, direction: str,
                                  entry_price: float, tref: str) -> float:
            try:
                try:
                    from core import chip_structure as _cs
                except Exception:
                    import chip_structure as _cs
                # 约定：返回的是“价格方向上的保护位”，多<entry，空>entry；不满足则返回 None
                return _cs.estimate_structure_stop(
                    conn=conn,
                    symbol=symbol,
                    timeframe=timeframe,
                    direction=direction,
                    entry_price=entry_price,
                    t_ref=tref,
                    cfg=cfg,
                )
            except Exception:
                return None

        # 计算ATR（简单SMA版），尽量取入场时刻之前的N+1根
        def _compute_atr_at_entry(symbol: str, timeframe: str, t_ref: str, N: int) -> float:
            recs = conn.execute(
                """
                SELECT t, open, high, low, close
                FROM ohlcv
                WHERE symbol=? AND timeframe=? AND t<=?
                ORDER BY t DESC
                LIMIT ?
                """,
                (symbol, timeframe, t_ref, max(N + 1, 2)),
            ).fetchall()
            if len(recs) < 2:
                return None
            # 逆序为时间正向
            recs = list(reversed(recs))
            trs = []
            prev_close = float(recs[0][4])
            for i in range(1, len(recs)):
                _t, _o, _h, _l, _c = recs[i]
                high = float(_h); low = float(_l); close = float(_c)
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                trs.append(tr)
                prev_close = close
            if not trs:
                return None
            # 简单均值
            if len(trs) >= N:
                from statistics import mean
                return float(mean(trs[-N:]))
            else:
                from statistics import mean
                return float(mean(trs))

        for r in rows:
            (pid, symbol, timeframe, direction,
             entry_px, t_ref,
             entry_atr0, stop0, be_armed, be_price,
             notional_usd,
             r0_sp0, r0_dist0, r0_atr0, r0_Rusd0, r0_def_at0) = r

            direction  = (str(direction or "")).lower()
            if direction not in ("long", "short") or not (entry_px and entry_px > 0):
                continue

            # === 1) 计算 entry_atr（若缺失） ===
            entry_atr = None
            if entry_atr0 is None:
                entry_atr = _compute_atr_at_entry(symbol, timeframe, t_ref, atr_len)
            else:
                try:
                    entry_atr = float(entry_atr0)
                except Exception:
                    entry_atr = None

            # === 2) 计算 ATR 止损价格（以入场为基） ===
            atr_stop = None
            if entry_atr and entry_atr > 0:
                if direction == "long":
                    atr_stop = entry_px - k_sl * entry_atr
                else:
                    atr_stop = entry_px + k_sl * entry_atr

            # === 3) 结构位（可选） ===
            struct_stop = _struct_stop_optional(symbol, timeframe, direction, entry_px, t_ref)

            # === 4) 合成“初始” stop（两者取更“紧”的一侧） ===
            init_stop = None
            try:
                if direction == "long":
                    # 多：更低者（更紧）
                    cand = [x for x in [atr_stop, struct_stop] if x is not None and x < entry_px]
                    init_stop = min(cand) if cand else atr_stop
                else:
                    # 空：更高者（更紧）
                    cand = [x for x in [atr_stop, struct_stop] if x is not None and x > entry_px]
                    init_stop = max(cand) if cand else atr_stop
            except Exception:
                init_stop = atr_stop

            # === 5) 与已有 stop0 / BE 单调合并（不放宽） ===
            final_stop = init_stop
            if stop0 is not None:
                try:
                    s0 = float(stop0)
                    if direction == "long":
                        final_stop = s0 if final_stop is None else max(s0, final_stop)
                    else:
                        final_stop = s0 if final_stop is None else min(s0, final_stop)
                except Exception:
                    final_stop = init_stop
            if be_armed == 1 and be_price is not None:
                try:
                    bp = float(be_price)
                    if direction == "long":
                        final_stop = bp if final_stop is None else max(final_stop, bp)
                    else:
                        final_stop = bp if final_stop is None else min(final_stop, bp)
                except Exception:
                    pass

            # === 6) 原有写库：仅在需要时写 entry_atr / stop_price ===
            sets = []
            params = []
            if entry_atr0 is None and entry_atr is not None:
                sets.append("entry_atr=?")
                params.append(float(entry_atr))
            if stop0 is None and final_stop is not None:
                sets.append("stop_price=?")
                params.append(float(final_stop))
            elif stop0 is not None and final_stop is not None:
                # 若合并后更紧，则也更新
                try:
                    s0 = float(stop0)
                    if (direction == "long" and final_stop > s0) or (direction == "short" and final_stop < s0):
                        sets.append("stop_price=?")
                        params.append(float(final_stop))
                except Exception:
                    pass

            if sets:
                params.append(int(pid))
                conn.execute(f"UPDATE positions_virtual SET {', '.join(sets)} WHERE id=?;", params)
                updated += 1
                print(f"[backfill] pid={pid} {symbol} {timeframe} dir={direction} "
                      f"atr_len={atr_len} entry_atr={entry_atr} k_sl={k_sl} "
                      f"atr_stop={atr_stop} struct_stop={struct_stop} stop->{final_stop}")

            # === 7) 新增：r0_* 五列“只写空位” ===
            # r0_stop_price —— 优先使用 init_stop（代表入场口径），其次用 ATR 止损；不使用 final_stop（避免被 BE/后续收紧污染）
            r0_stop_candidate = init_stop if init_stop is not None else atr_stop
            r0_px_dist = None
            r0_atr = None
            r0_R_usd = None

            if r0_stop_candidate is not None:
                try:
                    r0_px_dist = abs(float(entry_px) - float(r0_stop_candidate))
                except Exception:
                    r0_px_dist = None

            if entry_atr and entry_atr > 0:
                r0_atr = float(entry_atr)

            # 计算 r0_R_usd（需要 notional_usd；缺失时置为 NULL，由上层可选再补）
            try:
                if (r0_px_dist is not None) and (r0_px_dist > 0) and (notional_usd is not None) and (entry_px > 0):
                    r0_R_usd = float(notional_usd) * float(r0_px_dist) / float(entry_px)
            except Exception:
                r0_R_usd = None

            # 仅当至少有一个 r0 值可写时才执行 UPDATE；并且全部用 COALESCE，只写空位不覆盖
            if (r0_stop_candidate is not None) or (r0_px_dist is not None) or (r0_atr is not None) or (r0_R_usd is not None):
                conn.execute(
                    """
                    UPDATE positions_virtual
                    SET
                        r0_stop_price = COALESCE(r0_stop_price, ?),
                        r0_px_dist    = COALESCE(r0_px_dist,    ?),
                        r0_atr        = COALESCE(r0_atr,        ?),
                        r0_R_usd      = COALESCE(r0_R_usd,      ?),
                        r0_defined_at = COALESCE(r0_defined_at, ?)
                    WHERE id=? AND status='OPEN';
                    """,
                    (
                        None if r0_stop_candidate is None else float(r0_stop_candidate),
                        None if r0_px_dist is None else float(r0_px_dist),
                        None if r0_atr is None else float(r0_atr),
                        None if r0_R_usd is None else float(r0_R_usd),
                        _now_ts(cfg),
                        int(pid),
                    )
                )
                # 打印一行 r0 回填日志（不影响原有 backfill 打印）
                try:
                    print(f"[r0_backfill] id={pid} {symbol} {timeframe} dir={direction} "
                          f"entry={entry_px:.6f} r0_stop={r0_stop_candidate} r0_dist={r0_px_dist} r0_R={r0_R_usd}")
                except Exception:
                    pass

        return {"ok": True, "updated": updated}

    except Exception as e:
        print(f"[backfill] error: {e}")
        return {"ok": False, "error": str(e)}


# === REPLACE THIS WHOLE DEF IN exit_daemon.py ===

def _apply_partial_tps_if_hit(
    conn: sqlite3.Connection,
    pos: dict,
    ctx: dict,
    cfg: dict,
) -> dict:
    """
    分批止盈（R 口径统一 + 中文标签 + 比例）：
      - 改动：R_den 优先使用 r0_px_dist（锁定分母）；ctx 无则尝试 DB 列 r0_px_dist；
              仍无则回退 max(|entry-stop|, k*entry_ATR, k*ATR_now)。
      - 其余与原版一致：命中 levels_R[tp_stage] -> 按 shares[tp_stage] 减仓；同根链式控制；exit_log 记录等。
    """
    import json
    from datetime import datetime

    try:
        pid        = int(pos.get("rowid") or pos.get("id") or 0)
        symbol     = str(pos.get("symbol") or "")
        timeframe  = str(pos.get("timeframe") or "1h")
        direction  = (str(pos.get("side") or pos.get("direction") or "")).lower()
        entry_px   = float(pos.get("entry_price") or 0.0)
        if pid <= 0 or direction not in ("long", "short") or entry_px <= 0:
            return {"ok": True, "triggered": False}

        # --- 配置 ---
        core_cfg = cfg.get("core", {})
        exit_cfg = core_cfg.get("exit", cfg.get("exit", {}))
        partials = exit_cfg.get("partials", {}) or {}
        levels_R = list(partials.get("levels_R", [1.5, 2.5, 4.0]))
        shares   = list(partials.get("shares",   [0.4, 0.3, 0.3]))
        allow_chain = bool(partials.get("allow_chain_same_bar", True))
        chain_cap   = int(partials.get("chain_max_per_bar", 2))

        L = min(len(levels_R), len(shares))
        if L == 0:
            return {"ok": True, "triggered": False}
        levels_R = levels_R[:L]; shares = shares[:L]

        # --- 当前仓位（以 DB 为准） ---
        row = conn.execute(
            "SELECT tp_stage, qty, notional_usd, stop_price, entry_atr FROM positions_virtual WHERE id=?;",
            (pid,),
        ).fetchone()
        if not row:
            return {"ok": True, "triggered": False}
        tp_stage      = int(row[0] or 0)
        cur_qty_db    = float(row[1] or 0.0)
        notional_db   = float(row[2] or 0.0)
        stop_price_db = None if row[3] is None else float(row[3])
        entry_atr_db  = None if row[4] is None else float(row[4])
        if tp_stage >= L or cur_qty_db <= 0:
            return {"ok": True, "triggered": False}

        # --- 同根链式控制（保持不变） ---
        t_ref = str(ctx.get("t_ref") or ctx.get("t_bar") or "")
        if t_ref:
            logs = conn.execute(
                "SELECT action, COUNT(*) FROM exit_log WHERE position_id=? AND t_ref=? GROUP BY action;",
                (pid, t_ref),
            ).fetchall()
            action_map = {str(a): int(c) for a, c in (logs or [])}
            non_tp_cnt = sum(c for a, c in action_map.items() if a != "TP")
            tp_cnt     = int(action_map.get("TP", 0))
            if non_tp_cnt > 0:
                return {"ok": True, "triggered": False}
            cap = max(1, chain_cap) if allow_chain else 1
            if tp_cnt >= cap:
                return {"ok": True, "triggered": False}

        # --- R_now 计算（分母锁定口径） ---
        regime  = (str(ctx.get("regime") or "ranging")).lower()
        atr_now = ctx.get("atr_now")
        if atr_now is None: atr_now = ctx.get("ATR14")
        if atr_now is None: atr_now = ctx.get("atr")
        atr_now = float(atr_now) if atr_now is not None else None

        # k 映射（仅用于回退口径）
        k_map = (exit_cfg.get("sl", {}) or {}).get("atr_mult", {"trending": 1.5, "ranging": 1.8})
        k_sl  = float(k_map.get("trending" if regime == "trending" else "ranging", 1.8))

        # 1) 优先：ctx.r0_px_dist
        R_den = 0.0
        r0_px_dist = ctx.get("r0_px_dist")
        if r0_px_dist is not None:
            try:
                r0_px_dist = float(r0_px_dist)
            except Exception:
                r0_px_dist = None

        # 2) DB 回退：positions_virtual.r0_px_dist（老库无列时忽略错误）
        if (r0_px_dist is None) or not (r0_px_dist > 0):
            try:
                row_r0 = conn.execute("SELECT r0_px_dist FROM positions_virtual WHERE id=?", (pid,)).fetchone()
                if row_r0 is not None and row_r0[0] is not None and float(row_r0[0]) > 0:
                    r0_px_dist = float(row_r0[0])
            except Exception:
                r0_px_dist = None

        # 3) 最终确定分母
        if r0_px_dist is not None and r0_px_dist > 0:
            R_den = r0_px_dist
        else:
            # 原有三候选回退
            dist = []
            if stop_price_db is not None and stop_price_db > 0:
                dist.append(abs(entry_px - stop_price_db))
            if entry_atr_db is not None and entry_atr_db > 0:
                dist.append(entry_atr_db * k_sl)
            if atr_now is not None and atr_now > 0:
                dist.append(atr_now * k_sl)
            R_den = max(dist) if dist else 0.0

        px_now = float(ctx.get("px_now") or 0.0)
        if px_now <= 0 or R_den <= 0:
            return {"ok": True, "triggered": False}

        r_now = (px_now - entry_px) / R_den if direction == "long" else (entry_px - px_now) / R_den

        # --- 命中本档 TP 吗（保持不变） ---
        target_R = float(levels_R[tp_stage])
        if r_now < target_R:
            return {"ok": True, "triggered": False}

        # --- 执行减仓（按比例，保持不变） ---
        share = float(shares[tp_stage])
        if share <= 0 or cur_qty_db <= 0:
            return {"ok": True, "triggered": False}
        qty_close = max(0.0, cur_qty_db * share)
        if qty_close <= 0:
            return {"ok": True, "triggered": False}

        _partial_close_position_row(
            conn=conn,
            pos_row={"id": pid},
            qty=qty_close,
            price=px_now,
            reason=f"TP{tp_stage+1}",
            now_ts=str(ctx.get("t_bar") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )

        row_after = conn.execute("SELECT qty FROM positions_virtual WHERE id=?", (pid,)).fetchone()
        remain_qty = float(row_after[0] or 0.0) if row_after else max(0.0, cur_qty_db - qty_close)
        share_pct = (qty_close / max(1e-12, qty_close + remain_qty)) * 100.0

        reason_text = _fmt_exit_reason(
            "TP", share_pct,
            f"tp_stage={tp_stage+1} R_now={r_now:.2f} target={target_R:.2f} @ {px_now:.6f}"
        )
        reasons_obj = json.dumps({
            "action": "TP",
            "label_cn": "止盈-分批",
            "tp_stage": int(tp_stage + 1),
            "share_pct": float(round(share_pct, 2)),
            "qty_before": float(cur_qty_db),
            "qty_closed": float(qty_close),
            "qty_after": float(remain_qty),
            "R_now": float(round(r_now, 4)),
            "target_R": float(target_R),
            "price": float(px_now)
        }, ensure_ascii=False)

        try:
            notional_slice = notional_db * (qty_close / max(1e-12, cur_qty_db))
        except Exception:
            notional_slice = 0.0
        realized_part = _calc_realized_pnl_usd(entry_px, px_now, direction, notional_slice)

        conn.execute(
            """
            INSERT INTO exit_log(position_id, t_ref, action, qty, price, pnl_usd, reason_text, reasons_obj, created_at)
            VALUES(?, ?, 'TP', ?, ?, ?, ?, ?, ?);
            """,
            (
                pid,
                t_ref or str(ctx.get("t_bar") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                float(qty_close),
                float(px_now),
                float(realized_part),
                reason_text,
                reasons_obj,
                _now_ts(cfg),
            ),
        )

        next_stage = tp_stage + 1
        conn.execute(
            "UPDATE positions_virtual SET tp_stage=?, updated_at=? WHERE id=?;",
            (int(next_stage), str(ctx.get("t_bar") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")), pid),
        )

        return {"ok": True, "triggered": True, "tp_idx": int(next_stage), "qty_closed": float(qty_close)}

    except Exception as e:
        try:
            print(f"[tp][ERR] {symbol} {timeframe} id={pid} {e}")
        except Exception:
            pass
        return {"ok": False, "triggered": False}




def _load_pos_ctx(conn, pos: dict, cfg: dict) -> dict:
    """
    统一加载退出判断上下文的适配层（shim）：
    - 优先调用 core.exit_logic.load_pos_ctx / _load_pos_ctx（如果存在）
    - 否则使用本地 fallback（等价于你之前的实现）
    - 保证产出包含：ok, symbol, timeframe, direction, t_bar, t_ref, px_now,
      bars(升序), atr, entry_price, entry_atr, stop_price, base_R_usd, risk_R,
      t_entry, since_entry_high/low, highest/lowest_close_since_entry, regime
      以及 since_hi/since_lo（与TSL实现兼容）
    """
    # 1) 优先走你在 exit_logic.py 中的实现（若存在）
    try:
        if hasattr(exit_logic, "load_pos_ctx"):
            ctx = exit_logic.load_pos_ctx(conn, pos, cfg)
        elif hasattr(exit_logic, "_load_pos_ctx"):
            ctx = exit_logic._load_pos_ctx(conn, pos, cfg)
        else:
            ctx = None
    except Exception:
        ctx = None

    # 2) 本地 fallback（与你之前贴出来的一致，仅在“基线读取”处补充 r0_* 五列并向下兼容）
    if not ctx:
        import math
        try:
            symbol    = pos.get("symbol")
            timeframe = pos.get("timeframe") or "1h"
            direction = (pos.get("side") or pos.get("direction") or "").lower()
            pid       = int(pos.get("rowid") or pos.get("id") or 0)
            if not symbol or direction not in ("long", "short") or pid <= 0:
                return {"ok": False, "reason": "bad_position_fields"}

            # 最后一根K线
            row_last = conn.execute(
                """
                SELECT t, open, high, low, close
                FROM ohlcv
                WHERE symbol=? AND timeframe=?
                ORDER BY t DESC
                LIMIT 1
                """,
                (symbol, timeframe),
            ).fetchone()
            if not row_last:
                return {"ok": False, "reason": "no_ohlcv"}

            t_bar = str(row_last[0])
            px_now = float(row_last[4])

            # 近端缓冲（升序）
            rows = conn.execute(
                """
                SELECT open, high, low, close
                FROM ohlcv
                WHERE symbol=? AND timeframe=? AND t<=?
                ORDER BY t DESC
                LIMIT 200
                """,
                (symbol, timeframe, t_bar),
            ).fetchall()
            if not rows or len(rows) < 5:
                return {"ok": False, "reason": "insufficient_bars"}
            bars = rows[::-1]

            # 读取R基线（优先尝试带 r0_* 版本；如旧库无此列则自动回退到原查询）
            try:
                base = conn.execute(
                    """
                    SELECT
                        entry_price, entry_atr, stop_price, base_R_usd, risk_R, t_ref,
                        r0_stop_price, r0_px_dist, r0_atr, r0_R_usd, r0_defined_at
                    FROM positions_virtual
                    WHERE id=?
                    """,
                    (pid,),
                ).fetchone()
                with_r0 = True
            except sqlite3.OperationalError:
                base = conn.execute(
                    """
                    SELECT entry_price, entry_atr, stop_price, base_R_usd, risk_R, t_ref
                    FROM positions_virtual
                    WHERE id=?
                    """,
                    (pid,),
                ).fetchone()
                with_r0 = False

            if not base:
                return {"ok": False, "reason": "no_position_baseline"}

            entry_price = float(base[0] or 0.0)
            entry_atr   = float(base[1] or 0.0) if base[1] is not None else 0.0
            stop_price  = float(base[2] or 0.0) if base[2] is not None else 0.0
            base_R_usd  = float(base[3] or 0.0) if base[3] is not None else 0.0
            risk_R      = float(base[4] or 0.0) if base[4] is not None else 0.0
            t_entry     = str(base[5] or "")

            # 新增：r0_* 五个只读字段（若旧库不含，则为 None）
            r0_stop_price = float(base[6]) if with_r0 and base[6] is not None else None
            r0_px_dist    = float(base[7]) if with_r0 and base[7] is not None else None
            r0_atr0       = float(base[8]) if with_r0 and base[8] is not None else None
            r0_R_usd      = float(base[9]) if with_r0 and base[9] is not None else None
            r0_defined_at = str(base[10]) if with_r0 and base[10] is not None else None

            # ATR（Wilder风格EMA，默认14）
            def _atr_of(bseq, n=14):
                try:
                    import numpy as np
                except Exception:
                    # 纯Python兜底（简单EMA）
                    o = [float(b[0]) for b in bseq]
                    h = [float(b[1]) for b in bseq]
                    l = [float(b[2]) for b in bseq]
                    c = [float(b[3]) for b in bseq]
                    if len(c) < 2:
                        return max(1e-12, abs(px_now) * 0.001)
                    pc = c[:-1]
                    tr_list = []
                    for i in range(1, len(c)):
                        tr_list.append(max(h[i]-l[i], abs(h[i]-pc[i-1]), abs(l[i]-pc[i-1])))
                    alpha = 2.0/(n+1.0)
                    a = tr_list[0]
                    for i in range(1, len(tr_list)):
                        a = alpha*tr_list[i] + (1-alpha)*a
                    return float(a if a>0 else max(1e-12, abs(px_now)*0.001))

                o = np.array([float(b[0]) for b in bseq], dtype=float)
                h = np.array([float(b[1]) for b in bseq], dtype=float)
                l = np.array([float(b[2]) for b in bseq], dtype=float)
                c = np.array([float(b[3]) for b in bseq], dtype=float)
                if c.shape[0] < 2:
                    return max(1e-12, abs(px_now) * 0.001)
                pc = np.roll(c, 1); pc[0] = c[0]
                tr = np.maximum.reduce([h - l, np.abs(h - pc), np.abs(l - pc)])
                alpha = 2.0 / (n + 1.0)
                a = tr.astype(float)
                for i in range(1, a.shape[0]):
                    a[i] = alpha * tr[i] + (1 - alpha) * a[i-1]
                return float(a[-1])

            atr = _atr_of(bars, 14)
            if not (atr > 0):
                atr = max(1e-12, abs(px_now) * 0.001)

            # 入场以来极值（含收盘极值）
            if t_entry:
                rows_since = conn.execute(
                    """
                    SELECT open, high, low, close
                    FROM ohlcv
                    WHERE symbol=? AND timeframe=? AND t>=? AND t<=?
                    ORDER BY t ASC
                    """,
                    (symbol, timeframe, t_entry, t_bar),
                ).fetchall()
                seq = rows_since if rows_since else [bars[-1]]
            else:
                seq = [bars[-1]]
            highs  = [float(r[1]) for r in seq]
            lows   = [float(r[2]) for r in seq]
            closes = [float(r[3]) for r in seq]
            since_entry_high = max(highs) if highs else px_now
            since_entry_low  = min(lows)  if lows  else px_now
            highest_close_since_entry = max(closes) if closes else px_now
            lowest_close_since_entry  = min(closes) if closes else px_now

            # Regime：优先 BTC 对齐（缺失时默认 trending） —— 保持原有口径
            regime = "trending"
            try:
                rows_align = conn.execute(
                    """
                    SELECT align_corr
                    FROM btc_alignment_snapshot
                    WHERE symbol=? AND timeframe=? AND t_ref<=?
                    ORDER BY t_ref DESC
                    LIMIT 2
                    """,
                    (symbol, timeframe, t_bar),
                ).fetchall()
                ok_cnt = 0
                for rr in rows_align:
                    ac = rr[0]
                    if ac is None:
                        continue
                    try:
                        ac = float(ac)
                    except Exception:
                        continue
                    if direction == "long" and ac >= 0:
                        ok_cnt += 1
                    elif direction == "short" and ac <= 0:
                        ok_cnt += 1
                regime = "trending" if ok_cnt >= 2 else "ranging"
            except Exception:
                regime = "trending"

            # —— 汇总 ctx ——（仅新增 r0_* 字段，其余保持不变）
            ctx = {
                "ok": True,
                "symbol": symbol,
                "timeframe": timeframe,
                "direction": direction,
                "t_bar": t_bar,
                "px_now": px_now,
                "bars": bars,  # [(o,h,l,c), ...] 升序
                "atr": atr,
                "entry_price": entry_price,
                "entry_atr": entry_atr,
                "stop_price": stop_price,
                "base_R_usd": base_R_usd,
                "risk_R": risk_R,
                "t_entry": t_entry,
                "since_entry_high": since_entry_high,
                "since_entry_low": since_entry_low,
                "highest_close_since_entry": highest_close_since_entry,
                "lowest_close_since_entry": lowest_close_since_entry,
                "regime": regime,

                # 新增（只读）：R 分母锁定相关（旧库时为 None）
                "r0_stop_price": r0_stop_price,
                "r0_px_dist":    r0_px_dist,
                "r0_atr":        r0_atr0,
                "r0_R_usd":      r0_R_usd,
                "r0_defined_at": r0_defined_at,
            }
            # 最小必要调试
            try:
                print(f"[pos_ctx] pid={pid} {symbol} {timeframe} t={t_bar} px={px_now:.6f} "
                      f"ATR14={ctx['atr']:.6f} regime={regime} "
                      f"entry={entry_price:.6f} since_hi/lo={since_entry_high:.6f}/{since_entry_low:.6f}")
            except Exception:
                pass
        except Exception as e:
            try:
                print(f"[pos_ctx][ERR] {e}")
            except Exception:
                pass
            return {"ok": False, "reason": str(e)}

    # 3) 统一/兜底关键字段，保证后续模块一致可用
    if not ctx.get("ok"):
        return ctx
    ctx.setdefault("t_ref", ctx.get("t_bar"))  # 去重/日志按bar口径
    # 兼容另一个TSL版本里用到的别名
    ctx.setdefault("since_hi", ctx.get("since_entry_high"))
    ctx.setdefault("since_lo", ctx.get("since_entry_low"))
    return ctx


def _check_structure_break(
    conn: sqlite3.Connection,
    pos: dict,
    ctx: dict,
    cfg: dict,
) -> dict:
    """
    结构性止损（结构破位）：
      - 触发条件：ctx['struct_break'] 或 ctx['struct_break_flag'] 为 True
      - 行为：整单平仓（100%），写 exit_log（中文 reason_text + reasons_obj），并关闭 positions_virtual
      - 返回：{"ok": True, "triggered": bool}
    说明：
      - level_tag/confirm_k/vpoc/val/vah 若在 ctx 中存在则记录；不存在也不报错（写 None）
      - t_ref 优先用 ctx['t_ref'] / ctx['t_bar']，否则用 _now_ts(cfg)
    """
    import json
    from datetime import datetime

    try:
        pid        = int(pos.get("rowid") or pos.get("id") or 0)
        symbol     = str(pos.get("symbol") or "")
        timeframe  = str(pos.get("timeframe") or "1h")
        direction  = (str(pos.get("side") or pos.get("direction") or "")).lower()
        entry_px   = float(pos.get("entry_price") or 0.0)

        if pid <= 0 or direction not in ("long", "short") or entry_px <= 0:
            return {"ok": True, "triggered": False}

        # 是否判定为“结构破位”（保留你现有上游检测口径）
        struct_flag = bool(ctx.get("struct_break") or ctx.get("struct_break_flag"))
        if not struct_flag:
            return {"ok": True, "triggered": False}

        # 读取 DB 的实时仓位（以 DB 为准）
        row = conn.execute(
            "SELECT qty, notional_usd FROM positions_virtual WHERE id=?;",
            (pid,),
        ).fetchone()
        if not row:
            return {"ok": True, "triggered": False}
        cur_qty = float(row[0] or 0.0)
        notional_db = float(row[1] or 0.0)
        if cur_qty <= 0:
            return {"ok": True, "triggered": False}

        px_now = float(ctx.get("px_now") or 0.0)
        if px_now <= 0:
            return {"ok": True, "triggered": False}

        # 计算已实现盈亏（整单）
        def _pnl(entry_price: float, exit_price: float, side: str, qty: float) -> float:
            if side == "long":
                return (exit_price - entry_price) * qty
            return (entry_price - exit_price) * qty

        realized = _pnl(entry_px, px_now, direction, cur_qty)

        # 结构破位的补充信息（若无则 None）
        level_tag = ctx.get("struct_level_tag") or ctx.get("level_tag")
        confirm_k = ctx.get("struct_confirm_bars") or ctx.get("confirm_k") or 0
        vpoc = ctx.get("vpoc")
        val  = ctx.get("val")
        vah  = ctx.get("vah")

        # 时间戳 / t_ref
        t_ref = str(ctx.get("t_ref") or ctx.get("t_bar") or _now_ts(cfg))
        now_ts = _now_ts(cfg)

        # reason_text / reasons_obj —— 中文明确标签 + 结构化字段
        reason_text = _fmt_exit_reason("STRUCT", 100.0, f"level={level_tag} k={int(confirm_k)} @ {px_now:.6f}")
        reasons_obj = json.dumps({
            "action": "STRUCT",
            "label_cn": "止损-结构破位",
            "share_pct": 100.0,
            "level": level_tag,
            "confirm_bars": int(confirm_k),
            "vpoc": float(vpoc) if vpoc is not None else None,
            "val":  float(val)  if val  is not None else None,
            "vah":  float(vah)  if vah  is not None else None,
            "price": float(px_now)
        }, ensure_ascii=False)

        # 写 exit_log
        conn.execute(
            """
            INSERT INTO exit_log(position_id, t_ref, action, qty, price, pnl_usd, reason_text, reasons_obj, created_at)
            VALUES (?, ?, 'STRUCT', ?, ?, ?, ?, ?, ?)
            """,
            (pid, t_ref, float(cur_qty), float(px_now), float(realized), reason_text, reasons_obj, now_ts),
        )

        # 关闭仓位
        conn.execute(
            """
            UPDATE positions_virtual
               SET status='CLOSED',
                   closed_at=?,
                   exit_reason='struct_break',
                   exit_price=?,
                   pnl_usd=COALESCE(pnl_usd,0)+?
             WHERE id=?;
            """,
            (now_ts, float(px_now), float(realized), pid),
        )

        return {"ok": True, "triggered": True}

    except Exception as e:
        try:
            print(f"[struct_break][ERR] {symbol} {timeframe} id={pid} {e}")
        except Exception:
            pass
        return {"ok": False, "triggered": False}



def _calc_realized_pnl_usd(entry_price: float, exit_price: float, side: str, notional_usd: float) -> float:
    """
    统一口径：按入场名义金额估算已实现PnL（不额外扣手续费；BE 里已把成本计入 be_price）。
    long:  notional * (exit/entry - 1)
    short: notional * (1 - exit/entry)
    """
    try:
        ep = float(entry_price)
        xp = float(exit_price)
        noz = float(notional_usd)
        s  = (side or "").lower()
        if ep <= 0 or xp <= 0 or noz <= 0 or s not in ("long", "short"):
            return 0.0
        if s == "long":
            return noz * (xp / ep - 1.0)
        else:
            return noz * (1.0 - xp / ep)
    except Exception:
        return 0.0

def _partial_close_position_row(conn, pos_row: dict, qty: float, price: float, reason: str, now_ts: str) -> None:
    """
    部分减仓：
      - 按数量比例更新 qty / notional_usd
      - 累加 pnl_usd（使用 _calc_realized_pnl_usd，按比例分摊）
      - 不改变 status（仍为 OPEN），不写 exit_reason
    仅数据库读写 + 最小debug；exit_log 由调用方负责写。
    """
    pid   = int(pos_row.get("rowid") or pos_row.get("id") or 0)
    if pid <= 0 or qty <= 0:
        return

    # 读取当前仓位
    cur = conn.execute(
        "SELECT qty, notional_usd, pnl_usd, entry_price, direction FROM positions_virtual WHERE id=?",
        (pid,),
    ).fetchone()
    if not cur:
        return

    cur_qty      = float(cur[0] or 0.0)
    cur_notional = float(cur[1] or 0.0)
    cur_pnl      = float(cur[2] or 0.0)
    entry_price  = float(cur[3] or 0.0)
    direction    = (cur[4] or "").lower()

    if cur_qty <= 0 or cur_notional < 0:
        return

    # clamp qty
    qty_close = min(max(0.0, float(qty)), cur_qty)
    if qty_close <= 0.0:
        return

    # 比例分摊名义金额 & 已实现PnL
    share = qty_close / cur_qty if cur_qty > 0 else 0.0
    notional_slice = cur_notional * share

    realized_inc = _calc_realized_pnl_usd(
        entry_price=entry_price,
        exit_price=float(price),
        side=direction,
        notional_usd=notional_slice,
    )

    remain_qty      = cur_qty - qty_close
    remain_notional = cur_notional * (remain_qty / cur_qty) if cur_qty > 0 else 0.0
    new_pnl         = cur_pnl + realized_inc

    conn.execute(
        """
        UPDATE positions_virtual
           SET qty=?,
               notional_usd=?,
               pnl_usd=?
         WHERE id=?
        """,
        (float(remain_qty), float(remain_notional), float(new_pnl), pid),
    )

    try:
        print(f"[partial] id={pid} reason={reason} qty_close={qty_close:.6f} "
              f"price={float(price):.6f} realized={realized_inc:.2f} "
              f"remain_qty={remain_qty:.6f}")
    except Exception:
        pass



def _close_position_row(
    conn,
    rowid: int,
    exit_reason: str,
    realized_pnl_usd: float,
    exit_price: float,
    now_ts_str: str,
) -> None:
    """
    整单关闭：
      - status='CLOSED'，写 exit_reason / exit_price / closed_at
      - 累加 pnl_usd = 旧值 + realized
      - qty / notional 置0
    仅数据库操作 + 最小debug；exit_log 由调用方负责写。
    """
    r = conn.execute(
        "SELECT pnl_usd FROM positions_virtual WHERE id=?",
        (int(rowid),),
    ).fetchone()
    old_pnl = float(r[0] or 0.0) if r else 0.0
    new_pnl = old_pnl + float(realized_pnl_usd)

    conn.execute(
        """
        UPDATE positions_virtual
           SET status='CLOSED',
               exit_reason=?,
               exit_price=?,
               closed_at=?,
               pnl_usd=?,
               qty=0.0,
               notional_usd=0.0
         WHERE id=?
        """,
        (
            str(exit_reason or "exit"),
            float(exit_price),
            str(now_ts_str),
            float(new_pnl),
            int(rowid),
        ),
    )

    try:
        print(f"[close] id={int(rowid)} reason={exit_reason} "
              f"price={float(exit_price):.6f} realized={float(realized_pnl_usd):.2f}")
    except Exception:
        pass

def _process_one_position(conn, pos: dict, cfg: dict) -> dict:
    """
    单仓一轮（同一根K线口径）处理顺序：
      1) _load_pos_ctx                    -> 统一上下文
      2) _update_mfe_and_break_even       -> 先更新MFE/BE（只收紧）
      3) _apply_partial_tps_if_hit (loop) -> TP可递进多次（但只做TP，不触发别的动作）
      4) _arm_and_update_tsl              -> 只收紧，不触发则仅更新
      5) _check_structure_break           -> 触发则全平
      6) _check_time_and_context          -> half/full，触发则执行
      7) 兜底：exit_logic.decide_exit_for_position -> True则全平
    返回：{"pid": int, "symbol": str, "timeframe": str, "actions": [...]}
    """
    actions = []
    pid = int(pos.get("rowid") or pos.get("id") or 0)
    symbol = pos.get("symbol")
    timeframe = pos.get("timeframe") or "1h"

    try:
        # 1) 上下文
        ctx = _load_pos_ctx(conn, pos, cfg)
        if not ctx.get("ok"):
            return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": [{"mode":"ctx","status":"skip","reason":ctx.get("reason")}]}

        # 2) MFE/BE
        be_res = _update_mfe_and_break_even(conn, pos, ctx, cfg)
        if be_res.get("ok"):
            actions.append({"mode": "be", "armed": be_res.get("armed"), "mfe_R": be_res.get("mfe_R")})

        # 实时刷新 pos（后面TP/TSL要用最新 qty/notional/tp_stage）
        pos = conn.execute("SELECT id, symbol, timeframe, direction, entry_price, qty, notional_usd, tp_stage FROM positions_virtual WHERE id=?", (pid,)).fetchone()
        if not pos:
            return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions + [{"mode":"pos","status":"gone"}]}
        pos = {
            "id": pos[0], "symbol": pos[1], "timeframe": pos[2], "direction": pos[3],
            "entry_price": pos[4], "qty": pos[5], "notional_usd": pos[6], "tp_stage": pos[7],
        }

        # 3) TP 递进：当前根可连打多档（避免一次只吃一档）
        # 为防误循环，最多尝试 n_levels 次
        core_cfg = cfg.get("core", cfg)
        exit_cfg = core_cfg.get("exit", {}) or {}
        partials = exit_cfg.get("partials", {}) or {}
        n_levels = min(len(partials.get("levels_R") or []), len(partials.get("shares") or []))
        tp_loop_cap = max(0, int(n_levels))  # 最多尝试到n_levels次
        for _ in range(tp_loop_cap):
            tp_res = _apply_partial_tps_if_hit(conn, pos, ctx, cfg)
            if not tp_res.get("ok"):
                break
            if not tp_res.get("triggered"):
                break
            actions.append({"mode": "tp", "tp_idx": tp_res.get("tp_idx"), "qty_closed": tp_res.get("qty_closed")})
            # 刷新仓位，若已打光则直接结束
            row = conn.execute("SELECT status FROM positions_virtual WHERE id=?", (pid,)).fetchone()
            if not row:
                return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions + [{"mode":"pos","status":"gone"}]}
            if (row[0] or "").upper() == "CLOSED":
                return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions}
            # 未打光则更新 pos 以供下轮TP
            row2 = conn.execute(
                "SELECT id, symbol, timeframe, direction, entry_price, qty, notional_usd, tp_stage FROM positions_virtual WHERE id=?",
                (pid,),
            ).fetchone()
            if not row2:
                return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions + [{"mode":"pos","status":"gone"}]}
            pos.update({"qty": row2[5], "notional_usd": row2[6], "tp_stage": row2[7]})

        # 4) TSL 更新/触发
        tsl_res = _arm_and_update_tsl(conn, cfg, pos, ctx)
        if tsl_res.get("ok"):
            actions.append({"mode": "tsl", "triggered": tsl_res.get("triggered")})
            # 触发则已全平，直接结束
            if tsl_res.get("triggered"):
                return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions}

        # 5) 结构失效
        struct_res = _check_structure_break(conn, pos, ctx, cfg)
        if struct_res.get("ok"):
            actions.append({"mode": "struct", "triggered": struct_res.get("triggered"), "level": struct_res.get("level")})
            if struct_res.get("triggered"):
                return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions}

        # 6) 情境/时间
        ct_res = _check_time_and_context(conn, pos, ctx, cfg)
        if ct_res.get("ok"):
            actions.append({"mode": ct_res.get("mode", "ctx_time"), "triggered": ct_res.get("triggered"), "action": ct_res.get("action")})
            if ct_res.get("triggered"):
                return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions}

        # 7) 兜底：exit_logic（必须全平的裁决）
        try:
            from core import exit_logic
        except Exception:
            exit_logic = None

        if exit_logic and hasattr(exit_logic, "decide_exit_for_position"):
            judge = exit_logic.decide_exit_for_position(conn, pos, ctx, cfg)
            if isinstance(judge, dict) and judge.get("should_exit"):
                # 执行全平（写 exit_log 交由 decide_exit_for_position 内部或这里兜底）
                # 兜底一份：写 exit_price，保证审计完整
                entry = float(ctx.get("entry_price") or 0.0)
                px_now = float(ctx.get("px_now") or 0.0)
                notional = float(pos.get("notional_usd") or 0.0)
                direction = (pos.get("direction") or "").lower()
                realized = _calc_realized_pnl_usd(entry, px_now, direction, notional)
                _close_position_row(
                    conn=conn,
                    rowid=pid,
                    exit_reason=str(judge.get("reason") or "judge_exit"),
                    realized_pnl_usd=realized,
                    exit_price=px_now,
                    now_ts_str=str(ctx.get("t_bar")),
                )
                actions.append({"mode": "judge", "triggered": True, "reason": judge.get("reason")})
                return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions}

        # 未触发任何关闭类动作，返回链路结果
        return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions}

    except Exception as e:
        try:
            print(f"[exit_chain][ERR] {symbol} {timeframe} id={pid} {e}")
        except Exception:
            pass
        return {"pid": pid, "symbol": symbol, "timeframe": timeframe, "actions": actions + [{"mode":"error","error":str(e)}]}

def _load_open_positions(conn) -> list:
    """
    返回 OPEN 仓位的轻量字段，避免无用IO。
    """
    rows = conn.execute(
        """
        SELECT id, symbol, timeframe, direction, entry_price, qty, notional_usd, tp_stage
        FROM positions_virtual
        WHERE status='OPEN' AND qty > 0
        ORDER BY opened_at ASC
        """
    ).fetchall()
    res = []
    for r in rows:
        res.append({
            "id": r[0], "symbol": r[1], "timeframe": r[2], "direction": r[3],
            "entry_price": r[4], "qty": r[5], "notional_usd": r[6], "tp_stage": r[7],
        })
    return res


def _check_time_and_context(
    conn: sqlite3.Connection,
    pos: dict,
    ctx: dict,
    cfg: dict,
) -> dict:
    """
    时间出场（TIME）与 BTC 上下文冲突减仓（CTX：半仓/全平）。

    变更要点：
      - CTX 全平分支写入：reason_text=【风控-冲突减仓 100%】streak=... flat_thr=... @ 价格
        reasons_obj={
          action:'CTX', label_cn:'风控-冲突减仓', share_pct:100.0, streak:int, mode:'flat', price:float
        }
      - CTX 半仓分支同样写 share_pct（按本次实际减掉比例计算）。
      - TIME 分支保留（若无需求也可按需删掉），与原逻辑兼容。

    返回: {"ok": True, "time_trigger": bool, "ctx_trigger": "flat"|"half"|None}
    """
    import json
    from datetime import datetime

    def _now() -> str:
        return _now_ts(cfg)

    def _pnl(entry_price: float, exit_price: float, side: str, qty: float) -> float:
        if side == "long":
            return (exit_price - entry_price) * qty
        return (entry_price - exit_price) * qty

    try:
        pid        = int(pos.get("rowid") or pos.get("id") or 0)
        symbol     = str(pos.get("symbol") or "")
        timeframe  = str(pos.get("timeframe") or "1h")
        direction  = (str(pos.get("side") or pos.get("direction") or "")).lower()
        entry_px   = float(pos.get("entry_price") or 0.0)

        if pid <= 0 or direction not in ("long", "short") or entry_px <= 0:
            return {"ok": True, "time_trigger": False, "ctx_trigger": None}

        # ---- 配置 ----
        core_cfg = cfg.get("core", {})
        exit_cfg = core_cfg.get("exit", cfg.get("exit", {}))

        # TIME
        ts_cfg         = (exit_cfg.get("time_stop") or {})
        max_bars_map   = ts_cfg.get("max_bars_without_progress", {}) or {}
        min_mfe_R      = float(ts_cfg.get("min_mfe_R", 0.8))

        # CTX
        ctx_cfg  = (exit_cfg.get("context") or {})
        half_thr = int(ctx_cfg.get("btc_mismatch_bars_half", 2))
        flat_thr = int(ctx_cfg.get("btc_mismatch_bars_flat", 4))

        # ---- 读库（以 DB 为准）----
        row = conn.execute(
            "SELECT qty, notional_usd, stop_price, entry_atr FROM positions_virtual WHERE id=?;",
            (pid,),
        ).fetchone()
        if not row:
            return {"ok": True, "time_trigger": False, "ctx_trigger": None}
        cur_qty      = float(row[0] or 0.0)
        notional_db  = float(row[1] or 0.0)
        stop_price   = None if row[2] is None else float(row[2])
        entry_atr_db = None if row[3] is None else float(row[3])
        if cur_qty <= 0:
            return {"ok": True, "time_trigger": False, "ctx_trigger": None}

        # ---- 当前行情/上下文 ----
        px_now = float(ctx.get("px_now") or 0.0)
        if px_now <= 0:
            return {"ok": True, "time_trigger": False, "ctx_trigger": None}

        regime  = (str(ctx.get("regime") or "ranging")).lower()
        atr_now = float(ctx.get("atr") or ctx.get("atr_now") or 0.0)
        t_ref   = str(ctx.get("t_ref") or ctx.get("t_bar") or _now())
        now_ts  = _now()

        # ---- bars_since_entry ----
        bars_since_entry = None
        if ctx.get("bars_since_entry") is not None:
            try:
                bars_since_entry = int(ctx.get("bars_since_entry"))
            except Exception:
                bars_since_entry = None
        if bars_since_entry is None:
            # 用 opened_at 推算
            opened_at = pos.get("opened_at") or pos.get("created_at")
            try:
                fmt = "%Y-%m-%d %H:%M:%S"
                t_open = datetime.strptime(str(opened_at)[:19], fmt)
                t_bar  = datetime.strptime(t_ref[:19], fmt)
                minutes = max(0, int((t_bar - t_open).total_seconds() // 60))
                tf_min = 60 if timeframe == "1h" else (240 if timeframe == "4h" else 60)
                bars_since_entry = max(0, minutes // tf_min)
            except Exception:
                bars_since_entry = 0

        # ---- MFE_R（无则近似用当前 R_now）----
        mfe_R = None
        for k in ("mfe_R", "mfe_r", "MFE_R"):
            if ctx.get(k) is not None:
                try:
                    mfe_R = float(ctx.get(k))
                    break
                except Exception:
                    pass
        if mfe_R is None:
            k_map = (exit_cfg.get("sl", {}) or {}).get("atr_mult", {"trending": 1.5, "ranging": 1.8})
            k_sl  = float(k_map.get("trending" if regime == "trending" else "ranging", 1.8))
            dist = []
            if stop_price is not None and stop_price > 0:
                dist.append(abs(entry_px - stop_price))
            if entry_atr_db is not None and entry_atr_db > 0:
                dist.append(entry_atr_db * k_sl)
            if atr_now > 0:
                dist.append(atr_now * k_sl)
            risk_unit = max(dist) if dist else (k_sl * atr_now if atr_now > 0 else 0.0)
            mfe_R = ((px_now - entry_px) / risk_unit if direction == "long"
                     else (entry_px - px_now) / risk_unit) if risk_unit > 0 else 0.0

        # =================== TIME 分支（保留） ===================
        time_triggered = False
        max_bars_allowed = int(max_bars_map.get(timeframe, max_bars_map.get("default", 72)))
        if bars_since_entry >= max_bars_allowed and float(mfe_R) < float(min_mfe_R):
            realized = _pnl(entry_px, px_now, direction, cur_qty)
            reason_text = _fmt_exit_reason("TIME", 100.0, f"bars={bars_since_entry} mfe_R={float(mfe_R):.2f} @ {px_now:.6f}")
            reasons_obj = json.dumps({
                "action": "TIME",
                "label_cn": "风控-时间出场",
                "share_pct": 100.0,
                "bars_since_entry": int(bars_since_entry),
                "mfe_R": float(mfe_R),
                "price": float(px_now)
            }, ensure_ascii=False)

            conn.execute(
                """
                INSERT INTO exit_log(position_id, t_ref, action, qty, price, pnl_usd, reason_text, reasons_obj, created_at)
                VALUES (?, ?, 'TIME', ?, ?, ?, ?, ?, ?)
                """,
                (pid, t_ref, float(cur_qty), float(px_now), float(realized), reason_text, reasons_obj, now_ts),
            )
            conn.execute(
                """
                UPDATE positions_virtual
                   SET status='CLOSED',
                       closed_at=?,
                       exit_reason='time_decay',
                       exit_price=?,
                       pnl_usd=COALESCE(pnl_usd,0)+?
                 WHERE id=?;
                """,
                (now_ts, float(px_now), float(realized), pid),
            )
            time_triggered = True

        # =================== CTX 分支（半仓 / 全平） ===================
        ctx_trigger = None
        mismatch_streak = None
        for k in ("btc_mismatch_streak", "ctx_conflict_streak", "mismatch_streak"):
            if ctx.get(k) is not None:
                try:
                    mismatch_streak = int(ctx.get(k))
                    break
                except Exception:
                    pass

        if mismatch_streak is not None and cur_qty > 0:
            if mismatch_streak >= flat_thr:
                # ---- 全平（FLAT）----
                realized = _pnl(entry_px, px_now, direction, cur_qty)

                reason_text = _fmt_exit_reason(
                    "CTX", 100.0,
                    f"streak={mismatch_streak} flat_thr={flat_thr} @ {px_now:.6f}"
                )
                reasons_obj = json.dumps({
                    "action": "CTX",
                    "label_cn": "风控-冲突减仓",
                    "share_pct": 100.0,
                    "streak": int(mismatch_streak),
                    "mode": "flat",
                    "price": float(px_now)
                }, ensure_ascii=False)

                conn.execute(
                    """
                    INSERT INTO exit_log(position_id, t_ref, action, qty, price, pnl_usd, reason_text, reasons_obj, created_at)
                    VALUES (?, ?, 'CTX', ?, ?, ?, ?, ?, ?)
                    """,
                    (pid, t_ref, float(cur_qty), float(px_now), float(realized), reason_text, reasons_obj, now_ts),
                )
                conn.execute(
                    """
                    UPDATE positions_virtual
                       SET status='CLOSED',
                           closed_at=?,
                           exit_reason='ctx_conflict_flat',
                           exit_price=?,
                           pnl_usd=COALESCE(pnl_usd,0)+?
                     WHERE id=?;
                    """,
                    (now_ts, float(px_now), float(realized), pid),
                )
                ctx_trigger = "flat"

            elif mismatch_streak >= half_thr:
                # ---- 半仓（HALF）----
                qty_close = max(0.0, cur_qty * 0.5)
                if qty_close > 0:
                    share_pct = (qty_close / max(1e-12, cur_qty)) * 100.0

                    reason_text = _fmt_exit_reason(
                        "CTX", share_pct,
                        f"streak={mismatch_streak} half_thr={half_thr} @ {px_now:.6f}"
                    )
                    reasons_obj = json.dumps({
                        "action": "CTX",
                        "label_cn": "风控-冲突减仓",
                        "share_pct": float(round(share_pct, 2)),
                        "streak": int(mismatch_streak),
                        "mode": "half",
                        "qty_closed": float(qty_close),
                        "price": float(px_now)
                    }, ensure_ascii=False)

                    conn.execute(
                        """
                        INSERT INTO exit_log(position_id, t_ref, action, qty, price, pnl_usd, reason_text, reasons_obj, created_at)
                        VALUES (?, ?, 'CTX', ?, ?, 0.0, ?, ?, ?)
                        """,
                        (pid, t_ref, float(qty_close), float(px_now), reason_text, reasons_obj, now_ts),
                    )
                    remain = max(0.0, cur_qty - qty_close)
                    conn.execute(
                        "UPDATE positions_virtual SET qty=?, notional_usd=?, updated_at=? WHERE id=?;",
                        (float(remain), float(remain * px_now), now_ts, pid),
                    )
                    ctx_trigger = "half"

        return {"ok": True, "time_trigger": bool(time_triggered), "ctx_trigger": ctx_trigger}

    except Exception as e:
        try:
            print(f"[time_ctx][ERR] {symbol} {timeframe} id={pid} {e}")
        except Exception:
            pass
        return {"ok": False, "time_trigger": False, "ctx_trigger": None}



def _call_exit_logic_adapter(
    conn,
    pos: dict,
    now_ts: str,
    cfg: dict,
    dd_kill_flag: bool,
    current_price: float,
) -> dict:
    """
    统一调用 exit_logic.decide_exit_for_position 的适配器：
    - 优先从 core.exit_logic 导入
    - 若签名与标准(6参)不同，则用反射“按能接受的参数”调用
    - 永远返回 dict，失败时返回 should_exit=False
    """
    try:
        try:
            from core import exit_logic as _xl
        except Exception:
            import exit_logic as _xl
    except Exception as e:
        print(f"[fallback] import exit_logic failed: {e}")
        return {"should_exit": False, "reason": None, "detail": {"note": "import_failed"}}

    # 反射签名，自适应不同历史版本
    try:
        import inspect
        fn = getattr(_xl, "decide_exit_for_position")
        sig = inspect.signature(fn)
        params = sig.parameters

        # 优先用关键字方式按签名喂参，尽量不触发位置参数错误
        call_kwargs = {}
        if "conn" in params:
            call_kwargs["conn"] = conn
        if "pos_row" in params:
            call_kwargs["pos_row"] = pos
        elif "pos" in params:
            call_kwargs["pos"] = pos
        if "now_ts" in params:
            call_kwargs["now_ts"] = now_ts
        if "cfg" in params:
            call_kwargs["cfg"] = cfg
        if "dd_kill_flag" in params:
            call_kwargs["dd_kill_flag"] = dd_kill_flag
        if "current_price" in params:
            call_kwargs["current_price"] = float(current_price)

        # 调用
        return fn(**call_kwargs)
    except TypeError as e:
        # 最后兜底：尽量用较老的最少参数版本（极少见）
        print(f"[fallback] signature mismatch, try minimal call: {e}")
        try:
            return fn(pos, now_ts, cfg)  # 老三参版本兜底
        except Exception as e2:
            print(f"[fallback] minimal call failed: {e2}")
            return {"should_exit": False, "reason": None, "detail": {"note": "signature_mismatch"}}
    except Exception as e:
        print(f"[fallback] call exit_logic failed: {e}")
        return {"should_exit": False, "reason": None, "detail": {"note": "call_failed"}}


def run_exit_cycle(conn: sqlite3.Connection, cfg: dict) -> Dict[str, Any]:
    """
    退出执行主循环（只做决策与写库，不发通知）。
    顺序：DD-KILL -> BE/MFE -> TP -> TSL -> STRUCT -> CTX/TIME -> [可选] 旧兜底
    返回的 closed[] 每条包含通知所需字段：rowid/id, symbol, direction/side, exit_reason, entry_price, exit_price, pnl_usd
    """
    # 先定义 errors，避免 try/except 中先引用未定义
    errors: List[str] = []

    # 在拉 open_positions 之前或之后都可；推荐之前
    try:
        _ = _backfill_r_baseline_for_open_positions(conn, cfg)
    except Exception as _e:
        errors.append(f"backfill:{_e}")

    # 是否启用 legacy 兜底（默认关闭；如需启用，config.yml: exit.legacy_fallback: true）
    use_legacy_fallback = bool(cfg.get("exit", {}).get("legacy_fallback", False))

    # --- 账户级风控与 dd_kill ---
    try:
        try:
            from core import risk_monitor as _rm
        except Exception:
            import risk_monitor as _rm
        rm_res = _rm.check_drawdown_and_update_snapshot(conn, cfg, t_ref=None)
        dd_kill_flag = bool(rm_res.get("dd_kill", False))
    except Exception as e:
        rm_res = {"dd_kill": False}
        dd_kill_flag = False
        errors.append(f"risk_monitor:{e}")

    # --- now_ts ---
    try:
        now_ts = _now_ts(cfg)
    except Exception:
        from datetime import datetime
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- 拉 OPEN 仓位 ---
    try:
        if '_fetch_open_positions' in globals() and callable(globals()['_fetch_open_positions']):
            open_positions = _fetch_open_positions(conn)
        elif '_load_open_positions' in globals() and callable(globals()['_load_open_positions']):
            open_positions = _load_open_positions(conn)
        else:
            rows = conn.execute(
                """
                SELECT id, symbol, timeframe, direction, entry_price, qty, notional_usd, tp_stage
                FROM positions_virtual
                WHERE status='OPEN' AND qty > 0
                ORDER BY opened_at ASC
                """
            ).fetchall()
            open_positions = [
                {
                    "id": r[0], "symbol": r[1], "timeframe": r[2], "direction": r[3],
                    "entry_price": r[4], "qty": r[5], "notional_usd": r[6], "tp_stage": r[7],
                }
                for r in rows
            ]
    except Exception as e:
        errors.append(f"fetch_open_positions:{e}")
        open_positions = []

    closed_list: List[dict] = []
    kept_list: List[dict] = []

    # --- 主循环 ---
    for pos in open_positions:
        pid = int(pos.get("rowid") or pos.get("id") or 0)
        symbol = pos.get("symbol")
        direction = (pos.get("side") or pos.get("direction") or "").lower()
        timeframe = pos.get("timeframe") or "1h"
        entry_price = float(pos.get("entry_price") or 0.0)
        notional_usd = float(pos.get("notional_usd") or 0.0)

        # A) dd_kill 最高优先：直接全平
        if dd_kill_flag:
            ctx = _load_pos_ctx(conn, pos, cfg)
            if not ctx.get("ok"):
                kept_list.append({"rowid": pid, "symbol": symbol, "reason": "ctx_unavailable_dd"})
                continue
            px_now = float(ctx.get("px_now") or 0.0)
            realized_pnl_usd = _calc_realized_pnl_usd(entry_price, px_now, direction, notional_usd)
            _close_position_row(conn, pid, "dd_kill", realized_pnl_usd, px_now, now_ts)
            closed_list.append({
                "rowid": pid,
                "symbol": symbol,
                "direction": direction,
                "timeframe": timeframe,
                "exit_reason": "dd_kill",
                "entry_price": entry_price,
                "exit_price": px_now,
                "pnl_usd": realized_pnl_usd,
            })
            continue

        # B) 正常路径：加载上下文（含 px_now/ATR/regime）
        ctx = _load_pos_ctx(conn, pos, cfg)
        if not ctx.get("ok"):
            kept_list.append({"rowid": pid, "symbol": symbol, "reason": "ctx_unavailable"})
            continue

        # 1) 更新 MFE / Break-even（只收紧）
        try:
            be_res = _update_mfe_and_break_even(conn, pos, ctx, cfg)
            if be_res.get("armed"):
                print(f"[BE] pid={pid} armed be_price={be_res.get('be_price')}")
        except Exception as e:
            errors.append(f"be:{symbol}:{e}")

        # 2) 分批止盈（命中则本根不再连环触发）
        try:
            tp_res = _apply_partial_tps_if_hit(conn, pos, ctx, cfg)
        except Exception as e:
            tp_res = {"ok": False}
            errors.append(f"tp:{symbol}:{e}")

        if tp_res.get("ok") and tp_res.get("triggered"):
            row = conn.execute("SELECT status, exit_price, pnl_usd FROM positions_virtual WHERE id=?", (pid,)).fetchone()
            if row and str(row[0]).upper() == "CLOSED":
                px_exit = float(row[1] or ctx.get("px_now") or 0.0)
                pnl_exit = float(row[2] or _calc_realized_pnl_usd(entry_price, px_exit, direction, notional_usd))
                closed_list.append({
                    "rowid": pid,
                    "symbol": symbol,
                    "direction": direction,
                    "timeframe": timeframe,
                    "exit_reason": f"tp{int(tp_res.get('tp_idx', -1))+1}",
                    "entry_price": entry_price,
                    "exit_price": px_exit,
                    "pnl_usd": pnl_exit,
                })
            else:
                kept_list.append({"rowid": pid, "symbol": symbol, "reason": "tp_partial"})
            continue  # 本根已执行过 TP，本根不再连环触发

        # 3) 追踪止损（只抬不放；触发可能全平）
        try:
            tsl_res = _arm_and_update_tsl(conn, cfg, pos, ctx)
            if tsl_res.get("armed") or tsl_res.get("raised"):
                print(f"[TSL] pid={pid} armed={tsl_res.get('armed')} price={tsl_res.get('tsl_price')}")
        except Exception as e:
            tsl_res = {"ok": False}
            errors.append(f"tsl:{symbol}:{e}")

        if tsl_res.get("ok") and tsl_res.get("triggered"):
            px_now = float(ctx.get("px_now") or 0.0)
            pnl = _calc_realized_pnl_usd(entry_price, px_now, direction, notional_usd)
            closed_list.append({
                "rowid": pid,
                "symbol": symbol,
                "direction": direction,
                "timeframe": timeframe,
                "exit_reason": "tsl",
                "entry_price": entry_price,
                "exit_price": px_now,
                "pnl_usd": pnl,
            })
            continue

        # 4) 结构失效（VPOC/VAL/VAH反穿或低低/高高+放量）
        try:
            st_res = _check_structure_break(conn, pos, ctx, cfg)
            if st_res.get("checked"):
                print(f"[STRUCT] pid={pid} trigger={st_res.get('triggered')}")
        except Exception as e:
            st_res = {"ok": False}
            errors.append(f"struct:{symbol}:{e}")

        if st_res.get("ok") and st_res.get("triggered"):
            px_now = float(ctx.get("px_now") or 0.0)
            pnl = _calc_realized_pnl_usd(entry_price, px_now, direction, notional_usd)
            closed_list.append({
                "rowid": pid,
                "symbol": symbol,
                "direction": direction,
                "timeframe": timeframe,
                "exit_reason": "struct_break",
                "entry_price": entry_price,
                "exit_price": px_now,
                "pnl_usd": pnl,
            })
            continue

        # 5) 情境/时间（half 不进 closed；full 进 closed）
        try:
            ct_res = _check_time_and_context(conn, pos, ctx, cfg)
            if ct_res.get("checked"):
                print(f"[CTX/TIME] pid={pid} mode={ct_res.get('mode')} action={ct_res.get('action')} triggered={ct_res.get('triggered')}")
        except Exception as e:
            ct_res = {"ok": False}
            errors.append(f"ctx_time:{symbol}:{e}")

        if ct_res.get("ok") and ct_res.get("triggered"):
            px_now = float(ctx.get("px_now") or 0.0)
            if ct_res.get("action") == "half":
                kept_list.append({"rowid": pid, "symbol": symbol, "reason": "ctx_half"})
            else:
                reason = ct_res.get("mode") or "ctx"
                pnl = _calc_realized_pnl_usd(entry_price, px_now, direction, notional_usd)
                closed_list.append({
                    "rowid": pid,
                    "symbol": symbol,
                    "direction": direction,
                    "timeframe": timeframe,
                    "exit_reason": reason,
                    "entry_price": entry_price,
                    "exit_price": px_now,
                    "pnl_usd": pnl,
                })
            continue

        # 6) 旧兜底（可选；默认关闭；建议先不用）
        if use_legacy_fallback:
            try:
                # 统一用适配器（需要你已添加 _call_exit_logic_adapter）
                print(f"[fallback] call exit_logic: {symbol} tf={timeframe} px_now={ctx.get('px_now')} dd_kill={dd_kill_flag}")
                verdict = _call_exit_logic_adapter(
                    conn=conn,
                    pos=pos,
                    now_ts=now_ts,
                    cfg=cfg,
                    dd_kill_flag=dd_kill_flag,
                    current_price=float(ctx.get("px_now") or 0.0),
                )
                if bool(verdict.get("should_exit", False)):
                    ctx_px = float(ctx.get("px_now") or 0.0)
                    pnl = _calc_realized_pnl_usd(entry_price, ctx_px, direction, notional_usd)
                    reason = verdict.get("reason") or "exit"
                    _close_position_row(conn, pid, reason, pnl, ctx_px, now_ts)
                    closed_list.append({
                        "rowid": pid,
                        "symbol": symbol,
                        "direction": direction,
                        "timeframe": timeframe,
                        "exit_reason": reason,
                        "entry_price": entry_price,
                        "exit_price": ctx_px,
                        "pnl_usd": pnl,
                    })
                else:
                    kept_list.append({"rowid": pid, "symbol": symbol, "reason": "keep"})
            except Exception as e:
                errors.append(f"fallback_suppressed:{symbol}:{e}")
                kept_list.append({"rowid": pid, "symbol": symbol, "reason": "keep"})
        else:
            kept_list.append({"rowid": pid, "symbol": symbol, "reason": "keep"})

    # --- 心跳（不中断主流程） ---
    try:
        _write_ops_heartbeat(
            conn=conn,
            t_ref=now_ts,
            rows_written=len(closed_list),
            alerts=[],   # 通知统一在 run_exit_cycle.py 脚本里发
            errors=errors,
        )
    except Exception:
        pass

    return {
        "t_ref": now_ts,
        "dd_kill": dd_kill_flag,
        "closed": closed_list,
        "kept": kept_list,
        "errors": errors,
        "equity_snapshot": {
            "equity_usd": rm_res.get("equity_usd"),
            "peak_equity_usd": rm_res.get("peak_equity_usd"),
            "drawdown_pct": rm_res.get("drawdown_pct"),
            "t_ref": rm_res.get("t_ref"),
        },
    }




def _arm_and_update_tsl(
    conn: sqlite3.Connection,
    pos: dict,
    ctx: dict,
    cfg: dict,
) -> dict:
    """
    移动止损（TSL）武装与更新，并在触发时整单平仓。
    - 仅在满足 arm_tsl_after_tp_idx 条件后武装；
    - 价格朝盈利方向推进时，TSL 只收紧不放松；
    - 触发条件：
        long  -> px_now <= tsl_price
        short -> px_now >= tsl_price
    - 触发后写 exit_log(action='TSL')，reason_text 使用中文标签并包含 100% 比例，reasons_obj 写结构化字段。
    返回：
      {"ok": True, "armed": bool, "updated": bool, "triggered": bool, "tsl_price": float|None}
    """
    import json
    from datetime import datetime

    def _now() -> str:
        return _now_ts(cfg)

    def _pnl(entry_price: float, exit_price: float, side: str, qty: float) -> float:
        if side == "long":
            return (exit_price - entry_price) * qty
        return (entry_price - exit_price) * qty

    try:
        pid        = int(pos.get("rowid") or pos.get("id") or 0)
        symbol     = str(pos.get("symbol") or "")
        timeframe  = str(pos.get("timeframe") or "1h")
        direction  = (str(pos.get("side") or pos.get("direction") or "")).lower()
        entry_px   = float(pos.get("entry_price") or 0.0)
        if pid <= 0 or direction not in ("long", "short") or entry_px <= 0:
            return {"ok": True, "armed": False, "updated": False, "triggered": False, "tsl_price": None}

        # ---- 读配置：TSL/分阶段/武装门 ----
        core_cfg = cfg.get("core", {})
        exit_cfg = core_cfg.get("exit", cfg.get("exit", {}))

        tsl_cfg = (exit_cfg.get("tsl") or {})
        method  = str(tsl_cfg.get("method", "chandelier")).lower()

        atr_mult_map = (tsl_cfg.get("atr_mult") or {"trending": 1.5, "ranging": 1.8})
        atr_len_map  = (tsl_cfg.get("atr_len")  or {"trending": 14,  "ranging": 10})

        stage_factor = (exit_cfg.get("tsl_stage_factor") or
                        {"after_tp0": 1.0, "after_tp1": 0.8, "after_tp2": 0.6})

        arm_after_idx = int((exit_cfg.get("partials") or {}).get("arm_tsl_after_tp_idx", 0))
        # 说明：-1 表示全程可武装；0 表示 TP1 后可武装；以此类推

        # ---- 读库为准：数量/已达 TP 阶段/已有 tsl_price ----
        row = conn.execute(
            "SELECT qty, tp_stage, tsl_price FROM positions_virtual WHERE id=?;",
            (pid,),
        ).fetchone()
        if not row:
            return {"ok": True, "armed": False, "updated": False, "triggered": False, "tsl_price": None}
        cur_qty   = float(row[0] or 0.0)
        tp_stage  = int(row[1] or 0)
        tsl_old   = None if row[2] is None else float(row[2])
        if cur_qty <= 0:
            return {"ok": True, "armed": False, "updated": False, "triggered": False, "tsl_price": tsl_old}

        # ---- 是否允许武装 ----
        allow_arm = True if arm_after_idx < 0 else (tp_stage > arm_after_idx)
        if not allow_arm:
            return {"ok": True, "armed": False, "updated": False, "triggered": False, "tsl_price": tsl_old}

        # ---- 计算新 TSL 水平（Chandelier）----
        if method != "chandelier":
            # 仅支持 chandelier；其他方法原样返回（不报错）
            return {"ok": True, "armed": True, "updated": False, "triggered": False, "tsl_price": tsl_old}

        regime  = (str(ctx.get("regime") or "ranging")).lower()
        atr_now = float(ctx.get("atr") or ctx.get("atr_now") or 0.0)
        px_now  = float(ctx.get("px_now") or 0.0)
        if px_now <= 0 or atr_now <= 0:
            return {"ok": True, "armed": True, "updated": False, "triggered": False, "tsl_price": tsl_old}

        base_mult = float(atr_mult_map.get("trending" if regime == "trending" else "ranging", 1.8))
        # 分阶段系数：tp_stage=0/1/2 -> after_tp0/after_tp1/after_tp2
        if tp_stage >= 2:
            stage_k = float(stage_factor.get("after_tp2", 0.6))
        elif tp_stage >= 1:
            stage_k = float(stage_factor.get("after_tp1", 0.8))
        else:
            stage_k = float(stage_factor.get("after_tp0", 1.0))

        eff_mult = max(0.1, base_mult * stage_k)

        # 获取极值（最高高/最低低）；若上游未给，保守用 px_now 代替
        hh = (ctx.get("chandelier_hi") or ctx.get("highest_high") or ctx.get("hh") or ctx.get("Hh"))
        ll = (ctx.get("chandelier_lo") or ctx.get("lowest_low")  or ctx.get("ll") or ctx.get("Ll"))
        try:
            hh = float(hh) if hh is not None else float(px_now)
        except Exception:
            hh = float(px_now)
        try:
            ll = float(ll) if ll is not None else float(px_now)
        except Exception:
            ll = float(px_now)

        tsl_new = None
        if direction == "long":
            tsl_new = hh - eff_mult * atr_now
            # 只收紧：long 取更高的 stop
            if tsl_old is not None:
                tsl_new = max(tsl_old, tsl_new)
        else:
            tsl_new = ll + eff_mult * atr_now
            # 只收紧：short 取更低的 stop
            if tsl_old is not None:
                tsl_new = min(tsl_old, tsl_new)

        # ---- 回写 tsl_price（若有变化）----
        updated = False
        if tsl_new is not None and (tsl_old is None or abs(tsl_new - tsl_old) > 1e-12):
            conn.execute(
                "UPDATE positions_virtual SET tsl_price=?, updated_at=? WHERE id=?;",
                (float(tsl_new), _now(), pid),
            )
            updated = True

        # ---- 触发判定 ----
        triggered = False
        if tsl_new is not None:
            if (direction == "long"  and px_now <= tsl_new) or \
               (direction == "short" and px_now >= tsl_new):
                # 触发：整单平仓
                realized = _pnl(entry_px, px_now, direction, cur_qty)

                reason_text = _fmt_exit_reason("TSL", 100.0, f"@ {px_now:.6f}")
                reasons_obj = json.dumps({
                    "action": "TSL",
                    "label_cn": "止损-移动止损",
                    "share_pct": 100.0,
                    "price": float(px_now),
                    "tsl_price": float(tsl_new),
                    "tp_stage": int(tp_stage),
                    "eff_mult": float(round(eff_mult, 4)),
                    "atr": float(round(atr_now, 8)),
                    "regime": regime
                }, ensure_ascii=False)

                conn.execute(
                    """
                    INSERT INTO exit_log(position_id, t_ref, action, qty, price, pnl_usd, reason_text, reasons_obj)
                    VALUES (?, ?, 'TSL', ?, ?, ?, ?, ?)
                    """,
                    (pid, _now(), float(cur_qty), float(px_now), float(realized), reason_text, reasons_obj),
                )
                conn.execute(
                    """
                    UPDATE positions_virtual
                       SET status='CLOSED',
                           closed_at=?,
                           exit_reason='tsl_trigger',
                           exit_price=?,
                           pnl_usd=COALESCE(pnl_usd,0)+?
                     WHERE id=?;
                    """,
                    (_now(), float(px_now), float(realized), pid),
                )
                triggered = True

        return {"ok": True, "armed": True, "updated": bool(updated), "triggered": bool(triggered), "tsl_price": float(tsl_new or 0.0)}

    except Exception as e:
        try:
            print(f"[tsl][ERR] {symbol} {timeframe} id={pid} {e}")
        except Exception:
            pass
        return {"ok": False, "armed": False, "updated": False, "triggered": False, "tsl_price": None}




def build_exit_push_cards(conn: sqlite3.Connection, since_hours: int = 48, limit: int = 200) -> list:
    """
    读取 exit_log + positions_virtual，生成“中文明确标签 + 比例”的推送卡片文本。
    - since_hours: 回看多少小时（默认48小时）
    - limit      : 最多返回多少条
    返回: [ { "t_ref": "...", "position_id": int, "action": "...",
             "symbol":"...", "timeframe":"...", "share_pct": float|None,
             "price": float, "qty": float, "pnl_usd": float|None,
             "text": "【止盈-分批 30%】SUI/USDT 1h @2.182100  TP2 R=0.88/0.60" } ... ]
    用法示例（在 run_exit_cycle.py 里）:
        cards = build_exit_push_cards(conn, 48)
        for c in cards:
            send_fn(c["text"])  # 你的 Telegram/钉钉发送函数
    """
    import json
    try:
        sql = """
        SELECT
            e.t_ref       AS t_ref,
            e.position_id AS position_id,
            e.action      AS action,
            e.reason_text AS reason_text,
            e.reasons_obj AS reasons_obj,
            e.price       AS price,
            e.qty         AS qty,
            e.pnl_usd     AS pnl_usd,
            p.symbol      AS symbol,
            p.timeframe   AS timeframe,
            p.direction   AS direction
        FROM exit_log e
        JOIN positions_virtual p ON p.id = e.position_id
        WHERE e.t_ref >= datetime('now','localtime', ?)
        ORDER BY e.t_ref DESC, e.id DESC
        LIMIT ?
        """
        cur = conn.execute(sql, (f'-{int(since_hours)} hours', int(limit)))
        rows = cur.fetchall()
    except Exception as e:
        try:
            print(f"[push_cards][ERR] read exit_log: {e}")
        except Exception:
            pass
        return []

    def _label_cn_from_action(action: str) -> str:
        mapping = {
            "TP": "止盈-分批",
            "TSL": "止损-移动止损",
            "STRUCT": "止损-结构破位",
            "CTX": "风控-冲突减仓",
            "TIME": "风控-时间出场",
        }
        return mapping.get((action or "").upper(), action or "?")

    cards = []
    for row in rows:
        # 兼容 tuple / sqlite3.Row
        t_ref        = row[0]
        position_id  = row[1]
        action       = row[2]
        reason_text  = row[3]
        reasons_obj  = row[4]
        price        = row[5]
        qty          = row[6]
        pnl_usd      = row[7]
        symbol       = row[8]
        timeframe    = row[9]
        direction    = row[10]

        share_pct = None
        label_cn  = None
        extra     = None
        if reasons_obj:
            try:
                robj = json.loads(reasons_obj)
                label_cn = robj.get("label_cn") or None
                sp = robj.get("share_pct")
                if sp is not None:
                    try:
                        share_pct = float(sp)
                    except Exception:
                        share_pct = None
                # 补充信息（可选）
                if action == "TP":
                    tp_stage = robj.get("tp_stage")
                    R_now    = robj.get("R_now")
                    target_R = robj.get("target_R")
                    if tp_stage is not None and R_now is not None and target_R is not None:
                        extra = f"TP{int(tp_stage)} R={float(R_now):.2f}/{float(target_R):.2f}"
                elif action == "CTX":
                    mode   = robj.get("mode")
                    streak = robj.get("streak")
                    if mode and streak is not None:
                        extra = f"{mode} streak={int(streak)}"
                elif action == "TIME":
                    bars_since_entry = robj.get("bars_since_entry")
                    mfe_R            = robj.get("mfe_R")
                    if bars_since_entry is not None and mfe_R is not None:
                        extra = f"bars={int(bars_since_entry)} MFE={float(mfe_R):.2f}"
            except Exception:
                pass

        if label_cn is None:
            label_cn = _label_cn_from_action(action)

        # 文案：优先 reason_text；没有就自组
        if reason_text and reason_text.strip():
            text = reason_text.strip()
            # 若没有“【】”前缀，则补一个标准头
            if "【" not in text:
                pct_part = (f" {share_pct:.0f}%" if share_pct is not None else "")
                text = f"【{label_cn}{pct_part}】{symbol} {timeframe} @{float(price):.6f}  " + text
            else:
                text = f"{text}  {symbol} {timeframe} @{float(price):.6f}"
        else:
            pct_part = (f"{share_pct:.0f}%" if share_pct is not None else "")
            head = f"【{label_cn} {pct_part}】".strip()
            text = f"{head}{symbol} {timeframe} @{float(price):.6f}"
            if extra:
                text += f"  {extra}"

        cards.append({
            "t_ref": str(t_ref),
            "position_id": int(position_id),
            "action": str(action),
            "symbol": str(symbol),
            "timeframe": str(timeframe),
            "share_pct": (None if share_pct is None else float(round(share_pct, 2))),
            "price": float(price),
            "qty": float(qty or 0.0),
            "pnl_usd": None if pnl_usd is None else float(pnl_usd),
            "text": text,
        })
    return cards
