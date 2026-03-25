#autoscan.py
import asyncio
import json
import logging
import os

from app.config import (
    AUTO_QTY,
    AUTOSCAN,
    AUTOTRADE,
    CANDIDATE_MULTIPLIER,
    FINAL_CANDIDATES_PATH,
    LOG_UNIVERSE,
    SIGNAL_LOG_PATH,
    SUMMARY_NOTIFS,
    UNIVERSE_ROWS,
)
from app.core.autoscan_owned import (
    advance_long_exit_state,
    build_owned_decision_state,
    build_owned_review_row,
    classify_exit_pressure,
    decide_long_exit,
    resolve_owned_input,
)
from app.core.autoscan_scan import (
    available_replacements,
    build_analysis_cache,
    candidate_bucket,
    candidate_sort_key_factory,
    is_affordable,
    replacement_bucket,
    replacement_is_meaningfully_better,
    should_rotate_candidate,
)
from app.core.autoscan_shared import (
    build_decision_snapshot,
    build_pipeline_analysis,
    dedupe_keep_order,
    fmt_price,
    fmt_score_plain,
    is_material_change,
    normalize_stock,
    now_utc,
    to_float,
)
from app.core.autoscan_state import (
    ensure_state_defaults,
    has_recent_order_key,
    increment_day_counter,
    is_excluded,
    is_global_trade_cooldown,
    is_in_cooldown,
    mark_global_trade_timestamp,
    mark_trade_timestamp,
    note_scan_pass,
    remember_order_key,
    scan_pass_count,
    set_exclude_minutes,
    state_counter,
    store_owned_snapshot,
    total_bucket_count,
)

from app.core.helpers import (
    get_market_session_info,
    is_dup,
    kill_switch_ok,
    market_status_text_sv,
)

from app.core.logview import (
    _BLUE,
    _CYAN,
    _GREEN,
    _RED,
    _YELLOW,
    _c,
    debug_log,
    fmt_sym_list,
    log_section,
    log_signal_line,
    short_reason_line,
)
from app.core.pipeline import run_pipeline
from app.core.pretrade import validate_pretrade_buy
from app.core.signals import execute_order

from app.core.storage_utils import (
    append_event,
    save_cycle_journal,
    save_daily_report,
    save_daily_snapshot,
    save_portfolio_review,
)

from app.core.universe_manager import (
    get_decision_state,
    get_exit_state,
    load_state,
    reset_symbol_rotation_state,
    rotate_universe,
    save_state,
    set_decision_state,
    set_exit_state,
    update_signal_state,
)

log = logging.getLogger("autoscan")


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "on", "yes", "y"}


def _env_int(key: str, default: int) -> int:
    try:
        raw = os.getenv(key, str(default))
        if "|" in raw:
            raw = raw.split("|")[0]
        return int(raw)
    except Exception:
        return default


def trim_jsonl(path: str, keep_last: int = 5000):
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    if len(lines) > keep_last:
        with p.open("w", encoding="utf-8") as f:
            f.writelines(lines[-keep_last:])


async def _trade_has_fill(trade, wait_sec: float = 8.0, poll_sec: float = 0.25) -> bool:
    if not trade:
        return False

    loops = max(1, int(wait_sec / poll_sec))

    for _ in range(loops):
        try:
            status = str(trade.orderStatus.status or "").lower()
        except Exception:
            status = ""

        try:
            filled = float(trade.orderStatus.filled or 0)
        except Exception:
            filled = 0.0

        if filled > 0:
            return True

        if status in {"filled"}:
            return True

        if status in {"cancelled", "inactive", "api cancelled"}:
            return False

        await asyncio.sleep(poll_sec)

    try:
        return float(trade.orderStatus.filled or 0) > 0
    except Exception:
        return False


async def _execute_order_safe(
    ib_client,
    raw: dict,
    side: str,
    qty: int,
    bot=None,
    chat_id=None,
    quote=None,
):
    try:
        return await execute_order(
            ib_client,
            raw,
            side,
            qty=qty,
            bot=bot,
            chat_id=chat_id,
            quote=quote,
        )
    except TypeError as e:
        # Bakåtkompatibel fallback om execute_order ännu inte fått quote-parametern
        if "unexpected keyword argument 'quote'" not in str(e):
            raise
        return await execute_order(
            ib_client,
            raw,
            side,
            qty=qty,
            bot=bot,
            chat_id=chat_id,
        )


def _group_symbols(rows: list[dict], held_only: bool = False) -> dict:
    grouped = {
        "buy_ready": [],
        "exit_ready": [],
        "sell_candidate": [],
        "exit_watch": [],
        "watch": [],
        "hold": [],
        "held": [],
        "review": [],
    }

    for row in rows or []:
        sym = row.get("symbol")
        action = str(row.get("action") or "").strip().lower()
        held_pos = float(row.get("held_position") or 0.0)
        owned_label = str(row.get("owned_label") or "").strip().upper()

        if held_only and held_pos == 0:
            continue

        if held_only:
            if owned_label == "EXIT":
                grouped["exit_ready"].append(sym)
            elif owned_label == "EXIT SOON":
                grouped["sell_candidate"].append(sym)
            elif owned_label == "EXIT WATCH":
                grouped["exit_watch"].append(sym)
            elif owned_label == "CHECK":
                grouped["review"].append(sym)
            elif action == "buy_ready":
                grouped["buy_ready"].append(sym)
            elif action == "watch":
                grouped["watch"].append(sym)
            else:
                grouped["hold"].append(sym)
            continue

        if action == "buy_ready":
            grouped["buy_ready"].append(sym)
        elif action == "exit_ready":
            if held_pos > 0:
                grouped["exit_ready"].append(sym)
            else:
                grouped["review"].append(sym)
        elif action == "sell_candidate":
            if held_pos > 0:
                grouped["sell_candidate"].append(sym)
            else:
                grouped["review"].append(sym)
        elif action == "exit_watch":
            if held_pos > 0:
                grouped["exit_watch"].append(sym)
            else:
                grouped["review"].append(sym)
        elif action == "watch":
            grouped["watch"].append(sym)
        elif action == "hold_candidate":
            grouped["hold"].append(sym)
        elif action == "hold_position":
            grouped["held"].append(sym)
        elif action == "review_needed":
            grouped["review"].append(sym)
        else:
            if held_pos > 0:
                grouped["held"].append(sym)
            else:
                grouped["hold"].append(sym)

    return grouped


def _apply_symbol_state(
    state: dict,
    sym: str,
    curr_decision: dict,
    effective_signal: str,
    removed_this_pass: set[str] | None = None,
    update_signal: bool = True,
):
    if removed_this_pass and sym in removed_this_pass:
        return

    set_decision_state(state, sym, curr_decision)

    if update_signal:
        update_signal_state(state, sym, effective_signal)


def _build_by_sym(universe: list[dict]) -> dict:
    by_sym = {}

    for s in universe or []:
        sym = (s.get("symbol") or "").upper().strip()
        if not sym:
            continue

        stock = dict(s.get("stock") or {})
        stock["symbol"] = sym
        stock["name"] = s.get("name") or stock.get("name") or sym

        stock["_pipeline_candidate_score"] = s.get("candidate_score", s.get("final_score", 0))
        stock["_pipeline_entry_score"] = s.get("entry_score", 0)
        stock["_pipeline_candidate_quality"] = s.get("candidate_quality", "C")
        stock["_pipeline_setup_type"] = s.get("setup_type", "unknown")
        stock["_pipeline_timing_state"] = s.get("timing_state", "unknown")
        stock["_pipeline_action"] = s.get("action", "watch")
        stock["_pipeline_positive_flags"] = s.get("positive_flags") or []
        stock["_pipeline_risk_flags"] = s.get("risk_flags") or []
        stock["_pipeline_entry_reasons"] = s.get("entry_reasons") or []
        stock["_pipeline_retention_score"] = s.get("retention_score", s.get("final_score", 0))
        stock["_pipeline_replacement_score"] = s.get("replacement_score", s.get("final_score", 0))
        stock["_pipeline_rank"] = s.get("rank")

        # Vanliga alias-fält för autoscan_scan.py bucket-funktioner
        stock["signal"] = s.get("signal")
        stock["final_score"] = s.get("final_score", 0)

        stock["candidate_score"] = s.get("candidate_score", s.get("final_score", 0))
        stock["entry_score"] = s.get("entry_score", 0)
        stock["candidate_quality"] = s.get("candidate_quality", "C")
        stock["setup_type"] = s.get("setup_type", "unknown")
        stock["timing_state"] = s.get("timing_state", "unknown")
        stock["action"] = s.get("action", "watch")
        stock["positive_flags"] = s.get("positive_flags") or []
        stock["risk_flags"] = s.get("risk_flags") or []
        stock["entry_reasons"] = s.get("entry_reasons") or []
        stock["retention_score"] = s.get("retention_score", s.get("final_score", 0))
        stock["replacement_score"] = s.get("replacement_score", s.get("final_score", 0))
        stock["rank"] = s.get("rank")

        pipeline_technicals = s.get("technicals") or {}

        stock["_pipeline_technicals"] = pipeline_technicals
        stock["technicals"] = pipeline_technicals
        stock["scores"] = s.get("scores") or {}
        stock["score_details"] = s.get("score_details") or {}

        if not to_float(stock.get("latestClose"), None):
            stock["latestClose"] = to_float(
                pipeline_technicals.get("price"),
                0.0,
            )

        by_sym[sym] = stock

    return by_sym


async def run_autoscan_once(bot, ib_client, admin_chat_id: int):
    autoscan_enabled = AUTOSCAN
    autotrade_enabled = AUTOTRADE
    universe_rows = UNIVERSE_ROWS
    candidate_mult = max(1, CANDIDATE_MULTIPLIER)
    auto_qty = AUTO_QTY
    summary_notifs = SUMMARY_NOTIFS
    log_universe = LOG_UNIVERSE

    sim_market = os.getenv("SIM_MARKET", "0").strip().lower() in {"1", "true", "yes", "on"}
    if sim_market:
        autotrade_enabled = False
        log.warning("[autoscan][SIM] Fake market mode active - AUTOTRADE forced OFF")

    entry_mode = os.getenv("ENTRY_MODE", "buy_only").strip().lower()
    only_trade_on_signal_change = _env_bool("ONLY_TRADE_ON_SIGNAL_CHANGE", True)
    cooldown_min = _env_int("COOLDOWN_MIN", 30)
    max_pos_per_symbol = _env_int("MAX_POS_PER_SYMBOL", 0)

    max_buys_per_day = _env_int("MAX_BUYS_PER_DAY", 1)
    max_new_entries_per_pass = _env_int("MAX_NEW_ENTRIES_PER_PASS", 2)
    max_sells_per_day = _env_int("MAX_SELLS_PER_DAY", 2)
    max_total_open_positions = _env_int("MAX_TOTAL_OPEN_POSITIONS", 6)
    max_new_entries_per_day_total = _env_int("MAX_NEW_ENTRIES_PER_DAY_TOTAL", 6)
    min_minutes_between_global_buys = _env_int("MIN_MINUTES_BETWEEN_GLOBAL_BUYS", 2)
    min_scan_passes_before_buy = _env_int("MIN_SCAN_PASSES_BEFORE_BUY", 2)
    persist_order_key_ttl_sec = _env_int("PERSIST_ORDER_KEY_TTL_SEC", 600)
    pass_ex_min = _env_int("PASS_EXCLUDE_MINUTES", _env_int("ASS_EXCLUDE_MINUTES", 20))
    exclude_bought_min = _env_int("EXCLUDE_BOUGHT_MIN", 120)
    drop_if_hold_streak = _env_int("DROP_IF_HOLD_STREAK", 6)
    min_rotation_pool_size = _env_int("MIN_ROTATION_POOL_SIZE", 3)
    max_order_value = to_float(os.getenv("MAX_ORDER_VALUE_USD", "30"), 30.0)

    entries_this_pass = 0

    log.info(
        "CFG UNIVERSE_ROWS=%s CAND_MULT=%s AUTOTRADE=%s ENTRY_MODE=%s PASS_EX_MIN=%s EXCLUDE_BOUGHT_MIN=%s MAX_ORDER_VALUE=%s",
        universe_rows,
        candidate_mult,
        autotrade_enabled,
        entry_mode,
        pass_ex_min,
        exclude_bought_min,
        max_order_value,
    )

    if not autoscan_enabled:
        return

    if not ib_client or not ib_client.ib.isConnected():
        if admin_chat_id and summary_notifs:
            await bot.send_message(admin_chat_id, "IBKR not connected - skipping autoscan.")
        log.warning("IB not connected - autoscan aborted.")
        return

    try:
        pipeline_snapshot = await run_pipeline(ib_client)
    except Exception as e:
        log.error("[autoscan] Failed to run pipeline: %s", e)
        return

    try:
        with open(FINAL_CANDIDATES_PATH, "r", encoding="utf-8") as f:
            universe = json.load(f)
    except Exception as e:
        log.error("[autoscan] Failed to read %s: %s", FINAL_CANDIDATES_PATH, e)
        return

    positions = await ib_client.ib.reqPositionsAsync()
    held = {
        (p.contract.symbol or "").upper(): float(p.position or 0.0)
        for p in positions
        if abs(float(p.position or 0.0)) >= 1
    }

    try:
        await ib_client.ib.reqOpenOrdersAsync()

        open_buy_syms = {
            (t.contract.symbol or "").upper()
            for t in ib_client.ib.openTrades()
            if (t.order.action or "").upper() == "BUY"
            and (t.orderStatus.status or "").lower() in {
                "presubmitted",
                "submitted",
                "pendingsubmit",
            }
        }

        open_sell_syms = {
            (t.contract.symbol or "").upper()
            for t in ib_client.ib.openTrades()
            if (t.order.action or "").upper() == "SELL"
            and (t.orderStatus.status or "").lower() in {
                "presubmitted",
                "submitted",
                "pendingsubmit",
            }
        }
    except Exception:
        open_buy_syms = set()
        open_sell_syms = set()

    state = ensure_state_defaults(load_state())
    today = now_utc().date().isoformat()

    by_sym = _build_by_sym(universe)
    analysis_cache = build_analysis_cache(by_sym)
    candidate_sort_key = candidate_sort_key_factory(by_sym)

    debug_log(log, "[autoscan] FINAL symbols=%s", sorted(list(by_sym.keys())))

    risk_ok, risk_reason = kill_switch_ok(
        getattr(ib_client, "pnl_realized_today", 0.0),
        getattr(ib_client, "pnl_unrealized_open", 0.0),
    )
    market_info = get_market_session_info()
    market_ok = bool(market_info["market_open"])
    regular_market_ok = market_info["phase"] == "regular"
    sim_mode = sim_market

    log.info("[market] %s", market_status_text_sv())

    debug_log(log, "[autoscan] market_ok=%s risk_ok=%s risk_reason=%s", market_ok, risk_ok, risk_reason)

    added = []
    removed = []
    removed_this_pass = set()

    def _long_position_count() -> int:
        held_long = {sym for sym, pos in held.items() if float(pos or 0.0) > 0}
        return len(held_long | set(open_buy_syms))


    def _upsert_portfolio_review(row: dict):
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            return

        for idx, existing in enumerate(portfolio_reviews):
            if str(existing.get("symbol") or "").upper().strip() == symbol:
                portfolio_reviews[idx] = row
                return

        portfolio_reviews.append(row)


    def _remove_portfolio_review(sym: str):
        sym = str(sym).upper().strip()
        portfolio_reviews[:] = [
            row for row in portfolio_reviews
            if str(row.get("symbol") or "").upper().strip() != sym
        ]


    def _sync_local_fill_state(sym: str, side: str, qty: int):
        sym = str(sym).upper().strip()
        side = str(side).strip()
        qty = int(qty or 0)
        if not sym or qty <= 0:
            return 0.0

        current = float(held.get(sym, 0.0) or 0.0)

        if side == "Köp":
            current += qty
            if abs(current) < 1e-9:
                held.pop(sym, None)
                current = 0.0
            else:
                held[sym] = current
            open_buy_syms.discard(sym)
            mark_global_trade_timestamp(state, "buy")
        else:
            current -= qty
            if current <= 0:
                held.pop(sym, None)
                current = 0.0
            else:
                held[sym] = current
            open_sell_syms.discard(sym)
            mark_global_trade_timestamp(state, "sell")

        return current

    orders_buy = 0
    orders_sell = 0
    paper_buy = 0
    paper_sell = 0
    paper_symbols = []

    rotations_out = []
    rotations_in = []

    orders_for_report = []
    owned_orders_for_report = []
    scan_results = []
    portfolio_reviews = []

    # =========================
    # PORTFOLIO REVIEW / OWNED ENGINE
    # =========================
    owned_sell_now = 0
    owned_sell_watch = 0
    owned_sell_soon = 0
    owned_checked = 0

    for sym, current_pos in list(held.items()):
        if float(current_pos or 0.0) == 0:
            continue

        raw = by_sym.get(sym) or {"symbol": sym, "name": sym}
        analysis = resolve_owned_input(sym, by_sym, state)
        missing_from_pipeline = bool(analysis.get("missing_from_pipeline"))
        action = str(analysis.get("action") or "").strip().lower()

        exit_state = get_exit_state(state, sym)
        effective_signal = "Håll"
        exit_mode = "hold"
        owned_reason = "healthy"
        pressure = "healthy"

        if current_pos > 0:
            if missing_from_pipeline:
                pressure = "healthy"
                effective_signal = "Håll"
                exit_mode = "hold"
                owned_reason = "missing_from_pipeline_neutral"
            else:
                pressure = classify_exit_pressure(analysis, current_pos)
                exit_state = advance_long_exit_state(exit_state, analysis, pressure)
                decision, owned_reason = decide_long_exit(exit_state, pressure)

                if decision == "full_exit":
                    effective_signal = "Sälj"
                    exit_mode = "full"
                elif decision == "soft_exit":
                    effective_signal = "Sälj"
                    exit_mode = "soft"
                elif decision == "watch_exit":
                    effective_signal = "Håll"
                    exit_mode = "watch"
                else:
                    effective_signal = "Håll"
                    exit_mode = "hold"

        elif current_pos < 0:
            if action in {"buy_ready", "watch"}:
                effective_signal = "Köp"
                exit_mode = "cover"
                owned_reason = "short_cover_signal"
            else:
                effective_signal = "Håll"
                exit_mode = "hold_short"
                owned_reason = "hold_short"

        if not missing_from_pipeline:
            set_exit_state(state, sym, exit_state)

        prev_decision = get_decision_state(state, sym)
        curr_decision, state_label, material_change, changed_fields = build_owned_decision_state(
            prev_decision=prev_decision,
            effective_signal=effective_signal,
            analysis=analysis,
            pressure=pressure,
            exit_mode=exit_mode,
            exit_state=exit_state,
        )

        owned_row = build_owned_review_row(
            sym=sym,
            raw=raw,
            analysis=analysis,
            current_pos=current_pos,
            effective_signal=effective_signal,
            exit_mode=exit_mode,
            owned_reason=owned_reason,
            exit_state=exit_state,
        )
        owned_row["state_label"] = state_label
        owned_row["changed_fields"] = changed_fields
        portfolio_reviews.append(owned_row)
        store_owned_snapshot(state, owned_row)

        owned_label = owned_row.get("owned_label", "HOLD")
        display_qty = int(abs(current_pos)) if current_pos > 0 else auto_qty

        show_owned_row = owned_label in {"EXIT", "EXIT SOON", "EXIT WATCH", "CHECK"} or material_change

        if owned_label == "EXIT":
            owned_sell_now += 1
        elif owned_label == "EXIT SOON":
            owned_sell_soon += 1
        elif owned_label == "EXIT WATCH":
            owned_sell_watch += 1
        elif owned_label == "CHECK":
            owned_checked += 1

        if show_owned_row:
            log_signal_line(
                log,
                label=owned_label,
                sym=sym,
                qty=display_qty,
                price=(analysis.get("raw_technicals") or {}).get("price"),
                score=analysis.get("total_score"),
            )

        if current_pos > 0 and (
            pressure != "healthy"
            or exit_mode in {"full", "soft", "watch"}
            or action in {"exit_ready", "sell_candidate", "exit_watch", "review_needed"}
        ):
            debug_log(
                log,
                "[OWNED-STATE] %s | pressure=%s | stage=%s | bearish_count=%s | soft_exit_done=%s | reason=%s | source=%s",
                sym,
                pressure,
                exit_state.get("stage"),
                exit_state.get("bearish_count"),
                exit_state.get("soft_exit_done"),
                owned_reason,
                analysis.get("data_source"),
            )

        if material_change:
            append_event(
                "owned_position_review",
                symbol=sym,
                name=raw.get("name") or raw.get("companyName") or sym,
                data={
                    "action": action,
                    "signal": effective_signal,
                    "held_position": current_pos,
                    "timing_state": analysis.get("timing_state"),
                    "quality": analysis.get("candidate_quality"),
                    "score": analysis.get("total_score"),
                    "exit_mode": exit_mode,
                    "exit_stage": exit_state.get("stage"),
                    "exit_reason": owned_reason,
                    "state_label": state_label,
                    "changed_fields": changed_fields,
                    "data_source": analysis.get("data_source"),
                    "missing_from_pipeline": missing_from_pipeline,
                },
            )

        persist_signal = effective_signal not in {"Sälj", "Köp"}

        if effective_signal not in {"Sälj", "Köp"}:
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=True,
            )
            continue

        sells_today_rec = state_counter(state, "sells_today", sym, today)
        buys_today_rec = state_counter(state, "buys_today", sym, today)

        if effective_signal == "Sälj" and max_sells_per_day > 0 and sells_today_rec["count"] >= max_sells_per_day:
            log.info("[OWNED-SKIP] %s → MAX_SELLS_PER_DAY reached", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        if effective_signal == "Köp" and max_buys_per_day > 0 and buys_today_rec["count"] >= max_buys_per_day:
            log.info("[OWNED-SKIP] %s → MAX_BUYS_PER_DAY reached", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        if is_in_cooldown(state, sym, cooldown_min):
            log.info("[OWNED-SKIP] %s → cooldown", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        if effective_signal == "Sälj" and sym in open_sell_syms:
            log.info("[OWNED-SKIP] %s → open sell order already exists", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        if exit_mode == "full":
            qty = int(abs(current_pos))
        elif exit_mode == "soft":
            if abs(current_pos) < 2:
                log.info("[OWNED-SKIP] %s → soft exit skipped, position too small", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue
            qty = max(1, int(abs(current_pos) / 2))
        elif exit_mode == "cover":
            qty = int(abs(current_pos))

        if qty <= 0:
            log.info("[OWNED-SKIP] %s → no manageable position", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        if effective_signal == "Sälj":
            today_bucket = "sells_today"
            event_name = "owned_sell_submitted"
            report_label = "SELL"
        else:
            today_bucket = "buys_today"
            event_name = "owned_cover_submitted"
            report_label = "BUY"

        if autotrade_enabled and risk_ok and market_ok:
            order_side = effective_signal
            key = f"{sym}:OWNED_{order_side}:{int(qty)}"

            if is_dup(key) or has_recent_order_key(state, key, persist_order_key_ttl_sec):
                log.info("[OWNED-SKIP] %s → duplicate order key", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            trade = None
            try:
                trade = await _execute_order_safe(
                    ib_client,
                    raw,
                    order_side,
                    qty=qty,
                    bot=bot,
                    chat_id=admin_chat_id,
                )
            except Exception as e:
                log.error("[OWNED-ORDER-ERR] %s → %s", sym, e)

            if trade:
                remember_order_key(state, key)
                filled_ok = await _trade_has_fill(trade)

                if filled_ok:
                    persist_signal = True

                    if order_side == "Sälj":
                        orders_sell += 1
                        increment_day_counter(state, today_bucket, sym, today)
                        if exit_mode == "soft" and not missing_from_pipeline:
                            exit_state["soft_exit_done"] = True
                            set_exit_state(state, sym, exit_state)
                    else:
                        orders_buy += 1
                        increment_day_counter(state, today_bucket, sym, today)

                    mark_trade_timestamp(state, sym)
                    set_exclude_minutes(state, sym, exclude_bought_min)
                    new_pos = _sync_local_fill_state(sym, order_side, qty)

                    if order_side == "Sälj" and new_pos <= 0:
                        _remove_portfolio_review(sym)

                    owned_orders_for_report.append(
                        f"OWNED {report_label} submitted: {sym} x{qty} "
                        f"(exit_mode={exit_mode}, reason={owned_reason}, stage={exit_state.get('stage')}, source={analysis.get('data_source')})"
                    )

                    append_event(
                        event_name,
                        symbol=sym,
                        name=raw.get("name") or raw.get("companyName") or sym,
                        data={
                            "qty": qty,
                            "exit_mode": exit_mode,
                            "exit_reason": owned_reason,
                            "exit_stage": exit_state.get("stage"),
                            "data_source": analysis.get("data_source"),
                        },
                    )
                else:
                    log.info("[OWNED-KEEP] %s → order submitted but not filled yet", sym)
            else:
                debug_log(log, "[OWNED-KEEP] %s → no real order sent", sym)

        else:
            why = []
            if not autotrade_enabled:
                why.append("AUTOTRADE=off")
            if not risk_ok:
                why.append(f"risk:{risk_reason}")
            if not market_ok:
                why.append("market_closed")

            paper_tag = "OWNED-PAPER-SELL" if effective_signal == "Sälj" else "OWNED-PAPER-BUY"

            log.info(
                "[%s] %s x%s | price %s | score %s | action=%s | exit_mode=%s | reason=%s | stage=%s | source=%s",
                paper_tag,
                sym,
                qty,
                fmt_price((analysis.get("raw_technicals") or {}).get("price")),
                fmt_score_plain(analysis.get("total_score")),
                action,
                exit_mode,
                owned_reason,
                exit_state.get("stage"),
                analysis.get("data_source"),
            )

            owned_orders_for_report.append(
                f"OWNED PAPER-{effective_signal.upper()}: would {effective_signal.lower()} {sym} x{qty} "
                f"(action={action}, timing={analysis.get('timing_state')}, quality={analysis.get('candidate_quality')}, "
                f"score={analysis.get('total_score')}, exit_mode={exit_mode}, exit_reason={owned_reason}, "
                f"exit_stage={exit_state.get('stage')}, source={analysis.get('data_source')}, {','.join(why) or '-'})"
            )

            append_event(
                "owned_paper_order",
                symbol=sym,
                name=raw.get("name") or raw.get("companyName") or sym,
                data={
                    "qty": qty,
                    "side": effective_signal,
                    "action": action,
                    "timing_state": analysis.get("timing_state"),
                    "quality": analysis.get("candidate_quality"),
                    "score": analysis.get("total_score"),
                    "exit_mode": exit_mode,
                    "exit_reason": owned_reason,
                    "exit_stage": exit_state.get("stage"),
                    "reason": ",".join(why) or "-",
                    "data_source": analysis.get("data_source"),
                },
            )

        _apply_symbol_state(
            state,
            sym,
            curr_decision,
            effective_signal,
            removed_this_pass,
            update_signal=persist_signal,
        )

    # =========================
    # SCAN CANDIDATES
    # =========================
    all_candidates = [
        s for s in by_sym.keys()
        if s not in held and s not in open_buy_syms and not is_excluded(state, s)
    ]

    entry_candidates = []
    watch_candidates = []
    fallback_candidates = []
    replacement_candidates = []

    for sym in all_candidates:
        stock = by_sym.get(sym) or {}

        bucket = candidate_bucket(stock)
        repl_bucket = replacement_bucket(stock)

        if bucket == "entry":
            entry_candidates.append(sym)
        elif bucket == "watch":
            watch_candidates.append(sym)
        elif bucket == "fallback":
            fallback_candidates.append(sym)

        if repl_bucket is not None:
            replacement_candidates.append(sym)

    entry_candidates = sorted(entry_candidates, key=candidate_sort_key, reverse=True)
    watch_candidates = sorted(watch_candidates, key=candidate_sort_key, reverse=True)
    fallback_candidates = sorted(fallback_candidates, key=candidate_sort_key, reverse=True)
    replacement_candidates = sorted(replacement_candidates, key=candidate_sort_key, reverse=True)

    tradable_entry_candidates = [
        s for s in entry_candidates
        if is_affordable(by_sym.get(s) or {}, auto_qty, max_order_value)
    ]

    non_tradable_entry_candidates = [
        s for s in entry_candidates if s not in tradable_entry_candidates
    ]

    log.info(
        "[autoscan] CANDIDATE FILTER | all=%d | entry=%d | tradable_entry=%d | non_tradable_entry=%d | watch=%d | fallback=%d",
        len(all_candidates),
        len(entry_candidates),
        len(tradable_entry_candidates),
        len(non_tradable_entry_candidates),
        len(watch_candidates),
        len(fallback_candidates),
    )

    for s in non_tradable_entry_candidates[:20]:
        stock = by_sym.get(s) or {}
        price = to_float(stock.get("latestClose"), 0) or to_float(
            ((stock.get("_pipeline_technicals") or {}).get("price")),
            0,
        )
        est_value = (price or 0) * auto_qty
        log.info(
            "[autoscan] ENTRY BLOCKED | %s | price=%.2f | qty=%s | est=%.2f | max=%.2f",
            s,
            price or 0.0,
            auto_qty,
            est_value,
            max_order_value,
        )

    for s in non_tradable_entry_candidates[:10]:
        stock = by_sym.get(s) or {}
        price = to_float(stock.get("latestClose"), 0) or to_float(
            ((stock.get("_pipeline_technicals") or {}).get("price")),
            0,
        )
        est_value = (price or 0) * auto_qty
        debug_log(
            log,
            "[ENTRY-SKIP] %s → not affordable | price=%.2f qty=%s est=%.2f max=%.2f",
            s,
            price or 0.0,
            auto_qty,
            est_value,
            max_order_value,
        )

    candidate_source = dedupe_keep_order(
        tradable_entry_candidates
        + watch_candidates
        + fallback_candidates
    )

    if not candidate_source and all_candidates:
        log.warning(
            "[autoscan] candidate_source became empty after affordability/filtering. "
            "Falling back to raw entry/watch/fallback candidates."
        )
        candidate_source = dedupe_keep_order(
            entry_candidates
            + watch_candidates
            + fallback_candidates
        )
            
  

    all_candidates = dedupe_keep_order(all_candidates)

    scan_seed = candidate_source[:universe_rows]

    replacement_source = dedupe_keep_order(
        [s for s in replacement_candidates if s not in scan_seed]
    )

    debug_log(
        log,
        "[autoscan] FILTERED candidates | all=%d | entry=%d | tradable_entry=%d | watch=%d | fallback=%d | selected=%d | replacement_candidates=%d",
        len(all_candidates),
        len(entry_candidates),
        len(tradable_entry_candidates),
        len(watch_candidates),
        len(fallback_candidates),
        len(candidate_source),
        len(replacement_source),
    )

    prev_uni = [s.upper() for s in state.get("universe", []) if s]

    if not candidate_source:
        log.warning(
            "[autoscan] candidate_source empty - keeping previous universe this pass"
        )
        scan_set = dedupe_keep_order(
            [s for s in prev_uni if s in by_sym]
        )[:universe_rows]
        dropped_pre = []
        added_pre = []
    else:
        scan_set, dropped_pre, added_pre = rotate_universe(prev_uni, candidate_source, state)
        scan_set = dedupe_keep_order(scan_set)[:universe_rows]

    log.info(
    "[autoscan] ROTATE RESULT | prev=%d | candidate_source=%d | scan_after_rotate=%d | dropped_pre=%d | added_pre=%d",
        len(prev_uni),
        len(candidate_source),
        len(scan_set),
        len(dropped_pre),
        len(added_pre),
    )

    for sym in dropped_pre:
        if sym not in removed:
            removed.append(sym)

    for sym in added_pre:
        if sym not in added:
            added.append(sym)

    def _available_replacements(current_scan, banned=None):
        pool, reason_counts = available_replacements(
            current_scan=current_scan,
            replacement_source=replacement_source,
            by_sym=by_sym,
            analysis_cache=analysis_cache,
            held=held,
            open_buy_syms=open_buy_syms,
            is_excluded_fn=lambda s: is_excluded(state, s),
            banned=banned or set(),
        )
        debug_log(log, "[REPL-DEBUG] %s", reason_counts)
        return pool

    def _take_replacement(current_scan, banned=None):
        pool = _available_replacements(current_scan, banned=banned)
        if not pool:
            return None
        return pool[0]

    def _fill_scan_set(current_scan, banned=None):
        banned = banned or set()
        current_scan = dedupe_keep_order(current_scan)
        while len(current_scan) < universe_rows:
            repl = _take_replacement(current_scan, banned=banned)
            if not repl:
                break
            current_scan.append(repl)
        return dedupe_keep_order(current_scan)[:universe_rows]

    scan_set = _fill_scan_set(scan_set, banned=removed_this_pass)
    note_scan_pass(state, scan_set)
    state["universe"] = list(scan_set)

    replacement_pool_size = len(_available_replacements(scan_set, banned=removed_this_pass))

    if dropped_pre:
        debug_log(log, "[PRE-REMOVE] %s", ", ".join(dropped_pre))
    if added_pre:
        debug_log(log, "[PRE-ADD] %s", ", ".join(added_pre))

    rows_for_log = []
    for sym in scan_set:
        raw = by_sym.get(sym) or {}
        tech_price = ((raw.get("_pipeline_technicals") or {}).get("price"))
        shown_price = tech_price if tech_price is not None else raw.get("latestClose")
        analysis = analysis_cache.get(sym) or build_pipeline_analysis(raw)
        rows_for_log.append(f"{sym} {fmt_price(shown_price)} [{analysis.get('candidate_quality')}]")

    log.info(
        "%s scan=%d | replacements=%d | mode=%s | market=%s | autotrade=%s",
        _c("RUN:", _CYAN, bold=True),
        len(scan_set),
        replacement_pool_size,
        "SIM" if sim_mode else "LIVE",
        "TRADABLE" if market_ok else "CLOSED",
        "ON" if autotrade_enabled else "OFF",
    )
    log.info("%s %s", _c("SCAN SET:", _CYAN, bold=True), ", ".join(rows_for_log))

    initial_scan = list(scan_set)

    for sym in initial_scan:
        if sym not in scan_set:
            continue

        raw = by_sym.get(sym) or {"symbol": sym, "name": sym}
        stock = normalize_stock(raw)

        try:
            analysis = analysis_cache.get(sym) or build_pipeline_analysis(stock)
            signal = analysis["signal"]

            score = int(analysis.get("total_score", 0) or 0)
            retention_score = int(analysis.get("retention_score", score) or 0)
            replacement_score = int(analysis.get("replacement_score", score) or 0)
            candidate_quality = analysis.get("candidate_quality", "C")
            timing_state = str(analysis.get("timing_state") or "unknown")
            action = str(analysis.get("action") or "watch")
            current_pos = float(held.get(sym, 0.0))

            effective_signal = "Håll"
            if action == "buy_ready":
                effective_signal = "Köp"
            elif action == "exit_ready":
                effective_signal = "Sälj"

            if current_pos <= 0 and action in {"sell_candidate", "exit_ready", "exit_watch", "review_needed"}:
                candidate_scan = [s for s in scan_set if s != sym]
                repl = _take_replacement(candidate_scan, banned=removed_this_pass | {sym})

                debug_log(
                    log,
                    "[FORCE-REPLACE] %s → invalid scan action without position | action=%s | score=%s | retention=%s",
                    sym,
                    action,
                    score,
                    retention_score,
                )

                if repl:
                    if sym in scan_set:
                        scan_set.remove(sym)

                    removed.append(sym)
                    removed_this_pass.add(sym)
                    set_exclude_minutes(state, sym, pass_ex_min)
                    reset_symbol_rotation_state(state, sym)

                    scan_set.append(repl)
                    scan_set = dedupe_keep_order(scan_set)[:universe_rows]
                    added.append(repl)

                    repl_raw = by_sym.get(repl) or {}
                    log.info("[ADD] %s → force-replaces %s", repl, sym)

                    rotations_out.append({
                        "symbol": sym,
                        "name": raw.get("name") or raw.get("companyName") or sym,
                        "reason": f"invalid scan action without position: {action}",
                    })

                    rotations_in.append({
                        "symbol": repl,
                        "name": repl_raw.get("name") or repl_raw.get("companyName") or repl,
                    })

                    append_event(
                        "rotation_out",
                        symbol=sym,
                        name=raw.get("name") or raw.get("companyName") or sym,
                        reason=f"invalid scan action without position: {action}",
                    )
                    append_event(
                        "rotation_in",
                        symbol=repl,
                        name=repl_raw.get("name") or repl_raw.get("companyName") or repl,
                        reason=f"force-replaced {sym}",
                    )
                    continue

                if sym in scan_set:
                    scan_set.remove(sym)

                removed.append(sym)
                removed_this_pass.add(sym)
                set_exclude_minutes(state, sym, pass_ex_min)
                reset_symbol_rotation_state(state, sym)

                log.info("[DROP-BAD] %s → removed, no valid replacement found", sym)
                continue

            if effective_signal == "Sälj" and current_pos <= 0:
                debug_log(
                    log,
                    "[INFO] %s → exit/bearish analysis, but no position exists. No sell action taken.",
                    sym,
                )
                effective_signal = "Håll"

            prev_decision = get_decision_state(state, sym)

            curr_decision = build_decision_snapshot(
                signal=effective_signal,
                action=action,
                timing_state=timing_state,
                pressure=None,
                exit_mode="scan",
                exit_stage=0,
                score=score,
                retention_score=retention_score,
            )
            material_change, changed_fields = is_material_change(prev_decision, curr_decision)

            if material_change:
                with open(str(SIGNAL_LOG_PATH), "a", encoding="utf-8") as f:
                    f.write(json.dumps(analysis, ensure_ascii=False) + "\n")
                trim_jsonl(str(SIGNAL_LOG_PATH), keep_last=5000)

        except Exception as e:
            signal = "Håll"
            score = 0
            retention_score = 0
            replacement_score = 0
            candidate_quality = "C"
            timing_state = "unknown"
            action = "watch"
            current_pos = float(held.get(sym, 0.0))
            effective_signal = "Håll"

            analysis = {
                "symbol": sym,
                "signal": "Håll",
                "total_score": 0,
                "candidate_quality": candidate_quality,
                "timing_state": timing_state,
                "action": action,
                "retention_score": retention_score,
                "replacement_score": replacement_score,
                "raw_technicals": {},
                "error": str(e),
                "timestamp": now_utc().isoformat(),
            }

            prev_decision = get_decision_state(state, sym)
            curr_decision = build_decision_snapshot(
                signal=effective_signal,
                action=action,
                timing_state=timing_state,
                pressure=None,
                exit_mode="scan",
                exit_stage=0,
                score=score,
                retention_score=retention_score,
            )
            material_change, changed_fields = is_material_change(prev_decision, curr_decision)

        raw_technicals = analysis.get("raw_technicals") or {}

        analysis_row = dict(analysis)
        analysis_row["symbol"] = sym
        analysis_row["name"] = raw.get("name") or raw.get("companyName") or sym
        analysis_row["held_position"] = float(held.get(sym, 0.0))
        scan_results.append(analysis_row)

        if material_change:
            append_event(
                "signal_evaluated",
                symbol=sym,
                name=raw.get("name") or raw.get("companyName") or sym,
                data={
                    "signal": signal,
                    "effective_signal": effective_signal,
                    "score": analysis.get("total_score"),
                    "price": raw_technicals.get("price"),
                    "changed_fields": changed_fields,
                },
            )

        if effective_signal == "Köp" and prev_decision.get("signal") != "Köp":
            append_event(
                "buy_signal",
                symbol=sym,
                name=raw.get("name") or raw.get("companyName") or sym,
                data={
                    "score": analysis.get("total_score"),
                    "price": raw_technicals.get("price"),
                    "event_kind": "new_buy_signal",
                },
            )

        if sym == "BRK-B" and not raw_technicals.get("price"):
            debug_log(log, "[KEEP] %s → IB technicals still missing, temporarily kept", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        if not raw_technicals:
            debug_log(log, "[KEEP] %s → technicals missing, kept in universe", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        drop_reason = None
        hold_streak = int(state.get("hold_streak", {}).get(sym, 0))
        effective_hold_streak = hold_streak + 1 if effective_signal == "Håll" else 0

        rotate_by_profile, rotate_reason = should_rotate_candidate(
            action=action,
            retention_score=retention_score,
            quality=candidate_quality,
            held_pos=current_pos,
        )

        if entry_mode == "buy_only":
            if action != "buy_ready":
                drop_reason = f"replace due to action={action}"
        elif entry_mode == "all":
            if rotate_by_profile:
                drop_reason = rotate_reason
            elif (
                market_ok
                and action == "watch"
                and effective_hold_streak >= drop_if_hold_streak
                and current_pos <= 0
                and retention_score <= 6
            ):
                drop_reason = f"replace due to watch_streak={effective_hold_streak}"
            elif (
                market_ok
                and action == "hold_candidate"
                and effective_hold_streak >= max(drop_if_hold_streak * 2, 12)
                and current_pos <= 0
                and retention_score <= 7
            ):
                drop_reason = f"replace due to stale_hold={effective_hold_streak}"
            elif score <= -3 and current_pos <= 0:
                drop_reason = f"replace due to low score={score}"

        if drop_reason:
            candidate_scan = [s for s in scan_set if s != sym]

            current_pool_size = len(_available_replacements(candidate_scan, banned=removed_this_pass | {sym}))
            if current_pool_size < min_rotation_pool_size:
                debug_log(
                    log,
                    "[KEEP] %s → %s but replacement_pool=%d < min=%d",
                    sym,
                    drop_reason,
                    current_pool_size,
                    min_rotation_pool_size,
                )
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            repl = _take_replacement(candidate_scan, banned=removed_this_pass | {sym})

            debug_log(
                log,
                "[ROTATE-CHECK] %s → %s | signal=%s | action=%s | score=%s | retention=%s | repl_score=%s",
                sym,
                drop_reason,
                signal,
                action,
                score,
                retention_score,
                replacement_score,
            )

            if not repl:
                debug_log(log, "[KEEP] %s → %s but no replacement is available", sym, drop_reason)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            repl_raw = by_sym.get(repl) or {}
            repl_analysis = analysis_cache.get(repl) or build_pipeline_analysis(repl_raw)

            debug_log(
                log,
                "[ROTATE-COMPARE] %s(ret=%s, action=%s, q=%s, streak=%s) vs %s(repl=%s, action=%s, q=%s)",
                sym,
                retention_score,
                action,
                candidate_quality,
                effective_hold_streak,
                repl,
                repl_analysis.get("replacement_score"),
                repl_analysis.get("action"),
                repl_analysis.get("candidate_quality"),
            )

            if not replacement_is_meaningfully_better(
                analysis,
                repl_analysis,
                watch_streak=effective_hold_streak,
            ):
                debug_log(log, "[KEEP] %s → replacement %s is not clearly better", sym, repl)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            if sym in scan_set:
                scan_set.remove(sym)

            removed.append(sym)
            removed_this_pass.add(sym)

            debug_log(log, "[REMOVE] %s → %s", sym, drop_reason)

            rotations_out.append({
                "symbol": sym,
                "name": raw.get("name") or raw.get("companyName") or sym,
                "reason": drop_reason,
            })

            append_event(
                "rotation_out",
                symbol=sym,
                name=raw.get("name") or raw.get("companyName") or sym,
                reason=drop_reason,
            )

            set_exclude_minutes(state, sym, pass_ex_min)
            reset_symbol_rotation_state(state, sym)

            scan_set.append(repl)
            scan_set = dedupe_keep_order(scan_set)[:universe_rows]
            added.append(repl)

            log.info("[ADD] %s → replaces %s", repl, sym)

            repl_raw = by_sym.get(repl) or {}
            rotations_in.append({
                "symbol": repl,
                "name": repl_raw.get("name") or repl_raw.get("companyName") or repl,
            })

            append_event(
                "rotation_in",
                symbol=repl,
                name=repl_raw.get("name") or repl_raw.get("companyName") or repl,
                reason=f"replaced {sym}",
            )
            continue

        if effective_signal not in {"Köp", "Sälj"}:
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=True,
            )
            continue

        if only_trade_on_signal_change and prev_decision.get("signal") == effective_signal:
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        buys_today_rec = state_counter(state, "buys_today", sym, today)
        sells_today_rec = state_counter(state, "sells_today", sym, today)

        if effective_signal == "Köp" and max_buys_per_day > 0 and buys_today_rec["count"] >= max_buys_per_day:
            log.info("[SKIP] %s → MAX_BUYS_PER_DAY reached", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        if effective_signal == "Sälj" and max_sells_per_day > 0 and sells_today_rec["count"] >= max_sells_per_day:
            log.info("[SKIP] %s → MAX_SELLS_PER_DAY reached", sym)
            _apply_symbol_state(
                state,
                sym,
                curr_decision,
                effective_signal,
                removed_this_pass,
                update_signal=False,
            )
            continue

        current_pos = float(held.get(sym, 0.0))
        qty = auto_qty
        trade = None
        persist_signal = False

        if autotrade_enabled and risk_ok and market_ok and not is_in_cooldown(state, sym, cooldown_min):
            action_signal = effective_signal

            if action_signal == "Sälj" and current_pos <= 0:
                log.info("[SKIP] %s → sell ignored, no position to close", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            if action_signal == "Sälj" and sym in open_sell_syms:
                log.info("[SKIP] %s → open sell order already exists", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            if action_signal == "Köp" and entries_this_pass >= max_new_entries_per_pass:
                log.info("[SKIP] %s → MAX_NEW_ENTRIES_PER_PASS reached", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue
            #
            if action_signal == "Köp" and current_pos <= 0 and max_total_open_positions > 0 and _long_position_count() >= max_total_open_positions:
                log.info("[SKIP] %s → MAX_TOTAL_OPEN_POSITIONS reached", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            if action_signal == "Köp" and total_bucket_count(state, "buys_today", today) >= max_new_entries_per_day_total:
                log.info("[SKIP] %s → MAX_NEW_ENTRIES_PER_DAY_TOTAL reached", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            if action_signal == "Köp" and is_global_trade_cooldown(state, "buy", min_minutes_between_global_buys):
                log.info("[SKIP] %s → global buy cooldown active", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            if action_signal == "Köp" and scan_pass_count(state, sym) < min_scan_passes_before_buy:
                log.info("[SKIP] %s → waiting for scan maturity (%d/%d)", sym, scan_pass_count(state, sym), min_scan_passes_before_buy)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            if action_signal == "Köp" and max_pos_per_symbol > 0:
                remaining_cap = max(0, max_pos_per_symbol - int(current_pos))
                if remaining_cap <= 0:
                    log.info("[SKIP] %s → MAX_POS_PER_SYMBOL reached", sym)
                    _apply_symbol_state(
                        state,
                        sym,
                        curr_decision,
                        effective_signal,
                        removed_this_pass,
                        update_signal=False,
                    )
                    continue
                qty = min(auto_qty, remaining_cap)

            if action_signal == "Sälj":
                qty = min(auto_qty, int(current_pos))
                if qty <= 0:
                    log.info("[SKIP] %s → no sellable position", sym)
                    _apply_symbol_state(
                        state,
                        sym,
                        curr_decision,
                        effective_signal,
                        removed_this_pass,
                        update_signal=False,
                    )
                    continue

            pretrade = None

            if action_signal == "Köp":
                pretrade = await validate_pretrade_buy(
                    symbol=sym,
                    raw=raw,
                    analysis=analysis,
                    ib_client=ib_client,
                    qty=qty,
                    max_order_value=max_order_value,
                )

                if not pretrade.get("ok"):
                    log.info("[SKIP] %s → pretrade blocked | %s", sym, pretrade.get("reason"))

                    append_event(
                        "buy_blocked_pretrade",
                        symbol=sym,
                        name=raw.get("name") or raw.get("companyName") or sym,
                        data={
                            "reason": pretrade.get("reason"),
                            "quote": pretrade.get("quote"),
                            "score": analysis.get("total_score"),
                            "entry_score": analysis.get("entry_score"),
                            "action": action,
                        },
                    )

                    _apply_symbol_state(
                        state,
                        sym,
                        curr_decision,
                        effective_signal,
                        removed_this_pass,
                        update_signal=False,
                    )
                    continue

            price_now = (
                (pretrade or {}).get("live_price")
                or to_float(raw_technicals.get("price"), 0)
                or to_float(raw.get("latestClose"), 0)
            )

            if action_signal == "Köp" and price_now > 0:
                est_value = price_now * qty
                if est_value > max_order_value:
                    log.info("[SKIP] %s → order value %.2f exceeds max %.2f", sym, est_value, max_order_value)
                    _apply_symbol_state(
                        state,
                        sym,
                        curr_decision,
                        effective_signal,
                        removed_this_pass,
                        update_signal=False,
                    )
                    continue

            key = f"{sym}:{action_signal}:{int(qty)}"

            if is_dup(key) or has_recent_order_key(state, key, persist_order_key_ttl_sec):
                log.info("[SKIP] %s → duplicate order key", sym)
                _apply_symbol_state(
                    state,
                    sym,
                    curr_decision,
                    effective_signal,
                    removed_this_pass,
                    update_signal=False,
                )
                continue

            try:
                trade = await _execute_order_safe(
                    ib_client,
                    raw,
                    action_signal,
                    qty=qty,
                    bot=bot,
                    chat_id=admin_chat_id,
                    quote=(pretrade or {}).get("quote"),
                )
            except Exception as e:
                trade = None
                log.error("[ORDER-ERR] %s → %s", sym, e)

            if trade:
                remember_order_key(state, key)
                filled_ok = await _trade_has_fill(trade)

                if filled_ok:
                    persist_signal = True

                    if action_signal == "Köp":
                        entries_this_pass += 1
                        orders_buy += 1
                        increment_day_counter(state, "buys_today", sym, today)

                        log_signal_line(log, "BUY", sym, qty, raw_technicals.get("price"), analysis.get("total_score"))

                        orders_for_report.append(f"BUY submitted: {sym} x{qty}")
                        append_event(
                            "buy_submitted",
                            symbol=sym,
                            name=raw.get("name") or raw.get("companyName") or sym,
                            data={"qty": qty},
                        )

                    elif action_signal == "Sälj":
                        orders_sell += 1
                        increment_day_counter(state, "sells_today", sym, today)

                        log_signal_line(log, "EXIT", sym, qty, raw_technicals.get("price"), analysis.get("total_score"))

                        orders_for_report.append(f"SELL submitted: {sym} x{qty}")
                        append_event(
                            "sell_submitted",
                            symbol=sym,
                            name=raw.get("name") or raw.get("companyName") or sym,
                            data={"qty": qty},
                        )

                    mark_trade_timestamp(state, sym)
                    set_exclude_minutes(state, sym, exclude_bought_min)
                    new_pos = _sync_local_fill_state(sym, action_signal, qty)

                    if action_signal == "Köp":
                        new_owned_row = build_owned_review_row(
                            sym=sym,
                            raw=raw,
                            analysis=analysis,
                            current_pos=new_pos,
                            effective_signal="Håll",
                            exit_mode="hold",
                            owned_reason="new_buy_fill",
                            exit_state={"stage": 0},
                        )
                        store_owned_snapshot(state, new_owned_row)
                        _upsert_portfolio_review(new_owned_row)
                    elif action_signal == "Sälj" and new_pos <= 0:
                        _remove_portfolio_review(sym)

                    if sym in scan_set:
                        scan_set.remove(sym)

                    removed.append(sym)
                    removed_this_pass.add(sym)
                    reset_symbol_rotation_state(state, sym)

                    reason_txt = "bought" if action_signal == "Köp" else "sold"
                    log.info("[REMOVE] %s → %s + excluded %d min", sym, reason_txt, exclude_bought_min)

                    rotations_out.append({
                        "symbol": sym,
                        "name": raw.get("name") or raw.get("companyName") or sym,
                        "reason": f"{reason_txt} + excluded {exclude_bought_min} min",
                    })

                    append_event(
                        "rotation_out",
                        symbol=sym,
                        name=raw.get("name") or raw.get("companyName") or sym,
                        reason=f"{reason_txt} + excluded {exclude_bought_min} min",
                    )

                    repl = _take_replacement(scan_set, banned=removed_this_pass)
                    if repl:
                        scan_set.append(repl)
                        scan_set = dedupe_keep_order(scan_set)[:universe_rows]
                        added.append(repl)
                        log.info("[ADD] %s → replaces %s", repl, sym)

                        repl_raw = by_sym.get(repl) or {}
                        rotations_in.append({
                            "symbol": repl,
                            "name": repl_raw.get("name") or repl_raw.get("companyName") or repl,
                        })

                        append_event(
                            "rotation_in",
                            symbol=repl,
                            name=repl_raw.get("name") or repl_raw.get("companyName") or repl,
                            reason=f"replaced {sym}",
                        )
                else:
                    log.info("[KEEP] %s → order submitted but not filled yet, no counters/exclude/rotation", sym)
            else:
                debug_log(log, "[KEEP] %s → no real order, kept in universe", sym)

        else:
            why = []
            if not autotrade_enabled:
                why.append("AUTOTRADE=off")
            if not risk_ok:
                why.append(f"risk:{risk_reason}")
            if not market_ok:
                why.append("market_closed")
            if is_in_cooldown(state, sym, cooldown_min):
                why.append("cooldown")

            if effective_signal in {"Köp", "Sälj"}:
                if effective_signal == "Köp":
                    paper_buy += 1
                    log.info(
                        "[PAPER-BUY] %s x%s | price %s | score %s | action=%s",
                        sym,
                        qty,
                        fmt_price(raw_technicals.get("price")),
                        fmt_score_plain(analysis.get("total_score")),
                        action,
                    )
                elif effective_signal == "Sälj":
                    paper_sell += 1
                    log.info(
                        "[PAPER-SELL] %s x%s | price %s | score %s | action=%s",
                        sym,
                        qty,
                        fmt_price(raw_technicals.get("price")),
                        fmt_score_plain(analysis.get("total_score")),
                        action,
                    )

                paper_symbols.append(f"{effective_signal}:{sym}")

                if material_change:
                    append_event(
                        "paper_signal",
                        symbol=sym,
                        name=raw.get("name") or raw.get("companyName") or sym,
                        data={
                            "signal": effective_signal,
                            "pipeline_signal": signal,
                            "action": action,
                            "timing_state": timing_state,
                            "candidate_quality": candidate_quality,
                            "qty": qty,
                            "reason": ",".join(why) or "-",
                            "score": analysis.get("total_score"),
                            "retention_score": retention_score,
                            "replacement_score": replacement_score,
                            "price": raw_technicals.get("price"),
                            "changed_fields": changed_fields,
                        },
                    )

                orders_for_report.append(
                    f"PAPER-SIM: would {effective_signal.lower()} {sym} x{qty} "
                    f"(action={action}, timing={timing_state}, quality={candidate_quality}, "
                    f"price={raw_technicals.get('price')}, score={analysis.get('total_score')}, {','.join(why) or '-'})"
                )

        if sym in removed_this_pass:
            continue

        _apply_symbol_state(
            state,
            sym,
            curr_decision,
            effective_signal,
            removed_this_pass,
            update_signal=persist_signal,
        )

    scan_set = _fill_scan_set(scan_set, banned=removed_this_pass)

    if len(scan_set) < universe_rows:
        log.warning(
            "[autoscan] Universe could not be fully filled (%d/%d). "
            "candidate_source=%d | entry=%d | tradable_entry=%d | watch=%d | fallback=%d | replacements=%d",
            len(scan_set),
            universe_rows,
            len(candidate_source),
            len(entry_candidates),
            len(tradable_entry_candidates),
            len(watch_candidates),
            len(fallback_candidates),
            len(replacement_source),
        )

    state["universe"] = list(scan_set)
    replacement_pool_size = len(_available_replacements(scan_set, banned=removed_this_pass))
    
    final_scan_index = {sym: i for i, sym in enumerate(scan_set)}
    final_scan_rows = sorted(
        [r for r in scan_results if r.get("symbol") in final_scan_index],
        key=lambda r: final_scan_index[r.get("symbol")],
    )

    save_daily_snapshot(
        state=state,
        summary={
            "universe_size": len(state.get("universe", [])),
            "scan_set_size": len(scan_set),
            "replacement_pool_size": replacement_pool_size,
            "orders_buy": orders_buy,
            "orders_sell": orders_sell,
        },
        scan_set=final_scan_rows,
        portfolio=portfolio_reviews,
        market_open=market_ok,
    )

    save_portfolio_review(portfolio_reviews)

    save_cycle_journal(
        market_open=market_ok,
        market_info=market_info,
        universe_size=len(by_sym),
        scan_set=final_scan_rows,
        replacement_pool_size=replacement_pool_size,
        portfolio=portfolio_reviews,
        rotations_out=rotations_out,
        rotations_in=rotations_in,
        orders=orders_for_report + owned_orders_for_report,
    )

    save_daily_report(
        market_open=market_ok,
        market_info=market_info,
        universe_size=len(by_sym),
        scan_set=final_scan_rows,
        replacement_pool_size=replacement_pool_size,
        rotations_out=rotations_out,
        rotations_in=rotations_in,
        orders=orders_for_report + owned_orders_for_report,
    )
    final_scan_set = set(scan_set)

    state["watchlist"] = dedupe_keep_order(
        [
            r["symbol"]
            for r in scan_results
            if r.get("symbol") in final_scan_set
            and str(r.get("action") or "").lower() == "watch"
        ]
    )

    save_state(state)

    if summary_notifs and admin_chat_id:
        add_txt = ", ".join(added) if added else "–"
        rem_txt = ", ".join(removed) if removed else "–"
        msg = [
            "Autoscan complete",
            f"• Stocks in scan: {len(scan_set)}",
            f"• Orders: Buy {orders_buy} · Sell {orders_sell}",
            f"• Paper sim: Buy {paper_buy} · Sell {paper_sell}",
            f"• Owned: EXIT NOW {owned_sell_now} · EXIT SOON {owned_sell_soon} · EXIT WATCH {owned_sell_watch} · CHECK {owned_checked}",
            f"• Rotations: + {add_txt} | - {rem_txt}",
            f"• Replacement pool: {replacement_pool_size}",
        ]
        await bot.send_message(admin_chat_id, "\n".join(msg))

    final_scan_set = set(scan_set)

    scan_watch_syms = [
        r["symbol"] for r in scan_results
        if r.get("symbol") in final_scan_set
        and str(r.get("action") or "").lower() == "watch"
    ]

    scan_hold_syms = [
        r["symbol"] for r in scan_results
        if r.get("symbol") in final_scan_set
        and str(r.get("action") or "").lower() == "hold_candidate"
    ]

    scan_buy_syms = [
        r["symbol"] for r in scan_results
        if r.get("symbol") in final_scan_set
        and str(r.get("action") or "").lower() == "buy_ready"
    ]

    scan_sell_syms = [
        r["symbol"] for r in scan_results
        if r.get("symbol") in final_scan_set
        and str(r.get("action") or "").lower() == "exit_ready"
    ]

    scan_exit_soon_syms = [
        r["symbol"] for r in scan_results
        if r.get("symbol") in final_scan_set
        and str(r.get("action") or "").lower() == "sell_candidate"
    ]

    scan_exit_watch_syms = [
        r["symbol"] for r in scan_results
        if r.get("symbol") in final_scan_set
        and str(r.get("action") or "").lower() == "exit_watch"
    ]

    if state.get("watchlist"):
        debug_log(log, "[WATCHLIST] %s", ", ".join(state["watchlist"]))

    scan_grouped = _group_symbols(scan_results, held_only=False)
    portfolio_grouped = _group_symbols(portfolio_reviews, held_only=True)

    log_section(log, "OWNED SUMMARY")
    held_syms = sorted(list(held.keys()))
    open_buy_list = sorted(list(open_buy_syms))

    log.info("%s %s", _c("OWNED       :", _CYAN, bold=True), fmt_sym_list(held_syms))
    log.info("%s %s", _c("OPEN BUYS   :", _BLUE, bold=True), fmt_sym_list(open_buy_list))
    log.info("%s %s", _c("STRONG      :", _GREEN, bold=True), fmt_sym_list(portfolio_grouped["buy_ready"]))
    log.info("%s %s", _c("EXIT NOW    :", _RED, bold=True), fmt_sym_list(portfolio_grouped["exit_ready"]))
    log.info("%s %s", _c("EXIT SOON   :", _YELLOW, bold=True), fmt_sym_list(portfolio_grouped["sell_candidate"]))
    log.info("%s %s", _c("EXIT WATCH  :", _CYAN, bold=True), fmt_sym_list(portfolio_grouped["exit_watch"]))
    log.info("%s %s", _c("WAIT        :", _BLUE, bold=True), fmt_sym_list(portfolio_grouped["watch"]))
    log.info("%s %s", _c("HOLD        :", _YELLOW, bold=True), fmt_sym_list(portfolio_grouped["hold"]))
    log.info("%s %s", _c("CHECK       :", _RED, bold=True), fmt_sym_list(portfolio_grouped["review"]))

    log_section(log, "SCAN SUMMARY")
    log.info(
        "%s  %s  %s  %s  %s  %s",
        _c(f"BUY: {len(scan_buy_syms)}", _GREEN, bold=True),
        _c(f"EXIT: {len(scan_sell_syms)}", _RED, bold=True),
        _c(f"EXIT SOON: {len(scan_exit_soon_syms)}", _YELLOW, bold=True),
        _c(f"EXIT WATCH: {len(scan_exit_watch_syms)}", _CYAN, bold=True),
        _c(f"WATCH: {len(scan_watch_syms)}", _CYAN, bold=True),
        _c(f"HOLD: {len(scan_hold_syms)}", _YELLOW, bold=True),
    )

    if scan_buy_syms:
        log.info("%s %s", _c("SCAN BUY        :", _GREEN, bold=True), fmt_sym_list(scan_buy_syms))
    if scan_sell_syms:
        log.info("%s %s", _c("SCAN EXIT       :", _RED, bold=True), fmt_sym_list(scan_sell_syms))
    if scan_exit_soon_syms:
        log.info("%s %s", _c("SCAN EXIT SOON  :", _YELLOW, bold=True), fmt_sym_list(scan_exit_soon_syms))
    if scan_exit_watch_syms:
        log.info("%s %s", _c("SCAN EXIT WATCH :", _CYAN, bold=True), fmt_sym_list(scan_exit_watch_syms))
    if scan_watch_syms:
        log.info("%s %s", _c("SCAN WATCH      :", _CYAN, bold=True), fmt_sym_list(scan_watch_syms))
    if scan_hold_syms:
        log.info("%s %s", _c("SCAN HOLD       :", _YELLOW, bold=True), fmt_sym_list(scan_hold_syms))

    today_buy_total = sum(
        int((state.get("buys_today", {}) or {}).get(sym, {}).get("count", 0) or 0)
        for sym in (state.get("buys_today", {}) or {})
    )
    today_sell_total = sum(
        int((state.get("sells_today", {}) or {}).get(sym, {}).get("count", 0) or 0)
        for sym in (state.get("sells_today", {}) or {})
    )

    log.info(
        "%s buys: %d | sells: %d",
        _c("TODAY:", _CYAN, bold=True),
        today_buy_total,
        today_sell_total,
    )

    top_buys = [r for r in scan_results if str(r.get("action") or "").lower() == "buy_ready"][:4]
    top_watch = [r for r in scan_results if str(r.get("action") or "").lower() == "watch"][:3]
    top_exit = [r for r in scan_results if str(r.get("action") or "").lower() in {"exit_ready", "sell_candidate", "exit_watch"}][:3]
    top_hold = [r for r in scan_results if str(r.get("action") or "").lower() == "hold_candidate"][:3]

    if top_buys:
        log.info("%s", _c("WHY BUY:", _GREEN, bold=True))
        for row in top_buys:
            log.info("  %s", short_reason_line(row))

    if top_hold:
        log.info("%s", _c("WHY HOLD:", _YELLOW, bold=True))
        for row in top_hold:
            log.info("  %s", short_reason_line(row))

    if top_watch:
        log.info("%s", _c("WHY WATCH:", _CYAN, bold=True))
        for row in top_watch:
            log.info("  %s", short_reason_line(row))

    if top_exit:
        log.info("%s", _c("WHY EXIT:", _RED, bold=True))
        for row in top_exit:
            log.info("  %s", short_reason_line(row))

    log.info(
        "%s raw=%d | s1=%d | s2=%d | s3=%d | final=%d | candidates=%d | replacements=%d",
        _c("UNIVERSE:", _CYAN, bold=True),
        int(pipeline_snapshot.get("universe_size", 0) or 0),
        int(pipeline_snapshot.get("stage1_passed", 0) or 0),
        int(pipeline_snapshot.get("stage2_passed", 0) or 0),
        int(pipeline_snapshot.get("stage3_passed", 0) or 0),
        len(by_sym),
        len(all_candidates),
        replacement_pool_size,
    )

    log.info(
        "%s universe=%d | candidates=%d | replacements=%d",
        _c("POOL:", _CYAN, bold=True),
        len(by_sym),
        len(all_candidates),
        replacement_pool_size,
    )

    if log_universe:
        log.info(
            "%s + %s | - %s",
            _c("BYTEN:", _CYAN, bold=True),
            _c(", ".join(added) if added else "–", _GREEN),
            _c(", ".join(removed) if removed else "–", _RED),
        )