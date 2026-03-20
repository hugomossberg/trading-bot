import os
import json
import random
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.signals import get_signal_analysis, execute_order
from app.core.universe_manager import (
    load_state,
    save_state,
    update_signal_state,
    rotate_universe,
    reset_symbol_rotation_state,
)
from app.core.helpers import kill_switch_ok, market_open_now, is_dup
from app.core.scanner import ensure_stock_info
from app.config import (
    STOCK_INFO_PATH,
    AUTOSCAN,
    AUTOTRADE,
    UNIVERSE_ROWS,
    CANDIDATE_MULTIPLIER,
    AUTO_QTY,
    SUMMARY_NOTIFS,
    LOG_UNIVERSE,
    DEBUG_AUTOTRADE,
    DROP_IF_HOLD_STREAK,
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


def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default


def _normalize_stock(stock: dict) -> dict:
    normalized = dict(stock or {})
    for key in ("latestClose", "PE", "marketCap", "beta", "trailingEps", "dividendYield"):
        normalized[key] = _to_float(normalized.get(key), 0.0)
    return normalized


def _now_utc():
    return datetime.now(timezone.utc)


def trim_jsonl(path: str, keep_last: int = 5000):
    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    if len(lines) > keep_last:
        with p.open("w", encoding="utf-8") as f:
            f.writelines(lines[-keep_last:])


async def _call_ensure_stock_info(ib_client, rows_target: int):
    return await ensure_stock_info(ib_client, min_count=rows_target)


def _dedupe_keep_order(items):
    seen = set()
    out = []
    for item in items or []:
        if not item:
            continue
        item = str(item).upper().strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


async def run_autoscan_once(bot, ib_client, admin_chat_id: int):
    autoscan_enabled = AUTOSCAN
    autotrade_enabled = AUTOTRADE
    universe_rows = UNIVERSE_ROWS
    candidate_mult = max(1, CANDIDATE_MULTIPLIER)
    auto_qty = AUTO_QTY
    summary_notifs = SUMMARY_NOTIFS
    log_universe = LOG_UNIVERSE
    debug_autotrade = DEBUG_AUTOTRADE

    entry_mode = os.getenv("ENTRY_MODE", "buy_only").strip().lower()  # buy_only | all
    only_trade_on_signal_change = _env_bool("ONLY_TRADE_ON_SIGNAL_CHANGE", True)
    cooldown_min = _env_int("COOLDOWN_MIN", 30)
    max_pos_per_symbol = _env_int("MAX_POS_PER_SYMBOL", 0)
    max_buys_per_day = _env_int("MAX_BUYS_PER_DAY", 1)
    max_sells_per_day = _env_int("MAX_SELLS_PER_DAY", 2)
    pass_ex_min = _env_int("PASS_EXCLUDE_MINUTES", _env_int("ASS_EXCLUDE_MINUTES", 20))
    exclude_bought_min = _env_int("EXCLUDE_BOUGHT_MIN", 120)

    log.info(
        "CFG UNIVERSE_ROWS=%s CAND_MULT=%s AUTOTRADE=%s ENTRY_MODE=%s PASS_EX_MIN=%s EXCLUDE_BOUGHT_MIN=%s",
        universe_rows,
        candidate_mult,
        autotrade_enabled,
        entry_mode,
        pass_ex_min,
        exclude_bought_min,
    )

    if not autoscan_enabled:
        return

    if not ib_client or not ib_client.ib.isConnected():
        if admin_chat_id and summary_notifs:
            await bot.send_message(admin_chat_id, "IBKR inte ansluten – hoppar över autoscan.")
        log.warning("IB inte ansluten – autoscan avbruten.")
        return

    rows_target = max(universe_rows * candidate_mult, universe_rows)
    try:
        await _call_ensure_stock_info(ib_client, rows_target)
    except Exception as e:
        log.error("[autoscan] Kunde inte kalla ensure_stock_info: %s", e)
        return

    try:
        with open(STOCK_INFO_PATH, "r", encoding="utf-8") as f:
            universe = json.load(f)
    except Exception as e:
        log.error("[autoscan] Kunde inte läsa %s: %s", STOCK_INFO_PATH, e)
        return

    positions = await ib_client.ib.reqPositionsAsync()
    held = {
        (p.contract.symbol or "").upper(): float(p.position or 0.0)
        for p in positions
        if abs(float(p.position or 0.0)) > 1e-6
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
                "pendingcancel",
            }
        }
    except Exception:
        open_buy_syms = set()

    state = load_state()
    state.setdefault("last_signal", {})
    state.setdefault("exclude_until", {})
    state.setdefault("last_trade_ts", {})
    state.setdefault("buys_today", {})
    state.setdefault("sells_today", {})
    state.setdefault("hold_streak", {})

    today = _now_utc().date().isoformat()

    def _in_cooldown(sym: str) -> bool:
        ts = state["last_trade_ts"].get(sym)
        if not ts:
            return False
        try:
            last = datetime.fromisoformat(str(ts))
            return (_now_utc() - last) < timedelta(minutes=cooldown_min)
        except Exception:
            return False

    def _counter(bucket: str, sym: str) -> dict:
        rec = state[bucket].get(sym, {"date": today, "count": 0})
        if rec.get("date") != today:
            rec = {"date": today, "count": 0}
        return rec

    def _is_excluded(sym: str) -> bool:
        iso = state["exclude_until"].get(sym)
        if not iso:
            return False
        try:
            until = datetime.fromisoformat(str(iso))
            return _now_utc() < until
        except Exception:
            return False

    by_sym = {(s.get("symbol") or "").upper(): s for s in universe if s.get("symbol")}

    all_candidates = [
        s for s in by_sym.keys()
        if s not in held and s not in open_buy_syms and not _is_excluded(s)
    ]

    if len(all_candidates) < universe_rows:
        all_candidates = [s for s in by_sym.keys() if s not in held and s not in open_buy_syms]

    all_candidates = _dedupe_keep_order(all_candidates)
    random.shuffle(all_candidates)

    prev_uni = [s.upper() for s in state.get("universe", []) if s]
    scan_set, dropped_pre, added_pre = rotate_universe(prev_uni, all_candidates, state)
    scan_set = _dedupe_keep_order(scan_set)[:universe_rows]

    added, removed = [], []
    removed_this_pass = set()
    orders_buy = 0
    orders_sell = 0

    def _available_replacements(current_scan, banned=None):
        banned = banned or set()
        current_set = set(current_scan)
        return [
            s for s in all_candidates
            if s not in current_set
            and s not in banned
            and s not in held
            and s not in open_buy_syms
            and not _is_excluded(s)
        ]

    def _take_replacement(current_scan, banned=None):
        pool = _available_replacements(current_scan, banned=banned)
        if not pool:
            return None
        return pool[0]

    def _fill_scan_set(current_scan, banned=None):
        banned = banned or set()
        current_scan = _dedupe_keep_order(current_scan)
        while len(current_scan) < universe_rows:
            repl = _take_replacement(current_scan, banned=banned)
            if not repl:
                break
            current_scan.append(repl)
        return _dedupe_keep_order(current_scan)[:universe_rows]

    scan_set = _fill_scan_set(scan_set, banned=removed_this_pass)
    state["universe"] = list(scan_set)

    if dropped_pre:
        log.info("[PRE-REMOVE] %s", ", ".join(dropped_pre))
    if added_pre:
        log.info("[PRE-ADD] %s", ", ".join(added_pre))

    log.info(
        "POOL universe=%d all_candidates=%d scan_set=%d replacement_pool=%d",
        len(by_sym),
        len(all_candidates),
        len(scan_set),
        len(_available_replacements(scan_set, banned=removed_this_pass)),
    )

    rows_for_log = []
    for sym in scan_set:
        raw = by_sym.get(sym) or {}
        price = _to_float(raw.get("latestClose"), None)
        price_str = "{:.2f}".format(price) if isinstance(price, (int, float)) and price is not None else "-"
        rows_for_log.append(f"{sym}({price_str})")
    log.info("SCAN_SET [%d]: %s", len(scan_set), ", ".join(rows_for_log))

    risk_ok, risk_reason = kill_switch_ok(
        getattr(ib_client, "pnl_realized_today", 0.0),
        getattr(ib_client, "pnl_unrealized_open", 0.0),
    )
    market_ok = market_open_now()

    log.info(
        "MARKET_OPEN=%s | RISK_OK=%s (%s) | AUTOTRADE=%s",
        "JA" if market_ok else "NEJ",
        "JA" if risk_ok else "NEJ",
        risk_reason or "-",
        "ON" if autotrade_enabled else "OFF",
    )

    initial_scan = list(scan_set)

    for sym in initial_scan:
        if sym not in scan_set:
            continue

        raw = by_sym.get(sym) or {"symbol": sym, "name": sym}
        stock = _normalize_stock(raw)

        try:
            analysis = get_signal_analysis(stock)
            signal = analysis["signal"]
            analysis["timestamp"] = _now_utc().isoformat()

            with open("storage/signal_log.json", "a", encoding="utf-8") as f:
                f.write(json.dumps(analysis, ensure_ascii=False) + "\n")

            trim_jsonl("storage/signal_log.json", keep_last=5000)

        except Exception as e:
            signal = "Håll"
            analysis = {
                "symbol": sym,
                "signal": "Håll",
                "error": str(e),
                "timestamp": _now_utc().isoformat(),
            }

        raw_technicals = analysis.get("raw_technicals") or {}

        if sym == "BRK-B" and not raw_technicals.get("price"):
            log.info("[KEEP] %s → IB technicals saknas ännu, behålls tillfälligt", sym)
            update_signal_state(state, sym, signal)
            continue

        if not raw_technicals:
            log.info("[KEEP] %s → technicals saknas, behålls i universe", sym)
            update_signal_state(state, sym, signal)
            continue

        prev_sig = state["last_signal"].get(sym)
        drop_reason = None
        hold_streak = int(state.get("hold_streak", {}).get(sym, 0))
        effective_hold_streak = hold_streak + 1 if signal == "Håll" else 0

        if entry_mode == "buy_only":
            if signal != "Köp":
                drop_reason = f"ersätt pga {signal}"

        elif entry_mode == "all":
            if signal == "Håll" and effective_hold_streak >= DROP_IF_HOLD_STREAK:
                drop_reason = f"ersätt pga Håll-streak={effective_hold_streak}"

        if drop_reason:
            if sym in scan_set:
                scan_set.remove(sym)
            removed.append(sym)
            removed_this_pass.add(sym)

            log.info("[REMOVE] %s → %s", sym, drop_reason)
            update_signal_state(state, sym, signal)
            state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=pass_ex_min)).isoformat()
            reset_symbol_rotation_state(state, sym)

            repl = _take_replacement(scan_set, banned=removed_this_pass)
            if repl:
                scan_set.append(repl)
                scan_set = _dedupe_keep_order(scan_set)[:universe_rows]
                added.append(repl)
                log.info("[ADD] %s → ersätter %s", repl, sym)

            continue

        if only_trade_on_signal_change and prev_sig == signal:
            log.info("[KEEP] %s → ingen signaländring", sym)
            update_signal_state(state, sym, signal)
            continue

        buys_today_rec = _counter("buys_today", sym)
        sells_today_rec = _counter("sells_today", sym)

        if signal == "Köp" and max_buys_per_day > 0 and buys_today_rec["count"] >= max_buys_per_day:
            update_signal_state(state, sym, signal)
            log.info("[SKIP] %s → MAX_BUYS_PER_DAY nådd", sym)
            continue

        if signal == "Sälj" and max_sells_per_day > 0 and sells_today_rec["count"] >= max_sells_per_day:
            update_signal_state(state, sym, signal)
            log.info("[SKIP] %s → MAX_SELLS_PER_DAY nådd", sym)
            continue

        current_pos = float(held.get(sym, 0.0))
        qty = auto_qty
        trade = None

        if autotrade_enabled and risk_ok and market_ok and not _in_cooldown(sym):
            action_signal = signal

            if action_signal == "Sälj" and current_pos <= 0:
                log.info("[SKIP] %s → Sälj ignoreras, ingen position att stänga", sym)
                update_signal_state(state, sym, signal)
                continue

            if action_signal == "Köp" and max_pos_per_symbol > 0:
                remaining_cap = max(0, max_pos_per_symbol - int(current_pos))
                if remaining_cap <= 0:
                    log.info("[SKIP] %s → MAX_POS_PER_SYMBOL nådd", sym)
                    update_signal_state(state, sym, signal)
                    continue
                qty = min(auto_qty, remaining_cap)

            if action_signal == "Sälj":
                qty = min(auto_qty, int(current_pos))
                if qty <= 0:
                    log.info("[SKIP] %s → ingen säljbar position", sym)
                    update_signal_state(state, sym, signal)
                    continue

            key = f"{sym}:{action_signal}:{int(qty)}"

            if not is_dup(key):
                try:
                    trade = await execute_order(
                        ib_client,
                        raw,
                        action_signal,
                        qty=qty,
                        bot=bot,
                        chat_id=admin_chat_id,
                    )
                except Exception as e:
                    trade = None
                    log.error("[ORDER-ERR] %s → %s", sym, e)

                if trade:
                    if action_signal == "Köp":
                        orders_buy += 1
                        buys_today_rec["count"] = int(buys_today_rec.get("count", 0)) + 1
                        state["buys_today"][sym] = buys_today_rec
                        log.info("[ORDER] KÖP %s x%d", sym, qty)

                    elif action_signal == "Sälj":
                        orders_sell += 1
                        sells_today_rec["count"] = int(sells_today_rec.get("count", 0)) + 1
                        state["sells_today"][sym] = sells_today_rec
                        log.info("[ORDER] SÄLJ %s x%d", sym, qty)

                    state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=exclude_bought_min)).isoformat()
                    state["last_trade_ts"][sym] = _now_utc().isoformat()

                    if sym in scan_set:
                        scan_set.remove(sym)

                    removed.append(sym)
                    removed_this_pass.add(sym)
                    reset_symbol_rotation_state(state, sym)

                    reason_txt = "köpt" if action_signal == "Köp" else "såld"
                    log.info("[REMOVE] %s → %s + exkluderas %d min", sym, reason_txt, exclude_bought_min)

                    repl = _take_replacement(scan_set, banned=removed_this_pass)
                    if repl:
                        scan_set.append(repl)
                        scan_set = _dedupe_keep_order(scan_set)[:universe_rows]
                        added.append(repl)
                        log.info("[ADD] %s → ersätter %s", repl, sym)
                else:
                    log.info("[KEEP] %s → ingen verklig order, behålls i universe", sym)
            else:
                log.info("[SKIP] %s → duplicerad ordernyckel", sym)
        else:
            if debug_autotrade:
                why = []
                if not autotrade_enabled:
                    why.append("AUTOTRADE=off")
                if not risk_ok:
                    why.append("risk")
                if not market_ok:
                    why.append("market_closed")
                if _in_cooldown(sym):
                    why.append("cooldown")
                log.info("[SIM] %s %s x%d (%s)", signal.upper(), sym, qty, ",".join(why) or "-")

            log.info("[KEEP] %s → ingen verklig order, behålls i universe", sym)

        update_signal_state(state, sym, signal)

    scan_set = _fill_scan_set(scan_set, banned=removed_this_pass)

    if len(scan_set) < universe_rows:
        log.warning(
            "[autoscan] Universe kunde inte fyllas helt (%d/%d). Kandidater saknas efter filter/exclude.",
            len(scan_set),
            universe_rows,
        )

    state["universe"] = list(scan_set)
    save_state(state)

    if summary_notifs and admin_chat_id:
        add_txt = ", ".join(added) if added else "–"
        rem_txt = ", ".join(removed) if removed else "–"
        msg = [
            "Autoscan klar",
            f"• Aktier (i scan): {len(scan_set)}",
            f"• Ordrar: Köp {orders_buy} · Sälj {orders_sell}",
            f"• Byten: + {add_txt}  |  − {rem_txt}",
        ]
        await bot.send_message(admin_chat_id, "\n".join(msg))

    if log_universe:
        log.info(
            "BYTEN: + %s | - %s",
            (", ".join(added) if added else "–"),
            (", ".join(removed) if removed else "–"),
        )