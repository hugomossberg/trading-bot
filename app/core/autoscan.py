#autoscan.py
import os
import json
import random
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

from app.core.signals import get_signal_analysis, execute_order
from app.core.universe_manager import load_state, save_state, update_signal_state, rotate_universe
from app.core.helpers import kill_switch_ok, us_market_open_now, is_dup
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
)

log = logging.getLogger("autoscan")
SE_TZ = ZoneInfo("Europe/Stockholm")


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "on", "yes", "y"}


def _env_int(key: str, default: int) -> int:
    """
    Robust int-läsning, tolererar felskrivningar som "2|3".
    Tar första delen före '|' om det råkar finnas.
    """
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


async def run_autoscan_once(bot, ib_client, admin_chat_id: int):
    """
    - Säkerställ Stock_info.json via ensure_stock_info(...) (en gång, robust).
    - Skanna UNIVERSE_ROWS tickers (ej ägda/ej exkluderade), rotera bort ej-köp direkt.
    - Lägg verkliga köpordrar (om AUTOTRADE + säkerhetsgrindar).
    - Terminal: tydlig INFO-logg: SCAN_SET, REMOVE-anledningar, ORDER/SIM.
    - Telegram: kompakt summering (bara verkliga ordrar räknas).
    """

    # -------- Config / env --------
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
    max_sells_per_day = _env_int("MAX_SELLS_PER_DAY", 2)  # kvar för framtida säljlogik
    #exclude_minutes = _env_int("EXCLUDE_MINUTES", 120)
    pass_ex_min = _env_int("PASS_EXCLUDE_MINUTES", _env_int("ASS_EXCLUDE_MINUTES", 20))
    exclude_bought_min = _env_int("EXCLUDE_BOUGHT_MIN", 120)
    prefer_non_hold = _env_bool("PREFER_NON_HOLD", True)
    drop_if_hold_streak = _env_int("DROP_IF_HOLD_STREAK", 1)
    churn_min = _env_int("CHURN_MIN", 2)

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

    # -------- IB-status --------
    if not ib_client or not ib_client.ib.isConnected():
        if admin_chat_id and summary_notifs:
            await bot.send_message(admin_chat_id, "IBKR inte ansluten – hoppar över autoscan.")
        log.warning("IB inte ansluten – autoscan avbruten.")
        return

    # -------- Bygg/uppdatera universum --------
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

    # -------- Läs positioner & öppna BUY-ordrar --------
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

    # -------- State --------
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

    # -------- Kandidat-pool --------
    by_sym = {(s.get("symbol") or "").upper(): s for s in universe if s.get("symbol")}

    all_candidates = [
        s for s in by_sym.keys()
        if s not in held and s not in open_buy_syms and not _is_excluded(s)
    ]

    if len(all_candidates) < universe_rows:
        all_candidates = [s for s in by_sym.keys() if s not in held]

    random.shuffle(all_candidates)

    prev_uni = [s.upper() for s in state.get("universe", []) if s]
    scan_set, dropped_pre, added_pre = rotate_universe(prev_uni, all_candidates, state)

    replacement_pool = [s for s in all_candidates if s not in scan_set]

    # spara direkt aktuellt universe i state
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
        len(replacement_pool),
    )

    rows_for_log = []
    for sym in scan_set:
        raw = by_sym.get(sym) or {}
        price = _to_float(raw.get("latestClose"), None)
        price_str = "{:.2f}".format(price) if isinstance(price, (int, float)) and price is not None else "-"
        rows_for_log.append(f"{sym}({price_str})")
    log.info("SCAN_SET [%d]: %s", len(scan_set), ", ".join(rows_for_log))

    # -------- Säkerhetsgrindar --------
    risk_ok, risk_reason = kill_switch_ok(
        getattr(ib_client, "pnl_realized_today", 0.0),
        getattr(ib_client, "pnl_unrealized_open", 0.0),
    )
    market_ok = us_market_open_now()
    log.info(
        "MARKET_OPEN=%s | RISK_OK=%s (%s) | AUTOTRADE=%s",
        "JA" if market_ok else "NEJ",
        "JA" if risk_ok else "NEJ",
        risk_reason or "-",
        "ON" if autotrade_enabled else "OFF",
    )

    added, removed = [], []
    orders_buy = 0
    orders_sell = 0

    # -------- Iterera kandidater -------
    for sym in list(scan_set):
        raw = by_sym.get(sym) or {"symbol": sym, "name": sym}
        stock = _normalize_stock(raw)

        try:
            analysis = get_signal_analysis(stock)
            signal = analysis["signal"]
            analysis["timestamp"] = _now_utc().isoformat()

            with open("storage/signal_log.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(analysis, ensure_ascii=False) + "\n")

            trim_jsonl("storage/signal_log.jsonl", keep_last=5000)

        except Exception as e:
            signal = "Håll"
            analysis = {
                "symbol": sym,
                "signal": "Håll",
                "error": str(e),
                "timestamp": _now_utc().isoformat(),
            }

        prev_sig = state["last_signal"].get(sym)
        drop_reason = None

        if entry_mode == "buy_only":
            if signal != "Köp":
                drop_reason = f"ersätt pga {signal}"
        elif entry_mode == "all":
            if signal == "Håll":
                drop_reason = "ersätt pga Håll"

        if only_trade_on_signal_change and prev_sig == signal:
            drop_reason = "ingen signaländring"
        elif _in_cooldown(sym):
            drop_reason = "cooldown"

        if drop_reason:
            if sym in scan_set:
                scan_set.remove(sym)
            removed.append(sym)

            repl = replacement_pool.pop(0) if replacement_pool else None
            if repl:
                scan_set.append(repl)
                scan_set = scan_set[:universe_rows]
                added.append(repl)
                log.info("[ADD] %s → ersätter %s", repl, sym)

            log.info("[REMOVE] %s → %s", sym, drop_reason)
            update_signal_state(state, sym, signal)
            state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=pass_ex_min)).isoformat()
            continue

        # -------- Köp --------
        if max_pos_per_symbol > 0:
            remaining_cap = max_pos_per_symbol
            if remaining_cap <= 0:
                update_signal_state(state, sym, signal)
                continue

        buys_today_rec = _counter("buys_today", sym)
        if max_buys_per_day > 0 and buys_today_rec["count"] >= max_buys_per_day:
            update_signal_state(state, sym, signal)
            log.info("[SKIP] %s → MAX_BUYS_PER_DAY nådd", sym)
            continue

        qty = auto_qty
        trade = None

        if autotrade_enabled and risk_ok and market_ok and not _in_cooldown(sym):
            action_signal = signal
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
                        log.info("[ORDER] KÖP %s x%d", sym, qty)
                    elif action_signal == "Sälj":
                        orders_sell += 1
                        log.info("[ORDER] SÄLJ %s x%d", sym, qty)

                    state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=exclude_bought_min)).isoformat()
                    state["last_trade_ts"][sym] = _now_utc().isoformat()
                    buys_today_rec["count"] = int(buys_today_rec.get("count", 0)) + 1
                    state["buys_today"][sym] = buys_today_rec
                    removed.append(sym)
                    log.info("[REMOVE] %s → köpt + exkluderas %d min", sym, exclude_bought_min)

                    repl = replacement_pool.pop(0) if replacement_pool else None
                    if repl:
                        scan_set.append(repl)
                        scan_set = scan_set[:universe_rows]
                        added.append(repl)
                        log.info("[ADD] %s → ersätter %s", repl, sym)
                else:
                    log.info("[KEEP] %s → ingen verklig order, behålls i universe", sym)
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
                log.info("[SIM] KÖP %s x%d (%s)", sym, qty, ",".join(why) or "-")

            log.info("[KEEP] %s → ingen verklig order, behålls i universe", sym)
            
        update_signal_state(state, sym, signal)

    # -------- Spara state --------
    state["universe"] = list(scan_set)
    save_state(state)

    # -------- Telegram-summering --------
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