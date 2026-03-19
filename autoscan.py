# autoscan.py
import os, json, random, logging, inspect
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from signals import buy_or_sell, execute_order
from universe_manager import load_state, save_state, update_signal_state
from helpers import kill_switch_ok, us_market_open_now, is_dup
from scanner import ensure_stock_info  # bygger/uppdaterar Stock_info.json

log = logging.getLogger("autoscan")
SE_TZ = ZoneInfo("Europe/Stockholm")

# -------------------- Env helpers --------------------
def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key, "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "on", "yes", "y"}

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
    s = dict(stock or {})
    for k in ("latestClose", "PE", "marketCap", "beta", "trailingEps", "dividendYield"):
        s[k] = _to_float(s.get(k), 0.0)
    return s

def _now_utc():
    return datetime.now(timezone.utc)

# -------------------- ensure_stock_info wrapper --------------------
async def _call_ensure_stock_info(ib_client, rows_target: int):
    """
    Kalla ensure_stock_info EN gång, robust mot olika signaturer:
      - async/sync
      - (ib=..., rows=...) eller (ib, rows) eller bara (rows) eller inga arg.
    """
    # Bygg en lista av möjliga varianter att prova i ordning
    ib = getattr(ib_client, "ib", None) if ib_client else None

    call_variants = []
    if ib is not None:
        call_variants.extend([
            ((), {"ib": ib, "rows": rows_target}),
            ((ib, rows_target), {}),
            ((), {"ib": ib}),
            ((ib,), {}),
        ])
    # varianter utan ib
    call_variants.extend([
        ((), {"rows": rows_target}),
        ((), {}),  # sista utvägen: inga argument
    ])

    is_async = inspect.iscoroutinefunction(ensure_stock_info)

    last_err = None
    for args, kwargs in call_variants:
        try:
            if is_async:
                res = await ensure_stock_info(*args, **kwargs)
            else:
                res = ensure_stock_info(*args, **kwargs)
            # lyckades → klart
            return res
        except TypeError as e:
            # typfel = fel signatur → prova nästa variant
            last_err = e
            continue
        except Exception as e:
            # annat fel → logga och prova nästa (men minns felet)
            last_err = e
            continue

    # Om allt misslyckades, kasta sista felet vidare
    if last_err:
        raise last_err

# -------------------- huvudkörning --------------------
async def run_autoscan_once(bot, ib_client, admin_chat_id: int):
    """
    - Säkerställ Stock_info.json via ensure_stock_info(...) (en gång, robust).
    - Skanna UNIVERSE_ROWS tickers (ej ägda/ej exkluderade), rotera bort ej-köp direkt.
    - Lägg verkliga köpordrar (om AUTOTRADE + säkerhetsgrindar).
    - Terminal: tydlig INFO-logg: SCAN_SET, REMOVE-anledningar, ORDER/SIM.
    - Telegram: kompakt summering (bara verkliga ordrar räknas).
    """
    # ---------- ENV ----------
    AUTOSCAN  = _env_bool("AUTOSCAN", True)
    AUTOTRADE = _env_bool("AUTOTRADE", False)
    ENTRY_MODE = os.getenv("ENTRY_MODE", "buy_only").strip().lower()  # "buy_only" | "all"
    ONLY_TRADE_ON_SIGNAL_CHANGE = _env_bool("ONLY_TRADE_ON_SIGNAL_CHANGE", True)
    COOLDOWN_MIN  = _env_int("COOLDOWN_MIN", 30)
    MAX_POS_PER_SYMBOL = _env_int("MAX_POS_PER_SYMBOL", 0)
    MAX_BUYS_PER_DAY   = _env_int("MAX_BUYS_PER_DAY", 1)
    MAX_SELLS_PER_DAY  = _env_int("MAX_SELLS_PER_DAY", 2)  # ej använd här, men kvar för framtida säljlogik
    UNIVERSE_ROWS      = _env_int("UNIVERSE_ROWS", 10)
    CAND_MULT          = max(1, _env_int("CANDIDATE_MULTIPLIER", 2))
    EXCLUDE_MINUTES    = _env_int("EXCLUDE_MINUTES", 120)
    PASS_EX_MIN        = _env_int("PASS_EXCLUDE_MINUTES", _env_int("ASS_EXCLUDE_MINUTES", 20))
    EXCLUDE_BOUGHT_MIN = _env_int("EXCLUDE_BOUGHT_MIN", 120)
    AUTO_QTY           = _env_int("AUTO_QTY", 2)
    SUMMARY_NOTIFS     = _env_bool("SUMMARY_NOTIFS", True)
    LOG_UNIVERSE       = _env_bool("LOG_UNIVERSE", True)
    DEBUG_AUTOTRADE    = _env_bool("DEBUG_AUTOTRADE", True)
    PREFER_NON_HOLD    = _env_bool("PREFER_NON_HOLD", True)
    DROP_IF_HOLD_STREAK = _env_int("DROP_IF_HOLD_STREAK", 1)
    CHURN_MIN          = _env_int("CHURN_MIN", 2)


    log.info(
        "CFG UNIVERSE_ROWS=%s CAND_MULT=%s AUTOTRADE=%s ENTRY_MODE=%s PASS_EX_MIN=%s EXCLUDE_BOUGHT_MIN=%s",
        UNIVERSE_ROWS, CAND_MULT, AUTOTRADE, ENTRY_MODE, PASS_EX_MIN, EXCLUDE_BOUGHT_MIN
    )

    if not AUTOSCAN:
        return

    # ---------- IB-status ----------
    if not ib_client or not ib_client.ib.isConnected():
        if admin_chat_id and SUMMARY_NOTIFS:
            await bot.send_message(admin_chat_id, "IBKR inte ansluten – hoppar över autoscan.")
        log.warning("IB inte ansluten – autoscan avbruten.")
        return

    # ---------- Bygg/uppdatera universum ----------
    rows_target = max(UNIVERSE_ROWS * CAND_MULT, UNIVERSE_ROWS)
    try:
        await _call_ensure_stock_info(ib_client, rows_target)
    except Exception as e:
        log.error("[autoscan] Kunde inte kalla ensure_stock_info: %s", e)
        return

    try:
        with open("Stock_info.json", "r", encoding="utf-8") as f:
            universe = json.load(f)
    except Exception as e:
        log.error("[autoscan] Kunde inte läsa Stock_info.json: %s", e)
        return

    # ---------- Läs positioner & öppna BUY-ordrar ----------
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
            and (t.orderStatus.status or "").lower()
            in {"presubmitted", "submitted", "pendingsubmit", "pendingcancel"}
        }
    except Exception:
        open_buy_syms = set()

    # ---------- State ----------
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
            return (_now_utc() - last) < timedelta(minutes=COOLDOWN_MIN)
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

    # ---------- Kandidat-pool ----------
    by_sym = {(s.get("symbol") or "").upper(): s for s in universe if s.get("symbol")}
    all_candidates = [s for s in by_sym.keys() if s not in held and s not in open_buy_syms and not _is_excluded(s)]
    random.shuffle(all_candidates)

    if len(all_candidates) < UNIVERSE_ROWS:
        all_candidates = [s for s in by_sym.keys() if s not in held]
        random.shuffle(all_candidates)

    scan_set = all_candidates[:UNIVERSE_ROWS]
    replacement_pool = [s for s in all_candidates if s not in scan_set]

    log.info(
        "POOL universe=%d all_candidates=%d scan_set=%d replacement_pool=%d",
        len(by_sym), len(all_candidates), len(scan_set), len(replacement_pool)
    )

    # Terminal: visa vilka vi kollar + pris
    rows_for_log = []
    for sym in scan_set:
        raw = by_sym.get(sym) or {}
        price = _to_float(raw.get("latestClose"), None)
        price_str = "{:.2f}".format(price) if isinstance(price, (int, float)) and price is not None else "-"
        rows_for_log.append(f"{sym}({price_str})")
    log.info("SCAN_SET [%d]: %s", len(scan_set), ", ".join(rows_for_log))

    # ---------- Säkerhetsgrindar ----------
    risk_ok, risk_reason = kill_switch_ok(
        getattr(ib_client, "pnl_realized_today", 0.0),
        getattr(ib_client, "pnl_unrealized_open", 0.0),
    )
    market_ok = us_market_open_now()
    log.info("MARKET_OPEN=%s | RISK_OK=%s (%s) | AUTOTRADE=%s",
             "JA" if market_ok else "NEJ",
             "JA" if risk_ok else "NEJ",
             risk_reason or "-",
             "ON" if AUTOTRADE else "OFF")

    added, removed = [], []
    orders_buy = 0
    orders_sell = 0  # reserverat för framtida säljlogik

    # ---------- Iterera kandidater ----------
    for sym in list(scan_set):
        raw = by_sym.get(sym) or {"symbol": sym, "name": sym}
        stock = _normalize_stock(raw)

        try:
            signal = buy_or_sell(stock)
        except Exception:
            signal = "Håll"

        prev_sig = state["last_signal"].get(sym)
        drop_reason = None

        if ENTRY_MODE == "buy_only":
            if signal != "Köp":
                drop_reason = f"ersätt pga {signal}"
        elif ENTRY_MODE == "all":
            if signal == "Håll":
                drop_reason = "ersätt pga Håll"

        if ONLY_TRADE_ON_SIGNAL_CHANGE and prev_sig == signal:
            drop_reason = "ingen signaländring"
        elif _in_cooldown(sym):
            drop_reason = "cooldown"
        if drop_reason:
            if sym in scan_set:
                scan_set.remove(sym)
            removed.append(sym)
            # ersätt en ny kandidat
            repl = replacement_pool.pop(0) if replacement_pool else None

            if repl:
                scan_set.append(repl)
                scan_set = scan_set[:UNIVERSE_ROWS]
                added.append(repl)
                log.info("[ADD] %s → ersätter %s", repl, sym)

            log.info("[REMOVE] %s → %s", sym, drop_reason)
            update_signal_state(state, sym, signal)
            # kort exkludering av pass
            state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=PASS_EX_MIN)).isoformat()
            continue

        # ---------- Köp ----------
        # Kapacitetsregler per symbol
        if MAX_POS_PER_SYMBOL > 0:
            # äger inte ännu → owned=0 (vi låter MAX_POS_PER_SYMBOL styra qty i framtiden om du vill)
            remaining_cap = MAX_POS_PER_SYMBOL
            if remaining_cap <= 0:
                update_signal_state(state, sym, signal)
                continue

        b = _counter("buys_today", sym)
        if MAX_BUYS_PER_DAY > 0 and b["count"] >= MAX_BUYS_PER_DAY:
            update_signal_state(state, sym, signal)
            log.info("[SKIP] %s → MAX_BUYS_PER_DAY nådd", sym)
            continue

        qty = AUTO_QTY
        trade = None

        if AUTOTRADE and risk_ok and market_ok and not _in_cooldown(sym):
            action_signal = signal
            key = f"{sym}:{action_signal}:{int(qty)}"

            if not is_dup(key):
                try:
                    trade = await execute_order(ib_client, raw, action_signal, qty=qty, bot=bot, chat_id=admin_chat_id)

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
            else:
                log.info("[SKIP] %s → dubblettnyckel", sym)
        else:
            if DEBUG_AUTOTRADE:
                why = []
                if not AUTOTRADE: why.append("AUTOTRADE=off")
                if not risk_ok:   why.append("risk")
                if not market_ok: why.append("market_closed")
                if _in_cooldown(sym): why.append("cooldown")
                log.info("[SIM] KÖP %s x%d (%s)", sym, qty, ",".join(why) or "-")

        # Exkludera nyköpta ett tag (även vid sim för att öka rotationen visuellt)
        state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=EXCLUDE_BOUGHT_MIN)).isoformat()
        state["last_trade_ts"][sym] = _now_utc().isoformat()
        b["count"] = int(b.get("count", 0)) + (1 if trade else 0)
        state["buys_today"][sym] = b
        removed.append(sym)  # flyttas ut från scan direkt efter (simulerat/verkligt) köp
        log.info("[REMOVE] %s → köpt/simulerad + exkluderas %d min", sym, EXCLUDE_BOUGHT_MIN)

        # Ersätt en köpt ticker
        repl = replacement_pool.pop(0) if replacement_pool else None

        if repl:
            scan_set.append(repl)
            scan_set = scan_set[:UNIVERSE_ROWS]
            added.append(repl)
            log.info("[ADD] %s → ersätter %s", repl, sym)

        update_signal_state(state, sym, signal)

    # ---------- Spara state ----------
    save_state(state)

    # ---------- Telegram-summering ----------
    if SUMMARY_NOTIFS and admin_chat_id:
        add_txt = ", ".join(added) if added else "–"
        rem_txt = ", ".join(removed) if removed else "–"
        msg = [
            "Autoscan klar",
            f"• Aktier (i scan): {len(scan_set)}",
            f"• Ordrar: Köp {orders_buy} · Sälj {orders_sell}",
            f"• Byten: + {add_txt}  |  − {rem_txt}",
        ]
        await bot.send_message(admin_chat_id, "\n".join(msg))

    # Extra logg om du vill ha allt i terminalen
    if LOG_UNIVERSE:
        log.info("BYTEN: + %s | - %s", (", ".join(added) if added else "–"), (", ".join(removed) if removed else "–"))
