import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.config import (
    AUTO_QTY,
    AUTOSCAN,
    AUTOTRADE,
    CANDIDATE_MULTIPLIER,
    DEBUG_AUTOTRADE,
    DROP_IF_HOLD_STREAK,
    FINAL_CANDIDATES_PATH,
    LOG_UNIVERSE,
    SIGNAL_LOG_PATH,
    SUMMARY_NOTIFS,
    UNIVERSE_ROWS,
)
from app.core.helpers import is_dup, kill_switch_ok, market_open_now
from app.core.pipeline import run_pipeline
from app.core.signals import execute_order
from app.core.storage_utils import (
    append_event,
    save_daily_report,
    save_daily_snapshot,
    save_portfolio_review,
)
from app.core.universe_manager import (
    get_exit_state,
    load_state,
    reset_symbol_rotation_state,
    rotate_universe,
    save_state,
    set_exit_state,
    update_signal_state,
)

log = logging.getLogger("autoscan")

# ===== Terminalfärger =====
_USE_COLOR = not bool(os.getenv("NO_COLOR", "").strip())

_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"

_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_BLUE = "\033[34m"
_CYAN = "\033[36m"
_GRAY = "\033[90m"


def _c(text: str, color: str = "", bold: bool = False, dim: bool = False) -> str:
    if not _USE_COLOR:
        return str(text)

    prefix = ""
    if bold:
        prefix += _BOLD
    if dim:
        prefix += _DIM
    prefix += color
    return f"{prefix}{text}{_RESET}"


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


def _to_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def _is_affordable(stock: dict, qty: int, max_order_value: float) -> bool:
    price = _to_float((stock or {}).get("latestClose"), None)
    if price is None or price <= 0:
        return False
    return (price * qty) <= max_order_value


def _fmt_price(value) -> str:
    v = _to_float(value, None)
    if v is None:
        return "-"
    return f"{v:.2f}"


def _fmt_score_plain(value) -> str:
    try:
        v = int(value)
    except Exception:
        return "-"
    return f"{v:+d}"


def _fmt_score_color(value) -> str:
    try:
        v = int(value)
    except Exception:
        return "-"

    if v <= -999:
        return _c(f"{v}", _RED, bold=True)
    if v > 0:
        return _c(f"{v:+d}", _GREEN, bold=True)
    if v < 0:
        return _c(f"{v:+d}", _RED, bold=True)
    return _c(f"{v:+d}", _YELLOW)


def _log_section(title: str):
    line = "-" * 78
    log.info("%s", _c(line, _GRAY))
    log.info("%s", _c(title, _CYAN, bold=True))
    log.info("%s", _c(line, _GRAY))


def _display_label_from_action(action: str, held_pos: float = 0.0) -> str:
    action = str(action or "").strip().lower()

    if held_pos > 0:
        if action == "buy_ready":
            return "ADD"
        if action == "exit_ready":
            return "EXIT"
        if action == "sell_candidate":
            return "EXIT SOON"
        if action == "exit_watch":
            return "EXIT WATCH"
        if action == "watch":
            return "WAIT"
        if action in {"hold_candidate", "hold_position"}:
            return "HOLD"
        if action == "review_needed":
            return "CHECK"
        return "HOLD"

    if action == "buy_ready":
        return "BUY"
    if action == "watch":
        return "WATCH"
    if action == "hold_candidate":
        return "HOLD"
    if action in {"sell_candidate", "exit_ready", "review_needed"}:
        return "CHECK"
    return "HOLD"


def _log_signal_line(label: str, sym: str, qty: int, price, score):
    label = (label or "").strip().upper()

    if label == "BUY":
        sig_txt = _c("BUY       ", _GREEN, bold=True)
    elif label == "ADD":
        sig_txt = _c("ADD       ", _GREEN, bold=True)
    elif label == "EXIT":
        sig_txt = _c("EXIT      ", _RED, bold=True)
    elif label == "EXIT SOON":
        sig_txt = _c("EXIT SOON ", _YELLOW, bold=True)
    elif label == "EXIT WATCH":
        sig_txt = _c("EXIT WATCH", _CYAN, bold=True)
    elif label == "WATCH":
        sig_txt = _c("WATCH     ", _CYAN, bold=True)
    elif label == "WAIT":
        sig_txt = _c("WAIT      ", _BLUE, bold=True)
    elif label == "CHECK":
        sig_txt = _c("CHECK     ", _RED, bold=True)
    else:
        sig_txt = _c("HOLD      ", _YELLOW, bold=True)

    sym_txt = _c(f"{sym:<6}", _CYAN, bold=True)
    qty_txt = _c(f"x{qty:<2}", _BLUE)
    price_txt = _c(f"pris {_fmt_price(price):>7}", _GRAY)
    score_txt = f"score {_fmt_score_color(score)}"

    log.info("%s %s %s | %s | %s", sig_txt, sym_txt, qty_txt, price_txt, score_txt)


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


def _quality_rank(value: str) -> int:
    mapping = {
        "A+": 5,
        "A": 4,
        "B": 3,
        "C": 2,
        "D": 1,
    }
    return mapping.get(str(value or "").upper(), 0)


def _is_buy_action(action: str) -> bool:
    return str(action or "").strip().lower() == "buy_ready"


def _is_watch_action(action: str) -> bool:
    return str(action or "").strip().lower() in {"watch", "hold_position", "hold_candidate"}


def _is_exit_action(action: str) -> bool:
    return str(action or "").strip().lower() == "exit_ready"


def _is_allowed_replacement_action(action: str, held_pos: float = 0.0) -> bool:
    action = str(action or "").strip().lower()

    if action == "buy_ready":
        return True

    if action in {"watch", "hold_candidate", "hold_position"}:
        return True

    if action in {"exit_ready", "sell_candidate"} and held_pos > 0:
        return True

    if action == "avoid":
        return False

    return False


def _update_watchlist(state: dict, sym: str, action: str, quality: str):
    watchlist = _dedupe_keep_order(state.get("watchlist", []))

    action = str(action or "").strip().lower()
    quality_rank = _quality_rank(quality)

    should_watch = action == "watch" and quality_rank >= 3

    if should_watch and sym not in watchlist:
        watchlist.append(sym)

    if not should_watch and sym in watchlist:
        watchlist.remove(sym)

    state["watchlist"] = _dedupe_keep_order(watchlist)


def _should_rotate_candidate(action: str, retention_score: int, quality: str, held_pos: float) -> tuple[bool, str | None]:
    action = str(action or "").strip().lower()
    quality_value = _quality_rank(quality)

    if action == "avoid":
        return True, "ersätt pga action=avoid"

    if action in {"exit_ready", "sell_candidate"} and held_pos <= 0:
        return True, f"ersätt pga {action} utan position"

    if retention_score <= 4 and quality_value <= 2 and held_pos <= 0:
        return True, f"ersätt pga retention_score={retention_score} quality={quality}"

    if retention_score <= 2 and held_pos <= 0:
        return True, f"ersätt pga låg retention_score={retention_score}"

    return False, None


def _required_replacement_delta(
    watch_streak: int,
    current_action: str,
    current_quality: str,
    current_retention: int,
) -> int:
    current_action = str(current_action or "").strip().lower()
    current_quality_rank = _quality_rank(current_quality)

    if watch_streak >= 30 and current_action in {"watch", "hold_candidate"}:
        return 0
    if watch_streak >= 20 and current_action in {"watch", "hold_candidate"}:
        return 1
    if watch_streak >= 10 and current_action in {"watch", "hold_candidate"}:
        return 2

    if current_retention <= 3 or current_quality_rank <= 2:
        return 1

    return 3


def _replacement_is_meaningfully_better(
    current_analysis: dict,
    replacement_stock: dict,
    watch_streak: int = 0,
) -> bool:
    current_retention = int(current_analysis.get("retention_score", current_analysis.get("total_score", 0)) or 0)
    current_quality = current_analysis.get("candidate_quality")
    current_action = str(current_analysis.get("action") or "").strip().lower()

    repl_analysis = _build_pipeline_analysis(replacement_stock or {})
    repl_replacement_score = int(repl_analysis.get("replacement_score", repl_analysis.get("total_score", 0)) or 0)
    repl_quality = repl_analysis.get("candidate_quality")
    repl_action = str(repl_analysis.get("action") or "").strip().lower()

    if repl_action == "buy_ready" and current_action not in {"buy_ready", "hold_position"}:
        return True

    required_delta = _required_replacement_delta(
        watch_streak=watch_streak,
        current_action=current_action,
        current_quality=current_quality,
        current_retention=current_retention,
    )

    if repl_replacement_score >= current_retention + required_delta:
        return True

    if _quality_rank(repl_quality) > _quality_rank(current_quality):
        if repl_replacement_score >= current_retention + max(0, required_delta - 1):
            return True

    return False


def _build_pipeline_analysis(stock: dict) -> dict:
    sym = (stock.get("symbol") or "").upper().strip()

    pipeline_signal = stock.get("_pipeline_signal") or "Håll"
    pipeline_score = stock.get("_pipeline_final_score", 0)
    pipeline_technicals = stock.get("_pipeline_technicals") or {}
    pipeline_scores = stock.get("_pipeline_scores") or {}
    pipeline_score_details = stock.get("_pipeline_score_details") or {}

    return {
        "symbol": sym,
        "signal": pipeline_signal,
        "total_score": pipeline_score,
        "candidate_score": stock.get("_pipeline_candidate_score", pipeline_score),
        "entry_score": stock.get("_pipeline_entry_score", 0),
        "candidate_quality": stock.get("_pipeline_candidate_quality", "C"),
        "setup_type": stock.get("_pipeline_setup_type", "unknown"),
        "timing_state": stock.get("_pipeline_timing_state", "unknown"),
        "action": stock.get("_pipeline_action", "watch"),
        "positive_flags": stock.get("_pipeline_positive_flags") or [],
        "risk_flags": stock.get("_pipeline_risk_flags") or [],
        "entry_reasons": stock.get("_pipeline_entry_reasons") or [],
        "retention_score": stock.get("_pipeline_retention_score", pipeline_score),
        "replacement_score": stock.get("_pipeline_replacement_score", pipeline_score),
        "rank": stock.get("_pipeline_rank"),
        "raw_technicals": pipeline_technicals,
        "pipeline_scores": pipeline_scores,
        "pipeline_score_details": pipeline_score_details,
        "timestamp": _now_utc().isoformat(),
    }


def _build_portfolio_review(held: dict, by_sym: dict) -> list[dict]:
    reviews = []

    for sym, pos in (held or {}).items():
        stock = by_sym.get(sym)
        if not stock:
            reviews.append({
                "symbol": sym,
                "held_position": pos,
                "status": "missing_from_pipeline",
                "action": "review_needed",
            })
            continue

        analysis = _build_pipeline_analysis(stock)
        reviews.append({
            "symbol": sym,
            "held_position": pos,
            "signal": analysis.get("signal"),
            "action": analysis.get("action"),
            "candidate_quality": analysis.get("candidate_quality"),
            "entry_score": analysis.get("entry_score"),
            "retention_score": analysis.get("retention_score"),
            "replacement_score": analysis.get("replacement_score"),
            "timing_state": analysis.get("timing_state"),
            "entry_reasons": analysis.get("entry_reasons") or [],
        })

    return reviews


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

        if held_only and held_pos == 0:
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


def _fmt_sym_list(items: list[str]) -> str:
    return ", ".join(items) if items else "–"


def _classify_exit_pressure(analysis: dict, current_pos: float) -> str:
    """
    long-position:
      healthy   = normalt / förbättrat
      weak      = första avvikelse
      bearish   = bekräftad svaghet
      emergency = sälj nu
    """
    if current_pos <= 0:
        return "healthy"

    action = str(analysis.get("action") or "").strip().lower()
    timing_state = str(analysis.get("timing_state") or "").strip().lower()
    quality_rank = _quality_rank(analysis.get("candidate_quality"))
    score = _to_int(analysis.get("total_score"), 0)
    retention = _to_int(analysis.get("retention_score"), score)

    if action == "exit_ready":
        return "emergency"

    if score <= -4:
        return "emergency"

    if retention <= 0 and timing_state == "avoid":
        return "emergency"

    if action == "sell_candidate":
        return "bearish"

    if action == "exit_watch":
        return "weak"

    if timing_state == "avoid" and (quality_rank <= 2 or retention <= 3):
        return "bearish"

    if action in {"watch"} and retention <= 3:
        return "weak"

    return "healthy"


def _advance_long_exit_state(exit_state: dict, analysis: dict, pressure: str) -> dict:
    stage = _to_int(exit_state.get("stage"), 0)
    bearish_count = _to_int(exit_state.get("bearish_count"), 0)
    soft_exit_done = bool(exit_state.get("soft_exit_done", False))

    score = _to_int(analysis.get("total_score"), 0)
    retention = _to_int(analysis.get("retention_score"), score)
    action = str(analysis.get("action") or "").strip().lower()
    timing_state = str(analysis.get("timing_state") or "").strip().lower()

    if pressure == "emergency":
        stage = 4
        bearish_count += 1

    elif pressure == "bearish":
        bearish_count += 1
        stage = max(stage, min(4, bearish_count))

    elif pressure == "weak":
        bearish_count = max(1, bearish_count)
        stage = max(stage, 1)
        if stage > 2:
            stage = 2

    else:  # healthy
        bearish_count = 0
        if action in {"buy_ready", "hold_candidate", "hold_position"} and retention >= 4:
            stage = 0
        else:
            stage = max(0, stage - 1)

        if stage == 0:
            soft_exit_done = False

    exit_state["stage"] = stage
    exit_state["bearish_count"] = bearish_count
    exit_state["last_action"] = action or "hold"
    exit_state["last_score"] = score
    exit_state["last_retention_score"] = retention
    exit_state["last_timing_state"] = timing_state or "unknown"
    exit_state["soft_exit_done"] = soft_exit_done
    exit_state["updated_at"] = _now_utc().isoformat()

    return exit_state


def _decide_long_exit(exit_state: dict, pressure: str) -> tuple[str, str]:
    stage = _to_int(exit_state.get("stage"), 0)
    bearish_count = _to_int(exit_state.get("bearish_count"), 0)
    soft_exit_done = bool(exit_state.get("soft_exit_done", False))

    if pressure == "emergency":
        return "full_exit", "emergency_exit"

    if stage >= 4 and bearish_count >= 4:
        return "full_exit", "confirmed_full_exit"

    if stage >= 3 and bearish_count >= 3 and not soft_exit_done:
        return "soft_exit", "confirmed_soft_exit"

    if stage >= 2:
        return "watch_exit", "confirmed_bearish_watch"

    if stage >= 1:
        return "watch_exit", "first_exit_warning"

    return "hold", "healthy"


def _owned_label_from_decision(decision: str) -> str:
    if decision == "full_exit":
        return "EXIT"
    if decision == "soft_exit":
        return "EXIT SOON"
    if decision == "watch_exit":
        return "EXIT WATCH"
    return "HOLD"

def _short_reason_line(row: dict) -> str:
    sym = row.get("symbol", "?")
    action = str(row.get("action") or "").lower()
    score = row.get("total_score")
    entry_score = row.get("entry_score")
    quality = row.get("candidate_quality")
    reasons = row.get("entry_reasons") or []

    above_sma = "price_above_sma20" in reasons
    below_sma = "price_below_sma20" in reasons
    trend_up = "sma20_above_or_equal_sma50" in reasons
    good_rsi = any(r in reasons for r in {"healthy_rsi", "acceptable_rsi"})
    high_rsi = "slightly_extended_rsi" in reasons
    good_volume = "ok_volume_confirmation" in reasons
    strong_momentum = any(r in reasons for r in {"strong_short_momentum", "strong_medium_momentum"})
    controlled_vol = "controlled_volatility" in reasons

    tags = []

    if action == "buy_ready":
        if above_sma:
            tags.append("över SMA20")
        if trend_up:
            tags.append("trend upp")
        if good_rsi:
            tags.append("bra RSI")
        elif high_rsi:
            tags.append("lite hög RSI")
        if good_volume:
            tags.append("bra volym")
        if strong_momentum:
            tags.append("starkt momentum")
        if controlled_vol:
            tags.append("kontrollerad vol")

        return f"{sym}: köp-läge | score {score} | entry {entry_score} | q {quality} | " + ", ".join(tags[:4])

    if action == "watch":
        if below_sma:
            tags.append("under SMA20")
        elif above_sma:
            tags.append("över SMA20")
        if trend_up:
            tags.append("trend upp")
        if good_rsi:
            tags.append("ok RSI")
        elif high_rsi:
            tags.append("hög RSI")
        if good_volume:
            tags.append("bra volym")
        if strong_momentum:
            tags.append("momentum finns")

        return f"{sym}: watch-läge | score {score} | entry {entry_score} | q {quality} | " + ", ".join(tags[:4])

    if action in {"exit_ready", "sell_candidate", "exit_watch"}:
        if below_sma:
            tags.append("under SMA20")
        if not trend_up:
            tags.append("trend svag")
        if high_rsi:
            tags.append("utsträckt")

        return f"{sym}: exit-läge | score {score} | entry {entry_score} | q {quality} | " + ", ".join(tags[:4])

    return f"{sym}: {action} | score {score} | entry {entry_score} | q {quality}"


async def run_autoscan_once(bot, ib_client, admin_chat_id: int):
    autoscan_enabled = AUTOSCAN
    autotrade_enabled = AUTOTRADE
    universe_rows = UNIVERSE_ROWS
    candidate_mult = max(1, CANDIDATE_MULTIPLIER)
    auto_qty = AUTO_QTY
    summary_notifs = SUMMARY_NOTIFS
    log_universe = LOG_UNIVERSE
    debug_autotrade = DEBUG_AUTOTRADE

    sim_market = os.getenv("SIM_MARKET", "0").strip().lower() in {"1", "true", "yes", "on"}
    if sim_market:
        autotrade_enabled = False
        log.warning("[autoscan][SIM] Fake market mode aktivt – AUTOTRADE tvingas OFF")

    entry_mode = os.getenv("ENTRY_MODE", "buy_only").strip().lower()
    only_trade_on_signal_change = _env_bool("ONLY_TRADE_ON_SIGNAL_CHANGE", True)
    cooldown_min = _env_int("COOLDOWN_MIN", 30)
    max_pos_per_symbol = _env_int("MAX_POS_PER_SYMBOL", 0)
    max_buys_per_day = _env_int("MAX_BUYS_PER_DAY", 1)
    max_sells_per_day = _env_int("MAX_SELLS_PER_DAY", 2)
    pass_ex_min = _env_int("PASS_EXCLUDE_MINUTES", _env_int("ASS_EXCLUDE_MINUTES", 20))
    exclude_bought_min = _env_int("EXCLUDE_BOUGHT_MIN", 120)
    max_order_value = _to_float(os.getenv("MAX_ORDER_VALUE_USD", "30"), 30.0)

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
            await bot.send_message(admin_chat_id, "IBKR inte ansluten – hoppar över autoscan.")
        log.warning("IB inte ansluten – autoscan avbruten.")
        return

    try:
        await run_pipeline(ib_client)
    except Exception as e:
        log.error("[autoscan] Kunde inte köra pipeline: %s", e)
        return

    try:
        with open(FINAL_CANDIDATES_PATH, "r", encoding="utf-8") as f:
            universe = json.load(f)
    except Exception as e:
        log.error("[autoscan] Kunde inte läsa %s: %s", FINAL_CANDIDATES_PATH, e)
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

    log.info(
        "[autoscan] HELD=%s | OPEN_BUYS=%s",
        sorted(list(held.keys())),
        sorted(list(open_buy_syms)),
    )

    state = load_state()
    state.setdefault("last_signal", {})
    state.setdefault("exclude_until", {})
    state.setdefault("last_trade_ts", {})
    state.setdefault("buys_today", {})
    state.setdefault("sells_today", {})
    state.setdefault("hold_streak", {})
    state.setdefault("watchlist", [])

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

    # =========================
    # BUILD by_sym
    # =========================
    by_sym = {}
    for s in universe:
        sym = (s.get("symbol") or "").upper().strip()
        if not sym:
            continue

        stock = dict(s.get("stock") or {})
        stock["symbol"] = sym
        stock["name"] = s.get("name") or stock.get("name") or sym

        stock["_pipeline_signal"] = s.get("signal")
        stock["_pipeline_final_score"] = s.get("final_score")
        stock["_pipeline_technicals"] = s.get("technicals") or {}
        stock["_pipeline_scores"] = s.get("scores") or {}
        stock["_pipeline_score_details"] = s.get("score_details") or {}

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

        by_sym[sym] = stock

    log.info("[autoscan] FINAL symbols=%s", sorted(list(by_sym.keys())))

    # =========================
    # GEMENSAM INIT
    # =========================
    risk_ok, risk_reason = kill_switch_ok(
        getattr(ib_client, "pnl_realized_today", 0.0),
        getattr(ib_client, "pnl_unrealized_open", 0.0),
    )
    market_ok = market_open_now()
    sim_mode = os.getenv("SIM_MARKET", "0").strip().lower() in {"1", "true", "yes", "on"}

    added = []
    removed = []
    removed_this_pass = set()

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

    # =========================
    # PORTFOLIO REVIEW / OWNED ENGINE
    # =========================
    portfolio_reviews = _build_portfolio_review(held, by_sym)

    owned_sell_now = 0
    owned_sell_watch = 0
    owned_sell_soon = 0
    owned_checked = 0


    for row in portfolio_reviews:
        sym = row.get("symbol")
        current_pos = float(row.get("held_position") or 0.0)
        action = str(row.get("action") or "").strip().lower()

        if current_pos == 0:
            continue

        raw = by_sym.get(sym) or {"symbol": sym, "name": sym}
        analysis = _build_pipeline_analysis(raw) if by_sym.get(sym) else {
            "symbol": sym,
            "action": "review_needed",
            "total_score": 0,
            "candidate_quality": None,
            "timing_state": None,
            "raw_technicals": {},
            "retention_score": 0,
        }

        pressure = "healthy"
        exit_state = get_exit_state(state, sym)
        effective_signal = "Håll"
        exit_mode = "hold"
        owned_reason = "healthy"

        if current_pos > 0:
            pressure = _classify_exit_pressure(analysis, current_pos)
            exit_state = _advance_long_exit_state(exit_state, analysis, pressure)
            decision, owned_reason = _decide_long_exit(exit_state, pressure)

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

        set_exit_state(state, sym, exit_state)

        display_qty = int(abs(current_pos)) if current_pos > 0 else auto_qty

        owned_label = _owned_label_from_decision(
            "full_exit" if exit_mode == "full"
            else "soft_exit" if exit_mode == "soft"
            else "watch_exit" if exit_mode == "watch"
            else "hold"
        )

        if current_pos < 0 and effective_signal == "Köp":
            owned_label = "EXIT"
        elif action == "buy_ready" and current_pos > 0 and exit_mode == "hold":
            owned_label = "ADD"
        elif action == "review_needed" and exit_mode == "hold":
            owned_label = "CHECK"

        show_owned_row = owned_label in {"EXIT", "EXIT SOON", "EXIT WATCH", "ADD", "CHECK"}

        if show_owned_row:
            _log_signal_line(
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
            log.info(
                "[OWNED-STATE] %s | pressure=%s | stage=%s | bearish_count=%s | soft_exit_done=%s | reason=%s",
                sym,
                pressure,
                exit_state.get("stage"),
                exit_state.get("bearish_count"),
                exit_state.get("soft_exit_done"),
                owned_reason,
            )

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
            },
        )

        if effective_signal not in {"Sälj", "Köp"}:
            update_signal_state(state, sym, effective_signal)
            continue

        sells_today_rec = _counter("sells_today", sym)

        if max_sells_per_day > 0 and effective_signal == "Sälj" and sells_today_rec["count"] >= max_sells_per_day:
            log.info("[OWNED-SKIP] %s → MAX_SELLS_PER_DAY nådd", sym)
            update_signal_state(state, sym, effective_signal)
            continue

        if _in_cooldown(sym):
            log.info("[OWNED-SKIP] %s → cooldown", sym)
            update_signal_state(state, sym, effective_signal)
            continue

        if exit_mode == "full":
            qty = int(abs(current_pos))
        elif exit_mode == "soft":
            qty = max(1, int(abs(current_pos) / 2))
        elif exit_mode == "cover":
            qty = int(abs(current_pos))
        else:
            qty = min(auto_qty, int(abs(current_pos)))

        if qty <= 0:
            log.info("[OWNED-SKIP] %s → ingen hanterbar position", sym)
            update_signal_state(state, sym, effective_signal)
            continue

        if effective_signal == "Sälj":
            today_bucket = "sells_today"
            event_name = "owned_sell_submitted"
            report_label = "SELL"
        else:
            today_bucket = "buys_today"
            event_name = "owned_cover_submitted"
            report_label = "BUY"

        today_rec = _counter(today_bucket, sym)

        if effective_signal == "Sälj" and max_sells_per_day > 0 and today_rec["count"] >= max_sells_per_day:
            log.info("[OWNED-SKIP] %s → MAX_SELLS_PER_DAY nådd", sym)
            update_signal_state(state, sym, effective_signal)
            continue

        if effective_signal == "Köp" and max_buys_per_day > 0 and today_rec["count"] >= max_buys_per_day:
            log.info("[OWNED-SKIP] %s → MAX_BUYS_PER_DAY nådd", sym)
            update_signal_state(state, sym, effective_signal)
            continue

        if autotrade_enabled and risk_ok and market_ok:
            order_side = effective_signal
            key = f"{sym}:OWNED_{order_side}:{int(qty)}"

            if is_dup(key):
                log.info("[OWNED-SKIP] %s → duplicerad ordernyckel", sym)
                update_signal_state(state, sym, effective_signal)
                continue

            trade = None
            try:
                trade = await execute_order(
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
                if order_side == "Sälj":
                    orders_sell += 1
                    if exit_mode == "soft":
                        exit_state["soft_exit_done"] = True
                        set_exit_state(state, sym, exit_state)
                else:
                    orders_buy += 1

                today_rec["count"] = int(today_rec.get("count", 0)) + 1
                state[today_bucket][sym] = today_rec
                state["last_trade_ts"][sym] = _now_utc().isoformat()
                state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=exclude_bought_min)).isoformat()

                owned_orders_for_report.append(
                    f"OWNED {report_label} submitted: {sym} x{qty} "
                    f"(exit_mode={exit_mode}, reason={owned_reason}, stage={exit_state.get('stage')})"
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
                    },
                )
            else:
                log.info("[OWNED-KEEP] %s → ingen verklig order skickad", sym)

        else:
            why = []
            if not autotrade_enabled:
                why.append("AUTOTRADE=off")
            if not risk_ok:
                why.append("risk")
            if not market_ok:
                why.append("market_closed")

            paper_tag = "OWNED-PAPER-SELL" if effective_signal == "Sälj" else "OWNED-PAPER-BUY"

            log.info(
                "[%s] %s x%s | pris %s | score %s | action=%s | exit_mode=%s | reason=%s | stage=%s",
                paper_tag,
                sym,
                qty,
                _fmt_price((analysis.get("raw_technicals") or {}).get("price")),
                _fmt_score_plain(analysis.get("total_score")),
                action,
                exit_mode,
                owned_reason,
                exit_state.get("stage"),
            )

            if effective_signal == "Sälj" and exit_mode == "soft":
                exit_state["soft_exit_done"] = True
                set_exit_state(state, sym, exit_state)

            owned_orders_for_report.append(
                f"OWNED PAPER-{effective_signal.upper()}: skulle {effective_signal.lower()} {sym} x{qty} "
                f"(action={action}, timing={analysis.get('timing_state')}, "
                f"quality={analysis.get('candidate_quality')}, score={analysis.get('total_score')}, "
                f"exit_mode={exit_mode}, exit_reason={owned_reason}, exit_stage={exit_state.get('stage')}, "
                f"{','.join(why) or '-'})"
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
                },
            )

        update_signal_state(state, sym, effective_signal)

    # =========================
    # SCAN CANDIDATES
    # =========================
    def _is_good_scan_candidate(stock: dict) -> bool:
        analysis = _build_pipeline_analysis(stock or {})
        action = str(analysis.get("action") or "").strip().lower()
        quality = str(analysis.get("candidate_quality") or "").upper()
        retention_score = int(analysis.get("retention_score", analysis.get("total_score", 0)) or 0)

        if action == "buy_ready":
            return True

        if action in {"watch", "hold_candidate"} and quality in {"A+", "A", "B"}:
            return True

        if action in {"watch", "hold_candidate"} and retention_score >= 4:
            return True

        return False

    all_candidates = [
        s for s in by_sym.keys()
        if s not in held and s not in open_buy_syms and not _is_excluded(s)
    ]

    good_candidates = [
        s for s in all_candidates
        if _is_good_scan_candidate(by_sym.get(s) or {})
    ]

    good_candidates = sorted(
        good_candidates,
        key=lambda s: (
            1 if str((_build_pipeline_analysis(by_sym.get(s) or {})).get("action", "")).lower() == "buy_ready" else 0,
            int((_build_pipeline_analysis(by_sym.get(s) or {})).get("retention_score", 0) or 0),
            int((_build_pipeline_analysis(by_sym.get(s) or {})).get("entry_score", 0) or 0),
            _quality_rank((_build_pipeline_analysis(by_sym.get(s) or {})).get("candidate_quality")),
        ),
        reverse=True,
    )

    tradable_candidates = [
        s for s in good_candidates
        if _is_affordable(by_sym.get(s) or {}, auto_qty, max_order_value)
    ]
################################################################################################################
    candidate_source = tradable_candidates if tradable_candidates else good_candidates[:]

    if len(candidate_source) < universe_rows:
        log.warning(
            "[autoscan] Få köpbara kandidater efter filter (%d/%d) – behåller mindre scan_set istället för att fylla med dyra aktier",
            len(candidate_source),
            universe_rows,
        )

    all_candidates = _dedupe_keep_order(all_candidates)
    all_candidates = sorted(
        all_candidates,
        key=lambda s: _to_float((by_sym.get(s) or {}).get("latestClose"), 999999)
    )

    candidate_source = _dedupe_keep_order(candidate_source)

    # Aktivt scan_set ska byggas av de bästa, men replacement_pool måste vara större
    scan_seed = candidate_source[:]

    # För replacement: börja med resten av tradable/good, inte bara scan_set
    replacement_source = _dedupe_keep_order(
        [s for s in tradable_candidates if s not in scan_seed[:universe_rows]]
        + [s for s in good_candidates if s not in scan_seed[:universe_rows]]
        + [s for s in all_candidates if s not in scan_seed[:universe_rows]]
    )

    log.info(
        "[autoscan] FILTERED candidates | all=%d | good=%d | tradable=%d | selected=%d | replacement_candidates=%d",
        len(all_candidates),
        len(good_candidates),
        len(tradable_candidates),
        len(candidate_source),
        len(replacement_source),
    )

    prev_uni = [s.upper() for s in state.get("universe", []) if s]
    scan_set, dropped_pre, added_pre = rotate_universe(prev_uni, candidate_source, state)
    scan_set = _dedupe_keep_order(scan_set)[:universe_rows]

    def _available_replacements(current_scan, banned=None):
        banned = banned or set()
        current_set = set(current_scan)

        pool = []
        for s in replacement_source:
            if s in current_set:
                continue
            if s in banned:
                continue
            if s in held:
                continue
            if s in open_buy_syms:
                continue
            if _is_excluded(s):
                continue

            stock = by_sym.get(s) or {}
            analysis = _build_pipeline_analysis(stock)
            action = str(analysis.get("action") or "").strip().lower()
            held_pos = float(held.get(s, 0.0))

            if not _is_allowed_replacement_action(action, held_pos):
                continue

            pool.append(s)

        pool.sort(
            key=lambda s: (
                1 if str((_build_pipeline_analysis(by_sym.get(s) or {})).get("action", "")).lower() == "buy_ready" else 0,
                int((_build_pipeline_analysis(by_sym.get(s) or {})).get("replacement_score", 0) or 0),
                int((_build_pipeline_analysis(by_sym.get(s) or {})).get("entry_score", 0) or 0),
                _quality_rank((_build_pipeline_analysis(by_sym.get(s) or {})).get("candidate_quality")),
                int((_build_pipeline_analysis(by_sym.get(s) or {})).get("total_score", 0) or 0),
            ),
            reverse=True,
        )

        return pool

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

    replacement_pool_size = len(_available_replacements(scan_set, banned=removed_this_pass))

    if dropped_pre:
        log.info("[PRE-REMOVE] %s", ", ".join(dropped_pre))
    if added_pre:
        log.info("[PRE-ADD] %s", ", ".join(added_pre))

    log.info(
        "POOL pipeline_final=%d | all_candidates=%d | good=%d | tradable=%d | scan_set=%d | replacement_pool=%d",
        len(by_sym),
        len(all_candidates),
        len(good_candidates),
        len(tradable_candidates),
        len(scan_set),
        replacement_pool_size,
    )
    rows_for_log = []
    for sym in scan_set:
        raw = by_sym.get(sym) or {}
        tech_price = ((raw.get("_pipeline_technicals") or {}).get("price"))
        shown_price = tech_price if tech_price is not None else raw.get("latestClose")
        rows_for_log.append(f"{sym}({_fmt_price(shown_price)})")

    log.info(
        "%s scan=%d | replacements=%d | mode=%s | market=%s | autotrade=%s",
        _c("RUN:", _CYAN, bold=True),
        len(scan_set),
        replacement_pool_size,
        "SIM" if sim_mode else "LIVE",
        "OPEN" if market_ok else "CLOSED",
        "ON" if autotrade_enabled else "OFF",
    )
    log.info("%s %s", _c("SCAN SET:", _CYAN, bold=True), ", ".join(rows_for_log))

    initial_scan = list(scan_set)

    for sym in initial_scan:
        if sym not in scan_set:
            continue

        raw = by_sym.get(sym) or {"symbol": sym, "name": sym}
        stock = _normalize_stock(raw)

        try:
            analysis = _build_pipeline_analysis(stock)
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

            _update_watchlist(state, sym, action, candidate_quality)

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
                "timestamp": _now_utc().isoformat(),
            }

        raw_technicals = analysis.get("raw_technicals") or {}

        analysis_row = dict(analysis)
        analysis_row["symbol"] = sym
        analysis_row["name"] = raw.get("name") or raw.get("companyName") or sym
        analysis_row["held_position"] = float(held.get(sym, 0.0))
        scan_results.append(analysis_row)

        prev_sig = state["last_signal"].get(sym)

        append_event(
            "signal_evaluated",
            symbol=sym,
            name=raw.get("name") or raw.get("companyName") or sym,
            data={
                "signal": signal,
                "score": analysis.get("total_score"),
                "price": raw_technicals.get("price"),
            },
        )

        if effective_signal == "Köp" and prev_sig != "Köp":
            append_event(
                "buy_signal",
                symbol=sym,
                name=raw.get("name") or raw.get("companyName") or sym,
                data={
                    "score": analysis.get("total_score"),
                    "price": raw_technicals.get("price"),
                },
            )

        if sym == "BRK-B" and not raw_technicals.get("price"):
            log.info("[KEEP] %s → IB technicals saknas ännu, behålls tillfälligt", sym)
            update_signal_state(state, sym, effective_signal)
            continue

        if not raw_technicals:
            log.info("[KEEP] %s → technicals saknas, behålls i universe", sym)
            update_signal_state(state, sym, effective_signal)
            continue

        drop_reason = None
        hold_streak = int(state.get("hold_streak", {}).get(sym, 0))
        effective_hold_streak = hold_streak + 1 if effective_signal == "Håll" else 0

        rotate_by_profile, rotate_reason = _should_rotate_candidate(
            action=action,
            retention_score=retention_score,
            quality=candidate_quality,
            held_pos=current_pos,
        )

        if entry_mode == "buy_only":
            if not _is_buy_action(action):
                drop_reason = f"ersätt pga action={action}"
        elif entry_mode == "all":
            if rotate_by_profile:
                drop_reason = rotate_reason
            elif (
                market_ok
                and action == "watch"
                and effective_hold_streak >= DROP_IF_HOLD_STREAK
                and current_pos <= 0
                and retention_score <= 6
            ):
                drop_reason = f"ersätt pga watch-streak={effective_hold_streak}"
            elif score <= -3 and current_pos <= 0:
                drop_reason = f"ersätt pga låg score={score}"

        if drop_reason:
            candidate_scan = [s for s in scan_set if s != sym]
            repl = _take_replacement(candidate_scan, banned=removed_this_pass | {sym})

            log.info(
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
                log.info("[KEEP] %s → %s men ingen ersättare finns", sym, drop_reason)
                update_signal_state(state, sym, effective_signal)
                continue

            repl_raw = by_sym.get(repl) or {}
            repl_analysis = _build_pipeline_analysis(repl_raw)

            log.info(
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

            if not _replacement_is_meaningfully_better(
                analysis,
                repl_raw,
                watch_streak=effective_hold_streak,
            ):
                log.info("[KEEP] %s → ersättare %s är inte tydligt bättre", sym, repl)
                update_signal_state(state, sym, effective_signal)
                continue

            if sym in scan_set:
                scan_set.remove(sym)

            removed.append(sym)
            removed_this_pass.add(sym)

            log.info("[REMOVE] %s → %s", sym, drop_reason)

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

            update_signal_state(state, sym, effective_signal)
            state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=pass_ex_min)).isoformat()
            reset_symbol_rotation_state(state, sym)

            scan_set.append(repl)
            scan_set = _dedupe_keep_order(scan_set)[:universe_rows]
            added.append(repl)

            log.info("[ADD] %s → ersätter %s", repl, sym)

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

        if only_trade_on_signal_change and prev_sig == effective_signal:
            update_signal_state(state, sym, effective_signal)
            continue

        buys_today_rec = _counter("buys_today", sym)
        sells_today_rec = _counter("sells_today", sym)

        if effective_signal == "Köp" and max_buys_per_day > 0 and buys_today_rec["count"] >= max_buys_per_day:
            update_signal_state(state, sym, effective_signal)
            log.info("[SKIP] %s → MAX_BUYS_PER_DAY nådd", sym)
            continue

        if effective_signal == "Sälj" and max_sells_per_day > 0 and sells_today_rec["count"] >= max_sells_per_day:
            update_signal_state(state, sym, effective_signal)
            log.info("[SKIP] %s → MAX_SELLS_PER_DAY nådd", sym)
            continue

        current_pos = float(held.get(sym, 0.0))
        qty = auto_qty
        trade = None

        if effective_signal == "Sälj" and current_pos <= 0:
            log.info("[INFO] %s → exit/bearish analys, men ingen position finns. Ingen säljåtgärd.", sym)
            effective_signal = "Håll"

        if autotrade_enabled and risk_ok and market_ok and not _in_cooldown(sym):
            action_signal = effective_signal

            if action_signal == "Sälj" and current_pos <= 0:
                log.info("[SKIP] %s → Sälj ignoreras, ingen position att stänga", sym)
                update_signal_state(state, sym, effective_signal)
                continue

            if action_signal == "Köp" and max_pos_per_symbol > 0:
                remaining_cap = max(0, max_pos_per_symbol - int(current_pos))
                if remaining_cap <= 0:
                    log.info("[SKIP] %s → MAX_POS_PER_SYMBOL nådd", sym)
                    update_signal_state(state, sym, effective_signal)
                    continue
                qty = min(auto_qty, remaining_cap)

            if action_signal == "Sälj":
                qty = min(auto_qty, int(current_pos))
                if qty <= 0:
                    log.info("[SKIP] %s → ingen säljbar position", sym)
                    update_signal_state(state, sym, effective_signal)
                    continue

            price_now = _to_float(raw_technicals.get("price"), 0) or _to_float(raw.get("latestClose"), 0)

            if action_signal == "Köp" and price_now > 0:
                est_value = price_now * qty
                if est_value > max_order_value:
                    log.info("[SKIP] %s → ordervärde %.2f över max %.2f", sym, est_value, max_order_value)
                    update_signal_state(state, sym, effective_signal)
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

                        _log_signal_line("BUY", sym, qty, raw_technicals.get("price"), analysis.get("total_score"))

                        orders_for_report.append(f"BUY submitted: {sym} x{qty}")
                        append_event(
                            "buy_submitted",
                            symbol=sym,
                            name=raw.get("name") or raw.get("companyName") or sym,
                            data={"qty": qty},
                        )

                    elif action_signal == "Sälj":
                        orders_sell += 1
                        sells_today_rec["count"] = int(sells_today_rec.get("count", 0)) + 1
                        state["sells_today"][sym] = sells_today_rec

                        _log_signal_line("EXIT", sym, qty, raw_technicals.get("price"), analysis.get("total_score"))

                        orders_for_report.append(f"SELL submitted: {sym} x{qty}")
                        append_event(
                            "sell_submitted",
                            symbol=sym,
                            name=raw.get("name") or raw.get("companyName") or sym,
                            data={"qty": qty},
                        )

                    state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=exclude_bought_min)).isoformat()
                    state["last_trade_ts"][sym] = _now_utc().isoformat()

                    if sym in scan_set:
                        scan_set.remove(sym)

                    removed.append(sym)
                    removed_this_pass.add(sym)
                    reset_symbol_rotation_state(state, sym)

                    reason_txt = "köpt" if action_signal == "Köp" else "såld"
                    log.info("[REMOVE] %s → %s + exkluderas %d min", sym, reason_txt, exclude_bought_min)

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
                        scan_set = _dedupe_keep_order(scan_set)[:universe_rows]
                        added.append(repl)
                        log.info("[ADD] %s → ersätter %s", repl, sym)

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
                    if debug_autotrade:
                        log.info("[KEEP] %s → ingen verklig order, behålls i universe", sym)
            else:
                log.info("[SKIP] %s → duplicerad ordernyckel", sym)
        else:
            why = []
            if not autotrade_enabled:
                why.append("AUTOTRADE=off")
            if not risk_ok:
                why.append("risk")
            if not market_ok:
                why.append("market_closed")
            if _in_cooldown(sym):
                why.append("cooldown")

            if effective_signal in {"Köp", "Sälj"}:
                if effective_signal == "Köp":
                    paper_buy += 1
                    log.info(
                        "[PAPER-BUY] %s x%s | pris %s | score %s | action=%s",
                        sym,
                        qty,
                        _fmt_price(raw_technicals.get("price")),
                        _fmt_score_plain(analysis.get("total_score")),
                        action,
                    )
                elif effective_signal == "Sälj":
                    paper_sell += 1
                    log.info(
                        "[PAPER-SELL] %s x%s | pris %s | score %s | action=%s",
                        sym,
                        qty,
                        _fmt_price(raw_technicals.get("price")),
                        _fmt_score_plain(analysis.get("total_score")),
                        action,
                    )

                paper_symbols.append(f"{effective_signal}:{sym}")

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
                    },
                )

                orders_for_report.append(
                    f"PAPER-SIM: skulle {effective_signal.lower()} {sym} x{qty} "
                    f"(action={action}, timing={timing_state}, quality={candidate_quality}, "
                    f"price={raw_technicals.get('price')}, score={analysis.get('total_score')}, {','.join(why) or '-'})"
                )

        update_signal_state(state, sym, effective_signal)

    scan_set = _fill_scan_set(scan_set, banned=removed_this_pass)

    if len(scan_set) < universe_rows:
        log.warning(
            "[autoscan] Universe kunde inte fyllas helt (%d/%d). Kandidater saknas efter filter/exclude.",
            len(scan_set),
            universe_rows,
        )

    state["universe"] = list(scan_set)
    replacement_pool_size = len(_available_replacements(scan_set, banned=removed_this_pass))

    save_daily_snapshot(
        state=state,
        summary={
            "universe_size": len(state.get("universe", [])),
            "scan_set_size": len(scan_set),
            "replacement_pool_size": replacement_pool_size,
            "orders_buy": orders_buy,
            "orders_sell": orders_sell,
        },
        scan_set=scan_results,
        portfolio=portfolio_reviews,
        market_open=market_ok,
    )

    save_portfolio_review(portfolio_reviews)

    save_daily_report(
        market_open=market_ok,
        universe_size=len(by_sym),
        scan_set=scan_results,
        replacement_pool_size=replacement_pool_size,
        rotations_out=rotations_out,
        rotations_in=rotations_in,
        orders=orders_for_report + owned_orders_for_report,
    )

    save_state(state)

    if summary_notifs and admin_chat_id:
        add_txt = ", ".join(added) if added else "–"
        rem_txt = ", ".join(removed) if removed else "–"
        msg = [
            "Autoscan klar",
            f"• Aktier (i scan): {len(scan_set)}",
            f"• Ordrar: Köp {orders_buy} · Sälj {orders_sell}",
            f"• Paper-sim: Köp {paper_buy} · Sälj {paper_sell}",
            f"• Owned: EXIT NOW {owned_sell_now} · EXIT SOON {owned_sell_soon} · EXIT WATCH {owned_sell_watch} · CHECK {owned_checked}",
            f"• Byten: + {add_txt}  |  − {rem_txt}",
            f"• Replacement pool: {replacement_pool_size}",
        ]
        await bot.send_message(admin_chat_id, "\n".join(msg))

    scan_watch_syms = [r["symbol"] for r in scan_results if str(r.get("action") or "").lower() == "watch"]
    scan_hold_syms = [r["symbol"] for r in scan_results if str(r.get("action") or "").lower() == "hold_candidate"]
    scan_buy_syms = [r["symbol"] for r in scan_results if str(r.get("action") or "").lower() == "buy_ready"]
    scan_sell_syms = [r["symbol"] for r in scan_results if str(r.get("action") or "").lower() == "exit_ready"]
    scan_exit_soon_syms = [r["symbol"] for r in scan_results if str(r.get("action") or "").lower() == "sell_candidate"]
    scan_exit_watch_syms = [r["symbol"] for r in scan_results if str(r.get("action") or "").lower() == "exit_watch"]

    if state.get("watchlist"):
        log.info("[WATCHLIST] %s", ", ".join(state["watchlist"]))

    scan_grouped = _group_symbols(scan_results, held_only=False)
    portfolio_grouped = _group_symbols(portfolio_reviews, held_only=True)

    _log_section("OWNED SUMMARY")
    log.info("%s %s", _c("STRONG     :", _GREEN, bold=True), _fmt_sym_list(portfolio_grouped["buy_ready"]))
    log.info("%s %s", _c("EXIT NOW   :", _RED, bold=True), _fmt_sym_list(portfolio_grouped["exit_ready"]))
    log.info("%s %s", _c("EXIT SOON  :", _YELLOW, bold=True), _fmt_sym_list(portfolio_grouped["sell_candidate"]))
    log.info("%s %s", _c("EXIT WATCH :", _CYAN, bold=True), _fmt_sym_list(portfolio_grouped["exit_watch"]))
    log.info("%s %s", _c("WAIT       :", _BLUE, bold=True), _fmt_sym_list(portfolio_grouped["watch"]))
    log.info("%s %s", _c("HOLD       :", _YELLOW, bold=True), _fmt_sym_list(portfolio_grouped["hold"]))
    log.info("%s %s", _c("CHECK      :", _RED, bold=True), _fmt_sym_list(portfolio_grouped["review"]))

    _log_section("SCAN SUMMARY")
    log.info(
        "%s  %s  %s  %s  %s  %s",
        _c(f"BUY: {len(scan_buy_syms)}", _GREEN, bold=True),
        _c(f"EXIT: {len(scan_sell_syms)}", _RED, bold=True),
        _c(f"EXIT SOON: {len(scan_exit_soon_syms)}", _YELLOW, bold=True),
        _c(f"EXIT WATCH: {len(scan_exit_watch_syms)}", _CYAN, bold=True),
        _c(f"WATCH: {len(scan_watch_syms)}", _CYAN, bold=True),
        _c(f"HOLD: {len(scan_hold_syms)}", _YELLOW, bold=True),
    )

    log.info("%s %s", _c("SCAN BUY        :", _GREEN, bold=True), _fmt_sym_list(scan_buy_syms))
    log.info("%s %s", _c("SCAN EXIT       :", _RED, bold=True), _fmt_sym_list(scan_sell_syms))
    log.info("%s %s", _c("SCAN EXIT SOON  :", _YELLOW, bold=True), _fmt_sym_list(scan_exit_soon_syms))
    log.info("%s %s", _c("SCAN EXIT WATCH :", _CYAN, bold=True), _fmt_sym_list(scan_exit_watch_syms))
    log.info("%s %s", _c("SCAN WATCH      :", _CYAN, bold=True), _fmt_sym_list(scan_watch_syms))
    log.info("%s %s", _c("SCAN HOLD       :", _YELLOW, bold=True), _fmt_sym_list(scan_hold_syms))

    top_buys = [r for r in scan_results if str(r.get("action") or "").lower() == "buy_ready"][:4]
    top_watch = [r for r in scan_results if str(r.get("action") or "").lower() == "watch"][:3]
    top_exit = [r for r in scan_results if str(r.get("action") or "").lower() in {"exit_ready", "sell_candidate", "exit_watch"}][:3]

    if top_buys:
        log.info("%s", _c("WHY BUY:", _GREEN, bold=True))
        for row in top_buys:
            log.info("  %s", _short_reason_line(row))

    if top_watch:
        log.info("%s", _c("WHY WATCH:", _CYAN, bold=True))
        for row in top_watch:
            log.info("  %s", _short_reason_line(row))

    if top_exit:
        log.info("%s", _c("WHY EXIT:", _RED, bold=True))
        for row in top_exit:
            log.info("  %s", _short_reason_line(row))

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