from datetime import datetime, timedelta

from app.core.autoscan_shared import now_utc


def ensure_state_defaults(state: dict) -> dict:
    state.setdefault("last_signal", {})
    state.setdefault("exclude_until", {})
    state.setdefault("last_trade_ts", {})
    state.setdefault("buys_today", {})
    state.setdefault("sells_today", {})
    state.setdefault("hold_streak", {})
    state.setdefault("watchlist", [])
    state.setdefault("owned_snapshot", {})
    return state


def state_counter(state: dict, bucket: str, sym: str, today: str) -> dict:
    rec = state.get(bucket, {}).get(sym, {"date": today, "count": 0})
    if rec.get("date") != today:
        rec = {"date": today, "count": 0}
    return rec


def is_in_cooldown(state: dict, sym: str, cooldown_min: int) -> bool:
    ts = state.get("last_trade_ts", {}).get(sym)
    if not ts:
        return False
    try:
        last = datetime.fromisoformat(str(ts))
        return (now_utc() - last) < timedelta(minutes=cooldown_min)
    except Exception:
        return False


def is_excluded(state: dict, sym: str) -> bool:
    iso = state.get("exclude_until", {}).get(sym)
    if not iso:
        return False
    try:
        until = datetime.fromisoformat(str(iso))
        return now_utc() < until
    except Exception:
        return False


def set_exclude_minutes(state: dict, sym: str, minutes: int):
    state["exclude_until"][sym] = (now_utc() + timedelta(minutes=minutes)).isoformat()


def mark_trade_timestamp(state: dict, sym: str):
    state["last_trade_ts"][sym] = now_utc().isoformat()


def increment_day_counter(state: dict, bucket: str, sym: str, today: str):
    rec = state_counter(state, bucket, sym, today)
    rec["count"] = int(rec.get("count", 0)) + 1
    state[bucket][sym] = rec


def store_owned_snapshot(state: dict, row: dict):
    sym = (row.get("symbol") or "").upper().strip()
    if not sym:
        return

    state.setdefault("owned_snapshot", {})
    state["owned_snapshot"][sym] = {
        "symbol": sym,
        "signal": row.get("signal"),
        "action": row.get("action"),
        "candidate_quality": row.get("candidate_quality"),
        "entry_score": row.get("entry_score"),
        "retention_score": row.get("retention_score"),
        "replacement_score": row.get("replacement_score"),
        "timing_state": row.get("timing_state"),
        "entry_reasons": row.get("entry_reasons") or [],
        "raw_technicals": row.get("raw_technicals") or {},
        "total_score": row.get("score", row.get("total_score", 0)),
        "updated_at": row.get("updated_at"),
        "data_source": row.get("data_source", "unknown"),
    }


def apply_symbol_state(
    *,
    state: dict,
    sym: str,
    decision_state: dict,
    signal: str,
    set_decision_state_fn,
    update_signal_state_fn,
    removed_this_pass: set[str] | None = None,
):
    if removed_this_pass and sym in removed_this_pass:
        return

    set_decision_state_fn(state, sym, decision_state)
    update_signal_state_fn(state, sym, signal)