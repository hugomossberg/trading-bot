#universe_manager.py
import os
import json
from datetime import datetime, timezone

from app.config import STATE_PATH, UNIVERSE_ROWS


def _now_utc():
    return datetime.now(timezone.utc)


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


def _parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _is_excluded(state, sym: str) -> bool:
    sym = str(sym).upper().strip()
    until = state.get("exclude_until", {}).get(sym)
    dt = _parse_dt(until)
    if not dt:
        return False
    return dt > _now_utc()


def _default_exit_state():
    return {
        "stage": 0,
        "bearish_count": 0,
        "last_action": "hold",
        "last_score": 0,
        "last_retention_score": 0,
        "last_timing_state": "unknown",
        "soft_exit_done": False,
        "updated_at": None,
    }


def _default_decision_state():
    return {
        "signal": None,
        "action": None,
        "timing_state": None,
        "pressure": None,
        "exit_mode": None,
        "exit_stage": 0,
        "score_bucket": None,
        "retention_bucket": None,
        "state_label": None,
        "updated_at": None,
    }

def load_state():
    state = {
        "hold_streak": {},
        "last_signal": {},
        "universe": [],
        "last_trade_ts": {},
        "buys_today": {},
        "sells_today": {},
        "exclude_until": {},
        "watchlist": [],
        "exit_state": {},
        "decision_state": {},
    }

    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                state.update(raw)
        except Exception:
            pass

    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})
    state.setdefault("universe", [])
    state.setdefault("last_trade_ts", {})
    state.setdefault("buys_today", {})
    state.setdefault("sells_today", {})
    state.setdefault("exclude_until", {})
    state.setdefault("watchlist", [])
    state.setdefault("exit_state", {})
    state.setdefault("decision_state", {})

    state["universe"] = _dedupe_keep_order(state.get("universe", []))

    if not isinstance(state.get("exit_state"), dict):
        state["exit_state"] = {}

    normalized_exit_state = {}
    for sym, data in state["exit_state"].items():
        sym_up = str(sym).upper().strip()
        if not sym_up:
            continue

        base = _default_exit_state()
        if isinstance(data, dict):
            base.update(data)

        try:
            base["stage"] = int(base.get("stage", 0) or 0)
        except Exception:
            base["stage"] = 0

        try:
            base["bearish_count"] = int(base.get("bearish_count", 0) or 0)
        except Exception:
            base["bearish_count"] = 0

        try:
            base["last_score"] = int(base.get("last_score", 0) or 0)
        except Exception:
            base["last_score"] = 0

        try:
            base["last_retention_score"] = int(base.get("last_retention_score", 0) or 0)
        except Exception:
            base["last_retention_score"] = 0

        base["last_action"] = str(base.get("last_action", "hold") or "hold").strip().lower()
        base["last_timing_state"] = str(base.get("last_timing_state", "unknown") or "unknown").strip().lower()
        base["soft_exit_done"] = bool(base.get("soft_exit_done", False))
        base["updated_at"] = base.get("updated_at")

        normalized_exit_state[sym_up] = base

    state["exit_state"] = normalized_exit_state

    if not isinstance(state.get("decision_state"), dict):
        state["decision_state"] = {}

    normalized_decision_state = {}
    for sym, data in state["decision_state"].items():
        sym_up = str(sym).upper().strip()
        if not sym_up:
            continue

        base = _default_decision_state()
        if isinstance(data, dict):
            base.update(data)

        try:
            base["exit_stage"] = int(base.get("exit_stage", 0) or 0)
        except Exception:
            base["exit_stage"] = 0

        base["signal"] = base.get("signal")
        base["action"] = base.get("action")
        base["timing_state"] = base.get("timing_state")
        base["pressure"] = base.get("pressure")
        base["exit_mode"] = base.get("exit_mode")
        base["score_bucket"] = base.get("score_bucket")
        base["retention_bucket"] = base.get("retention_bucket")
        base["state_label"] = base.get("state_label")
        base["updated_at"] = base.get("updated_at")

        normalized_decision_state[sym_up] = base

    state["decision_state"] = normalized_decision_state

    return state


def save_state(state):
    dir_path = os.path.dirname(STATE_PATH)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, default=str)


def update_signal_state(state, sym: str, signal: str):
    sym = str(sym).upper().strip()
    if not sym:
        return

    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})

    prev_signal = state["last_signal"].get(sym)

    if signal == "Håll":
        if prev_signal == "Håll":
            state["hold_streak"][sym] = int(state["hold_streak"].get(sym, 0)) + 1
        else:
            state["hold_streak"][sym] = 1
    else:
        state["hold_streak"][sym] = 0

    state["last_signal"][sym] = signal


def get_exit_state(state, sym: str) -> dict:
    sym = str(sym).upper().strip()
    if not sym:
        return _default_exit_state()

    state.setdefault("exit_state", {})

    current = state["exit_state"].get(sym)
    if not isinstance(current, dict):
        current = _default_exit_state()
        state["exit_state"][sym] = current
        return current

    merged = _default_exit_state()
    merged.update(current)
    state["exit_state"][sym] = merged
    return merged


def set_exit_state(state, sym: str, exit_state: dict):
    sym = str(sym).upper().strip()
    if not sym:
        return

    state.setdefault("exit_state", {})

    merged = _default_exit_state()
    if isinstance(exit_state, dict):
        merged.update(exit_state)

    try:
        merged["stage"] = int(merged.get("stage", 0) or 0)
    except Exception:
        merged["stage"] = 0

    try:
        merged["bearish_count"] = int(merged.get("bearish_count", 0) or 0)
    except Exception:
        merged["bearish_count"] = 0

    try:
        merged["last_score"] = int(merged.get("last_score", 0) or 0)
    except Exception:
        merged["last_score"] = 0

    try:
        merged["last_retention_score"] = int(merged.get("last_retention_score", 0) or 0)
    except Exception:
        merged["last_retention_score"] = 0

    merged["last_action"] = str(merged.get("last_action", "hold") or "hold").strip().lower()
    merged["last_timing_state"] = str(merged.get("last_timing_state", "unknown") or "unknown").strip().lower()
    merged["soft_exit_done"] = bool(merged.get("soft_exit_done", False))
    merged["updated_at"] = merged.get("updated_at") or _now_utc().isoformat()

    state["exit_state"][sym] = merged


def get_decision_state(state, sym: str) -> dict:
    sym = str(sym).upper().strip()
    if not sym:
        return _default_decision_state()

    state.setdefault("decision_state", {})

    current = state["decision_state"].get(sym)
    if not isinstance(current, dict):
        current = _default_decision_state()
        state["decision_state"][sym] = current
        return current

    merged = _default_decision_state()
    merged.update(current)
    state["decision_state"][sym] = merged
    return merged


def set_decision_state(state, sym: str, decision_state: dict):
    sym = str(sym).upper().strip()
    if not sym:
        return

    state.setdefault("decision_state", {})

    merged = _default_decision_state()
    if isinstance(decision_state, dict):
        merged.update(decision_state)

    try:
        merged["exit_stage"] = int(merged.get("exit_stage", 0) or 0)
    except Exception:
        merged["exit_stage"] = 0

    merged["updated_at"] = _now_utc().isoformat()
    state["decision_state"][sym] = merged



def reset_exit_state(state, sym: str):
    sym = str(sym).upper().strip()
    if not sym:
        return

    state.setdefault("exit_state", {})
    state["exit_state"].pop(sym, None)


def reset_symbol_rotation_state(state, sym: str):
    """
    Nollställ rotationshistorik för en symbol när den lämnar universe.
    Viktigt för att symbolen inte ska komma tillbaka med gammal hold_streak
    eller gammalt exit-state.
    """
    sym = str(sym).upper().strip()
    if not sym:
        return

    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})
    state.setdefault("exit_state", {})
    state.setdefault("decision_state", {})

    state["hold_streak"].pop(sym, None)
    state["last_signal"].pop(sym, None)
    state["exit_state"].pop(sym, None)
    state["decision_state"].pop(sym, None)

def rotate_universe(prev_uni, candidates, state):
    """
    Bygger ett stabilt universe.

    Viktigt:
    - Den här funktionen ska INTE fatta beslut om hold-streak-drop.
      Det ska autoscan.py göra.
    - Den här funktionen ska bara:
        1) behålla så mycket som möjligt av tidigare universe
        2) respektera exclude_until
        3) fylla på från kandidater
        4) helst undvika symboler som senast var Håll, men bara som preferens

    Returnerar:
    - new_uni
    - dropped
    - added
    """

    state = state or {}
    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})
    state.setdefault("universe", [])
    state.setdefault("exclude_until", {})
    state.setdefault("exit_state", {})
    state.setdefault("decision_state", {})

    prefer_non_hold = os.getenv("PREFER_NON_HOLD", "1").lower() in {"1", "true", "yes", "on"}

    prev_uni = _dedupe_keep_order(prev_uni)
    candidates = _dedupe_keep_order(candidates)

    target = max(1, int(UNIVERSE_ROWS))
    candidate_set = set(candidates)
    last_signal = state.get("last_signal", {})

    # 1. behåll gamla symboler om:
    #    - de fortfarande finns i candidates
    #    - de inte är excludade
    keep = []
    for sym in prev_uni:
        if sym not in candidate_set:
            continue
        if _is_excluded(state, sym):
            continue
        keep.append(sym)

    keep = _dedupe_keep_order(keep)

    # 2. tillgängliga kandidater som inte redan finns i keep och inte är excludade
    available = []
    keep_set = set(keep)

    for sym in candidates:
        if sym in keep_set:
            continue
        if _is_excluded(state, sym):
            continue
        available.append(sym)

    # 3. preferera kandidater som inte senast hade Håll
    if prefer_non_hold:
        preferred = [sym for sym in available if last_signal.get(sym) != "Håll"]
        fallback = [sym for sym in available if last_signal.get(sym) == "Håll"]
        available = preferred + fallback

    # 4. bygg nytt universe upp till target
    new_uni = keep[:]
    for sym in available:
        if len(new_uni) >= target:
            break
        if sym not in new_uni:
            new_uni.append(sym)

    new_uni = _dedupe_keep_order(new_uni)[:target]

    dropped = [sym for sym in prev_uni if sym not in new_uni]
    added = [sym for sym in new_uni if sym not in prev_uni]

    return new_uni, dropped, added