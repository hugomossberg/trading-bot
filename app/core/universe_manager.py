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


def load_state(): 
    state = {
        "hold_streak": {},
        "last_signal": {},
        "universe": [],
        "last_trade_ts": {},
        "buys_today": {},
        "sells_today": {},
        "exclude_until": {},
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

    state["universe"] = _dedupe_keep_order(state.get("universe", []))

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

    if signal == "Håll":
        state["hold_streak"][sym] = int(state["hold_streak"].get(sym, 0)) + 1
    else:
        state["hold_streak"][sym] = 0

    state["last_signal"][sym] = signal


def reset_symbol_rotation_state(state, sym: str):
    """
    Nollställ rotationshistorik för en symbol när den lämnar universe.
    Viktigt för att symbolen inte ska komma tillbaka med gammal hold_streak.
    """
    sym = str(sym).upper().strip()
    if not sym:
        return

    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})

    state["hold_streak"].pop(sym, None)
    state["last_signal"].pop(sym, None)


def rotate_universe(prev_uni, candidates, state):
    """
    Bygger ett stabilt universe.

    Viktigt:
    - Den här funktionen ska INTE längre fatta beslut om hold-streak-drop.
      Det ska autoscan.py göra.
    - Den här funktionen ska bara:
        1) behålla så mycket som möjligt av tidigare universe
        2) respektera exclude_until
        3) fylla på från kandidater
        4) helst undvika symboler som senast var Håll, men bara som preferens

    Returnerar:
    - new_uni
    - dropped (sådant som försvann pga ej längre kandidat / exclude / target-limit)
    - added
    """

    state = state or {}
    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})
    state.setdefault("universe", [])
    state.setdefault("exclude_until", {})

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