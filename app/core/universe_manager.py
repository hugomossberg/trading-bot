#universe_manager.py
import os
import json
from datetime import datetime, timezone

from app.config import STATE_PATH, UNIVERSE_ROWS


def _now_utc():
    return datetime.now(timezone.utc)


def load_state():
    state = {
        "hold_streak": {},
        "last_signal": {},
        "universe": [],
        "last_trade_ts": {},
        "buys_today": {},
        "sells_today": {},
    }

    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                state.update(raw)
        except Exception:
            pass

    return state


def save_state(state):
    dir_path = os.path.dirname(STATE_PATH)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, default=str)


def update_signal_state(state, sym: str, signal: str):
    sym = sym.upper()
    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})

    if signal == "Håll":
        state["hold_streak"][sym] = state["hold_streak"].get(sym, 0) + 1
    else:
        state["hold_streak"][sym] = 0

    state["last_signal"][sym] = signal


def rotate_universe(prev_uni, candidates, state):
    state = state or {}
    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})
    state.setdefault("universe", [])

    drop_if_hold_streak = int(os.getenv("DROP_IF_HOLD_STREAK", "1"))
    churn_min = int(os.getenv("CHURN_MIN", "0"))
    prefer_non_hold = os.getenv("PREFER_NON_HOLD", "1").lower() in {"1", "true", "yes", "on"}

    prev_uni = [s.upper() for s in (prev_uni or [])]
    cands = [c.upper() for c in (candidates or []) if c]

    # 1) droppa de med för lång "Håll"-svit
    to_drop = [s for s in prev_uni if state["hold_streak"].get(s, 0) >= drop_if_hold_streak]

    # 2) churna minst CHURN_MIN
    extra_needed = max(0, churn_min - len(to_drop))
    if extra_needed > 0:
        for s in prev_uni:
            if s in to_drop:
                continue
            to_drop.append(s)
            if len(to_drop) >= churn_min:
                break

    keep = [s for s in prev_uni if s not in set(to_drop)]

    # 3) fyll på från kandidater (helst de som inte är "Håll")
    new_candidates = [c for c in cands if c not in set(keep)]
    if prefer_non_hold:
        non_hold = [c for c in new_candidates if state["last_signal"].get(c) != "Håll"]
        holders = [c for c in new_candidates if state["last_signal"].get(c) == "Håll"]
        new_candidates = non_hold + holders

    new_uni = (keep + new_candidates)[:UNIVERSE_ROWS]
    dropped = sorted(set(prev_uni) - set(new_uni))
    added = sorted(set(new_uni) - set(prev_uni))
    return new_uni, dropped, added