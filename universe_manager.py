# universe_manager.py
import os, json
from datetime import datetime, timezone

STATE_PATH = os.getenv("STATE_PATH", "trade_state.json") 

def _now_utc():
    return datetime.now(timezone.utc)

def load_state():
    # baseline med alla nycklar vi använder
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
                state.update(raw)  # uppgradera gamla filer
        except Exception:
            pass
    return state

def save_state(state):
    # (frivilligt: skapa katalogen om du sätter STATE_PATH till t.ex. data/trade_state.json)
    dir_ = os.path.dirname(STATE_PATH)
    if dir_:
        os.makedirs(dir_, exist_ok=True)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, default=str)

    ####### inte under denna rad
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
    # 🔒 säkra defaults
    state = state or {}
    state.setdefault("hold_streak", {})
    state.setdefault("last_signal", {})
    state.setdefault("universe", [])

    UNIVERSE_ROWS        = int(os.getenv("UNIVERSE_ROWS", "10"))
    DROP_IF_HOLD_STREAK  = int(os.getenv("DROP_IF_HOLD_STREAK", "1"))
    CHURN_MIN            = int(os.getenv("CHURN_MIN", "0"))
    PREFER_NON_HOLD      = os.getenv("PREFER_NON_HOLD", "1").lower() in {"1","true","yes","on"}

    prev_uni = [s.upper() for s in (prev_uni or [])]
    cands    = [c.upper() for c in (candidates or []) if c]

    # 1) droppa de med för lång "Håll"-svit
    to_drop = [s for s in prev_uni if state["hold_streak"].get(s, 0) >= DROP_IF_HOLD_STREAK]

    # 2) churna minst CHURN_MIN
    extra_needed = max(0, CHURN_MIN - len(to_drop))
    if extra_needed > 0:
        for s in prev_uni:
            if s in to_drop:
                continue
            to_drop.append(s)
            if len(to_drop) >= CHURN_MIN:
                break

    keep = [s for s in prev_uni if s not in set(to_drop)]

    # 3) fyll på från kandidater (helst de som inte är "Håll")
    new_candidates = [c for c in cands if c not in set(keep)]
    if PREFER_NON_HOLD:
        non_hold = [c for c in new_candidates if state["last_signal"].get(c) != "Håll"]
        holders  = [c for c in new_candidates if state["last_signal"].get(c) == "Håll"]
        new_candidates = non_hold + holders

    new_uni = (keep + new_candidates)[:UNIVERSE_ROWS]
    dropped = sorted(set(prev_uni) - set(new_uni))
    added   = sorted(set(new_uni) - set(prev_uni))
    return new_uni, dropped, added
