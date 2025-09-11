# universe.py
import json, os, time
from typing import List, Dict, Tuple

UNIVERSE_FILE = "Universe.json"
STATE_FILE = "Universe_state.json"

def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_universe() -> List[str]:
    return _load_json(UNIVERSE_FILE, [])

def save_universe(tickers: List[str]):
    _save_json(UNIVERSE_FILE, tickers)

def load_state() -> Dict[str, dict]:
    return _load_json(STATE_FILE, {})

def save_state(state: Dict[str, dict]):
    _save_json(STATE_FILE, state)

def seed_universe_if_missing(tickers: List[str]):
    cur = load_universe()
    if not cur:
        save_universe(tickers)

def update_signals_in_state(state: Dict[str, dict], symbol: str, signal: str):
    entry = state.get(symbol, {"added_at": time.time(), "hold_streak": 0})
    # uppdatera streak
    if signal == "Håll":
        entry["hold_streak"] = int(entry.get("hold_streak", 0)) + 1
    else:
        entry["hold_streak"] = 0
    entry["last_signal"] = signal
    entry["last_update_ts"] = time.time()
    state[symbol] = entry

def rotate_universe(
    universe: List[str],
    state: Dict[str, dict],
    held_symbols: set,
    candidates_ranked: List[str],
    max_slots: int,
    drop_if_hold_streak: int,
) -> List[str]:
    # 1) behåll alltid tickers där du har position
    keep = []
    for sym in universe:
        st = state.get(sym, {})
        if sym in held_symbols:
            keep.append(sym)
            continue
        # droppa om Håll-streak är för hög
        if int(st.get("hold_streak", 0)) >= drop_if_hold_streak:
            continue
        keep.append(sym)

    # 2) fyll på med kandidater (utan dubbletter)
    seen = set(keep)
    for sym in candidates_ranked:
        if len(keep) >= max_slots:
            break
        if sym in seen:
            continue
        keep.append(sym)
        seen.add(sym)

    return keep[:max_slots]
