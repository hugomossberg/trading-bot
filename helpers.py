# helpers.py
import os, time
from datetime import datetime
from zoneinfo import ZoneInfo

# Telegram long message
async def send_long_message(bot, chat_id, text):
    max_length = 4096
    for i in range(0, len(text), max_length):
        await bot.send_message(chat_id, text[i:i+max_length])

def convert_keys_to_str(d):
    if isinstance(d, dict):
        return {str(k): convert_keys_to_str(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [convert_keys_to_str(item) for item in d]
    else:
        return d

# --- Kill switch (din befintliga logik; trygg casting) ---
_KILL = {"on": False, "reason": ""}
try:
    MAX_DAILY_LOSS = float(os.getenv("RISK_MAX_DAILY_LOSS", "250"))
except ValueError:
    MAX_DAILY_LOSS = 250.0

def kill_switch_ok(pnl_realized_today: float, pnl_unrealized_open: float):
    if _KILL["on"]:
        return False, _KILL["reason"]
    if (pnl_realized_today + pnl_unrealized_open) <= -abs(MAX_DAILY_LOSS):
        _KILL.update(on=True, reason=f"Daily loss ≤ -{MAX_DAILY_LOSS}")
        return False, _KILL["reason"]
    return True, None

def panic_on(reason="manual panic"): _KILL.update(on=True, reason=reason)
def panic_off(): _KILL.update(on=False, reason="")

# --- Marknadsöppettider (enkel RTH-check) ---
US_TZ = ZoneInfo("America/New_York")
def us_market_open_now(now_et: datetime | None = None) -> bool:
    now_et = now_et or datetime.now(US_TZ)
    if now_et.weekday() >= 5:
        return False
    start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end   = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return start <= now_et <= end

# --- Enkel dubblettskydd med TTL i minnet ---
_DUP_CACHE = {}
_DUP_TTL_SEC = int(os.getenv("DUP_TTL_SEC", "45"))

def is_dup(key: str) -> bool:
    now = time.time()
    # städa
    dead = [k for k, ts in _DUP_CACHE.items() if now - ts > _DUP_TTL_SEC]
    for k in dead:
        _DUP_CACHE.pop(k, None)
    if key in _DUP_CACHE:
        return True
    _DUP_CACHE[key] = now
    return False
