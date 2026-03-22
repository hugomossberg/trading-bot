import os, time
from datetime import datetime
from zoneinfo import ZoneInfo
from app.core.market_profile import PROFILE

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

def market_open_now(now_local: datetime | None = None) -> bool:
    if os.getenv("SIM_MARKET", "").strip().lower() in {"1", "true", "yes", "on"}:
        return True

    tz = ZoneInfo(PROFILE["timezone"])
    now_local = now_local or datetime.now(tz)

    if now_local.weekday() >= 5:
        return False

    start = now_local.replace(
        hour=PROFILE["open_hour"],
        minute=PROFILE["open_minute"],
        second=0,
        microsecond=0,
    )
    end = now_local.replace(
        hour=PROFILE["close_hour"],
        minute=PROFILE["close_minute"],
        second=0,
        microsecond=0,
    )
    return start <= now_local <= end

_DUP_CACHE = {}
_DUP_TTL_SEC = int(os.getenv("DUP_TTL_SEC", "45"))

def is_dup(key: str) -> bool:
    now = time.time()
    dead = [k for k, ts in _DUP_CACHE.items() if now - ts > _DUP_TTL_SEC]
    for k in dead:
        _DUP_CACHE.pop(k, None)
    if key in _DUP_CACHE:
        return True
    _DUP_CACHE[key] = now
    return False