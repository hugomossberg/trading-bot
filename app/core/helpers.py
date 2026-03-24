import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.market_profile import PROFILE


# Telegram long message
async def send_long_message(bot, chat_id, text):
    max_length = 4096
    for i in range(0, len(text), max_length):
        await bot.send_message(chat_id, text[i:i + max_length])


def convert_keys_to_str(d):
    if isinstance(d, dict):
        return {str(k): convert_keys_to_str(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [convert_keys_to_str(item) for item in d]
    else:
        return d


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on", "y"}


# =========================
# Kill switch
# =========================
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


def panic_on(reason="manual panic"):
    _KILL.update(on=True, reason=reason)


def panic_off():
    _KILL.update(on=False, reason="")


# =========================
# Market time helpers
# =========================
_SWEDEN_TZ = ZoneInfo("Europe/Stockholm")

_PHASE_LABEL_SV = {
    "sim": "SIM",
    "regular": "ORDINARIE",
    "premarket": "PREMARKET",
    "afterhours": "AFTER MARKET",
    "overnight": "OVERNIGHT",
    "closed": "STÄNGD",
}


def _coerce_market_now(now_local: datetime | None, market_tz: ZoneInfo) -> datetime:
    if now_local is None:
        return datetime.now(market_tz)

    if now_local.tzinfo is None:
        return now_local.replace(tzinfo=market_tz)

    return now_local.astimezone(market_tz)


def get_market_session_info(now_local: datetime | None = None) -> dict:
    """
    Returnerar tydlig marknadsstatus för US-marknaden.
    Beslut tas i marknadens timezone, men vi visar även svensk tid för terminalen.
    """
    market_tz = ZoneInfo(PROFILE["timezone"])
    now_market = _coerce_market_now(now_local, market_tz)
    now_sweden = now_market.astimezone(_SWEDEN_TZ)

    if _env_bool("SIM_MARKET", False):
        return {
            "market_open": True,
            "phase": "sim",
            "phase_sv": _PHASE_LABEL_SV["sim"],
            "allow_extended": True,
            "allow_overnight": True,
            "now_market": now_market,
            "now_sweden": now_sweden,
        }

    allow_extended = _env_bool("ALLOW_EXTENDED_HOURS", False)
    allow_overnight = _env_bool("ALLOW_OVERNIGHT_HOURS", False)

    weekday = now_market.weekday()  # Mon=0 ... Sun=6
    hhmm = now_market.hour * 60 + now_market.minute

    regular_start = PROFILE["open_hour"] * 60 + PROFILE["open_minute"]   # ex 09:30
    regular_end = PROFILE["close_hour"] * 60 + PROFILE["close_minute"]   # ex 16:00

    premarket_start = 4 * 60          # 04:00 ET
    afterhours_end = 20 * 60          # 20:00 ET
    overnight_end = 3 * 60 + 50       # 03:50 ET

    phase = "closed"
    market_open = False

    # 1) Regular market, måndag-fredag
    if weekday < 5 and regular_start <= hhmm <= regular_end:
        phase = "regular"
        market_open = True

    # 2) Premarket, måndag-fredag
    elif allow_extended and weekday < 5 and premarket_start <= hhmm < regular_start:
        phase = "premarket"
        market_open = True

    # 3) After hours, måndag-fredag
    elif allow_extended and weekday < 5 and regular_end < hhmm <= afterhours_end:
        phase = "afterhours"
        market_open = True

    # 4) Overnight session (separat från vanlig outsideRTH)
    #    Gäller söndag kväll -> fredag morgon
    elif allow_overnight:
        evening_session = weekday in {6, 0, 1, 2, 3} and hhmm >= afterhours_end
        morning_session = weekday in {0, 1, 2, 3, 4} and hhmm <= overnight_end

        if evening_session or morning_session:
            phase = "overnight"
            market_open = True

    return {
        "market_open": market_open,
        "phase": phase,
        "phase_sv": _PHASE_LABEL_SV.get(phase, phase.upper()),
        "allow_extended": allow_extended,
        "allow_overnight": allow_overnight,
        "now_market": now_market,
        "now_sweden": now_sweden,
    }


def market_open_now(now_local: datetime | None = None) -> bool:
    return bool(get_market_session_info(now_local)["market_open"])


def market_status_text_sv(now_local: datetime | None = None) -> str:
    """
    Snygg terminaltext i svensk tid + market time.
    """
    info = get_market_session_info(now_local)

    now_market = info["now_market"]
    now_sweden = info["now_sweden"]

    market_clock = now_market.strftime("%Y-%m-%d %H:%M:%S %Z")
    sweden_clock = now_sweden.strftime("%Y-%m-%d %H:%M:%S %Z")

    handel = "JA" if info["market_open"] else "NEJ"
    ext = "JA" if info["allow_extended"] else "NEJ"
    ovn = "JA" if info["allow_overnight"] else "NEJ"

    return (
        f"MARKNAD: {info['phase_sv']} | "
        f"USA: {market_clock} | "
        f"Sverige: {sweden_clock} | "
        f"Handel möjlig: {handel} | "
        f"Extended: {ext} | "
        f"Overnight: {ovn}"
    )


# =========================
# Duplicate guard
# =========================
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