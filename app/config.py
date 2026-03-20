#config.py
import os


def env_bool(key: str, default: bool = False) -> bool:
    value = os.getenv(key, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on", "y"}


def env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID = env_int("ADMIN_CHAT_ID", 0)
TWS_PORT = env_int("TWS_PORT", 4002)

STATE_PATH = os.getenv("STATE_PATH", "storage/trade_state.json")
STOCK_INFO_PATH = os.getenv("STOCK_INFO_PATH", "storage/Stock_info.json")

AUTOSCAN = env_bool("AUTOSCAN", True)
AUTOTRADE = env_bool("AUTOTRADE", False)
REFRESH_MINUTES = env_int("REFRESH_MINUTES", 2)
UNIVERSE_ROWS = env_int("UNIVERSE_ROWS", 10)
CANDIDATE_MULTIPLIER = env_int("CANDIDATE_MULTIPLIER", 2)
AUTO_QTY = env_int("AUTO_QTY", 2)

SUMMARY_NOTIFS = env_bool("SUMMARY_NOTIFS", True)
LOG_UNIVERSE = env_bool("LOG_UNIVERSE", True)
DEBUG_AUTOTRADE = env_bool("DEBUG_AUTOTRADE", True)
DROP_IF_HOLD_STREAK = env_int("DROP_IF_HOLD_STREAK", 3)