#config.py
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=BASE_DIR / ".env")




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


# ===== Base paths =====
STORAGE_DIR = Path("storage")
STATE_DIR = STORAGE_DIR / "state"
SNAPSHOT_DIR = STORAGE_DIR / "snapshots"
EVENTS_DIR = STORAGE_DIR / "events"
REPORTS_DIR = STORAGE_DIR / "reports"

STATE_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ===== JSON files =====
STATE_PATH = Path(os.getenv("STATE_PATH", str(STATE_DIR / "trade_state.json")))
SIGNAL_LOG_PATH = Path(os.getenv("SIGNAL_LOG_PATH", str(STATE_DIR / "signal_log.jsonl")))
STOCK_INFO_PATH = Path(os.getenv("STOCK_INFO_PATH", str(STATE_DIR / "stock_info.json")))
PIPELINE_SNAPSHOT_PATH = Path(os.getenv("PIPELINE_SNAPSHOT_PATH", str(STATE_DIR / "pipeline_snapshot.json")))
FINAL_CANDIDATES_PATH = Path(os.getenv("FINAL_CANDIDATES_PATH", str(STATE_DIR / "final_candidates.json")))

REBUILD_LOCK_PATH = Path(
    os.getenv("REBUILD_LOCK_PATH", str(STATE_DIR / "stock_info_rebuild.lock"))
)

# ===== Env config =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID = env_int("ADMIN_CHAT_ID", 0)
TWS_PORT = env_int("TWS_PORT", 4002)

AUTOSCAN = env_bool("AUTOSCAN", True)
AUTOTRADE = env_bool("AUTOTRADE", False)
REFRESH_MINUTES = env_int("REFRESH_MINUTES", 2)
UNIVERSE_ROWS = env_int("UNIVERSE_ROWS", 10)
CANDIDATE_MULTIPLIER = env_int("CANDIDATE_MULTIPLIER", 2)
AUTO_QTY = env_int("AUTO_QTY", 2)
NO_BUY_FIRST_MINUTES_AFTER_OPEN = env_int("NO_BUY_FIRST_MINUTES_AFTER_OPEN", 30)

SUMMARY_NOTIFS = env_bool("SUMMARY_NOTIFS", True)
LOG_UNIVERSE = env_bool("LOG_UNIVERSE", True)
DEBUG_AUTOTRADE = env_bool("DEBUG_AUTOTRADE", True)
DROP_IF_HOLD_STREAK = env_int("DROP_IF_HOLD_STREAK", 3)

ALLOW_ADD_TO_EXISTING = env_bool("ALLOW_ADD_TO_EXISTING", False)