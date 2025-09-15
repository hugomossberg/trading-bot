import os, logging
from datetime import time
from zoneinfo import ZoneInfo
from telegram.ext import Application

from autoscan import run_autoscan_once
from premarket import run_premarket_scan

log = logging.getLogger("jobs")
SE_TZ = ZoneInfo("Europe/Stockholm")
US_TZ = ZoneInfo("America/New_York")

def _env_int(key, default):
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default

def setup_jobs(app: Application, ib_client):
    # Autoscan var X minut(er)
    every_min = _env_int("REFRESH_MINUTES", 2)
    app.job_queue.run_repeating(
        lambda ctx: run_autoscan_once(app.bot, ib_client, int(os.getenv("ADMIN_CHAT_ID","0") or "0")),
        interval=every_min*60,
        first=5,
        name="autoscan_job",
        job_kwargs={"misfire_grace_time": 30},
    )
    log.info("Autoscan schemalagd var %d min.", every_min)

    # Premarket dagligen (ET)
    et_str = os.getenv("PREMARKET_ET", "09:10")
    try:
        hh, mm = [int(x) for x in et_str.split(":")]
    except Exception:
        hh, mm = 9, 10

    app.job_queue.run_daily(
        lambda ctx: run_premarket_scan(app.bot, ib_client, int(os.getenv("ADMIN_CHAT_ID","0") or "0"),
                                       want_ai=True, open_ai=app.bot_data.get("open_ai")),
        time=time(hh, mm, tzinfo=US_TZ),
        days=(0,1,2,3,4),  # mån-fre
        name="premarket_job",
        job_kwargs={"misfire_grace_time": 300},
    )
    log.info("Premarket schemalagd %02d:%02d ET (mån–fre).", hh, mm)
