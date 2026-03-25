import os
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo
from telegram.ext import Application

from app.core.autoscan import run_autoscan_once
from app.jobs.premarket import run_premarket_scan

log = logging.getLogger("jobs")
SE_TZ = ZoneInfo("Europe/Stockholm")
US_TZ = ZoneInfo("America/New_York")


def _env_int(key, default):
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def premarket_schedule_text_sv(hour_et: int = 9, minute_et: int = 10) -> str:
    now_us = datetime.now(US_TZ)
    sched_us = now_us.replace(hour=hour_et, minute=minute_et, second=0, microsecond=0)
    sched_se = sched_us.astimezone(SE_TZ)

    return (
        f"Premarket schemalagd {sched_us:%H:%M} ET / "
        f"{sched_se:%H:%M} svensk tid (mån-fre)."
    )


def setup_jobs(app: Application, ib_client):
    every_min = _env_int("REFRESH_MINUTES", 2)

    app.job_queue.run_repeating(
        lambda ctx: run_autoscan_once(
            app.bot,
            ib_client,
            int(os.getenv("ADMIN_CHAT_ID", "0") or "0"),
        ),
        interval=every_min * 60,
        first=5,
        name="autoscan_job",
        job_kwargs={
            "misfire_grace_time": 30,
            "max_instances": 1,
            "coalesce": True,
        },
    )
    log.info("Autoscan scheduled every %d minutes.", every_min)

    et_str = os.getenv("PREMARKET_ET", "09:10")
    try:
        hh, mm = [int(x) for x in et_str.split(":")]
    except Exception:
        hh, mm = 9, 10

    app.job_queue.run_daily(
        lambda ctx: run_premarket_scan(
            app.bot,
            ib_client,
            int(os.getenv("ADMIN_CHAT_ID", "0") or "0"),
            want_ai=True,
            open_ai=app.bot_data.get("open_ai"),
        ),
        time=time(hh, mm, tzinfo=US_TZ),
        days=(0, 1, 2, 3, 4),
        name="premarket_job",
        job_kwargs={
            "misfire_grace_time": 300,
            "max_instances": 1,
            "coalesce": True,
        },
    )

    log.info("%s", premarket_schedule_text_sv(hh, mm))