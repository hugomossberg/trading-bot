#main.py
import os
import asyncio
import logging

from dotenv import load_dotenv
import nest_asyncio
from telegram.request import HTTPXRequest
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

from app.core.technicals import set_ib_client
from app.brokers.ibkr_client import ib_client
from app.tg_bot.llm_client import LLMClient
from app.tg_bot.router import TelegramRouter
from app.jobs.scheduler import setup_jobs
from app.config import TELEGRAM_TOKEN, ADMIN_CHAT_ID, LOG_LEVEL, TWS_PORT

load_dotenv()
nest_asyncio.apply()


# --- Logging: tydligt för aktier, dämpa httpx/telegram-spam ---
root_level = LOG_LEVEL
logging.basicConfig(
    level=getattr(logging, root_level, logging.INFO),
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.INFO)
log = logging.getLogger("main")

# sätt global nivå från .env
lvl = os.getenv("LOG_LEVEL", "WARNING").upper()
logging.getLogger().setLevel(lvl)

# tysta chatt-/http- och IB-spam
for name in [
    "httpx",
    "apscheduler",
    "telegram.ext.application",
    "telegram.ext.dispatcher",
    "ib_insync.wrapper",
    "ib_insync.client",
    "ib_insync.ib",
]:
    logging.getLogger(name).setLevel(logging.WARNING)

llm_client = LLMClient()
router = TelegramRouter(llm_client)


async def disconnect_command(update, context):
    if ib_client:
        await ib_client.disconnect_ibkr()
        await update.message.reply_text("IBKR API disconnected!")
    else:
        await update.message.reply_text("IBKR API är redan nedkopplad.")


async def nyheter_cmd(update, context):
    from app.jobs.premarket import run_premarket_scan
    await run_premarket_scan(
        context.application.bot,
        ib_client,
        ADMIN_CHAT_ID,
        want_ai=True,
    )


async def main():
    # 1) IBKR
    await ib_client.connect()
    set_ib_client(ib_client)
    log.info("API Connected on %s!", TWS_PORT)

    # 2) Telegram-app (med timeouts)
    request = HTTPXRequest(
        connect_timeout=20.0,
        read_timeout=40.0,
        write_timeout=40.0,
        pool_timeout=20.0,
        connection_pool_size=50,
    )

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).request(request).build()

    # Dela resurser
    app.bot_data["ib"] = ib_client
    app.bot_data["open_ai"] = llm_client

    # Handlers
    app.add_handler(CommandHandler("dc", disconnect_command))
    app.add_handler(CommandHandler("nyheter", nyheter_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, router.handle_text))

    # 3) Schemalägg jobb
    setup_jobs(app, ib_client)

    log.info("API Connected on %s!", TWS_PORT)

    # 4) Kör boten
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    try:
        await asyncio.Event().wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        await ib_client.disconnect_ibkr()


if __name__ == "__main__":
    asyncio.run(main())