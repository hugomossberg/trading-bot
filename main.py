# main.py
import os, asyncio, logging
from dotenv import load_dotenv
import nest_asyncio
from telegram.request import HTTPXRequest
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

from ibkr_client import ib_client
from chatgpt_client import OpenAi   # ai_router + sälja allt + status/tickers
from jobs import setup_jobs

load_dotenv()
nest_asyncio.apply()

# --- Logging: tydligt för aktier, dämpa httpx/telegram-spam ---
root_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, root_level, logging.INFO),
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.INFO)  # lagom
log = logging.getLogger("main")


# sätt global nivå från .env
lvl = os.getenv("LOG_LEVEL", "WARNING").upper()
logging.getLogger().setLevel(lvl)

# tysta chatt-/http- och IB-spam
for name in [
    "httpx",                       # "HTTP/1.1 200 OK"
    "apscheduler",                 # schemaläggarens spam
    "telegram.ext.application",    # telegram-start/stopp
    "telegram.ext.dispatcher",
    "ib_insync.wrapper",
    "ib_insync.client",
    "ib_insync.ib",
]:
    logging.getLogger(name).setLevel(logging.WARNING)



TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN saknas i .env")

open_ai = OpenAi()

try:
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
except ValueError:
    ADMIN_CHAT_ID = 0

async def disconnect_command(update, context):
    if ib_client:
        await ib_client.disconnect_ibkr()
        await update.message.reply_text("IBKR API disconnected!")
    else:
        await update.message.reply_text("IBKR API är redan nedkopplad.")

# manuellt /nyheter – samma som “n”
async def nyheter_cmd(update, context):
    from premarket import run_premarket_scan
    await run_premarket_scan(context.application.bot, ib_client, ADMIN_CHAT_ID, want_ai=True)

async def main():
    # 1) IBKR
    await ib_client.connect()
    log.info("✅ API Connected on %s!", os.getenv("TWS_PORT", "4002"))

    # 2) Telegram-app (med timeouts)
    request = HTTPXRequest(
        connect_timeout=20.0, read_timeout=40.0, write_timeout=40.0,
        pool_timeout=20.0, connection_pool_size=50
    )
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).request(request).build()

    # Dela resurser
    app.bot_data["ib"] = ib_client
    app.bot_data["open_ai"] = open_ai

    # Handlers
    app.add_handler(CommandHandler("dc", disconnect_command))
    app.add_handler(CommandHandler("nyheter", nyheter_cmd))  # /nyheter
    # All text -> OpenAi.ai_router (hanterar "status", "tickers", "sellall", "n", "!", "p", "📰", manuella order m.m.)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, open_ai.ai_router))

    # 3) Schemalägg jobb
    setup_jobs(app, ib_client)

    log.info("✅ API Connected on %s!", os.getenv("TWS_PORT", "4002"))
    # 4) Kör boten
    await app.run_polling()

    # 5) Städning
    await ib_client.disconnect_ibkr()

if __name__ == "__main__":
    asyncio.run(main())
