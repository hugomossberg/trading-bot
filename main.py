# main.py
import os
import asyncio
import logging

from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import nest_asyncio
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# --- init ---
load_dotenv()
nest_asyncio.apply()
logging.getLogger("ib_insync").setLevel(logging.ERROR)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or 0)

# egna moduler
from ibkr_client import ib_client
from chatgpt_client import OpenAi, auto_scan_and_trade
from yfinance_stock import analyse_stock

open_ai = OpenAi()


# /dc – koppla ned IBKR
async def disconnect_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ib_client:
        await ib_client.disconnect_ibkr()
        await update.message.reply_text("IBKR API disconnected!")
    else:
        await update.message.reply_text("IBKR API är redan nedkopplad.")


# Körs av schemaläggaren (var 15:e minut)
async def run_auto_trade(app):
    await auto_scan_and_trade(
        bot=app.bot,
        ib_client=ib_client,
        admin_chat_id=ADMIN_CHAT_ID,
    )


async def main():
    # 1) IBKR-anslutning + hämta/lagra aktiedata
    await ib_client.connect()
    await analyse_stock(ib_client)

    # 2) Telegram-app
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Dela klienter globalt för handlers
    app.bot_data["ib"] = ib_client
    app.bot_data["open_ai"] = open_ai

    # Handlers
    app.add_handler(CommandHandler("dc", disconnect_command))
    # AI-router tar hand om både tickers, trade-kommandon och vanlig chat
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, open_ai.ai_router))

    # 3) Schemaläggare (efter att 'app' finns!)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_auto_trade,
        trigger="interval",
        minutes=15,
        args=[app],             # skickar in app till vår coroutine
        id="auto_trade",
        replace_existing=True,
    )
    scheduler.start()
    print("scheduler startad")

    # 4) Starta boten
    await app.run_polling()

    # 5) Städa
    await ib_client.disconnect_ibkr()


if __name__ == "__main__":
    asyncio.run(main())
