# main.py
import os
import asyncio
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from yfinance_stock import analyse_stock

import nest_asyncio

load_dotenv()
nest_asyncio.apply()

import logging

logging.getLogger("ib_insync").setLevel(logging.ERROR)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ib_client = None


async def chat_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from chatgpt_client import chat_gpt  # se till att den importen finns

    user_message = update.message.text
    response = chat_gpt(user_message)
    await update.message.reply_text(response)


async def ask_ai_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_message = update.message.text
    if user_message:
        await update.message.reply_text(f"Hämtar aktier med ticker {user_message}...")
        tickers = await analyse_stock(ib_client)
        if tickers:
            await update.message.reply_text(
                f"Hämtade {len(tickers)} aktier: {', '.join(tickers)}"
            )
        else:
            await update.message.reply_text("Inga aktier hittades.")


async def disconnect_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ib_client:
        await ib_client.disconnect_ibkr()
        await update.message.reply_text("IBKR API disconnected!")
    else:
        await update.message.reply_text("IBKR API är redan nedkopplad.")


async def main():
    global ib_client
    from ibkr_client import IbClient  # Lokal import för att undvika cirkulära beroenden

    ib_client = IbClient()
    await ib_client.connect()

    # Skicka in den redan anslutna IbClient-instansen
    await analyse_stock(ib_client)
    # Du kan bearbeta analyzed_stocks vidare om du vill

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat_response))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, ask_ai_stock))
    app.add_handler(CommandHandler("dc", disconnect_command))

    await app.run_polling()
    await ib_client.disconnect_ibkr()


if __name__ == "__main__":
    asyncio.run(main())
