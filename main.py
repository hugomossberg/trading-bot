import json
import requests
import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from chatgpt_client import chat_gpt
from ibkr_client import IbClient


load_dotenv()


TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")


async def chat_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Svara på alla meddelanden med ChatGPT"""
    user_message = update.message.text  # Hämta användarens textmeddelande
    response = chat_gpt(user_message)  # Skicka till OpenAI API
    await update.message.reply_text(response)  # Skicka tillbaka svaret till chatten


def main():
    # telegram
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # IBKR
    ib_client = IbClient()
    ib_client.get_stock()

    # openapi
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat_response))

    # disconnect IBKR API
    ib_client.disconnect_ibkr()

    app.run_polling()


if __name__ == "__main__":
    main()
