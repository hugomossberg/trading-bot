import json
import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
import yfinance as yf
from chatgpt_client import chat_gpt

load_dotenv()



TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")


async def chat_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Svara på alla meddelanden med ChatGPT"""
    user_message = update.message.text  # Hämta användarens textmeddelande
    response = chat_gpt(user_message)  # Skicka till OpenAI API
    await update.message.reply_text(response)  # Skicka tillbaka svaret till chatten



async def user_ask_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_message = update.message.text
    ai_respons = chat_gpt(f"Ge mig en analys i JSON-format: {user_message}")
    

    


def main():
 

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()




    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat_response))
    app.run_polling()

if __name__ == "__main__":
    main()

