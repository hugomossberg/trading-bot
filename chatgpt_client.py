import re
import json
import openai
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


load_dotenv()

API_KEY = os.getenv("CHATGPT_API")

class OpenAi:
    def __init__(self):
        self.api_key = API_KEY
    async def chat_gpt(self, user_message):
        try:
            client = openai.OpenAI(api_key = self.api_key)  # Ange API-nyckeln här
            response = client.chat.completions.create(
                model="gpt-4o-2024-08-06",  # gpt-3.5-turbo
                messages=[
                    {
                        "role": "system",
                        "content": "Jag är en noggran ai som gillar detaljer",
                    },
                    {"role": "user", "content": user_message},
                ],
            )
            return response.choices[0].message.content  # Returnera GPT-svaret

        except openai.OpenAIError as e:
            print(f"OpenAI API-fel: {e}")
            return "Ett fel uppstod vid anropet till OpenAI."
        except Exception as e:
            print(f"Ett oväntat fel uppstod: {e}")
            return "Ett oväntat fel inträffade."

    async def chat_response(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:  
        user_message = update.message.text
        response = await self.chat_gpt(user_message)
        await update.message.reply_text(response)

    async def ask_ai_stock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        print("🔵 ask_ai_stock har anropats!")  # 👀 Detta borde alltid synas

        with open("Stock_info.json", "r", encoding="utf-8") as json_open:
            data = json.load(json_open)

        # Försök extrahera en aktiesymbol från användarens text
        match = re.search(r"\b[A-Z]{2,}\b", update.message.text)
        if match:
            symbol = match.group(0)
        else:
            symbol = update.message.text.upper()

        print(f"🔍 Extraherad symbol: {symbol}")
        print(f"📃 Alla tillgängliga symboler: {[stock['symbol'] for stock in data]}")

        # Kolla om symbolen finns i JSON
        for stock in data:
            if stock["symbol"].upper() == symbol:
                print(f"✅ Match hittad för: {stock['symbol']}")
                await update.message.reply_text(f"Aktieinfo för {stock['name']}:")
                await update.message.reply_text(f"Latest Close: {stock['latestClose']}")
                await update.message.reply_text(f"P/E: {stock['PE']}")
                
            
                news_list = stock.get("News")
                if news_list:
                    for news in news_list:
                        title = news["content"].get("title", "Ingen titel")
                        summary = news["content"].get("summary", "ingen sammanfattning")
                        await update.message.reply_text(f"Nyhet: {title}\nSammanfattning: {summary}")
                else:
                    await update.message.reply_text("Inga nyhetsartiklar för denna aktie.")
                await update.message.reply_text(f"Sektor: {stock.get('sector', 'okänd')}")
                return
        print(f"❌ Aktie med symbolen {symbol} hittades INTE i JSON!")
    

        # Om aktien inte hittades, fråga ChatGPT istället
        try:
            response = await self.chat_gpt(update.message.text)
            await update.message.reply_text(response)
        except Exception as e:
            await update.message.reply_text(f"Ett fel uppstod: {str(e)}")
