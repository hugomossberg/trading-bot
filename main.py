import json
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from chatgpt_client import chat_gpt

reminders = []
def save_reminders():
    with open("reminders.json",  "w") as f:
        json.dump(reminders, f)


def load_reminders():
    global reminders
    try:
        with open("reminders.json", "r") as f:
            reminders = json.load(f)
    except FileNotFoundError:
        reminders = []

async def Start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Hej! Jag är din bot.")

async def add_remind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    if len(context.args) == 0:
        await update.message.reply_text("Använd: /remind  <din påminnelse> ")
        return
    reminder_text = "".join(context.args)
    reminders.append(reminder_text)
    save_reminders()

    await update.message.reply_text(f"Påminnelse tillagd: {reminder_text}")

async def display_reminds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(reminders) == 0:
        await update.message.reply_text("Inga påminnelser")
        return
    await update.message.reply_text(f"{reminders}")

async def delete_reminds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reminders.clear()
    await update.message.reply_text("påminnelser raderade ")

async def chat_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Svara på alla meddelanden med ChatGPT"""
    user_message = update.message.text  # Hämta användarens textmeddelande
    response = chat_gpt(user_message)  # Skicka till OpenAI API
    await update.message.reply_text(response)  # Skicka tillbaka svaret till chatten

    


def main():
    load_reminders()

    app = ApplicationBuilder().token("7589907637:AAFN5ZuDZ5PoRdzJLsBhvKF1BBZoJLd4mgw").build()
    
    app.add_handler(CommandHandler("deletelist", delete_reminds))
    app.add_handler(CommandHandler("start", Start))
    app.add_handler(CommandHandler("remind", add_remind))
    app.add_handler(CommandHandler("list", display_reminds))


    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat_response))
    app.run_polling()

if __name__ == "__main__":

    main()

