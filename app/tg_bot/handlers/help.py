from app.tg_bot.formatters import format_help


async def send_help(update, context):
    await update.message.reply_text(format_help())