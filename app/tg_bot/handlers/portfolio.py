from app.tg_bot.formatters import format_portfolio
from app.tg_bot.ib_views import extract_positions


async def send_portfolio(update, context):
    ack = await update.message.reply_text("Checking portfolio...")
    ib_client = context.application.bot_data.get("ib")
    if not ib_client:
        return await ack.edit_text("IB client missing in bot_data.")

    ib = ib_client.ib
    if not ib.isConnected():
        return await ack.edit_text("IB not connected.")

    try:
        positions = await ib.reqPositionsAsync()
        positions_data = extract_positions(positions)
    except Exception as e:
        return await ack.edit_text(f"Could not read positions: {e}")

    await ack.edit_text(format_portfolio(positions_data))