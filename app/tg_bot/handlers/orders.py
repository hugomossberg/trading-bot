from app.tg_bot.formatters import format_orders
from app.tg_bot.ib_views import extract_open_orders


async def send_orders(update, context):
    ack = await update.message.reply_text("Checking orders...")
    ib_client = context.application.bot_data.get("ib")
    if not ib_client:
        return await ack.edit_text("IB client missing in bot_data.")

    ib = ib_client.ib
    if not ib.isConnected():
        return await ack.edit_text("IB not connected.")

    try:
        await ib.reqOpenOrdersAsync()
        orders_data = extract_open_orders(ib)
    except Exception as e:
        return await ack.edit_text(f"Could not read open orders: {e}")

    await ack.edit_text(format_orders(orders_data))