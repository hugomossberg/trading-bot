from datetime import datetime
from zoneinfo import ZoneInfo

from app.tg_bot.formatters import format_status
from app.tg_bot.ib_views import extract_positions, extract_open_orders

SE_TZ = ZoneInfo("Europe/Stockholm")
US_TZ = ZoneInfo("America/New_York")


def is_us_market_open(now_et=None) -> bool:
    now_et = now_et or datetime.now(US_TZ)
    if now_et.weekday() >= 5:
        return False
    start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return start <= now_et <= end


async def send_status(update, context):
    ack = await update.message.reply_text("Checking status...")
    ib_client = context.application.bot_data.get("ib")
    if not ib_client:
        return await ack.edit_text("IB client missing in bot_data.")

    ib = ib_client.ib
    connected = ib.isConnected()

    now_se = datetime.now(SE_TZ)
    now_et = datetime.now(US_TZ)
    market_open = is_us_market_open(now_et)

    positions_data = []
    orders_data = []

    try:
        if connected:
            positions = await ib.reqPositionsAsync()
            positions_data = extract_positions(positions)

            await ib.reqOpenOrdersAsync()
            orders_data = extract_open_orders(ib)
    except Exception as e:
        return await ack.edit_text(f"Could not read broker state: {e}")

    msg = format_status(
        connected=connected,
        now_se=now_se,
        now_et=now_et,
        market_open=market_open,
        positions=positions_data,
        orders=orders_data,
    )
    await ack.edit_text(msg)