from app.tg_bot.stock_data import get_all_symbols, get_stock_info_updated_time
from app.tg_bot.formatters import format_tickers


async def send_tickers(update, context):
    try:
        syms_all = get_all_symbols()
    except Exception:
        await update.message.reply_text("Could not read Stock_info.json.")
        return

    held_syms = set()
    ib_client = context.application.bot_data.get("ib")
    if ib_client and ib_client.ib.isConnected():
        try:
            positions = await ib_client.ib.reqPositionsAsync()
            for p in positions:
                qty = float(p.position or 0.0)
                if abs(qty) > 1e-6:
                    held_syms.add((p.contract.symbol or "").upper())
        except Exception:
            pass

    watch_syms = [s for s in syms_all if s and s not in held_syms]
    owned_syms = sorted(held_syms)

    try:
        updated = get_stock_info_updated_time()
    except Exception:
        updated = "unknown"

    msg = format_tickers(watch_syms, owned_syms, updated)
    await update.message.reply_text(msg)