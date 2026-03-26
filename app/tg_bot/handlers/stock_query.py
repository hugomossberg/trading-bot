import logging

from app.core.signals import get_signal_analysis
from app.tg_bot.stock_data import get_stock_by_symbol
from app.tg_bot.formatters import format_stock_brief

log_chat = logging.getLogger("chat")


async def handle_stock_query(update, context, ticker: str, llm_client):
    try:
        stock = get_stock_by_symbol(ticker)
    except Exception as e:
        log_chat.warning("Stock_info.json error: %s", e)
        await update.message.reply_text("Could not read Stock_info.json.")
        return

    if not stock:
        log_chat.info("[query-result] %s NOT_FOUND", ticker)
        await update.message.reply_text(f"No data found for {ticker}.")
        return

    try:
        analysis = get_signal_analysis(stock)
    except Exception as e:
        log_chat.warning("Signal analysis failed for %s: %s", ticker, e)
        analysis = {
            "symbol": ticker,
            "signal": "Hold",
            "total_score": 0,
            "scores": {
                "fundamentals": 0,
                "financials": 0,
                "news": 0,
            },
            "details": {},
            "error": str(e),
        }

    summary = ""
    try:
        summary = await llm_client.summarize_stock(stock)
    except Exception as e:
        log_chat.warning("LLM summary failed for %s: %s", ticker, e)

    msg = format_stock_brief(ticker, stock, analysis, summary)
    await update.message.reply_text(msg)