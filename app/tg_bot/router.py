import re
import logging

from telegram import Update
from telegram.ext import ContextTypes

from app.tg_bot.handlers.help import send_help
from app.tg_bot.handlers.orders import send_orders
from app.tg_bot.handlers.portfolio import send_portfolio
from app.tg_bot.handlers.sell import sell_all, sell_one
from app.tg_bot.handlers.status import send_status
from app.tg_bot.handlers.stock_query import handle_stock_query
from app.tg_bot.handlers.tickers import send_tickers

log_chat = logging.getLogger("chat")


class TelegramRouter:
    def __init__(self, llm_client):
        self.llm_client = llm_client

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (update.message.text or "").strip()
        low = text.lower()

        if low in {"help", "/help", "h", "menu"}:
            return await send_help(update, context)

        if low in {"status", "/status"}:
            return await send_status(update, context)

        if low in {"portfolio", "/portfolio", "p"}:
            return await send_portfolio(update, context)

        if low in {"orders", "/orders", "o"}:
            return await send_orders(update, context)

        if low in {"tickers", "/tickers", "t"}:
            return await send_tickers(update, context)

        if low in {"sellall", "/sellall"}:
            return await sell_all(update, context)

        m = re.fullmatch(r"sell\s+([A-Za-z]{1,5})\s*(\d+)?", text, re.IGNORECASE)
        if m:
            sym = m.group(1).upper()
            qty = int(m.group(2)) if m.group(2) else None
            return await sell_one(update, context, sym, qty)

        if low.startswith("ticker "):
            sym = text.split(None, 1)[1].strip().rstrip("?").upper()
            log_chat.info("[query] user=%s ticker=%s (via 'ticker')", update.effective_user.username, sym)
            return await handle_stock_query(update, context, sym, self.llm_client)

        m = re.fullmatch(r"([A-Za-z]{2,5}(?:[.\-][A-Za-z]+)?)\??", text)
        if m:
            sym = m.group(1).upper()
            log_chat.info("[query] user=%s ticker=%s", update.effective_user.username, sym)
            return await handle_stock_query(update, context, sym, self.llm_client)

        return await send_help(update, context)