# chatgpt_client.py
import os, json, re
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ContextTypes
from signals import buy_or_sell, execute_order

# NYTT: tider för status
from datetime import datetime
from zoneinfo import ZoneInfo

load_dotenv()

# ---------------- Router prompt ----------------
ROUTER_SYS = (
    "Du är en strikt router. Läs användarens meddelande och svara ENBART med JSON.\n"
    'Format: {"intent":"stock_query|trade_intent|status|smalltalk|other","ticker":null|"TICKER","qty":null|int,"side":null|"BUY"|"SELL"}\n'
    "• Om meddelandet frågar om en aktie (pris/nyheter/analys) → intent=stock_query, försök hitta en ticker (t.ex. DQ).\n"
    "• Om meddelandet är en order (köp/sälj X av en ticker) → intent=trade_intent, fyll i ticker, qty, side.\n"
    "• Om det gäller status/koll (t.ex. 'status', 'läge', 'hur går det') → intent=status.\n"
    "• Om det är vanlig konversation → intent=smalltalk.\n"
    "• Annars → intent=other.\n"
    "Svara alltid som giltig JSON. Inga extra ord."
)

# --- Tidszoner för status ---
SE_TZ = ZoneInfo("Europe/Stockholm")
US_TZ = ZoneInfo("America/New_York")

def _is_us_market_open(now_et: datetime | None = None) -> bool:
    """
    Grov check för ordinarie RTH (NYSE/Nasdaq): Mån–Fre 09:30–16:00 ET.
    Helgdagar ignoreras i denna minimala version.
    """
    now_et = now_et or datetime.now(US_TZ)
    if now_et.weekday() >= 5:  # 5=lör, 6=sön
        return False
    start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end   = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return start <= now_et <= end


class OpenAi:
    def __init__(self):
        self.api_key = os.getenv("CHATGPT_API")

    async def _chat(self, system: str, user: str) -> str:
        import openai
        client = openai.OpenAI(api_key=self.api_key)
        r = client.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=[{"role":"system","content":system},{"role":"user","content":user}],
        )
        return r.choices[0].message.content

    async def _summarize_stock(self, stock: dict) -> str:
        payload = {
            "symbol": stock.get("symbol"),
            "name": stock.get("name"),
            "latestClose": stock.get("latestClose"),
            "PE": stock.get("PE"),
            "marketCap": stock.get("marketCap"),
            "beta": stock.get("beta"),
            "eps": stock.get("trailingEps"),
            "dividendYield": stock.get("dividendYield"),
            "sector": stock.get("sector"),
            "news": [
                {
                    "title": (n.get("content",{}) or {}).get("title",""),
                    "summary": (n.get("content",{}) or {}).get("summary",""),
                }
                for n in (stock.get("News") or [])[:2]
            ],
        }
        prompt = (
            "Svara kort (max 6 meningar) på svenska: pris, P/E, mcap, risk (beta), "
            "1–2 nyheter i lugn ton, och avsluta med neutral slutsats.\n"
            f"DATA:\n{json.dumps(payload, ensure_ascii=False)}"
        )
        return await self._chat("Du sammanfattar aktier kort och tydligt.", prompt)
    
    async def _handle_stock_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE, ticker: str):
        # Läs cache
        try:
            with open("Stock_info.json","r",encoding="utf-8") as f:
                data = json.load(f)
            stocks_by_symbol = {s["symbol"].upper(): s for s in data}
        except Exception:
            await update.message.reply_text("Kunde inte läsa Stock_info.json.")
            return

        stock = stocks_by_symbol.get(ticker.upper())
        if not stock:
            await update.message.reply_text(f"Hittar ingen data för {ticker} i Stock_info.json.")
            return

        summary = await self._summarize_stock(stock)
        await update.message.reply_text(summary)
        signal = buy_or_sell(stock)
        await update.message.reply_text(f"Signal: {signal}")


    # --- NYTT: status-svar som snygg sammanställning ---
    async def _send_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        ack = await update.message.reply_text("🔎 Kollar status…")

        ib_client = context.application.bot_data.get("ib")
        if not ib_client:
            return await ack.edit_text("❌ Ingen IB-klient i bot_data.")

        ib = ib_client.ib
        connected = ib.isConnected()

        now_se = datetime.now(SE_TZ)
        now_et = datetime.now(US_TZ)
        market_open = _is_us_market_open(now_et)

        pos_lines = []
        ord_lines = []
        try:
            if connected:
                # 🛠️ HÄR: hämta positioner asynkront
                positions = await ib.reqPositionsAsync()
                for p in positions[:5]:
                    sym = p.contract.symbol
                    qty = p.position
                    avg = float(p.avgCost or 0.0)
                    pos_lines.append(f"• {sym}: {qty} @ {avg:.2f}")

                # 🛠️ HÄR: trigga uppdatering av öppna ordrar innan vi läser cachen
                await ib.reqOpenOrdersAsync()
                for t in ib.openTrades()[:5]:
                    s = t.contract.symbol
                    side = t.order.action
                    qty = int(t.order.totalQuantity)
                    filled = int(t.orderStatus.filled or 0)
                    st = t.orderStatus.status or "?"
                    rth = "AH" if getattr(t.order, 'outsideRth', False) else "RTH"
                    ord_lines.append(f"• {s} {side} {filled}/{qty} ({st}, {rth})")
        except Exception as e:
            pos_lines = pos_lines or ["(kunde inte läsa positioner)"]
            ord_lines = ord_lines or [f"(kunde inte läsa öppna ordrar: {e})"]

        pos_text = "\n".join(pos_lines) if pos_lines else "–"
        ord_text = "\n".join(ord_lines) if ord_lines else "–"

        msg = (
            f"✅ IB connected: {connected}\n"
            f"🕒 SE {now_se:%Y-%m-%d %H:%M} | ET {now_et:%H:%M}\n"
            f"🏛️ US Market open: {'JA' if market_open else 'NEJ'} (ord. 15:30–22:00 SE)\n"
            f"\n📈 Positioner (topp):\n{pos_text}"
            f"\n\n🧾 Öppna ordrar:\n{ord_text}"
        )
        await ack.edit_text(msg)

    async def ai_router(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (update.message.text or "").strip()
        if not text:
            return

        lower = text.lower()

        # 0) Snabb: systemstatus (kodsvar) – hanteras FÖRE all regex
        if lower in {"status", "läge", "hur går det"}:
            return await self._send_status(update, context)

        # 1) AI-prefix (frivilligt): allt som börjar med ".ai" går direkt till LLM
        if lower.startswith(".ai"):
            prompt = text[3:].strip() or "Hej!"
            reply = await self._chat("Du svarar hjälpsamt och kort på svenska.", prompt)
            await update.message.reply_text(reply)
            return

        # 2) Deterministisk trade-parse: "köp 10 aapl" / "sälj 5 nvda" (sv/en)
        m_trade = re.fullmatch(
            r"(köp|buy|sälj|sell)\s+(\d+)\s+([A-Za-z0-9.\-]{1,6})",
            text, re.IGNORECASE
        )
        if m_trade:
            side_word, qty_str, ticker = m_trade.groups()
            side_word = side_word.lower()
            qty = int(qty_str)
            side = "BUY" if side_word in {"köp", "buy"} else "SELL"
            ticker = ticker.upper()

            # Läs cache
            try:
                with open("Stock_info.json","r",encoding="utf-8") as f:
                    data = json.load(f)
                stocks_by_symbol = {s["symbol"].upper(): s for s in data}
            except Exception:
                await update.message.reply_text("Kunde inte läsa Stock_info.json.")
                return

            stock = stocks_by_symbol.get(ticker)
            if not stock:
                await update.message.reply_text(f"Hittar ingen data för {ticker}.")
                return

            ib = context.application.bot_data.get("ib")
            if not ib or not ib.ib.isConnected():
                await update.message.reply_text("⚠️ IBKR inte ansluten – ingen order lagd.")
                return

            ack = await update.message.reply_text(
                f"⏳ Lägger order: {'KÖP' if side=='BUY' else 'SÄLJ'} {qty} {stock['symbol']} …"
            )
            trade = await execute_order(
                ib, stock, "Köp" if side == "BUY" else "Sälj", qty,
                bot=context.application.bot,
                chat_id=int(os.getenv("ADMIN_CHAT_ID") or 0),
            )
            if trade:
                await ack.edit_text(
                    f"📨 Order skickad: {trade.order.action} {int(trade.order.totalQuantity)} {stock['symbol']} "
                    f"(status: {trade.orderStatus.status})"
                )
            else:
                await ack.edit_text("Ingen order skickad.")
            return

        # 3) Ticker-snabbväg för lookup (exakt ticker eller "TICKER aktie")
        #    OBS: vi kör EFTER status/trade, och bara på rena ticker-meddelanden.
        m1 = re.fullmatch(r"([A-Za-z0-9.\-]{1,6})(?:\s+(aktie|stock))?", text, re.IGNORECASE)
        m2 = re.fullmatch(r"(aktie|stock)\s+([A-Za-z0-9.\-]{1,6})", text, re.IGNORECASE)
        if m1:
            return await self._handle_stock_query(update, context, m1.group(1).upper())
        if m2:
            return await self._handle_stock_query(update, context, m2.group(2).upper())

        # 4) Annars: låt GPT-routern tolka (stock_query / trade_intent / smalltalk)
        try:
            raw = await self._chat(ROUTER_SYS, text)
            parsed = json.loads(raw)
        except Exception:
            parsed = {"intent": "smalltalk", "ticker": None, "qty": None, "side": None}

        intent = parsed.get("intent")
        ticker = parsed.get("ticker")
        qty    = parsed.get("qty")
        side   = parsed.get("side")

        if intent == "status":
            return await self._send_status(update, context)

        # Läs cache för stock_query / trade_intent
        stocks_by_symbol = {}
        if intent in ("stock_query", "trade_intent"):
            try:
                with open("Stock_info.json","r",encoding="utf-8") as f:
                    data = json.load(f)
                stocks_by_symbol = {s["symbol"].upper(): s for s in data}
            except Exception:
                await update.message.reply_text("Kunde inte läsa Stock_info.json.")
                return

        if intent == "stock_query":
            if not ticker:
                await update.message.reply_text("Vilken ticker menar du?")
                return
            return await self._handle_stock_query(update, context, ticker.upper())

        if intent == "trade_intent":
            if not (ticker and isinstance(qty, int) and side in ("BUY", "SELL")):
                await update.message.reply_text("Kan du specificera t.ex. 'Köp 10 DQ' eller 'Sälj 5 DUO'?")
                return

            stock = stocks_by_symbol.get(ticker.upper())
            if not stock:
                await update.message.reply_text(f"Hittar ingen data för {ticker}.")
                return

            ib = context.application.bot_data.get("ib")
            if not ib or not ib.ib.isConnected():
                await update.message.reply_text("⚠️ IBKR inte ansluten – ingen order lagd.")
                return

            ack = await update.message.reply_text(
                f"⏳ Lägger order: {'KÖP' if side=='BUY' else 'SÄLJ'} {qty} {stock['symbol']} …"
            )
            trade = await execute_order(
                ib, stock, "Köp" if side == "BUY" else "Sälj", qty,
                bot=context.application.bot,
                chat_id=int(os.getenv("ADMIN_CHAT_ID") or 0),
            )
            if trade:
                await ack.edit_text(
                    f"📨 Order skickad: {trade.order.action} {int(trade.order.totalQuantity)} {stock['symbol']} "
                    f"(status: {trade.orderStatus.status})"
                )
            else:
                await ack.edit_text("Ingen order skickad.")
            return

        # 5) Smalltalk/fallback → AI
        if intent == "smalltalk":
            reply = await self._chat("Du svarar hjälpsamt och kort på svenska.", text)
            await update.message.reply_text(reply)
            return

        reply = await self._chat("Svara kort på svenska.", text)
        await update.message.reply_text(reply)




# --------- Auto-scan för schemaläggaren (anropar din signal + ev order) ----------
# --- auto_scan_and_trade ---
async def auto_scan_and_trade(bot, ib_client, admin_chat_id: int):
    try:
        with open("Stock_info.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        if not ib_client or not ib_client.ib.isConnected():
            if admin_chat_id:
                await bot.send_message(admin_chat_id, "⚠️ IBKR inte ansluten – hoppar över autoscan.")
            return

        for stock in data:
            signal = buy_or_sell(stock)
            if admin_chat_id:
                await bot.send_message(admin_chat_id, f"{stock['symbol']} – Signal: {signal}")

            trade = await execute_order(
                ib_client, stock, signal, qty=10,
                bot=bot, chat_id=admin_chat_id
            )

            if trade is not None and admin_chat_id:
                await bot.send_message(
                    admin_chat_id,
                    f"Order skickad: {trade.order.action} {int(trade.order.totalQuantity)} {stock['symbol']} "
                    f"(status: {trade.orderStatus.status})"
                )
            elif admin_chat_id:
                await bot.send_message(admin_chat_id, f"{stock['symbol']} – Ingen order (Håll).")
    except Exception as e:
        if admin_chat_id:
            await bot.send_message(admin_chat_id, f"❌ auto_scan_and_trade fel: {e}")
