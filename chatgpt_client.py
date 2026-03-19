# chatgpt_client.py
import os, json, re, logging, asyncio
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ContextTypes
from datetime import datetime
from zoneinfo import ZoneInfo
from ib_insync import MarketOrder  # för sellall

from signals import buy_or_sell
from universe_manager import load_state, save_state, update_signal_state
from helpers import us_market_open_now

load_dotenv()

log_chat = logging.getLogger("chat")
log_scan = logging.getLogger("autoscan")
log_scanner = logging.getLogger("scanner")

SE_TZ = ZoneInfo("Europe/Stockholm")
US_TZ = ZoneInfo("America/New_York")

def _is_us_market_open(now_et=None) -> bool:
    now_et = now_et or datetime.now(US_TZ)
    if now_et.weekday() >= 5:
        return False
    start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end   = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return start <= now_et <= end


# ---------- LLM-klient (valfri, används för korta summeringar) ----------
class OpenAi:
    def __init__(self):
        self.api_key = os.getenv("CHATGPT_API")

    async def _chat(self, system: str, user: str) -> str:
        """
        Helt frivillig. Om du inte har CHATGPT_API satt kommer vi bara
        returnera en tom sträng så botten fungerar ändå.
        """
        if not self.api_key:
            return ""
        try:
            import openai
            client = openai.OpenAI(api_key=self.api_key)
            r = client.chat.completions.create(
                model="gpt-4o-2024-08-06",
                messages=[{"role":"system","content":system},{"role":"user","content":user}],
            )
            return r.choices[0].message.content or ""
        except Exception:
            return ""

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
        txt = await self._chat("Du sammanfattar aktier kort och tydligt.", prompt)
        return txt or "(ingen summering)"

    # ---------- Telegram ROUTER ----------
    async def ai_router(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Enkel textrouter för Telegram:
        - 'status'      → portfölj + ordrar
        - 'tickers'/'t' → universum + vad som ägs
        - 'sellall'     → stänger alla öppna positioner (marknadsorder)
        - en ticker     → snabb summering + signal (t.ex. TSLA, NVDA?, sqqq)
        - annars        → kort hjälptext
        """
        text = (update.message.text or "").strip()
        low  = text.lower()

                # "ticker TSLA" eller "ticker tsla?"
        if low.startswith("ticker "):
            sym = text.split(None, 1)[1].strip().rstrip("?").upper()
            log_chat.info(f"[query] user=%s ticker=%s (via 'ticker')", update.effective_user.username, sym)
            return await self._handle_stock_query(update, context, sym)

        # gissa ticker? (minst 2 tecken för att undvika 'n'/'!' osv)
        m = re.fullmatch(r"([A-Za-z]{2,5}(?:[.\-][A-Za-z]+)?)\??", text)
        if m:
            sym = m.group(1).upper()
            log_chat.info(f"[query] user=%s ticker=%s", update.effective_user.username, sym)
            return await self._handle_stock_query(update, context, sym)

        if text in {"!", "n", "📰", "p"}:
            # TODO: koppla mot din autoscan-funktion om du vill
            return await update.message.reply_text("Premarket/nyhets-trigger är inte kopplad här ännu.")

        if low in {"status", "/status"}:
            return await self._send_status(update, context)

        if low in {"tickers", "/tickers", "t"}:
            return await self._send_tickers(update, context)

        if low in {"sellall", "/sellall"}:
            return await self._sell_all(update, context)

        m = re.fullmatch(r"sell\s+([A-Za-z]{1,5})\s*(\d+)?", text, re.IGNORECASE)
        if m:
            sym = m.group(1).upper()
            qty = int(m.group(2)) if m.group(2) else None
            return await self._sell_one(update, context, sym, qty)

        # gissa ticker? (A–Z 1–5 tecken, ev. ? i slutet)
        m = re.fullmatch(r"([A-Za-z]{1,5})(?:[.\-][A-Za-z]+)?\??", text)
        if m:
            return await self._handle_stock_query(update, context, m.group(1).upper())

    
        # fallback
        return await update.message.reply_text(
            "Kommandon:\n"
            "• status – visa portfölj/ordrar\n"
            "• tickers (eller 't') – visa universum och ägda\n"
            "• sellall – stäng alla positioner\n"
            "• sell TICKER [antal] – sälj en position\n"
            "• TICKER – snabb koll (t.ex. TSLA, NVDA?, SQQQ)"
        )

    # ---------- Stock lookup ----------
    async def _handle_stock_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE, ticker: str):
        try:
            with open("Stock_info.json","r",encoding="utf-8") as f:
                data = json.load(f)
            stocks_by_symbol = {str(s.get("symbol","")).upper(): s for s in data if s.get("symbol")}
        except Exception as e:
            log_chat.warning("Stock_info.json fel: %s", e)
            await update.message.reply_text("Kunde inte läsa Stock_info.json.")
            return

        stock = stocks_by_symbol.get(ticker.upper())
        if not stock:
            log_chat.info("[query-result] %s NOT_FOUND", ticker)
            await update.message.reply_text(f"Hittar ingen data för {ticker} i Stock_info.json.")
            return

        # kort logg om vi hittat
        lc = stock.get("latestClose")
        pe = stock.get("PE")
        mc = stock.get("marketCap")
        log_chat.info("[query-result] %s OK latestClose=%s PE=%s mcap=%s", ticker, lc, pe, mc)

        summary = await self._summarize_stock(stock)
        signal = buy_or_sell(stock)
        msg = f"📈 {ticker}\n{summary}\n\nSignal: {signal}"
        await update.message.reply_text(msg)


    # ---------- Status ----------
    async def _send_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        ack = await update.message.reply_text("🔎 Kollar status…")
        ib_client = context.application.bot_data.get("ib")
        if not ib_client:
            return await ack.edit_text("Ingen IB-klient i bot_data.")

        ib = ib_client.ib
        connected = ib.isConnected()

        now_se = datetime.now(SE_TZ)
        now_et = datetime.now(US_TZ)
        market_open = _is_us_market_open(now_et)

        pos_lines = []
        ord_lines = []
        try:
            if connected:
                positions = await ib.reqPositionsAsync()

                # filtrera bort 0-positioner och dubbletter
                nonzero = []
                seen = set()
                for p in positions:
                    qty = float(p.position or 0.0)
                    if abs(qty) < 1e-6:
                        continue
                    key = p.contract.conId or (p.contract.symbol, p.contract.exchange)
                    if key in seen:
                        continue
                    seen.add(key)
                    nonzero.append(p)

                # sortera störst först
                positions_sorted = sorted(nonzero, key=lambda p: abs(float(p.position or 0.0)), reverse=True)

                max_rows = 10
                for p in positions_sorted[:max_rows]:
                    sym = p.contract.symbol
                    qty = float(p.position or 0.0)
                    qty_str = str(int(qty)) if float(qty).is_integer() else f"{qty:.2f}"
                    avg = float(p.avgCost or 0.0)
                    pos_lines.append(f"• {sym}: {qty_str} @ {avg:.2f}")

                extra = len(positions_sorted) - min(len(positions_sorted), max_rows)
                if extra > 0:
                    pos_lines.append(f"… +{extra} till")

                # öppna ordrar (topp 5)
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
            if not pos_lines:
                pos_lines = ["(kunde inte läsa positioner)"]
            if not ord_lines:
                ord_lines = [f"(kunde inte läsa öppna ordrar: {e})"]

        pos_text = "\n".join(pos_lines) if pos_lines else "–"
        ord_text = "\n".join(ord_lines) if ord_lines else "–"

        msg = (
            f"✅ IB connected: {connected}\n"
            f"   SE {now_se:%Y-%m-%d %H:%M} | ET {now_et:%H:%M}\n"
            f"   US Market open: {'JA' if market_open else 'NEJ'} (ord. 15:30–22:00 SE)\n"
            f"\nPositioner (topp):\n{pos_text}"
            f"\n\nÖppna ordrar:\n{ord_text}"
        )
        
        await ack.edit_text(msg)

    # ---------- Tickers (universum + ägda) ----------
    async def _send_tickers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            with open("Stock_info.json", "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            await update.message.reply_text("Kunde inte läsa Stock_info.json.")
            return

        syms_all = sorted({(s.get("symbol") or "").upper() for s in data if s.get("symbol")})

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

        scan_syms = [s for s in syms_all if s and s not in held_syms]
        port_syms = sorted(held_syms)

        try:
            mtime = os.path.getmtime("Stock_info.json")
            ts = datetime.fromtimestamp(mtime, ZoneInfo("Europe/Stockholm"))
            updated = ts.strftime("%Y-%m-%d %H:%M")
        except Exception:
            updated = "okänd tid"

        def chunk_lines(items, chunk=12, max_items=120):
            items = items[:max_items]
            lines = []
            for i in range(0, len(items), chunk):
                lines.append("· " + " · ".join(items[i:i+chunk]))
            return "\n".join(lines) if items else "–"

        msg_parts = []
        msg_parts.append(f"🧭 Universum (ej ägda): {len(scan_syms)}\n{chunk_lines(scan_syms)}")
        msg_parts.append(f"📦 Portfölj (ägda): {len(port_syms)}\n{chunk_lines(port_syms)}")
        msg_parts.append(f"ℹ️ Stock_info.json uppdaterad: {updated}")
        await update.message.reply_text("\n\n".join(msg_parts))

    # ---------- Sell all (stänger positioner utan att bry sig om MAX_SELLS_PER_DAY) ----------
    async def _sell_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        ib_client = context.application.bot_data.get("ib")
        if not ib_client:
            return await update.message.reply_text("Ingen IB-klient i bot_data.")
        ib = ib_client.ib
        if not ib.isConnected():
            return await update.message.reply_text("IB ej ansluten.")

        is_open = _is_us_market_open()

        try:
            positions = await ib.reqPositionsAsync()
        except Exception as e:
            return await update.message.reply_text(f"Kunde inte läsa positioner: {e}")

        sent = []
        skipped = []

        for p in positions:
            qty = float(p.position or 0.0)
            if abs(qty) < 1e-6:
                continue

            # Skippa fractional direkt
            if not float(abs(qty)).is_integer():
                skipped.append(f"• {p.contract.symbol}: fractional {abs(qty)}")
                continue

            action = "SELL" if qty > 0 else "BUY"
            order = MarketOrder(action, int(abs(qty)))

            try:
                order.outsideRth = False if is_open else True
            except Exception:
                pass

            try:
                contract = p.contract
                contract.exchange = "SMART"   # försök undvika direkt-routing till NASDAQ
                ib.placeOrder(contract, order)

                sent.append(f"• {contract.symbol} {action} {int(abs(qty))}")
            except Exception as e:
                skipped.append(f"• {p.contract.symbol}: {e}")

        if not sent and not skipped:
            return await update.message.reply_text("Inga positioner att stänga.")

        msg = []
        if sent:
            msg.append("📤 Skickade stängningsorder:\n" + "\n".join(sent))
        if skipped:
            msg.append("⚠️ Skippade:\n" + "\n".join(skipped))

        await update.message.reply_text("\n\n".join(msg))

    async def _sell_one(self, update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str, qty: int | None = None):
        ib_client = context.application.bot_data.get("ib")
        if not ib_client:
            return await update.message.reply_text("Ingen IB-klient i bot_data.")
        ib = ib_client.ib
        if not ib.isConnected():
            return await update.message.reply_text("IB ej ansluten.")

        try:
            positions = await ib.reqPositionsAsync()
        except Exception as e:
            return await update.message.reply_text(f"Kunde inte läsa positioner: {e}")

        pos = None
        for p in positions:
            if (p.contract.symbol or "").upper() == symbol.upper() and abs(float(p.position or 0.0)) > 1e-6:
                pos = p
                break

        if not pos:
            return await update.message.reply_text(f"Hittade ingen öppen position i {symbol}.")
        current_qty = float(pos.position or 0.0)

        if not float(abs(current_qty)).is_integer():
            return await update.message.reply_text(
                f"{symbol} har fractional position ({current_qty}), sälj manuellt i TWS."
            )

        sell_qty = int(abs(current_qty)) if qty is None else min(int(abs(current_qty)), qty)
        action = "SELL" if current_qty > 0 else "BUY"

        order = MarketOrder(action, sell_qty)

        try:
            contract = pos.contract
            contract.exchange = "SMART"
            ib.placeOrder(contract, order)
            return await update.message.reply_text(f"📤 Skickade order: {action} {sell_qty} {symbol}")
        except Exception as e:
            return await update.message.reply_text(f"Kunde inte sälja {symbol}: {e}")