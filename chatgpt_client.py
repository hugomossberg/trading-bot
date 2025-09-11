# chatgpt_client.py
import os, json, re
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ContextTypes
from signals import buy_or_sell, execute_order
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from universe_manager import load_state, save_state, update_signal_state

load_dotenv()

ALLOW_SHORTS = os.getenv("ALLOW_SHORTS", "0").lower() in {"1","true","yes","on"}



STATE_PATH = os.getenv("STATE_PATH", "trade_state.json")

ONLY_TRADE_ON_SIGNAL_CHANGE = os.getenv("ONLY_TRADE_ON_SIGNAL_CHANGE","1").lower() in {"1","true","yes","on"}
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN","30"))
MAX_POS_PER_SYMBOL = int(os.getenv("MAX_POS_PER_SYMBOL","0"))
MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY","1"))
MAX_SELLS_PER_DAY = int(os.getenv("MAX_SELLS_PER_DAY","2"))

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
                #  HÄR: hämta positioner asynkront
                positions = await ib.reqPositionsAsync()

# Filtrera bort 0-positioner och dubbletter
                nonzero = []
                seen = set()
                for p in positions:
                    qty = float(p.position or 0.0)
                    if abs(qty) < 1e-6:
                        continue  # hoppa över 0.0
                    key = p.contract.conId or (p.contract.symbol, p.contract.exchange)
                    if key in seen:
                        continue
                    seen.add(key)
                    nonzero.append(p)

                # Sortera så största absoluta innehav överst
                positions_sorted = sorted(
                    nonzero, key=lambda p: abs(float(p.position or 0.0)), reverse=True
                )

                max_rows = 10  # visa upp till 10
                for p in positions_sorted[:max_rows]:
                    sym = p.contract.symbol
                    qty = float(p.position or 0.0)
                    qty_str = str(int(qty)) if qty.is_integer() else f"{qty:.2f}"  # 20 istället för 20.0
                    avg = float(p.avgCost or 0.0)
                    pos_lines.append(f"• {sym}: {qty_str} @ {avg:.2f}")

                extra = len(positions_sorted) - min(len(positions_sorted), max_rows)
                if extra > 0:
                    pos_lines.append(f"… +{extra} till")



                # HÄR: trigga uppdatering av öppna ordrar innan vi läser cachen
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
            f"   SE {now_se:%Y-%m-%d %H:%M} | ET {now_et:%H:%M}\n"
            f"   US Market open: {'JA' if market_open else 'NEJ'} (ord. 15:30–22:00 SE)\n"
            f"\n Positioner (topp):\n{pos_text}"
            f"\n\n Öppna ordrar:\n{ord_text}"
        )
        await ack.edit_text(msg)
    
    async def _send_tickers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            with open("Stock_info.json", "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            await update.message.reply_text("Kunde inte läsa Stock_info.json.")
            return

        # Alla tickers i cachen
        syms_all = sorted({(s.get("symbol") or "").upper() for s in data if s.get("symbol")})

        # Ägda tickers (via IB, om ansluten)
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

        # Kandidater = ej ägda
        scan_syms = [s for s in syms_all if s and s not in held_syms]
        port_syms = sorted(held_syms)

        # Senaste uppdateringstid
        try:
            mtime = os.path.getmtime("Stock_info.json")
            ts = datetime.fromtimestamp(mtime, ZoneInfo("Europe/Stockholm"))
            updated = ts.strftime("%Y-%m-%d %H:%M")
        except Exception:
            updated = "okänd tid"

        def chunk_lines(items, chunk=10):
            lines = []
            for i in range(0, len(items), chunk):
                lines.append("· " + " · ".join(items[i:i+chunk]))
            return "\n".join(lines) if items else "–"

        msg = []
        msg.append(f" Universum (ej ägda): {len(scan_syms)} — uppd. {updated}")
        msg.append(chunk_lines(scan_syms))
        msg.append("")
        msg.append(f" Portfölj (ägda): {len(port_syms)}")
        msg.append(chunk_lines(port_syms))
        await update.message.reply_text("\n".join(msg))

                
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
                
        # Snabbkommando: lista tickers ur Stock_info.json
        if lower in {"tickers", "/tickers", "universum", "lista", "aktier"}:
            return await self._send_tickers(update, context)

        # 2) Deterministisk trade-parse: "köp 10 aapl" / "sälj 5 nvda" (sv/en)
        # 2) Deterministisk trade-parse: "köp 10 aapl" / "sälj 20.0 nvda"
        m_trade = re.fullmatch(
            r"(köp|buy|sälj|sell)\s+(\d+(?:[.,]\d+)?)\s+([A-Za-z0-9.\-]{1,6})",
            text, re.IGNORECASE
        )
        if m_trade:
            side_word, qty_str, ticker = m_trade.groups()
            try:
                qty = int(float(qty_str.replace(",", ".")))  # tillåt 20.0 och 20,0
            except ValueError:
                await update.message.reply_text("Ogiltigt antal.")
                return

            side = "BUY" if side_word.lower() in {"köp", "buy"} else "SELL"
            ticker = ticker.upper()

            # Läs cache (valfritt)
            stocks_by_symbol = {}
            try:
                with open("Stock_info.json","r",encoding="utf-8") as f:
                    data = json.load(f)
                stocks_by_symbol = {s["symbol"].upper(): s for s in data}
            except Exception:
                pass

            stock = stocks_by_symbol.get(ticker) or {"symbol": ticker, "name": ticker}

            ib = context.application.bot_data.get("ib")
            if not ib or not ib.ib.isConnected():
                await update.message.reply_text("IBKR inte ansluten – ingen order lagd.")
                return

            # 🔒 Blockera oönskad short / sälj mer än du äger
            if side == "SELL" and not ALLOW_SHORTS:
                positions = await ib.ib.reqPositionsAsync()
                held = {p.contract.symbol.upper(): float(p.position or 0) for p in positions}
                pos = held.get(ticker, 0.0)

                if pos <= 0:
                    await update.message.reply_text(f" Skippar SÄLJ {ticker} – äger inte aktien.")
                    return

                if qty > pos:
                    await update.message.reply_text(
                        f"⛔ Du äger bara {int(pos)} {ticker}. Säg t.ex. 'sälj {int(pos)} {ticker}'."
                    )
                    return

               
            # Lägg order (gäller både BUY och SELL efter ev. validering)
            ack = await update.message.reply_text(
                f" Lägger order: {'KÖP' if side=='BUY' else 'SÄLJ'} {qty} {ticker} …"
            )
            trade = await execute_order(
                ib, stock, "Köp" if side == "BUY" else "Sälj", qty,
                bot=context.application.bot,
                chat_id=int(os.getenv("ADMIN_CHAT_ID") or 0),
            )
            if trade:
                await ack.edit_text(
                    f"Order skickad: {trade.order.action} {int(trade.order.totalQuantity)} {ticker} "
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
                await update.message.reply_text("IBKR inte ansluten – ingen order lagd.")
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
                    f"Order skickad: {trade.order.action} {int(trade.order.totalQuantity)} {stock['symbol']} "
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

    async def trade_comment(self, stock: dict, signal: str) -> str:
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
            f"Svara på svenska med 3–5 punkter varför signalen är '{signal}' just nu.\n"
            "Använd datan (pris, P/E, mcap, beta, senaste nyheter). Håll lugn ton.\n"
            "Struktur:\n"
            "• Värdering/grunddata (P/E, mcap, ev. utdelning)\n"
            "• Momentum/nyheter (vad som driver intresset nu)\n"
            "• Teknik/kort sikt (om relevant)\n"
            "• Risker (beta/likviditet/småbolagsrisk)\n"
            "Avsluta med: 'Bedömning: ...' (en rad, neutral).\n"
            f"DATA:\n{json.dumps(payload, ensure_ascii=False)}"
        )
        return await self._chat("Du motiverar aktiesignaler kort och tydligt.", prompt)

    
    # --------- Auto-scan för schemaläggaren (anropar din signal + ev order) ----------

async def auto_scan_and_trade(bot, ib_client, admin_chat_id: int):
    try:
        import random

        # ---------- Helpers ----------
        def _env_int(key: str, default: int) -> int:
            try:
                return int(os.getenv(key, str(default)))
            except Exception:
                return default

        def _to_int(x, default=0) -> int:
            try:
                # hantera "2.0", " 3 ", etc.
                if isinstance(x, str):
                    x = x.strip().replace(",", ".")
                return int(float(x))
            except Exception:
                return default

        def _to_float(x, default=None):
            try:
                if isinstance(x, str):
                    x = x.strip().replace(",", ".")
                return float(x)
            except Exception:
                return default

        def _normalize_stock(stock: dict) -> dict:
            """Kasta om kända fält till float så buy_or_sell inte får strängar."""
            s = dict(stock or {})
            for k in ["latestClose", "PE", "marketCap", "beta", "trailingEps", "dividendYield"]:
                s[k] = _to_float(s.get(k), 0.0)
            return s

        def _now_utc():
            return datetime.now(timezone.utc)

        # ---------- 1) Läs universum ----------
        with open("Stock_info.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        # ---------- 2) IB? ----------
        if not ib_client or not ib_client.ib.isConnected():
            if admin_chat_id:
                await bot.send_message(admin_chat_id, "IBKR inte ansluten – hoppar över autoscan.")
            return

        # ---------- 3) Env / parametrar ----------
        AUTOTRADE  = os.getenv("AUTOTRADE", "0").lower() in {"1","on","true","yes"}
        AUTO_QTY   = _env_int("AUTO_QTY", 10)
        ONLY_TRADE_ON_SIGNAL_CHANGE = os.getenv("ONLY_TRADE_ON_SIGNAL_CHANGE","1").lower() in {"1","true","yes","on"}
        COOLDOWN_MIN        = _env_int("COOLDOWN_MIN", 30)
        MAX_POS_PER_SYMBOL  = _env_int("MAX_POS_PER_SYMBOL", 0)
        MAX_BUYS_PER_DAY    = _env_int("MAX_BUYS_PER_DAY", 1)
        MAX_SELLS_PER_DAY   = _env_int("MAX_SELLS_PER_DAY", 2)
        ENTRY_MODE = os.getenv("ENTRY_MODE", "all").lower()               # "all" | "buy_only"
        EXCLUDE_MINUTES       = _env_int("EXCLUDE_MINUTES", 180)          # nyköpta exkluderas så länge
        PASS_EXCLUDE_MINUTES  = _env_int("PASS_EXCLUDE_MINUTES", 20)      # Pass exkluderas så länge
        RANDOMIZE_CANDIDATES  = os.getenv("RANDOMIZE_CANDIDATES","1").lower() in {"1","on","true","yes"}
        SCAN_LIMIT            = _env_int("UNIVERSE_ROWS", 10)
        SUMMARY_NOTIFS        = os.getenv("SUMMARY_NOTIFS","1").lower() in {"1","true","yes","on"}
        WANT_AI               = os.getenv("AI_TRADE_COMMENT","1").lower() in {"1","true","yes","on"}
        AI_MODE               = os.getenv("AI_TRADE_COMMENT_ON","buy").lower()  # buy|sell|both

        # ---------- 4) State ----------
        state = load_state()
        state.setdefault("last_signal", {})
        state.setdefault("hold_streak", {})
        state.setdefault("universe", state.get("universe", []))
        state.setdefault("last_trade_ts", {})   # sym -> ISO
        state.setdefault("buys_today", {})      # sym -> {"date": "YYYY-MM-DD", "count": n}
        state.setdefault("sells_today", {})     # sym -> {"date": "YYYY-MM-DD", "count": n}
        state.setdefault("exclude_until", {})   # sym -> ISO

        now = _now_utc()
        today = now.date().isoformat()

        # ---------- 5) Positioner + öppna BUY-ordrar ----------
        positions = await ib_client.ib.reqPositionsAsync()
        held = { (p.contract.symbol or "").upper(): float(p.position or 0) for p in positions }

        try:
            await ib_client.ib.reqOpenOrdersAsync()
            open_buy_syms = {
                (t.contract.symbol or "").upper()
                for t in ib_client.ib.openTrades()
                if (t.order.action or "").upper() == "BUY"
                and (t.orderStatus.status or "").lower() in {
                    "presubmitted","submitted","pendingsubmit","pendingcancel"
                }
            }
        except Exception:
            open_buy_syms = set()

        # ---------- Hjälpare ----------
        def _in_cooldown(sym: str) -> bool:
            ts = state["last_trade_ts"].get(sym)
            if not ts:
                return False
            try:
                last = datetime.fromisoformat(str(ts))
                return (_now_utc() - last) < timedelta(minutes=COOLDOWN_MIN)
            except Exception:
                return False

        def _get_counter(bucket: str, sym: str) -> dict:
            rec = state[bucket].get(sym, {"date": today, "count": 0})
            if rec.get("date") != today:
                rec = {"date": today, "count": 0}
            rec["count"] = _to_int(rec.get("count", 0), 0)
            return rec

        def _is_excluded(sym: str) -> bool:
            iso = state["exclude_until"].get(sym)
            if not iso:
                return False
            try:
                if isinstance(iso, (int, float)):
                    until = datetime.fromtimestamp(float(iso), tz=timezone.utc)
                else:
                    until = datetime.fromisoformat(str(iso))
                return _now_utc() < until
            except Exception:
                return False

        # ---------- 6) Kandidater (ej ägda) ----------
        stocks_by_symbol = {(s.get("symbol") or "").upper(): s for s in data if s.get("symbol")}
        all_syms = list(stocks_by_symbol.keys())

        candidate_syms = [
            s for s in all_syms
            if held.get(s, 0.0) == 0.0 and s not in open_buy_syms and not _is_excluded(s)
        ]
        if RANDOMIZE_CANDIDATES:
            random.shuffle(candidate_syms)
        if SCAN_LIMIT > 0:
            candidate_syms = candidate_syms[:SCAN_LIMIT]

        summary = {"universe": len(candidate_syms), "signals": {"Köp":0,"Pass":0}, "trades": []}
        ai_queue = []  # (sym, signal, stock)

        # --- Bara Köp/Pass på icke-ägda ---
        for sym in candidate_syms:
            raw_stock = stocks_by_symbol.get(sym) or {"symbol": sym, "name": sym}
            stock = _normalize_stock(raw_stock)  # <-- NYTT: normalisera
            try:
                signal = buy_or_sell(stock)
            except Exception as e:
                # Om din signal-funktion ändå kraschar → logga och behandla som Pass
                if admin_chat_id:
                    await bot.send_message(admin_chat_id, f"buy_or_sell fel för {sym}: {e}")
                signal = "Håll"

            prev_sig = state["last_signal"].get(sym)

            if signal != "Köp":
                summary["signals"]["Pass"] += 1
                state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=PASS_EXCLUDE_MINUTES)).isoformat()
                update_signal_state(state, sym, "Håll")
                continue

            # == "Köp"
            summary["signals"]["Köp"] += 1

            if ENTRY_MODE == "buy_only" and signal != "Köp":
                update_signal_state(state, sym, "Håll"); continue
            if ONLY_TRADE_ON_SIGNAL_CHANGE and prev_sig == signal:
                update_signal_state(state, sym, signal); continue
            if _in_cooldown(sym):
                update_signal_state(state, sym, signal); continue

            qty = AUTO_QTY
            if MAX_POS_PER_SYMBOL > 0:
                owned = 0
                remaining_cap = MAX_POS_PER_SYMBOL - owned
                if remaining_cap <= 0:
                    update_signal_state(state, sym, signal); continue
                qty = min(qty, remaining_cap)
                if qty <= 0:
                    update_signal_state(state, sym, signal); continue

            if MAX_BUYS_PER_DAY > 0:
                b = _get_counter("buys_today", sym)
                if b["count"] >= MAX_BUYS_PER_DAY:
                    update_signal_state(state, sym, signal); continue

            trade = None
            if AUTOTRADE:
                trade = await execute_order(ib_client, raw_stock, "Köp", qty=qty, bot=bot, chat_id=admin_chat_id)

            if trade is not None:
                state["last_trade_ts"][sym] = _now_utc().isoformat()
                b = _get_counter("buys_today", sym); b["count"] += 1
                state["buys_today"][sym] = b
                state["exclude_until"][sym] = (_now_utc() + timedelta(minutes=EXCLUDE_MINUTES)).isoformat()
                summary["trades"].append(f"Köp {qty} {sym}")
                if WANT_AI and (AI_MODE in {"both","buy"}):
                    ai_queue.append((sym, "Köp", raw_stock))

            update_signal_state(state, sym, signal)

        # ---------- 7) Ägda – kör full signal (Köp/Håll/Sälj) ----------
        held_syms = [s for s in all_syms if held.get(s, 0.0) > 0.0]
        for sym in held_syms:
            raw_stock = stocks_by_symbol.get(sym) or {"symbol": sym, "name": sym}
            stock = _normalize_stock(raw_stock)  # <-- NYTT
            try:
                signal = buy_or_sell(stock)
            except Exception as e:
                if admin_chat_id:
                    await bot.send_message(admin_chat_id, f"buy_or_sell fel för {sym}: {e}")
                signal = "Håll"

            prev_sig = state["last_signal"].get(sym)
            if ONLY_TRADE_ON_SIGNAL_CHANGE and prev_sig == signal:
                update_signal_state(state, sym, signal); continue
            if _in_cooldown(sym):
                update_signal_state(state, sym, signal); continue

            trade = None
            qty = AUTO_QTY

            if signal == "Köp":
                if MAX_POS_PER_SYMBOL > 0:
                    owned = _to_int(held.get(sym, 0), 0)
                    remaining_cap = MAX_POS_PER_SYMBOL - owned
                    if remaining_cap <= 0:
                        update_signal_state(state, sym, signal); continue
                    qty = min(qty, remaining_cap)
                    if qty <= 0:
                        update_signal_state(state, sym, signal); continue

                if MAX_BUYS_PER_DAY > 0:
                    b = _get_counter("buys_today", sym)
                    if b["count"] >= MAX_BUYS_PER_DAY:
                        update_signal_state(state, sym, signal); continue

                if AUTOTRADE:
                    trade = await execute_order(ib_client, raw_stock, "Köp", qty=qty, bot=bot, chat_id=admin_chat_id)
                if trade is not None:
                    state["last_trade_ts"][sym] = _now_utc().isoformat()
                    b = _get_counter("buys_today", sym); b["count"] += 1
                    state["buys_today"][sym] = b
                    summary["trades"].append(f"Köp {qty} {sym}")
                    if WANT_AI and (AI_MODE in {"both","buy"}):
                        ai_queue.append((sym, "Köp", raw_stock))

            elif signal == "Sälj":
                pos = _to_int(held.get(sym, 0), 0)
                qty = min(qty, pos)
                if qty <= 0:
                    update_signal_state(state, sym, signal); continue

                if MAX_SELLS_PER_DAY > 0:
                    srec = _get_counter("sells_today", sym)
                    if srec["count"] >= MAX_SELLS_PER_DAY:
                        update_signal_state(state, sym, signal); continue

                if AUTOTRADE:
                    trade = await execute_order(ib_client, raw_stock, "Sälj", qty=qty, bot=bot, chat_id=admin_chat_id)
                if trade is not None:
                    state["last_trade_ts"][sym] = _now_utc().isoformat()
                    srec = _get_counter("sells_today", sym); srec["count"] += 1
                    state["sells_today"][sym] = srec
                    summary["trades"].append(f"Sälj {qty} {sym}")
                    if WANT_AI and (AI_MODE in {"both","sell"}):
                        ai_queue.append((sym, "Sälj", raw_stock))

            update_signal_state(state, sym, signal)

        # ---------- 8) Spara ----------
        save_state(state)

        # ---------- 9) Summering (innan AI-reflektion) ----------
        if admin_chat_id and SUMMARY_NOTIFS:
            k = summary["signals"]["Köp"]; p = summary["signals"]["Pass"]
            lines = [
                "🔁 Autoscan klar",
                f"• Kandidater (ej ägda): {summary['universe']}",
                f"• Scanning: Köp {k} · Pass {p}",
            ]
            if summary["trades"]:
                lines.append("• Ordrar:")
                lines += [f"   • {row}" for row in summary["trades"]]
            else:
                lines.append("• Ordrar: –")
            await bot.send_message(admin_chat_id, "\n".join(lines))

        # ---------- 10) AI-kommentarer efter summering ----------
        if admin_chat_id and WANT_AI and ai_queue:
            ai = OpenAi()
            for sym, sig, raw_stock in ai_queue:
                try:
                    text = await ai.trade_comment(raw_stock, sig)
                    await bot.send_message(admin_chat_id, f"🤖 {sym}: Varför {sig.lower()}?\n{text}")
                except Exception as e:
                    await bot.send_message(admin_chat_id, f"(AI-kommentar misslyckades för {sym}: {e})")

    except Exception as e:
        if admin_chat_id:
            await bot.send_message(admin_chat_id, f"auto_scan_and_trade fel: {e}")
