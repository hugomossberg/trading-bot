import os, json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import yfinance as yf

from signals import buy_or_sell
from helpers import send_long_message

US_TZ = ZoneInfo("America/New_York")
SE_TZ = ZoneInfo("Europe/Stockholm")

def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default

def _normalize_stock(stock: dict) -> dict:
    s = dict(stock or {})
    s["latestClose"]   = _to_float(s.get("latestClose"),   0.0)
    s["PE"]            = _to_float(s.get("PE"),            0.0)
    s["marketCap"]     = _to_float(s.get("marketCap"),     0.0)
    s["beta"]          = _to_float(s.get("beta"),          0.0)
    s["trailingEps"]   = _to_float(s.get("trailingEps"),   0.0)
    s["dividendYield"] = _to_float(s.get("dividendYield"), 0.0)
    return s

def _fetch_yf_snapshot(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    info = t.info or {}
    latest_close = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
    stock = {
        "symbol": ticker,
        "name": info.get("shortName") or info.get("longName") or ticker,
        "latestClose": latest_close,
        "PE": info.get("trailingPE") or info.get("forwardPE"),
        "marketCap": info.get("marketCap"),
        "beta": info.get("beta"),
        "trailingEps": info.get("trailingEps"),
        "dividendYield": info.get("dividendYield"),
        "sector": info.get("sector"),
        "News": [],
    }
    try:
        news = t.news or []
        for n in news[:3]:
            stock["News"].append({
                "content": {
                    "title": n.get("title",""),
                    "summary": n.get("summary","") or "",
                    "publisher": n.get("publisher",""),
                    "link": n.get("link",""),
                }
            })
    except Exception:
        pass
    return stock

async def run_premarket_scan(bot, ib_client, admin_chat_id: int, want_ai: bool = True, open_ai=None):
    """
    1) Läser positioner (IB)
    2) Hämtar snabbdata + rubriker (yfinance)
    3) Kör buy_or_sell → Köp/Håll/Sälj
    4) Skickar rapport i Telegram
    5) (om want_ai & open_ai) AI-kommentar per ticker
    """
    if not ib_client or not ib_client.ib.isConnected():
        if admin_chat_id:
            await bot.send_message(admin_chat_id, "Premarket: IBKR inte ansluten – hoppar.")
        return

    positions = await ib_client.ib.reqPositionsAsync()
    held = {}
    for p in positions:
        sym = (p.contract.symbol or "").upper()
        qty = float(p.position or 0.0)
        if abs(qty) > 1e-6:
            held[sym] = held.get(sym, 0.0) + qty

    if not held:
        if admin_chat_id:
            await bot.send_message(admin_chat_id, "Premarket: Inga aktier ägs just nu.")
        return

    rows = []
    ai_blocks = []

    for sym, qty in sorted(held.items(), key=lambda kv: (-abs(kv[1]), kv[0])):
        snap = _fetch_yf_snapshot(sym)
        norm = _normalize_stock(snap)
        try:
            signal = buy_or_sell(norm)
        except Exception:
            signal = "Håll"

        tag = {"Köp":"🟢 Köp", "Sälj":"🔴 Sälj"}.get(signal, "⚪ Håll")
        nh = snap.get("News") or []
        nline = ""
        if nh:
            first = nh[0].get("content",{}) or {}
            tit = (first.get("title") or "").strip()
            pub = (first.get("publisher") or "").strip()
            if tit:
                nline = f" • Nyhet: {tit} ({pub})"

        price = norm.get("latestClose")
        price_str = f"{price:.2f}" if isinstance(price,(int,float)) and price is not None else "–"
        rows.append(f"• {sym} {tag} — pris {price_str}{nline}")

        if want_ai and open_ai:
            try:
                txt = await open_ai.trade_comment(snap, signal)
                ai_blocks.append(f"🤖 {sym}: Varför {signal.lower()}?\n{txt}")
            except Exception as e:
                ai_blocks.append(f"(AI-kommentar misslyckades för {sym}: {e})")

    now_se = datetime.now(SE_TZ)
    now_et = datetime.now(US_TZ)
    header = (
        f"🌅 Premarket-check ({now_se:%Y-%m-%d})\n"
        f"SE {now_se:%H:%M} | ET {now_et:%H:%M}\n"
        f"Analyserar ägda tickers + senaste rubriker • markerar Köp/Håll/Sälj\n"
    )
    await send_long_message(bot, admin_chat_id, f"{header}\n" + "\n".join(rows))

    if ai_blocks:
        await send_long_message(bot, admin_chat_id, "\n\n".join(ai_blocks))

    try:
        out = {"ts": datetime.now(timezone.utc).isoformat(), "rows": rows, "ai": ai_blocks}
        with open(f"premarket_report_{now_se:%Y%m%d}.json", "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
