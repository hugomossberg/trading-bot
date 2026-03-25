import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from app.core.scanner import rebuild_stock_info_for_premarket
from app.core.signals import get_signal_analysis
from app.core.helpers import send_long_message
from app.data.fmp_client import FMPClient

log = logging.getLogger("premarket")

US_TZ = ZoneInfo("America/New_York")
SE_TZ = ZoneInfo("Europe/Stockholm")

fmp = FMPClient()


def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default


def _normalize_stock(stock: dict) -> dict:
    s = dict(stock or {})
    s["latestClose"] = _to_float(s.get("latestClose"), 0.0)
    s["PE"] = _to_float(s.get("PE"), 0.0)
    s["marketCap"] = _to_float(s.get("marketCap"), 0.0)
    s["beta"] = _to_float(s.get("beta"), 0.0)
    s["trailingEps"] = _to_float(s.get("trailingEps"), 0.0)
    s["dividendYield"] = _to_float(s.get("dividendYield"), 0.0)
    return s


def _fetch_fmp_snapshot(ticker: str) -> dict:
    quote = fmp.quote_short(ticker) or {}
    profile = fmp.profile(ticker) or {}
    news = fmp.stock_news(ticker, limit=3) or []

    stock = {
        "symbol": ticker,
        "name": profile.get("companyName") or ticker,
        "latestClose": quote.get("price"),
        "PE": profile.get("pe"),
        "marketCap": profile.get("marketCap"),
        "beta": profile.get("beta"),
        "trailingEps": profile.get("eps"),
        "dividendYield": profile.get("lastDividend"),
        "sector": profile.get("sector"),
        "News": [],
    }

    for n in news[:3]:
        stock["News"].append({
            "content": {
                "title": n.get("title", ""),
                "summary": n.get("text", "") or "",
                "publisher": n.get("publisher") or n.get("site", ""),
                "link": n.get("url", ""),
            }
        })

    return stock


async def run_premarket_scan(bot, ib_client, admin_chat_id: int, want_ai: bool = True, open_ai=None):
    """
    Körs före öppning.
    1) Bygger dagens stock_info.json fullt klart
    2) Läser positioner (IB) om IB finns
    3) Premarket-rapport på ägda innehav
    """

    try:
        built = await rebuild_stock_info_for_premarket(ib_client=ib_client, limit=50)
        if bot and admin_chat_id:
            await bot.send_message(
                admin_chat_id,
                f"Premarket: stock_info rebuild klar ({len(built)} rows)."
            )
    except Exception as e:
        log.exception("Premarket rebuild fail")
        if bot and admin_chat_id:
            await bot.send_message(
                admin_chat_id,
                f"Premarket: stock_info rebuild misslyckades ({e})."
            )

    if not ib_client or not ib_client.ib.isConnected():
        if bot and admin_chat_id:
            await bot.send_message(
                admin_chat_id,
                "Premarket: IBKR inte ansluten – hoppar innehavsanalys."
            )
        return

    positions = await ib_client.ib.reqPositionsAsync()
    held = {}
    for p in positions:
        sym = (p.contract.symbol or "").upper()
        qty = float(p.position or 0.0)
        if abs(qty) > 1e-6:
            held[sym] = held.get(sym, 0.0) + qty

    if not held:
        if bot and admin_chat_id:
            await bot.send_message(admin_chat_id, "Premarket: Inga aktier ägs just nu.")
        return

    rows = []
    ai_blocks = []

    for sym, qty in sorted(held.items(), key=lambda kv: (-abs(kv[1]), kv[0])):
        try:
            snap = _fetch_fmp_snapshot(sym)
            norm = _normalize_stock(snap)
        except Exception as e:
            snap = {"symbol": sym, "News": []}
            norm = _normalize_stock({"symbol": sym})
            rows.append(f"• {sym} ⚪ Håll — kunde inte hämta FMP-data ({e})")
            continue

        try:
            analysis = get_signal_analysis(norm)
            signal = analysis["signal"]
        except Exception as e:
            analysis = {
                "symbol": sym,
                "signal": "Håll",
                "error": str(e),
            }
            signal = analysis["signal"]

        tag = {"Köp": "🟢 Köp", "Sälj": "🔴 Sälj"}.get(signal, "⚪ Håll")

        nh = snap.get("News") or []
        nline = ""
        if nh:
            first = nh[0].get("content", {}) or {}
            tit = (first.get("title") or "").strip()
            pub = (first.get("publisher") or "").strip()
            if tit:
                nline = f" • Nyhet: {tit} ({pub})"

        price = norm.get("latestClose")
        price_str = f"{price:.2f}" if isinstance(price, (int, float)) and price is not None else "–"
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

    if bot and admin_chat_id:
        await send_long_message(bot, admin_chat_id, f"{header}\n" + "\n".join(rows))

        if ai_blocks:
            await send_long_message(bot, admin_chat_id, "\n\n".join(ai_blocks))

    try:
        Path("storage/reports").mkdir(parents=True, exist_ok=True)
        out = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "rows": rows,
            "ai": ai_blocks,
        }
        report_path = f"storage/reports/premarket_report_{now_se:%Y%m%d}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    except Exception:
        pass