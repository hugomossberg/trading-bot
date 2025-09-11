# yfinance_stock.py
import os, json, asyncio
import yfinance as yf
from helpers import convert_keys_to_str
import universe_manager as UM
from universe_manager import load_state, save_state, rotate_universe
from signals import buy_or_sell  # om du vill logga signaler lokalt (valfritt)

async def analyse_stock(ib_client):
    UNIVERSE_ROWS = int(os.getenv("UNIVERSE_ROWS", "10"))
    CANDIDATE_MULTIPLIER = int(os.getenv("CANDIDATE_MULTIPLIER", "3"))

    # 1) Hämta tickers: kärna + kandidater
    core = await ib_client.get_stocks(rows=UNIVERSE_ROWS)
    print(f"✅ Hämtade {len(core)} aktier: {core}")

    candidates = await ib_client.get_stocks(rows=UNIVERSE_ROWS * CANDIDATE_MULTIPLIER)
    print(f"✅ Hämtade {len(candidates)} aktier: {candidates}")

    # 2) Läs state och rotera universum (via UM.*)
    state = UM.load_state()
    prev_uni = state.get("universe") or core
    new_uni, dropped, added = UM.rotate_universe(prev_uni, candidates, state)
    state["universe"] = new_uni
    UM.save_state(state)

    print(f"✅ {len(new_uni)} tickers (roterat).")
    if dropped: print(f"   − Droppade: {', '.join(dropped)}")
    if added:   print(f"   + Lade till: {', '.join(added)}")

    # 3) Hämta Yahoo-data för ENDAST universum
    results = []
    for symbol in new_uni:
        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            hist = t.history(period="1mo")
            latest_close = hist["Close"].iloc[-1] if not hist.empty else None
            stock_data = {
                "symbol": symbol,
                "name": info.get("shortName", "Okänd"),
                "sector": info.get("sector", "Okänd"),
                "previousClose": info.get("previousClose"),
                "priceToEarningsRatio": info.get("priceToEarningsRatio"),
                "priceToBookRatio": info.get("priceToBookRatio"),
                "marketCap": info.get("marketCap"),
                "PE": info.get("trailingPE"),
                "beta": info.get("beta"),
                "trailingEps": info.get("trailingEps"),
                "dividendYield": info.get("dividendYield"),
                "latestClose": latest_close,
                "News": getattr(t, "news", None),
            }
            print(f"✅ {symbol}: {latest_close}")
            results.append(stock_data)
        except Exception as e:
            print(f"Fel vid hämtning av {symbol}: {e}")

    with open("Stock_info.json", "w", encoding="utf-8") as f:
        json.dump(convert_keys_to_str(results), f, indent=4, ensure_ascii=False, default=str, allow_nan=True)

    print("Stock data sparad i Stock_info.json!")
    return results

