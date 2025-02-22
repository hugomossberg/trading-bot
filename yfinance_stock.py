# yfinance_stock.py
import yfinance as yf
import json

async def analyse_stock(ib_client):
    tickers = await ib_client.get_stocks()  # Hämtar aktielistan
    results = []

    for symbol in tickers:
        ticker_obj = yf.Ticker(symbol)

        # Hantera 404-fel
        try:
            info = ticker_obj.info
            if not info:
                print(f"⚠️ Ingen data hittades för {symbol}, hoppar över...")
                continue

        except Exception as e:
            print(f"❌ Fel vid hämtning av {symbol}: {e}")
            continue  # Hoppa över den här aktien om fel uppstår

        # Hämta senaste stängningspris
        history_df = ticker_obj.history(period="1mo")
        latest_close = history_df["Close"].iloc[-1] if not history_df.empty else None

        stock_data = {
            "symbol": symbol,
            "name": info.get("shortName", "Okänd"),
            "sector": info.get("sector", "Okänd"),
            "previousClose": info.get("previousClose"),
            "latestClose": latest_close,
        }

        print(f"✅ {symbol}: {stock_data['latestClose']}")

        results.append(stock_data)

        # Vänta 1.5 sekunder för att undvika API-blockering

    # Spara till JSON-fil
    with open("Stock_info.json", "w") as final:
        json.dump(results, final, indent=4, default=str)

    print("📁 Stock data sparad i Stock_info.json!")
    return results
