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
        news_for_you = ticker_obj.news # Hämta nyhetsartiklar
        pe_ratio = ticker_obj.info 

        stock_data = {
            "symbol": symbol,
            "name": info.get("shortName", "Okänd"),
            "sector": info.get("sector", "Okänd"),
            "prev3iousClose": info.get("previousClose"),
            "priceToEarningsRatio": info.get("priceToEarningsRatio"),
            "priceToBookRatio": info.get("priceToBookRatio"),
            "PE": info.get("trailingPE"),
            "latestClose": latest_close,
            "News": news_for_you,
            

        }

        print(f"✅ {symbol}: {stock_data['latestClose']}")
        #print(f"✅ {symbol}: {stock_data}['News']")


        results.append(stock_data)

    # Spara till JSON-fil
    with open("Stock_info.json", "w") as final:
        json.dump(results, final, indent=4, default=str)

    print("📁 Stock data sparad i Stock_info.json!")
    return results
