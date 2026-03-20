# scanner.py
import os, json, logging, time
from datetime import datetime
from zoneinfo import ZoneInfo
from app.config import STOCK_INFO_PATH

import yfinance as yf

log = logging.getLogger("scanner")

SE_TZ = ZoneInfo("Europe/Stockholm")

def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default

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

def _write_stock_info(rows: list[dict]):
    with open(STOCK_INFO_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def _read_stock_info() -> list[dict] | None:
    try:
        with open(STOCK_INFO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _fallback_tickers() -> list[str]:
    # En liten, ofarlig fallback-lista
    return ["AAPL","MSFT","NVDA","AMZN","META","GOOGL","AMD","TSLA","NFLX","INTC","QCOM","AVGO"]

async def _ib_scanner(ib_client, limit: int) -> list[str]:
    """
    Försök hämta tickers via IBKR Market Scanner. Returnerar en lista med symboler.
    Kräver att ib_client.ib är connected.
    """
    try:
        from ib_insync import ScannerSubscription, TagValue
        instrument   = os.getenv("SCANNER_INSTRUMENT","STK")
        locationCode = os.getenv("SCANNER_LOCATION","STK.NASDAQ")
        scanCode     = os.getenv("SCANNER_CODE","MOST_ACTIVE")

        sub = ScannerSubscription()
        sub.instrument = instrument
        sub.locationCode = locationCode
        sub.scanCode = scanCode

        # IB kräver att vi kallar reqScannerData; ib_insync returnerar list med ScanData
        items = await ib_client.ib.reqScannerDataAsync(sub, [])
        syms = []
        for it in items[:limit*3]:  # ta lite extra, filtrerar sedan via yfinance
            con = getattr(it, "contractDetails", None)
            if con and con.contract and con.contract.symbol:
                syms.append(con.contract.symbol.upper())
        # Rensa dubbletter, bevara ordning
        seen = set(); out = []
        for s in syms:
            if s not in seen:
                seen.add(s); out.append(s)
        return out[:max(5,limit*2)]
    except Exception as e:
        log.error("[scanner] IB scanner misslyckades: %s", e)
        return []

async def refresh_stock_info(ib_client, limit: int = 50) -> list[dict]:
    """
    Bygger om Stock_info.json från IBKR-scanner (med yfinance-detaljer).
    Faller tillbaka till en statisk lista om IB misslyckas.
    """
    tickers = []
    if ib_client and ib_client.ib.isConnected():
        tickers = await _ib_scanner(ib_client, limit)
    if not tickers:
        tickers = _fallback_tickers()

    rows = []
    for sym in tickers[:limit]:
        try:
            rows.append(_fetch_yf_snapshot(sym))
            time.sleep(0.05)
        except Exception:
            pass

    if not rows:
        # sista fallback: skapa tom men giltig fil
        rows = [{"symbol": s, "name": s} for s in tickers[:limit]]

    _write_stock_info(rows)
    log.info("[scanner] Stock_info.json uppdaterad (%d rader).", len(rows))
    return rows

async def ensure_stock_info(ib_client, min_count: int = 10) -> list[dict]:
    """
    Garantier:
    - Finns fil? Är JSON giltig? Innehåller minst min_count rader?
    - Om inte: bygg om via refresh_stock_info.
    Returnerar listan.
    """
    data = _read_stock_info()
    if not isinstance(data, list) or len(data) < min_count:
        log.info("[scanner] Stock_info.json saknas/korrupt/otillräcklig – bygger om…")
        data = await refresh_stock_info(ib_client, limit=max(min_count * 3, 30))
    return data or []