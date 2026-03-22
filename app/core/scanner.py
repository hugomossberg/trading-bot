import json
import logging
from pathlib import Path

from app.config import STOCK_INFO_PATH
from app.core.market_profile import PROFILE, MARKET_PROFILE
from app.data.market_data import MarketDataService

log = logging.getLogger("scanner")
md = MarketDataService()

def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default

def _write_stock_info(rows: list[dict]):
    Path(STOCK_INFO_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(STOCK_INFO_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def _read_stock_info() -> list[dict] | None:
    try:
        with open(STOCK_INFO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _fallback_tickers() -> list[str]:
    if PROFILE["currency"] == "SEK":
        return ["VOLV-B.ST", "ERIC-B.ST", "SEB-A.ST", "ATCO-A.ST", "ABB.ST", "SWED-A.ST"]
    return ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "AMD", "TSLA", "NFLX", "INTC", "QCOM", "AVGO"]

def _is_good_snapshot(stock: dict) -> tuple[bool, str | None]:
    price = _to_float(stock.get("latestClose"))
    market_cap = _to_float(stock.get("marketCap"))
    name = (stock.get("name") or "").lower()
    symbol = (stock.get("symbol") or "").upper()

    min_price = PROFILE["min_price"]
    min_market_cap = PROFILE["min_market_cap"]

    leveraged_hints = [
        "2x", "3x", "ultra", "ultrapro", "daily",
        "bull", "bear", "short", "leveraged"
    ]

    if price is None:
        return False, "saknar pris"

    if price < min_price:
        return False, f"pris under {min_price}"

    if market_cap is not None and market_cap < min_market_cap:
        return False, f"market cap under {min_market_cap}"

    if symbol in {"TSLL", "TSLQ", "SQQQ"}:
        return False, "leveraged/inverse ETF"

    if any(hint in name for hint in leveraged_hints):
        return False, "leveraged/inverse ETF"

    return True, None

def _build_stock_row(
    symbol: str,
    quote: dict,
    profile: dict,
    fundamentals: dict | None = None,
    financials: dict | None = None,
) -> dict:
    fundamentals = fundamentals or {}
    financials = financials or {}

    return {
        "symbol": symbol,
        "name": profile.get("name") or symbol,
        "latestClose": quote.get("price"),
        "PE": fundamentals.get("pe"),
        "marketCap": (
            profile.get("marketCap")
            or quote.get("marketCap")
            or fundamentals.get("marketCap")
            or financials.get("marketCap")
        ),
        "beta": profile.get("beta"),
        "trailingEps": fundamentals.get("epsTTM"),
        "dividendYield": fundamentals.get("dividendYieldTTM"),
        "sector": profile.get("sector"),
        "News": [],
    }

def _screen_filters(limit: int) -> dict:
    filters = {
        "limit": max(limit * 4, 40),
        "priceMoreThan": PROFILE["min_price"],
        "volumeMoreThan": 200000,
    }

    min_market_cap = PROFILE.get("min_market_cap")
    if min_market_cap:
        filters["marketCapMoreThan"] = int(min_market_cap)

    if PROFILE["currency"] == "SEK" or MARKET_PROFILE == "SE":
        filters["country"] = "SE"
    else:
        filters["country"] = "US"

    return filters

def _get_candidate_symbols(limit: int) -> list[str]:
    try:
        filters = _screen_filters(limit)
        rows = md.screen_stocks(**filters)

        seen = set()
        symbols: list[str] = []

        for row in rows:
            sym = (row.get("symbol") or "").upper().strip()
            if not sym:
                continue
            if sym in seen:
                continue

            seen.add(sym)
            symbols.append(sym)

            if len(symbols) >= max(limit * 3, 25):
                break

        if symbols:
            log.info("[scanner] FMP screener gav %d kandidater", len(symbols))
            return symbols

    except Exception as e:
        log.warning("[scanner] FMP screener misslyckades: %s", e)

    fallback = _fallback_tickers()
    log.warning("[scanner] Använder fallback-tickers (%d st)", len(fallback))
    return fallback

async def refresh_stock_info(ib_client=None, limit: int = 50) -> list[dict]:
    tickers = _get_candidate_symbols(limit)

    if not tickers:
        old = _read_stock_info()
        if isinstance(old, list) and old:
            log.warning("[scanner] Inga tickers – behåller gammal Stock_info.json (%d rader).", len(old))
            return old
        log.warning("[scanner] Inga tickers och ingen gammal fil finns.")
        return []

    rows: list[dict] = []
    selected = tickers[: min(max(limit, 20), 40)]

    try:
        quotes = md.get_batch_quotes(selected)
    except Exception as e:
        log.warning("[scanner] batch quotes misslyckades: %s", e)
        quotes = {}

    for i, sym in enumerate(selected, start=1):
        try:
            log.info("[scanner] Hämtar %s (%d/%d)", sym, i, len(selected))

            # 1. Quote
            quote = quotes.get(sym) or {}
            if not quote:
                try:
                    quote = md.get_quote(sym)
                except Exception as e:
                    log.warning("[scanner] quote fail för %s: %s", sym, e)
                    continue

            # 2. Profile
            try:
                profile = md.get_profile(sym)
            except Exception as e:
                log.warning("[scanner] profile fail för %s: %s", sym, e)
                profile = {
                    "name": sym,
                    "marketCap": quote.get("marketCap"),
                    "beta": None,
                    "sector": None,
                    "lastDividend": None,
                    "isEtf": None,
                }

            # Skippa ETF:er
            if profile.get("isEtf"):
                log.info("[scanner] Skippar %s → ETF", sym)
                continue

            name_lower = (profile.get("name") or "").lower()
            if any(x in name_lower for x in ["fund", "index fund", "etf", "trust"]):
                log.info("[scanner] Skippar %s → fond/ETF-lik", sym)
                continue

            # 3. Fundamentals (TTM)
            try:
                fundamentals = md.get_fundamentals(sym)
            except Exception as e:
                log.warning("[scanner] fundamentals fail för %s: %s", sym, e)
                fundamentals = {}

            # 4. Financials (årliga)
            try:
                financials = md.get_financials(sym)
            except Exception as e:
                log.warning("[scanner] financials fail för %s: %s", sym, e)
                financials = {}

            # Bygg stock-dict och lägg till finansiell data
            stock = _build_stock_row(sym, quote, profile, fundamentals, financials)
            stock.update(financials)

            # 5. Nyheter
            try:
                news_items = md.fmp.stock_news(sym, limit=3) or []
                stock["News"] = [
                    {
                        "content": {
                            "title": n.get("title", ""),
                            "summary": n.get("text", "") or "",
                            "publisher": n.get("publisher") or n.get("site", ""),
                            "link": n.get("url", ""),
                        }
                    }
                    for n in news_items
                ]
            except Exception as e:
                log.warning("[scanner] news fail för %s: %s", sym, e)
                stock["News"] = []

            log.info(
                "[scanner] %s | PE=%s EPS=%s revGrowth=%s margin=%s debtEq=%s news=%d",
                sym,
                stock.get("PE"),
                stock.get("trailingEps"),
                stock.get("revenueGrowth"),
                stock.get("profitMargin"),
                stock.get("debtToEquity"),
                len(stock.get("News", [])),
            )

            ok, reason = _is_good_snapshot(stock)
            if not ok:
                log.info("[scanner] Skippar %s → %s", sym, reason)
                continue

            rows.append(stock)

            if len(rows) >= limit:
                break

        except Exception as e:
            log.warning("[scanner] Misslyckades med %s: %s", sym, e)

    if not rows:
        old = _read_stock_info()
        if isinstance(old, list) and old:
            log.warning("[scanner] Ingen ny data – behåller befintlig Stock_info.json (%d rader).", len(old))
            return old
        log.warning("[scanner] Ingen ny data och ingen gammal fil finns – skriver tom Stock_info.json.")
        return []

    _write_stock_info(rows)
    log.info("[scanner] Stock_info.json uppdaterad (%d rader).", len(rows))
    return rows

async def ensure_stock_info(ib_client=None, min_count: int = 10) -> list[dict]:
    data = _read_stock_info()
    minimum_usable = max(10, min(min_count, 30))

    if isinstance(data, list) and len(data) >= minimum_usable:
        return data

    log.info(
        "[scanner] Stock_info.json saknas/korrupt/otillräcklig (%s/%s) – bygger om…",
        len(data) if isinstance(data, list) else 0,
        minimum_usable,
    )
    data = await refresh_stock_info(ib_client=ib_client, limit=min_count)
    return data or []