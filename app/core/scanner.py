import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from app.config import STOCK_INFO_PATH
from app.core.market_profile import PROFILE, MARKET_PROFILE
from app.data.market_data import MarketDataService

log = logging.getLogger("scanner")
md = MarketDataService()

_STOCK_INFO_MAX_AGE_MIN = 60
_BAD_IB_SYMBOLS = {"CCIV", "TWTR", "NETE"}


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _to_float(x, default=None):
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
        return float(x)
    except Exception:
        return default


def _fmt_num(x, digits=2):
    v = _to_float(x, None)
    if v is None:
        return "-"
    return f"{v:.{digits}f}"


def _write_stock_info(rows: list[dict]):
    Path(STOCK_INFO_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(STOCK_INFO_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def _read_stock_info() -> list[dict] | None:
    try:
        with open(STOCK_INFO_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else None
    except Exception:
        return None


def _stock_info_is_stale(max_age_minutes: int = _STOCK_INFO_MAX_AGE_MIN) -> bool:
    p = Path(STOCK_INFO_PATH)
    if not p.exists():
        return True
    try:
        age_seconds = datetime.now(timezone.utc).timestamp() - p.stat().st_mtime
        return age_seconds > (max_age_minutes * 60)
    except Exception:
        return True


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
        "limit": max(limit * 5, 80),
        "priceMoreThan": PROFILE["min_price"],
        "volumeMoreThan": 200000,
    }

    min_market_cap = PROFILE.get("min_market_cap")
    if min_market_cap:
        filters["marketCapMoreThan"] = int(min_market_cap)

    max_scan_price = _env_int("MAX_SCAN_PRICE", 0)
    if max_scan_price > 0:
        filters["priceLowerThan"] = max_scan_price

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

        target_candidates = min(max(limit * 8, 120), 300)

        for row in rows:
            sym = (row.get("symbol") or "").upper().strip()
            if not sym or sym in seen:
                continue

            seen.add(sym)
            symbols.append(sym)

            if len(symbols) >= target_candidates:
                break

        if symbols:
            log.info("[scanner] Screener gav %d kandidater", len(symbols))
            return symbols

    except Exception as e:
        log.warning("[scanner] Screener misslyckades: %s", e)

    fallback = _fallback_tickers()
    log.warning("[scanner] Använder fallback-tickers (%d st)", len(fallback))
    return fallback


def _is_etf_like_name(name: str) -> bool:
    name_lower = (name or "").lower()
    tokens = set(name_lower.replace(",", " ").replace(".", " ").split())

    return (
        "etf" in tokens
        or "fund" in tokens
        or "trust" in tokens
        or ("index" in tokens and "fund" in tokens)
    )


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

    target_rows_default = min(max(limit * 5, 80), 150)
    full_fetch_limit_default = min(max(limit + 60, 120), 150)

    target_rows = _env_int("SCANNER_TARGET_ROWS", target_rows_default)
    full_fetch_limit = _env_int("SCANNER_FETCH_LIMIT", full_fetch_limit_default)

    selected = tickers[:full_fetch_limit]

    log.info(
        "[scanner] BUILD START | candidates=%d | fetch=%d | target_rows=%d",
        len(tickers),
        len(selected),
        target_rows,
    )

    try:
        quotes = md.get_batch_quotes(selected)
    except Exception as e:
        log.warning("[scanner] batch quotes misslyckades: %s", e)
        quotes = {}

    for i, sym in enumerate(selected, start=1):
        try:
            if sym in _BAD_IB_SYMBOLS:
                log.info("[scanner] SKIP  %3d/%d %-6s | blacklist/ogiltig hos IB", i, len(selected), sym)
                continue

            quote = quotes.get(sym) or {}
            if not quote:
                try:
                    quote = md.get_quote(sym)
                except Exception as e:
                    log.warning("[scanner] FETCH %3d/%d %-6s | quote fail: %s", i, len(selected), sym, e)
                    continue

            try:
                profile = md.get_profile(sym)
            except Exception as e:
                log.warning("[scanner] FETCH %3d/%d %-6s | profile fail: %s", i, len(selected), sym, e)
                profile = {
                    "name": sym,
                    "marketCap": quote.get("marketCap"),
                    "beta": None,
                    "sector": None,
                    "lastDividend": None,
                    "isEtf": None,
                }

            if profile.get("isEtf"):
                log.info("[scanner] SKIP  %3d/%d %-6s | ETF", i, len(selected), sym)
                continue

            if _is_etf_like_name(profile.get("name") or sym):
                log.info("[scanner] SKIP  %3d/%d %-6s | fond/ETF-lik", i, len(selected), sym)
                continue

            try:
                fundamentals = md.get_fundamentals(sym)
            except Exception as e:
                log.warning("[scanner] FETCH %3d/%d %-6s | fundamentals fail: %s", i, len(selected), sym, e)
                fundamentals = {}

            try:
                financials_limit = _env_int("SCANNER_FINANCIALS_LIMIT", 30)
                if i <= financials_limit:
                    financials = md.get_financials(sym)
                else:
                    financials = {}
            except Exception as e:
                log.warning("[scanner] FETCH %3d/%d %-6s | financials fail: %s", i, len(selected), sym, e)
                financials = {}

            stock = _build_stock_row(sym, quote, profile, fundamentals, financials)
            stock.update(financials)

            stock["News"] = []

            ok, reason = _is_good_snapshot(stock)
            if not ok:
                log.info("[scanner] SKIP  %3d/%d %-6s | %s", i, len(selected), sym, reason)
                continue

            rows.append(stock)

            log.info(
                "[scanner] KEEP  %3d/%d %-6s | pris=%7s | PE=%7s | EPS=%7s | rev=%7s",
                i,
                len(selected),
                sym,
                _fmt_num(stock.get("latestClose")),
                _fmt_num(stock.get("PE")),
                _fmt_num(stock.get("trailingEps")),
                _fmt_num(stock.get("revenueGrowth")),
            )

            if len(rows) >= target_rows:
                break

        except Exception as e:
            log.warning("[scanner] FETCH %3d/%d %-6s | fail: %s", i, len(selected), sym, e)

    if not rows:
        old = _read_stock_info()
        if isinstance(old, list) and old:
            log.warning("[scanner] Ingen ny data – behåller befintlig Stock_info.json (%d rader).", len(old))
            return old
        log.warning("[scanner] Ingen ny data och ingen gammal fil finns – skriver tom Stock_info.json.")
        return []

    _write_stock_info(rows)
    log.info("[scanner] BUILD DONE | stock_info rows=%d", len(rows))
    return rows


async def ensure_stock_info(ib_client=None, min_count: int = 10) -> list[dict]:
    data = _read_stock_info()
    minimum_usable = max(10, min(min_count, 30))

    if (
        isinstance(data, list)
        and len(data) >= minimum_usable
        and not _stock_info_is_stale()
    ):
        return data

    log.info(
        "[scanner] Stock_info.json saknas/korrupt/gammal/otillräcklig (%s/%s) – bygger om…",
        len(data) if isinstance(data, list) else 0,
        minimum_usable,
    )
    data = await refresh_stock_info(ib_client=ib_client, limit=min_count)
    return data or []