import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from app.config import STOCK_INFO_PATH
from app.core.helpers import market_open_now
from app.core.market_profile import PROFILE, MARKET_PROFILE
from app.data.market_data import MarketDataService

log = logging.getLogger("scanner")
md = MarketDataService()

# gamla / ogiltiga / problematiska ticker-symboler
_BAD_IB_SYMBOLS = {
    "CCIV",
    "TWTR",
    "NETE",
    "ANTM",
    "ATVI",
}

# symboler som i praktiken bör undvikas redan i scanner-lagret
_STALE_OR_DELISTED_SYMBOLS = {
    "CCIV",
    "TWTR",
    "NETE",
    "ANTM",
    "ATVI",
}


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on", "y"}


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


def _file_age_minutes(path: str) -> float | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        age_sec = max(0.0, datetime.now(timezone.utc).timestamp() - p.stat().st_mtime)
        return age_sec / 60.0
    except Exception:
        return None


def _built_today(path: str) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    try:
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).date()
        today = datetime.now(timezone.utc).date()
        return mtime == today
    except Exception:
        return False


def should_rebuild_stock_info(path: str, min_rows: int, current_rows: int) -> tuple[bool, str]:
    p = Path(path)

    if _env_bool("SCANNER_DISABLE_CACHE", False):
        return True, "SCANNER_DISABLE_CACHE=on"

    if _env_bool("FORCE_UNIVERSE_REBUILD", False):
        return True, "FORCE_UNIVERSE_REBUILD=on"

    if not p.exists():
        return True, "saknas"

    if current_rows < min_rows:
        return True, f"otillräcklig ({current_rows}/{min_rows})"

    age_min = _file_age_minutes(path)
    if age_min is None:
        return True, "kan inte läsa filålder"

    # under öppet marknadsläge: tillåt ändå refresh om filen blivit gammal
    intraday_max_age = _env_int("SCANNER_MAX_AGE_MIN_OPEN", 45)
    closed_max_age = _env_int("SCANNER_MAX_AGE_MIN_CLOSED", 360)

    if market_open_now():
        if age_min > intraday_max_age:
            return True, f"för gammal under öppning ({int(age_min)} min > {intraday_max_age})"
        return False, f"market open, cache ({int(age_min)} min)"

    # utanför öppning: rebuild om ny dag eller gammal cache
    if not _built_today(path):
        return True, "ny dag / gammal cache"

    if age_min > closed_max_age:
        return True, f"för gammal ({int(age_min)} min > {closed_max_age})"

    return False, f"fil ok ({int(age_min)} min)"


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

    if symbol in _STALE_OR_DELISTED_SYMBOLS:
        return False, "stale/delisted symbol"

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
        "limit": max(limit * 8, 150),
        "priceMoreThan": PROFILE["min_price"],
        "volumeMoreThan": _env_int("SCANNER_MIN_VOLUME", 200000),
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

        target_candidates = _env_int("SCANNER_TARGET_CANDIDATES", min(max(limit * 10, 180), 500))

        for row in rows:
            sym = (row.get("symbol") or "").upper().strip()
            if not sym or sym in seen:
                continue
            if sym in _STALE_OR_DELISTED_SYMBOLS:
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

    target_rows_default = min(max(limit * 5, 80), 200)
    full_fetch_limit_default = min(max(limit + 100, 180), 300)

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
            if sym in _BAD_IB_SYMBOLS or sym in _STALE_OR_DELISTED_SYMBOLS:
                log.info("[scanner] SKIP  %3d/%d %-6s | blacklist/stale", i, len(selected), sym)
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
                    "isActivelyTrading": None,
                }

            if profile.get("isActivelyTrading") is False:
                log.info("[scanner] SKIP  %3d/%d %-6s | inactive", i, len(selected), sym)
                continue

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
                financials_limit = _env_int("SCANNER_FINANCIALS_LIMIT", 120)
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
    minimum_usable = max(10, min(min_count, 40))
    current_rows = len(data) if isinstance(data, list) else 0

    needs_rebuild, rebuild_reason = should_rebuild_stock_info(
        path=str(STOCK_INFO_PATH),
        min_rows=minimum_usable,
        current_rows=current_rows,
    )

    if not needs_rebuild and isinstance(data, list):
        log.info(
            "[scanner] Stock_info.json OK – %s (%s rows)",
            rebuild_reason,
            len(data),
        )
        return data

    log.info(
        "[scanner] Stock_info rebuild: %s (%s/%s)",
        rebuild_reason,
        current_rows,
        minimum_usable,
    )

    data = await refresh_stock_info(ib_client=ib_client, limit=min_count)
    return data or []