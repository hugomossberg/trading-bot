import os

from app.core.autoscan_shared import to_float
from app.data.market_data import MarketDataService


_md = MarketDataService()


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _get_fmp_quote(symbol: str) -> dict | None:
    q = _md.get_quote(symbol) or {}

    price = to_float(q.get("price"), 0)
    previous_close = to_float(q.get("previousClose"), 0)
    volume = to_float(q.get("volume"), None)

    ref_price = price or previous_close
    if not ref_price or ref_price <= 0:
        return None

    return {
        "symbol": symbol,
        "bid": None,
        "ask": None,
        "last": ref_price,
        "market": ref_price,
        "close": previous_close or ref_price,
        "mid": ref_price,
        "spread": None,
        "spread_pct": None,
        "volume": volume,
        "source": "fmp",
    }


async def validate_pretrade_buy(
    *,
    symbol: str,
    raw: dict,
    analysis: dict,
    ib_client,
    qty: int,
    max_order_value: float,
) -> dict:
    raw = raw or {}
    analysis = analysis or {}
    raw_technicals = analysis.get("raw_technicals") or raw.get("_pipeline_technicals") or {}

    pipeline_price = (
        to_float(raw_technicals.get("price"), 0)
        or to_float(raw.get("latestClose"), 0)
    )

    sma20 = to_float(raw_technicals.get("sma20"), None)
    sma50 = to_float(raw_technicals.get("sma50"), None)
    rsi14 = to_float(raw_technicals.get("rsi14"), None)
    atr_pct = to_float(raw_technicals.get("atr_pct"), None)
    volume_ratio = to_float(raw_technicals.get("volume_ratio"), None)

    quote = _get_fmp_quote(symbol)
    if not quote:
        return {"ok": False, "reason": "no_fmp_quote"}

    bid = quote.get("bid")
    ask = quote.get("ask")
    last = quote.get("last")
    mid = quote.get("mid")
    market = quote.get("market")
    close = quote.get("close")
    spread_pct = quote.get("spread_pct")

    live_price = ask or last or mid or market or close
    if not live_price or live_price <= 0:
        return {"ok": False, "reason": "no_live_price", "quote": quote}

    # FMP har normalt inte riktig bid/ask, så default ska vara AV här
    require_bid_ask = os.getenv("PRETRADE_REQUIRE_BID_ASK", "0").strip().lower() in {
        "1", "true", "yes", "on"
    }

    if require_bid_ask and (bid is None or ask is None):
        return {"ok": False, "reason": "missing_bid_ask", "quote": quote}

    max_spread_pct = _env_float("PRETRADE_MAX_SPREAD_PCT", 0.35)
    max_drift_pct = _env_float("PRETRADE_MAX_DRIFT_PCT", 1.00)
    max_distance_sma20_pct = _env_float("PRETRADE_MAX_DISTANCE_SMA20_PCT", 4.0)
    max_rsi_buy = _env_float("PRETRADE_MAX_RSI_BUY", 76.0)
    min_volume_ratio_buy = _env_float("PRETRADE_MIN_VOLUME_RATIO_BUY", 0.90)
    max_atr_pct_buy = _env_float("PRETRADE_MAX_ATR_PCT_BUY", 8.0)

    if spread_pct is not None and spread_pct > max_spread_pct:
        return {
            "ok": False,
            "reason": f"spread_too_wide:{spread_pct:.2f}%",
            "quote": quote,
        }

    drift_pct = None
    if pipeline_price and pipeline_price > 0:
        drift_pct = abs((live_price - pipeline_price) / pipeline_price) * 100.0
        if drift_pct > max_drift_pct:
            return {
                "ok": False,
                "reason": f"price_drift_too_large:{drift_pct:.2f}%",
                "quote": quote,
            }

    if sma20 is not None and live_price <= sma20:
        return {"ok": False, "reason": "live_price_below_sma20", "quote": quote}

    if sma20 is not None and sma50 is not None and sma20 < sma50:
        return {"ok": False, "reason": "trend_structure_broken", "quote": quote}

    if sma20 is not None and sma20 > 0 and live_price > sma20:
        distance_sma20_pct = ((live_price - sma20) / sma20) * 100.0
        if distance_sma20_pct > max_distance_sma20_pct:
            return {
                "ok": False,
                "reason": f"too_extended_vs_sma20:{distance_sma20_pct:.2f}%",
                "quote": quote,
            }

    if rsi14 is not None and rsi14 > max_rsi_buy:
        return {"ok": False, "reason": f"rsi_too_high:{rsi14:.1f}", "quote": quote}

    if atr_pct is not None and atr_pct > max_atr_pct_buy:
        return {"ok": False, "reason": f"atr_too_high:{atr_pct:.2f}%", "quote": quote}

    if volume_ratio is not None and volume_ratio < min_volume_ratio_buy:
        return {
            "ok": False,
            "reason": f"volume_ratio_too_low:{volume_ratio:.2f}",
            "quote": quote,
        }

    est_value = live_price * qty
    if est_value > max_order_value:
        return {
            "ok": False,
            "reason": f"order_value_too_large:{est_value:.2f}",
            "quote": quote,
        }

    return {
        "ok": True,
        "reason": "ok",
        "live_price": live_price,
        "spread_pct": spread_pct,
        "drift_pct": drift_pct,
        "quote": quote,
        "est_value": est_value,
    }