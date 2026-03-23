import math
import os
import time
import logging
from copy import deepcopy
from typing import Optional

import pandas as pd

from app.core.market_profile import MARKET_PROFILE

log = logging.getLogger("technicals")

_HISTORY_CACHE = {}
_HISTORY_TTL_SEC = 60
_IB_CLIENT = None

# Kända problem-/döda symboler som bara skapar Error 200 / brus i IB
_INVALID_IB_SYMBOLS = {
    "CCIV",
    "TWTR",
    "NETE",
}

# Symboler som behöver mappas för IB-kontrakt
_IB_SYMBOL_MAP = {
    "BRK-B": "BRK B",
}

# Undvik att spamma samma sim-logg hela tiden
_LAST_SIM_LOG = {}


def _sim_enabled() -> bool:
    return os.getenv("SIM_MARKET", "0").strip().lower() in {"1", "true", "yes", "on"}


def _sim_profile() -> str:
    return os.getenv("SIM_PROFILE", "flat").strip().lower()


def set_ib_client(ib_client):
    global _IB_CLIENT
    _IB_CLIENT = ib_client


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    except Exception:
        return default


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").upper().strip()


def _should_skip_ib_symbol(symbol: str) -> bool:
    sym = _normalize_symbol(symbol)
    if not sym:
        return True
    if sym in _INVALID_IB_SYMBOLS:
        return True
    return False


def _period_to_ib_duration(period: str) -> str:
    mapping = {
        "1mo": "1 M",
        "3mo": "3 M",
        "6mo": "6 M",
        "1y": "1 Y",
        "2y": "2 Y",
    }
    return mapping.get(period, "6 M")


def _interval_to_ib_bar_size(interval: str) -> str:
    mapping = {
        "1d": "1 day",
        "1h": "1 hour",
        "30m": "30 mins",
        "15m": "15 mins",
        "5m": "5 mins",
        "1m": "1 min",
    }
    return mapping.get(interval, "1 day")


def _build_contract(symbol: str):
    try:
        from ib_insync import Stock
    except Exception as e:
        log.warning("[technicals] Kunde inte importera ib_insync Stock: %s", e)
        return None

    sym = _normalize_symbol(symbol)
    if not sym:
        return None

    if _should_skip_ib_symbol(sym):
        return None

    if MARKET_PROFILE == "SE":
        base_symbol = sym.replace(".ST", "")
        if not base_symbol:
            return None
        return Stock(base_symbol, "SMART", "SEK")

    mapped_symbol = _IB_SYMBOL_MAP.get(sym, sym)
    return Stock(mapped_symbol, "SMART", "USD")


def _bars_to_df(bars):
    if not bars:
        return None

    rows = []
    for bar in bars:
        rows.append(
            {
                "Date": getattr(bar, "date", None),
                "Open": _safe_float(getattr(bar, "open", None)),
                "High": _safe_float(getattr(bar, "high", None)),
                "Low": _safe_float(getattr(bar, "low", None)),
                "Close": _safe_float(getattr(bar, "close", None)),
                "Volume": _safe_float(getattr(bar, "volume", None), 0.0),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return None

    needed = ["Open", "High", "Low", "Close", "Volume"]
    for col in needed:
        if col not in df.columns:
            df[col] = None

    # Rensa bort helt trasiga rader
    df = df.dropna(subset=["Close"])
    if df.empty:
        return None

    return df.reset_index(drop=True)


def fetch_price_history(symbol: str, period: str = "6mo", interval: str = "1d"):
    """
    Hämtar historiska candles via IB.
    Returnerar pandas DataFrame eller None.
    Cachar kortvarigt för att minska upprepade anrop.
    """
    sym = _normalize_symbol(symbol)
    cache_key = (sym, period, interval)
    now = time.time()

    cached = _HISTORY_CACHE.get(cache_key)
    if cached:
        ts, df = cached
        if now - ts < _HISTORY_TTL_SEC:
            return df

    if _should_skip_ib_symbol(sym):
        _HISTORY_CACHE[cache_key] = (now, None)
        return None

    if _IB_CLIENT is None or not getattr(_IB_CLIENT, "ib", None):
        log.warning("[technicals] IB client saknas för %s", sym)
        _HISTORY_CACHE[cache_key] = (now, None)
        return None

    try:
        ib = _IB_CLIENT.ib
        if not ib.isConnected():
            log.warning("[technicals] IB ej ansluten för %s", sym)
            _HISTORY_CACHE[cache_key] = (now, None)
            return None

        contract = _build_contract(sym)
        if contract is None:
            _HISTORY_CACHE[cache_key] = (now, None)
            return None

        duration_str = _period_to_ib_duration(period)
        bar_size = _interval_to_ib_bar_size(interval)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
            keepUpToDate=False,
        )

        df = _bars_to_df(bars)
        if df is None or df.empty:
            log.warning("[technicals] Ingen IB-prisdata för %s", sym)
            _HISTORY_CACHE[cache_key] = (now, None)
            return None

        _HISTORY_CACHE[cache_key] = (now, df)
        return df

    except Exception as e:
        log.warning("[technicals] IB historical fail för %s: %s", sym, e)
        _HISTORY_CACHE[cache_key] = (now, None)
        return None


def compute_sma(series, window: int) -> Optional[float]:
    if series is None or len(series) < window:
        return None
    value = series.rolling(window=window).mean().iloc[-1]
    return _safe_float(value)


def compute_rsi(series, window: int = 14) -> Optional[float]:
    if series is None or len(series) < window + 1:
        return None

    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()

    last_gain = _safe_float(avg_gain.iloc[-1])
    last_loss = _safe_float(avg_loss.iloc[-1])

    if last_gain is None or last_loss is None:
        return None

    if last_loss == 0:
        return 100.0

    rs = last_gain / last_loss
    rsi = 100 - (100 / (1 + rs))
    return _safe_float(rsi)


def compute_atr(df, window: int = 14) -> Optional[float]:
    if df is None or len(df) < window + 1:
        return None

    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    true_range = tr1.combine(tr2, max).combine(tr3, max)
    atr = true_range.rolling(window=window).mean().iloc[-1]
    return _safe_float(atr)


def compute_momentum(series, lookback: int = 20) -> Optional[float]:
    if series is None or len(series) < lookback + 1:
        return None

    current = _safe_float(series.iloc[-1])
    past = _safe_float(series.iloc[-1 - lookback])

    if current is None or past is None or past == 0:
        return None

    return ((current / past) - 1.0) * 100.0


def _empty_snapshot():
    return {
        "price": None,
        "sma20": None,
        "sma50": None,
        "rsi14": None,
        "atr14": None,
        "atr_pct": None,
        "volume": None,
        "avg_volume_20": None,
        "avg_dollar_volume_20": None,
        "volume_ratio": None,
        "momentum_20": None,
        "momentum_60": None,
    }


def _log_sim_once(symbol: str, profile: str, price):
    key = _normalize_symbol(symbol)
    payload = (profile, price)

    if _LAST_SIM_LOG.get(key) == payload:
        return

    _LAST_SIM_LOG[key] = payload
    if os.getenv("DEBUG_SIM_TECHNICALS", "0").strip().lower() in {"1", "true", "yes", "on"}:
        log.info("[technicals] Simulerar technicals för %s med profil %s, pris ~%s", symbol, profile, price)

def _apply_simulation(symbol: str, technicals: dict) -> dict:
    t = deepcopy(technicals or {})
    profile = _sim_profile()

    price = _safe_float(t.get("price"))
    sma20 = _safe_float(t.get("sma20"))
    sma50 = _safe_float(t.get("sma50"))
    rsi14 = _safe_float(t.get("rsi14"))
    volume_ratio = _safe_float(t.get("volume_ratio"))
    momentum_20 = _safe_float(t.get("momentum_20"))
    momentum_60 = _safe_float(t.get("momentum_60"))
    atr_pct = _safe_float(t.get("atr_pct"))

    # Om ingen riktig price finns, simulera inte fram skräp
    if price is None:
        return t

    if profile == "breakout":
        t["price"] = round(price * 1.025, 2)
        if sma20 is not None:
            t["sma20"] = round(min(t["price"] * 0.995, sma20 * 1.005), 2)
        if sma50 is not None:
            t["sma50"] = round(min((t.get("sma20") or sma50) * 0.995, sma50 * 1.002), 2)
        if rsi14 is not None:
            t["rsi14"] = min(82.0, rsi14 + 8.0)
        t["volume_ratio"] = max(1.8, volume_ratio) if volume_ratio is not None else 1.8
        if momentum_20 is not None:
            t["momentum_20"] = momentum_20 + 4.0
        if momentum_60 is not None:
            t["momentum_60"] = momentum_60 + 6.0
        if atr_pct is not None:
            t["atr_pct"] = max(atr_pct, 2.2)

    elif profile == "selloff":
        t["price"] = round(price * 0.975, 2)
        if sma20 is not None:
            t["sma20"] = round(max(t["price"] * 1.01, sma20 * 0.998), 2)
        if sma50 is not None:
            t["sma50"] = round(max((t.get("sma20") or sma50) * 0.995, sma50 * 0.999), 2)
        if rsi14 is not None:
            t["rsi14"] = max(18.0, rsi14 - 10.0)
        t["volume_ratio"] = max(1.6, volume_ratio) if volume_ratio is not None else 1.6
        if momentum_20 is not None:
            t["momentum_20"] = momentum_20 - 5.0
        if momentum_60 is not None:
            t["momentum_60"] = momentum_60 - 7.0
        if atr_pct is not None:
            t["atr_pct"] = max(atr_pct, 2.5)

    elif profile == "choppy":
        t["price"] = round(price * 1.003, 2)
        if rsi14 is not None:
            t["rsi14"] = min(75.0, max(25.0, rsi14 + 2.0))
        if volume_ratio is not None:
            t["volume_ratio"] = max(1.1, volume_ratio)

    elif profile == "flat":
        pass

    _log_sim_once(symbol, profile, t.get("price"))
    return t


def build_technical_snapshot(symbol: str):
    """
    Returnerar technicals-dict för symbolen.
    Returnerar alltid en dict, aldrig {}.
    """
    sym = _normalize_symbol(symbol)
    if not sym:
        return _empty_snapshot()

    df = fetch_price_history(sym, period="6mo", interval="1d")
    if df is None or df.empty:
        snapshot = _empty_snapshot()
        if _sim_enabled():
            return _apply_simulation(sym, snapshot)
        return snapshot

    close = df["Close"]
    volume = df["Volume"]

    price = _safe_float(close.iloc[-1])
    sma20 = compute_sma(close, 20)
    sma50 = compute_sma(close, 50)
    rsi14 = compute_rsi(close, 14)
    atr14 = compute_atr(df, 14)
    avg_volume_20 = compute_sma(volume, 20)
    momentum_20 = compute_momentum(close, 20)
    momentum_60 = compute_momentum(close, 60)

    volume_now = _safe_float(volume.iloc[-1])

    volume_ratio = None
    if volume_now is not None and avg_volume_20 not in (None, 0):
        volume_ratio = volume_now / avg_volume_20

    atr_pct = None
    if atr14 is not None and price not in (None, 0):
        atr_pct = (atr14 / price) * 100.0

    avg_dollar_volume_20 = None
    if price not in (None, 0) and avg_volume_20 not in (None, 0):
        avg_dollar_volume_20 = price * avg_volume_20

    snapshot = {
        "price": price,
        "sma20": sma20,
        "sma50": sma50,
        "rsi14": rsi14,
        "atr14": atr14,
        "atr_pct": atr_pct,
        "volume": volume_now,
        "avg_volume_20": avg_volume_20,
        "avg_dollar_volume_20": avg_dollar_volume_20,
        "volume_ratio": volume_ratio,
        "momentum_20": momentum_20,
        "momentum_60": momentum_60,
    }

    if _sim_enabled():
        return _apply_simulation(sym, snapshot)

    return snapshot