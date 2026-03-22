#market_profile.py
import os

MARKET_PROFILE = os.getenv("MARKET_PROFILE", "US").strip().upper()

MARKET_PROFILES = {
    "US": {
        "timezone": "America/New_York",
        "open_hour": 9,
        "open_minute": 30,
        "close_hour": 16,
        "close_minute": 0,
        "currency": "USD",
        "scanner_instrument": "STK",
        "scanner_location": "STK.NASDAQ",
        "scanner_code": "TOP_PERC_GAIN",
        "min_price": 2.0,
        "min_market_cap": 300_000_000,
        "min_avg_cash_volume": 5_000_000,
    },
    "SE": {
        "timezone": "Europe/Stockholm",
        "open_hour": 9,
        "open_minute": 0,
        "close_hour": 17,
        "close_minute": 30,
        "currency": "SEK",
        "scanner_instrument": "STOCK.EU",
        "scanner_location": "STK.EU",
        "scanner_code": "TOP_PERC_GAIN",
        "min_price": 10.0,
        "min_market_cap": 1_000_000_000,
        "min_avg_cash_volume": 5_000_000,
    },
}

PROFILE = MARKET_PROFILES.get(MARKET_PROFILE, MARKET_PROFILES["US"])