#stock_data.py
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from app.config import STOCK_INFO_PATH

from app.config import STOCK_INFO_PATH
SE_TZ = ZoneInfo("Europe/Stockholm")


def load_stock_info() -> list:
    with open(STOCK_INFO_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_stock_by_symbol(symbol: str) -> dict | None:
    data = load_stock_info()
    stocks_by_symbol = {
        str(s.get("symbol", "")).upper(): s
        for s in data
        if s.get("symbol")
    }
    return stocks_by_symbol.get(symbol.upper())


def get_all_symbols() -> list[str]:
    data = load_stock_info()
    return sorted({
        (s.get("symbol") or "").upper()
        for s in data
        if s.get("symbol")
    })


def get_stock_info_updated_time() -> str:
    mtime = os.path.getmtime(STOCK_INFO_PATH)
    ts = datetime.fromtimestamp(mtime, SE_TZ)
    return ts.strftime("%Y-%m-%d %H:%M")