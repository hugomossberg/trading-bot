import os
import requests
from typing import Any
from dotenv import load_dotenv

load_dotenv()

class FMPClient:
    BASE_URL = "https://financialmodelingprep.com/stable"

    def __init__(self, api_key: str | None = None, timeout: int = 10):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise ValueError("FMP_API_KEY saknas")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"apikey": self.api_key})

    def _get(self, path: str, **params) -> Any:
        url = f"{self.BASE_URL}/{path.lstrip('/')}"
        try:
            r = self.session.get(url, params=params, timeout=self.timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            raise RuntimeError(f"FMP request failed: {path} params={params} error={e}") from e

    # Quotes
    def quote(self, symbol: str):
        data = self._get("quote", symbol=symbol)
        return data[0] if data else {}

    def quote_short(self, symbol: str):
        data = self._get("quote-short", symbol=symbol)
        return data[0] if data else {}

    def batch_quote(self, symbols: list[str]):
        return self._get("batch-quote", symbols=",".join(symbols))

    def batch_quote_short(self, symbols: list[str]):
        return self._get("batch-quote-short", symbols=",".join(symbols))

    def aftermarket_quote(self, symbol: str):
        data = self._get("aftermarket-quote", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data

    def batch_aftermarket_quote(self, symbols: list[str]):
        return self._get("batch-aftermarket-quote", symbols=",".join(symbols))

    # Company
    def profile(self, symbol: str):
        data = self._get("profile", symbol=symbol)
        return data[0] if data else {}

    def key_metrics_ttm(self, symbol: str):
        data = self._get("key-metrics-ttm", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data

    def ratios_ttm(self, symbol: str):
        data = self._get("ratios-ttm", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data

    def income_statement_ttm(self, symbol: str):
        data = self._get("income-statement-ttm", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data

    def analyst_estimates(self, symbol: str, period: str = "annual", limit: int = 5):
        return self._get("analyst-estimates", symbol=symbol, period=period, page=0, limit=limit)

    # News
    def stock_news(self, symbols: str, limit: int = 3):
        return self._get("news/stock", symbols=symbols, limit=limit)

    def stock_news_latest(self, limit: int = 20, page: int = 0):
        return self._get("news/stock-latest", page=page, limit=limit)

    # Screener
    def screener(self, **filters):
        return self._get("company-screener", **filters)

    # Charts
    def historical_chart(self, symbol: str, interval: str = "1min"):
        return self._get(f"historical-chart/{interval}", symbol=symbol)

    def historical_eod_light(self, symbol: str):
        return self._get("historical-price-eod/light", symbol=symbol)

    def income_statement(self, symbol: str, period: str = "annual", limit: int = 5):
        return self._get("income-statement", symbol=symbol, period=period, limit=limit)

    def balance_sheet(self, symbol: str, period: str = "annual", limit: int = 5):
        return self._get("balance-sheet-statement", symbol=symbol, period=period, limit=limit)

    def cash_flow(self, symbol: str, period: str = "annual", limit: int = 5):
        return self._get("cash-flow-statement", symbol=symbol, period=period, limit=limit)

    def ratios(self, symbol: str):
        data = self._get("ratios", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data

    def key_metrics(self, symbol: str):
        data = self._get("key-metrics", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data
    
    def financial_growth(self, symbol: str):
        data = self._get("financial-growth", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data

    def financial_scores(self, symbol: str):
        data = self._get("financial-scores", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data