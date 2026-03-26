import os
import time
import logging
import requests
from collections import Counter, deque
from typing import Any
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("fmp")


class FMPClient:
    BASE_URL = "https://financialmodelingprep.com/stable"

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 10,
        max_retries: int = 2,
        backoff_base: float = 1.5,
        min_interval_sec: float = 0.12,
    ):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise ValueError("FMP_API_KEY saknas")

        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.min_interval_sec = min_interval_sec

        self.session = requests.Session()
        self.session.headers.update({"apikey": self.api_key})

        self._last_request_ts = 0.0

        # Live usage monitor
        self._usage_window = deque()   # [(timestamp, endpoint_name), ...]
        self._usage_log_every_sec = 5.0
        self._last_usage_log_ts = 0.0
        self._warn_soft = 240
        self._warn_hard = 280
        self._usage_limit_per_min = 300

    def _respect_min_interval(self):
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self.min_interval_sec:
            time.sleep(self.min_interval_sec - elapsed)


    def _endpoint_label(self, path: str) -> str:
        path = (path or "").strip("/").lower()

        if path in {"quote", "quote-short", "batch-quote", "batch-quote-short", "aftermarket-quote", "batch-aftermarket-quote"}:
            return "quote"
        if path == "profile":
            return "profile"
        if path in {"key-metrics-ttm", "ratios-ttm"}:
            return "fundamentals"
        if path in {"financial-growth", "ratios", "financial-scores", "key-metrics", "income-statement"}:
            return "financials"
        if path.startswith("news/"):
            return "news"
        if path == "company-screener":
            return "screener"
        if path.startswith("historical-chart") or path.startswith("historical-price-eod"):
            return "history"

        return path

    def _usage_prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self._usage_window and self._usage_window[0][0] < cutoff:
            self._usage_window.popleft()

    def _usage_record(self, path: str, now: float | None = None) -> None:
        now = now or time.time()
        endpoint = self._endpoint_label(path)

        self._usage_window.append((now, endpoint))
        self._usage_prune(now)

        # logga inte exakt varje request, bara ibland
        if now - self._last_usage_log_ts < self._usage_log_every_sec:
            return

        self._last_usage_log_ts = now

        counts = Counter(endpoint_name for _, endpoint_name in self._usage_window)
        total = len(self._usage_window)

        parts = []
        for key in ("quote", "profile", "fundamentals", "financials", "news", "screener", "history"):
            if counts.get(key):
                parts.append(f"{key}={counts[key]}")

        msg = f"last_60s={total}/{self._usage_limit_per_min}"
        if parts:
            msg += " | " + " | ".join(parts)

        if total >= self._warn_hard:
            log.warning("[FMP-HARD] %s", msg)
        elif total >= self._warn_soft:
            log.warning("[FMP-WARN] %s", msg)
        else:
            log.info("[FMP] %s", msg)
    

    def _get(self, path: str, **params) -> Any:
        url = f"{self.BASE_URL}/{path.lstrip('/')}"

        last_error = None

        for attempt in range(self.max_retries + 1):
            self._respect_min_interval()

            try:

                r = self.session.get(url, params=params, timeout=self.timeout)
                now = time.time()
                self._last_request_ts = now
                self._usage_record(path, now=now)

                # 429 / 5xx -> retry med backoff
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    wait = self.backoff_base ** attempt
                    if attempt < self.max_retries:
                        time.sleep(wait)
                        continue

                r.raise_for_status()
                return r.json()

            except requests.RequestException as e:
                last_error = e

                if attempt < self.max_retries:
                    wait = self.backoff_base ** attempt
                    time.sleep(wait)
                    continue

        raise RuntimeError(f"FMP request failed: {path} params={params} error={last_error}") from last_error

    # Quotes
    def quote(self, symbol: str):
        data = self._get("quote", symbol=symbol)
        return data[0] if data else {}

    def quote_short(self, symbol: str):
        data = self._get("quote-short", symbol=symbol)
        return data[0] if data else {}

    def batch_quote(self, symbols: list[str]):
        if not symbols:
            return []
        return self._get("batch-quote", symbols=",".join(symbols))

    def batch_quote_short(self, symbols: list[str]):
        if not symbols:
            return []
        return self._get("batch-quote-short", symbols=",".join(symbols))

    def aftermarket_quote(self, symbol: str):
        data = self._get("aftermarket-quote", symbol=symbol)
        return data[0] if isinstance(data, list) and data else data

    def batch_aftermarket_quote(self, symbols: list[str]):
        if not symbols:
            return []
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

    # Financial statements / ratios / metrics
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