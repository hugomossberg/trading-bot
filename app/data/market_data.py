from app.data.fmp_client import FMPClient


class MarketDataService:
    def __init__(self):
        self.fmp = FMPClient()

    def get_quote(self, symbol: str) -> dict:
        q = self.fmp.quote(symbol) or {}
        return {
            "symbol": q.get("symbol", symbol),
            "price": q.get("price"),
            "change": q.get("change"),
            "changePercent": q.get("changesPercentage") or q.get("changePercentage"),
            "volume": q.get("volume"),
            "dayLow": q.get("dayLow"),
            "dayHigh": q.get("dayHigh"),
            "yearHigh": q.get("yearHigh"),
            "yearLow": q.get("yearLow"),
            "marketCap": q.get("marketCap"),
            "avgVolume": q.get("avgVolume"),
            "open": q.get("open"),
            "previousClose": q.get("previousClose"),
        }

    def get_batch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        out = {}

        try:
            rows = self.fmp.batch_quote(symbols) or []
            for q in rows:
                sym = q.get("symbol")
                if not sym:
                    continue
                out[sym] = {
                    "symbol": sym,
                    "price": q.get("price"),
                    "change": q.get("change"),
                    "changePercent": q.get("changesPercentage") or q.get("changePercentage"),
                    "volume": q.get("volume"),
                    "marketCap": q.get("marketCap"),
                    "avgVolume": q.get("avgVolume"),
                }
            if out:
                return out
        except Exception:
            pass

        try:
            rows = self.fmp.batch_quote_short(symbols) or []
            for q in rows:
                sym = q.get("symbol")
                if not sym:
                    continue
                out[sym] = {
                    "symbol": sym,
                    "price": q.get("price"),
                    "change": q.get("change"),
                    "changePercent": q.get("changePercentage"),
                    "volume": q.get("volume"),
                    "marketCap": None,
                    "avgVolume": None,
                }
            if out:
                return out
        except Exception:
            pass

        for sym in symbols:
            try:
                q = self.get_quote(sym)
                if q:
                    out[sym] = q
            except Exception:
                continue

        return out

    def get_profile(self, symbol: str) -> dict:
        p = self.fmp.profile(symbol) or {}
        return {
            "symbol": symbol,
            "name": p.get("companyName"),
            "sector": p.get("sector"),
            "industry": p.get("industry"),
            "country": p.get("country"),
            "exchange": p.get("exchange"),
            "marketCap": p.get("marketCap"),
            "beta": p.get("beta"),
            "lastDividend": p.get("lastDividend"),
            "currency": p.get("currency"),
            "isEtf": p.get("isEtf"),
            "isActivelyTrading": p.get("isActivelyTrading"),
        }

    def get_fundamentals(self, symbol: str) -> dict:
        km = self.fmp.key_metrics_ttm(symbol) or {}
        rt = self.fmp.ratios_ttm(symbol) or {}
        return {
            "symbol": symbol,
            "pe": rt.get("priceToEarningsRatioTTM") or rt.get("priceEarningsRatioTTM") or rt.get("peRatioTTM"),
            "pb": rt.get("priceToBookRatioTTM"),
            "ps": rt.get("priceToSalesRatioTTM"),
            "roe": km.get("returnOnEquityTTM") or rt.get("returnOnEquityTTM"),
            "roa": km.get("returnOnAssetsTTM") or rt.get("returnOnAssetsTTM"),
            "currentRatio": rt.get("currentRatioTTM"),
            "debtToEquity": rt.get("debtToEquityRatioTTM") or rt.get("debtEquityRatioTTM"),
            "epsTTM": rt.get("netIncomePerShareTTM"),
            "fcfPerShareTTM": rt.get("freeCashFlowPerShareTTM") or km.get("freeCashFlowPerShareTTM"),
            "marketCap": km.get("marketCapTTM") or km.get("marketCap"),
            "dividendYieldTTM": rt.get("dividendYieldTTM"),
            "dividendPerShareTTM": rt.get("dividendPerShareTTM"),
        }
   
    def screen_stocks(self, **filters) -> list[dict]:
        return self.fmp.screener(**filters) or []
    
    def get_financials(self, symbol: str) -> dict:
        income = self.fmp.income_statement(symbol, period="annual", limit=2) or []
        ratios = self.fmp.ratios(symbol) or {}
        key = self.fmp.key_metrics(symbol) or {}
        growth = self.fmp.financial_growth(symbol) or {}
        scores = self.fmp.financial_scores(symbol) or {}

        revenue_growth = None
        if len(income) >= 2:
            try:
                rev_curr = float(income[0]["revenue"])
                rev_prev = float(income[1]["revenue"])
                if rev_prev:
                    revenue_growth = 100 * (rev_curr - rev_prev) / rev_prev
            except Exception:
                pass

        raw_growth = growth.get("revenueGrowth")
        if raw_growth is not None:
            try:
                raw_growth = float(raw_growth)
                if -1 < raw_growth < 1:
                    raw_growth *= 100
            except Exception:
                raw_growth = None

        return {
            "revenueGrowth": raw_growth if raw_growth is not None else revenue_growth,
            "profitMargin": (
                ratios.get("netProfitMargin")
                or ratios.get("netProfitMarginRatio")
            ),
            "grossMargin": (
                ratios.get("grossProfitMargin")
                or ratios.get("grossProfitMarginRatio")
            ),
            "debtToEquity": (
                ratios.get("debtToEquity")
                or ratios.get("debtEquityRatio")
                or ratios.get("debtToEquityRatio")
            ),
            "currentRatio": ratios.get("currentRatio"),
            "roe": (
                key.get("returnOnEquity")
                or ratios.get("returnOnEquity")
            ),
            "roa": (
                key.get("returnOnAssets")
                or ratios.get("returnOnAssets")
            ),
            "freeCashFlowPerShare": (
                key.get("freeCashFlowPerShare")
                or key.get("freeCashFlowPerShareTTM")
            ),
            "altmanZ": (
                scores.get("altmanZScore")
                or scores.get("altmanZ")
            ),
            "piotroski": (
                scores.get("piotroskiScore")
                or scores.get("piotroski")
            ),
        }