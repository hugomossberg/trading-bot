from textblob import TextBlob


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", ".")
            if value.lower() in {"inf", "infinity", "nan", ""}:
                return default
        return float(value)
    except Exception:
        return default


def analyze_news_sentiment(news_articles):
    if not news_articles:
        return 0.0

    total_sentiment = 0.0
    count = 0

    for article in news_articles:
        if not article:
            continue
        try:
            blob = TextBlob(str(article))
            total_sentiment += blob.sentiment.polarity
            count += 1
        except Exception:
            continue

    if count == 0:
        return 0.0

    return total_sentiment / count


def get_news_summaries(stock_data):
    summaries = []
    for news in stock_data.get("News", []):
        summary = news.get("content", {}).get("summary", "")
        if summary:
            summaries.append(summary)
    return summaries


# =========================
# FUNDAMENTALS
# =========================

def score_pe(stock_data):
    pe = _safe_float(stock_data.get("PE"))
    if pe is None or pe <= 0:
        return 0

    if pe < 10:
        return 2
    if pe < 18:
        return 1
    if pe <= 25:
        return 0
    if pe <= 35:
        return -1
    return -2


def score_eps(stock_data):
    eps = _safe_float(stock_data.get("trailingEps"))
    if eps is None:
        return 0

    if eps > 5:
        return 2
    if eps > 0:
        return 1
    return -2


def score_dividend(stock_data):
    dividend = _safe_float(stock_data.get("dividendYield"))
    if dividend is None:
        return 0

    # vissa API:er ger 0.04, andra 4.0
    if dividend < 1:
        dividend *= 100

    if dividend >= 6:
        return 2
    if dividend >= 3:
        return 1
    return 0


def score_beta(stock_data):
    beta = _safe_float(stock_data.get("beta"))
    if beta is None or beta <= 0:
        return 0

    if beta < 0.8:
        return 1
    if beta <= 1.5:
        return 0
    if beta <= 2.0:
        return -1
    return -2


# =========================
# FINANCIALS
# =========================

def score_revenue_growth(finance_data):
    growth = _safe_float(finance_data.get("revenueGrowth"))
    if growth is None:
        return 0

    if growth >= 20:
        return 2
    if growth >= 8:
        return 1
    if growth >= 0:
        return 0
    if growth >= -10:
        return -1
    return -2


def score_profit_margin(finance_data):
    margin = _safe_float(finance_data.get("profitMargin"))
    if margin is None:
        return 0

    if margin >= 20:
        return 2
    if margin >= 10:
        return 1
    if margin >= 0:
        return 0
    return -2


def score_debt_to_equity(finance_data):
    debt_to_equity = _safe_float(finance_data.get("debtToEquity"))
    if debt_to_equity is None:
        return 0

    if debt_to_equity < 0.5:
        return 2
    if debt_to_equity < 1.0:
        return 1
    if debt_to_equity <= 1.5:
        return 0
    if debt_to_equity <= 2.5:
        return -1
    return -2


# =========================
# NEWS / SENTIMENT
# =========================

def score_news(stock_data):
    news_summaries = get_news_summaries(stock_data)
    sentiment = analyze_news_sentiment(news_summaries)

    if sentiment > 0.25:
        return 1, sentiment
    if sentiment < -0.25:
        return -1, sentiment
    return 0, sentiment


# =========================
# TECHNICALS
# =========================

def score_price_trend(technicals):
    price = _safe_float(technicals.get("price"))
    sma20 = _safe_float(technicals.get("sma20"))
    sma50 = _safe_float(technicals.get("sma50"))

    if price is None or sma20 is None or sma50 is None:
        return 0

    if price > sma20 > sma50:
        return 2
    if price > sma20 and price > sma50:
        return 1
    if price < sma20 < sma50:
        return -2
    if price < sma20 and price < sma50:
        return -1
    return 0


def score_rsi(technicals):
    rsi = _safe_float(technicals.get("rsi14"))
    if rsi is None:
        return 0

    if 50 <= rsi <= 65:
        return 1
    if 65 < rsi <= 75:
        return 0
    if rsi > 75:
        return -1
    if rsi < 30:
        return -1
    return 0


def score_volume_spike(technicals):
    volume_ratio = _safe_float(technicals.get("volume_ratio"))
    if volume_ratio is None:
        return 0

    if volume_ratio >= 2.0:
        return 2
    if volume_ratio >= 1.3:
        return 1
    return 0


def score_volatility(technicals):
    atr_pct = _safe_float(technicals.get("atr_pct"))
    if atr_pct is None:
        return 0

    if atr_pct < 2:
        return 1
    if atr_pct > 9:
        return -2
    if atr_pct > 6:
        return -1
    return 0


def score_momentum(technicals):
    mom20 = _safe_float(technicals.get("momentum_20"))
    mom60 = _safe_float(technicals.get("momentum_60"))

    score = 0

    if mom20 is not None:
        if mom20 > 8:
            score += 1
        elif mom20 < -8:
            score -= 1

    if mom60 is not None:
        if mom60 > 15:
            score += 1
        elif mom60 < -15:
            score -= 1

    return score


def score_liquidity(technicals):
    adv = _safe_float(technicals.get("avg_dollar_volume_20"))
    if adv is None:
        return 0

    if adv >= 50_000_000:
        return 2
    if adv >= 15_000_000:
        return 1
    if adv < 2_000_000:
        return -2
    if adv < 5_000_000:
        return -1
    return 0