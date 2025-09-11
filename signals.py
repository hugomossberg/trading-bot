from textblob import TextBlob
import json
import asyncio

def analyze_news_sentiment(news_articles):
    total_sentiment = 0
    for article in news_articles:
        blob = TextBlob(article)
        total_sentiment += blob.sentiment.polarity
    return total_sentiment / len(news_articles) if news_articles else 0

def evaluate_annual_report(annual_data):
    score = 0
    revenue_growth = annual_data.get("revenueGrowth")
    if revenue_growth is not None:
        score += 1 if revenue_growth > 5 else -1
    profit_margin = annual_data.get("profitMargin")
    if profit_margin is not None:
        score += 1 if profit_margin > 10 else -1
    debt_to_equity = annual_data.get("debtToEquity")
    if debt_to_equity is not None:
        if debt_to_equity < 0.5:
            score += 1
        elif debt_to_equity > 1.5:
            score -= 1
    return score

def buy_or_sell(stock_data):
    score = 0
    pe = stock_data.get("PE")
    if pe is not None:
        if pe < 15:
            score += 1
        elif pe > 25:
            score -= 1
    eps = stock_data.get("trailingEps")
    if eps is not None:
        score += 1 if eps > 0 else -1
    dividend = stock_data.get("dividendYield")
    if dividend is not None:
        if dividend < 1:
            dividend *= 100
        if dividend >= 4:
            score += 1
    beta = stock_data.get("beta")
    if beta is not None:
        if beta == 1:
            score += 2
        elif beta < 1:
            score += 1
        elif beta > 1:
            score -= 1
    news_summaries = [news["content"].get("summary", "") for news in stock_data.get("News", [])]
    news_sentiment = analyze_news_sentiment(news_summaries)
    if news_sentiment > 0.1:
        score += 1
    elif news_sentiment < -0.1:
        score -= 1
    annual_data = stock_data.get("quarterlyFinance", {})
    annual_score = evaluate_annual_report(annual_data)
    score += annual_score
    if score >= 2:
        return "Köp"
    elif score <= -2:
        return "Sälj"
    else:
        return "Håll"

# signals.py
async def execute_order(ib_client, stock, signal, qty=10, bot=None, chat_id=None):
    if signal == "Köp":
        side = "BUY"
    elif signal == "Sälj":
        side = "SELL"
    else:
        return None

    trade = await ib_client.place_order(
        stock["symbol"], side, qty,
        bot=bot,
        chat_id=chat_id,
    )
    return trade



