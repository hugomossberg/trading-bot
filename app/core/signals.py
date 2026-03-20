# app/core/signals.py

from app.core.analyzer import analyze_stock
from app.core.decision import decide_signal


def get_signal_analysis(stock_data):
    """
    Kör full analys på en aktie och lägger till färdig signal:
    'Köp', 'Sälj' eller 'Håll'.
    """
    analysis = analyze_stock(stock_data or {})

    if not isinstance(analysis, dict):
        analysis = {}

    analysis["signal"] = decide_signal(analysis)
    return analysis


def buy_or_sell(stock_data):
    """
    Returnerar endast signalen för en aktie.
    """
    analysis = get_signal_analysis(stock_data)
    return analysis.get("signal", "Håll")


def signal_to_side(signal):
    """
    Översätter intern svensk signal till IB-order-sida.
    """
    mapping = {
        "Köp": "BUY",
        "Sälj": "SELL",
    }
    return mapping.get(signal)


async def execute_order(ib_client, stock, signal, qty=10, bot=None, chat_id=None):
    """
    Skickar order via ib_client baserat på signal.

    Returnerar trade-objekt om order skickas, annars None.
    """
    side = signal_to_side(signal)
    if side is None:
        return None

    if not stock or not isinstance(stock, dict):
        raise ValueError("stock måste vara en dict")

    symbol = str(stock.get("symbol", "")).strip().upper()
    if not symbol:
        raise ValueError("stock saknar giltig symbol")

    if qty is None or int(qty) <= 0:
        raise ValueError("qty måste vara > 0")

    trade = await ib_client.place_order(
        symbol,
        side,
        int(qty),
        bot=bot,
        chat_id=chat_id,
    )
    return trade