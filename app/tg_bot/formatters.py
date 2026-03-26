from datetime import datetime
from zoneinfo import ZoneInfo

SE_TZ = ZoneInfo("Europe/Stockholm")
US_TZ = ZoneInfo("America/New_York")


def fmt_qty(qty: float) -> str:
    return str(int(qty)) if float(qty).is_integer() else f"{qty:.2f}"


def fmt_price(value) -> str:
    if value in (None, "", 0):
        return "-"
    try:
        return f"{float(value):,.2f}"
    except Exception:
        return str(value)


def fmt_number(value) -> str:
    if value in (None, "", 0):
        return "-"
    try:
        return f"{float(value):,.2f}"
    except Exception:
        return str(value)


def fmt_market_cap(value) -> str:
    if value in (None, "", 0):
        return "-"
    try:
        value = float(value)
        if value >= 1_000_000_000_000:
            return f"{value / 1_000_000_000_000:.2f}T"
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        if value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        return f"{value:.0f}"
    except Exception:
        return str(value)


def fmt_signal_block(signal: str, total_score: int | float, scores: dict) -> str:
    return (
        f"Signal: {signal}\n"
        f"Score: {total_score}\n\n"
        f"Breakdown\n"
        f"Fundamentals: {scores.get('fundamentals', 0)}\n"
        f"Financials: {scores.get('financials', 0)}\n"
        f"News: {scores.get('news', 0)}"
    )


def format_stock_brief(ticker: str, stock: dict, analysis: dict, summary: str | None = None) -> str:
    latest_close = stock.get("latestClose")
    pe = stock.get("PE")
    market_cap = stock.get("marketCap")
    beta = stock.get("beta")
    sector = stock.get("sector")

    signal = analysis.get("signal", "Hold")
    total_score = analysis.get("total_score", 0)
    scores = analysis.get("scores", {})

    msg = (
        f"{ticker}\n\n"
        f"Price: {fmt_price(latest_close)}\n"
        f"{fmt_signal_block(signal, total_score, scores)}\n\n"
        f"Quick view\n"
        f"Sector: {sector or '-'}\n"
        f"PE: {fmt_number(pe)}\n"
        f"MCap: {fmt_market_cap(market_cap)}\n"
        f"Beta: {fmt_number(beta)}"
    )

    if summary and summary.strip() and summary.strip() != "(ingen summering)":
        msg += f"\n\nSummary\n{summary.strip()}"

    return msg


def format_portfolio(positions: list[dict]) -> str:
    if not positions:
        return "PORTFOLIO\n\nNo open positions."

    lines = ["PORTFOLIO", "", f"{len(positions)} positions"]
    for p in positions:
        lines.append(f"{p['symbol']:<6} {p['qty']:>8} @ {p['avg']}")
    return "\n".join(lines)


def format_orders(orders: list[dict]) -> str:
    if not orders:
        return "OPEN ORDERS\n\nNone."

    lines = ["OPEN ORDERS", ""]
    for o in orders:
        lines.append(
            f"{o['symbol']:<6} {o['side']:<4} {o['filled']}/{o['qty']} {o['status']} {o['session']}"
        )
    return "\n".join(lines)


def format_status(
    connected: bool,
    now_se: datetime,
    now_et: datetime,
    market_open: bool,
    positions: list[dict],
    orders: list[dict],
) -> str:
    pos_count = len(positions)
    pos_preview = positions[:5]
    order_preview = orders[:5]

    lines = [
        "BOT STATUS",
        "",
        f"IB: {'connected' if connected else 'disconnected'}",
        f"Time: SE {now_se:%Y-%m-%d %H:%M} | ET {now_et:%H:%M}",
        f"US market: {'open' if market_open else 'closed'}",
        "",
        "Portfolio",
        f"{pos_count} positions",
    ]

    if pos_preview:
        for p in pos_preview:
            lines.append(f"{p['symbol']:<6} {p['qty']:>8} @ {p['avg']}")
        extra = len(positions) - len(pos_preview)
        if extra > 0:
            lines.append(f"... +{extra} more")
    else:
        lines.append("None")

    lines += ["", "Open orders"]

    if order_preview:
        for o in order_preview:
            lines.append(
                f"{o['symbol']:<6} {o['side']:<4} {o['filled']}/{o['qty']} {o['status']} {o['session']}"
            )
        extra = len(orders) - len(order_preview)
        if extra > 0:
            lines.append(f"... +{extra} more")
    else:
        lines.append("None")

    return "\n".join(lines)


def format_tickers(watch_syms: list[str], owned_syms: list[str], updated: str) -> str:
    def compact(items: list[str], head: int = 12) -> str:
        if not items:
            return "None"
        shown = items[:head]
        txt = " · ".join(shown)
        extra = len(items) - len(shown)
        if extra > 0:
            txt += f"\n... +{extra} more"
        return txt

    return (
        "TICKERS\n\n"
        f"Watch universe\n"
        f"{len(watch_syms)} symbols\n"
        f"{compact(watch_syms)}\n\n"
        f"Owned\n"
        f"{len(owned_syms)} symbols\n"
        f"{compact(owned_syms)}\n\n"
        f"Stock info updated\n"
        f"{updated}"
    )


def format_help() -> str:
    return (
        "COMMANDS\n\n"
        "Overview\n"
        "status       full bot overview\n"
        "portfolio    open positions only\n"
        "orders       open orders only\n"
        "tickers / t  watch universe + owned\n\n"
        "Stock lookup\n"
        "NVDA         quick stock view\n"
        "ticker NVDA  same as above\n\n"
        "Actions\n"
        "sellall\n"
        "sell NVDA\n"
        "sell NVDA 10"
    )