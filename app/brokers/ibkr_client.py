#ibkr_client.py
from ib_insync import IB, Stock, MarketOrder, LimitOrder, ScannerSubscription
import asyncio
import math
import os

from app.config import TWS_PORT
from app.core.helpers import order_outside_rth_allowed
from app.data.market_data import MarketDataService


# ===== Terminalstil =====
_USE_COLOR = os.getenv("NO_COLOR", "").strip().lower() not in {"1", "true", "yes", "on"}

_RESET = "\033[0m" if _USE_COLOR else ""
_BOLD = "\033[1m" if _USE_COLOR else ""
_DIM = "\033[2m" if _USE_COLOR else ""

_RED = "\033[91m" if _USE_COLOR else ""
_GREEN = "\033[92m" if _USE_COLOR else ""
_YELLOW = "\033[93m" if _USE_COLOR else ""
_BLUE = "\033[94m" if _USE_COLOR else ""
_MAGENTA = "\033[95m" if _USE_COLOR else ""
_CYAN = "\033[96m" if _USE_COLOR else ""


def _c(text, color):
    return f"{color}{text}{_RESET}" if _USE_COLOR else str(text)


def _side_color(side: str):
    return _GREEN if str(side).upper() == "BUY" else _RED


def _to_num(value):
    try:
        value = float(value)
    except Exception:
        return None

    if not math.isfinite(value):
        return None

    if value <= 0:
        return None

    return value


def _fmt_price(value):
    v = _to_num(value)
    if v is None:
        return "-"
    return f"{v:.2f}"


def _fmt_qty(value):
    try:
        return str(int(value))
    except Exception:
        return str(value)


class IbClient:
    def __init__(self):
        self.ib = IB()
        self.market_data = MarketDataService()

    async def connect(self):
        if not self.ib.isConnected():
            try:
                await self.ib.connectAsync("127.0.0.1", TWS_PORT, clientId=1, timeout=30)
                print(f"{_c('● CONNECT', _CYAN)} {_c(f'API connected on {TWS_PORT}', _BOLD)}")
            except Exception as e:
                print(f"{_c('● CONNECT ERROR', _RED)} {e}")
        else:
            print(f"{_c('● CONNECT', _YELLOW)} IBKR already connected")

    

    def _get_fmp_quote(self, symbol: str):
        q = self.market_data.get_quote(symbol) or {}

        price = _to_num(q.get("price"))
        previous_close = _to_num(q.get("previousClose"))

        ref_price = price or previous_close
        if ref_price is None:
            return None

        return {
            "symbol": symbol,
            "bid": None,
            "ask": None,
            "last": ref_price,
            "market": ref_price,
            "close": previous_close or ref_price,
            "mid": ref_price,
            "spread": None,
            "spread_pct": None,
            "source": "fmp",
        }


    async def _get_reference_price(self, contract):
        price = None
        ticker = None

        try:
            try:
                self.ib.reqMarketDataType(1)  # live
            except Exception:
                pass

            ticker = self.ib.reqMktData(contract, "", False, False)
            await asyncio.sleep(1.5)

            price = (
                _to_num(ticker.last)
                or _to_num(ticker.marketPrice())
                or _to_num(ticker.close)
                or _to_num(ticker.bid)
                or _to_num(ticker.ask)
            )

            if price is not None:
                return price

            try:
                if ticker is not None:
                    self.ib.cancelMktData(contract)
            except Exception:
                pass

            try:
                self.ib.reqMarketDataType(3)  # delayed
            except Exception:
                pass

            ticker = self.ib.reqMktData(contract, "", False, False)
            await asyncio.sleep(1.5)

            price = (
                _to_num(ticker.last)
                or _to_num(ticker.marketPrice())
                or _to_num(ticker.close)
                or _to_num(ticker.bid)
                or _to_num(ticker.ask)
            )

            return price

        except Exception:
            return None
        finally:
            try:
                if ticker is not None:
                    self.ib.cancelMktData(contract)
            except Exception:
                pass

    async def get_live_quote(self, symbol: str, wait_sec: float = 1.2):
        contract = Stock(symbol, "SMART", "USD")

        try:
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                return None
            contract = qualified[0]
        except Exception:
            return None

        async def _read_quote(market_data_type: int):
            ticker = None
            try:
                try:
                    self.ib.reqMarketDataType(market_data_type)
                except Exception:
                    pass

                ticker = self.ib.reqMktData(contract, "", False, False)
                await asyncio.sleep(wait_sec)

                bid = _to_num(ticker.bid)
                ask = _to_num(ticker.ask)
                last = _to_num(ticker.last)
                market = _to_num(ticker.marketPrice())
                close = _to_num(ticker.close)

                mid = None
                spread = None
                spread_pct = None

                if bid is not None and ask is not None and ask >= bid:
                    mid = round((bid + ask) / 2, 4)
                    spread = round(ask - bid, 4)
                    if mid > 0:
                        spread_pct = round((spread / mid) * 100.0, 4)

                if any(v is not None for v in [bid, ask, last, market, close, mid]):
                    return {
                        "symbol": symbol,
                        "bid": bid,
                        "ask": ask,
                        "last": last,
                        "market": market,
                        "close": close,
                        "mid": mid,
                        "spread": spread,
                        "spread_pct": spread_pct,
                    }

                return None

            except Exception:
                return None
            finally:
                try:
                    if ticker is not None:
                        self.ib.cancelMktData(contract)
                except Exception:
                    pass

        quote = await _read_quote(1)  # live
        if quote:
            return quote

        quote = await _read_quote(3)  # delayed
        if quote:
            return quote

        return None

    async def place_order(self, symbol, side, qty, bot=None, chat_id=None, quote=None):
        contract = Stock(symbol, "SMART", "USD")
        side_up = side.upper()
        side_col = _side_color(side_up)

        try:
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                print(f"{_c('● ORDER ERROR', _RED)} Could not qualify contract for {_c(symbol, _BOLD)}")
                return None
            contract = qualified[0]
        except Exception as e:
            print(f"{_c('● ORDER ERROR', _RED)} qualifyContracts failed for {_c(symbol, _BOLD)}: {e}")
            return None

        quote = quote or self._get_fmp_quote(symbol)
        ref_price = None

        if quote:
            if side_up == "BUY":
                ref_price = (
                    quote.get("ask")
                    or quote.get("last")
                    or quote.get("mid")
                    or quote.get("market")
                    or quote.get("close")
                )
            else:
                ref_price = (
                    quote.get("bid")
                    or quote.get("last")
                    or quote.get("mid")
                    or quote.get("market")
                    or quote.get("close")
                )

        ref_price = _to_num(ref_price)

        use_limit_orders = os.getenv("USE_LIMIT_ORDERS", "1").strip().lower() in {
            "1", "true", "yes", "on"
        }

        if order_outside_rth_allowed():
            use_limit_orders = True

        if use_limit_orders:
            if ref_price is None:
                print(
                    f"{_c('● ORDER SKIP', _YELLOW)} "
                    f"{_c(symbol, _BOLD)} no valid quote/reference price for limit order"
                )
                return None

            buy_buffer = float(os.getenv("BUY_LIMIT_BUFFER_PCT", "0.002"))
            sell_buffer = float(os.getenv("SELL_LIMIT_BUFFER_PCT", "0.002"))

            if side_up == "BUY":
                limit_price = round(ref_price * (1 + buy_buffer), 2)
            else:
                limit_price = round(ref_price * (1 - sell_buffer), 2)

            limit_price = _to_num(limit_price)
            if limit_price is None:
                print(
                    f"{_c('● ORDER SKIP', _YELLOW)} "
                    f"{_c(symbol, _BOLD)} invalid limit price"
                )
                return None

            order = LimitOrder(side_up, qty, limit_price)

            print()
            print(_c("═" * 72, side_col))
            print(
                f"{_c('ORDER', _BOLD)}  "
                f"{_c(side_up, side_col)}  "
                f"{_c(symbol, _BOLD)}  "
                f"qty={_c(_fmt_qty(qty), _BOLD)}  "
                f"type={_c('LIMIT', _CYAN)}  "
                f"limit={_c(_fmt_price(limit_price), _BOLD)}  "
                f"{_DIM}ref={_fmt_price(ref_price)}{_RESET}"
            )
            print(_c("═" * 72, side_col))
        else:
            order = MarketOrder(side_up, qty)

            print()
            print(_c("═" * 72, side_col))
            print(
                f"{_c('ORDER', _BOLD)}  "
                f"{_c(side_up, side_col)}  "
                f"{_c(symbol, _BOLD)}  "
                f"qty={_c(_fmt_qty(qty), _BOLD)}  "
                f"type={_c('MARKET', _MAGENTA)}"
            )
            print(_c("═" * 72, side_col))

        allow_outside_rth = order_outside_rth_allowed()
        order_tif = os.getenv("ORDER_TIF", "DAY").strip().upper() or "DAY"

        try:
            order.outsideRth = allow_outside_rth
        except Exception:
            pass

        try:
            order.tif = order_tif
        except Exception:
            pass

        trade = self.ib.placeOrder(contract, order)

        def on_status(trade_):
            status = trade_.orderStatus.status
            filled = trade_.orderStatus.filled
            remaining = trade_.orderStatus.remaining
            avg_fill = trade_.orderStatus.avgFillPrice

            status_color = _YELLOW
            if str(status).lower() in {"filled"}:
                status_color = _GREEN
            elif str(status).lower() in {"cancelled", "inactive", "api cancelled"}:
                status_color = _RED
            elif str(status).lower() in {"submitted", "presubmitted", "pendingsubmit"}:
                status_color = _CYAN

            print(
                f"{_c('STATUS', _BLUE)}  "
                f"{_c(symbol, _BOLD)}  "
                f"{_c(status, status_color)}  "
                f"{_DIM}|{_RESET} filled={_fmt_qty(filled)}  "
                f"remaining={_fmt_qty(remaining)}  "
                f"avg={_fmt_price(avg_fill)}"
            )

        def on_filled(trade_):
            avg_fill = trade_.orderStatus.avgFillPrice
            print(
                f"{_c('✓ FILLED', _GREEN)}  "
                f"{_c(symbol, _BOLD)}  "
                f"{_c(side_up, side_col)}  "
                f"qty={_c(_fmt_qty(qty), _BOLD)}  "
                f"avg={_c(_fmt_price(avg_fill), _BOLD)}"
            )
            print(_c("─" * 72, _GREEN))

        def on_fill(trade_, fill):
            execu = fill.execution
            print(
                f"{_c('FILL', _GREEN)}  "
                f"{_c(symbol, _BOLD)}  "
                f"{_c(execu.side, _side_color(execu.side))}  "
                f"shares={_fmt_qty(execu.shares)}  "
                f"price={_fmt_price(execu.price)}"
            )

        def _detach(trade_=None):
            try:
                trade.statusEvent -= on_status
            except Exception:
                pass
            try:
                trade.filledEvent -= on_filled
            except Exception:
                pass
            try:
                trade.fillEvent -= on_fill
            except Exception:
                pass
            try:
                trade.filledEvent -= _detach
            except Exception:
                pass
            try:
                trade.cancelledEvent -= _detach
            except Exception:
                pass

        trade.statusEvent += on_status
        trade.filledEvent += on_filled
        trade.fillEvent += on_fill
        trade.filledEvent += _detach
        trade.cancelledEvent += _detach

        await asyncio.sleep(0.75)

        status = (trade.orderStatus.status or "").lower()
        if status in {"cancelled", "inactive"}:
            print(
                f"{_c('● ORDER CANCELLED', _RED)} "
                f"{_c(trade.contract.symbol, _BOLD)} "
                f"status={trade.orderStatus.status}"
            )
            _detach(trade)
            return None

        return trade

    async def get_stocks(
        self,
        rows: int | None = None,
        instrument: str | None = None,
        locationCode: str | None = None,
        scanCode: str | None = None,
    ):
        rows = rows or int(os.getenv("UNIVERSE_ROWS", "30"))
        instrument = instrument or os.getenv("SCANNER_INSTRUMENT", "STK")
        locationCode = locationCode or os.getenv("SCANNER_LOCATION", "STK.NASDAQ")
        scanCode = scanCode or os.getenv("SCANNER_CODE", "MOST_ACTIVE")

        await asyncio.sleep(0.5)

        sub = ScannerSubscription(
            instrument=instrument,
            locationCode=locationCode,
            scanCode=scanCode,
            numberOfRows=rows,
        )
        data = await self.ib.reqScannerDataAsync(sub)
        if not data:
            print(f"{_c('● SCANNER', _YELLOW)} No scanner data returned")
            return []

        seen, tickers = set(), []
        for d in data:
            sym = d.contractDetails.contract.symbol
            if sym not in seen:
                seen.add(sym)
                tickers.append(sym)

        print(f"{_c('● SCANNER', _CYAN)} Fetched {len(tickers)} symbols: {tickers}")
        return tickers

    async def scanner_parameters(self):
        scanner_xml = self.ib.reqScannerParameters()
        with open("scanner_parameters.xml", "w", encoding="utf-8") as f:
            f.write(scanner_xml)
        print(f"{_c('● SCANNER', _CYAN)} Scanner parameters saved")

    async def disconnect_ibkr(self):
        await asyncio.sleep(2)
        self.ib.disconnect()
        print(f"{_c('● DISCONNECT', _YELLOW)} API disconnected")


ib_client = IbClient()