#ibkr_client.py
from ib_insync import IB, Stock, MarketOrder, LimitOrder, ScannerSubscription
import asyncio
import os


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
        if value <= 0:
            return None
        return value
    except Exception:
        return None


def _fmt_price(value):
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "-"


def _fmt_qty(value):
    try:
        return str(int(value))
    except Exception:
        return str(value)




class IbClient:
    def __init__(self):
        self.ib = IB()

    # Port 4002 för paper, 4001 för live
    async def connect(self):
        if not self.ib.isConnected():
            try:
                await self.ib.connectAsync("127.0.0.1", 4002, clientId=1, timeout=30)
                print(f"{_c('● CONNECT', _CYAN)} {_c('API connected on 4002', _BOLD)}")
            except Exception as e:
                print(f"{_c('● CONNECT ERROR', _RED)} {e}")
        else:
            print(f"{_c('● CONNECT', _YELLOW)} IBKR already connected")

    async def _get_reference_price(self, contract):
        price = None

        try:
            ticker = self.ib.reqMktData(contract, "", False, False)
            await asyncio.sleep(1.5)

            price = (
                ticker.last
                or ticker.marketPrice()
                or ticker.close
                or ticker.bid
                or ticker.ask
            )
        except Exception:
            price = None
        finally:
            try:
                self.ib.cancelMktData(contract)
            except Exception:
                pass

        try:
            if price is not None:
                price = float(price)
                if price > 0:
                    return price
        except Exception:
            pass

        return None

    
    async def get_live_quote(self, symbol: str, wait_sec: float = 1.2):
        contract = Stock(symbol, "SMART", "USD")

        try:
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                return None
            contract = qualified[0]
        except Exception:
            return None

        ticker = None
        try:
            # 1 = live om tillgängligt
            try:
                self.ib.reqMarketDataType(1)
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
                if mid and mid > 0:
                    spread_pct = round((spread / mid) * 100.0, 4)

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

        except Exception:
            return None
        finally:
            try:
                if ticker is not None:
                    self.ib.cancelMktData(contract)
            except Exception:
                pass

    async def place_order(self, symbol, side, qty, bot=None, chat_id=None):
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

        quote = await self.get_live_quote(symbol)
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

        use_limit_orders = os.getenv("USE_LIMIT_ORDERS", "1").strip().lower() in {
            "1", "true", "yes", "on"
        }

        if use_limit_orders and ref_price:
            buy_buffer = float(os.getenv("BUY_LIMIT_BUFFER_PCT", "0.002"))
            sell_buffer = float(os.getenv("SELL_LIMIT_BUFFER_PCT", "0.002"))

            if side_up == "BUY":
                limit_price = round(ref_price * (1 + buy_buffer), 2)
            else:
                limit_price = round(ref_price * (1 - sell_buffer), 2)

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
        allow_ext_hours = os.getenv("ALLOW_EXTENDED_HOURS", "0").strip().lower() in {
            "1", "true", "yes", "on"
        }

        order_tif = os.getenv("ORDER_TIF", "DAY").strip().upper() or "DAY"

        try:
            order.outsideRth = allow_ext_hours
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


# Global instans att återanvända
ib_client = IbClient()