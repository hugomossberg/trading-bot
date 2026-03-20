from ib_insync import IB, ScannerSubscription, Stock, MarketOrder
import asyncio
import os


class IbClient:
    def __init__(self):
        self.ib = IB()

    # Port 4002 för paper, 4001 för live
    async def connect(self):
        if not self.ib.isConnected():
            try:
                await self.ib.connectAsync("127.0.0.1", 4002, clientId=1, timeout=30)
                print("✅ API Connected on 4002!")
            except Exception as e:
                print(f"❌ API connection failed: {e}")
        else:
            print("ℹ️ IBKR redan ansluten")

    async def place_order(self, symbol, side, qty, bot=None, chat_id=None):
        """
        side: 'BUY' eller 'SELL'
        qty: heltal antal
        bot/chat_id: valfria, för att skicka status till Telegram
        """
        symbol = (symbol or "").upper().strip()

        # IB gillar BRK B bättre än BRK-B
        if symbol == "BRK-B":
            symbol = "BRK B"

        contract = Stock(symbol, "SMART", "USD")

        # Validera kontrakt innan order skickas
        try:
            qualified = await self.ib.qualifyContractsAsync(contract)
        except Exception as e:
            print(f"❌ Kunde inte kvalificera kontrakt för {symbol}: {e}")
            return None

        if not qualified:
            print(f"❌ Ingen giltig contract hittades för {symbol}")
            return None

        contract = qualified[0]
        order = MarketOrder(side, qty)

        try:
            trade = await asyncio.to_thread(self.ib.placeOrder, contract, order)
        except Exception as e:
            print(f"❌ Kunde inte skicka order för {symbol}: {e}")
            return None

        print(f"📨 Order skickad: {trade}")

        def _send_msg(msg: str):
            print(msg)
            if bot and chat_id:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(bot.send_message(chat_id=chat_id, text=msg))
                except Exception as e:
                    print(f"[telegram] fel: {e}")

        def on_status(trade_):
            try:
                status = trade_.orderStatus.status or ""
                filled = trade_.orderStatus.filled or 0
                total = getattr(trade_.order, "totalQuantity", 0) or 0
                msg = f"📊 Orderstatus {trade_.contract.symbol}: {status}, fylld {filled}/{total}"
                _send_msg(msg)
            except Exception as e:
                print(f"[on_status] fel: {e}")

        def on_filled(trade_):
            try:
                avg = float(trade_.orderStatus.avgFillPrice or 0.0)
                filled = float(trade_.orderStatus.filled or 0.0)
                total = float(getattr(trade_.order, "totalQuantity", 0) or 0)
                sym = trade_.contract.symbol
                msg = f"✅ {sym}: Filled {filled}/{total} @ {avg:.2f}"
                _send_msg(msg)
            except Exception as e:
                print(f"[on_filled] fel: {e}")

        def on_fill(trade_, fill):
            try:
                sym = trade_.contract.symbol
                px = float(getattr(fill.execution, "price", 0.0) or 0.0)
                qty_ = float(getattr(fill.execution, "shares", 0.0) or 0.0)
                cum = float(trade_.orderStatus.filled or 0.0)
                total = float(getattr(trade_.order, "totalQuantity", 0) or 0)
                msg = f"🧾 Fill {sym}: {qty_} @ {px} (cum {cum}/{total})"
                _send_msg(msg)
            except Exception as e:
                print(f"[on_fill] fel: {e}")

        def _detach(trade_):
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
                trade.cancelledEvent -= _detach
            except Exception:
                pass
            try:
                trade.filledEvent -= _detach
            except Exception:
                pass

        trade.statusEvent += on_status
        trade.filledEvent += on_filled
        trade.fillEvent += on_fill
        trade.filledEvent += _detach
        trade.cancelledEvent += _detach

        # Vänta kort så vi hinner få direkt cancel på ogiltiga orders
        await asyncio.sleep(0.5)

        status = (trade.orderStatus.status or "").lower()
        if status in {"cancelled", "inactive"}:
            print(f"❌ Order avbruten direkt för {trade.contract.symbol}: {trade.orderStatus.status}")
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
        # Läs defaults från .env om ej skickat in
        rows = rows or int(os.getenv("UNIVERSE_ROWS", "30"))
        instrument = instrument or os.getenv("SCANNER_INSTRUMENT", "STK")
        locationCode = locationCode or os.getenv("SCANNER_LOCATION", "STK.NASDAQ")
        scanCode = scanCode or os.getenv("SCANNER_CODE", "MOST_ACTIVE")

        # liten paus så TWS hinner andas
        await asyncio.sleep(0.5)

        sub = ScannerSubscription(
            instrument=instrument,
            locationCode=locationCode,
            scanCode=scanCode,
            numberOfRows=rows,
        )
        data = await self.ib.reqScannerDataAsync(sub)
        if not data:
            print("⚠️ Ingen scanner-data returnerad.")
            return []

        # Plocka symboler (unika, behåll ordning)
        seen, tickers = set(), []
        for d in data:
            sym = d.contractDetails.contract.symbol
            if sym not in seen:
                seen.add(sym)
                tickers.append(sym)

        print(f"✅ Hämtade {len(tickers)} aktier: {tickers}")
        return tickers

    async def scanner_parameters(self):
        scanner_xml = self.ib.reqScannerParameters()
        with open("scanner_parameters.xml", "w", encoding="utf-8") as f:
            f.write(scanner_xml)
        print("✅ Scanner parameters saved!")

    async def disconnect_ibkr(self):
        await asyncio.sleep(2)
        self.ib.disconnect()
        print("❌ API Disconnected!")


# --- Global instans att återanvända ---
ib_client = IbClient()