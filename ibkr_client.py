# ibkr_client.py
from ib_insync import IB, ScannerSubscription, Stock, MarketOrder
import asyncio

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
        qty:  heltal antal
        bot/chat_id: valfria, för att skicka status till Telegram
        """
        contract = Stock(symbol, "SMART", "USD")
        order = MarketOrder(side, qty)

        # Kör synkron ib.placeOrder i tråd
        trade = await asyncio.to_thread(self.ib.placeOrder, contract, order)
        print(f"📨 Order skickad: {trade}")

        # Callback för status/fill
        # ibkr_client.py (i place_order)
        def on_status(trade):
            try:
                status = trade.orderStatus.status or ""
                filled = trade.orderStatus.filled or 0
                # ✅ totalQuantity sitter på trade.order, inte orderStatus
                total  = getattr(trade.order, "totalQuantity", 0) or 0
                msg = f"📊 Orderstatus {symbol}: {status}, fylld {trade.orderStatus.filled}/{trade.order.totalQuantity}"
                print(msg)
                if bot and chat_id:
                    asyncio.get_running_loop().create_task(
                        bot.send_message(chat_id=chat_id, text=msg)
                    )
            except Exception as e:
                print(f"[on_status] fel: {e}")

        def on_fill(trade, fill):
            try:
                msg = (f"✅ Fill {trade.contract.symbol}: {fill.execution.shares} @ {fill.execution.price} "
                    f"(cum {trade.orderStatus.filled}/{getattr(trade.order,'totalQuantity',0) or 0})")
                print(msg)
                if bot and chat_id:
                    asyncio.get_running_loop().create_task(
                        bot.send_message(chat_id=chat_id, text=msg)
                    )
            except Exception as e:
                print(f"[on_fill] fel: {e}")

        trade.statusEvent += on_status
        trade.filledEvent += on_fill

    async def get_stocks(self):
        await asyncio.sleep(2)
        subscription = ScannerSubscription(
            instrument="STK",
            locationCode="STK.NASDAQ",
            scanCode="MOST_ACTIVE",
            numberOfRows=15
        )
        scan_data = await self.ib.reqScannerDataAsync(subscription)
        if not scan_data:
            print("⚠️ Ingen scanner-data returnerad! Kontrollera IBKR API.")
            return []
        tickers = [d.contractDetails.contract.symbol for d in scan_data]
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
