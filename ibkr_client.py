# ibkr_client.py
from ib_insync import IB, ScannerSubscription, Stock, MarketOrder
import asyncio


class IbClient:
    def __init__(self):
        self.ib = IB()
       

    async def connect(self):
        if not self.ib.isConnected():
            try:
                await self.ib.connectAsync("127.0.0.1", 4001, clientId=1, timeout=30)
                print("✅ API Connected on 4002!")
            except Exception as e:
                print(f"❌ API connection failed: {e}")
        else:
            print("ℹ️ IBKR redan ansluten")

    

    async def place_order(self, symbol, side, qty):
        contract = Stock(symbol, "SMART", "USD")
        order = MarketOrder(side, qty)
        # Kör den synkrona metoden i en separat tråd
        trade = await asyncio.to_thread(self.ib.placeOrder, contract, order)
        print(f"Order placerad: {trade}")
        return trade


    async def get_stocks(self):
        await asyncio.sleep(2)
        subscription = ScannerSubscription(
            instrument="STK",
            locationCode="STK.US.MAJOR",
            scanCode="TOP_PERC_LOSE",
        )
        scan_data = await self.ib.reqScannerDataAsync(subscription)
        if not scan_data:
            print("⚠️ Ingen scanner-data returnerad! Kontrollera IBKR API.")
            return []
        tickers = [data.contractDetails.contract.symbol for data in scan_data]
        print(f"✅ Hämtade {len(tickers)} aktier: {tickers}")
        return tickers

    async def scanner_parameters(self):
        scanner_xml = self.ib.reqScannerParameters()
        with open("scanner_parameters.xml", "w", encoding="utf-8") as file:
            file.write(scanner_xml)
        print("✅ Scanner parameters saved!")

    async def disconnect_ibkr(self):
        await asyncio.sleep(2)
        self.ib.disconnect()
        print("❌ API Disconnected!")
