import os
from dotenv import load_dotenv
from ib_insync import *
import time
import yfinance as yf

load_dotenv()


sp500 = yf.Ticker("QQQ").holdings
stocks = sp500.history(period="1d")


class IbClient:
    def __init__(self):
        self.ib = IB()
        self.ib.connect("127.0.0.1", 4002, clientId=1)

        print("api Connceted!")

    def get_stock():
        stocks.get_tickers()

    def disconnect_ibkr(self):
        time.sleep(2)
        self.ib.disconnect()
        print("api Disconnected!")
