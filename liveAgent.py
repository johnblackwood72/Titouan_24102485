import asyncio
import websockets
import json
import pandas as pd
from collections import deque
from bitmex_websocket import BitMEXWebsocket

class ArbitrageBot:
    BTSE_WS_URL = "wss://ws.btse.com/ws/futures"
    BITMEX_WS_URL = "wss://ws.bitmex.com/realtime"
    BTSE_SYMBOL = 'BTC-250328'
    BITMEX_SYMBOL = 'XBTH25'
    INITIAL_CAPITAL = 10000  # USDT
    TRADE_SIZE_USDT = 1000  # USDT per trade
    OPENING_THRESHOLD = 0.6
    CLOSING_THRESHOLD = 0.1
    TRADING_FEE = 0.0005  # 0.05% fee per trade

    def __init__(self):
        self.btse_balance = self.INITIAL_CAPITAL / 2
        self.bitmex_balance = self.INITIAL_CAPITAL / 2
        self.btse_price = None
        self.bitmex_price = None
        self.price_diffs = deque(maxlen=100)
        self.position_open = False
        self.open_trade_info = None
        self.fee = None

    async def subscribe_btse(self):
        url = self.BTSE_WS_URL

        while True:  # Infinite loop for automatic reconnection
            try:
                async with websockets.connect(url) as ws:
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [f"tradeHistoryApiV2:{self.BTSE_SYMBOL}"]
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    # Background task to send ping messages
                    async def keep_alive():
                        while True:
                            try:
                                await ws.ping()
                                await asyncio.sleep(30)  # Adjust ping interval if needed
                            except:
                                break  # Stop keep-alive task on failure

                    asyncio.create_task(keep_alive())

                    while True:
                        response = await ws.recv()
                        data = json.loads(response)
                        
                        if "data" in data and isinstance(data["data"], list):
                            for trade in data["data"]:
                                if "price" in trade:
                                    self.btse_price = trade["price"]
                                    print(f"BTSE Price: {self.btse_price}")
                                    await self.calculate_difference()

            except websockets.exceptions.ConnectionClosedError as e:
                print(f"BTSE WebSocket connection lost: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

            except Exception as e:
                print(f"Unexpected error in BTSE WebSocket: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def subscribe_bitmex(self):
        while True:
            try:
                print("Connecting to BitMEX WebSocket...")
                ws = BitMEXWebsocket(endpoint=self.BITMEX_WS_URL, symbol=self.BITMEX_SYMBOL, api_key=None, api_secret=None)
                
                while ws.ws.sock and ws.ws.sock.connected:
                    recent_trades = ws.recent_trades()
                    if recent_trades and self.bitmex_price != recent_trades[-1]["price"]:
                        self.bitmex_price = recent_trades[-1]["price"]
                        print(f"BitMEX Price: {self.bitmex_price}")
                        await self.calculate_difference()
                    await asyncio.sleep(1)
            
            except Exception as e:
                print(f"BitMEX WebSocket connection lost: {e}")
                print("Reconnecting in 3 seconds...")
                await asyncio.sleep(3)  # Wait before retrying


    async def calculate_difference(self):
        if self.btse_price is not None and self.bitmex_price is not None:
            higher_price = max(self.btse_price, self.bitmex_price)
            lower_price = min(self.btse_price, self.bitmex_price)
            spread_p = 100 * ((higher_price - lower_price) / lower_price)
            print(f"Spread %: {spread_p:.4f}")
            
            if not self.position_open and spread_p > self.OPENING_THRESHOLD:
                self.open_trade_info = self.open_position(spread_p)
            if self.position_open and spread_p < self.CLOSING_THRESHOLD:
                self.close_position(spread_p, self.open_trade_info)

    def open_position(self, diff):
        if self.btse_price > self.bitmex_price:
            short_exchange, long_exchange = "BTSE", "BitMEX"
            short_price, long_price = self.btse_price, self.bitmex_price
        else:
            short_exchange, long_exchange = "BitMEX", "BTSE"
            short_price, long_price = self.bitmex_price, self.btse_price

        contracts_short = self.TRADE_SIZE_USDT / short_price
        contracts_long = self.TRADE_SIZE_USDT / long_price

        # if short_exchange == "BTSE":
        #     self.btse_balance -= self.TRADE_SIZE_USDT * (1 + self.TRADING_FEE)
        #     self.bitmex_balance -= self.TRADE_SIZE_USDT * (1 + self.TRADING_FEE)
        # else:
        #     self.bitmex_balance -= self.TRADE_SIZE_USDT * (1 + self.TRADING_FEE)
        #     self.btse_balance -= self.TRADE_SIZE_USDT * (1 + self.TRADING_FEE)

        self.position_open = True
        print(f"🚀 OPENING POSITION: Short {short_exchange}, Long {long_exchange}")
        
        trade_info = {
            "entry_diff": diff,
            "short_exchange": short_exchange,
            "long_exchange": long_exchange,
            "contracts_short": contracts_short,
            "contracts_long": contracts_long,
            "short_price": short_price,
            "long_price": long_price
        }
        self.log_trade("OPEN", trade_info)
        return trade_info

    def close_position(self, diff, trade_info):
        new_short_price = self.btse_price if trade_info["short_exchange"] == "BTSE" else self.bitmex_price
        new_long_price = self.bitmex_price if trade_info["long_exchange"] == "BitMEX" else self.btse_price

        # Correct PnL formulas
        # trade_size1 * (entry_1 - exit_1) - total_fee
        # fee1 = (transaction_cost * trade_size1 * entry_1) + (transaction_cost * trade_size1 * exit_1)
        # fee2 = (transaction_cost * trade_size2 * entry_2) + (transaction_cost * trade_size2 * exit_2)
        # total_fee = fee1 + fee2
        fee_short = (self.TRADING_FEE * trade_info["contracts_short"] * trade_info["short_price"]) + (self.TRADING_FEE * trade_info["contracts_short"] * new_short_price)
        fee_long = (self.TRADING_FEE * trade_info["contracts_long"] * trade_info["long_price"]) + (self.TRADING_FEE * trade_info["contracts_long"] * new_long_price)
        self.fee = fee_short + fee_long
        pnl_short = trade_info["contracts_short"] * (trade_info["short_price"] - new_short_price) #- fee_short
        pnl_long = trade_info["contracts_long"] * (new_long_price - trade_info["long_price"]) #- fee_long
        total_pnl = pnl_short + pnl_long

        # Update prices in trade info
        trade_info["short_price"] = new_short_price
        trade_info["long_price"] = new_long_price

        # Correct fee-adjusted balance update
        # if trade_info["short_exchange"] == "BTSE":
        #     self.btse_balance += (trade_info["contracts_short"] * new_short_price) * (1 - self.TRADING_FEE)
        #     self.bitmex_balance += (trade_info["contracts_long"] * new_long_price) * (1 - self.TRADING_FEE)
        # else:
        #     self.bitmex_balance += (trade_info["contracts_short"] * new_short_price) * (1 - self.TRADING_FEE)
        #     self.btse_balance += (trade_info["contracts_long"] * new_long_price) * (1 - self.TRADING_FEE)

        self.position_open = False
        print(f"✅ CLOSING POSITION: Short {trade_info['short_exchange']}, Long {trade_info['long_exchange']}")
        self.log_trade("CLOSE", trade_info, total_pnl)


    def log_trade(self, action, trade_info, pnl=0):
        total_balance = self.btse_balance + self.bitmex_balance + pnl - self.fee
        log_data = {
            "Action": action,
            "Short Exchange": trade_info.get("short_exchange", ""),
            "Long Exchange": trade_info.get("long_exchange", ""),
            "Contracts Short": trade_info.get("contracts_short", 0),
            "Contracts Long": trade_info.get("contracts_long", 0),
            "Short Price": trade_info.get("short_price", 0),
            "Long Price": trade_info.get("long_price", 0),
            "PnL": pnl,
            "Total Fee": self.fee,
            "BTSE Balance": self.btse_balance,
            "BitMEX Balance": self.bitmex_balance,
            "Total Balance": total_balance
        }
        df = pd.DataFrame([log_data])
        df.to_csv("trading_log.csv", mode="a", header=not pd.io.common.file_exists("trading_log.csv"), index=False)

    async def run(self):
        await asyncio.gather(self.subscribe_btse(), self.subscribe_bitmex())

if __name__ == "__main__":
    bot = ArbitrageBot()
    asyncio.run(bot.run())
