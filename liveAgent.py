import asyncio
import websockets
import json
import pandas as pd
from collections import deque
from bitmex_websocket import BitMEXWebsocket

class ArbitrageBot:
    BTSE_WS_URL = "wss://ws.btse.com/ws/futures"
    BTSE_WS_OB_URL = "wss://ws.btse.com/ws/oss/futures"
    BITMEX_WS_URL = "wss://ws.bitmex.com/realtime"
    BTSE_SYMBOL = 'BTC-250627'
    BITMEX_SYMBOL = 'XBTM25'
    INITIAL_CAPITAL = 10000  # USDT
    TRADE_SIZE_USDT = 1000  # USDT per trade
    OPENING_THRESHOLD = 0.3
    CLOSING_THRESHOLD = 0.1
    TRADING_FEE = 0.0005  # 0.05% fee per trade
    
    def __init__(self):
        # BitMEX WebSocket setup
        # self.bitmex_ws = BitMEXWebsocket(endpoint=self.BITMEX_WS_URL, symbol=self.BTSE_SYMBOL)
        self.bitmex_last_trade_price = None
        self.bitmex_order_book = None
        self.bitmex_wap = None  # Store WAP for comparison

        # BTSE WebSocket setup
        self.btse_last_trade_price = None
        self.btse_order_book = None
        self.btse_wap = None  # Store WAP for comparison

        self.btse_balance = self.INITIAL_CAPITAL / 2
        self.bitmex_balance = self.INITIAL_CAPITAL / 2
        self.btse_price = None
        self.bitmex_price = None
        self.prics = deque(maxlen=100)
        self.is_long_short_position_open = False
        self.is_short_long_position_open = False
        self.open_trade_info = None
        self.open_fee_short = None
        self.open_fee_long = None
        self.close_fee_short = None
        self.close_fee_long = None
        self.fee = None
        
    async def fetch_bitmex_order_book(self):        
        """Fetches the latest order book snapshot."""
        try:
            async with websockets.connect(self.BITMEX_WS_URL) as ws:
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [f"orderBook10:{self.BITMEX_SYMBOL}"]
                }
                await ws.send(json.dumps(subscribe_msg))

                while True:
                    response = await ws.recv()
                    data = json.loads(response)
                    if "data" in data and isinstance(data["data"], list):
                        self.bitmex_order_book = data["data"][0]  # Take the latest snapshot
                        return  
        except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError) as e:
            print(f"BitMEX WebSocket error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        
    async def fetch_bitmex_last_trade_price(self):
        """Subscribe to BitMEX trade history and update last traded price."""
        url = self.BITMEX_WS_URL
        try:
            while True:  # Infinite loop for automatic reconnection
                async with websockets.connect(url) as ws:
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [f"trade:{self.BITMEX_SYMBOL}"]
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    # Keep WebSocket alive
                    async def keep_alive():
                        while True:
                            try:
                                await ws.ping()
                                await asyncio.sleep(30)
                            except:
                                break  

                    asyncio.create_task(keep_alive())

                    while True:
                        response = await ws.recv()
                        data = json.loads(response)
                        
                        if "data" in data and isinstance(data["data"], list):
                            for trade in data["data"]:
                                if "price" in trade:
                                    self.bitmex_last_trade_price = float(trade["price"])
                                    print(f"BitMEX Last Traded Price: {self.bitmex_last_trade_price}")
                                    return
        except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError) as e:
            print(f"BitMEX WebSocket error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)



    async def subscribe_btse(self):
        """Connects to BTSE WebSocket for trade updates."""
        url = self.BTSE_WS_URL
        while True:
            try:
                async with websockets.connect(url) as ws:
                    subscribe_msg = {"op": "subscribe", "args": [f"tradeHistoryApiV2:{self.BTSE_SYMBOL}"]}
                    await ws.send(json.dumps(subscribe_msg))

                    async def keep_alive():
                        while True:
                            try:
                                await ws.ping()
                                await asyncio.sleep(15)
                            except:
                                break  

                    asyncio.create_task(keep_alive())

                    while True:
                        response = await ws.recv()
                        data = json.loads(response)
                        if "data" in data and isinstance(data["data"], list):
                            for trade in data["data"]:
                                if "price" in trade:
                                    self.btse_last_trade_price = float(trade["price"])
                                    print(f"BTSE Last Traded Price: {self.btse_last_trade_price}")

            except websockets.exceptions.ConnectionClosedError as e:
                print(f"BTSE WebSocket connection lost: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def fetch_btse_order_book(self):
        """Fetches BTSE order book snapshot."""
        try:
            async with websockets.connect(self.BTSE_WS_OB_URL) as ws:
                subscribe_msg = {"op": "subscribe", "args": [f"update:{self.BTSE_SYMBOL}"]}
                await ws.send(json.dumps(subscribe_msg))

                while True:
                    response = await ws.recv()
                    data = json.loads(response)
                    if "data" in data:
                        self.btse_order_book = data["data"]
                        return  
        except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError) as e:
            print(f"BTSE WebSocket error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        
    def compute_slippage(self, exchange, order_size, order_type="buy"):
        """
        Computes slippage for BitMEX or BTSE.

        Parameters:
            exchange (str): "bitmex" or "btse".
            order_size (float): Order size in USD.
            order_type (str): "buy" or "sell".

        Returns:
            tuple: (WAP, slippage percentage)
        """
        if exchange == "bitmex":
            order_book = self.bitmex_order_book
            last_trade_price = self.bitmex_last_trade_price
        else:  # BTSE
            order_book = self.btse_order_book
            last_trade_price = self.btse_last_trade_price

        if not order_book or last_trade_price is None:
            print(f"{exchange.upper()} order book or last trade price unavailable.")
            return None, None

        order_book_side = order_book["asks"] if order_type == "buy" else order_book["bids"]
        order_book_side = [{"price": float(entry[0]), "size": float(entry[1])} for entry in order_book_side]
        order_book_side.sort(key=lambda x: x["price"], reverse=(order_type == "sell"))

        if not order_book_side:
            print(f"No liquidity in {exchange.upper()} order book.")
            return None, None

        total_executed = 0
        weighted_sum = 0

        for level in order_book_side:
            price = level["price"]
            liquidity = level["size"]
            if exchange == 'btse':
                liquidity = level["size"] * 0.00001 * price
            # print('++++++++++++++++++++++++++++',exchange ,liquidity, price)
            if total_executed + liquidity >= order_size:
                weighted_sum += price * (order_size - total_executed)
                total_executed = order_size
                break
            else:
                weighted_sum += price * liquidity
                total_executed += liquidity

        if total_executed < order_size:
            print(f"Not enough liquidity in {exchange.upper()} order book.")
            return None, None

        wap = weighted_sum / order_size
        slippage = ((wap - last_trade_price) / last_trade_price) * 100 if order_type == "buy" else ((last_trade_price - wap) / last_trade_price) * 100

        return wap, slippage

    async def calculate_slippage(self, exchange):
        """Fetches order book and computes slippage for the given exchange."""
        if exchange == "bitmex":
            await self.fetch_bitmex_order_book()
            last_trade_price = self.bitmex_last_trade_price
        else:  # BTSE
            await self.fetch_btse_order_book()
            last_trade_price = self.btse_last_trade_price

        if last_trade_price is None:
            print(f"No last traded price available for {exchange.upper()}.")
            return

        wap_buy, slippage_buy = self.compute_slippage(exchange, self.TRADE_SIZE_USDT, 'buy')
        wap_sell, slippage_sell = self.compute_slippage(exchange, self.TRADE_SIZE_USDT, 'sell')

        if exchange == "bitmex":
            self.bitmex_wap_buy = wap_buy
            self.bitmex_wap_sell = wap_sell
        else:
            self.btse_wap_buy = wap_buy
            self.btse_wap_sell = wap_sell

        if wap_buy and wap_sell and slippage_buy and slippage_sell is not None:
            print(f"{exchange.upper()} Estimated WAP BUY order: {wap_buy:.2f}")
            print(f"{exchange.upper()} Estimated Slippage BUY order: {slippage_buy:.8f}%")
            print(f"{exchange.upper()} Estimated WAP SELL order: {wap_sell:.2f}")
            print(f"{exchange.upper()} Estimated Slippage SELL order: {slippage_sell:.8f}%")
        # Compute and print WAP difference if both exchanges have values
        try:
            if self.bitmex_wap_buy and self.btse_wap_sell:
                low_price = self.bitmex_wap_buy
                high_price = self.btse_wap_sell
                self.wap_difference_buy_sell = ((high_price - low_price) / min(self.bitmex_wap_buy, self.btse_wap_sell)) * 100
                print(f"WAP Difference between buy BitMEX & sell BTSE: {self.wap_difference_buy_sell:.4f}%")
                
            if self.bitmex_wap_sell and self.btse_wap_buy:
                low_price = self.btse_wap_buy
                high_price = self.bitmex_wap_sell
                self.wap_difference_sell_buy = ((high_price - low_price) / min(self.btse_wap_buy, self.bitmex_wap_sell)) * 100
                print(f"WAP Difference between sell BitMEX & buy BTSE: {self.wap_difference_sell_buy:.4f}%")
            
            if not self.is_long_short_position_open and self.wap_difference_buy_sell > self.OPENING_THRESHOLD:
                self.open_trade_info = self.open_position(long_exchange='BITMEX', short_exchange='BTSE', long_price=self.bitmex_wap_buy, short_price=self.btse_wap_sell, diff=self.wap_difference_buy_sell)
                self.is_long_short_position_open = True
            elif not self.is_short_long_position_open and self.wap_difference_sell_buy > self.OPENING_THRESHOLD:
                self.open_trade_info = self.open_position(long_exchange='BTSE', short_exchange='BITMEX', long_price=self.btse_wap_buy, short_price=self.bitmex_wap_sell, diff=self.wap_difference_sell_buy)
                self.is_short_long_position_open = True

            if self.is_long_short_position_open and self.wap_difference_sell_buy > self.CLOSING_THRESHOLD:
                self.close_position(new_long_price=self.bitmex_wap_sell, new_short_price=self.btse_wap_buy, trade_info=self.open_trade_info)
                self.is_long_short_position_open = False
            elif self.is_short_long_position_open and self.wap_difference_buy_sell > self.CLOSING_THRESHOLD:
                self.close_position(new_long_price=self.btse_wap_sell, new_short_price=self.bitmex_wap_buy, trade_info=self.open_trade_info)
                self.is_short_long_position_open = False
                
        except Exception as e:
            print(f"Unexpected error: {e}")
            
    async def run(self):
        """Runs BitMEX and BTSE WebSocket connections concurrently."""
        await asyncio.gather(
            self.subscribe_btse(),
            self.bitmex_loop()
        )

    async def bitmex_loop(self):
        """Fetches BitMEX trade price and order book continuously."""
        while True:
            await self.fetch_bitmex_last_trade_price()
            await self.calculate_slippage("bitmex")
            await self.calculate_slippage("btse")
            await asyncio.sleep(0.1)  # Adjust polling interval

    def open_position(self, long_exchange, short_exchange, long_price, short_price, diff):
        contracts_short = self.TRADE_SIZE_USDT / short_price
        contracts_long = self.TRADE_SIZE_USDT / long_price
        self.open_fee_short = self.TRADING_FEE * self.TRADE_SIZE_USDT
        self.open_fee_long = self.TRADING_FEE * self.TRADE_SIZE_USDT
        self.fee = self.open_fee_short + self.open_fee_long 
        if short_exchange == "BTSE":
            self.btse_balance -= self.open_fee_short
            self.bitmex_balance -= self.open_fee_long
        else:
            self.bitmex_balance -= self.open_fee_short
            self.btse_balance -= self.open_fee_long

        self.is_position_open = True
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

    def close_position(self, new_long_price, new_short_price, trade_info):        
        self.close_fee_short = self.TRADING_FEE * trade_info["contracts_short"] * new_short_price
        self.close_fee_long = self.TRADING_FEE * trade_info["contracts_long"] * new_long_price
        fee_short = self.open_fee_short + self.close_fee_short
        fee_long = self.open_fee_long + self.close_fee_long
        self.fee = fee_short + fee_long
        pnl_short = trade_info["contracts_short"] * (trade_info["short_price"] - new_short_price)
        pnl_long = trade_info["contracts_long"] * (new_long_price - trade_info["long_price"])
        total_pnl = pnl_short + pnl_long

        # Update prices in trade info
        trade_info["short_price"] = new_short_price
        trade_info["long_price"] = new_long_price

        # Correct fee-adjusted balance update
        if trade_info["short_exchange"] == "BTSE":
            self.btse_balance += pnl_short - self.close_fee_short
            self.bitmex_balance += pnl_long - self.close_fee_long
        else:
            self.bitmex_balance += pnl_short - self.close_fee_short
            self.btse_balance += pnl_long - self.close_fee_long
            
        self.is_position_open = False
        print(f"✅ CLOSING POSITION: Short {trade_info['short_exchange']}, Long {trade_info['long_exchange']}")
        self.log_trade("CLOSE", trade_info, total_pnl)


    def log_trade(self, action, trade_info, pnl=0):
        total_balance = self.btse_balance + self.bitmex_balance
        log_data = {
            "Timestamp": pd.Timestamp.utcnow(),
            "Action": action,
            "Short Exchange": trade_info.get("short_exchange", ""),
            "Long Exchange": trade_info.get("long_exchange", ""),
            "Contracts Short": trade_info.get("contracts_short", 0),
            "Contracts Long": trade_info.get("contracts_long", 0),
            "Short Price": trade_info.get("short_price", 0),
            "Long Price": trade_info.get("long_price", 0),
            "PnL": f"{pnl:.4f}",
            "Total Fee": f"{self.fee:.4f}",
            "Net PnL": f"{pnl - self.fee:.4f}",
            "BTSE Balance": f"{self.btse_balance:.2f}",
            "BitMEX Balance": f"{self.bitmex_balance:.2f}",
            "Total Balance": f"{total_balance:.2f}"
        }
        df = pd.DataFrame([log_data])
        df.to_csv("trading_log.csv", mode="a", header=not pd.io.common.file_exists("trading_log.csv"), index=False)
        
        
if __name__ == "__main__":
    bot = ArbitrageBot()
    asyncio.run(bot.run())
