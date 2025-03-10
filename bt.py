import asyncio
import json
import websockets
from bitmex_websocket import BitMEXWebsocket

class BitMEXSlippageCalculator:
    def __init__(self, symbol="XBTH25"):
        self.symbol = symbol
        self.ws = BitMEXWebsocket(endpoint="wss://ws.bitmex.com/realtime", symbol=symbol)
        self.last_trade_price = None
        self.order_book = None

    async def fetch_order_book(self):
        """Fetches the latest order book snapshot."""
        self.order_book = self.ws.market_depth()
        return self.order_book

    async def fetch_last_trade_price(self):
        """Fetches the most recent trade price."""
        trades = self.ws.recent_trades()
        if trades:
            self.last_trade_price = trades[0]['price']
            print(f'Last price: {self.last_trade_price}')
        return self.last_trade_price

    def compute_slippage(self, order_size, order_type="buy"):
        """
        Computes Weighted Average Execution Price (WAP) and slippage.

        Parameters:
            order_size (float): Order size in USD.
            order_type (str): "buy" or "sell".

        Returns:
            tuple: (WAP, slippage percentage)
        """
        if not self.order_book:
            print("Order book is empty, cannot compute slippage.")
            return None, None

        # Ensure order book is a list of dictionaries
        if not isinstance(self.order_book, list):
            print("Unexpected order book format. Expected a list.")
            return None, None

        # Determine order book side: Buy -> Asks | Sell -> Bids
        order_book_side = [entry for entry in self.order_book if entry['side'] == ('Sell' if order_type == "buy" else 'Buy')]

        if not order_book_side:
            print("No liquidity available.")
            return None, None

        # Sort by price: Buy (ascending), Sell (descending)
        order_book_side.sort(key=lambda x: x['price'], reverse=(order_type == "sell"))

        best_price = float(order_book_side[0]['price'])  # Best available price
        total_executed = 0
        weighted_sum = 0

        for level in order_book_side:
            price = float(level['price'])
            liquidity = float(level['size'])

            if total_executed + liquidity >= order_size:
                weighted_sum += price * (order_size - total_executed)
                total_executed = order_size
                break
            else:
                weighted_sum += price * liquidity
                total_executed += liquidity

        if total_executed < order_size:
            print("Not enough liquidity in order book!")
            return None, None

        wap = weighted_sum / order_size  # Weighted Average Execution Price
        slippage = ((wap - self.last_trade_price) / self.last_trade_price) * 100 if order_type == "buy" else ((self.last_trade_price - wap) / self.last_trade_price) * 100

        return wap, slippage


    async def calculate_slippage(self):
        """Fetches order book and computes slippage using last traded price."""
        if self.last_trade_price is None:
            print("No last traded price available yet.")
            return
        
        await self.fetch_order_book()
        order_size = 1000  # Adjust order size as needed
        wap, slippage = self.compute_slippage(order_size, order_type="buy")

        if wap and slippage is not None:
            print(f"Estimated WAP: {wap:.2f}")
            print(f"Estimated Slippage: {slippage:.8f}%")

    async def run(self):
        """Main loop to fetch prices and compute slippage continuously."""
        while True:
            await self.fetch_last_trade_price()
            await self.calculate_slippage()
            await asyncio.sleep(1)  # Adjust polling interval as needed

if __name__ == "__main__":
    calculator = BitMEXSlippageCalculator()
    asyncio.run(calculator.run())
