import asyncio
import json
import websockets

BITMEX_WS_URL = "wss://www.bitmex.com/realtime"
BITMEX_SYMBOL = "XBTH25"  # Modify for your trading pair

class BitMEXClient:
    def __init__(self):
        self.bitmex_price = None  # Last traded price
        self.order_book = None  # Latest order book snapshot

    async def subscribe_bitmex(self):
        """Subscribe to BitMEX trade history and update last traded price."""
        url = BITMEX_WS_URL

        while True:  # Infinite loop for automatic reconnection
            try:
                async with websockets.connect(url) as ws:
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [f"trade:{BITMEX_SYMBOL}"]
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
                                    self.bitmex_price = float(trade["price"])
                                    print(f"BitMEX Last Traded Price: {self.bitmex_price}")
                                    await self.calculate_slippage()

            except websockets.exceptions.ConnectionClosedError as e:
                print(f"BitMEX WebSocket connection lost: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def fetch_order_book(self):
        """Fetches the latest order book snapshot."""
        async with websockets.connect(BITMEX_WS_URL) as ws:
            subscribe_msg = {
                "op": "subscribe",
                "args": [f"orderBook10:{BITMEX_SYMBOL}"]
            }
            await ws.send(json.dumps(subscribe_msg))

            while True:
                response = await ws.recv()
                data = json.loads(response)
                if "data" in data and isinstance(data["data"], list):
                    self.order_book = data["data"][0]  # Take the latest snapshot
                    print(self.order_book)
                    return  

    def compute_slippage(self, order_size, order_type="buy"):
        """
        Simulates a market order execution and computes Weighted Average Execution Price (WAP) and slippage.

        Parameters:
            order_size (float): Order size in USD.
            order_type (str): "buy" or "sell".

        Returns:
            tuple: (WAP, slippage percentage)
        """
        if not self.order_book:
            print("Order book is empty, cannot compute slippage.")
            return None, None

        order_book_side = self.order_book["asks"] if order_type == "buy" else self.order_book["bids"]
        best_price = float(order_book_side[0][0])
        
        total_executed = 0
        weighted_sum = 0
        order_book_copy = order_book_side[:]
        for level in order_book_copy:
            print(level)
            price = float(level[0])
            liquidity = float(level[1])

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
        slippage = ((wap - best_price) / best_price) * 100 if order_type == "buy" else ((best_price - wap) / best_price) * 100

        return wap, slippage

    async def calculate_slippage(self):
        """Fetches order book, computes slippage using last traded price as the market order level."""
        if self.bitmex_price is None:
            print("No last traded price available yet.")
            return
        while True:
            await self.fetch_order_book()
            
            order_size = 1000  # Modify order size as needed
            wap, slippage = self.compute_slippage(order_size, order_type="sell")

            if wap and slippage is not None:
                print(f"Estimated WAP: {wap:.2f}")
                print(f"Estimated Slippage: {slippage:.8f}%")
            await asyncio.sleep(0.001)

async def main():
    client = BitMEXClient()
    await client.subscribe_bitmex()

asyncio.run(main())
