import asyncio
from binance import BinanceSocketManager

# ------------------------------------------------------------
# Async K-line stream producer
# ------------------------------------------------------------
async def stream_klines(symbol: str, interval: str, queue: asyncio.Queue, bm: BinanceSocketManager):
    """Streams live K-lines (candlesticks) via websocket."""
    stream = bm.kline_socket(symbol.lower(), interval=interval)
    async with stream as ws:
        while True:
            msg = await ws.recv()
            k = msg["k"]
            data = {
                "open_time": k["t"],
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
                "close_time": k["T"],
            }
            await queue.put(("klines", symbol, data))


# ------------------------------------------------------------
# Async Order book stream producer
# ------------------------------------------------------------

async def stream_orderbook(symbol: str, queue: asyncio.Queue, bm: BinanceSocketManager):
    """Streams live order book depth via websocket."""
    stream = bm.depth_socket(symbol.lower(), depth=10)
    async with stream as ws:
        while True:
            msg = await ws.recv()

            # Sometimes msg is wrapped as {"stream": "...", "data": {...}}
            if "data" in msg:
                msg = msg["data"]

            # Ignore anything that doesn't have bids/asks
            if not isinstance(msg, dict) or "b" not in msg or "a" not in msg:
                continue

            bids = msg["b"]
            asks = msg["a"]
            await queue.put(("orderbook", symbol, (bids, asks)))
