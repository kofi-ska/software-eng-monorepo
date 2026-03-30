import asyncio
import pandas as pd
import sys, os
from binance import AsyncClient, BinanceSocketManager


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from analysis.klines import kline_computation



async def stream_klines(symbol: str, interval: str, bm, window: int = 45):
    buffer = pd.DataFrame()

    async with bm.kline_socket(symbol.lower(), interval=interval) as ws:
        print(f"[KLINE] Connected for {symbol}")
        while True:
            msg = await ws.recv()
            if "data" in msg:
                msg = msg["data"]
            if "k" not in msg:
                continue

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

            # append to buffer
            buffer = pd.concat([buffer, pd.DataFrame([data])], ignore_index=True)
            if len(buffer) < window:
                continue

            # keep last N rows
            buffer = buffer.tail(window * 2)

            # compute analytics
            result = kline_computation(buffer, window)
            print(f"[{symbol}] {result}")


# ------------------------------------------------------------
# Main entrypoint
# ------------------------------------------------------------
async def main():
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)

    symbols = ["BTCUSDT", "SOLUSDT"]
    interval = "5m"

    try:
        await asyncio.gather(*(stream_klines(s, interval, bm) for s in symbols))
    finally:
        await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
