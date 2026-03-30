"""
End-to-End Real-Time Entropy Monitor (Binance WebSocket)

Features
--------
• Streams trades, order-book, and optional kline data.
• Computes real-time entropy metrics for each ticker.
• Aggregates a 'market view' across tickers.
• Exposes a shared entropy_history buffer for Dash visualization.
• No trading, just observation and metrics.

Usage
-----
1️⃣  Run this monitor:
    uv run --active python entropy_monitor.py
2️⃣  In another terminal, run dash_app.py (see separate file)
"""

import asyncio
import json
import time
from collections import deque, defaultdict
import numpy as np
from scipy.stats import entropy
import websockets

# -------------------- Config --------------------
AGG_TRADE = "{symbol}@aggTrade"
DEPTH = "{symbol}@depth@100ms"
KLINE = "{symbol}@kline_1m"
BASE_URL = "wss://stream.binance.com:9443/stream?streams="

WINDOW = 400  # number of recent trades to keep
INTERVAL = 3  # seconds between entropy updates
HISTORY_LIMIT = 500  # max points per ticker for dash

# -------------------- Shared History for Dash --------------------
# Dash app can import this dict directly
entropy_history = defaultdict(lambda: deque(maxlen=HISTORY_LIMIT))


# -------------------- Entropy Computations --------------------
def shannon(values, bins=10):
    if len(values) == 0:
        return 0.0
    hist, _ = np.histogram(values, bins=bins, density=True)
    return float(entropy(hist + 1e-9, base=2))


def direction_entropy(trades):
    if not trades:
        return 0.0
    sides = [t["isBuyerMaker"] for t in trades]
    p_buy = sides.count(False) / len(sides)
    p_sell = 1 - p_buy
    p = np.array([p_buy, p_sell])
    return float(entropy(p + 1e-9, base=2))


def volume_entropy(trades):
    vols = [float(t["q"]) for t in trades]
    return shannon(vols)


def liquidity_entropy(book):
    if not book:
        return 0.0
    bids = [float(b[1]) for b in book["bids"][:10]]
    asks = [float(a[1]) for a in book["asks"][:10]]
    all_depths = np.array(bids + asks)
    if np.sum(all_depths) == 0:
        return 0.0
    probs = all_depths / np.sum(all_depths)
    return float(entropy(probs + 1e-9, base=2))


def imbalance_entropy(book):
    if not book:
        return 0.0
    bid_sum = sum(float(b[1]) for b in book["bids"][:10])
    ask_sum = sum(float(a[1]) for a in book["asks"][:10])
    imb = (bid_sum - ask_sum) / max(bid_sum + ask_sum, 1e-9)
    return abs(imb)


def normalize_book(data):
    """Normalize Binance depth message to {'bids','asks'}."""
    if not data:
        return {"bids": [], "asks": []}
    bids = data.get("b") or data.get("bids") or []
    asks = data.get("a") or data.get("asks") or []
    return {"bids": bids, "asks": asks}


def normalize_trade(data):
    """Normalize trade messages to include isBuyerMaker field."""
    if "isBuyerMaker" in data:
        return data
    if "m" in data:
        data["isBuyerMaker"] = data["m"]
    else:
        data["isBuyerMaker"] = False
    return data


# -------------------- Core Async Engine --------------------
class EntropyMonitor:
    def __init__(self, tickers, use_kline=True, use_orderbook=True):
        self.tickers = [t.lower() for t in tickers]
        self.use_kline = use_kline
        self.use_orderbook = use_orderbook

        self.trades = defaultdict(lambda: deque(maxlen=WINDOW))
        self.books = defaultdict(dict)
        self.kline_info = defaultdict(dict)
        self.last_update = time.time()

    # ---- message routing ----
    async def handle_message(self, msg):
        stream = msg.get("stream", "")
        data = msg.get("data", {})
        for symbol in self.tickers:
            if symbol in stream:
                if "@aggTrade" in stream:
                    self.trades[symbol].append(normalize_trade(data))
                elif "@depth" in stream and self.use_orderbook:
                    self.books[symbol] = normalize_book(data)
                elif "@kline" in stream and self.use_kline:
                    self.kline_info[symbol] = data.get("k", {})
                break

    # ---- entropy aggregation ----
    def compute_entropies(self):
        results = {}
        for sym in self.tickers:
            trades = list(self.trades[sym])
            book = self.books.get(sym, {})
            H_dir = direction_entropy(trades)
            H_vol = volume_entropy(trades)
            H_liq = liquidity_entropy(book)
            H_imb = imbalance_entropy(book)
            H_total = 0.3 * H_dir + 0.3 * H_vol + 0.3 * H_liq + 0.1 * H_imb
            results[sym] = {
                "direction": H_dir,
                "volume": H_vol,
                "liquidity": H_liq,
                "imbalance": H_imb,
                "MSE": H_total,
            }
        return results

    def market_view(self, entropies):
        vals = [e["MSE"] for e in entropies.values() if e["MSE"] > 0]
        return np.mean(vals) if vals else 0.0

    # ---- display + push to history ----
    def print_snapshot(self, entropies):
        ts = time.time()
        print("\n" + "=" * 70)
        print(f"Timestamp: {time.strftime('%H:%M:%S')}")
        for sym, e in entropies.items():
            # push to global history for Dash
            entropy_history[sym].append(
                {
                    "t": ts,
                    "Hdir": e["direction"],
                    "Hvol": e["volume"],
                    "Hliq": e["liquidity"],
                    "Himb": e["imbalance"],
                    "MSE": e["MSE"],
                }
            )
            print(
                f"{sym.upper():10s}  "
                f"Hdir={e['direction']:.3f}  Hvol={e['volume']:.3f}  "
                f"Hliq={e['liquidity']:.3f}  Himb={e['imbalance']:.3f}  "
                f"MSE={e['MSE']:.3f}"
            )
        print(f"Market Structural Entropy (avg): {self.market_view(entropies):.3f}")
        print("=" * 70)

    # ---- main loop ----
    async def run(self):
        streams = [AGG_TRADE.format(symbol=s) for s in self.tickers]
        if self.use_orderbook:
            streams += [DEPTH.format(symbol=s) for s in self.tickers]
        if self.use_kline:
            streams += [KLINE.format(symbol=s) for s in self.tickers]
        url = BASE_URL + "/".join(streams)

        async with websockets.connect(url, ping_interval=20) as ws:
            print(f"Connected to Binance for {', '.join(self.tickers)}")
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    msg = json.loads(msg)
                    await self.handle_message(msg)
                    now = time.time()
                    if now - self.last_update > INTERVAL:
                        entropies = self.compute_entropies()
                        self.print_snapshot(entropies)
                        self.last_update = now
                except asyncio.TimeoutError:
                    await ws.ping()
                except Exception as e:
                    print("Error:", e)
                    break


# -------------------- Entry Point --------------------
def main():
    tickers = ["btcusdt", "ethusdt", "solusdt", "lineausdt", "bnbusdt", "adausdt"]
    use_kline = True
    use_orderbook = True

    monitor = EntropyMonitor(tickers, use_kline, use_orderbook)
    asyncio.run(monitor.run())


if __name__ == "__main__":
    main()
