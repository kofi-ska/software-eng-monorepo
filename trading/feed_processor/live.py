import asyncio
import json
import websockets
from abc import ABC, abstractmethod
from collections import deque
import math


BINANCE_WSS = "wss://fstream.binance.com/ws"




class Welford:
    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0

    def push(self, x: float):
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        self.M2 += delta * (x - self.mean)

    def variance(self):
        if self.n < 2:
            return 0.0
        return self.M2 / (self.n - 1)

    def std(self):
        return math.sqrt(self.variance())



class FeedProcessor(ABC):
    def __init__(self, symbol: str):
        self.symbol = symbol.lower()

    @abstractmethod
    async def retrieve_feed(self):
        pass

    @abstractmethod
    def process_feed(self, raw):
        pass




class KlinesProcessor(FeedProcessor):
    def __init__(self, symbol: str, interval="1m"):
        super().__init__(symbol)
        self.interval = interval

        # memory for sequential direction + previous OHLC
        self.prev_close = None
        self.prev_high = None
        self.prev_low = None
        self.seq_up = 0
        self.seq_down = 0

        # Welford for rolling volatility of closes
        self.welford = Welford()

    async def retrieve_feed(self):
        stream = f"{self.symbol}@kline_{self.interval}"
        url = f"{BINANCE_WSS}/{stream}"

        async with websockets.connect(url) as ws:
            while True:
                msg = await ws.recv()
                yield json.loads(msg)

    def process_feed(self, raw):
        k = raw.get("k", {})

        o = float(k.get("o", 0))
        h = float(k.get("h", 0))
        l = float(k.get("l", 0))
        c = float(k.get("c", 0))
        v = float(k.get("v", 0))

        # candle geometry
        body = abs(c - o)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        candle_range = h - l
        body_ratio = body / candle_range if candle_range else 0
        wick_ratio_upper = upper_wick / candle_range if candle_range else 0
        wick_ratio_lower = lower_wick / candle_range if candle_range else 0

        # true range
        true_range = h - l

        # VWAP (standard)
        vwap = ((o + h + l + c) / 4)

        # distance from VWAP
        dist_vwap = c - vwap

        # direction
        if self.prev_close is None:
            direction = ""
        else:
            if c > self.prev_close:
                direction = "up"
                self.seq_up += 1
                self.seq_down = 0
            elif c < self.prev_close:
                direction = "down"
                self.seq_down += 1
                self.seq_up = 0
            else:
                direction = "flat"

        # previous high/low checks
        close_above_prev_high = (self.prev_high is not None and c > self.prev_high)
        close_below_prev_low = (self.prev_low is not None and c < self.prev_low)

        # volatility via welford
        self.welford.push(c)
        volatility = self.welford.std()

        # range compression heuristic
        range_compression = candle_range < (self.prev_high - self.prev_low) * 0.5 if self.prev_high and self.prev_low else False

        # micro structure label
        if direction == "up" and body_ratio > 0.6:
            micro_structure = "impulse"
        elif direction == "down" and body_ratio > 0.6:
            micro_structure = "impulse"
        elif body_ratio < 0.2:
            micro_structure = "neutral"
        else:
            micro_structure = "pullback"

        # update memory
        self.prev_close = c
        self.prev_high = h
        self.prev_low = l

        return {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,

            "body": body,
            "upper_wick": upper_wick,
            "lower_wick": lower_wick,
            "candle_range": candle_range,
            "body_ratio": body_ratio,
            "wick_ratio_upper": wick_ratio_upper,
            "wick_ratio_lower": wick_ratio_lower,

            "true_range": true_range,
            "volatility": volatility,
            "range_compression": range_compression,

            "direction": direction,
            "close_above_prev_high": close_above_prev_high,
            "close_below_prev_low": close_below_prev_low,

            "vwap": vwap,
            "distance_from_vwap": dist_vwap,

            "sequential_up": self.seq_up,
            "sequential_down": self.seq_down,
            "micro_structure": micro_structure
        }



class OrderbookProcessor(FeedProcessor):
    def __init__(self, symbol: str, depth=20):
        super().__init__(symbol)
        self.depth = depth

    async def retrieve_feed(self):
        stream = f"{self.symbol}@depth{self.depth}"
        url = f"{BINANCE_WSS}/{stream}"

        async with websockets.connect(url) as ws:
            while True:
                msg = await ws.recv()
                yield json.loads(msg)

    def process_feed(self, raw):
        bids = raw.get("b", [])
        asks = raw.get("a", [])

        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        midprice = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
        spread = best_ask - best_bid if best_ask and best_bid else 0

        # totals
        top_bids = bids[:self.depth]
        top_asks = asks[:self.depth]

        bids_total = sum(float(x[1]) for x in top_bids)
        asks_total = sum(float(x[1]) for x in top_asks)

        imbalance = (bids_total - asks_total) / (bids_total + asks_total) if (bids_total + asks_total) else 0

        # slopes
        bid_slope = 0
        ask_slope = 0
        if len(top_bids) >= 2:
            bid_slope = float(top_bids[1][1]) - float(top_bids[0][1])
        if len(top_asks) >= 2:
            ask_slope = float(top_asks[1][1]) - float(top_asks[0][1])

        # walls
        bid_wall = {"price": 0, "size": 0}
        ask_wall = {"price": 0, "size": 0}

        if top_bids:
            bid_wall = max(top_bids, key=lambda x: float(x[1]))
            bid_wall = {"price": float(bid_wall[0]), "size": float(bid_wall[1])}

        if top_asks:
            ask_wall = max(top_asks, key=lambda x: float(x[1]))
            ask_wall = {"price": float(ask_wall[0]), "size": float(ask_wall[1])}

        # microstructure heuristics
        top5_bid_volume = sum(float(x[1]) for x in bids[:5])
        top5_ask_volume = sum(float(x[1]) for x in asks[:5])

        liquidity_density_bid = top5_bid_volume / spread if spread else 0
        liquidity_density_ask = top5_ask_volume / spread if spread else 0

        bid_exhaustion = top5_bid_volume < (top5_ask_volume * 0.5)
        ask_exhaustion = top5_ask_volume < (top5_bid_volume * 0.5)

        thin_book = (top5_bid_volume + top5_ask_volume) < 1000

        absorption = False  # placeholder if you want deeper logic later

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "midprice": midprice,
            "spread": spread,

            "bids_total": bids_total,
            "asks_total": asks_total,
            "imbalance": imbalance,

            "bid_slope": bid_slope,
            "ask_slope": ask_slope,

            "bid_wall": bid_wall,
            "ask_wall": ask_wall,

            "bid_exhaustion": bid_exhaustion,
            "ask_exhaustion": ask_exhaustion,
            "absorption": absorption,
            "thin_book": thin_book,

            "top5_bid_volume": top5_bid_volume,
            "top5_ask_volume": top5_ask_volume,
            "liquidity_density_bid": liquidity_density_bid,
            "liquidity_density_ask": liquidity_density_ask
        }




async def stream_state(symbol):
    kproc = KlinesProcessor(symbol)
    oproc = OrderbookProcessor(symbol)

    k_q = asyncio.Queue()
    o_q = asyncio.Queue()

    async def run_klines():
        async for raw in kproc.retrieve_feed():
            k_q.put_nowait(kproc.process_feed(raw))

    async def run_orderbook():
        async for raw in oproc.retrieve_feed():
            o_q.put_nowait(oproc.process_feed(raw))

    asyncio.create_task(run_klines())
    asyncio.create_task(run_orderbook())

    state = {"symbol": symbol, "klines": {}, "orderbook": {}}

    while True:
        # wait for whichever feed updates first
        k_task = asyncio.create_task(k_q.get())
        o_task = asyncio.create_task(o_q.get())

        done, pending = await asyncio.wait(
            {k_task, o_task},
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()


        for task in done:
            data = task.result()
            if "open" in data:
                state["klines"] = data
            else:
                state["orderbook"] = data

        print(json.dumps(state, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(stream_state("BTCUSDT"))
    except KeyboardInterrupt:
        print("Shutting down...")
