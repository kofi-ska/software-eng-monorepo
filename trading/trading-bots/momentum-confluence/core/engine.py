# core/engine.py

import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from core.rules import RuleEngine
from core.decision import DecisionEngine, TradeDecision
from outputs.base import TradePositionOutput


# -------------------------------------------------------------------------
# Feed Layer (extended)
# -------------------------------------------------------------------------
class TradeFeed(ABC):
    """Abstract interface for market data sources."""

    @abstractmethod
    async def fetch_klines(self, pair: str, interval: str = "1m", limit: int = 500) -> List[Dict[str, Any]]:
        """
        Return list of candle dicts (oldest..newest):
        [{'open':..,'high':..,'low':..,'close':..,'volume':..}, ...]
        """
        raise NotImplementedError

    @abstractmethod
    async def fetch_orderbook(self, pair: str, depth: int = 100) -> Dict[str, Any]:
        """Return dict with 'bids' and 'asks' lists: [[price, qty], ...]"""
        raise NotImplementedError

    async def fetch_24h_ticker(self, pair: str) -> Dict[str, Any]:
        """
        Optional: return 24h ticker / stats (volume, price change, open interest if available).
        Default implementation returns empty dict; concrete feeds may override.
        """
        return {}


class BinanceFuturesTradeFeed(TradeFeed):
    """Concrete feed for Binance Futures REST endpoints."""

    BASE_URL = "https://fapi.binance.com/fapi/v1"

    def __init__(self, session):
        self.session = session

    async def fetch_klines(self, pair: str, interval: str = "1m", limit: int = 500):
        url = f"{self.BASE_URL}/klines?symbol={pair}&interval={interval}&limit={limit}"
        async with self.session.get(url) as resp:
            data = await resp.json()
        # convert to expected dict format (oldest..newest)
        klines = [
            {
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
            for k in data
        ]
        return klines

    async def fetch_orderbook(self, pair: str, depth: int = 100):
        url = f"{self.BASE_URL}/depth?symbol={pair}&limit={depth}"
        async with self.session.get(url) as resp:
            data = await resp.json()
        return {
            "bids": [[float(b[0]), float(b[1])] for b in data.get("bids", [])],
            "asks": [[float(a[0]), float(a[1])] for a in data.get("asks", [])],
        }

    async def fetch_24h_ticker(self, pair: str) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/ticker/24hr?symbol={pair}"
        async with self.session.get(url) as resp:
            data = await resp.json()
        # normalize critical stats
        try:
            return {
                "priceChangePercent": float(data.get("priceChangePercent", 0.0)),
                "volume": float(data.get("volume", 0.0)),
                "quoteVolume": float(data.get("quoteVolume", 0.0)),
                "openPrice": float(data.get("openPrice", 0.0)),
                "lastPrice": float(data.get("lastPrice", 0.0)),
            }
        except Exception:
            return {}


# -------------------------------------------------------------------------
# Engine Orchestration (multi-interval lookback)
# -------------------------------------------------------------------------
class TradingEngine:
    """
    Orchestrates per-pair evaluation:
    - fetch multi-interval klines + orderbook + 24h stats
    - run rules (which expect kline_data dict and market_stats)
    - run decision engine and output any trade ideas
    """

    DEFAULT_INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d"]
    DEFAULT_LIMITS = {
        "1m": 500,
        "5m": 500,
        "15m": 500,
        "1h": 500,
        "4h": 500,
        "1d": 365,
    }

    def __init__(self, feed: TradeFeed, output: TradePositionOutput, concurrency: int = 6):
        self.feed = feed
        self.output = output
        # semaphore to limit concurrent per-pair evaluation
        self._sem = asyncio.Semaphore(concurrency)

    async def _fetch_all_intervals(self, pair: str, intervals: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch klines for all intervals in parallel and return mapping interval->klines.
        """
        intervals = intervals or self.DEFAULT_INTERVALS
        tasks = []
        for itv in intervals:
            limit = self.DEFAULT_LIMITS.get(itv, 500)
            tasks.append(asyncio.create_task(self.feed.fetch_klines(pair, interval=itv, limit=limit)))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        kline_map: Dict[str, List[Dict[str, Any]]] = {}
        for itv, res in zip(intervals, results):
            if isinstance(res, Exception):
                # keep empty list on error (rules will handle insufficient data)
                kline_map[itv] = []
            else:
                kline_map[itv] = res
        return kline_map

    async def _fetch_orderbook_and_stats(self, pair: str, depth: int = 100):
        """
        Fetch orderbook + optional 24h stats in parallel.
        """
        ob_task = asyncio.create_task(self.feed.fetch_orderbook(pair, depth=depth))
        stats_task = asyncio.create_task(self.feed.fetch_24h_ticker(pair))
        res_ob, res_stats = await asyncio.gather(ob_task, stats_task, return_exceptions=True)

        orderbook = res_ob if not isinstance(res_ob, Exception) else {"bids": [], "asks": []}
        market_stats = res_stats if not isinstance(res_stats, Exception) else {}
        return orderbook, market_stats

    async def evaluate_pair(self, pair: str, intervals: Optional[List[str]] = None) -> None:
        """Evaluate a single ticker end-to-end with deeper lookback/context."""
        async with self._sem:
            try:
                # parallel fetch klines for multiple intervals
                kline_data = await self._fetch_all_intervals(pair, intervals=intervals)
                orderbook, market_stats = await self._fetch_orderbook_and_stats(pair, depth=100)

                # instantiate rule engine with rich context
                rule_engine = RuleEngine(kline_data=kline_data, orderbook=orderbook, market_stats=market_stats)
                signals = rule_engine.evaluate_all(require_liquidity=True, regime_protect=True)

                # determine latest price: prefer 1m close then fallbacks
                latest_price = None
                for preferred in ("1m", "5m", "15m", "1h", "4h", "1d"):
                    kl = kline_data.get(preferred) or []
                    if kl:
                        latest_price = kl[-1]["close"]
                        break
                if latest_price is None:
                    # nothing to do
                    return

                decision_engine = DecisionEngine(pair, latest_price)
                trade: Optional[TradeDecision] = decision_engine.build_decision(signals)

                if trade:
                    self.output.write_position(trade.as_dict())

            except Exception as exc:
                # simple logging - you can replace with structured logger
                print(f"[{pair}] evaluation error: {repr(exc)}")

    async def run(self, tickers: List[str], intervals: Optional[List[str]] = None) -> None:
        """
        Evaluate all tickers concurrently (bounded by semaphore).
        """
        intervals = intervals or self.DEFAULT_INTERVALS
        tasks = [asyncio.create_task(self.evaluate_pair(t)) for t in tickers]
        # wait for all to finish
        await asyncio.gather(*tasks, return_exceptions=True)
