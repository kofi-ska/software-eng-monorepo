# logic/exchanges/BinanceFeed.py

import json
import asyncio
import logging
import websockets
from typing import Dict, Any

from logic.core.Registry import register_exchange
from logic.feeds.OrderBookFeed import OrderBookFeed
from logic.insights.OrderBookInsights import OrderBookInsights


@register_exchange("binance")
class BinanceFeed:
    """
    Binance exchange adapter.
    Dynamically creates live feeds for futures or spot markets.
    """

    FUTURES_URL = "wss://fstream.binance.com/ws"
    SPOT_URL = "wss://stream.binance.com/ws"

    def __init__(self, market: str, feed_type: str):
        self.market = market
        self.feed_type = feed_type
        self.symbol_feeds: Dict[str, asyncio.Task] = {}
        self.insight_class = OrderBookInsights  # can later map by feed_type

    async def add_symbol(self, symbol: str):
        """
        Start a live feed for the given symbol.
        """
        if symbol in self.symbol_feeds:
            logging.info(f"[BINANCE] Feed already active for {symbol}")
            return

        feed = BinanceOrderBookFeed(
            exchange="binance",
            market=self.market,
            feed_type=self.feed_type,
            symbol=symbol,
        )
        insights = self.insight_class()
        task = asyncio.create_task(feed.run(insights))
        self.symbol_feeds[symbol] = task
        logging.info(f"[BINANCE] Started {self.feed_type} feed for {symbol}")

    async def remove_symbol(self, symbol: str):
        """
        Stop a live feed for a given symbol.
        """
        task = self.symbol_feeds.pop(symbol, None)
        if task:
            task.cancel()
            logging.info(f"[BINANCE] Stopped feed for {symbol}")

    async def close(self):
        """
        Gracefully stop all feeds for this exchange context.
        """
        for sym, task in list(self.symbol_feeds.items()):
            task.cancel()
            logging.info(f"[BINANCE] Cancelled {sym}")
        self.symbol_feeds.clear()


class BinanceOrderBookFeed(OrderBookFeed):
    """
    Binance order book feed for either spot or futures markets.
    """

    def __init__(self, exchange: str, market: str, feed_type: str, symbol: str):
        super().__init__(exchange, market, feed_type, symbol)
        self.url = (
            f"{BinanceFeed.FUTURES_URL}/{symbol.lower()}@depth@100ms"
            if market == "futures"
            else f"{BinanceFeed.SPOT_URL}/{symbol.lower()}@depth@100ms"
        )

    async def connect(self):
        """
        Establish WebSocket connection to Binance and subscribe to the symbol stream.
        """
        self.ws = await websockets.connect(self.url)
        logging.info(f"[BINANCE] Connected WS for {self.symbol} ({self.market})")

    async def _stream_messages(self):
        """
        Stream messages from Binance WS.
        """
        while self._running and self.ws:
            try:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=30)
                yield json.loads(msg)
            except asyncio.TimeoutError:
                logging.warning(f"[BINANCE] Timeout waiting for {self.symbol}")
                continue
            except Exception as e:
                logging.error(f"[BINANCE] Error: {e}")
                break

    async def handle_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Binance depth update message into normalized structure.
        """
        try:
            bids = msg.get("bids") or []
            asks = msg.get("asks") or []
            if not bids or not asks:
                return {}

            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            spread = round(best_ask - best_bid, 2)

            return {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "bid_volume": sum(float(b[1]) for b in bids[:5]),
                "ask_volume": sum(float(a[1]) for a in asks[:5]),
            }
        except Exception as e:
            logging.error(f"[BINANCE] Message parse error: {e}")
            return {}
