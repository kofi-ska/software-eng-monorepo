import abc
import asyncio
import logging
from typing import Any, Dict, Optional
from logic.schemas.UnifiedMessage import UnifiedMessage


class LiveMarketFeed(abc.ABC):
    """
    Abstract base for all live market feeds (e.g. order book, klines).
    Handles the event loop and connection lifecycle.
    """

    def __init__(self, exchange: str, market: str, feed_type: str, symbol: str):
        self.exchange = exchange
        self.market = market
        self.feed_type = feed_type
        self.symbol = symbol.upper()
        self.ws: Optional[Any] = None
        self._running = False

    @abc.abstractmethod
    async def connect(self):
        """Establish WebSocket connection to the exchange."""
        pass

    @abc.abstractmethod
    async def handle_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw exchange message into unified feed data."""
        pass


    async def run(self, insights=None):
        self._running = True
        await self.connect()
        logging.info(f"[{self.exchange.upper()}] Connected feed for {self.symbol}")

        try:
            async for msg in self._stream_messages():
                normalized = await self.handle_message(msg)
                if not normalized:
                    continue

                if insights:
                    insight_data = insights.update(normalized)
                    if insight_data:
                        logging.debug(
                            f"[{self.exchange.upper()}] Insights updated for {self.symbol}"
                        )
                else:
                    insight_data = {}

                unified = UnifiedMessage(
                    exchange=self.exchange,
                    market=self.market,
                    symbol=self.symbol,
                    feed={"type": self.feed_type, "data": normalized},
                    insights=insight_data or {},
                ).to_dict()

        except asyncio.CancelledError:
            logging.info(f"[{self.exchange.upper()}] Feed cancelled for {self.symbol}")
        except Exception as e:
            logging.error(f"[{self.exchange.upper()}] Feed error for {self.symbol}: {e}")
        finally:
            await self.close()

    async def _stream_messages(self):
        """
        Placeholder: simulate WS iteration.
        Real subclasses will override with `async for msg in self.ws: ...`
        """
        while self._running:
            await asyncio.sleep(1)
            yield {"type": "heartbeat"}  # dummy message

    async def close(self):
        """Close WebSocket and stop feed."""
        self._running = False
        if self.ws:
            await self.ws.close()
        logging.info(f"[{self.exchange.upper()}] Closed feed for {self.symbol}")
