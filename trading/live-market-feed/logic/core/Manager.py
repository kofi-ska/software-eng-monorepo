import asyncio
import logging
from typing import Dict, Any, Tuple
from logic.core.Registry import get_exchange_class, list_registered_exchanges
from logic.core.Config import AppConfig


class LiveFeedManager:
    """
    Runtime orchestrator for all active exchange contexts and tickers.
    """

    def __init__(self, config: AppConfig = AppConfig()):
        self.config = config
        self.exchange_contexts: Dict[str, Any] = (
            {}
        )  # key: context_id, value: ExchangeContext
        self._lock = asyncio.Lock()
        self.needs_swap = False

    def _make_context_id(self, exchange: str, market: str, feed_type: str) -> str:
        return f"{exchange.lower()}-{market.lower()}-{feed_type.lower()}"

    async def add_ticker(self, symbol: str, exchange: str, market: str, feed_type: str):
        """
        Add a ticker under a given exchange context.
        Creates context if it doesn't exist.
        """
        context_id = self._make_context_id(exchange, market, feed_type)

        # enforce concurrency cap
        if len(self.exchange_contexts) >= self.config.max_concurrent_exchanges:
            self.needs_swap = True
            logging.warning("Reached max concurrent exchanges.")
            return

        # create or fetch exchange class
        exchange_cls = get_exchange_class(exchange)
        if not exchange_cls:
            raise ValueError(f"Exchange '{exchange}' not registered.")

        if context_id not in self.exchange_contexts:
            self.exchange_contexts[context_id] = exchange_cls(market, feed_type)

        ctx = self.exchange_contexts[context_id]
        await ctx.add_symbol(symbol)
        logging.info(
            f"[{exchange.upper()}] Added ticker {symbol} ({market}, {feed_type})"
        )

    async def remove_ticker(self, symbol: str):
        """Remove ticker across all active contexts."""
        for ctx in self.exchange_contexts.values():
            await ctx.remove_symbol(symbol)

    def list_contexts(self):
        """Return list of all active exchange contexts."""
        return list(self.exchange_contexts.keys())

    def get_context(self, context_id: str):
        """Get a specific exchange context."""
        return self.exchange_contexts.get(context_id)

    def swap_exchange(self, remove_id: str, add_tuple: Tuple[str, str, str, str]):
        """Replace an active exchange context with a new one."""
        if remove_id in self.exchange_contexts:
            del self.exchange_contexts[remove_id]
            logging.info(f"Removed context {remove_id}")

        exchange, market, feed_type, symbol = add_tuple
        asyncio.create_task(self.add_ticker(symbol, exchange, market, feed_type))

    async def shutdown(self):
        """Gracefully close all contexts and feeds."""
        for ctx_id, ctx in self.exchange_contexts.items():
            await ctx.close()
        self.exchange_contexts.clear()
        logging.info("Shut down all exchange contexts.")
