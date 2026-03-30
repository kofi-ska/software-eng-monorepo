
from typing import Dict, Any
from logic.feeds.BaseFeed import LiveMarketFeed


class KlinesFeed(LiveMarketFeed):
    """
    Generic candlestick (OHLCV) feed.
    Each exchange subclass should override `connect()` if needed.
    """

    async def handle_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a kline/candle message.
        Expected structure: { open, high, low, close, volume }
        """
        try:
            o = float(msg["open"])
            h = float(msg["high"])
            l = float(msg["low"])
            c = float(msg["close"])
            v = float(msg.get("volume", 0.0))
        except (KeyError, TypeError, ValueError):
            return {}

        return {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
        }
