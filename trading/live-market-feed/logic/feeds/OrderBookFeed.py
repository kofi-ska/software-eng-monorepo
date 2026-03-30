from typing import Dict, Any
from logic.feeds.BaseFeed import LiveMarketFeed


class OrderBookFeed(LiveMarketFeed):
    """
    Generic order book feed.
    Each exchange subclass should override `connect()` and `handle_message()`.
    """

    async def handle_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize raw WS data into best bid/ask and spread.
        Subclasses can override if their format differs.
        """
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        if not bids or not asks:
            return {}

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        spread = best_ask - best_bid

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
        }
