# logic/insights/OrderBookInsights.py

from typing import Dict, Any
from logic.insights.BaseInsights import LiveFeedInsights


class OrderBookInsights(LiveFeedInsights):
    """
    Derives live liquidity and spread insights from order book data.
    """

    def __init__(self):
        self.last_spread = None

    def update(self, feed_data: Dict[str, Any]) -> Dict[str, Any]:
        if not feed_data:
            return {}

        spread = feed_data.get("spread")
        if spread is None:
            return {}

        trend = self._get_spread_trend(spread)
        self.last_spread = spread

        # Example derived metric: liquidity imbalance
        imbalance = self._compute_imbalance(feed_data)

        return {"spread_trend": trend, "imbalance": imbalance}

    def _get_spread_trend(self, spread: float) -> str:
        if self.last_spread is None:
            return "neutral"
        if spread < self.last_spread:
            return "tightening"
        elif spread > self.last_spread:
            return "widening"
        return "steady"

    def _compute_imbalance(self, feed_data: Dict[str, Any]) -> float:
        """
        Placeholder — real versions can use bid/ask volume.
        Here, just simulate a simple metric if data present.
        """
        bid_vol = feed_data.get("bid_volume", 1.0)
        ask_vol = feed_data.get("ask_volume", 1.0)
        total = bid_vol + ask_vol
        return (bid_vol - ask_vol) / total if total else 0.0
