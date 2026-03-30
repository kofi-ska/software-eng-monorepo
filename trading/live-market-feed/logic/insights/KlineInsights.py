# logic/insights/KlineInsights.py

from typing import Dict, Any
from logic.insights.BaseInsights import LiveFeedInsights


class KlineInsights(LiveFeedInsights):
    """
    Simple trend and volatility analytics from OHLCV data.
    """

    def __init__(self):
        self.last_close = None

    def update(self, feed_data: Dict[str, Any]) -> Dict[str, Any]:
        if not feed_data:
            return {}

        close = feed_data.get("close")
        high = feed_data.get("high")
        low = feed_data.get("low")

        if close is None or high is None or low is None:
            return {}

        trend = self._compute_trend(close)
        volatility = self._compute_volatility(high, low)

        self.last_close = close

        return {"trend": trend, "volatility": volatility}

    def _compute_trend(self, close: float) -> str:
        if self.last_close is None:
            return "neutral"
        if close > self.last_close:
            return "bullish"
        elif close < self.last_close:
            return "bearish"
        return "flat"

    def _compute_volatility(self, high: float, low: float) -> float:
        if low == 0:
            return 0.0
        return round((high - low) / low, 4)
