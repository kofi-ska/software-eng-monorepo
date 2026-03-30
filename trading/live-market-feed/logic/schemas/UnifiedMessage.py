
from dataclasses import dataclass, field
from typing import Any, Dict
import time


@dataclass
class UnifiedMessage:
    """Standardized structure for all feed + insight outputs."""

    exchange: str  # e.g. "binance"
    market: str  # e.g. "futures", "spot"
    symbol: str  # e.g. "BTCUSDT"
    feed: Dict[str, Any]  # normalized feed data
    insights: Dict[str, Any]  # analysis results
    timestamp: float = field(default_factory=lambda: time.time())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "exchange": self.exchange,
            "market": self.market,
            "symbol": self.symbol,
            "feed": self.feed,
            "insights": self.insights,
            "timestamp": self.timestamp,
        }
