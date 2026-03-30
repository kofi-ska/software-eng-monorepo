from dataclasses import dataclass


@dataclass
class AppConfig:
    """
    Global runtime config.
    Can be extended later for keys, rate limits, etc.
    """

    max_concurrent_exchanges: int = 7
    default_exchange: str = "binance"
    default_market: str = "futures"
    default_feed_type: str = "orderbook"
    default_symbol: str = "BTCUSDT"
