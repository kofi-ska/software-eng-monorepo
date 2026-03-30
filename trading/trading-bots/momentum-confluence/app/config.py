import os
from dataclasses import dataclass

@dataclass
class Config:
    """Runtime configuration for the trading engine."""
    TICKERS_FILE: str = os.getenv("TICKERS_FILE", "data/tickers.txt")
    CSV_FILE: str = os.getenv("CSV_FILE", "data/trade_positions.csv")
    BINANCE_BASE_URL: str = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com/fapi/v1")
    INTERVAL: str = os.getenv("INTERVAL", "1m")
    DEPTH: int = int(os.getenv("DEPTH", 50))
    SCORE_THRESHOLD: float = float(os.getenv("SCORE_THRESHOLD", 0.5))
    LOGGING: bool = os.getenv("LOGGING", "true").lower() == "true"
    TIMEOUT: int = int(os.getenv("TIMEOUT", 10))
