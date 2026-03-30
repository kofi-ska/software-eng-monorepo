import csv
import os
from datetime import datetime
from typing import Dict
from outputs.base import TradePositionOutput


class CSVTradeOutput(TradePositionOutput):
    """
    Writes trade decisions to a local CSV file (append-only).
    This acts as the main KISS persistence layer.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        # Initialize file with headers if empty
        if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
            self._write_headers()

    def _write_headers(self):
        headers = [
            "timestamp",
            "pair",
            "direction",
            "entry",
            "stop",
            "target",
            "score",
            "rationale",
            "status",
        ]
        with open(self.filepath, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

    def write_position(self, trade: Dict) -> None:
        """
        Append a new trade decision to the CSV file.
        """
        trade_row = [
            trade.get("timestamp", datetime.utcnow().isoformat()),
            trade.get("pair"),
            trade.get("direction"),
            trade.get("entry"),
            trade.get("stop"),
            trade.get("target"),
            trade.get("score"),
            trade.get("rationale"),
            "OPEN",
        ]
        with open(self.filepath, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(trade_row)
        print(f"[CSV] Logged trade for {trade['pair']} → {trade['direction']}")
