from abc import ABC, abstractmethod
from typing import Dict


class TradePositionOutput(ABC):
    """
    Abstract base interface for any trade output sink.
    Ensures all output types follow the same contract.
    """

    @abstractmethod
    def write_position(self, trade: Dict) -> None:
        """Write a trade record (dict) to the output sink."""
        pass

    def flush(self) -> None:
        """Optional: clean up, commit, or close handles."""
        pass

    def health_check(self) -> bool:
        """Optional: verify output target is accessible."""
        return True
