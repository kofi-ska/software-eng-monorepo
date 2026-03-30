# logic/insights/BaseInsights.py

import abc
from typing import Dict, Any


class LiveFeedInsights(abc.ABC):
    """
    Abstract base class for all live insight processors.
    Feeds call .update() with each normalized message,
    and receive derived analytics.
    """

    @abc.abstractmethod
    def update(self, feed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Compute derived insights from a single feed message."""
        pass
