import logging
from typing import Dict, List, Tuple, Type

import tornado.web

from services.api.tornado.controllers.asset_controller import (
    HealthHandler,
    AssetHandler,
    AssetSymbolsHandler,
)

logger = logging.getLogger(__name__)

ROUTES: Dict[str, Type[tornado.web.RequestHandler]] = {
    r"/health": HealthHandler,
    r"/asset": AssetHandler,
    r"/asset/symbols": AssetSymbolsHandler,
}


def get_routes() -> List[Tuple[str, Type[tornado.web.RequestHandler]]]:
    """Return Tornado-compatible route tuples."""
    logger.debug("Registering %d routes", len(ROUTES))
    return list(ROUTES.items())
