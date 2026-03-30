from __future__ import annotations

import logging
import os
from typing import NoReturn

import tornado.ioloop
import tornado.web
from dotenv import load_dotenv

from services.api.tornado.routes import get_routes

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)

def _parse_bool(value: str | None, default: bool = True) -> bool:
    """Parse boolean-like environment variables safely."""
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


API_DEBUG_STATUS = _parse_bool(os.getenv("API_DEBUG_STATUS"), default=True)
API_PORT = int(os.getenv("API_PORT", "8888"))


def make_app() -> tornado.web.Application:
    """Create and configure the Tornado application."""
    logger.debug("Creating Tornado application (debug=%s)", API_DEBUG_STATUS)

    return tornado.web.Application(
        get_routes(),
        debug=API_DEBUG_STATUS,
    )


def main() -> NoReturn:
    """Start the Tornado IOLoop."""
    logger.info("Starting Tornado API on port %s", API_PORT)

    app = make_app()
    app.listen(API_PORT)

    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        logger.info("Tornado API shutdown requested")
    finally:
        logger.info("Tornado API stopped")


if __name__ == "__main__":
    main()
