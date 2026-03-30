from __future__ import annotations

import json
import logging
from typing import Any, Dict

import tornado.web
from pydantic import ValidationError

from services.api.tornado.models.asset import AssetRegisterRequest

logger = logging.getLogger(__name__)


class BaseHandler(tornado.web.RequestHandler):
    """Base handler with common helpers."""

    def write_error_response(self, status: int, message: Any) -> None:
        """
        Write a JSON error response and log failure.
        """
        logger.warning("HTTP %s failure: %s", status, message)
        self.set_status(status)
        self.write({"error": message})

    def write_success(self, payload: dict, status: int = 200) -> None:
        """Write a successful JSON response"""
        self.set_status(status)
        self.write(payload)


class HealthHandler(BaseHandler):
    """Healthcheck handler"""

    def get(self) -> None:
        logger.info("GET /health")
        logger.info("Healthcheck successful")
        self.write_success({"status": "ok"})


class AssetHandler(BaseHandler):
    """Register, view, or deregister an asset"""

    def get(self) -> None:
        """View a live asset snapshot"""
        asset_type = self.get_query_argument("asset_type", None)
        symbol = self.get_query_argument("symbol", None)
        utc_offset = float(self.get_query_argument("utc_offset", 0))

        logger.info(
            "GET /asset asset_type=%s symbol=%s utc_offset=%s",
            asset_type,
            symbol,
            utc_offset,
        )

        if not asset_type or not symbol:
            self.write_error_response(
                400, "asset_type and symbol are required"
            )
            return

        logger.info(
            "Asset snapshot returned successfully asset_type=%s symbol=%s",
            asset_type,
            symbol,
        )

        self.write_success(
            {
                "asset_type": asset_type,
                "symbol": symbol,
                "utc_offset": utc_offset,
                "data": None,
            }
        )

    def post(self) -> None:
        """Register an asset and start background ingestion"""
        logger.info("POST /asset")

        try:
            payload: Dict[str, Any] = json.loads(self.request.body or b"{}")
            logger.debug("POST /asset payload=%s", payload)
            req = AssetRegisterRequest(**payload)
        except json.JSONDecodeError:
            self.write_error_response(400, "invalid JSON")
            return
        except ValidationError as exc:
            self.write_error_response(400, exc.errors())
            return

        logger.info(
            "Registering asset asset_type=%s symbol=%s",
            req.asset_type,
            req.symbol,
        )

        logger.info(
            "Asset registered successfully asset_type=%s symbol=%s",
            req.asset_type,
            req.symbol,
        )

        self.write_success(
            {
                "status": "active",
                "asset_type": req.asset_type,
                "symbol": req.symbol,
            }
        )

    def delete(self) -> None:
        """Deregister an asset and stop background ingestion"""
        asset_type = self.get_query_argument("asset_type", None)
        symbol = self.get_query_argument("symbol", None)

        logger.info(
            "DELETE /asset asset_type=%s symbol=%s",
            asset_type,
            symbol,
        )

        if not asset_type or not symbol:
            self.write_error_response(
                400, "asset_type and symbol are required"
            )
            return

        logger.info(
            "Asset deregistered successfully asset_type=%s symbol=%s",
            asset_type,
            symbol,
        )

        self.set_status(204)


class AssetSymbolsHandler(BaseHandler):
    """List active assets"""

    def get(self) -> None:
        """List active symbols for an asset type"""
        asset_type = self.get_query_argument("asset_type", None)

        logger.info(
            "GET /asset/symbols asset_type=%s",
            asset_type,
        )

        if not asset_type:
            self.write_error_response(400, "asset_type is required")
            return

        logger.info(
            "Asset symbols listed successfully asset_type=%s",
            asset_type,
        )

        self.write_success(
            {
                "asset_type": asset_type,
                "symbols": [],
            }
        )
