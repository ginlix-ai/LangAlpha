"""ginlix-data REST client package.

Provides :class:`GinlixDataClient` for fetching aggregates from the
ginlix-data market data proxy service.
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional

from .client import GinlixDataClient
from .mcp_client import DAILY_INTERVALS, GinlixMCPClient, split_date_time

__all__ = [
    "DAILY_INTERVALS",
    "GinlixDataClient",
    "GinlixMCPClient",
    "close_ginlix_data_client",
    "close_ginlix_mcp_client",
    "get_ginlix_data_client",
    "get_ginlix_mcp_client",
    "split_date_time",
]

# -- Host-side singleton (service-token auth) --------------------------------

_client: Optional[GinlixDataClient] = None
_lock = asyncio.Lock()


async def get_ginlix_data_client() -> GinlixDataClient:
    """Get or create a singleton :class:`GinlixDataClient`."""
    global _client
    async with _lock:
        if _client is None:
            from src.config.settings import GINLIX_DATA_URL

            service_token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
            _client = GinlixDataClient(base_url=GINLIX_DATA_URL, service_token=service_token)
        return _client


async def close_ginlix_data_client() -> None:
    """Close the singleton client (call on shutdown)."""
    global _client
    async with _lock:
        if _client is not None:
            await _client.close()
            _client = None


# -- Sandbox-side singleton (OAuth token-file auth) --------------------------

_mcp_client: Optional[GinlixMCPClient] = None


def get_ginlix_mcp_client() -> GinlixMCPClient:
    """Get or create a singleton :class:`GinlixMCPClient`."""
    global _mcp_client
    if _mcp_client is None:
        _mcp_client = GinlixMCPClient()
    return _mcp_client


async def close_ginlix_mcp_client() -> None:
    """Close the singleton MCP client (call on shutdown)."""
    global _mcp_client
    if _mcp_client is not None:
        await _mcp_client.close()
        _mcp_client = None
