"""
FMP (Financial Modeling Prep) data source module.
"""

from typing import Optional
import asyncio

from .fmp_client import FMPClient

__all__ = ["FMPClient", "get_fmp_client", "close_fmp_client"]

# Async singleton for FMPClient
_fmp_client: Optional[FMPClient] = None
_client_lock = asyncio.Lock()


async def get_fmp_client() -> FMPClient:
    """
    Get or create a singleton FMPClient instance (async-safe).

    Uses asyncio.Lock to ensure thread-safe initialization in async context.
    The client is lazily initialized on first use.

    Returns:
        FMPClient: The singleton FMP client instance
    """
    global _fmp_client
    async with _client_lock:
        if _fmp_client is None:
            _fmp_client = FMPClient()
        return _fmp_client


async def close_fmp_client() -> None:
    """
    Close the singleton FMPClient (call on shutdown).

    Should be called during application shutdown to properly close
    the HTTP client and release resources.
    """
    global _fmp_client
    async with _client_lock:
        if _fmp_client is not None:
            await _fmp_client.close()
            _fmp_client = None
