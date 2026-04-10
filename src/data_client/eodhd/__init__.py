"""
EODHD (eodhistoricaldata.com) data source module.
"""

from typing import Optional
import asyncio
from contextlib import asynccontextmanager

from .eodhd_client import EODHDClient

__all__ = ["EODHDClient", "get_eodhd_client", "close_eodhd_client", "eodhd_lifespan"]

_eodhd_client: Optional[EODHDClient] = None
_client_lock = asyncio.Lock()


async def get_eodhd_client() -> EODHDClient:
    """Get or create a singleton EODHDClient instance (async-safe)."""
    global _eodhd_client
    async with _client_lock:
        if _eodhd_client is None:
            _eodhd_client = EODHDClient()
        return _eodhd_client


async def close_eodhd_client() -> None:
    """Close the singleton EODHDClient (call on shutdown)."""
    global _eodhd_client
    async with _client_lock:
        if _eodhd_client is not None:
            await _eodhd_client.close()
            _eodhd_client = None


@asynccontextmanager
async def eodhd_lifespan(app):
    """Lifespan that closes the shared EODHDClient on shutdown."""
    yield
    await close_eodhd_client()
