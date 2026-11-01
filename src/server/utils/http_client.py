"""The process's shared client for calls to the platform's auth and quota
service, kept out of any one caller so the auth layer and the usage limits
borrow it without importing each other."""

from __future__ import annotations

import asyncio

import httpx

_http_client: httpx.AsyncClient | None = None
_http_client_lock = asyncio.Lock()


async def get_http_client() -> httpx.AsyncClient:
    """The shared client. One per process, closed at shutdown."""
    global _http_client
    async with _http_client_lock:
        if _http_client is None:
            _http_client = httpx.AsyncClient(timeout=5.0)
        return _http_client


async def close_http_client() -> None:
    """Close the shared client. Call during application shutdown."""
    global _http_client
    async with _http_client_lock:
        if _http_client is not None:
            await _http_client.aclose()
            _http_client = None
