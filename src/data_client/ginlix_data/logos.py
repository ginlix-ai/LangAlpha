"""The data service's logo route, fetched without this server's credential.

The route is public, and the relay that calls it serves anyone, so the request
carries nothing the shared client would add: no service token, no user. What an
anonymous caller can cost stays bounded by the data service alone.
"""

from __future__ import annotations

import httpx

from .v2_routes import path_segment

_http: httpx.AsyncClient | None = None


def _client() -> httpx.AsyncClient:
    global _http
    if _http is None:
        from src.config.settings import GINLIX_DATA_URL

        # Under the data service's own 20 s resolution deadline, so its 503 arrives first.
        _http = httpx.AsyncClient(base_url=GINLIX_DATA_URL.rstrip("/"), timeout=25.0)
    return _http


async def close_logo_client() -> None:
    """Close the client at shutdown; a later lifespan in the process opens a new one."""
    global _http
    if _http is not None:
        client, _http = _http, None
        await client.aclose()


async def fetch_logo(
    symbol: str, *, size: int | None, format: str | None, if_none_match: str | None
) -> httpx.Response:
    """The logo route's response as it came, status and caching headers included."""
    params = {k: v for k, v in (("size", size), ("format", format)) if v is not None}
    headers = {"If-None-Match": if_none_match} if if_none_match else {}
    return await _client().get(
        f"/api/v1/data/logos/{path_segment(symbol)}", params=params, headers=headers
    )
