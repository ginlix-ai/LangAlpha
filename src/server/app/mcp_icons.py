"""The mark a server, plugin or catalog entry names, resolved on this origin.

The URLs it serves are built by ``services/icon_urls``, which mints the handle
this route resolves.

Endpoints:
- GET /api/v1/mcp/server-icons/{handle}
"""

from __future__ import annotations

from fastapi import APIRouter, Response

from src.server.services.brand_icons import icon_response_for_handle
from src.server.services.icon_urls import ICON_ROUTE

router = APIRouter(tags=["MCP Catalog"])


@router.get(f"{ICON_ROUTE}/{{handle}}")
async def get_server_icon(handle: str) -> Response:
    """The mark a server, plugin or catalog entry named, proxied.

    Unauthenticated for the same reason the brokerage route is: an ``<img>``
    cannot carry a bearer token, and there is nothing here to authenticate
    anyway. The handle is the whole access control. It is minted only while
    an authenticated listing names a source (a user's own server or plugin, a
    shipped bundle, an entry of the configured catalog), so the set of
    resolvable sources is exactly the set those declared, and a route that
    took the URL outright would fetch whatever any caller named.

    What comes back is bytes we fetched, never a redirect to the server's own
    address: resolving on the host means one fetch serves everyone, instead of
    every render of the page telling a third party who is looking.
    """
    return await icon_response_for_handle(handle)
