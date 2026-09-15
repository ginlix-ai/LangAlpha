"""The mark an MCP server declares for itself, resolved on this origin.

One route and the builder for its URL, together because the path shape is the
whole contract between them: a handle is minted while listing a user's own
servers, and the route resolves only handles that were minted.

Endpoints:
- GET /api/v1/mcp/server-icons/{handle}
"""

from __future__ import annotations

from fastapi import APIRouter, Response

from src.server.services.brand_icons import (
    icon_response_for_handle,
    publish_icon_source,
)
from src.server.services.mcp_identity import icon_source

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])


async def icon_url(server_info: dict | None) -> str | None:
    """This origin's path to the mark a server's handshake named, if any."""
    source = icon_source(server_info)
    if source is None:
        return None
    handle = await publish_icon_source(source)
    return None if handle is None else f"/api/v1/mcp/server-icons/{handle}"


@router.get("/server-icons/{handle}")
async def get_server_icon(handle: str) -> Response:
    """The mark an MCP server declared for itself, proxied.

    Unauthenticated for the same reason the brokerage route is: an ``<img>``
    cannot carry a bearer token, and there is nothing here to authenticate
    anyway. The handle is the whole access control. It is minted only while
    listing a user's own servers, so the set of resolvable sources is exactly
    the set some user's server declared, and a route that took the URL outright
    would fetch whatever any caller named.

    What comes back is bytes we fetched, never a redirect to the server's own
    address: resolving on the host means one fetch serves everyone, instead of
    every render of the page telling a third party who is looking.
    """
    return await icon_response_for_handle(handle)
