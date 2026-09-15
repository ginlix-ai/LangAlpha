"""Brokerage connectors: the ones this build ships, off until switched on.

The list is static and user-independent; what the user owns is a catalog row,
created inert the first time they turn one on and toggled through the same
apply every other switch on the page uses. These are the rows that can place
orders, so a weaker disable than the server beside them is the last thing they
should have.

Endpoints:
- GET   /api/v1/mcp/brokerages
- PATCH /api/v1/mcp/brokerages/{name}/enabled
- GET   /api/v1/mcp/brokerages/{name}/icon
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Response

from src.server.app.mcp_catalog import (
    apply_catalog_enabled,
    decorated,
    oauth_for_server,
)
from src.server.database.mcp_servers import (
    create_catalog_server,
    get_catalog_server,
)
from src.server.models.mcp_server import (
    BrokerageList,
    CatalogServer,
    EnabledInput,
    McpServerInput,
    brokerage_to_response,
)
from src.server.services.brand_icons import icon_response
from src.server.services.brokerages import (
    BROKERAGES,
    Brokerage,
    brokerage_by_name,
)
from src.server.services.mcp_config import builtin_names
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP Catalog"])


@router.get("/brokerages")
@handle_api_exceptions("list brokerage connectors", logger)
async def list_brokerages(user_id: CurrentUserId) -> BrokerageList:
    """The brokerage connectors this build ships.

    Static and user-independent: whether one is configured is answered by the
    catalog list, which the page already holds and joins on ``name``. Behind
    the same auth as the rest of the router regardless: its only reader is the
    page, which is holding a token already, so there is nothing an exception
    here would buy.
    """
    return BrokerageList(brokerages=[brokerage_to_response(b) for b in BROKERAGES])


async def _create_brokerage_row(user_id: str, brokerage: Brokerage) -> None:
    """Bring a shipped brokerage into the user's catalog, inert.

    Inert and then toggled, never created live: the switch is the only thing
    that should decide a row's enabled state, and it is the one that already
    knows what each direction owes an OAuth connection.
    """
    if brokerage.name in builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{brokerage.name!r} collides with a built-in server name",
        )
    try:
        # Through the same validator every user-written row passes, so our own
        # definition cannot be the one payload that skips the URL policy. Its
        # ValidationError is a ValueError, so it answers here rather than
        # escaping the decorator as an untyped 500.
        server = McpServerInput(
            name=brokerage.name,
            transport="http",
            url=brokerage.url,
            description=brokerage.description,
        )
        await create_catalog_server(user_id, server.name, **server.to_catalog_fields())
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    logger.info(
        "[mcp_catalog] brokerage %s configured for user %s", brokerage.name, user_id
    )


@router.patch("/brokerages/{name}/enabled")
@handle_api_exceptions("toggle brokerage connector", logger)
async def set_brokerage_enabled(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> CatalogServer:
    """Turn a shipped brokerage on or off, creating its row the first time.

    One route for both, so the page never has to know whether a row exists yet
    and that also keeps it from being the thing that chooses the endpoint URL.

    An existing row is toggled and never rewritten. Once it is the user's, its
    URL is theirs to edit, and a row they built themselves under this name is
    still theirs; silently restoring our address on every enable would undo a
    deliberate edit at the moment they were only reaching for the switch.
    """
    brokerage = brokerage_by_name(name)
    if brokerage is None:
        raise HTTPException(status_code=404, detail="Unknown brokerage")

    existing = await get_catalog_server(user_id, name)
    # A plugin-owned row under a brokerage name is not the user's own edit, and
    # this route would adopt it and hand it the vendor's identity: the tab joins
    # by name, so it would be presented as this broker while Connect went to
    # whatever address the plugin chose. New installs cannot claim these names
    # any more; one installed before they were reserved still can, so refuse it
    # here rather than trusting that no such row exists. The row stays usable
    # on the Connectors tab, under the plugin that owns it.
    if existing and existing.get("plugin_id"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"{name!r} is a server installed by a plugin, so it cannot be "
                "managed as a brokerage connector. Open it on the Connectors tab."
            ),
        )

    if existing is None:
        if not body.enabled:
            raise HTTPException(
                status_code=404,
                detail=f"{name!r} is not configured, so there is nothing to disable",
            )
        await _create_brokerage_row(user_id, brokerage)

    # The same apply every other switch on this page goes through. These are the
    # rows that can place orders, so a weaker disable than the server beside them
    # is the last thing they should have.
    row, warning = await apply_catalog_enabled(user_id, name, body.enabled)
    if row is None:
        # Deleted between the read and the write.
        raise HTTPException(status_code=404, detail="MCP server not found")

    # A recreate over a name whose OAuth connection outlived the old row is
    # already connected, so read the status rather than assuming none. One row,
    # so one lookup: listing every connection to decorate a single response is
    # a second round trip that answers the same question.
    response = decorated(row, await oauth_for_server(user_id, name))
    if warning:
        response.warnings = [warning]
    return response


@router.get("/brokerages/{name}/icon")
async def get_brokerage_icon(name: str) -> Response:
    """The broker's own logo, proxied from their site.

    Unauthenticated because it has nothing to authenticate: the only input is
    a name this build ships, so the answer is the same public logo for every
    caller and no user's configuration is read to produce it. Serving it under
    the user's bearer token was never an option anyway, since an ``<img>`` cannot
    send one, and routing brand art through a fetch-to-blob just to carry a
    credential that guards nothing is machinery for its own sake.

    404 is a normal answer, not an error: a vendor may simply have no usable
    mark, and the row draws its monogram instead. It carries a cache header so
    a page full of rows does not re-ask on every render.
    """
    brokerage = brokerage_by_name(name)
    return await icon_response(brokerage.site if brokerage else None)
