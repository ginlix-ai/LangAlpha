"""Plugin marketplace API: the catalog the Plugins page's Discover tab lists.

Endpoints:
- GET /api/v1/marketplace   the configured catalog, each entry with this
                            user's installed plugin from it and whether
                            the catalog holds a newer one

A prefix of its own rather than a path under /api/v1/plugins, where any GET
would sit beside GET /api/v1/plugins/{name} and shadow a plugin of the same
name. Installing an entry is the ordinary POST /api/v1/plugins with the
entry's ``source_url``, and updating one to it is POST
/api/v1/plugins/{name}/update with the same.
"""

import logging

from fastapi import APIRouter, HTTPException

from src.server.database.plugins import list_plugins
from src.server.models.marketplace import MarketplaceResponse
from src.server.services.icon_urls import source_icon_urls
from src.server.services.plugins import PluginFatal, load_marketplace
from src.server.services.plugins.marketplace import installed_from
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/marketplace", tags=["Plugins"])


@router.get("", response_model=MarketplaceResponse)
@handle_api_exceptions("load the plugin marketplace", logger)
async def get_marketplace(user_id: CurrentUserId) -> MarketplaceResponse:
    """The catalog's entries, each naming this user's plugin installed from
    it. A catalog that cannot be read is the upstream's failure, not the
    request's, so it answers 502."""
    try:
        catalog = await load_marketplace()
    except PluginFatal as e:
        raise HTTPException(status_code=502, detail=str(e))
    if catalog is None:
        return MarketplaceResponse(configured=False)
    installed = installed_from(catalog, await list_plugins(user_id))
    icons = await source_icon_urls(catalog.icons.values())
    entries = []
    for entry in catalog.entries:
        mine = installed.get(entry.name)
        source = catalog.icons.get(entry.name)
        entries.append(
            entry.model_copy(
                update={
                    "plugin": mine.plugin if mine else None,
                    "installed": mine is not None,
                    "update_available": bool(mine and mine.update_available),
                    "icon_url": icons.get(source) if source else None,
                }
            )
        )
    return MarketplaceResponse(
        configured=True,
        name=catalog.name,
        description=catalog.description,
        url=catalog.url,
        vocabulary=catalog.vocabulary,
        entries=entries,
    )
