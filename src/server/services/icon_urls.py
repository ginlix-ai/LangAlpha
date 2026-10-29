"""This origin's URL for the mark a server, plugin or catalog entry names.

A handle is minted here, while an authenticated listing names a source: a
user's own server or plugin, a bundle this build ships, or an entry of the
configured catalog. The icon route resolves only handles that were minted, so
the path shape below is the whole contract between the two.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from src.server.services.brand_icons import publish_icon_source
from src.server.services.mcp_identity import icon_source

#: Where the icon route serves a minted handle.
ICON_ROUTE = "/api/v1/mcp/server-icons"


async def source_icon_url(source: str | None) -> str | None:
    """This origin's path to the mark ``source`` names, if it names one."""
    if source is None:
        return None
    handle = await publish_icon_source(source)
    return None if handle is None else f"{ICON_ROUTE}/{handle}"


async def source_icon_urls(sources: Iterable[str]) -> dict[str, str]:
    """Each distinct source's path, minted together: a catalog lists hundreds,
    and many entries share one mark (a publisher's avatar)."""
    unique = list(dict.fromkeys(sources))
    urls = await asyncio.gather(*(source_icon_url(s) for s in unique))
    return {s: url for s, url in zip(unique, urls) if url is not None}


async def icon_url(server_info: dict | None) -> str | None:
    """This origin's path to the mark a server's handshake named, if any."""
    return await source_icon_url(icon_source(server_info))
