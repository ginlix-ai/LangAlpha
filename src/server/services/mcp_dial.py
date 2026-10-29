"""Where a catalog server is dialled, and which sign-in may go there.

A check proves a server as it dials: an edit that moves none of the dial
fields is saved without dialling, and one that moves any of them is a server
no earlier check says anything about. A sign-in's token is consent for one
address, so it goes along only while the server still points there. Every
door that saves a server reads both rules from here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.server.database.mcp_oauth import SERVABLE, get_connection
from src.server.models.mcp_server import McpServerInput
from src.server.services.mcp_config import same_consented_url
from src.server.services.mcp_oauth.lifecycle import (
    TokenUnavailable,
    ensure_fresh_access_token,
)

#: Where a server is dialled and with what. The other fields change how it
#: reads, not whether it answers.
DIAL_FIELDS = ("transport", "url", "command", "args", "env", "headers")

_EMPTY: dict[str, Any] = {"args": [], "env": {}, "headers": {}}


def dial_of(fields: Mapping[str, Any]) -> dict[str, Any]:
    """The dial fields of a row or of ``to_catalog_fields()``, with an empty
    value spelled one way, so the two compare equal when they dial alike."""
    return {f: fields.get(f) or _EMPTY.get(f) for f in DIAL_FIELDS}


def dial_moved(prior: Mapping[str, Any], fields: Mapping[str, Any]) -> bool:
    """Whether saving ``fields`` over ``prior`` changes how the server is
    reached or what it is sent."""
    return dial_of(prior) != dial_of(fields)


async def consented_bearer(user_id: str, server: McpServerInput) -> str | None:
    """The token of a live sign-in ``server`` holds for the address it names.

    The rule the relay and discovery apply: a server whose address moved is
    checked without it, and one that wants a sign-in then answers that it
    needs a new one. None too when the token cannot be minted, so the check
    asks for a sign-in (409) and the client starts the repair rather than
    saving past it.
    """
    if server.transport != "http":
        return None
    connection = await get_connection(user_id, server.name)
    if connection is None or connection.status not in SERVABLE:
        return None
    if not same_consented_url(connection.server_url, server.url):
        return None
    try:
        token = await ensure_fresh_access_token(connection.connection_id)
    except TokenUnavailable:
        return None
    return token.access_token
