"""Where an organization connector's per-user state is stored: under a
server name no server of the user's own can hold, since none holds a colon."""

from __future__ import annotations

_PREFIX = "org:"


def org_key(connector_id: str) -> str:
    """The server name a member's state for the connector is stored under."""
    return f"{_PREFIX}{connector_id}"


def org_connector_id(server_name: str) -> str | None:
    """The connector a server name is the :func:`org_key` of, or None for a
    server of the user's own."""
    if not server_name.startswith(_PREFIX):
        return None
    return server_name[len(_PREFIX):]
