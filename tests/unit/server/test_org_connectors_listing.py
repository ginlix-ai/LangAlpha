"""The member's view of the organization's connectors: the listing route, and
``origin: "org"`` rows in a workspace's server list."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.org_keys import org_key
from src.server.services.account_contract import OrgConnector

ROUTE = "src.server.app.mcp_org_connectors"

WIKI = OrgConnector(
    id="c-wiki", name="wiki", url="https://wiki.example.test/mcp", auth="member",
    revision="r1", description="The wiki",
)
CRM = OrgConnector(
    id="c-crm", name="crm", url="https://crm.example.test/mcp", auth="shared",
    revision="r1",
)


def _reads(connectors, *, connections=(), markers=(), schemas=()):
    return (
        patch("src.server.services.org_connectors.org_connectors_for", AsyncMock(return_value=connectors)),
        patch("src.server.database.mcp_oauth.list_connections", AsyncMock(return_value=list(connections))),
        patch("src.server.database.mcp_servers.list_scope_markers_for_user", AsyncMock(return_value=list(markers))),
        patch("src.server.database.mcp_tool_schemas.get_user_tool_schemas", AsyncMock(return_value=list(schemas))),
    )


@pytest.mark.asyncio
async def test_the_listing_reports_each_connector_as_managed():
    from contextlib import ExitStack

    from src.server.app.mcp_org_connectors import list_org_connectors

    connections = [
        {"server_name": org_key("c-wiki"), "server_url": WIKI.url, "status": "connected",
         "connection_id": "00000000-0000-4000-8000-0000000000c1"},
        # A server of the member's own with the connector's name is not its sign-in.
        {"server_name": "crm", "server_url": CRM.url, "status": "connected"},
    ]
    markers = [
        {"workspace_id": "ws-2", "name": org_key("c-crm"), "source": "org"},
        {"workspace_id": "ws-1", "name": org_key("c-crm"), "source": "org"},
        {"workspace_id": "ws-3", "name": "crm", "source": "user"},
    ]
    with ExitStack() as stack:
        for p in _reads((WIKI, CRM), connections=connections, markers=markers):
            stack.enter_context(p)
        listed = await list_org_connectors("u")
    assert [c.model_dump() for c in listed.connectors] == [
        {"id": "c-crm", "name": "crm", "description": "", "auth": "shared",
         "connection_status": None, "disabled_workspace_ids": ["ws-1", "ws-2"],
         "managed": True, "discovery": "pending", "tool_count": 0},
        {"id": "c-wiki", "name": "wiki", "description": "The wiki", "auth": "member",
         "connection_status": "connected", "disabled_workspace_ids": [],
         "managed": True, "discovery": "pending", "tool_count": 0},
    ]


@pytest.mark.asyncio
async def test_the_listing_says_what_the_members_listing_of_its_tools_found():
    from contextlib import ExitStack

    from src.server.app.mcp_org_connectors import list_org_connectors
    from src.server.services.mcp_config import org_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    def row(connector, **fields):
        cfg = org_server_config(connector)
        return {"server_name": org_key(connector.id),
                "config_hash": mcp_discovery_fingerprint(cfg), **fields}

    schemas = [
        row(WIKI, status="ok", tools=[{"name": "search"}, {"name": "page"}]),
        row(CRM, status="error", error="timed out", tools=[]),
    ]
    with ExitStack() as stack:
        for p in _reads((WIKI, CRM), schemas=schemas):
            stack.enter_context(p)
        listed = await list_org_connectors("u")
    found = {c.name: (c.discovery, c.tool_count) for c in listed.connectors}
    assert found == {"crm": ("error", 0), "wiki": ("ok", 2)}


@pytest.mark.asyncio
async def test_a_sign_in_given_for_another_url_needs_doing_again():
    from contextlib import ExitStack

    from src.server.app.mcp_org_connectors import list_org_connectors

    moved = [{"server_name": org_key("c-wiki"), "server_url": "https://old.example.test",
              "status": "connected"}]
    with ExitStack() as stack:
        for p in _reads((WIKI,), connections=moved):
            stack.enter_context(p)
        listed = await list_org_connectors("u")
    assert listed.connectors[0].connection_status == "needs_reauth"


@pytest.mark.asyncio
async def test_the_listing_answers_503_while_the_account_is_unknown():
    from contextlib import ExitStack

    from src.server.app.mcp_org_connectors import list_org_connectors

    with ExitStack() as stack:
        for p in _reads(None):
            stack.enter_context(p)
        with pytest.raises(HTTPException) as unknown:
            await list_org_connectors("u")
    assert unknown.value.status_code == 503


@pytest.mark.asyncio
async def test_with_the_flag_off_the_listing_reads_nothing():
    from src.server.app.mcp_org_connectors import list_org_connectors

    account = AsyncMock()
    connections = AsyncMock()
    with (
        patch("src.server.services.org_connectors.is_feature_enabled_system", return_value=False),
        patch("src.server.services.org_connectors.account_for", account),
        patch("src.server.database.mcp_oauth.list_connections", connections),
    ):
        assert (await list_org_connectors("u")).connectors == []
    account.assert_not_awaited()
    connections.assert_not_awaited()


def test_a_workspace_list_row_says_org_and_hides_the_address():
    from src.server.app.mcp_servers import _effective_server
    from src.server.services.mcp_config import (
        Origin,
        ResolvedServer,
        State,
        org_server_config,
    )

    row = _effective_server(
        ResolvedServer(org_server_config(CRM), Origin.ORG, State.ACTIVE),
        status="connected",
        config_version=3,
    )
    wire = row.model_dump(mode="json")
    assert wire["origin"] == "org"
    assert wire["editable"] is False
    assert wire["url"] is None
    assert wire["headers"] == {} and wire["env"] == {}


@pytest.mark.asyncio
async def test_the_listing_shows_only_what_a_workspace_resolves():
    """A connector named like a built-in is skipped in every workspace, so
    listing it would offer the member something no workspace can get."""
    from contextlib import ExitStack

    from ptc_agent.config.core import MCPServerConfig
    from src.server.app.mcp_org_connectors import list_org_connectors
    from src.server.services.mcp_config import Origin
    from tests.unit.server.services.test_resolve_mcp_config import _base_config, _resolve

    shadow = OrgConnector(
        id="c-alpha", name="alpha", url="https://alpha.example.test/mcp",
        auth="shared", revision="r1",
    )
    base = _base_config(MCPServerConfig(name="alpha"))
    with patch(
        "src.server.services.org_connectors.org_connectors_for",
        AsyncMock(return_value=(shadow, CRM)),
    ):
        resolved = await _resolve(base, [])
    with ExitStack() as stack:
        for p in _reads((shadow, CRM)):
            stack.enter_context(p)
        stack.enter_context(patch("src.server.app.setup.agent_config", base))
        listed = await list_org_connectors("u")
    assert [c.name for c in listed.connectors] == ["crm"]
    assert [e.name for e in resolved.entries if e.origin is Origin.ORG] == ["crm"]
