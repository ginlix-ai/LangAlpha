"""The ``user_connectors`` rule: the user's own MCP servers are not used while
it stands, brokerages aside, and come back unchanged once it is lifted;
nothing new is read when no policy is configured and ``teams`` is off."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from tests.unit.server.policy_standing import standing_under
from src.config.models import PolicyLayer
from src.server.services.policy import (
    ALLOW_ALL,
    USER_CONNECTORS,
    governed_by_user_connectors,
    merge,
)

POLICY = "src.server.services.policy"
DB = "src.server.database.mcp_servers"
NO_OWN = merge(None, PolicyLayer.model_validate({"user_connectors": False}))

OWN = {"name": "acme", "transport": "http", "url": "https://acme.example.test/mcp"}
STDIO = {"name": "local", "transport": "stdio", "command": "npx", "url": None}
BROKER = {"name": "robinhood", "transport": "http",
          "url": "https://agent.robinhood.com/mcp/trading"}



def test_the_rule_merges_strictly_and_is_reported():
    assert merge(PolicyLayer.model_validate({"user_connectors": False}), None
                 ).allows(USER_CONNECTORS) is False
    assert ALLOW_ALL.allows(USER_CONNECTORS) is True
    client = NO_OWN.for_client()
    assert client["user_connectors"] is False
    assert client["set_by"] == {USER_CONNECTORS: "organization"}
    with pytest.raises(ValueError):
        PolicyLayer.model_validate({"user_connectors": "no"})


def test_a_brokerage_is_the_trading_rules_not_this_ones():
    assert governed_by_user_connectors(OWN["url"]) is True
    assert governed_by_user_connectors(None) is True
    assert governed_by_user_connectors(BROKER["url"]) is False
    # Told apart by host, as everywhere a brokerage is identified.
    assert governed_by_user_connectors("https://agent.robinhood.com/other") is False


# --- user_tier_rows ----------------------------------------------------------


@pytest.mark.asyncio
async def test_user_tier_rows_keeps_only_brokerages_under_the_rule():
    from src.server.services.mcp_sources import user_tier_rows

    reader = AsyncMock(return_value=[OWN, STDIO, BROKER])
    with patch(f"{DB}.list_enabled_user_servers", new=reader):
        with standing_under(ALLOW_ALL):
            assert await user_tier_rows("u") == [OWN, STDIO, BROKER]
        with standing_under(NO_OWN):
            assert await user_tier_rows("u") == [BROKER]


@pytest.mark.asyncio
async def test_user_tier_rows_is_unknown_and_reads_no_rows_while_the_policy_is():
    from src.server.services.mcp_sources import user_tier_rows

    reader = AsyncMock(return_value=[OWN])
    with patch(f"{DB}.list_enabled_user_servers", new=reader), standing_under(None):
        assert await user_tier_rows("u") is None
    reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_teams_off_and_no_policy_nothing_new_is_read():
    from src.server.services.mcp_sources import user_tier_rows

    cache = MagicMock()
    reader = AsyncMock(return_value=[OWN])
    with (
        patch("src.server.services.account.is_feature_enabled_system", return_value=False),
        patch("src.server.services.membership.is_feature_enabled_system", return_value=False),
        patch("src.server.services.account.deployment_policy", return_value=None),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch(f"{DB}.list_enabled_user_servers", new=reader),
    ):
        assert await user_tier_rows("u") == [OWN]
    assert not cache.method_calls
    reader.assert_awaited_once_with("u")


# --- resolution points -------------------------------------------------------


@pytest.mark.asyncio
async def test_resolution_drops_own_servers_and_keeps_brokerages():
    from ptc_agent.config.core import MCPServerConfig
    from tests.unit.server.services.test_resolve_mcp_config import (
        _base_config,
        _resolve,
        _user_row,
    )

    base = _base_config(MCPServerConfig(name="alpha"))
    rows = [_user_row("acme"), _user_row("robinhood", url=BROKER["url"])]
    with standing_under(ALLOW_ALL):
        allowed = await _resolve(base, [], user_rows=rows)
    with standing_under(NO_OWN):
        denied = await _resolve(base, [], user_rows=rows)
    assert [s.name for s in allowed.servers] == ["alpha", "acme", "robinhood"]
    assert [s.name for s in denied.servers] == ["alpha", "robinhood"]


@pytest.mark.asyncio
async def test_resolution_refuses_while_the_policy_is_unknown():
    from ptc_agent.config.core import MCPServerConfig
    from tests.unit.server.services.test_resolve_mcp_config import (
        _base_config,
        _resolve,
    )

    with standing_under(None), pytest.raises(HTTPException) as unknown:
        await _resolve(_base_config(MCPServerConfig(name="alpha")), [])
    assert unknown.value.status_code == 503


@pytest.mark.asyncio
async def test_discovery_drops_a_result_for_a_server_the_rule_withholds(monkeypatch):
    from ptc_agent.config.core import MCPServerConfig
    from src.server.services.mcp_discovery import _stale_server_names

    monkeypatch.setattr(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"workspace_id": "ws", "user_id": "u"}),
    )
    server = MCPServerConfig(name="local", transport="stdio", command="npx", source="user")
    with patch(f"{DB}.list_enabled_user_servers", new=AsyncMock(return_value=[STDIO])):
        with standing_under(ALLOW_ALL):
            assert await _stale_server_names("ws", [server]) == set()
        with standing_under(NO_OWN):
            assert await _stale_server_names("ws", [server]) == {"local"}
        with standing_under(None):
            assert await _stale_server_names("ws", [server]) == {"local"}


@pytest.mark.asyncio
async def test_a_subagents_connector_list_reads_withheld_servers_as_off(monkeypatch):
    from types import SimpleNamespace

    from src.server.services.subagents import context

    catalog = [{**OWN, "enabled": True}, {**BROKER, "enabled": True}]
    monkeypatch.setattr(f"{DB}.list_catalog_servers", AsyncMock(return_value=catalog))
    monkeypatch.setattr(
        "src.server.database.mcp_oauth.list_connections", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        "src.server.services.mcp_config.account_disabled_builtins",
        AsyncMock(return_value=frozenset()),
    )
    config = SimpleNamespace(mcp=SimpleNamespace(servers=[]))
    with standing_under(NO_OWN):
        listed, _ = await context._connectors("u", config)
    assert {c.name: c.enabled for c in listed} == {"acme": False, "robinhood": True}
    with standing_under(None), pytest.raises(HTTPException) as unknown:
        await context._connectors("u", config)
    assert unknown.value.status_code == 503


# --- a change notice ---------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("teams", [True, False])
async def test_a_notice_rebuilds_warm_tools_only_where_teams_is_on(teams):
    from src.server.app.users import membership_changed

    bump = AsyncMock(return_value=1)
    with (
        patch(
            "src.server.services.membership.forget_membership",
            new=AsyncMock(),
        ),
        patch("src.server.services.account.is_feature_enabled_system", return_value=teams),
        patch("src.server.services.membership.is_feature_enabled_system", return_value=teams),
        patch(
            "src.server.database.mcp_config_version.bump_user_workspaces_mcp_version",
            new=bump,
        ),
    ):
        await membership_changed("u", MagicMock(body=AsyncMock(return_value=b"")))
    assert bump.await_count == (1 if teams else 0)
