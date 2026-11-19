"""Organization connectors in a workspace's resolved set: read only where the
deployment serves them, refused while the account is unknown, between the
built-ins and the user's own servers, switched off per workspace by a marker
keyed by the connector's id, and relay-bound in the sandbox."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.config.core import MCPServerConfig
from src.server.database.org_keys import org_key
from src.server.services.account_contract import NO_ORG, Account, Org, OrgConnector
from src.server.services.mcp_config import Origin, State
from tests.unit.server.services.test_resolve_mcp_config import (
    _base_config,
    _resolve,
    _user_row,
    _ws_row,
)

OC = "src.server.services.org_connectors"

CRM = OrgConnector(
    id="c-crm", name="crm", url="https://crm.example.test/mcp", auth="shared",
    revision="r1", description="The CRM",
)
WIKI = OrgConnector(
    id="c-wiki", name="wiki", url="https://wiki.example.test/mcp", auth="member",
    revision="r1",
)
BROKER = OrgConnector(
    id="c-rh", name="rh", url="https://agent.robinhood.com/mcp/trading", auth="shared",
    revision="r1",
)


def _connectors(value):
    return patch(f"{OC}.org_connectors_for", new=AsyncMock(return_value=value))


def _account(*connectors):
    return Account(
        org=Org("o1", "Acme"),
        features={"teams": True, "org_connectors": True},
        connectors=tuple(connectors),
    )


# --- org_connectors_for -----------------------------------------------------


@pytest.mark.asyncio
async def test_with_the_flag_off_nothing_is_read():
    from src.server.services.org_connectors import current_org_digest, org_connectors_for

    cache = MagicMock()
    account = AsyncMock()
    with (
        patch(f"{OC}.is_feature_enabled_system", return_value=False),
        patch(f"{OC}.account_for", new=account),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        assert await org_connectors_for("u") == ()
        assert await current_org_digest("u") == ""
    account.assert_not_awaited()
    assert not cache.method_calls


@pytest.mark.asyncio
async def test_an_unknown_account_is_none_and_a_brokerage_is_left_out():
    from src.server.services.org_connectors import org_connectors_for

    with patch(f"{OC}.is_feature_enabled_system", return_value=True):
        with patch(f"{OC}.account_for", new=AsyncMock(return_value=None)):
            assert await org_connectors_for("u") is None
        with (
            patch(f"{OC}.account_for", new=AsyncMock(return_value=_account(CRM, BROKER))),
            patch(
                "src.server.services.features.feature_enabled_for_account",
                new=AsyncMock(return_value=True),
            ),
        ):
            assert await org_connectors_for("u") == (CRM,)


@pytest.mark.asyncio
async def test_an_organization_that_does_not_grant_the_flag_gives_none():
    from src.server.services.org_connectors import org_connectors_for

    account = Account(
        org=Org("o1", "Acme"), features={"teams": True}, connectors=(CRM,)
    )
    granted = AsyncMock(return_value=False)
    with (
        patch(f"{OC}.is_feature_enabled_system", return_value=True),
        patch(f"{OC}.account_for", new=AsyncMock(return_value=account)),
        patch("src.server.services.features.feature_enabled_for_account", new=granted),
    ):
        assert await org_connectors_for("u") == ()
    granted.assert_awaited_once()
    # An account in no organization lists none and reads no flags.
    granted.reset_mock()
    with (
        patch(f"{OC}.is_feature_enabled_system", return_value=True),
        patch(f"{OC}.account_for", new=AsyncMock(return_value=NO_ORG)),
        patch("src.server.services.features.feature_enabled_for_account", new=granted),
    ):
        assert await org_connectors_for("u") == ()
    granted.assert_not_awaited()


def test_the_digest_moves_with_what_a_workspace_depends_on():
    from dataclasses import replace

    from src.server.services.org_connectors import org_digest

    assert org_digest(()) == ""
    base = org_digest((CRM, WIKI))
    assert base == org_digest((WIKI, CRM))
    assert org_digest((replace(CRM, revision="r2"), WIKI)) != base
    assert org_digest((replace(CRM, url="https://x.example.test/mcp"), WIKI)) != base
    # Prompt text is not a reason to rebuild the tools.
    assert org_digest((replace(CRM, description="other"), WIKI)) == base


# --- resolution -------------------------------------------------------------


@pytest.mark.asyncio
async def test_precedence_is_builtin_then_organization_then_user():
    base = _base_config(MCPServerConfig(name="alpha"))
    shadow = OrgConnector(
        id="c-alpha", name="alpha", url="https://a.example.test/mcp",
        auth="shared", revision="r1",
    )
    user_rows = [_user_row("crm"), _user_row("zeta")]
    with _connectors((CRM, shadow)):
        resolved = await _resolve(base, [], user_rows=user_rows)
    assert [(e.name, e.origin) for e in resolved.entries] == [
        ("alpha", Origin.BUILTIN),
        ("crm", Origin.ORG),
        ("zeta", Origin.USER),
    ]
    crm = resolved.entries[1]
    assert crm.config.url == CRM.url and crm.config.source == "org"
    assert crm.binding_plan is None and crm.denied_tools is None
    assert crm.host_side_oauth
    assert resolved.org_digest


@pytest.mark.asyncio
async def test_a_shadowed_user_server_comes_back_as_it_was():
    base = _base_config()
    user_rows = [_user_row("crm", url="https://mine.example.test/mcp")]
    rows = [_ws_row("crm", source="user", enabled=False)]
    with _connectors((CRM,)):
        shadowed = await _resolve(base, rows, user_rows=user_rows)
    with _connectors(()):
        back = await _resolve(base, rows, user_rows=user_rows)
    assert [(e.name, e.origin, e.state) for e in shadowed.entries] == [
        ("crm", Origin.ORG, State.ACTIVE)
    ]
    # Its own tombstone in this workspace still stands, untouched.
    assert [(e.name, e.origin, e.state) for e in back.entries] == [
        ("crm", Origin.USER, State.TOMBSTONED)
    ]
    assert back.entries[0].config.url == "https://mine.example.test/mcp"


@pytest.mark.asyncio
async def test_a_marker_keyed_by_the_id_switches_one_off_here():
    rows = [_ws_row(org_key(CRM.id), source="org", enabled=False)]
    with _connectors((CRM, WIKI)):
        resolved = await _resolve(_base_config(), rows)
    assert [(e.name, e.state) for e in resolved.entries] == [
        ("wiki", State.ACTIVE),
        ("crm", State.DISABLED),
    ]
    assert [s.name for s in resolved.servers] == ["wiki"]


@pytest.mark.asyncio
async def test_a_member_connection_binds_only_for_the_connectors_url():
    good = {
        "connection_id": "conn-1", "server_name": org_key(WIKI.id),
        "server_url": WIKI.url, "status": "connected",
    }
    moved = {**good, "server_url": "https://elsewhere.example.test/mcp"}
    with _connectors((WIKI,)):
        bound = await _resolve(_base_config(), [], connections=[good])
        unbound = await _resolve(_base_config(), [], connections=[moved])
        never = await _resolve(_base_config(), [])
    assert bound.entries[0].config.oauth_connection_id == "conn-1"
    assert bound.entries[0].oauth_status == "connected"
    assert unbound.entries[0].config.oauth_connection_id is None
    assert unbound.entries[0].oauth_status == "needs_reauth"
    assert never.entries[0].oauth_status is None


@pytest.mark.asyncio
async def test_an_unknown_account_refuses_resolution():
    with _connectors(None), pytest.raises(HTTPException) as unknown:
        await _resolve(_base_config(MCPServerConfig(name="alpha")), [])
    assert unknown.value.status_code == 503


@pytest.mark.asyncio
async def test_with_the_flag_off_resolution_is_unchanged_and_reads_nothing():
    b1 = MCPServerConfig(name="alpha")
    account = AsyncMock()
    with (
        patch(f"{OC}.is_feature_enabled_system", return_value=False),
        patch(f"{OC}.account_for", new=account),
    ):
        resolved = await _resolve(_base_config(b1), [])
    assert resolved.servers[0] is b1
    assert resolved.org_digest == ""
    account.assert_not_awaited()


@pytest.mark.asyncio
async def test_user_connectors_denied_does_not_withhold_the_organizations():
    from src.config.models import PolicyLayer
    from src.server.services.policy import merge

    no_own = merge(None, PolicyLayer.model_validate({"user_connectors": False}))
    with (
        patch("src.server.services.account.policy_for", new=AsyncMock(return_value=no_own)),
        _connectors((CRM,)),
    ):
        resolved = await _resolve(_base_config(), [], user_rows=[_user_row("zeta")])
    assert [s.name for s in resolved.servers] == ["crm"]


# --- the sandbox entry ------------------------------------------------------


def test_the_sandbox_entry_carries_no_url_and_no_headers():
    from ptc_agent.core.tool_generator import ToolFunctionGenerator
    from src.server.services.mcp_config import org_server_config

    server = org_server_config(CRM)
    gen = ToolFunctionGenerator()
    config = gen.generate_client_config([server], working_dir="/work")
    assert config["servers"]["crm"] == {
        "transport": "http",
        "untrusted": True,
        "relay_bound": True,
    }
    code = gen.generate_mcp_client_code([server], working_dir="/work")
    assert "crm.example.test" not in code


def test_a_snapshot_is_read_from_the_user_tier_under_the_connectors_id():
    from src.server.services.mcp_config import org_server_config
    from src.server.services.mcp_discovery import (
        ToolSnapshotIndex,
        mcp_discovery_fingerprint,
    )

    server = org_server_config(CRM)
    fp = mcp_discovery_fingerprint(server)
    row = {"server_name": org_key(CRM.id), "config_hash": fp, "status": "ok"}
    same_name = {"server_name": "crm", "config_hash": fp, "status": "ok"}
    assert ToolSnapshotIndex(user_rows=[row]).snapshot(server) is row
    assert ToolSnapshotIndex(user_rows=[same_name], workspace_rows=[same_name]).snapshot(
        server
    ) is None


def test_a_new_revision_moves_the_discovery_fingerprint():
    from dataclasses import replace

    from src.server.services.mcp_config import org_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    assert mcp_discovery_fingerprint(org_server_config(CRM)) != mcp_discovery_fingerprint(
        org_server_config(replace(CRM, revision="r2"))
    )


# --- workspace toggles ------------------------------------------------------


def _route_patches(route, ws_rows):
    return (
        patch.object(route, "_require_owned_workspace", AsyncMock(return_value={})),
        patch.object(route, "builtin_names", return_value=set()),
        patch(
            "src.server.database.mcp_servers.list_workspace_servers",
            new=AsyncMock(return_value=ws_rows),
        ),
        patch.object(route, "_apply_toggle_now", AsyncMock()),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_toggling_one_writes_or_drops_its_marker(enabled):
    from contextlib import ExitStack

    from src.server.app import mcp_servers as route
    from src.server.models.mcp_server import EnabledInput

    upsert, delete, tombstone = AsyncMock(), AsyncMock(), AsyncMock()
    # A user server of the same name with its own tombstone is not touched.
    ws_rows = [_ws_row("crm", source="user", enabled=False)]
    with ExitStack() as stack:
        for p in _route_patches(route, ws_rows):
            stack.enter_context(p)
        stack.enter_context(_connectors((CRM,)))
        stack.enter_context(patch.object(route, "upsert_workspace_server", upsert))
        stack.enter_context(patch.object(route, "delete_workspace_server", delete))
        stack.enter_context(patch.object(route, "tombstone_user_server", tombstone))
        stack.enter_context(patch.object(route, "untombstone_user_server", tombstone))
        await route.set_enabled("ws", "crm", EnabledInput(enabled=enabled), "u")
    if enabled:
        delete.assert_awaited_once_with("ws", org_key(CRM.id))
        upsert.assert_not_awaited()
    else:
        upsert.assert_awaited_once_with(
            "ws", org_key(CRM.id), source="org", enabled=False, config=None
        )
        delete.assert_not_awaited()
    tombstone.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_member_cannot_edit_one():
    from src.server.models.mcp_server import McpServerInput
    from src.server.services.mcp_saves import ServerEdit

    body = McpServerInput(name="crm", transport="http", url="https://x.example.test/mcp")
    with (
        patch(
            "src.server.services.mcp_saves._owned_workspace", new=AsyncMock(return_value={})
        ),
        patch("src.server.services.mcp_saves.builtin_names", return_value=set()),
        patch(
            "src.server.database.mcp_servers.list_workspace_servers",
            new=AsyncMock(return_value=[]),
        ),
        _connectors((CRM,)),
        pytest.raises(HTTPException) as refused,
    ):
        await ServerEdit("u", "crm", body, "ws").refuse()
    assert refused.value.status_code == 403


@pytest.mark.asyncio
async def test_turning_one_on_is_refused_while_the_account_is_unknown():
    from contextlib import ExitStack

    from src.server.app import mcp_servers as route
    from src.server.models.mcp_server import EnabledInput

    with ExitStack() as stack:
        for p in _route_patches(route, []):
            stack.enter_context(p)
        stack.enter_context(_connectors(None))
        with pytest.raises(HTTPException) as unknown:
            await route.set_enabled("ws", "crm", EnabledInput(enabled=True), "u")
    assert unknown.value.status_code == 503


@pytest.mark.asyncio
async def test_turning_ones_own_server_off_works_while_the_account_is_unknown():
    """A switch-off only narrows, so an outage at the auth service cannot hold
    it; a name that is nothing else stays unknown rather than not found."""
    from contextlib import ExitStack

    from src.server.app import mcp_servers as route
    from src.server.models.mcp_server import EnabledInput

    tombstone = AsyncMock(return_value=True)
    catalog = AsyncMock(return_value=_user_row("crm", enabled=True, user_mcp_server_id="s1"))
    with ExitStack() as stack:
        for p in _route_patches(route, []):
            stack.enter_context(p)
        stack.enter_context(_connectors(None))
        stack.enter_context(
            patch("src.server.database.mcp_servers.get_catalog_server", new=catalog)
        )
        stack.enter_context(patch.object(route, "tombstone_user_server", tombstone))
        out = await route.set_enabled("ws", "crm", EnabledInput(enabled=False), "u")
        assert out == {"name": "crm", "enabled": False}
        tombstone.assert_awaited_once()

        catalog.return_value = None
        with pytest.raises(HTTPException) as unknown:
            await route.set_enabled("ws", "crm", EnabledInput(enabled=False), "u")
    assert unknown.value.status_code == 503


def test_a_duplicate_copies_the_organization_markers():
    import inspect

    from src.server.database.mcp_servers import start_new_workspace_selection

    source = inspect.getsource(start_new_workspace_selection)
    assert "l.source IN ('builtin', 'org')" in source


# --- a warm computer --------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("digest", "resolves"),
    [("", False), ("moved", True), (None, True)],
    ids=["unchanged", "list-moved", "unknown"],
)
async def test_a_warm_view_resolves_again_when_the_list_moves(digest, resolves):
    from src.server.services.workspace_manager import WorkspaceManager
    from tests.unit.server.services.test_workspace_manager_mcp import (
        _make_binding,
        _make_config,
        _make_session,
    )

    WorkspaceManager.reset_instance()
    try:
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=3, summary="already")
        resolve = AsyncMock(side_effect=RuntimeError("stop here"))
        with (
            patch("src.server.services.mcp_config.resolve_mcp_config", resolve),
            patch(
                "src.server.services.computer_manager._mcp.current_org_digest",
                AsyncMock(return_value=digest),
            ),
        ):
            await wm._apply_session_mcp(
                _make_binding("ws", "computer-1"), "user-1", session, ws_version=3
            )
        assert resolve.await_count == (1 if resolves else 0)
    finally:
        WorkspaceManager.reset_instance()
