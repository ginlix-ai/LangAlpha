"""Tests for the per-workspace MCP server router (app/mcp_servers.py).

Covers the effective list + status derivation, the add/import/edit routes that
write the user's one account-level server, PATCH builtin disable-marker
semantics, masked env/header values, and the debounced discover probe. DB +
WorkspaceManager are mocked; the resolver is the real chokepoint fed a mocked
DB.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.config.core import MCPServerConfig
from src.server.app.mcp_servers import _derive_status
from src.server.database.account_disables import AccountDisables
from src.server.database.mcp_servers import MAX_CATALOG_SERVERS_PER_USER
from src.server.models.mcp_server import McpServerInput
from src.server.services.computer_manager import MachineTarget
from src.server.services.mcp_catalog import CatalogEdit, detach_warning
from src.server.services.mcp_config import Origin
from src.server.services.plugins.bundled import ComponentOwners
from src.server.services.mcp_discovery import mcp_discovery_fingerprint
from tests.conftest import create_test_app
from src.server.services.mcp_validation import CheckFailure, SignInNeeded
from tests.unit.server.mcp_builders import checks_pass, passing, resolved_mcp

NOW = datetime.now(timezone.utc)
USER = "test-user-123"


def _ws(workspace_id=None, user_id=USER, status="running", **overrides):
    return {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": "Test Workspace",
        "status": status,
        "sandbox_id": "sb-1",
        "config": None,
        "mcp_config_version": 3,
        **overrides,
    }


def _builtin(name="builtin_search"):
    return MCPServerConfig(name=name, transport="stdio", command="npx", source="builtin")


def _user_server(name="remote_server", **kw):
    """A server on the user's account, as it resolves into the workspace."""
    return MCPServerConfig(
        name=name,
        transport="http",
        url="https://api.example.com/mcp",
        headers=kw.pop("headers", {}),
        source="user",
        **kw,
    )


def _stdio_user_server(name="stdio_server"):
    """An account server with no host-side path: discovery runs in the sandbox."""
    return MCPServerConfig(
        name=name, transport="stdio", command="npx", args=["-y", "pkg"], source="user"
    )


def _catalog_row(name="remote_server", **kw):
    """A user_mcp_servers row as the catalog reads return it."""
    base = {
        "name": name, "enabled": True, "transport": "http", "command": None,
        "args": [], "url": "https://api.example.com/mcp", "env": {},
        "headers": {}, "description": "", "instruction": "",
        "tool_exposure_mode": "summary", "created_at": None, "updated_at": None,
        "plugin_id": None, "plugin_name": None, "plugin_enabled": None,
    }
    base.update(kw)
    return base


def _agent_config(servers):
    cfg = MagicMock()
    cfg.mcp.servers = servers
    return cfg


@pytest_asyncio.fixture
async def client():
    from src.server.app.mcp_servers import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture(autouse=True)
def _probe_kick_always_claimed():
    """Background discovery claims its kick in Postgres before dialling, and
    these tests have no pool. The claim answers with the stamp it wrote, which
    the probe then fences its write on; the throttle itself is pinned in
    test_mcp_discovery_schedule.py."""
    with patch(
        "src.server.services.mcp_oauth.discovery.claim_probe_kick",
        new=AsyncMock(return_value=datetime.now(timezone.utc)),
    ):
        yield


@pytest.fixture(autouse=True)
def catalog_kick():
    """A new account row earns its first verdict from the host-side probe;
    the tests assert the kick, not what the probe would find."""
    with patch("src.server.app.mcp_servers.schedule_catalog_discovery") as kick:
        yield kick


@pytest.fixture(autouse=True)
def _no_user_level_rows():
    """Default the account-level reads to empty.

    Tests that need a row re-patch these inside their own ``with`` blocks.
    """
    with (
        patch(
            "src.server.app.mcp_servers.get_user_secret_names",
            new=AsyncMock(return_value=set()),
        ),
        patch(
            "src.server.services.mcp_import.get_user_secret_names",
            new=AsyncMock(return_value=set()),
        ),
        patch(
            "src.server.app.mcp_servers.get_user_tool_schemas",
            new=AsyncMock(return_value=[]),
        ),
        # The add route asks for the name and the account's room before it
        # checks anything.
        patch(
            "src.server.services.mcp_catalog.list_catalog_servers",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.app.mcp_servers.list_catalog_servers",
            new=AsyncMock(return_value=[]),
        ),
        # classify_server_name resolves the Connectors tier itself.
        patch(
            "src.server.database.mcp_servers.get_catalog_server",
            new=AsyncMock(return_value=None),
        ),
        # catalog_write_warnings asks whether an OAuth connection holds the name.
        patch(
            "src.server.app.mcp_catalog.get_connection",
            new=AsyncMock(return_value=None),
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def checks():
    """Every server these routes write passes its check unless a test says
    otherwise; the gate itself is pinned in the tests that fail it."""
    with checks_pass() as seams:
        yield seams


@pytest.fixture(autouse=True)
def _import_txn():
    """Stub the per-entry import transaction with a sentinel connection.

    The import commits each entry's secrets + server row in one transaction;
    the tests assert on the writers, so the connection only has to be a real
    context manager that re-raises (an entry's failure must reach the caller).
    """
    from contextlib import asynccontextmanager

    conn = MagicMock(name="conn")

    @asynccontextmanager
    async def _txn():
        yield None

    conn.transaction = _txn

    @asynccontextmanager
    async def _connection():
        yield conn

    with patch(
        "src.server.services.mcp_import.get_db_connection", new=_connection
    ):
        yield conn


# ---------------------------------------------------------------------------
# Status derivation (pure unit)
# ---------------------------------------------------------------------------


def test_status_builtin_is_connected():
    status, err, missing = _derive_status(
        origin=Origin.BUILTIN, refs=set(),
        secret_names=set(), schema_row=None,
    )
    assert status == "connected" and err == "" and missing == []


def test_status_needs_secret_when_ref_missing():
    status, _, missing = _derive_status(
        origin=Origin.USER, refs={"API_KEY"},
        secret_names=set(), schema_row={"status": "ok", "tools": []},
    )
    assert status == "needs_secret"
    assert missing == ["API_KEY"]


def test_status_connected_when_schema_ok_and_secret_present():
    status, _, missing = _derive_status(
        origin=Origin.USER, refs={"API_KEY"},
        secret_names={"API_KEY"}, schema_row={"status": "ok", "tools": []},
    )
    assert status == "connected"
    assert missing == []


def test_status_error_passes_text():
    status, err, _ = _derive_status(
        origin=Origin.USER, refs=set(),
        secret_names=set(), schema_row={"status": "error", "error": "boom"},
    )
    assert status == "error" and err == "boom"


def test_status_pending_when_no_schema_row():
    status, _, _ = _derive_status(
        origin=Origin.USER, refs=set(),
        secret_names=set(), schema_row=None,
    )
    assert status == "pending"


# ---------------------------------------------------------------------------
# GET effective list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_effective_servers_masks_and_decorates(client):
    ws = _ws()
    base = _agent_config([_builtin()])
    user_srv = _user_server(headers={"Authorization": "${vault:API_KEY}"})
    resolved = resolved_mcp(builtins=[_builtin()], inherited=[user_srv])
    schema_rows = [
        {"server_name": "remote_server", "status": "ok",
         "tools": [{"name": "search", "description": "d", "input_schema": {}}],
         "error": "", "config_hash": mcp_discovery_fingerprint(user_srv),
         "discovered_at": NOW.isoformat()},
    ]
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        # The user's vault is the one namespace the sandbox resolves refs in.
        patch("src.server.app.mcp_servers.get_user_secret_names", new=AsyncMock(return_value={"API_KEY"})),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=schema_rows)),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    body = resp.json()
    assert body["sandbox_running"] is True
    assert body["sandbox_warming"] is False  # already running ⇒ not warming
    assert body["max_servers"] == MAX_CATALOG_SERVERS_PER_USER
    assert body["config_version"] == 3
    by_name = {s["name"]: s for s in body["servers"]}

    bi = by_name["builtin_search"]
    assert bi["origin"] == "builtin" and bi["status"] == "connected"
    assert bi["editable"] is False

    us = by_name["remote_server"]
    assert us["origin"] == "user" and us["status"] == "connected"
    assert us["editable"] is True
    assert us["header_refs"] == ["API_KEY"]
    assert us["tool_count"] == 1
    # tool_exposure_mode is non-null: a config None coalesces to "summary".
    assert us["tool_exposure_mode"] == "summary"
    assert bi["tool_exposure_mode"] == "summary"
    # The stored reference map is echoed for user servers so the edit form
    # can round-trip it: values are ref strings, never resolved secrets.
    assert us["headers"] == {"Authorization": "${vault:API_KEY}"}
    assert us["env"] == {}
    # Built-ins never echo maps.
    assert bi["env"] == {} and bi["headers"] == {}


@pytest.mark.asyncio
async def test_list_surfaces_applied_config_version(client):
    """The running session's applied version flows into the response so the UI
    can show a version-accurate "synced/applying" state instead of a timer."""
    ws = _ws()
    base = _agent_config([_builtin()])
    resolved = resolved_mcp(builtins=[_builtin()])
    wm = MagicMock()
    wm.get_applied_mcp_config_version.return_value = 2  # behind the saved version
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
        patch("src.server.app.mcp_servers.WorkspaceManager.get_instance", return_value=wm),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    body = resp.json()
    assert body["config_version"] == 3
    # applied (2) < saved (3) ⇒ the UI reads "applying", not "synced".
    assert body["applied_config_version"] == 2
    # Fenced on the row's binding: a version from a session holding a superseded
    # sandbox is not something this worker can vouch for.
    wm.get_applied_mcp_config_version.assert_called_once_with(
        ws["workspace_id"],
        expected_sandbox_id="sb-1",
        target=MachineTarget.of_workspace(ws),
    )


def test_live_sandbox_declines_a_superseded_handle():
    """Discovery probes a sandbox and persists the schemas under the workspace,
    so a handle for a replaced sandbox would attribute a dead sandbox's toolset
    to the live one. Declining leaves the rows ``pending``, which is recoverable.
    """
    from src.server.app.mcp_servers import _get_live_sandbox

    ws = _ws(sandbox_id="sb-replaced")
    wm = MagicMock()
    wm.get_session_if_ready.return_value = None  # bound elsewhere ⇒ declined
    with patch(
        "src.server.app.mcp_servers.WorkspaceManager.get_instance", return_value=wm
    ):
        assert _get_live_sandbox(ws["workspace_id"], ws) is None
    wm.get_session_if_ready.assert_called_once_with(
        ws["workspace_id"],
        expected_sandbox_id="sb-replaced",
        target=MachineTarget.of_workspace(ws),
    )


@pytest.mark.asyncio
async def test_list_surfaces_sandbox_warming(client):
    """A workspace transitioning up (status 'starting') reports sandbox_warming
    so the UI keeps polling and shows "Starting workspace…" rather than resting
    on a stale stopped state."""
    ws = _ws(status="starting")
    base = _agent_config([_builtin()])
    resolved = resolved_mcp(builtins=[_builtin()], version=1)
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    body = resp.json()
    assert body["sandbox_running"] is False
    assert body["sandbox_warming"] is True


@pytest.mark.asyncio
async def test_list_reuses_cached_schema_across_unrelated_mutation(client):
    """Regression: toggling/adding ANY server bumps the workspace
    config_version, but a snapshot cached under the server's own per-server
    fingerprint stays valid — the unrelated server reads 'connected', not a
    needless re-verify. (The bug: the cache used to be keyed by config_version,
    so any mutation orphaned every server's snapshot.)"""
    ws = _ws()
    base = _agent_config([])
    user_srv = _user_server()
    # The version has long since moved on from when the snapshot was cached.
    resolved = resolved_mcp(inherited=[user_srv], version=99)
    schema_rows = [{
        "server_name": "remote_server", "status": "ok",
        "tools": [{"name": "search", "description": "d", "input_schema": {}}],
        "error": "", "config_hash": mcp_discovery_fingerprint(user_srv),
        "discovered_at": NOW.isoformat(),
    }]
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=schema_rows)),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    srv = {s["name"]: s for s in resp.json()["servers"]}["remote_server"]
    assert srv["status"] == "connected"
    assert srv["tool_count"] == 1


@pytest.mark.asyncio
async def test_list_reverifies_when_server_own_config_changed(client):
    """A cached snapshot whose fingerprint no longer matches the server's
    current config (its OWN definition changed) is treated as pending so only
    THAT server re-verifies — not the whole workspace."""
    ws = _ws()
    base = _agent_config([])
    user_srv = _user_server()
    resolved = resolved_mcp(inherited=[user_srv])
    schema_rows = [{
        "server_name": "remote_server", "status": "ok",
        "tools": [{"name": "search", "description": "d", "input_schema": {}}],
        "error": "", "config_hash": "fingerprint-of-an-older-config",
        "discovered_at": NOW.isoformat(),
    }]
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=schema_rows)),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    srv = {s["name"]: s for s in resp.json()["servers"]}["remote_server"]
    assert srv["status"] == "pending"
    assert srv["tool_count"] == 0


@pytest.mark.asyncio
async def test_list_keeps_disabled_builtin_visible(client):
    ws = _ws()
    disabled = _builtin("builtin_disabled")
    base = _agent_config([_builtin(), disabled])
    resolved = resolved_mcp(
        builtins=[_builtin()], disabled_builtins=[disabled], version=4
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    by_name = {s["name"]: s for s in resp.json()["servers"]}
    # The disabled builtin stays visible so the UI keeps its re-enable toggle.
    row = by_name["builtin_disabled"]
    assert row["origin"] == "builtin"
    assert row["enabled"] is False
    assert row["status"] == "disabled"
    assert row["editable"] is False
    assert row["tool_count"] == 0


@pytest.mark.asyncio
async def test_list_keeps_a_server_switched_off_here_visible(client):
    # A server tombstoned in this workspace is dropped from the effective set
    # but must still render (greyed, with its toggle) so it can be switched
    # back on, and stays editable: the edit lands on the account row.
    ws = _ws()
    base = _agent_config([_builtin()])
    off_here = _user_server(name="disabled_remote")
    resolved = resolved_mcp(
        builtins=[_builtin()], tombstoned=[off_here], version=5
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    by_name = {s["name"]: s for s in resp.json()["servers"]}
    row = by_name["disabled_remote"]
    assert row["origin"] == "user"
    assert row["enabled"] is False
    assert row["status"] == "disabled"
    assert row["editable"] is True


@pytest.mark.asyncio
async def test_list_needs_secret_surfaces_missing(client):
    ws = _ws()
    base = _agent_config([])
    user_srv = _user_server(headers={"Authorization": "${vault:API_KEY}"})
    resolved = resolved_mcp(inherited=[user_srv])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    s = resp.json()["servers"][0]
    assert s["status"] == "needs_secret"
    assert s["missing_secrets"] == ["API_KEY"]


@pytest.mark.asyncio
async def test_list_needs_secret_counts_args_refs(client):
    """A ref only in stdio args (the import path writes ``--flag=${vault:N}``)
    must surface as needs_secret — it fails at call time exactly like an env
    ref, and an ok discovery snapshot must not mask it."""
    ws = _ws()
    base = _agent_config([])
    srv = MCPServerConfig(
        name="stdio_args",
        transport="stdio",
        command="uvx",
        args=["some-mcp-server", "--token=${vault:ARG_TOKEN}"],
        source="user",
    )
    resolved = resolved_mcp(inherited=[srv])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    s = resp.json()["servers"][0]
    assert s["status"] == "needs_secret"
    assert s["missing_secrets"] == ["ARG_TOKEN"]
    # The projections stay env/header-only; the ref still counts toward status.
    assert s["env_refs"] == [] and s["header_refs"] == []


@pytest.mark.asyncio
async def test_list_surfaces_oauth_status_on_inherited_rows(client):
    """An OAuth connection the user still has to repair must say so, not sit on
    'pending' while the UI shows a Verifying state nothing can ever resolve. A
    revoked one is not in that set: the resolver drops it, so the row reaches
    this list as a plain header row. A row no connection claims never carries
    it."""
    ws = _ws()
    base = _agent_config([])
    connected = _user_server(name="robinhood")
    plain = _user_server(name="plain_remote")
    resolved = resolved_mcp(
        inherited=[connected, plain],
        oauth_status={"robinhood": "needs_reauth"},
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    by_name = {s["name"]: s for s in resp.json()["servers"]}
    assert by_name["robinhood"]["origin"] == "user"
    assert by_name["robinhood"]["oauth_status"] == "needs_reauth"
    assert by_name["robinhood"]["status"] == "pending"
    assert by_name["plain_remote"]["oauth_status"] is None
    # A brokerage's row is written by its connect flow on Plugins, so the
    # workspace offers no edit for it.
    assert by_name["robinhood"]["editable"] is False
    assert by_name["plain_remote"]["editable"] is True


@pytest.mark.asyncio
async def test_list_inherited_prefers_user_cache_over_workspace_cache(client):
    """The user-level snapshot tracks the OAuth lifecycle (purged on
    disconnect, refreshed on connect); a workspace snapshot's fingerprint is
    OAuth-blind and can outlive both — so the user cache must win."""
    ws = _ws()
    base = _agent_config([])
    inherited = _user_server(name="robinhood")
    fingerprint = mcp_discovery_fingerprint(inherited)
    resolved = resolved_mcp(inherited=[inherited])
    ws_rows = [
        {"server_name": "robinhood", "status": "error", "tools": [],
         "error": "stale pre-connect probe", "config_hash": fingerprint,
         "discovered_at": NOW.isoformat()},
    ]
    user_rows = [
        {"server_name": "robinhood", "status": "ok",
         "tools": [{"name": "get_positions", "description": "", "input_schema": {}}],
         "error": "", "config_hash": fingerprint,
         "discovered_at": NOW.isoformat()},
    ]
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=ws_rows)),
        patch("src.server.app.mcp_servers.get_user_tool_schemas", new=AsyncMock(return_value=user_rows)),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    s = resp.json()["servers"][0]
    assert s["status"] == "connected"
    assert s["tool_count"] == 1


# ---------------------------------------------------------------------------
# POST add: installs on the account, switched on only in this workspace
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_server_409_on_builtin_collision(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    create = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.services.mcp_saves.create_workspace_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "builtin_search", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_add_server_409_on_a_shipped_brokerage_name(client):
    """Adding from a workspace mints a catalog row, so it owes the catalog's
    reservation: a row named after a shipped brokerage is shown on Plugins
    wearing that broker's label and tile, wherever its URL points."""
    ws = _ws()
    create = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.services.mcp_saves.create_workspace_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "robinhood", "transport": "http", "url": "https://not-rh.example.com/mcp"},
        )
    assert resp.status_code == 409
    assert "reserved" in resp.json()["detail"]
    create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["class", "mcp_client", "__init__"])
async def test_add_server_422_on_a_name_the_sandbox_reserves(client, name):
    ws = _ws()
    create = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.services.mcp_saves.create_workspace_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": name, "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 422
    assert "sandbox" in resp.json()["detail"]
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_add_server_409_when_the_account_has_the_name(client):
    """A name is one server across the account, so a workspace cannot add a
    second definition under it, and the refusal says where the name is taken."""
    ws = _ws()
    create = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch(
            "src.server.services.mcp_catalog.list_catalog_servers",
            new=AsyncMock(return_value=[_catalog_row(name="dupe_server")]),
        ),
        patch("src.server.services.mcp_saves.create_workspace_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "dupe_server", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    assert resp.json()["detail"] == (
        "A server named 'dupe_server' already exists on your account. "
        "Choose another name."
    )
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_add_server_409_when_a_concurrent_create_wins_the_name(client):
    # The pre-check and the insert are separate reads, so the insert's own
    # conflict is what stops the loser: a 409, never a silent 201.
    ws = _ws()
    create = AsyncMock(
        side_effect=ValueError("MCP catalog server 'dupe_server' already exists for this user")
    )
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.services.mcp_saves.create_workspace_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "dupe_server", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    assert "already exists" in resp.json()["detail"]
    assert create.await_count == 1


@pytest.mark.asyncio
async def test_add_server_409_when_over_cap(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch(
            "src.server.services.mcp_saves.create_workspace_catalog_server",
            new=AsyncMock(side_effect=ValueError("Maximum of 50 MCP catalog servers per user reached")),
        ),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "new_server", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    assert "Maximum of 50" in resp.json()["detail"]


async def _rejected(user_id, server, **_kwargs):
    return CheckFailure(
        server=server.name,
        verdict="credential_rejected",
        reason="the server rejected the key",
    )


@pytest.mark.asyncio
async def test_add_server_writes_nothing_when_the_server_fails_its_check(
    client, checks, catalog_kick
):
    """Checked on this workspace's computer, where a command server would run,
    and refused before the row or its tombstones exist anywhere."""
    ws = _ws()
    checks.catalog.side_effect = _rejected
    create = AsyncMock()
    body = {"name": "new_server", "transport": "http", "url": "https://api.example.com/mcp"}
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.services.mcp_saves.create_workspace_catalog_server", new=create),
        patch("src.server.services.mcp_saves.schedule_proactive_apply") as sched,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers", json=body
        )
    assert resp.status_code == 422
    assert resp.json()["detail"]["code"] == "mcp_check_failed"
    assert resp.json()["detail"]["failures"][0]["verdict"] == "credential_rejected"
    args, kwargs = checks.catalog.await_args
    assert args == (USER, McpServerInput(**body))
    assert kwargs["workspace_id"] == ws["workspace_id"]
    create.assert_not_awaited()
    sched.assert_not_called()
    catalog_kick.assert_not_called()


@pytest.mark.asyncio
async def test_add_server_installs_on_the_account_switched_on_only_here(client, catalog_kick):
    """The row is the one Plugins lists. The writer creates it enabled and
    tombstones it in every other workspace in one transaction, so the route
    hands it this workspace and the account-row fields, nothing else."""
    ws = _ws()
    base = _agent_config([])
    create = AsyncMock(return_value=_catalog_row(name="new_server"))
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.services.mcp_saves.create_workspace_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "new_server", "transport": "stdio", "command": "npx", "args": ["-y", "pkg"]},
        )
    assert resp.status_code == 201
    assert resp.json() == {"name": "new_server", "source": "user", "enabled": True}
    args, kwargs = create.await_args
    assert args == (USER, ws["workspace_id"], "new_server")
    assert kwargs["command"] == "npx" and kwargs["args"] == ["-y", "pkg"]
    # A new row carries no verdict of its own until the host probes it.
    catalog_kick.assert_called_once_with(USER, "new_server", reason="create")


@pytest.mark.asyncio
async def test_mutation_schedules_proactive_apply(client):
    """A successful add front-loads applying the new config to the running
    session (live before the next turn), not only on the next message."""
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch(
            "src.server.services.mcp_saves.create_workspace_catalog_server",
            new=AsyncMock(return_value=_catalog_row(name="new_server")),
        ),
        patch("src.server.services.mcp_saves.schedule_proactive_apply") as sched,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "new_server", "transport": "stdio", "command": "npx", "args": ["-y", "pkg"]},
        )
    assert resp.status_code == 201
    sched.assert_called_once_with(ws["workspace_id"], USER)


@pytest.mark.asyncio
async def test_proactive_apply_coalesces_burst_mutations(monkeypatch):
    """Mutations inside the settle window collapse into ONE apply; a mutation
    after the window schedules a fresh one."""
    import asyncio as aio

    from src.server.services import mcp_apply as mod
    from src.server.services.workspace_manager import WorkspaceManager

    wm = MagicMock()
    wm.proactively_apply_mcp_config = AsyncMock()
    monkeypatch.setattr(
        WorkspaceManager, "get_instance", classmethod(lambda cls: wm)
    )
    monkeypatch.setattr(mod, "_PROACTIVE_APPLY_SETTLE_S", 0.02)

    for _ in range(5):
        mod.schedule_proactive_apply("ws-1", "user-1")
    await aio.sleep(0.1)
    assert wm.proactively_apply_mcp_config.await_count == 1

    mod.schedule_proactive_apply("ws-1", "user-1")
    await aio.sleep(0.1)
    assert wm.proactively_apply_mcp_config.await_count == 2
    assert "ws-1" not in mod._proactive_apply_pending


@pytest.mark.asyncio
async def test_schedule_session_mcp_refresh_drives_refresh(monkeypatch):
    """The probe's post-ok hook drives WorkspaceManager.refresh_session_mcp
    (undebounced — probes are explicit single user actions)."""
    import asyncio as aio

    from src.server.services import mcp_apply as mod
    from src.server.services.workspace_manager import WorkspaceManager

    wm = MagicMock()
    wm.refresh_session_mcp = AsyncMock()
    monkeypatch.setattr(
        WorkspaceManager, "get_instance", classmethod(lambda cls: wm)
    )

    mod.schedule_session_mcp_refresh("ws-1", "user-1")
    await aio.sleep(0.05)

    wm.refresh_session_mcp.assert_awaited_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_add_server_rejects_empty_command(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "evil", "transport": "stdio", "command": ""},
        )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST import: a standard mcpServers blob into account rows + the user's vault
# ---------------------------------------------------------------------------


def _created_row(user_id, workspace_id, name, conn=None, **fields):
    return _catalog_row(name=name)


def _names_converged(after: AsyncMock) -> list[str]:
    """Every secret name the import handed to the batch fan-out."""
    return [name for c in after.await_args_list for name in c.args[1]]


@pytest.mark.asyncio
async def test_import_creates_and_extracts_secret(client, catalog_kick):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    insert = AsyncMock(side_effect=_created_row)
    create_secret = AsyncMock()
    after = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 8))),
        patch("src.server.services.mcp_import.create_user_secret", new=create_secret),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=insert),
        patch("src.server.app.mcp_servers.after_secrets_changed", new=after),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    "my-stock-mcp": {
                        "type": "streamablehttp",
                        "url": "https://api.example.com/ds/stock",
                        "headers": {"Authorization": "EXAMPLE-OPAQUE-TOKEN-1234567890"},
                    }
                }
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] == 1
    assert body["config_version"] == 8
    row = body["results"][0]
    assert row["status"] == "created"
    assert row["name"] == "my_stock_mcp" and row["renamed"] is True
    # The literal Authorization token went into the user's vault, the one
    # every workspace resolves against, not inline.
    assert body["secrets_created"] == ["MY_STOCK_MCP_AUTHORIZATION"]
    create_secret.assert_awaited_once()
    sec_args, _ = create_secret.await_args
    assert sec_args[:3] == (
        USER, "MY_STOCK_MCP_AUTHORIZATION", "EXAMPLE-OPAQUE-TOKEN-1234567890"
    )
    # The account row references the vault, never the raw token, and is
    # switched on in this workspace only.
    ins_args, ins_kwargs = insert.await_args
    assert ins_args == (USER, ws["workspace_id"], "my_stock_mcp")
    assert ins_kwargs["headers"] == {
        "Authorization": "${vault:MY_STOCK_MCP_AUTHORIZATION}"
    }
    # An authenticated remote server is set to use its secret during discovery,
    # otherwise tools/list returns 401.
    assert ins_kwargs["discovery_uses_secrets"] is True
    # Secret and server row are written on ONE connection — the entry is atomic.
    assert create_secret.await_args.kwargs["conn"] is ins_kwargs["conn"]
    assert "EXAMPLE-OPAQUE-TOKEN-1234567890" not in resp.text
    # A new secret converges the way one saved on the vault page does.
    after.assert_awaited_once_with(USER, ["MY_STOCK_MCP_AUTHORIZATION"])
    catalog_kick.assert_called_once_with(USER, "my_stock_mcp", reason="import")


@pytest.mark.asyncio
async def test_import_checks_each_entry_on_this_workspaces_computer(client, checks, catalog_kick):
    ws = _ws()

    async def answer(user_id, server, **kwargs):
        if server.name == "broken":
            return await _rejected(user_id, server)
        return await passing(user_id, server)

    checks.imports.side_effect = answer
    insert = AsyncMock(side_effect=_created_row)
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 8))),
        patch("src.server.services.mcp_import.create_user_secret", new=AsyncMock()),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=insert),
        patch("src.server.app.mcp_servers.after_secrets_changed", new=AsyncMock()),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    "works": {"type": "http", "url": "https://api.example.com/a"},
                    "broken": {"type": "http", "url": "https://api.example.com/b"},
                }
            },
        )
    assert resp.status_code == 200
    statuses = {r["name"]: r["status"] for r in resp.json()["results"]}
    assert statuses == {"works": "created", "broken": "check_failed"}
    assert [c.args[2] for c in insert.await_args_list] == ["works"]
    assert {c.kwargs["workspace_id"] for c in checks.imports.await_args_list} == {
        ws["workspace_id"]
    }


@pytest.mark.asyncio
async def test_import_extracts_secret_in_args(client):
    """A credential in stdio args (``--api-key=TOKEN``) is vaulted on import and
    the arg rewritten to a ${vault:NAME} ref — never stored/echoed in plaintext
    (the generated client resolves the ref vault-only at spawn)."""
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    insert = AsyncMock(side_effect=_created_row)
    create_secret = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 8))),
        patch("src.server.services.mcp_import.create_user_secret", new=create_secret),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=insert),
        patch("src.server.app.mcp_servers.after_secrets_changed", new=AsyncMock()),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    "my-tool": {
                        "command": "npx",
                        "args": ["-y", "@foo/bar", "--api-key=EXAMPLE-OPAQUE-TOKEN-1234567890"],
                    }
                }
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] == 1
    assert body["secrets_created"] == ["MY_TOOL_API_KEY"]
    create_secret.assert_awaited_once()
    sec_args, _ = create_secret.await_args
    assert sec_args[2] == "EXAMPLE-OPAQUE-TOKEN-1234567890"
    # The arg now references the vault; the raw token is gone from config + response.
    _, ins_kwargs = insert.await_args
    assert ins_kwargs["args"] == [
        "-y", "@foo/bar", "--api-key=${vault:MY_TOOL_API_KEY}"
    ]
    assert "EXAMPLE-OPAQUE-TOKEN-1234567890" not in resp.text


@pytest.mark.asyncio
async def test_import_dedupes_identical_token_across_servers(client):
    ws = _ws()
    base = _agent_config([])
    create_secret = AsyncMock()
    insert = AsyncMock(side_effect=_created_row)
    token = "SHARED-OPAQUE-TOKEN-ABCDEFGHIJ"
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.services.mcp_import.create_user_secret", new=create_secret),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=insert),
        patch("src.server.app.mcp_servers.after_secrets_changed", new=AsyncMock()),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    "srv_one": {"type": "http", "url": "https://api.example.com/a", "headers": {"Authorization": token}},
                    "srv_two": {"type": "http", "url": "https://api.example.com/b", "headers": {"Authorization": token}},
                }
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] == 2
    # The identical token is stored exactly once; both servers reference it.
    assert create_secret.await_count == 1
    assert len(body["secrets_created"]) == 1
    ref = f"${{vault:{body['secrets_created'][0]}}}"
    for call in insert.await_args_list:
        assert call.kwargs["headers"] == {"Authorization": ref}


@pytest.mark.asyncio
async def test_import_cap_mid_server_lands_nothing(client):
    """The vault cap firing on a server's SECOND secret aborts the whole entry.

    The first secret went into the same transaction, so Postgres discards it —
    there is nothing to compensate, and the entry leaves no server row.
    """
    ws = _ws()
    base = _agent_config([])
    create_secret = AsyncMock(
        side_effect=[None, ValueError("vault secret cap (50) reached")]
    )
    insert = AsyncMock()
    after = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.services.mcp_import.create_user_secret", new=create_secret),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=insert),
        patch("src.server.app.mcp_servers.after_secrets_changed", new=after),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    "capper": {
                        "type": "http",
                        "url": "https://api.example.com/mcp",
                        "headers": {
                            "Authorization": "OPAQUE-TOKEN-AAAAAAAAAA",
                            "X-Api-Key": "OPAQUE-TOKEN-BBBBBBBBBB",
                        },
                    }
                }
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["results"][0]["status"] == "error"
    assert body["secrets_created"] == []
    # Both secrets were attempted on the one connection; neither survives, and
    # the server row was never reached.
    assert create_secret.await_count == 2
    insert.assert_not_awaited()
    assert _names_converged(after) == []


@pytest.mark.asyncio
async def test_import_raced_name_keeps_no_extracted_secrets(client):
    """A name a concurrent create took after the pre-check makes the account
    writer raise, which rolls the entry's transaction back, so its extracted
    secrets are never committed."""
    ws = _ws()
    base = _agent_config([])
    create_secret = AsyncMock()
    after = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.services.mcp_import.create_user_secret", new=create_secret),
        patch(
            "src.server.app.mcp_servers.create_workspace_catalog_server",
            new=AsyncMock(side_effect=ValueError("MCP catalog server 'racer' already exists for this user")),
        ),
        patch("src.server.app.mcp_servers.after_secrets_changed", new=after),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    "racer": {
                        "type": "http",
                        "url": "https://api.example.com/mcp",
                        "headers": {"Authorization": "OPAQUE-TOKEN-CCCCCCCCCC"},
                    }
                }
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["results"][0]["status"] == "error"
    assert body["created"] == 0
    assert body["secrets_created"] == []
    create_secret.assert_awaited_once()
    assert create_secret.await_args.args[1] == "RACER_AUTHORIZATION"
    assert _names_converged(after) == []


@pytest.mark.asyncio
async def test_import_skips_builtin_and_existing(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch(
            "src.server.app.mcp_servers.list_catalog_servers",
            new=AsyncMock(return_value=[_catalog_row(name="already_here")]),
        ),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=AsyncMock()) as ins,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    "builtin_search": {"command": "npx"},
                    "already_here": {"command": "uvx"},
                }
            },
        )
    assert resp.status_code == 200
    by_name = {r["name"]: r for r in resp.json()["results"]}
    assert by_name["builtin_search"]["status"] == "skipped"
    # A name is taken by the account row, whichever workspace it is on in.
    assert by_name["already_here"]["status"] == "exists"
    assert by_name["already_here"]["reason"] == "already exists in your Plugins"
    # Neither pre-existing/collision row should reach the DB insert.
    assert ins.await_count == 0


@pytest.mark.asyncio
async def test_import_counts_against_the_account_cap(client):
    """The rows land on the account, so the cap is the Plugins one, counted
    across every server the user has rather than this workspace's."""
    ws = _ws()
    full = [_catalog_row(name=f"srv_{i}") for i in range(MAX_CATALOG_SERVERS_PER_USER)]
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.app.mcp_servers.list_catalog_servers", new=AsyncMock(return_value=full)),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=AsyncMock()) as ins,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={"mcpServers": {"one_more": {"command": "npx", "args": ["-y", "pkg"]}}},
        )
    assert resp.status_code == 200
    result = resp.json()["results"][0]
    assert result["status"] == "error"
    assert result["error"] == f"Plugins server cap ({MAX_CATALOG_SERVERS_PER_USER}) reached"
    ins.assert_not_awaited()


@pytest.mark.asyncio
async def test_import_reports_invalid_server_without_aborting(client):
    ws = _ws()
    base = _agent_config([])
    insert = AsyncMock(side_effect=_created_row)
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.services.mcp_import.create_user_secret", new=AsyncMock()),
        patch("src.server.app.mcp_servers.create_workspace_catalog_server", new=insert),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={
                "mcpServers": {
                    # Private-IP URL → rejected by the URL policy after parse.
                    "bad_one": {"type": "http", "url": "https://10.0.0.5/mcp"},
                    "good_one": {"command": "npx", "args": ["-y", "pkg"]},
                }
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    by_name = {r["name"]: r for r in body["results"]}
    assert by_name["bad_one"]["status"] == "invalid"
    assert by_name["good_one"]["status"] == "created"
    assert body["created"] == 1


@pytest.mark.asyncio
async def test_import_empty_payload_422(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/import",
            json={"mcpServers": {}},
        )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# PUT edit: the account row, reached from a workspace
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_edit_builtin_409(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    apply = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.services.mcp_saves.apply_catalog_edit", new=apply),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search",
            json={"name": "builtin_search", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    apply.assert_not_awaited()


@pytest.mark.asyncio
async def test_edit_brokerage_409(client):
    """A brokerage's row is written by its connect flow on Plugins, which owns
    the shipped address and the consent it carries."""
    ws = _ws()
    apply = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.services.mcp_saves.apply_catalog_edit", new=apply),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/robinhood",
            json={"name": "robinhood", "transport": "http", "url": "https://api.example.com/mcp"},
        )
    assert resp.status_code == 409
    assert resp.json()["detail"] == "Manage this brokerage connection from Plugins"
    apply.assert_not_awaited()


@pytest.mark.asyncio
async def test_edit_404_for_a_name_this_workspace_does_not_resolve(client):
    ws = _ws()
    apply = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.database.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch("src.server.services.mcp_saves.apply_catalog_edit", new=apply),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/ghost",
            json={"name": "ghost", "transport": "http", "url": "https://api.example.com/mcp"},
        )
    assert resp.status_code == 404
    apply.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace_rows, enabled",
    [
        ([], True),
        # Switched off here: still the same account row, still editable.
        ([{"name": "remote_server", "source": "user", "enabled": False, "config": None}], False),
    ],
)
async def test_edit_lands_on_the_account_row(client, workspace_rows, enabled):
    """There is one definition per name, so this is the Plugins edit reached
    from a workspace: it rewrites the account row everywhere the server is on,
    detaches a plugin-owned row the way the Plugins edit does, and is checked
    as the Plugins edit is, a command server on this workspace's computer."""
    ws = _ws()
    body = {"name": "remote_server", "transport": "http", "url": "https://api.example.com/mcp"}
    apply = AsyncMock(return_value=CatalogEdit(row=_catalog_row(), detached_from_plugin=None))
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch(
            "src.server.database.mcp_servers.list_workspace_servers",
            new=AsyncMock(return_value=workspace_rows),
        ),
        patch(
            "src.server.database.mcp_servers.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row()),
        ),
        patch("src.server.services.mcp_saves.apply_catalog_edit", new=apply),
        patch("src.server.services.mcp_saves.schedule_proactive_apply") as sched,
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server",
            json=body,
        )
    assert resp.status_code == 200
    assert resp.json() == {"name": "remote_server", "source": "user", "enabled": enabled}
    apply.assert_awaited_once_with(
        USER, "remote_server", McpServerInput(**body).to_catalog_fields(),
        detach_plugin=True,
        check=McpServerInput(**body),
        workspace_id=ws["workspace_id"],
    )
    sched.assert_called_once_with(ws["workspace_id"], USER)


@pytest.mark.asyncio
async def test_edit_writes_nothing_when_the_edit_fails_its_check(client, checks):
    ws = _ws()
    checks.catalog.side_effect = _rejected
    update = AsyncMock()
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.database.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch(
            "src.server.database.mcp_servers.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row()),
        ),
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row()),
        ),
        patch("src.server.services.mcp_catalog.update_catalog_server", new=update),
        patch("src.server.services.mcp_saves.schedule_proactive_apply") as sched,
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server",
            json={"name": "remote_server", "transport": "http", "url": "https://moved.example.com/mcp"},
        )
    assert resp.status_code == 422
    assert resp.json()["detail"]["code"] == "mcp_check_failed"
    assert checks.catalog.await_args.kwargs["workspace_id"] == ws["workspace_id"]
    update.assert_not_awaited()
    sched.assert_not_called()


@pytest.mark.asyncio
async def test_edit_warns_when_it_detaches_a_plugin_row(client):
    ws = _ws()
    apply = AsyncMock(
        return_value=CatalogEdit(row=_catalog_row(), detached_from_plugin="acme-research")
    )
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.database.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch(
            "src.server.database.mcp_servers.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row()),
        ),
        patch("src.server.services.mcp_saves.apply_catalog_edit", new=apply),
        patch("src.server.services.mcp_saves.schedule_proactive_apply"),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server",
            json={"name": "remote_server", "transport": "http", "url": "https://api.example.com/mcp"},
        )
    assert resp.status_code == 200
    # The same sentence the Plugins edit returns, so the two read as one act.
    assert resp.json()["warnings"] == [detach_warning("acme-research")]


@pytest.mark.asyncio
async def test_edit_keeps_a_name_the_sandbox_reserves(client):
    """A row saved before its name was reserved stays editable from here too."""
    ws = _ws()
    row = _catalog_row(name="class")
    apply = AsyncMock(return_value=CatalogEdit(row=row, detached_from_plugin=None))
    with (
        patch("src.server.services.mcp_saves.get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.database.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch("src.server.database.mcp_servers.get_catalog_server", new=AsyncMock(return_value=row)),
        patch("src.server.services.mcp_saves.apply_catalog_edit", new=apply),
        patch("src.server.services.mcp_saves.schedule_proactive_apply"),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/class",
            json={"name": "class", "transport": "http", "url": "https://api.example.com/mcp"},
        )
    assert resp.status_code == 200
    assert apply.await_args.args[1] == "class"


# ---------------------------------------------------------------------------
# PATCH enabled — builtin disable-marker semantics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_patch_disable_builtin_upserts_marker(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.upsert_workspace_server", new=AsyncMock(return_value={})) as up,
        patch("src.server.app.mcp_servers.delete_workspace_server", new=AsyncMock(return_value=True)) as dele,
        patch(
            "src.server.app.mcp_servers._sync_sandbox_grants_now", new=AsyncMock()
        ) as grants,
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/enabled",
            json={"enabled": False},
        )
    assert resp.status_code == 200
    _, kwargs = up.await_args
    assert kwargs["source"] == "builtin" and kwargs["enabled"] is False
    assert dele.await_count == 0
    # The narrowing is only real once the grant is gone, so the 200 has to
    # stand behind it rather than behind a task that has not run yet.
    assert grants.await_count == 1


@pytest.mark.asyncio
async def test_patch_enable_builtin_deletes_marker(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch(
            "src.server.app.mcp_servers.account_disabled_builtins",
            new=AsyncMock(return_value=frozenset()),
        ),
        patch("src.server.app.mcp_servers.upsert_workspace_server", new=AsyncMock(return_value={})) as up,
        patch("src.server.app.mcp_servers.delete_workspace_server", new=AsyncMock(return_value=True)) as dele,
        patch(
            "src.server.app.mcp_servers._sync_sandbox_grants_now", new=AsyncMock()
        ) as grants,
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/enabled",
            json={"enabled": True},
        )
    assert resp.status_code == 200
    assert dele.await_count == 1 and up.await_count == 0
    assert grants.await_count == 1


@pytest.mark.asyncio
async def test_patch_enable_builtin_conflicts_when_disabled_for_user(client):
    """A workspace cannot re-enable what the account tier switched off: the
    marker delete would report success and change nothing."""
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch(
            "src.server.app.mcp_servers.account_disabled_builtins",
            new=AsyncMock(return_value=frozenset({"builtin_search"})),
        ),
        patch("src.server.app.mcp_servers.delete_workspace_server", new=AsyncMock(return_value=True)) as dele,
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/enabled",
            json={"enabled": True},
        )
    assert resp.status_code == 409
    assert dele.await_count == 0


@pytest.mark.asyncio
async def test_patch_enable_builtin_conflicts_when_its_bundle_is_off(client):
    """Same refusal when the subtraction came from the bundle, not the server.

    A bundle disable leaves no per-server row, so the workspace's marker
    delete would succeed and change nothing — success reported for a switch
    that did not move. Patched one layer lower than the test above on
    purpose: what is under test is that the router asks a question covering
    both routes, not that a stub answers.
    """
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    owners = ComponentOwners(servers={"builtin_search": "some-bundle"}, skills={})
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch(
            "src.server.database.account_disables.list_account_disables",
            new=AsyncMock(
                return_value=AccountDisables(
                    servers=frozenset(), bundles=frozenset({"some-bundle"})
                )
            ),
        ),
        patch(
            "src.server.services.plugins.bundled.component_owners",
            return_value=owners,
        ),
        patch("src.server.app.mcp_servers.delete_workspace_server", new=AsyncMock(return_value=True)) as dele,
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/enabled",
            json={"enabled": True},
        )
    assert resp.status_code == 409
    assert dele.await_count == 0


_TOMBSTONE = {
    "workspace_mcp_server_id": "t-1", "name": "remote_server", "source": "user",
    "enabled": False, "config": None,
}


@pytest.mark.parametrize(
    ("workspace_rows", "catalog", "untombstoned", "status"),
    [
        # Tombstoned here: the drop reads the account's switch under its lock.
        ([_TOMBSTONE], None, "enabled", 200),
        ([_TOMBSTONE], None, "account_off", 409),
        # Deleted since, or replaced by a server scoped off here.
        ([_TOMBSTONE], None, "gone", 404),
        # Not tombstoned: the no-op enable is no success while its plugin is off.
        ([], _catalog_row(), None, 200),
        ([], _catalog_row(plugin_id="p-1", plugin_name="acme", plugin_enabled=False), None, 409),
    ],
)
@pytest.mark.asyncio
async def test_patch_enable_user_server_only_where_the_account_runs_it(
    client, workspace_rows, catalog, untombstoned, status
):
    ws = _ws()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch(
            "src.server.database.mcp_servers.list_workspace_servers",
            new=AsyncMock(return_value=workspace_rows),
        ),
        patch("src.server.database.mcp_servers.get_catalog_server", new=AsyncMock(return_value=catalog)),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=catalog)),
        patch(
            "src.server.app.mcp_servers.untombstone_user_server",
            new=AsyncMock(return_value=untombstoned),
        ) as untombstone,
        patch("src.server.app.mcp_servers.delete_workspace_server", new=AsyncMock(return_value=True)) as dele,
        patch("src.server.app.mcp_servers._sync_sandbox_grants_now", new=AsyncMock()),
        patch("src.server.app.mcp_servers.schedule_proactive_apply"),
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/enabled",
            json={"enabled": True},
        )
    assert resp.status_code == status, resp.text
    # The drop names the tombstone it classified, never just the name.
    if workspace_rows:
        untombstone.assert_awaited_once_with(
            ws["user_id"], ws["workspace_id"], "remote_server", "t-1"
        )
    else:
        assert untombstone.await_count == 0
    assert dele.await_count == 0


@pytest.mark.parametrize(
    ("workspace_rows", "catalog"),
    [
        # Inherited and on: the enable is a no-op, but no success either.
        ([], _catalog_row()),
        # Switched off here, on at the account.
        ([_TOMBSTONE], _catalog_row()),
    ],
)
@pytest.mark.asyncio
async def test_patch_enable_refuses_a_server_that_lacks_its_sign_in(
    client, checks, workspace_rows, catalog
):
    ws = _ws()
    checks.sign_in.return_value = SignInNeeded(server="remote_server", url=catalog["url"])
    untombstone = AsyncMock(return_value="enabled")
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch(
            "src.server.database.mcp_servers.list_workspace_servers",
            new=AsyncMock(return_value=workspace_rows),
        ),
        patch("src.server.database.mcp_servers.get_catalog_server", new=AsyncMock(return_value=catalog)),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=catalog)),
        patch("src.server.app.mcp_servers.untombstone_user_server", new=untombstone),
        patch("src.server.app.mcp_servers._sync_sandbox_grants_now", new=AsyncMock()) as synced,
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/enabled",
            json={"enabled": True},
        )
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "mcp_sign_in_required"
    assert resp.json()["detail"]["servers"] == [
        {"server": "remote_server", "url": catalog["url"]}
    ]
    untombstone.assert_not_awaited()
    synced.assert_not_awaited()


@pytest.mark.asyncio
async def test_patch_enable_reports_the_account_switch_before_a_sign_in(client, checks):
    """Signing in would not turn on a server the account keeps off, so that
    is the refusal the user hears first."""
    ws = _ws()
    checks.sign_in.return_value = SignInNeeded(server="remote_server", url="https://api.example.com/mcp")
    off = _catalog_row(enabled=False)
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch(
            "src.server.database.mcp_servers.list_workspace_servers",
            new=AsyncMock(return_value=[_TOMBSTONE]),
        ),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=off)),
        patch(
            "src.server.app.mcp_servers.untombstone_user_server",
            new=AsyncMock(return_value="account_off"),
        ),
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/enabled",
            json={"enabled": True},
        )
    assert resp.status_code == 409
    from src.server.app.mcp_servers import _ACCOUNT_DISABLED

    assert resp.json()["detail"] == _ACCOUNT_DISABLED
    checks.sign_in.assert_not_awaited()


@pytest.mark.asyncio
async def test_patch_disable_never_asks_for_a_sign_in(client, checks):
    ws = _ws()
    checks.sign_in.return_value = SignInNeeded(server="remote_server", url="https://api.example.com/mcp")
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.database.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch(
            "src.server.database.mcp_servers.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row(user_mcp_server_id="u-1")),
        ),
        patch("src.server.app.mcp_servers.tombstone_user_server", new=AsyncMock(return_value=True)),
        patch("src.server.app.mcp_servers._sync_sandbox_grants_now", new=AsyncMock()),
        patch("src.server.app.mcp_servers.schedule_proactive_apply"),
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/enabled",
            json={"enabled": False},
        )
    assert resp.status_code == 200
    checks.sign_in.assert_not_awaited()


@pytest.mark.parametrize(("tombstoned", "status"), [(True, 200), (False, 404)])
@pytest.mark.asyncio
async def test_patch_disable_user_server_names_the_server_it_read(client, tombstoned, status):
    """A disable switches off the server it classified; a same-name
    replacement it never read answers 404 and stays on."""
    ws = _ws()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", _agent_config([])),
        patch("src.server.database.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch(
            "src.server.database.mcp_servers.get_catalog_server",
            new=AsyncMock(return_value=_catalog_row(user_mcp_server_id="s-1")),
        ),
        patch(
            "src.server.app.mcp_servers.tombstone_user_server",
            new=AsyncMock(return_value=tombstoned),
        ) as tombstone,
        patch("src.server.app.mcp_servers._sync_sandbox_grants_now", new=AsyncMock()),
        patch("src.server.app.mcp_servers.schedule_proactive_apply"),
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/enabled",
            json={"enabled": False},
        )
    assert resp.status_code == status, resp.text
    tombstone.assert_awaited_once_with(
        ws["user_id"], ws["workspace_id"], "remote_server", "s-1"
    )


@pytest.mark.asyncio
async def test_patch_404_when_absent(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.database.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/ghost/enabled",
            json={"enabled": False},
        )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Discover — debounce + sandbox=None pending + builtin reject
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_builtin_409(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/discover"
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_discover_debounce_returns_cached(client):
    ws = _ws()
    base = _agent_config([])
    user_srv = _stdio_user_server()
    resolved = resolved_mcp(inherited=[user_srv])
    fresh = {
        "server_name": "stdio_server", "status": "ok", "tools": [], "error": "",
        "config_hash": mcp_discovery_fingerprint(user_srv),
        "discovered_at": datetime.now(timezone.utc).isoformat(),
    }
    discover = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[fresh])),
        patch("src.server.services.mcp_discovery.discover_and_cache", new=discover),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/stdio_server/discover"
        )
    assert resp.status_code == 200
    # Cache 'ok' is surfaced as 'connected' (same enum as the effective list).
    assert resp.json()["server"]["status"] == "connected"
    assert discover.await_count == 0  # debounced — no re-run


@pytest.mark.asyncio
async def test_discover_runs_when_stale_and_stopped_yields_pending(client):
    ws = _ws(status="stopped")
    base = _agent_config([])
    user_srv = _stdio_user_server()
    resolved = resolved_mcp(inherited=[user_srv])
    stale = {
        "server_name": "stdio_server", "status": "ok", "tools": [], "error": "",
        "config_hash": mcp_discovery_fingerprint(user_srv),
        "discovered_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
    }
    pending_row = {
        "server_name": "stdio_server", "status": "pending", "tools": [], "error": "",
        "config_hash": mcp_discovery_fingerprint(user_srv), "discovered_at": NOW.isoformat(),
    }
    discover = AsyncMock(return_value=[pending_row])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[stale])),
        patch("src.server.services.mcp_discovery.discover_and_cache", new=discover),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/stdio_server/discover"
        )
    assert resp.status_code == 200
    assert resp.json()["server"]["status"] == "pending"
    # Stopped workspace ⇒ sandbox=None passed to discover_and_cache.
    args, _ = discover.await_args
    assert args[1] is None


@pytest.mark.asyncio
async def test_discover_unknown_server_404(client):
    ws = _ws()
    base = _agent_config([])
    resolved = resolved_mcp()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/ghost/discover"
        )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Ownership guards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_owner_403(client):
    ws = _ws(user_id="someone-else")
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock()),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_workspace_not_found_404(client):
    with patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=None)):
        resp = await client.get(f"/api/v1/workspaces/{uuid.uuid4()}/mcp/servers")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# A toggle on a row that is on no computer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_toggle_on_home_before_its_first_turn_syncs_grants_without_a_warm(
    client,
):
    """Home before its first turn is on no computer, so there is no sandbox to
    warm and the acquire would only fail. Its grants are synced all the same."""
    ws = _ws(status="unbound")
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.upsert_workspace_server", new=AsyncMock(return_value={})),
        patch("src.server.app.mcp_servers._sync_sandbox_grants_now", new=AsyncMock()) as synced,
        patch("src.server.app.mcp_servers.schedule_proactive_apply") as warm,
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/enabled",
            json={"enabled": False},
        )
    assert resp.status_code == 200
    synced.assert_awaited_once_with(ws["workspace_id"], USER)
    warm.assert_not_called()
