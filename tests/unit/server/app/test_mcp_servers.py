"""Tests for the per-workspace MCP server router (app/mcp_servers.py).

Covers the effective list + status derivation, 409 builtin collision, template
copy, PATCH builtin disable-marker semantics, masked env/header values, and the
debounced discover probe. DB + WorkspaceManager are mocked; the resolver is the
real chokepoint fed a mocked DB.
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
from src.server.services.mcp_config import ResolvedMCP
from src.server.services.mcp_discovery import mcp_discovery_fingerprint
from tests.conftest import create_test_app

NOW = datetime.now(timezone.utc)
USER = "test-user-123"


def _ws(workspace_id=None, user_id=USER, status="running", **overrides):
    return {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": "Test Workspace",
        "status": status,
        "config": None,
        "mcp_config_version": 3,
        **overrides,
    }


def _builtin(name="builtin_search"):
    return MCPServerConfig(name=name, transport="stdio", command="npx", source="builtin")


def _user_server(name="remote_server", **kw):
    return MCPServerConfig(
        name=name,
        transport="http",
        url="https://api.example.com/mcp",
        headers=kw.pop("headers", {}),
        source="workspace",
        **kw,
    )


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


# ---------------------------------------------------------------------------
# Status derivation (pure unit)
# ---------------------------------------------------------------------------


def test_status_builtin_is_connected():
    status, err, missing = _derive_status(
        origin="builtin", env_refs=[], header_refs=[],
        secret_names=set(), schema_row=None,
    )
    assert status == "connected" and err == "" and missing == []


def test_status_needs_secret_when_ref_missing():
    status, _, missing = _derive_status(
        origin="workspace", env_refs=[], header_refs=["API_KEY"],
        secret_names=set(), schema_row={"status": "ok", "tools": []},
    )
    assert status == "needs_secret"
    assert missing == ["API_KEY"]


def test_status_connected_when_schema_ok_and_secret_present():
    status, _, missing = _derive_status(
        origin="workspace", env_refs=[], header_refs=["API_KEY"],
        secret_names={"API_KEY"}, schema_row={"status": "ok", "tools": []},
    )
    assert status == "connected"
    assert missing == []


def test_status_error_passes_text():
    status, err, _ = _derive_status(
        origin="workspace", env_refs=[], header_refs=[],
        secret_names=set(), schema_row={"status": "error", "error": "boom"},
    )
    assert status == "error" and err == "boom"


def test_status_pending_when_no_schema_row():
    status, _, _ = _derive_status(
        origin="workspace", env_refs=[], header_refs=[],
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
    resolved = ResolvedMCP(
        servers=[_builtin(), user_srv],
        builtin_names=frozenset({"builtin_search"}),
        user_names=frozenset({"remote_server"}),
        version=3,
    )
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
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value={"API_KEY"})),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=schema_rows)),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    body = resp.json()
    assert body["sandbox_running"] is True
    assert body["sandbox_warming"] is False  # already running ⇒ not warming
    assert body["max_servers"] == 20
    assert body["config_version"] == 3
    by_name = {s["name"]: s for s in body["servers"]}

    bi = by_name["builtin_search"]
    assert bi["origin"] == "builtin" and bi["status"] == "connected"
    assert bi["editable"] is False and bi["deletable"] is False

    us = by_name["remote_server"]
    assert us["origin"] == "workspace" and us["status"] == "connected"
    assert us["header_refs"] == ["API_KEY"]
    assert us["tool_count"] == 1
    # tool_exposure_mode is non-null: a config None coalesces to "summary".
    assert us["tool_exposure_mode"] == "summary"
    assert bi["tool_exposure_mode"] == "summary"
    # The stored reference map is echoed for workspace servers so the edit
    # form can round-trip it — values are ref strings, never resolved secrets.
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
    resolved = ResolvedMCP(
        servers=[_builtin()],
        builtin_names=frozenset({"builtin_search"}),
        user_names=frozenset(),
        version=3,
    )
    wm = MagicMock()
    wm.get_applied_mcp_config_version.return_value = 2  # behind the saved version
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
        patch("src.server.app.mcp_servers.WorkspaceManager.get_instance", return_value=wm),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    body = resp.json()
    assert body["config_version"] == 3
    # applied (2) < saved (3) ⇒ the UI reads "applying", not "synced".
    assert body["applied_config_version"] == 2
    wm.get_applied_mcp_config_version.assert_called_once_with(ws["workspace_id"])


@pytest.mark.asyncio
async def test_list_surfaces_sandbox_warming(client):
    """A workspace transitioning up (status 'starting') reports sandbox_warming
    so the UI keeps polling and shows "Starting workspace…" rather than resting
    on a stale stopped state."""
    ws = _ws(status="starting")
    base = _agent_config([_builtin()])
    resolved = ResolvedMCP(
        servers=[_builtin()],
        builtin_names=frozenset({"builtin_search"}),
        user_names=frozenset(),
        version=1,
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
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
    resolved = ResolvedMCP(
        servers=[user_srv], builtin_names=frozenset(),
        user_names=frozenset({"remote_server"}),
        version=99,  # the version has long since moved on from when it was cached
    )
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
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
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
    resolved = ResolvedMCP(
        servers=[user_srv], builtin_names=frozenset(),
        user_names=frozenset({"remote_server"}), version=3,
    )
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
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
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
    resolved = ResolvedMCP(
        servers=[_builtin()],
        builtin_names=frozenset({"builtin_search"}),
        user_names=frozenset(),
        version=4,
        disabled_builtin_names=frozenset({"builtin_disabled"}),
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
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
    assert row["editable"] is False and row["deletable"] is False
    assert row["tool_count"] == 0


@pytest.mark.asyncio
async def test_list_keeps_disabled_workspace_server_visible(client):
    # A disabled workspace server is dropped from the effective set but must
    # still render (greyed, with its toggle) so it can be re-enabled.
    ws = _ws()
    base = _agent_config([_builtin()])
    disabled_srv = _user_server(name="disabled_remote")
    resolved = ResolvedMCP(
        servers=[_builtin()],
        builtin_names=frozenset({"builtin_search"}),
        user_names=frozenset(),
        version=5,
        disabled_workspace_servers=[disabled_srv],
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    assert resp.status_code == 200
    by_name = {s["name"]: s for s in resp.json()["servers"]}
    row = by_name["disabled_remote"]
    assert row["origin"] == "workspace"
    assert row["enabled"] is False
    assert row["status"] == "disabled"
    # Still fully manageable: re-enable toggle, edit, delete, promote.
    assert row["editable"] is True and row["deletable"] is True


@pytest.mark.asyncio
async def test_list_needs_secret_surfaces_missing(client):
    ws = _ws()
    base = _agent_config([])
    user_srv = _user_server(headers={"Authorization": "${vault:API_KEY}"})
    resolved = ResolvedMCP(
        servers=[user_srv], builtin_names=frozenset(),
        user_names=frozenset({"remote_server"}), version=3,
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.resolve_mcp_config", new=AsyncMock(return_value=resolved)),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.get_tool_schemas", new=AsyncMock(return_value=[])),
    ):
        resp = await client.get(f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers")

    s = resp.json()["servers"][0]
    assert s["status"] == "needs_secret"
    assert s["missing_secrets"] == ["API_KEY"]


# ---------------------------------------------------------------------------
# POST add — collision + cap + happy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_server_409_on_builtin_collision(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "builtin_search", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_add_server_409_when_over_cap(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch(
            "src.server.app.mcp_servers.insert_workspace_server",
            new=AsyncMock(side_effect=ValueError("Maximum of 20 MCP servers per workspace reached")),
        ),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "new_server", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    assert "Maximum of 20" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_add_server_409_when_name_exists(client):
    # A concurrent create losing the ON CONFLICT DO NOTHING race surfaces as
    # insert_workspace_server returning None — must be a 409, never a silent 201.
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=AsyncMock(return_value=None)) as ins,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "dupe_server", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409
    assert "already exists" in resp.json()["detail"]
    assert ins.await_count == 1


@pytest.mark.asyncio
async def test_add_server_happy(client):
    ws = _ws()
    base = _agent_config([])
    row = {"name": "new_server", "source": "workspace", "enabled": True}
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=AsyncMock(return_value=row)) as ins,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "new_server", "transport": "stdio", "command": "npx", "args": ["-y", "pkg"]},
        )
    assert resp.status_code == 201
    assert ins.await_count == 1
    args, kwargs = ins.await_args
    assert args[0] == ws["workspace_id"] and args[1] == "new_server"
    assert "config" in kwargs


@pytest.mark.asyncio
async def test_mutation_schedules_proactive_apply(client):
    """A successful add front-loads applying the new config to the running
    session (live before the next turn), not only on the next message."""
    ws = _ws()
    base = _agent_config([])
    row = {"name": "new_server", "source": "workspace", "enabled": True}
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=AsyncMock(return_value=row)),
        patch("src.server.app.mcp_servers._schedule_proactive_apply") as sched,
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

    from src.server.app import mcp_servers as mod

    wm = MagicMock()
    wm.proactively_apply_mcp_config = AsyncMock()
    monkeypatch.setattr(
        mod.WorkspaceManager, "get_instance", classmethod(lambda cls: wm)
    )
    monkeypatch.setattr(mod, "_PROACTIVE_APPLY_SETTLE_S", 0.02)

    for _ in range(5):
        mod._schedule_proactive_apply("ws-1", "user-1")
    await aio.sleep(0.1)
    assert wm.proactively_apply_mcp_config.await_count == 1

    mod._schedule_proactive_apply("ws-1", "user-1")
    await aio.sleep(0.1)
    assert wm.proactively_apply_mcp_config.await_count == 2
    assert "ws-1" not in mod._proactive_apply_pending


@pytest.mark.asyncio
async def test_schedule_session_mcp_refresh_drives_refresh(monkeypatch):
    """The probe's post-ok hook drives WorkspaceManager.refresh_session_mcp
    (undebounced — probes are explicit single user actions)."""
    import asyncio as aio

    from src.server.app import mcp_servers as mod

    wm = MagicMock()
    wm.refresh_session_mcp = AsyncMock()
    monkeypatch.setattr(
        mod.WorkspaceManager, "get_instance", classmethod(lambda cls: wm)
    )

    mod._schedule_session_mcp_refresh("ws-1", "user-1")
    await aio.sleep(0.05)

    wm.refresh_session_mcp.assert_awaited_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_add_server_rejects_bash_command(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "evil", "transport": "stdio", "command": "bash"},
        )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST add — from template (validates + copies)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_from_template_copies_and_revalidates(client):
    ws = _ws()
    base = _agent_config([])
    template = {
        "name": "tmpl_server", "transport": "http",
        "url": "https://api.example.com/mcp", "command": None, "args": [],
        "env": {}, "headers": {"Authorization": "${vault:API_KEY}"},
        "description": "d", "instruction": "i", "tool_exposure_mode": "summary",
    }
    row = {"name": "tmpl_server", "source": "workspace", "enabled": True}
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=template)),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=AsyncMock(return_value=row)) as ins,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"from_template": "tmpl_server"},
        )
    assert resp.status_code == 201
    _, kwargs = ins.await_args
    assert kwargs["config"]["url"] == "https://api.example.com/mcp"
    assert kwargs["config"]["headers"] == {"Authorization": "${vault:API_KEY}"}


@pytest.mark.asyncio
async def test_add_from_template_revalidation_422_string_detail(client):
    ws = _ws()
    base = _agent_config([])
    # A stored template that no longer passes the (tightened) URL policy.
    template = {
        "name": "tmpl_server", "transport": "http",
        "url": "https://100.64.0.1/mcp", "command": None, "args": [],
        "env": {}, "headers": {}, "description": "", "instruction": "",
        "tool_exposure_mode": "summary",
    }
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=template)),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"from_template": "tmpl_server"},
        )
    assert resp.status_code == 422
    # Template re-validation 422 detail is a flat string, like the direct path.
    assert isinstance(resp.json()["detail"], str)


@pytest.mark.asyncio
async def test_add_from_missing_template_404(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=None)),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"from_template": "nope"},
        )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST import — standard mcpServers blob → create + auto-extract secrets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_import_creates_and_extracts_secret(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    insert = AsyncMock(
        side_effect=lambda w, name, config=None: {
            "name": name, "source": "workspace", "enabled": True,
        }
    )
    create_secret = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 8))),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.create_secret_db", new=create_secret),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=insert),
        patch("src.server.app.mcp_servers._push_vault_to_sandbox", new=AsyncMock()) as push,
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
    row = body["results"][0]
    assert row["status"] == "created"
    assert row["name"] == "my_stock_mcp" and row["renamed"] is True
    # The literal Authorization token was vaulted, not stored inline.
    assert body["secrets_created"] == ["MY_STOCK_MCP_AUTHORIZATION"]
    create_secret.assert_awaited_once()
    sec_args, _ = create_secret.await_args
    assert sec_args[1] == "MY_STOCK_MCP_AUTHORIZATION"
    assert sec_args[2] == "EXAMPLE-OPAQUE-TOKEN-1234567890"
    # The inserted config references the vault, never the raw token.
    _, ins_kwargs = insert.await_args
    assert ins_kwargs["config"]["headers"] == {
        "Authorization": "${vault:MY_STOCK_MCP_AUTHORIZATION}"
    }
    # An authenticated remote server is set to use its secret during discovery,
    # otherwise tools/list returns 401.
    assert ins_kwargs["config"]["discovery_uses_secrets"] is True
    assert "EXAMPLE-OPAQUE-TOKEN-1234567890" not in resp.text
    push.assert_awaited_once()


@pytest.mark.asyncio
async def test_import_extracts_secret_in_args(client):
    """A credential in stdio args (``--api-key=TOKEN``) is vaulted on import and
    the arg rewritten to a ${vault:NAME} ref — never stored/echoed in plaintext
    (the generated client resolves the ref vault-only at spawn)."""
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    insert = AsyncMock(
        side_effect=lambda w, name, config=None: {
            "name": name, "source": "workspace", "enabled": True,
        }
    )
    create_secret = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 8))),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.create_secret_db", new=create_secret),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=insert),
        patch("src.server.app.mcp_servers._push_vault_to_sandbox", new=AsyncMock()),
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
    assert ins_kwargs["config"]["args"] == [
        "-y", "@foo/bar", "--api-key=${vault:MY_TOOL_API_KEY}"
    ]
    assert "EXAMPLE-OPAQUE-TOKEN-1234567890" not in resp.text


@pytest.mark.asyncio
async def test_import_dedupes_identical_token_across_servers(client):
    ws = _ws()
    base = _agent_config([])
    create_secret = AsyncMock()
    insert = AsyncMock(
        side_effect=lambda w, name, config=None: {
            "name": name, "source": "workspace", "enabled": True,
        }
    )
    token = "SHARED-OPAQUE-TOKEN-ABCDEFGHIJ"
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.create_secret_db", new=create_secret),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=insert),
        patch("src.server.app.mcp_servers._push_vault_to_sandbox", new=AsyncMock()),
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
        assert call.kwargs["config"]["headers"] == {"Authorization": ref}


@pytest.mark.asyncio
async def test_import_cap_mid_server_rolls_back_created_secrets(client):
    """The vault cap firing on a server's SECOND secret must delete the first —
    a failed server import never strands orphaned vault entries."""
    ws = _ws()
    base = _agent_config([])
    create_secret = AsyncMock(
        side_effect=[None, ValueError("vault secret cap (20) reached")]
    )
    delete_secret = AsyncMock()
    insert = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.create_secret_db", new=create_secret),
        patch("src.server.app.mcp_servers.delete_secret_db", new=delete_secret),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=insert),
        patch("src.server.app.mcp_servers._push_vault_to_sandbox", new=AsyncMock()) as push,
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
    # The one secret that DID get created was rolled back; no server row, no push.
    delete_secret.assert_awaited_once()
    assert delete_secret.await_args.args[0] == ws["workspace_id"]
    assert delete_secret.await_args.args[1] == "CAPPER_AUTHORIZATION"
    insert.assert_not_awaited()
    push.assert_not_awaited()


@pytest.mark.asyncio
async def test_import_existing_server_rolls_back_extracted_secrets(client):
    """A name that turns out to already exist (insert returns None) must not
    keep the secrets vaulted for it during extraction."""
    ws = _ws()
    base = _agent_config([])
    create_secret = AsyncMock()
    delete_secret = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.create_secret_db", new=create_secret),
        patch("src.server.app.mcp_servers.delete_secret_db", new=delete_secret),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=AsyncMock(return_value=None)),
        patch("src.server.app.mcp_servers._push_vault_to_sandbox", new=AsyncMock()) as push,
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
    assert body["results"][0]["status"] == "exists"
    assert body["secrets_created"] == []
    delete_secret.assert_awaited_once()
    assert delete_secret.await_args.args[1] == "RACER_AUTHORIZATION"
    push.assert_not_awaited()


@pytest.mark.asyncio
async def test_import_skips_builtin_and_existing(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    existing = [{"name": "already_here", "source": "workspace", "enabled": True, "config": {}}]
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=(existing, 9))),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=AsyncMock()) as ins,
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
    assert by_name["already_here"]["status"] == "exists"
    # Neither pre-existing/collision row should reach the DB insert.
    assert ins.await_count == 0


@pytest.mark.asyncio
async def test_import_reports_invalid_server_without_aborting(client):
    ws = _ws()
    base = _agent_config([])
    insert = AsyncMock(
        side_effect=lambda w, name, config=None: {
            "name": name, "source": "workspace", "enabled": True,
        }
    )
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.get_workspace_servers_and_version", new=AsyncMock(return_value=([], 9))),
        patch("src.server.app.mcp_servers.get_workspace_secret_names", new=AsyncMock(return_value=set())),
        patch("src.server.app.mcp_servers.create_secret_db", new=AsyncMock()),
        patch("src.server.app.mcp_servers.insert_workspace_server", new=insert),
        patch("src.server.app.mcp_servers._push_vault_to_sandbox", new=AsyncMock()),
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
# POST promote — workspace server UP into the user template catalog
# ---------------------------------------------------------------------------


def _promotable_row(name="remote_server", source="workspace", **config_over):
    """A workspace MCP row (source='workspace') with a full config blob."""
    config = {
        "name": name, "transport": "http",
        "url": "https://api.example.com/mcp", "command": None, "args": [],
        "env": {}, "headers": {"Authorization": "${vault:API_KEY}"},
        "description": "d", "instruction": "i", "tool_exposure_mode": "summary",
        "discovery_uses_secrets": True,
    }
    config.update(config_over)
    return {"name": name, "source": source, "enabled": True, "config": config}


def _catalog_row(name="remote_server", **kw):
    """A user_mcp_servers row shaped for catalog_row_to_response()."""
    base = {
        "name": name, "transport": "http", "command": None, "args": [],
        "url": "https://api.example.com/mcp", "env": {},
        "headers": {"Authorization": "${vault:API_KEY}"},
        "description": "d", "instruction": "i", "tool_exposure_mode": "summary",
        "created_at": None, "updated_at": None,
    }
    base.update(kw)
    return base


@pytest.mark.asyncio
async def test_promote_creates_template(client):
    ws = _ws()
    base = _agent_config([])
    create = AsyncMock(return_value=_catalog_row())
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[_promotable_row()])),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=None)),
        patch("src.server.app.mcp_servers.create_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/promote",
            json={"overwrite": False},
        )
    assert resp.status_code == 201
    body = resp.json()
    # Only the vault ref name surfaces — the secret value never travels.
    assert body["header_refs"] == ["API_KEY"]
    assert body["transport"] == "http"
    _, kwargs = create.await_args
    assert kwargs["url"] == "https://api.example.com/mcp"
    assert kwargs["headers"] == {"Authorization": "${vault:API_KEY}"}


@pytest.mark.asyncio
async def test_promote_409_when_template_exists_without_overwrite(client):
    ws = _ws()
    base = _agent_config([])
    create = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[_promotable_row()])),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=_catalog_row())),
        patch("src.server.app.mcp_servers.create_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/promote",
            json={"overwrite": False},
        )
    assert resp.status_code == 409
    assert "already exists" in resp.json()["detail"]
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_promote_overwrite_updates_existing(client):
    ws = _ws()
    base = _agent_config([])
    update = AsyncMock(return_value=_catalog_row(description="updated"))
    create = AsyncMock()
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[_promotable_row()])),
        patch("src.server.app.mcp_servers.update_catalog_server", new=update),
        patch("src.server.app.mcp_servers.create_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/promote",
            json={"overwrite": True},
        )
    assert resp.status_code == 201
    update.assert_awaited_once()
    # Overwrite path never falls through to create when a row was updated.
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_promote_404_when_server_absent(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/nope/promote",
            json={},
        )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_promote_409_on_builtin(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/promote",
            json={},
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_promote_404_for_disabled_builtin_marker(client):
    # A (source='builtin', config=NULL) disable-marker row is not a definition.
    ws = _ws()
    base = _agent_config([])
    marker = {"name": "remote_server", "source": "builtin", "enabled": False, "config": None}
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[marker])),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/promote",
            json={},
        )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_promote_409_when_catalog_over_cap(client):
    ws = _ws()
    base = _agent_config([])
    create = AsyncMock(side_effect=ValueError("Maximum of 50 MCP catalog servers per user reached"))
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[_promotable_row()])),
        patch("src.server.app.mcp_servers.get_catalog_server", new=AsyncMock(return_value=None)),
        patch("src.server.app.mcp_servers.create_catalog_server", new=create),
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/promote",
            json={},
        )
    assert resp.status_code == 409
    assert "Maximum of 50" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# PUT edit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_edit_builtin_409(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search",
            json={"name": "builtin_search", "transport": "stdio", "command": "npx"},
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_edit_workspace_row_happy(client):
    ws = _ws()
    base = _agent_config([])
    rows = [{"name": "remote_server", "source": "workspace", "enabled": True, "config": {}}]
    out = {"name": "remote_server", "source": "workspace", "enabled": True}
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=rows)),
        patch("src.server.app.mcp_servers.upsert_workspace_server", new=AsyncMock(return_value=out)) as up,
    ):
        resp = await client.put(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server",
            json={"name": "remote_server", "transport": "http", "url": "https://api.example.com/mcp"},
        )
    assert resp.status_code == 200
    assert up.await_count == 1


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
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/enabled",
            json={"enabled": False},
        )
    assert resp.status_code == 200
    _, kwargs = up.await_args
    assert kwargs["source"] == "builtin" and kwargs["enabled"] is False
    assert dele.await_count == 0


@pytest.mark.asyncio
async def test_patch_enable_builtin_deletes_marker(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.upsert_workspace_server", new=AsyncMock(return_value={})) as up,
        patch("src.server.app.mcp_servers.delete_workspace_server", new=AsyncMock(return_value=True)) as dele,
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search/enabled",
            json={"enabled": True},
        )
    assert resp.status_code == 200
    assert dele.await_count == 1 and up.await_count == 0


@pytest.mark.asyncio
async def test_patch_workspace_row_404_when_absent(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.set_workspace_server_enabled", new=AsyncMock(return_value=False)),
    ):
        resp = await client.patch(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/ghost/enabled",
            json={"enabled": False},
        )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_builtin_409(client):
    ws = _ws()
    base = _agent_config([_builtin("builtin_search")])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
    ):
        resp = await client.delete(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/builtin_search"
        )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_delete_workspace_row_happy(client):
    ws = _ws()
    base = _agent_config([])
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.delete_workspace_server", new=AsyncMock(return_value=True)),
    ):
        resp = await client.delete(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server"
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


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
    user_srv = _user_server()
    resolved = ResolvedMCP(
        servers=[user_srv], builtin_names=frozenset(),
        user_names=frozenset({"remote_server"}), version=3,
    )
    fresh = {
        "server_name": "remote_server", "status": "ok", "tools": [], "error": "",
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
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/discover"
        )
    assert resp.status_code == 200
    # Cache 'ok' is surfaced as 'connected' (same enum as the effective list).
    assert resp.json()["server"]["status"] == "connected"
    assert discover.await_count == 0  # debounced — no re-run


@pytest.mark.asyncio
async def test_discover_runs_when_stale_and_stopped_yields_pending(client):
    ws = _ws(status="stopped")
    base = _agent_config([])
    user_srv = _user_server()
    resolved = ResolvedMCP(
        servers=[user_srv], builtin_names=frozenset(),
        user_names=frozenset({"remote_server"}), version=3,
    )
    stale = {
        "server_name": "remote_server", "status": "ok", "tools": [], "error": "",
        "config_hash": mcp_discovery_fingerprint(user_srv),
        "discovered_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
    }
    pending_row = {
        "server_name": "remote_server", "status": "pending", "tools": [], "error": "",
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
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers/remote_server/discover"
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
    resolved = ResolvedMCP(
        servers=[], builtin_names=frozenset(), user_names=frozenset(), version=3,
    )
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
