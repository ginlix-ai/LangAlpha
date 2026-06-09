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

from src.ptc_agent.config.core import MCPServerConfig
from src.server.app.mcp_servers import _derive_status
from src.server.handlers.chat.mcp_config import ResolvedMCP
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
    status, err = _derive_status(
        origin="builtin", enabled=True, env_refs=[], header_refs=[],
        secret_names=set(), schema_row=None,
    )
    assert status == "connected" and err == ""


def test_status_needs_secret_when_ref_missing():
    status, _ = _derive_status(
        origin="workspace", enabled=True, env_refs=[], header_refs=["API_KEY"],
        secret_names=set(), schema_row={"status": "ok", "tools": []},
    )
    assert status == "needs_secret"


def test_status_connected_when_schema_ok_and_secret_present():
    status, _ = _derive_status(
        origin="workspace", enabled=True, env_refs=[], header_refs=["API_KEY"],
        secret_names={"API_KEY"}, schema_row={"status": "ok", "tools": []},
    )
    assert status == "connected"


def test_status_error_passes_text():
    status, err = _derive_status(
        origin="workspace", enabled=True, env_refs=[], header_refs=[],
        secret_names=set(), schema_row={"status": "error", "error": "boom"},
    )
    assert status == "error" and err == "boom"


def test_status_pending_when_no_schema_row():
    status, _ = _derive_status(
        origin="workspace", enabled=True, env_refs=[], header_refs=[],
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
         "error": "", "config_version": 3, "discovered_at": NOW.isoformat()},
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
    # Literal vault-ref string is never echoed as a raw header value.
    assert "Authorization" not in resp.text


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
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch(
            "src.server.app.mcp_servers.upsert_workspace_server",
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
async def test_add_server_happy(client):
    ws = _ws()
    base = _agent_config([])
    row = {"name": "new_server", "source": "workspace", "enabled": True}
    with (
        patch("src.server.app.mcp_servers.db_get_workspace", new=AsyncMock(return_value=ws)),
        patch("src.server.app.setup.agent_config", base),
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch("src.server.app.mcp_servers.upsert_workspace_server", new=AsyncMock(return_value=row)) as up,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"name": "new_server", "transport": "stdio", "command": "npx", "args": ["-y", "pkg"]},
        )
    assert resp.status_code == 201
    assert up.await_count == 1
    # source forced to 'workspace', enabled True
    _, kwargs = up.await_args
    assert kwargs["source"] == "workspace" and kwargs["enabled"] is True


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
        patch("src.server.app.mcp_servers.list_workspace_servers", new=AsyncMock(return_value=[])),
        patch("src.server.app.mcp_servers.upsert_workspace_server", new=AsyncMock(return_value=row)) as up,
    ):
        resp = await client.post(
            f"/api/v1/workspaces/{ws['workspace_id']}/mcp/servers",
            json={"from_template": "tmpl_server"},
        )
    assert resp.status_code == 201
    _, kwargs = up.await_args
    assert kwargs["config"]["url"] == "https://api.example.com/mcp"
    assert kwargs["config"]["headers"] == {"Authorization": "${vault:API_KEY}"}


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
        "config_version": 3, "discovered_at": datetime.now(timezone.utc).isoformat(),
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
    assert resp.json()["server"]["status"] == "ok"
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
        "config_version": 3,
        "discovered_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
    }
    pending_row = {
        "server_name": "remote_server", "status": "pending", "tools": [], "error": "",
        "config_version": 3, "discovered_at": NOW.isoformat(),
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
