"""Tests for GET /api/v1/workspaces/{id}/vault/blueprints.

Covers filtering (enabled-only, already-set), dedup across servers, auth guards,
startup-race handling, and `remaining_slots` math.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from src.ptc_agent.config.core import MCPServerConfig, VaultBlueprint
from tests.conftest import create_test_app

NOW = datetime.now(timezone.utc)


def _ws(workspace_id=None, user_id="test-user-123", **overrides):
    return {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": "Test Workspace",
        "description": None,
        "sandbox_id": "sandbox-abc",
        "status": "running",
        "mode": "ptc",
        "sort_order": 0,
        "is_pinned": False,
        "created_at": NOW,
        "updated_at": NOW,
        "last_activity_at": None,
        "stopped_at": None,
        "config": None,
        **overrides,
    }


def _agent_config(servers: list[MCPServerConfig]) -> MagicMock:
    """Build a minimal agent_config double with the given MCP servers."""
    cfg = MagicMock()
    cfg.mcp.servers = servers
    return cfg


def _bp(name="X_BEARER_TOKEN", label="X Bearer Token", **overrides) -> VaultBlueprint:
    return VaultBlueprint(
        name=name,
        label=label,
        description=overrides.pop("description", "docs"),
        docs_url=overrides.pop("docs_url", "https://console.x.com/"),
        regex=overrides.pop("regex", "^[A-Za-z0-9%_-]{20,}$"),
    )


def _srv(
    name="x_api",
    enabled=True,
    blueprints: list[VaultBlueprint] | None = None,
) -> MCPServerConfig:
    return MCPServerConfig(
        name=name,
        enabled=enabled,
        transport="stdio",
        vault_blueprints=blueprints or [],
    )


@pytest_asyncio.fixture
async def client():
    from src.server.app.vault import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Happy path — blueprint surfaces when vault is empty
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_blueprint_returned_when_key_not_set(client):
    ws = _ws()
    cfg = _agent_config([_srv(blueprints=[_bp()])])

    with (
        patch(
            "src.server.app.vault.db_get_workspace",
            new_callable=AsyncMock,
            return_value=ws,
        ),
        patch(
            "src.server.app.vault.get_workspace_secret_names",
            new_callable=AsyncMock,
            return_value=set(),
        ),
        patch("src.server.app.setup.agent_config", cfg),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["remaining_slots"] == 20
    assert len(body["blueprints"]) == 1
    bp = body["blueprints"][0]
    assert bp["name"] == "X_BEARER_TOKEN"
    assert bp["label"] == "X Bearer Token"
    assert bp["regex"] == "^[A-Za-z0-9%_-]{20,}$"
    assert bp["sources"] == ["x_api"]


# ---------------------------------------------------------------------------
# Filter: already-set keys are removed from the recommended list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_keys_are_filtered_out(client):
    ws = _ws()
    cfg = _agent_config([_srv(blueprints=[_bp()])])

    with (
        patch("src.server.app.vault.db_get_workspace", new_callable=AsyncMock, return_value=ws),
        patch(
            "src.server.app.vault.get_workspace_secret_names",
            new_callable=AsyncMock,
            return_value={"X_BEARER_TOKEN"},
        ),
        patch("src.server.app.setup.agent_config", cfg),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["blueprints"] == []
    assert body["remaining_slots"] == 19  # 20 - 1 set secret


# ---------------------------------------------------------------------------
# Filter: disabled servers' blueprints are excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disabled_server_blueprints_excluded(client):
    ws = _ws()
    cfg = _agent_config([
        _srv(name="x_api", enabled=False, blueprints=[_bp()]),
        _srv(name="other", enabled=True, blueprints=[]),
    ])

    with (
        patch("src.server.app.vault.db_get_workspace", new_callable=AsyncMock, return_value=ws),
        patch(
            "src.server.app.vault.get_workspace_secret_names",
            new_callable=AsyncMock,
            return_value=set(),
        ),
        patch("src.server.app.setup.agent_config", cfg),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    assert resp.status_code == 200
    assert resp.json()["blueprints"] == []


# ---------------------------------------------------------------------------
# Dedup: first-declaration wins on metadata; sources lists both origins
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_blueprint_name_dedupes_first_wins(client):
    ws = _ws()
    first = _bp(description="FIRST", docs_url="https://first.example", regex="^first$")
    second = _bp(description="SECOND", docs_url="https://second.example", regex="^second$")
    cfg = _agent_config([
        _srv(name="server_a", blueprints=[first]),
        _srv(name="server_b", blueprints=[second]),
    ])

    with (
        patch("src.server.app.vault.db_get_workspace", new_callable=AsyncMock, return_value=ws),
        patch(
            "src.server.app.vault.get_workspace_secret_names",
            new_callable=AsyncMock,
            return_value=set(),
        ),
        patch("src.server.app.setup.agent_config", cfg),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    body = resp.json()
    assert len(body["blueprints"]) == 1
    bp = body["blueprints"][0]
    assert bp["description"] == "FIRST"  # first wins
    assert bp["docs_url"] == "https://first.example"
    assert bp["regex"] == "^first$"
    assert bp["sources"] == ["server_a", "server_b"]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_workspace_not_found_returns_404(client):
    with patch(
        "src.server.app.vault.db_get_workspace",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get(f"/api/v1/workspaces/{uuid.uuid4()}/vault/blueprints")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_non_owner_returns_403(client):
    ws = _ws(user_id="someone-else")
    with patch(
        "src.server.app.vault.db_get_workspace",
        new_callable=AsyncMock,
        return_value=ws,
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# remaining_slots edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remaining_slots_at_cap_is_zero(client):
    ws = _ws()
    cfg = _agent_config([_srv(blueprints=[])])
    full_vault = {f"SECRET_{i:02d}" for i in range(20)}  # exactly the cap

    with (
        patch("src.server.app.vault.db_get_workspace", new_callable=AsyncMock, return_value=ws),
        patch(
            "src.server.app.vault.get_workspace_secret_names",
            new_callable=AsyncMock,
            return_value=full_vault,
        ),
        patch("src.server.app.setup.agent_config", cfg),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    assert resp.json()["remaining_slots"] == 0


# ---------------------------------------------------------------------------
# Startup race — agent_config is None before lifespan completes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_race_agent_config_none(client):
    ws = _ws()
    with (
        patch("src.server.app.vault.db_get_workspace", new_callable=AsyncMock, return_value=ws),
        patch(
            "src.server.app.vault.get_workspace_secret_names",
            new_callable=AsyncMock,
            return_value=set(),
        ),
        patch("src.server.app.setup.agent_config", None),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body == {"blueprints": [], "remaining_slots": 20}


# ---------------------------------------------------------------------------
# No MCP servers configured — valid empty state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_mcp_servers_list(client):
    ws = _ws()
    cfg = _agent_config([])  # no servers at all

    with (
        patch("src.server.app.vault.db_get_workspace", new_callable=AsyncMock, return_value=ws),
        patch(
            "src.server.app.vault.get_workspace_secret_names",
            new_callable=AsyncMock,
            return_value=set(),
        ),
        patch("src.server.app.setup.agent_config", cfg),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{ws['workspace_id']}/vault/blueprints"
        )

    assert resp.status_code == 200
    assert resp.json() == {"blueprints": [], "remaining_slots": 20}


# ---------------------------------------------------------------------------
# VaultBlueprint model validation (covers the YAML-load failure path)
# ---------------------------------------------------------------------------


def test_blueprint_rejects_malformed_name():
    with pytest.raises(ValidationError):
        VaultBlueprint(name="1STARTS_WITH_DIGIT", label="x")

    with pytest.raises(ValidationError):
        VaultBlueprint(name="has-dashes", label="x")

    with pytest.raises(ValidationError):
        VaultBlueprint(name="", label="x")

    with pytest.raises(ValidationError):
        VaultBlueprint(name="A" * 65, label="x")  # > max_length


def test_blueprint_requires_non_empty_label():
    with pytest.raises(ValidationError):
        VaultBlueprint(name="OK_NAME", label="")


def test_blueprint_rejects_overlong_label():
    with pytest.raises(ValidationError):
        VaultBlueprint(name="OK_NAME", label="L" * 81)


def test_blueprint_rejects_overlong_description():
    # max_length=256 matches CreateSecretRequest.description so pre-fill never
    # produces a body the create endpoint would 422 on.
    with pytest.raises(ValidationError):
        VaultBlueprint(name="OK_NAME", label="ok", description="D" * 257)


def test_blueprint_rejects_malformed_regex():
    with pytest.raises(ValidationError):
        VaultBlueprint(name="OK_NAME", label="ok", regex="[unterminated")


def test_blueprint_rejects_non_http_docs_url_schemes():
    # docs_url renders into an <a href>; javascript:/data: would execute on click.
    for bad in ("javascript:alert(1)", "data:text/html,x", "file:///etc/passwd", "ftp://x"):
        with pytest.raises(ValidationError):
            VaultBlueprint(name="OK_NAME", label="ok", docs_url=bad)


def test_blueprint_accepts_http_and_https_docs_url():
    for good in ("https://console.x.com/", "http://localhost:8080/docs"):
        bp = VaultBlueprint(name="OK_NAME", label="ok", docs_url=good)
        assert bp.docs_url == good


def test_blueprint_accepts_minimal_valid_input():
    bp = VaultBlueprint(name="MY_KEY", label="My Key")
    assert bp.name == "MY_KEY"
    assert bp.label == "My Key"
    assert bp.description == ""
    assert bp.docs_url is None
    assert bp.regex is None
