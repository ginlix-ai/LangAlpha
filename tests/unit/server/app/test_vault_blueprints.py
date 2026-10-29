"""Tests for GET /api/v1/mcp/vault/blueprints (config-declared blueprints).

Covers filtering (enabled-only, already-set), dedup across servers,
startup-race handling, and `remaining_slots` math against the user's vault.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from ptc_agent.config.core import MCPServerConfig, VaultBlueprint
from src.server.database.user_vault_secrets import MAX_SECRETS_PER_USER
from tests.conftest import create_test_app

URL = "/api/v1/mcp/vault/blueprints"


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


@contextmanager
def _vault(cfg, secret_names=frozenset()):
    """The user's stored secret names and the process config, no plugins."""
    with (
        patch(
            "src.server.database.user_vault_secrets.get_user_secret_names",
            new=AsyncMock(return_value=set(secret_names)),
        ),
        patch(
            "src.server.database.plugins.list_plugins",
            new=AsyncMock(return_value=[]),
        ),
        patch("src.server.app.setup.agent_config", cfg),
    ):
        yield


@pytest_asyncio.fixture
async def client():
    from src.server.app.user_vault import router

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
    with _vault(_agent_config([_srv(blueprints=[_bp()])])):
        resp = await client.get(URL)

    assert resp.status_code == 200
    body = resp.json()
    assert body["remaining_slots"] == MAX_SECRETS_PER_USER
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
    cfg = _agent_config([_srv(blueprints=[_bp()])])
    with _vault(cfg, {"X_BEARER_TOKEN"}):
        resp = await client.get(URL)

    assert resp.status_code == 200
    body = resp.json()
    assert body["blueprints"] == []
    assert body["remaining_slots"] == MAX_SECRETS_PER_USER - 1


# ---------------------------------------------------------------------------
# Filter: disabled servers' blueprints are excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disabled_server_blueprints_excluded(client):
    cfg = _agent_config([
        _srv(name="x_api", enabled=False, blueprints=[_bp()]),
        _srv(name="other", enabled=True, blueprints=[]),
    ])
    with _vault(cfg):
        resp = await client.get(URL)

    assert resp.status_code == 200
    assert resp.json()["blueprints"] == []


# ---------------------------------------------------------------------------
# Dedup: first-declaration wins on metadata; sources lists both origins
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_blueprint_name_dedupes_first_wins(client):
    first = _bp(description="FIRST", docs_url="https://first.example", regex="^first$")
    second = _bp(description="SECOND", docs_url="https://second.example", regex="^second$")
    cfg = _agent_config([
        _srv(name="server_a", blueprints=[first]),
        _srv(name="server_b", blueprints=[second]),
    ])
    with _vault(cfg):
        resp = await client.get(URL)

    body = resp.json()
    assert len(body["blueprints"]) == 1
    bp = body["blueprints"][0]
    assert bp["description"] == "FIRST"  # first wins
    assert bp["docs_url"] == "https://first.example"
    assert bp["regex"] == "^first$"
    assert bp["sources"] == ["server_a", "server_b"]


# ---------------------------------------------------------------------------
# remaining_slots edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remaining_slots_at_cap_is_zero(client):
    full_vault = {f"SECRET_{i:02d}" for i in range(MAX_SECRETS_PER_USER)}
    with _vault(_agent_config([_srv(blueprints=[])]), full_vault):
        resp = await client.get(URL)

    assert resp.json()["remaining_slots"] == 0


# ---------------------------------------------------------------------------
# Startup race — agent_config is None before lifespan completes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_race_agent_config_none(client):
    with _vault(None):
        resp = await client.get(URL)

    assert resp.status_code == 200
    assert resp.json() == {"blueprints": [], "remaining_slots": MAX_SECRETS_PER_USER}


# ---------------------------------------------------------------------------
# No MCP servers configured — valid empty state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_mcp_servers_list(client):
    with _vault(_agent_config([])):
        resp = await client.get(URL)

    assert resp.status_code == 200
    assert resp.json() == {"blueprints": [], "remaining_slots": MAX_SECRETS_PER_USER}


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
