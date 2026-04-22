"""
Tests for the API Keys router (src/server/app/api_keys.py).

Covers GET/PUT/DELETE of BYOK API keys, key masking, and
the public /api/v1/models endpoint.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SUPPORTED_PROVIDERS = ["openai", "anthropic"]
PROVIDER_INFO_MAP = {
    "openai": {"display_name": "OpenAI", "access_type": "api_key", "brand_key": "openai"},
    "anthropic": {"display_name": "Anthropic", "access_type": "api_key", "brand_key": "anthropic"},
}

DB = "src.server.app.api_keys"


@pytest_asyncio.fixture
async def client():
    from src.server.app.api_keys import router
    import src.server.app.api_keys as api_keys_mod

    # Reset module-level cache so our mock takes effect
    api_keys_mod._BYOK_PROVIDERS_CACHE = None

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        with (
            patch(
                f"{DB}._get_supported_providers",
                return_value=SUPPORTED_PROVIDERS,
            ),
            patch(
                f"{DB}._get_provider_info_map",
                return_value=PROVIDER_INFO_MAP,
            ),
        ):
            yield c

    # Clean up cache
    api_keys_mod._BYOK_PROVIDERS_CACHE = None


# ---------------------------------------------------------------------------
# GET /api/v1/users/me/api-keys
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_api_keys(client):
    data = {
        "byok_enabled": True,
        "keys": {"openai": "sk-abc123def456xyz"},
        "base_urls": {},
    }
    with (
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=data,
        ),
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        resp = await client.get("/api/v1/users/me/api-keys")

    assert resp.status_code == 200
    body = resp.json()
    assert body["byok_enabled"] is True
    # Keys should be masked
    openai_prov = next(p for p in body["providers"] if p["provider"] == "openai")
    assert openai_prov["has_key"] is True
    assert openai_prov["masked_key"].startswith("sk-")
    assert "..." in openai_prov["masked_key"]


@pytest.mark.asyncio
async def test_get_api_keys_no_keys(client):
    data = {
        "byok_enabled": False,
        "keys": {},
        "base_urls": {},
    }
    with (
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=data,
        ),
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        resp = await client.get("/api/v1/users/me/api-keys")

    assert resp.status_code == 200
    body = resp.json()
    assert body["byok_enabled"] is False
    for prov in body["providers"]:
        assert prov["has_key"] is False
        assert prov["masked_key"] is None


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me/api-keys
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_api_keys_set_key(client):
    updated = {
        "byok_enabled": True,
        "keys": {"openai": "sk-newkey1234567890"},
        "base_urls": {},
    }
    with (
        patch(f"{DB}.set_byok_enabled", new_callable=AsyncMock),
        patch(f"{DB}.upsert_api_key", new_callable=AsyncMock),
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=updated,
        ),
        patch(
            "src.server.services.onboarding.maybe_complete_personalization",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={
                "byok_enabled": True,
                "api_keys": {"openai": "sk-newkey1234567890"},
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["byok_enabled"] is True


@pytest.mark.asyncio
async def test_update_api_keys_delete_key(client):
    """Setting a key to null should call delete_api_key."""
    updated = {
        "byok_enabled": True,
        "keys": {},
        "base_urls": {},
    }
    with (
        patch(f"{DB}.delete_api_key", new_callable=AsyncMock) as mock_delete,
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=updated,
        ),
        patch(
            "src.server.services.onboarding.maybe_complete_personalization",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"openai": None}},
        )

    assert resp.status_code == 200
    mock_delete.assert_awaited_once_with("test-user-123", "openai")


@pytest.mark.asyncio
async def test_update_api_keys_unsupported_provider(client):
    with patch(
        f"{DB}._get_custom_providers",
        new_callable=AsyncMock,
        return_value=[],
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"unknown_provider": "sk-1234567890abcdef"}},
        )

    assert resp.status_code == 400
    assert "Unsupported provider" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_api_keys_base_url_validation(client):
    """Base URL not starting with http(s) should be rejected."""
    resp = await client.put(
        "/api/v1/users/me/api-keys",
        json={"base_urls": {"openai": "ftp://invalid"}},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# DELETE /api/v1/users/me/api-keys/{provider}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_api_key(client):
    updated = {
        "byok_enabled": True,
        "keys": {},
        "base_urls": {},
    }
    with (
        patch(f"{DB}.delete_api_key", new_callable=AsyncMock) as mock_delete,
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=updated,
        ),
    ):
        resp = await client.delete("/api/v1/users/me/api-keys/openai")

    assert resp.status_code == 200
    mock_delete.assert_awaited_once_with("test-user-123", "openai")


@pytest.mark.asyncio
async def test_remove_api_key_unsupported_provider(client):
    with patch(
        f"{DB}._get_custom_providers",
        new_callable=AsyncMock,
        return_value=[],
    ):
        resp = await client.delete(
            "/api/v1/users/me/api-keys/unknown_provider"
        )

    assert resp.status_code == 400
    assert "Unsupported provider" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /api/v1/models (public, no auth)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_models(client):
    mock_models = {
        "openai": [{"name": "gpt-4o", "model_id": "gpt-4o"}],
    }
    mock_mc = MagicMock()
    mock_mc.get_display_name.side_effect = lambda p: p.title()
    mock_mc.get_model_metadata.return_value = {}

    mock_llm_cls = MagicMock()
    mock_llm_cls.get_model_config.return_value = mock_mc

    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

    agent_cfg = AgentConfig(
        llm=LLMConfig(name="gpt-4o", flash="gpt-4o-mini"),
        security=SecurityConfig(), logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test")), mcp=MCPConfig(), filesystem=FilesystemConfig(),
    )
    with (
        patch(
            "src.llms.llm.get_configured_llm_models",
            return_value=mock_models,
        ),
        patch(
            "src.llms.llm.LLM",
            mock_llm_cls,
        ),
        patch("src.server.app.setup.agent_config", agent_cfg),
    ):
        resp = await client.get("/api/v1/models")

    assert resp.status_code == 200
    body = resp.json()
    assert "models" in body
    assert "system_defaults" in body
    assert body["system_defaults"]["default_model"] == "gpt-4o"


# ---------------------------------------------------------------------------
# GET /api/v1/providers/{parent}/visible-models
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_visible_models_returns_parent_catalog(client):
    """Wizard endpoint returns the parent's visible models for one-click import."""
    mock_mc = MagicMock()
    mock_mc.get_provider_info.return_value = {"display_name": "OpenAI"}
    mock_mc.get_display_name.return_value = "OpenAI"
    mock_mc.get_model_config.side_effect = lambda name: {
        "gpt-4o": {"model_id": "gpt-4o", "input_modalities": ["text", "image"]},
        "gpt-4o-mini": {"model_id": "gpt-4o-mini"},
    }.get(name)

    mock_llm_cls = MagicMock()
    mock_llm_cls.get_model_config.return_value = mock_mc

    with (
        patch("src.llms.llm.LLM", mock_llm_cls),
        patch(
            "src.llms.llm.get_configured_llm_models",
            return_value={"openai": ["gpt-4o", "gpt-4o-mini"]},
        ),
    ):
        resp = await client.get("/api/v1/providers/openai/visible-models")

    assert resp.status_code == 200
    body = resp.json()
    assert body["parent"] == "openai"
    assert body["parent_display_name"] == "OpenAI"
    names = [m["name"] for m in body["models"]]
    assert names == ["gpt-4o", "gpt-4o-mini"]
    # input_modalities echoed for the wizard to render capability chips
    gpt4o = next(m for m in body["models"] if m["name"] == "gpt-4o")
    assert gpt4o["input_modalities"] == ["text", "image"]


@pytest.mark.asyncio
async def test_list_visible_models_unknown_parent_returns_404(client):
    """Unknown parent provider → 404, not 500."""
    mock_mc = MagicMock()
    mock_mc.get_provider_info.return_value = {}  # empty = unknown

    mock_llm_cls = MagicMock()
    mock_llm_cls.get_model_config.return_value = mock_mc

    with patch("src.llms.llm.LLM", mock_llm_cls):
        resp = await client.get("/api/v1/providers/unknown-brand/visible-models")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_visible_models_platform_only_rejected(client):
    """Platform-only providers can't be imported through the wizard."""
    mock_mc = MagicMock()
    mock_mc.get_provider_info.return_value = {"platform": True}

    mock_llm_cls = MagicMock()
    mock_llm_cls.get_model_config.return_value = mock_mc

    with patch("src.llms.llm.LLM", mock_llm_cls):
        resp = await client.get("/api/v1/providers/platform-proxy/visible-models")

    assert resp.status_code == 400
