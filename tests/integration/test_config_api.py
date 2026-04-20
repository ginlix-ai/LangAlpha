"""Integration tests for config-related API endpoints against real PostgreSQL.

Hits actual FastAPI routes with real DB — the first HTTP-level integration tests
in this project. External services (LLM providers, Daytona) are mocked.

Covers:
- GET /api/v1/models — model listing with real settings
- GET /api/v1/users/me/api-keys — BYOK key listing with real encrypted storage
- PUT /api/v1/users/me/api-keys — BYOK key CRUD (set, update, delete, toggle)
- DELETE /api/v1/users/me/api-keys/{provider}
- PUT /api/v1/users/me/preferences — model preference persistence (preferred_model,
  reasoning_effort, custom_models, custom_providers)
- BYOK + preferences round-trip: set BYOK keys, configure custom model, verify both
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# Deterministic encryption key for pgcrypto
_TEST_ENCRYPTION_KEY = "test-byok-encryption-key-32char!"

_TEST_USER_ID = "test-user-integration-001"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _mock_encryption_key():
    """Provide a test encryption key for pgcrypto operations."""
    with patch.dict(os.environ, {"BYOK_ENCRYPTION_KEY": _TEST_ENCRYPTION_KEY}):
        yield


@pytest_asyncio.fixture
async def api_keys_client(seed_user, patched_get_db_connection):
    """httpx client hitting the real api_keys router with real test DB."""
    import src.server.app.api_keys as api_keys_mod
    from src.server.app.api_keys import router
    from src.server.utils.api import get_current_user_id

    api_keys_mod._BYOK_PROVIDERS_CACHE = None

    app = create_test_app(router)
    # Override auth to return our integration test user
    app.dependency_overrides[get_current_user_id] = lambda: _TEST_USER_ID

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client

    api_keys_mod._BYOK_PROVIDERS_CACHE = None


@pytest_asyncio.fixture
async def users_client(seed_user, patched_get_db_connection):
    """httpx client hitting the real users router with real test DB."""
    from src.server.app.users import router
    from src.server.utils.api import get_current_user_id

    app = create_test_app(router)
    app.dependency_overrides[get_current_user_id] = lambda: _TEST_USER_ID

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# GET /api/v1/models — real config, mocked LLM manifest
# ---------------------------------------------------------------------------


class TestModelsEndpointIntegration:
    async def test_models_response_shape(self, api_keys_client):
        """GET /api/v1/models returns well-formed response with real settings loader."""
        mock_models = {
            "openai": [
                {"name": "gpt-4o", "model_id": "gpt-4o"},
                {"name": "gpt-4o-mini", "model_id": "gpt-4o-mini"},
            ],
            "anthropic": [
                {"name": "claude-sonnet-4-5", "model_id": "claude-sonnet-4-20250514"},
            ],
        }
        mock_mc = MagicMock()
        mock_mc.get_display_name.side_effect = lambda p: p.replace("_", " ").title()
        mock_mc.get_model_metadata.return_value = {"sdk": "openai"}

        mock_llm_cls = MagicMock()
        mock_llm_cls.get_model_config.return_value = mock_mc

        from ptc_agent.config.agent import AgentConfig, LLMConfig
        from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

        agent_cfg = AgentConfig(
            llm=LLMConfig(
                name="claude-sonnet-4-5",
                flash="claude-haiku-4-5",
                compaction="claude-haiku-4-5",
                fallback=["gpt-4o"],
            ),
            security=SecurityConfig(),
            logging=LoggingConfig(),
            sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test")),
            mcp=MCPConfig(),
            filesystem=FilesystemConfig(),
        )
        with (
            patch("src.llms.llm.get_configured_llm_models", return_value=mock_models),
            patch("src.llms.llm.LLM", mock_llm_cls),
            patch("src.server.app.setup.agent_config", agent_cfg),
        ):
            resp = await api_keys_client.get("/api/v1/models")

        assert resp.status_code == 200
        body = resp.json()

        # Structure checks
        assert "models" in body
        assert "system_defaults" in body
        assert "model_metadata" in body

        # System defaults populated from config
        defaults = body["system_defaults"]
        assert defaults["default_model"] == "claude-sonnet-4-5"
        assert defaults["flash_model"] == "claude-haiku-4-5"
        assert defaults["fallback_models"] == ["gpt-4o"]

        # Providers present
        assert "openai" in body["models"]
        assert "anthropic" in body["models"]
        assert len(body["models"]["openai"]["models"]) == 2


# ---------------------------------------------------------------------------
# BYOK CRUD — real DB with encrypted storage
# ---------------------------------------------------------------------------


class TestBYOKCRUDIntegration:
    async def test_initial_state_no_keys(self, api_keys_client):
        """New user should have no BYOK keys and byok_enabled=False."""
        resp = await api_keys_client.get("/api/v1/users/me/api-keys")
        assert resp.status_code == 200
        body = resp.json()
        assert body["byok_enabled"] is False
        for prov in body["providers"]:
            assert prov["has_key"] is False

    async def test_set_and_get_api_key(self, api_keys_client):
        """Set an API key, then verify it's stored and masked correctly."""
        resp = await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={
                "byok_enabled": True,
                "api_keys": {"openai": "sk-test-openai-key-1234567890"},
            },
        )
        assert resp.status_code == 200

        # Read back
        resp = await api_keys_client.get("/api/v1/users/me/api-keys")
        body = resp.json()
        assert body["byok_enabled"] is True

        openai = next((p for p in body["providers"] if p["provider"] == "openai"), None)
        assert openai is not None
        assert openai["has_key"] is True
        assert "..." in openai["masked_key"]
        # Key should be masked, not exposed
        assert "1234567890" not in openai["masked_key"] or len(openai["masked_key"]) < 20

    async def test_update_existing_key(self, api_keys_client):
        """Updating an existing key should overwrite it."""
        # Set initial key
        await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"openai": "sk-old-key-xxxxxxxxxx"}},
        )

        # Update with new key
        resp = await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"openai": "sk-new-key-yyyyyyyyyy"}},
        )
        assert resp.status_code == 200

        # Verify the key was actually changed (check DB directly)
        from src.server.database.api_keys import get_key_for_provider
        key = await get_key_for_provider(_TEST_USER_ID, "openai")
        assert key == "sk-new-key-yyyyyyyyyy"

    async def test_delete_api_key(self, api_keys_client):
        """Setting key to null should delete it."""
        # First set a key
        await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"openai": "sk-to-delete-xxxxx"}},
        )

        # Delete by setting to null
        resp = await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"openai": None}},
        )
        assert resp.status_code == 200

        # Verify deleted
        resp = await api_keys_client.get("/api/v1/users/me/api-keys")
        openai = next((p for p in resp.json()["providers"] if p["provider"] == "openai"), None)
        assert openai is not None
        assert openai["has_key"] is False

    async def test_delete_via_endpoint(self, api_keys_client):
        """DELETE /api/v1/users/me/api-keys/{provider} removes key."""
        # Set key first
        await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"openai": "sk-delete-endpoint-x"}},
        )

        resp = await api_keys_client.delete("/api/v1/users/me/api-keys/openai")
        assert resp.status_code == 200

        # Verify
        from src.server.database.api_keys import get_key_for_provider
        key = await get_key_for_provider(_TEST_USER_ID, "openai")
        assert key is None

    async def test_set_key_with_custom_base_url(self, api_keys_client):
        """API key with custom base_url should persist both values."""
        resp = await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={
                "api_keys": {"openai": "sk-custom-base-url-key"},
                "base_urls": {"openai": "https://my-proxy.example.com/v1"},
            },
        )
        assert resp.status_code == 200

        # Verify base_url persisted
        from src.server.database.api_keys import get_byok_config_for_provider, set_byok_enabled
        await set_byok_enabled(_TEST_USER_ID, True)
        config = await get_byok_config_for_provider(_TEST_USER_ID, "openai")
        assert config is not None
        assert config["base_url"] == "https://my-proxy.example.com/v1"
        assert config["api_key"] == "sk-custom-base-url-key"

    async def test_byok_toggle_persistence(self, api_keys_client):
        """byok_enabled toggle should persist across requests."""
        # Enable
        await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={"byok_enabled": True},
        )

        resp = await api_keys_client.get("/api/v1/users/me/api-keys")
        assert resp.json()["byok_enabled"] is True

        # Disable
        await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={"byok_enabled": False},
        )

        resp = await api_keys_client.get("/api/v1/users/me/api-keys")
        assert resp.json()["byok_enabled"] is False

    async def test_multiple_provider_keys(self, api_keys_client):
        """Setting keys for multiple providers should all persist."""
        await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={
                "byok_enabled": True,
                "api_keys": {
                    "openai": "sk-openai-key-xxxx",
                    "anthropic": "sk-ant-key-yyyy",
                },
            },
        )

        resp = await api_keys_client.get("/api/v1/users/me/api-keys")
        body = resp.json()
        providers_with_keys = [p for p in body["providers"] if p["has_key"]]
        assert len(providers_with_keys) >= 2


# ---------------------------------------------------------------------------
# User preferences — model selection persistence
# ---------------------------------------------------------------------------


class TestPreferencesIntegration:
    async def test_set_preferred_model(self, users_client):
        """preferred_model in other_preference should persist."""
        resp = await users_client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"preferred_model": "gpt-4o"}},
        )
        assert resp.status_code == 200

        # Read back
        resp = await users_client.get("/api/v1/users/me/preferences")
        assert resp.status_code == 200
        other = resp.json().get("other_preference") or {}
        assert other.get("preferred_model") == "gpt-4o"

    async def test_set_reasoning_effort(self, users_client):
        """reasoning_effort should persist in other_preference."""
        resp = await users_client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"reasoning_effort": "high"}},
        )
        assert resp.status_code == 200

        resp = await users_client.get("/api/v1/users/me/preferences")
        other = resp.json().get("other_preference") or {}
        assert other.get("reasoning_effort") == "high"

    async def test_set_flash_model(self, users_client):
        """preferred_flash_model should persist."""
        resp = await users_client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"preferred_flash_model": "gpt-4o-mini"}},
        )
        assert resp.status_code == 200

        resp = await users_client.get("/api/v1/users/me/preferences")
        other = resp.json().get("other_preference") or {}
        assert other.get("preferred_flash_model") == "gpt-4o-mini"

    async def test_set_fallback_models(self, users_client):
        """fallback_models list should persist."""
        resp = await users_client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"fallback_models": ["gpt-4o", "claude-haiku-4-5"]}},
        )
        assert resp.status_code == 200

        resp = await users_client.get("/api/v1/users/me/preferences")
        other = resp.json().get("other_preference") or {}
        assert other.get("fallback_models") == ["gpt-4o", "claude-haiku-4-5"]

    async def test_set_custom_providers_and_models(self, users_client):
        """Custom providers and models should persist in other_preference."""
        mc = MagicMock()
        mc.get_model_config.return_value = None  # Not a system model
        mc.get_byok_eligible_providers.return_value = ["openai", "anthropic"]

        with patch("src.llms.llm.ModelConfig", return_value=mc):
            resp = await users_client.put(
                "/api/v1/users/me/preferences",
                json={
                    "other_preference": {
                        "custom_providers": [
                            {"name": "my-openai", "parent_provider": "openai"},
                        ],
                        "custom_models": [
                            {
                                "name": "my-gpt4",
                                "model_id": "gpt-4o",
                                "provider": "my-openai",
                            },
                        ],
                    }
                },
            )
        assert resp.status_code == 200

        resp = await users_client.get("/api/v1/users/me/preferences")
        other = resp.json().get("other_preference") or {}
        assert len(other.get("custom_providers", [])) == 1
        assert other["custom_providers"][0]["name"] == "my-openai"
        assert len(other.get("custom_models", [])) == 1
        assert other["custom_models"][0]["name"] == "my-gpt4"

    async def test_model_preference_overwrite(self, users_client):
        """Changing preferred_model should overwrite the previous value."""
        await users_client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"preferred_model": "gpt-4o"}},
        )

        await users_client.put(
            "/api/v1/users/me/preferences",
            json={"other_preference": {"preferred_model": "claude-sonnet-4-5"}},
        )

        resp = await users_client.get("/api/v1/users/me/preferences")
        other = resp.json().get("other_preference") or {}
        assert other.get("preferred_model") == "claude-sonnet-4-5"


# ---------------------------------------------------------------------------
# BYOK + Preferences round-trip
# ---------------------------------------------------------------------------


class TestBYOKPreferencesRoundTrip:
    async def test_full_byok_custom_model_flow(
        self, api_keys_client, users_client
    ):
        """End-to-end: enable BYOK, add key, configure custom model, verify all persisted."""
        # 1. Enable BYOK and set OpenAI key
        resp = await api_keys_client.put(
            "/api/v1/users/me/api-keys",
            json={
                "byok_enabled": True,
                "api_keys": {"openai": "sk-user-openai-key-xxxxx"},
                "base_urls": {"openai": "https://my-openai-proxy.com/v1"},
            },
        )
        assert resp.status_code == 200

        # 2. Configure custom provider + model in preferences
        mc = MagicMock()
        mc.get_model_config.return_value = None
        mc.get_byok_eligible_providers.return_value = ["openai", "anthropic"]

        with patch("src.llms.llm.ModelConfig", return_value=mc):
            resp = await users_client.put(
                "/api/v1/users/me/preferences",
                json={
                    "other_preference": {
                        "preferred_model": "my-gpt4",
                        "reasoning_effort": "high",
                        "custom_providers": [
                            {"name": "my-openai", "parent_provider": "openai"},
                        ],
                        "custom_models": [
                            {
                                "name": "my-gpt4",
                                "model_id": "gpt-4o",
                                "provider": "my-openai",
                                "parameters": {"temperature": 0.7},
                            },
                        ],
                    }
                },
            )
        assert resp.status_code == 200

        # 3. Verify BYOK state
        resp = await api_keys_client.get("/api/v1/users/me/api-keys")
        byok_body = resp.json()
        assert byok_body["byok_enabled"] is True
        openai_provider = next(
            (p for p in byok_body["providers"] if p["provider"] == "openai"), None
        )
        assert openai_provider is not None
        assert openai_provider["has_key"] is True

        # 4. Verify preferences state
        resp = await users_client.get("/api/v1/users/me/preferences")
        prefs = resp.json()
        other = prefs.get("other_preference") or {}
        assert other.get("preferred_model") == "my-gpt4"
        assert other.get("reasoning_effort") == "high"
        assert len(other.get("custom_models", [])) == 1
        assert other["custom_models"][0]["parameters"]["temperature"] == 0.7

        # 5. Verify encrypted key is retrievable from DB
        from src.server.database.api_keys import get_byok_config_for_provider
        config = await get_byok_config_for_provider(_TEST_USER_ID, "openai")
        assert config is not None
        assert config["api_key"] == "sk-user-openai-key-xxxxx"
        assert config["base_url"] == "https://my-openai-proxy.com/v1"
