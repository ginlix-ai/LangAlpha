"""
Tests for GET /api/v1/models — public model listing endpoint.

Covers:
- Response shape: models, model_metadata, system_defaults
- System defaults populated from agent_config's LLMConfig
- Multiple providers in response
- No auth required
"""

from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig
from tests.conftest import create_test_app

DB = "src.server.app.api_keys"


def _mock_agent_config(**llm_overrides) -> AgentConfig:
    """Create a minimal AgentConfig with given LLM overrides."""
    llm_defaults = {"name": "test-model"}
    llm_defaults.update(llm_overrides)
    return AgentConfig(
        llm=LLMConfig(**llm_defaults),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )


@pytest_asyncio.fixture
async def client():
    from src.server.app.api_keys import router
    import src.server.app.api_keys as api_keys_mod

    api_keys_mod._BYOK_PROVIDERS_CACHE = None

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        with (
            patch(f"{DB}._get_supported_providers", return_value=["openai", "anthropic"]),
            patch(f"{DB}._get_provider_info_map", return_value={
                "openai": {"display_name": "OpenAI", "access_type": "api_key", "brand_key": "openai"},
                "anthropic": {"display_name": "Anthropic", "access_type": "api_key", "brand_key": "anthropic"},
            }),
            patch(f"{DB}._build_provider_catalog", return_value=[]),
        ):
            yield c

    api_keys_mod._BYOK_PROVIDERS_CACHE = None


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------


class TestListModelsResponse:
    @pytest.mark.asyncio
    async def test_response_has_required_keys(self, client):
        mock_models = {"openai": [{"name": "gpt-4o", "model_id": "gpt-4o"}]}
        mock_mc = MagicMock()
        mock_mc.get_display_name.side_effect = lambda p: p.title()
        mock_mc.get_model_metadata.return_value = {}

        mock_llm_cls = MagicMock()
        mock_llm_cls.get_model_config.return_value = mock_mc

        agent_cfg = _mock_agent_config(name="gpt-4o", flash="gpt-4o-mini")
        with (
            patch("src.llms.llm.get_configured_llm_models", return_value=mock_models),
            patch("src.llms.llm.LLM", mock_llm_cls),
            patch("src.server.app.setup.agent_config", agent_cfg),
        ):
            resp = await client.get("/api/v1/models")

        assert resp.status_code == 200
        body = resp.json()
        assert "models" in body
        assert "model_metadata" in body
        assert "system_defaults" in body
        assert "provider_catalog" in body

    @pytest.mark.asyncio
    async def test_system_defaults_populated(self, client):
        """System defaults should come from AgentConfig LLMConfig."""
        mock_models = {}
        mock_mc = MagicMock()
        mock_mc.get_display_name.side_effect = lambda p: p
        mock_mc.get_model_metadata.return_value = {}

        mock_llm_cls = MagicMock()
        mock_llm_cls.get_model_config.return_value = mock_mc

        agent_cfg = _mock_agent_config(
            name="claude-sonnet-4-5",
            flash="claude-haiku-4-5",
            summarization="claude-haiku-4-5",
            fetch="claude-haiku-4-5",
            fallback=["gpt-4o"],
        )
        with (
            patch("src.llms.llm.get_configured_llm_models", return_value=mock_models),
            patch("src.llms.llm.LLM", mock_llm_cls),
            patch("src.server.app.setup.agent_config", agent_cfg),
        ):
            resp = await client.get("/api/v1/models")

        body = resp.json()
        defaults = body["system_defaults"]
        assert defaults["default_model"] == "claude-sonnet-4-5"
        assert defaults["flash_model"] == "claude-haiku-4-5"
        assert defaults["summarization_model"] == "claude-haiku-4-5"
        assert defaults["fetch_model"] == "claude-haiku-4-5"
        assert defaults["fallback_models"] == ["gpt-4o"]

    @pytest.mark.asyncio
    async def test_multiple_providers(self, client):
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
        mock_mc.get_display_name.side_effect = lambda p: p.title()
        mock_mc.get_model_metadata.return_value = {"sdk": "openai"}

        mock_llm_cls = MagicMock()
        mock_llm_cls.get_model_config.return_value = mock_mc

        agent_cfg = _mock_agent_config(name="gpt-4o")
        with (
            patch("src.llms.llm.get_configured_llm_models", return_value=mock_models),
            patch("src.llms.llm.LLM", mock_llm_cls),
            patch("src.server.app.setup.agent_config", agent_cfg),
        ):
            resp = await client.get("/api/v1/models")

        body = resp.json()
        assert "openai" in body["models"]
        assert "anthropic" in body["models"]
        assert len(body["models"]["openai"]["models"]) == 2
        assert len(body["models"]["anthropic"]["models"]) == 1

    @pytest.mark.asyncio
    async def test_no_auth_required(self, client):
        """The models endpoint should work without authentication."""
        mock_models = {}
        mock_mc = MagicMock()
        mock_mc.get_display_name.side_effect = lambda p: p
        mock_mc.get_model_metadata.return_value = {}

        mock_llm_cls = MagicMock()
        mock_llm_cls.get_model_config.return_value = mock_mc

        agent_cfg = _mock_agent_config(name="test")
        with (
            patch("src.llms.llm.get_configured_llm_models", return_value=mock_models),
            patch("src.llms.llm.LLM", mock_llm_cls),
            patch("src.server.app.setup.agent_config", agent_cfg),
        ):
            resp = await client.get("/api/v1/models")

        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_model_metadata_includes_access_type(self, client):
        """model_metadata values should include access_type alongside sdk and provider."""
        mock_models = {
            "openai": [{"name": "gpt-4o", "model_id": "gpt-4o"}],
        }
        mock_mc = MagicMock()
        mock_mc.get_display_name.side_effect = lambda p: p.title()
        mock_mc.get_model_metadata.return_value = {
            "gpt-4o": {"sdk": "openai", "provider": "openai", "access_type": "api_key"},
        }

        mock_llm_cls = MagicMock()
        mock_llm_cls.get_model_config.return_value = mock_mc

        agent_cfg = _mock_agent_config(name="gpt-4o")
        with (
            patch("src.llms.llm.get_configured_llm_models", return_value=mock_models),
            patch("src.llms.llm.LLM", mock_llm_cls),
            patch("src.server.app.setup.agent_config", agent_cfg),
        ):
            resp = await client.get("/api/v1/models")

        body = resp.json()
        metadata = body["model_metadata"]
        assert "gpt-4o" in metadata
        assert "access_type" in metadata["gpt-4o"]
        assert metadata["gpt-4o"]["access_type"] == "api_key"
