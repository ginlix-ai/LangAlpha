"""
Tests for resolve_llm_config() and related model resolution in chat/llm_config.py.

Covers:
- Model priority: per-request > user preference > system default
- PTC vs flash mode model field selection
- User preference application (summarization, fetch, fallback overrides)
- BYOK client resolution path
- OAuth client resolution path
- Reasoning effort priority: per-request > user pref > None
- fast_mode / service_tier resolution
- Custom model with BYOK disabled falls back to system default
"""

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import SandboxConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SecurityConfig,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


HANDLER = "src.server.handlers.chat.llm_config"


def _make_config(**llm_overrides) -> AgentConfig:
    """Create a minimal AgentConfig for testing model resolution."""
    llm_defaults = {"name": "system-default-model", "flash": "system-flash-model"}
    llm_defaults.update(llm_overrides)
    return AgentConfig(
        llm=LLMConfig(**llm_defaults),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )


def _mock_model_config(system_models=None):
    """Create a mock ModelConfig that knows about system models."""
    if system_models is None:
        system_models = {"system-default-model", "system-flash-model", "gpt-4o"}
    mc = MagicMock()
    mc.get_model_config.side_effect = lambda name: {"provider": "openai"} if name in system_models else None
    mc.get_provider_info.return_value = {}
    mc.get_parent_provider.return_value = "openai"
    return mc


@pytest.fixture
def base_config():
    return _make_config()


# ---------------------------------------------------------------------------
# Model priority: per-request > user preference > system default
# ---------------------------------------------------------------------------


class TestModelPriority:
    @pytest.mark.asyncio
    async def test_system_default_used(self, base_config):
        """No per-request or preference → system default."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        assert config.llm.name == "system-default-model"

    @pytest.mark.asyncio
    async def test_user_preference_overrides_default(self, base_config):
        """User preferred_model overrides system default."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_model": "gpt-4o"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        assert config.llm.name == "gpt-4o"

    @pytest.mark.asyncio
    async def test_per_request_overrides_preference(self, base_config):
        """Per-request llm_model overrides both preference and default."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_model": "gpt-4o"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", "gpt-4o", False
            )
        assert config.llm.name == "gpt-4o"

    @pytest.mark.asyncio
    async def test_does_not_mutate_base_config(self, base_config):
        """resolve_llm_config should deep-copy, not mutate the base config."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        original_name = base_config.llm.name
        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_model": "gpt-4o"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        # Returned config changed, but base_config unchanged
        assert config.llm.name == "gpt-4o"
        assert base_config.llm.name == original_name


# ---------------------------------------------------------------------------
# PTC vs Flash mode
# ---------------------------------------------------------------------------


class TestModeModelField:
    @pytest.mark.asyncio
    async def test_flash_mode_uses_flash_field(self, base_config):
        """Flash mode reads/writes the 'flash' field, not 'name'."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, False, mode="flash"
            )
        # Should use flash field default
        assert config.llm.flash == "system-flash-model"

    @pytest.mark.asyncio
    async def test_flash_mode_per_request_override(self, base_config):
        """Per-request model in flash mode sets the flash field."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", "gpt-4o", False, mode="flash"
            )
        assert config.llm.flash == "gpt-4o"

    @pytest.mark.asyncio
    async def test_flash_mode_user_preference(self, base_config):
        """Flash mode uses preferred_flash_model preference key."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_flash_model": "gpt-4o"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, False, mode="flash"
            )
        assert config.llm.flash == "gpt-4o"


# ---------------------------------------------------------------------------
# User preference overrides for other model fields
# ---------------------------------------------------------------------------


class TestOtherModelPreferences:
    @pytest.mark.asyncio
    async def test_summarization_model_preference(self, base_config):
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"summarization_model": "gpt-4o-mini"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        assert config.llm.summarization == "gpt-4o-mini"

    @pytest.mark.asyncio
    async def test_fetch_model_preference(self, base_config):
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"fetch_model": "gpt-4o-mini"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        assert config.llm.fetch == "gpt-4o-mini"

    @pytest.mark.asyncio
    async def test_fallback_models_preference(self, base_config):
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"fallback_models": ["model-a", "model-b"]},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        assert config.llm.fallback == ["model-a", "model-b"]


# ---------------------------------------------------------------------------
# Reasoning effort priority
# ---------------------------------------------------------------------------


class TestReasoningEffort:
    @pytest.mark.asyncio
    async def test_per_request_reasoning(self, base_config):
        """Per-request reasoning_effort takes precedence."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        mock_llm = MagicMock()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"reasoning_effort": "low"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, False, reasoning_effort="high"
            )
        # Should use per-request "high", not pref "low"
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args
        assert call_kwargs.kwargs.get("reasoning_effort") == "high" or call_kwargs[1].get("reasoning_effort") == "high"

    @pytest.mark.asyncio
    async def test_user_pref_reasoning(self, base_config):
        """User pref reasoning_effort used when no per-request value."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        mock_llm = MagicMock()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"reasoning_effort": "low"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, False, reasoning_effort=None
            )
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args
        assert call_kwargs.kwargs.get("reasoning_effort") == "low" or call_kwargs[1].get("reasoning_effort") == "low"


# ---------------------------------------------------------------------------
# BYOK path
# ---------------------------------------------------------------------------


class TestBYOKResolution:
    @pytest.mark.asyncio
    async def test_byok_client_injected(self, base_config):
        """When BYOK is active, a fresh LLM client should be created and injected."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        mock_byok_llm = MagicMock(name="byok-llm-client")
        with (
            patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
                return_value=mock_byok_llm,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, True  # is_byok=True
            )
        assert config.llm_client is mock_byok_llm

    @pytest.mark.asyncio
    async def test_byok_not_active_no_client(self, base_config):
        """When is_byok=False, BYOK resolution is skipped."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
            ) as mock_resolve,
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, False
            )
        mock_resolve.assert_not_awaited()


# ---------------------------------------------------------------------------
# OAuth path
# ---------------------------------------------------------------------------


class TestOAuthResolution:
    @pytest.mark.asyncio
    async def test_oauth_takes_precedence_over_byok(self, base_config):
        """OAuth client is tried first; if found, BYOK is skipped."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        mock_oauth_llm = MagicMock(name="oauth-llm-client")
        with (
            patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
            patch(
                f"{HANDLER}.resolve_oauth_llm_client",
                new_callable=AsyncMock,
                return_value=mock_oauth_llm,
            ),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
            ) as mock_byok,
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, True  # is_byok=True
            )
        assert config.llm_client is mock_oauth_llm
        mock_byok.assert_not_awaited()


# ---------------------------------------------------------------------------
# Custom model + BYOK disabled → fallback
# ---------------------------------------------------------------------------


class TestCustomModelFallback:
    @pytest.mark.asyncio
    async def test_custom_model_without_byok_reverts_to_default(self, base_config):
        """Custom model selected but BYOK disabled → fall back to system default."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        # Model not in system models → treated as custom
        mock_mc = _mock_model_config(system_models={"system-default-model", "system-flash-model"})
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_model": "my-custom-model"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value={"name": "my-custom-model", "model_id": "gpt-4o", "provider": "openai"},
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(
                base_config, "user-1", None, False  # is_byok=False
            )
        # Should revert to system default
        assert config.llm.name == "system-default-model"


# ---------------------------------------------------------------------------
# fast_mode / service_tier
# ---------------------------------------------------------------------------


class TestFastMode:
    @pytest.mark.asyncio
    async def test_fast_mode_per_request(self, base_config):
        """Per-request fast_mode should be passed to OAuth resolver as service_tier."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
            patch(
                f"{HANDLER}.resolve_oauth_llm_client",
                new_callable=AsyncMock,
                return_value=None,
            ) as mock_oauth,
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            await resolve_llm_config(
                base_config, "user-1", None, False, fast_mode=True
            )
        # OAuth resolver should be called with service_tier="priority"
        call_kwargs = mock_oauth.call_args
        assert call_kwargs.kwargs.get("service_tier") == "priority"

    @pytest.mark.asyncio
    async def test_fast_mode_from_preference(self, base_config):
        """User pref fast_mode used when no per-request value."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"fast_mode": True},
            ),
            patch(
                f"{HANDLER}.resolve_oauth_llm_client",
                new_callable=AsyncMock,
                return_value=None,
            ) as mock_oauth,
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            await resolve_llm_config(
                base_config, "user-1", None, False
            )
        call_kwargs = mock_oauth.call_args
        assert call_kwargs.kwargs.get("service_tier") == "priority"


# ---------------------------------------------------------------------------
# _resolve_one error handling (fallback model resolution)
# ---------------------------------------------------------------------------


class TestResolveOneFallbackGuard:
    @pytest.mark.asyncio
    async def test_resolve_one_catches_exception_from_oauth(self, base_config):
        """If resolve_oauth_llm_client raises for a fallback model, it should be skipped (not crash)."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(system_models={"system-default-model", "system-flash-model", "oauth-model"})

        async def _oauth_side_effect(user_id, model_name, *args, **kwargs):
            if model_name == "oauth-model":
                raise Exception("OAuth token expired")
            return None

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"fallback_models": ["oauth-model"]},
            ),
            patch(
                f"{HANDLER}.resolve_oauth_llm_client",
                new_callable=AsyncMock,
                side_effect=_oauth_side_effect,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)

        # The fallback should be skipped, not crash the whole request
        assert config.llm.name == "system-default-model"
        # No fallback clients resolved (the one model failed)
        assert not getattr(config, "fallback_llm_clients", None)

    @pytest.mark.asyncio
    async def test_mixed_fallback_valid_and_invalid(self, base_config):
        """Mixed fallback list: valid API-key model resolves, invalid OAuth model is skipped."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model", "valid-model", "oauth-model"}
        )
        mock_valid_client = MagicMock(name="valid-fallback-client")

        async def _oauth_side_effect(user_id, model_name, *args, **kwargs):
            if model_name == "oauth-model":
                raise Exception("OAuth provider not configured")
            return None

        async def _byok_side_effect(user_id, model_name, is_byok, *args, **kwargs):
            if model_name == "valid-model":
                return mock_valid_client
            return None

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"fallback_models": ["valid-model", "oauth-model"]},
            ),
            patch(
                f"{HANDLER}.resolve_oauth_llm_client",
                new_callable=AsyncMock,
                side_effect=_oauth_side_effect,
            ),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
                side_effect=_byok_side_effect,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, True)

        # Valid model resolved, invalid skipped
        assert len(config.fallback_llm_clients) == 1
        assert config.fallback_llm_clients[0] is mock_valid_client
