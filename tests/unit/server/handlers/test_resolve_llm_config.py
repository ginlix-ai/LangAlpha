"""
Tests for resolve_llm_config() and related model resolution in chat/llm_config.py.

Covers:
- Model priority: per-request > user preference > system default
- PTC vs flash mode model field selection
- User preference application (compaction, fetch, fallback overrides)
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
    async def test_compaction_model_preference(self, base_config):
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"compaction_model": "gpt-4o-mini"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        assert config.llm.compaction == "gpt-4o-mini"

    @pytest.mark.asyncio
    async def test_legacy_summarization_model_preference(self, base_config):
        """Platform DB may still carry the legacy ``summarization_model`` key."""
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
        assert config.llm.compaction == "gpt-4o-mini"

    @pytest.mark.asyncio
    async def test_compaction_model_wins_when_both_keys_present(self, base_config):
        """New ``compaction_model`` must override legacy ``summarization_model``."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={
                    "compaction_model": "gpt-4o-mini",
                    "summarization_model": "stale-legacy-model",
                },
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        assert config.llm.compaction == "gpt-4o-mini"

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

    @pytest.mark.asyncio
    async def test_compaction_profile_aggressive_applies_preset(self, base_config):
        from src.server.handlers.chat.llm_config import resolve_llm_config
        from ptc_agent.config.agent import COMPACTION_PROFILES

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"compaction_profile": "aggressive"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        preset = COMPACTION_PROFILES["aggressive"]
        assert config.compaction.token_threshold == preset["token_threshold"]
        assert config.compaction.truncate_args_trigger_messages == preset["truncate_args_trigger_messages"]
        assert config.compaction.keep_messages == preset["keep_messages"]

    @pytest.mark.asyncio
    async def test_compaction_profile_extended_applies_preset(self, base_config):
        from src.server.handlers.chat.llm_config import resolve_llm_config
        from ptc_agent.config.agent import COMPACTION_PROFILES

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"compaction_profile": "extended"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        preset = COMPACTION_PROFILES["extended"]
        assert config.compaction.token_threshold == preset["token_threshold"]
        assert config.compaction.truncate_args_trigger_messages == preset["truncate_args_trigger_messages"]
        assert config.compaction.keep_messages == preset["keep_messages"]

    @pytest.mark.asyncio
    async def test_compaction_profile_relaxed_applies_preset(self, base_config):
        """Relaxed profile targets 1M-context models with a 300k threshold."""
        from src.server.handlers.chat.llm_config import resolve_llm_config
        from ptc_agent.config.agent import COMPACTION_PROFILES

        mock_mc = _mock_model_config()
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"compaction_profile": "relaxed"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, False)
        preset = COMPACTION_PROFILES["relaxed"]
        assert config.compaction.token_threshold == preset["token_threshold"]
        assert config.compaction.truncate_args_trigger_messages == preset["truncate_args_trigger_messages"]
        assert config.compaction.keep_messages == preset["keep_messages"]

    @pytest.mark.asyncio
    async def test_compaction_profile_invalid_falls_through_to_yaml(self, base_config):
        """Unknown strings and non-string values leave compaction config unchanged."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        baseline = base_config.compaction.model_copy(deep=True)
        mock_mc = _mock_model_config()
        for bad in ("nonsense", "", 123, None, {"aggressive": True}):
            with (
                patch(
                    f"{HANDLER}.get_model_preference",
                    new_callable=AsyncMock,
                    return_value={"compaction_profile": bad},
                ),
                patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
                patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            ):
                config = await resolve_llm_config(base_config, "user-1", None, False)
            assert config.compaction.token_threshold == baseline.token_threshold
            assert config.compaction.truncate_args_trigger_messages == baseline.truncate_args_trigger_messages
            assert config.compaction.keep_messages == baseline.keep_messages


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
    async def test_custom_model_without_byok_raises_byok_required(self, base_config):
        """Custom model selected but BYOK disabled → raise byok_key_required with a
        CTA link, so the user sees a clear 'enable BYOK and add a key' banner
        instead of a silent downgrade to the platform default."""
        from fastapi import HTTPException

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
            with pytest.raises(HTTPException) as excinfo:
                await resolve_llm_config(
                    base_config, "user-1", None, False  # is_byok=False
                )
        detail = excinfo.value.detail
        assert isinstance(detail, dict)
        assert detail.get("type") == "byok_key_required"
        assert "my-custom-model" in detail.get("message", "")
        assert detail.get("link", {}).get("url")

    @pytest.mark.asyncio
    async def test_custom_model_byok_on_no_key_raises_byok_required(self, base_config):
        """Custom model selected, BYOK on, but no key stored → raise byok_key_required.
        This is the K2.6 regression: previously crashed downstream with
        'Model K2.6 not found in models.json'."""
        from fastapi import HTTPException

        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(system_models={"system-default-model", "system-flash-model"})
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_model": "K2.6"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value={"name": "K2.6", "model_id": "moonshot-v1-8k", "provider": "moonshot"},
            ),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
                return_value=None,  # no key stored
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            with pytest.raises(HTTPException) as excinfo:
                await resolve_llm_config(
                    base_config, "user-1", None, True  # is_byok=True
                )
        assert excinfo.value.detail.get("type") == "byok_key_required"

    @pytest.mark.asyncio
    async def test_custom_model_byok_on_with_key_returns_custom_client(self, base_config):
        """Custom model + BYOK on + key present → BYOK client is injected (no error)."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(system_models={"system-default-model", "system-flash-model"})
        mock_client = MagicMock(name="custom-byok-client")
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_model": "my-custom"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value={"name": "my-custom", "model_id": "gpt-4o", "provider": "openai"},
            ),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
                return_value=mock_client,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, True)
        assert config.llm_client is mock_client
        assert config.llm.name == "my-custom"

    @pytest.mark.asyncio
    async def test_collision_classified_as_custom_shadows_system(self, base_config):
        """Shadow semantics: when a user's ``custom_models`` entry and a
        built-in share the same ``name``, the custom entry wins. The custom
        entry's ``input_modalities`` (and routing) must take precedence over
        the built-in's metadata."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        # gpt-4o exists as a system model AND the user has a custom entry
        # with the same name — the variant-routing use case.
        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model", "gpt-4o"}
        )
        mock_client = MagicMock(name="custom-byok-client")
        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"preferred_model": "gpt-4o"},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value={
                    "name": "gpt-4o",
                    "model_id": "gpt-4o",
                    "provider": "my-openai",
                    "input_modalities": ["text", "image"],
                },
            ),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
                return_value=mock_client,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, True)
        # Custom path wins; BYOK client built through the custom provider.
        assert config.llm_client is mock_client
        # Custom modalities ARE threaded onto the resolved config.
        assert getattr(config, "input_modalities", None) == ["text", "image"]

    @pytest.mark.asyncio
    async def test_custom_model_on_builtin_variant_prefers_variant_key(self):
        """Custom model routed through a built-in variant (e.g. ``moonshot-coding``)
        should resolve the BYOK key against the variant's own slug before walking
        up to the parent. The wizard stores the key under ``moonshot-coding`` —
        looking it up under ``moonshot`` would falsely raise ``byok_key_required``.
        """
        from src.server.handlers.chat.llm_config import _resolve_custom_model_byok

        mc = MagicMock()
        mc.get_parent_provider.return_value = "moonshot"
        mc.get_child_variants.return_value = ["moonshot-coding"]
        mc.get_provider_info.return_value = {"base_url": "https://api.kimi.com/coding"}

        async def _batch_lookup(user_id, providers):
            # Key is stored under the variant, not the parent.
            return {
                p: {"api_key": "user-kimi-key", "base_url": None}
                for p in providers
                if p == "moonshot-coding"
            }

        with (
            patch(
                f"{HANDLER}.get_custom_provider_config",
                new_callable=AsyncMock,
                return_value=None,  # no custom provider, this is a built-in variant
            ),
            patch(
                "src.server.database.api_keys.get_byok_configs_for_providers",
                new_callable=AsyncMock,
                side_effect=_batch_lookup,
            ),
        ):
            byok_config, base_url, _ = await _resolve_custom_model_byok(
                "user-1",
                "kimi-k2.6",
                {"name": "kimi-k2.6", "model_id": "kimi-k2.6", "provider": "moonshot-coding"},
                mc,
            )

        assert byok_config == {"api_key": "user-kimi-key", "base_url": None}
        # Variant's declared base_url is honored when key row has none.
        assert base_url == "https://api.kimi.com/coding"

    @pytest.mark.asyncio
    async def test_custom_model_on_parent_descends_to_variant_key(self):
        """Mirror of the variant→parent walk: custom model tagged with the
        parent slug (``moonshot``) should fall through to any configured
        sibling variant (``moonshot-coding``) when the parent has no key.
        This covers the flow where the user added a custom model under the
        parent in Settings but their only key is on the coding-plan variant.
        """
        from src.server.handlers.chat.llm_config import _resolve_custom_model_byok

        mc = MagicMock()
        # Parent has no further parent — get_parent_provider returns self.
        mc.get_parent_provider.return_value = "moonshot"
        mc.get_child_variants.return_value = ["moonshot-coding"]

        def _provider_info(name):
            if name == "moonshot-coding":
                return {"base_url": "https://api.kimi.com/coding"}
            return {"base_url": "https://api.moonshot.cn/v1"}

        mc.get_provider_info.side_effect = _provider_info

        async def _batch_lookup(user_id, providers):
            return {
                p: {"api_key": "user-kimi-coding-key", "base_url": None}
                for p in providers
                if p == "moonshot-coding"
            }

        with (
            patch(
                f"{HANDLER}.get_custom_provider_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "src.server.database.api_keys.get_byok_configs_for_providers",
                new_callable=AsyncMock,
                side_effect=_batch_lookup,
            ),
        ):
            byok_config, base_url, _ = await _resolve_custom_model_byok(
                "user-1",
                "kimi-custom",
                {"name": "kimi-custom", "model_id": "kimi-custom", "provider": "moonshot"},
                mc,
            )

        assert byok_config == {"api_key": "user-kimi-coding-key", "base_url": None}
        # Must use the variant's base_url, not the parent's — the key only
        # works against the variant's endpoint.
        assert base_url == "https://api.kimi.com/coding"

    @pytest.mark.asyncio
    async def test_custom_fallback_without_key_skipped_without_crash(self, base_config):
        """A custom fallback model without a BYOK key is silently skipped with a
        warning instead of crashing the whole request via create_llm()."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        # Only the main model is in models.json; "my-fallback" is custom.
        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model"}
        )

        async def _byok_side_effect(user_id, model_name, is_byok, *args, **kwargs):
            # Main model has a key; fallback custom does not.
            if model_name == "system-default-model":
                return MagicMock(name="main-client")
            return None

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"fallback_models": ["my-fallback"]},
            ),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(
                f"{HANDLER}.resolve_byok_llm_client",
                new_callable=AsyncMock,
                side_effect=_byok_side_effect,
            ),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=None,  # main model is NOT custom
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            config = await resolve_llm_config(base_config, "user-1", None, True)

        # Custom fallback dropped silently; no crash from create_llm().
        assert not getattr(config, "fallback_llm_clients", None)


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


# ---------------------------------------------------------------------------
# Classification call-site accounting — guards against accidental extra
# classifications in resolve_llm_config / _resolve_one. Inner helpers
# (resolve_byok_llm_client) are mocked here, so this test only covers the
# outer orchestration layer; their own cheap self-classification is fine.
# ---------------------------------------------------------------------------


class TestClassifyModelDirect:
    """Direct unit tests for ``classify_model`` — the central entry point that
    answers 'is this name system, custom, or unknown?'. Shadow semantics: a
    user's custom entry wins when its name collides with a built-in, so a
    user can route ``gpt-5`` through their own endpoint."""

    @pytest.mark.asyncio
    async def test_returns_custom_when_user_has_entry(self):
        from src.server.handlers.chat.llm_config import classify_model, ModelSource

        custom_entry = {"name": "my-model", "model_id": "m1", "provider": "my-provider"}
        with patch(
            f"{HANDLER}.get_custom_model_config",
            new_callable=AsyncMock,
            return_value=custom_entry,
        ):
            source, cfg = await classify_model("user-1", "my-model")

        assert source == ModelSource.CUSTOM
        assert cfg is custom_entry

    @pytest.mark.asyncio
    async def test_returns_system_when_only_in_manifest(self):
        from src.server.handlers.chat.llm_config import classify_model, ModelSource

        system_info = {"provider": "openai", "model_id": "gpt-5.4"}
        mock_mc = MagicMock()
        mock_mc.get_model_config.return_value = system_info
        with (
            patch(f"{HANDLER}.get_custom_model_config", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            source, cfg = await classify_model("user-1", "gpt-5.4")

        assert source == ModelSource.SYSTEM
        assert cfg is system_info

    @pytest.mark.asyncio
    async def test_returns_unknown_when_neither(self):
        from src.server.handlers.chat.llm_config import classify_model, ModelSource

        mock_mc = MagicMock()
        mock_mc.get_model_config.return_value = None
        with (
            patch(f"{HANDLER}.get_custom_model_config", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            source, cfg = await classify_model("user-1", "no-such-model")

        assert source == ModelSource.UNKNOWN
        assert cfg == {}

    @pytest.mark.asyncio
    async def test_custom_shadows_system_on_name_collision(self):
        """When a name appears in BOTH custom and manifest, custom wins.
        Regression guard for the shadow-semantics flip — without it, a user's
        ``gpt-5`` custom entry would be invisible at routing time."""
        from src.server.handlers.chat.llm_config import classify_model, ModelSource

        custom_entry = {"name": "gpt-5", "model_id": "gpt-5", "provider": "my-openai"}
        mock_mc = MagicMock()
        mock_mc.get_model_config.return_value = {"provider": "openai", "model_id": "gpt-5"}
        with (
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=custom_entry,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            source, cfg = await classify_model("user-1", "gpt-5")

        assert source == ModelSource.CUSTOM
        assert cfg is custom_entry
        # System lookup never even consulted — short-circuit on custom hit.
        mock_mc.get_model_config.assert_not_called()


class TestClassifyModelDedup:
    @pytest.mark.asyncio
    async def test_classify_called_once_per_distinct_model(self, base_config):
        """resolve_llm_config should classify each distinct model exactly once
        at the orchestration layer: one call for the main model and one per
        fallback (via _resolve_one). Inner helpers are mocked, so any
        re-classification they do internally is excluded from this count.
        """
        from src.server.handlers.chat.llm_config import (
            resolve_llm_config,
            ModelSource,
        )

        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model", "fb-a", "fb-b"}
        )

        async def _classify_side_effect(user_id, model_name, _pref_cache=None):
            return ModelSource.SYSTEM, {"provider": "openai"}

        classify_mock = AsyncMock(side_effect=_classify_side_effect)

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={"fallback_models": ["fb-a", "fb-b"]},
            ),
            patch(f"{HANDLER}.classify_model", classify_mock),
            patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
            patch(f"{HANDLER}.resolve_byok_llm_client", new_callable=AsyncMock, return_value=None),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
        ):
            await resolve_llm_config(base_config, "user-1", None, True)

        # 1 main + 2 fallbacks = 3 classification calls. No more, no less.
        assert classify_mock.await_count == 3
        called_names = [call.args[1] for call in classify_mock.await_args_list]
        assert set(called_names) == {"system-default-model", "fb-a", "fb-b"}


# ---------------------------------------------------------------------------
# Stale-preference recovery: saved model vanished from the manifest
# ---------------------------------------------------------------------------


class TestStaleModelPreference:
    @pytest.mark.asyncio
    async def test_stale_preferred_model_scrubs_pref_and_raises(self, base_config):
        """Saved preferred_model no longer in manifest → scrub pref + raise
        model_removed CTA. Next request won't re-hit the same wall."""
        from fastapi import HTTPException

        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model"}
        )
        pref = {"preferred_model": "qwen3.5-flash"}

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value=pref,
            ),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                f"{HANDLER}.get_custom_provider_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            patch(
                "src.server.database.user.upsert_user_preferences",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "src.server.database.user.invalidate_user_prefs_cache",
                new_callable=AsyncMock,
            ) as mock_invalidate,
        ):
            with pytest.raises(HTTPException) as excinfo:
                await resolve_llm_config(base_config, "user-1", None, False)

        detail = excinfo.value.detail
        assert isinstance(detail, dict)
        assert detail.get("type") == "model_removed"
        assert "qwen3.5-flash" in detail.get("message", "")
        assert detail.get("link", {}).get("url")

        # Pref was scrubbed: preferred_model deleted via None value
        mock_upsert.assert_awaited_once()
        kwargs = mock_upsert.await_args.kwargs
        assert kwargs["user_id"] == "user-1"
        assert kwargs["other_preference"] == {"preferred_model": None}
        # Cache is invalidated twice: once to bust stale cache before the
        # race-safe re-read, once after the write lands.
        assert mock_invalidate.await_count == 2
        for call in mock_invalidate.await_args_list:
            assert call.args == ("user-1",)

    @pytest.mark.asyncio
    async def test_stale_pref_bulk_scrubs_fallback_list(self, base_config):
        """When the main model is stale, scrub every stale entry in one DB
        write — fallback_models included — so the user doesn't hit cascading
        errors on subsequent requests."""
        from fastapi import HTTPException

        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model", "good-model"}
        )
        pref = {
            "preferred_model": "qwen3.5-flash",
            "fetch_model": "gone-too",
            "fallback_models": ["good-model", "also-gone", "qwen3.5-flash"],
        }

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value=pref,
            ),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                f"{HANDLER}.get_custom_provider_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            patch(
                "src.server.database.user.upsert_user_preferences",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "src.server.database.user.invalidate_user_prefs_cache",
                new_callable=AsyncMock,
            ),
        ):
            with pytest.raises(HTTPException):
                await resolve_llm_config(base_config, "user-1", None, False)

        written = mock_upsert.await_args.kwargs["other_preference"]
        # Stale scalars deleted, fallback list filtered down to only "good-model"
        assert written["preferred_model"] is None
        assert written["fetch_model"] is None
        assert written["fallback_models"] == ["good-model"]

    @pytest.mark.asyncio
    async def test_stale_request_model_raises_without_scrub(self, base_config):
        """Frontend sent a stale model name as request_model (e.g. its
        React Query cache still held the pre-scrub value). Raise
        model_removed so the UI shows the CTA banner, but don't scrub the
        saved prefs — there's nothing in them to clean."""
        from fastapi import HTTPException

        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model"}
        )
        # Prefs already clean from a prior scrub pass
        pref = {}

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value=pref,
            ),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                f"{HANDLER}.get_custom_provider_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            patch(
                "src.server.database.user.upsert_user_preferences",
                new_callable=AsyncMock,
            ) as mock_upsert,
        ):
            with pytest.raises(HTTPException) as excinfo:
                await resolve_llm_config(
                    base_config, "user-1", "qwen3.5-flash", False
                )

        detail = excinfo.value.detail
        assert detail.get("type") == "model_removed"
        assert "qwen3.5-flash" in detail.get("message", "")
        # No DB write — prefs are already clean, request_model has no pref to scrub
        mock_upsert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_yaml_default_stale_does_not_raise_model_removed(
        self, base_config
    ):
        """If effective_model came from agent_config.yaml (not user pref),
        fall through so the downstream error path surfaces the server bug —
        raising model_removed would mislead every user on the instance."""
        from src.server.handlers.chat.llm_config import resolve_llm_config

        # YAML default is a model the mock_mc doesn't know about → UNKNOWN,
        # but no user pref references it, so cleanup shouldn't flag it as
        # "from pref" and shouldn't raise model_removed.
        stale_yaml_config = _make_config(name="stale-yaml-default")
        mock_mc = _mock_model_config(system_models={"system-flash-model"})

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                f"{HANDLER}.get_custom_provider_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                f"{HANDLER}.resolve_oauth_llm_client",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            patch(
                "src.server.database.user.upsert_user_preferences",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "src.server.database.user.invalidate_user_prefs_cache",
                new_callable=AsyncMock,
            ),
        ):
            # Should NOT raise model_removed — falls through to normal resolution
            result = await resolve_llm_config(
                stale_yaml_config, "user-1", None, False
            )

        # No pref changes were written because no stale pref values existed
        mock_upsert.assert_not_awaited()
        # Config returned without an injected client — downstream will raise
        # the original ValueError so the admin can fix agent_config.yaml
        assert result is not None

    @pytest.mark.asyncio
    async def test_concurrent_save_is_not_clobbered(self, base_config):
        """Race guard: the caller's pref snapshot says preferred_model is
        stale, but between the snapshot read and the scrub write, the user
        clicked Save in Settings and wrote a fresh valid value. The scrub
        must NOT overwrite the fresh save — it re-reads the DB after busting
        the cache and skips keys whose current value differs from the stale
        name it originally detected."""
        from fastapi import HTTPException

        from src.server.handlers.chat.llm_config import resolve_llm_config

        mock_mc = _mock_model_config(
            system_models={"system-default-model", "system-flash-model", "freshly-saved"}
        )
        # Caller's snapshot: stale preferred_model
        snapshot_pref = {"preferred_model": "qwen3.5-flash"}
        # DB after the user's concurrent Save landed: new valid value
        fresh_pref = {"preferred_model": "freshly-saved"}

        call_count = {"n": 0}

        async def prefs_sequence(_user_id):
            """First call returns the stale snapshot (seen by resolve_llm_config).
            Second call (inside the scrub after cache-bust) returns the
            post-save fresh state."""
            call_count["n"] += 1
            return snapshot_pref if call_count["n"] == 1 else fresh_pref

        with (
            patch(
                f"{HANDLER}.get_model_preference",
                new=prefs_sequence,
            ),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                f"{HANDLER}.get_custom_provider_config",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
            patch(
                "src.server.database.user.upsert_user_preferences",
                new_callable=AsyncMock,
            ) as mock_upsert,
            patch(
                "src.server.database.user.invalidate_user_prefs_cache",
                new_callable=AsyncMock,
            ) as mock_invalidate,
        ):
            with pytest.raises(HTTPException) as excinfo:
                await resolve_llm_config(base_config, "user-1", None, False)

        # Still raises model_removed for THIS request (its model is unusable)
        detail = excinfo.value.detail
        assert detail.get("type") == "model_removed"

        # The cache gets busted so the re-read hits Postgres, but since the
        # fresh DB value no longer matches the stale snapshot name, NO
        # delete is queued and NO write happens. The user's just-saved
        # "freshly-saved" pref survives.
        mock_upsert.assert_not_awaited()
        # Cache was invalidated once (pre-read), never a second time because
        # no write happened.
        assert mock_invalidate.await_count == 1
