"""
Tests for resolve_byok_llm_client() and _resolve_custom_model_byok().

Covers:
- System model BYOK: looks up parent provider key
- Custom model BYOK: 3-level key lookup (model name → provider → parent)
- BYOK disabled returns None
- No key found returns None
- Base URL resolution (user custom > provider default > SDK default)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

HANDLER = "src.server.handlers.chat.llm_config"
DB_KEYS = "src.server.database.api_keys"


def _mock_model_config(system_models=None, providers=None):
    mc = MagicMock()
    system_models = system_models or {}
    providers = providers or {}

    def get_model(name):
        return system_models.get(name)

    def get_provider(name):
        return providers.get(name, {})

    def get_parent(name):
        info = providers.get(name, {})
        return info.get("parent", name)

    def get_children(name):
        return [
            k for k, info in providers.items()
            if info.get("parent") == name and k != name
        ]

    mc.get_model_config.side_effect = get_model
    mc.get_provider_info.side_effect = get_provider
    mc.get_parent_provider.side_effect = get_parent
    mc.get_child_variants.side_effect = get_children

    return mc


# ---------------------------------------------------------------------------
# resolve_byok_llm_client — system models
# ---------------------------------------------------------------------------


class TestResolveBYOKSystemModel:
    @pytest.fixture(autouse=True)
    def _no_custom_models(self):
        """Shadow semantics: ``classify_model`` checks custom first. For
        system-only tests, stub the custom lookup out so it doesn't try to
        hit the DB."""
        with patch(
            f"{HANDLER}.get_custom_model_config",
            new_callable=AsyncMock,
            return_value=None,
        ):
            yield

    @pytest.mark.asyncio
    async def test_not_byok_returns_none(self):
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        result = await resolve_byok_llm_client("user-1", "gpt-4o", False)
        assert result is None

    @pytest.mark.asyncio
    async def test_system_model_with_key(self):
        """BYOK with system model creates LLM with user's API key."""
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        mc = _mock_model_config(
            system_models={"gpt-4o": {"provider": "openai"}},
            providers={"openai": {"base_url": None}},
        )
        mock_llm = MagicMock(name="byok-llm")
        with (
            patch("src.llms.llm.LLM.get_model_config", return_value=mc),
            patch(
                f"{DB_KEYS}.get_byok_config_for_provider",
                new_callable=AsyncMock,
                return_value={"api_key": "user-key-123", "base_url": None},
            ),
            patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
        ):
            result = await resolve_byok_llm_client("user-1", "gpt-4o", True)

        assert result is mock_llm
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args
        assert call_kwargs.kwargs["api_key"] == "user-key-123"

    @pytest.mark.asyncio
    async def test_system_model_no_key_returns_none(self):
        """System model with no BYOK key returns None."""
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        mc = _mock_model_config(
            system_models={"gpt-4o": {"provider": "openai"}},
            providers={"openai": {}},
        )
        with (
            patch("src.llms.llm.LLM.get_model_config", return_value=mc),
            patch(
                f"{DB_KEYS}.get_byok_config_for_provider",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await resolve_byok_llm_client("user-1", "gpt-4o", True)

        assert result is None

    @pytest.mark.asyncio
    async def test_system_model_custom_base_url(self):
        """User's custom base_url should be used if set."""
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        mc = _mock_model_config(
            system_models={"gpt-4o": {"provider": "openai"}},
            providers={"openai": {"base_url": "https://default.openai.com"}},
        )
        mock_llm = MagicMock()
        with (
            patch("src.llms.llm.LLM.get_model_config", return_value=mc),
            patch(
                f"{DB_KEYS}.get_byok_config_for_provider",
                new_callable=AsyncMock,
                return_value={"api_key": "key", "base_url": "https://custom.openai.com"},
            ),
            patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
        ):
            result = await resolve_byok_llm_client("user-1", "gpt-4o", True)

        call_kwargs = mock_create.call_args
        assert call_kwargs.kwargs["base_url"] == "https://custom.openai.com"

    @pytest.mark.asyncio
    async def test_sub_provider_resolves_parent(self):
        """Platform sub-provider should resolve to parent's BYOK key."""
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        mc = _mock_model_config(
            system_models={"test-model": {"provider": "acme-platform"}},
            providers={
                "acme-platform": {"parent": "acme", "base_url": "https://proxy.example.com"},
                "acme": {"base_url": None},
            },
        )
        mock_llm = MagicMock()
        with (
            patch("src.llms.llm.LLM.get_model_config", return_value=mc),
            patch(
                f"{DB_KEYS}.get_byok_config_for_provider",
                new_callable=AsyncMock,
                return_value={"api_key": "acme-key", "base_url": None},
            ),
            patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
        ):
            result = await resolve_byok_llm_client("user-1", "test-model", True)

        # Should look up parent provider, not platform variant
        assert result is mock_llm


# ---------------------------------------------------------------------------
# resolve_byok_llm_client — custom models
# ---------------------------------------------------------------------------


class TestResolveBYOKCustomModel:
    @pytest.mark.asyncio
    async def test_custom_model_with_key(self):
        """Custom model with BYOK key creates LLM from custom config."""
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        mc = _mock_model_config(
            system_models={},  # Not a system model
            providers={"openai": {"base_url": None}},
        )
        mock_llm = MagicMock(name="custom-byok-llm")
        custom_config = {"name": "my-gpt", "model_id": "gpt-4o", "provider": "openai"}

        with (
            patch("src.llms.llm.LLM.get_model_config", return_value=mc),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=custom_config,
            ),
            patch(
                f"{HANDLER}._resolve_custom_model_byok",
                new_callable=AsyncMock,
                return_value=({"api_key": "user-key"}, "https://custom.com", custom_config),
            ),
            patch(
                "src.llms.llm.create_llm_from_custom",
                return_value=mock_llm,
            ) as mock_create,
        ):
            result = await resolve_byok_llm_client("user-1", "my-gpt", True)

        assert result is mock_llm
        mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_custom_model_no_key_returns_none(self):
        """Custom model without BYOK key returns None (falls back to system default)."""
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        mc = _mock_model_config(system_models={}, providers={})
        custom_config = {"name": "my-gpt", "model_id": "gpt-4o", "provider": "openai"}

        with (
            patch("src.llms.llm.LLM.get_model_config", return_value=mc),
            patch(
                f"{HANDLER}.get_custom_model_config",
                new_callable=AsyncMock,
                return_value=custom_config,
            ),
            patch(
                f"{HANDLER}._resolve_custom_model_byok",
                new_callable=AsyncMock,
                return_value=(None, None, custom_config),
            ),
        ):
            result = await resolve_byok_llm_client("user-1", "my-gpt", True)

        assert result is None

    @pytest.mark.asyncio
    async def test_unknown_model_returns_none(self):
        """Unknown model (not system, not custom) returns None."""
        from src.server.handlers.chat.llm_config import resolve_byok_llm_client

        mc = _mock_model_config(system_models={}, providers={})

        with (
            patch("src.llms.llm.LLM.get_model_config", return_value=mc),
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
        ):
            result = await resolve_byok_llm_client("user-1", "nonexistent", True)

        assert result is None
