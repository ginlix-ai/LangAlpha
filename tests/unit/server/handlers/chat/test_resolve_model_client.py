"""Tests for resolve_model_client() — the unified resolution primitive.

Covers credential-source attribution (OAUTH / BYOK / PLATFORM / NONE), the
platform-fallback gate (SYSTEM only), the orthogonality of model_source vs
credential_source, and HTTPException propagation from the OAuth path.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import CredentialSource

HANDLER = "src.server.services.llm.config"
USER_MODELS = "src.server.services.llm.user_models"
CLIENTS = "src.server.services.llm.clients"


def _patch_classify(source):
    """Patch classify_model to return (source, {})."""
    from src.server.services.llm.user_models import ModelSource  # noqa: F401

    return patch(
        f"{USER_MODELS}.classify_model",
        new_callable=AsyncMock,
        return_value=(source, {}),
    )


@pytest.mark.asyncio
async def test_oauth_present_returns_oauth_and_skips_byok():
    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    oauth_client = MagicMock(name="oauth-client")
    byok = AsyncMock(name="byok")
    with (
        _patch_classify(ModelSource.SYSTEM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=oauth_client,
        ),
        patch(f"{CLIENTS}.resolve_byok_llm_client", byok),
    ):
        result = await resolve_model_client("u", "m", is_byok=True)

    assert result.client is oauth_client
    assert result.credential_source == CredentialSource.OAUTH
    byok.assert_not_called()


@pytest.mark.asyncio
async def test_byok_present_returns_byok():
    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    byok_client = MagicMock(name="byok-client")
    with (
        _patch_classify(ModelSource.SYSTEM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{CLIENTS}.resolve_byok_llm_client",
            new_callable=AsyncMock,
            return_value=byok_client,
        ),
    ):
        result = await resolve_model_client("u", "m", is_byok=True)

    assert result.client is byok_client
    assert result.credential_source == CredentialSource.BYOK


@pytest.mark.asyncio
async def test_platform_fallback_when_byok_none_and_system():
    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    platform_client = MagicMock(name="platform-client")
    with (
        _patch_classify(ModelSource.SYSTEM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{CLIENTS}.resolve_byok_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch("src.llms.llm.create_llm", return_value=platform_client) as mock_create,
    ):
        result = await resolve_model_client(
            "u", "m", is_byok=True, allow_platform_fallback=True,
        )

    assert result.client is platform_client
    assert result.credential_source == CredentialSource.PLATFORM
    mock_create.assert_called_once()


@pytest.mark.asyncio
async def test_platform_fallback_without_a_provider_key_takes_the_lazy_path():
    """An effort makes the platform client eager; a provider with no key
    configured then gives no client, as a turn without an effort gets, so the
    turn fails as a run rather than as a 500 before it starts."""
    from src.llms.llm import MissingProviderKeyError
    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    with (
        _patch_classify(ModelSource.SYSTEM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{CLIENTS}.resolve_byok_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "src.llms.llm.create_llm",
            side_effect=MissingProviderKeyError("EXAMPLE_API_KEY environment variable is not set"),
        ),
    ):
        result = await resolve_model_client(
            "u", "m", is_byok=False, reasoning_effort="high", allow_platform_fallback=True,
        )

    assert result.client is None
    assert result.credential_source == CredentialSource.NONE


def test_a_provider_with_no_key_raises_the_missing_key_error(monkeypatch):
    from src.llms.llm import LLM, MissingProviderKeyError

    llm = LLM.__new__(LLM)
    llm.api_key_override = None
    llm.env_key = "EXAMPLE_PROVIDER_API_KEY"
    llm.provider_info = {}
    monkeypatch.delenv("EXAMPLE_PROVIDER_API_KEY", raising=False)

    with pytest.raises(MissingProviderKeyError):
        llm._resolve_api_key()


def test_an_empty_user_key_never_takes_the_deployments(monkeypatch):
    # A saved row with no key but the user's base URL: the deployment's key
    # would go to the user's server.
    from src.llms.llm import LLM

    llm = LLM.__new__(LLM)
    llm.api_key_override = ""
    llm.env_key = "EXAMPLE_PROVIDER_API_KEY"
    llm.provider_info = {"access_type": "api_key"}
    monkeypatch.setenv("EXAMPLE_PROVIDER_API_KEY", "deployment-secret")

    assert llm._resolve_api_key() == "EMPTY"


@pytest.mark.asyncio
async def test_no_fallback_when_disabled():
    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    with (
        _patch_classify(ModelSource.SYSTEM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{CLIENTS}.resolve_byok_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch("src.llms.llm.create_llm") as mock_create,
    ):
        result = await resolve_model_client(
            "u", "m", is_byok=True, allow_platform_fallback=False,
        )

    assert result.client is None
    assert result.credential_source == CredentialSource.NONE
    mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_platform_fallback_not_built_for_custom():
    """Platform fallback only applies to SYSTEM models, never CUSTOM."""
    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    with (
        _patch_classify(ModelSource.CUSTOM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{CLIENTS}.resolve_byok_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch("src.llms.llm.create_llm") as mock_create,
    ):
        result = await resolve_model_client(
            "u", "m", is_byok=True, allow_platform_fallback=True,
        )

    assert result.client is None
    assert result.credential_source == CredentialSource.NONE
    mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_byok_on_system_model_is_orthogonal():
    """A BYOK user on a SYSTEM-catalog model: model_source=SYSTEM, cred=BYOK."""
    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    byok_client = MagicMock(name="byok-client")
    with (
        _patch_classify(ModelSource.SYSTEM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{CLIENTS}.resolve_byok_llm_client",
            new_callable=AsyncMock,
            return_value=byok_client,
        ),
    ):
        result = await resolve_model_client("u", "m", is_byok=True)

    assert result.model_source == ModelSource.SYSTEM
    assert result.credential_source == CredentialSource.BYOK


@pytest.mark.asyncio
async def test_oauth_required_exception_propagates():
    """An OAuth-required HTTPException must NOT be swallowed."""
    from fastapi import HTTPException

    from src.server.services.llm.clients import resolve_model_client
    from src.server.services.llm.user_models import ModelSource

    exc = HTTPException(status_code=400, detail={"type": "oauth_required"})
    with (
        _patch_classify(ModelSource.SYSTEM),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            side_effect=exc,
        ),
        patch(f"{CLIENTS}.resolve_byok_llm_client", new_callable=AsyncMock) as byok,
    ):
        with pytest.raises(HTTPException) as raised:
            await resolve_model_client("u", "m", is_byok=True)

    assert raised.value.detail["type"] == "oauth_required"
    byok.assert_not_called()
