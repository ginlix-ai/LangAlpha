"""A model a subscription lists but the manifest does not tune is saved as a
custom model on the subscription's provider ("Show all"). It runs on the
subscription's sign-in, like a built-in of that provider, with no API key."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.config.agent import CredentialSource
from src.server.services import oauth_health
from src.server.services.llm.user_models import ModelSource
from src.server.services.oauth_protocol import TokenSet

CLIENTS = "src.server.services.llm.clients"

_ENTRY = {"name": "listed-model", "model_id": "listed-model", "provider": "codex-oauth"}
_PREF = {"custom_models": [_ENTRY]}


def _token() -> TokenSet:
    return TokenSet(
        access_token="access-example",
        refresh_token="refresh-example",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        account_id="account-example",
    )


@pytest.mark.asyncio
async def test_custom_model_on_a_subscription_runs_on_its_token():
    from src.server.services.llm.clients import resolve_model_client

    built = MagicMock(name="client")
    byok = AsyncMock(return_value=None)
    with (
        patch.object(oauth_health, "valid_token", AsyncMock(return_value=_token())),
        patch("src.llms.llm.create_llm_from_custom", return_value=built) as from_custom,
        patch(f"{CLIENTS}.resolve_byok_llm_client", byok),
    ):
        resolved = await resolve_model_client(
            "u1", "listed-model", is_byok=False, _pref_cache=_PREF,
        )

    assert resolved.client is built
    assert resolved.model_source == ModelSource.CUSTOM
    assert resolved.credential_source == CredentialSource.OAUTH
    entry = from_custom.call_args.args[0]
    assert entry["provider"] == "codex-oauth"
    kwargs = from_custom.call_args.kwargs
    assert kwargs["api_key"] == "access-example"
    assert kwargs["default_headers"] == {"ChatGPT-Account-Id": "account-example"}
    byok.assert_not_called()


@pytest.mark.asyncio
async def test_custom_model_on_an_unconnected_subscription_asks_to_connect():
    from src.server.services.llm.clients import resolve_model_client

    with patch.object(oauth_health, "valid_token", AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as raised:
            await resolve_model_client("u1", "listed-model", is_byok=False, _pref_cache=_PREF)
    assert raised.value.detail["type"] == "oauth_required"


@pytest.mark.asyncio
async def test_custom_model_on_a_key_provider_does_not_reach_the_subscription():
    from src.server.services.llm.clients import resolve_oauth_llm_client

    token = AsyncMock(return_value=_token())
    with patch.object(oauth_health, "valid_token", token):
        client = await resolve_oauth_llm_client(
            "u1", "keyed", custom_entry={"name": "keyed", "model_id": "keyed", "provider": "openai"},
        )
    assert client is None
    token.assert_not_called()


@pytest.mark.asyncio
async def test_availability_needs_no_key_for_a_subscription_custom_model():
    from src.server.services.llm.config import check_model_available

    config = MagicMock(input_modalities=None)
    # No raise: the subscription, not a key, serves it.
    assert await check_model_available(
        config, "u1", _PREF, "listed-model", None, is_byok=False,
    ) is True

    keyed = {"custom_models": [{"name": "keyed", "model_id": "keyed", "provider": "openai"}]}
    with pytest.raises(HTTPException) as raised:
        await check_model_available(config, "u1", keyed, "keyed", None, is_byok=False)
    assert raised.value.detail["type"] == "byok_key_required"


@pytest.mark.asyncio
async def test_plan_gate_passes_a_subscription_custom_model_under_a_builtin_name():
    from src.server.services.llm.availability import model_on_plan

    mc = MagicMock()
    mc.get_model_config.return_value = {"provider": "openai", "tier": 2}
    pref = {"custom_models": [{"name": "tiered", "model_id": "tiered", "provider": "codex-oauth"}]}

    async def no_tier(_tier):
        return False

    with (
        patch("src.server.dependencies.usage_limits.platform_gating_active", return_value=True),
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
    ):
        assert await model_on_plan(
            "u1", "tiered", model_pref=pref, permits=no_tier, is_byok=False, byok_cache={},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("params", [
    {"base_url": "https://collector.example/v1"},
    {"model_kwargs": {"openai_api_base": "https://collector.example/v1"}},
    {"openai_proxy": "http://proxy.example:8080"},
])
async def test_a_subscription_token_never_goes_to_an_endpoint_the_entry_names(params):
    from src.server.services.llm.clients import resolve_model_client

    entry = {**_ENTRY, "parameters": params}
    from_custom = MagicMock()
    with (
        patch.object(oauth_health, "valid_token", AsyncMock(return_value=_token())),
        patch("src.llms.llm.create_llm_from_custom", from_custom),
    ):
        with pytest.raises(HTTPException) as raised:
            await resolve_model_client(
                "u1", "listed-model", is_byok=False, _pref_cache={"custom_models": [entry]},
            )
    assert raised.value.detail["type"] == "oauth_custom_endpoint"
    from_custom.assert_not_called()


@pytest.mark.asyncio
async def test_saving_a_subscription_model_with_an_endpoint_is_refused():
    from src.server.services.llm.custom_catalog import refuse_custom_model_endpoints

    entry = {**_ENTRY, "parameters": {"base_url": "https://collector.example/v1"}}
    with pytest.raises(HTTPException) as raised:
        await refuse_custom_model_endpoints([entry])
    assert "subscription" in raised.value.detail
    await refuse_custom_model_endpoints([_ENTRY])
    # One saved before the rule does not block the rest of the list; the
    # client is refused where it is built instead.
    await refuse_custom_model_endpoints([entry], stored=[entry])
