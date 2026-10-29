"""A turn never moves onto a payer the user did not choose: the fallbacks they
saved run as picked, credits included, while the deployment's defaults keep a
turn on the user's own key or subscription off the platform's key."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.config.agent import CredentialSource, LLMConfig
from src.server.services.llm import clients
from src.server.services.llm.clients import ResolvedClient
from src.server.services.llm.config import select_model
from src.server.services.llm.user_models import ModelSource

OWN_KEY = (CredentialSource.BYOK, CredentialSource.OAUTH)
NO_OWN_KEY = (CredentialSource.PLATFORM, CredentialSource.NONE)


@pytest.fixture
def ladder(monkeypatch):
    """``resolve_model_client`` over a fixed table of the models the user holds
    a credential for; the platform builds the rest when it is allowed to."""
    state = SimpleNamespace(own={})

    async def _resolve(user_id, model_name, *, is_byok, cache_key=None, reasoning_effort=None,
                       service_tier=None, allow_platform_fallback=False, _pref_cache=None,
                       _byok_cache=None):
        credential = state.own.get(model_name)
        if credential is None and allow_platform_fallback:
            credential = CredentialSource.PLATFORM
        if credential is None:
            return ResolvedClient(None, ModelSource.SYSTEM, CredentialSource.NONE)
        return ResolvedClient(SimpleNamespace(model=model_name), ModelSource.SYSTEM, credential)

    monkeypatch.setattr(clients, "resolve_model_client", _resolve)
    monkeypatch.setattr(clients, "_stamp", lambda *_a, **_k: None)
    return state


async def _fallbacks(credential: CredentialSource, *names: str, saved: bool = False) -> list[str]:
    config = SimpleNamespace(
        credential_source=credential, prompt_guidance=None,
        llm=SimpleNamespace(fallback=list(names)),
    )
    await clients._resolve_fallback_clients(
        config, "user-1", {}, {}, is_byok=True, cache_key="t-1", users_fallbacks=saved,
    )
    assert [c.model for c in config.fallback_llm_clients] == config.fallback_llm_names
    # Whose list it is reaches the middleware, which judges subagent calls.
    assert config.fallback_users_choice is saved
    return config.fallback_llm_names


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", OWN_KEY)
async def test_an_own_key_turn_keeps_only_the_default_fallbacks_the_user_pays_for(ladder, credential):
    ladder.own = {"keyed": CredentialSource.BYOK, "signed-in": CredentialSource.OAUTH}

    assert await _fallbacks(credential, "keyless", "keyed", "signed-in") == ["keyed", "signed-in"]


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", NO_OWN_KEY)
async def test_a_platform_turn_runs_every_fallback(ladder, credential):
    ladder.own = {"keyed": CredentialSource.BYOK}

    assert await _fallbacks(credential, "keyless", "keyed") == ["keyless", "keyed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", OWN_KEY)
async def test_an_own_key_turn_runs_the_fallbacks_the_user_saved_on_credits_too(ladder, credential):
    ladder.own = {"keyed": CredentialSource.BYOK}

    assert await _fallbacks(credential, "keyless", "keyed", saved=True) == ["keyless", "keyed"]


@pytest.mark.parametrize(
    ("pref", "users"),
    [({}, False), ({"fallback_models": []}, True), ({"fallback_models": ["saved"]}, True)],
)
def test_a_saved_fallback_list_is_the_users_even_when_empty(pref, users):
    config = SimpleNamespace(llm=LLMConfig(name="main", fallback=["default"]), llm_client=None)

    assert select_model(config, pref, "primary", None).users_fallbacks is users
