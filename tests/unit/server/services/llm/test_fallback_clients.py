"""A fallback never mixes our bill with the user's: a turn on the user's own
key or subscription builds no fallback on the platform's key, whoever saved
the list."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.config.agent import CredentialSource
from src.server.services.llm import clients
from src.server.services.llm.clients import ResolvedClient
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


async def _fallbacks(credential: CredentialSource, *names: str) -> list[str]:
    config = SimpleNamespace(
        credential_source=credential, prompt_guidance=None,
        llm=SimpleNamespace(fallback=list(names)),
    )
    await clients._resolve_fallback_clients(
        config, "user-1", {}, {}, is_byok=True, cache_key="t-1",
    )
    assert [c.model for c in config.fallback_llm_clients] == config.fallback_llm_names
    return config.fallback_llm_names


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", OWN_KEY)
async def test_an_own_key_turn_keeps_only_the_fallbacks_the_user_pays_for(ladder, credential):
    ladder.own = {"keyed": CredentialSource.BYOK, "signed-in": CredentialSource.OAUTH}

    assert await _fallbacks(credential, "keyless", "keyed", "signed-in") == ["keyed", "signed-in"]


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", NO_OWN_KEY)
async def test_a_platform_turn_runs_every_fallback(ladder, credential):
    ladder.own = {"keyed": CredentialSource.BYOK}

    assert await _fallbacks(credential, "keyless", "keyed") == ["keyless", "keyed"]
