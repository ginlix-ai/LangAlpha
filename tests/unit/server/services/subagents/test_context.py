"""The user's own facts behind a pane's problems, read as ``load_context`` reads them."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.llms.llm import LLM
from src.server.services.subagents import context
from src.server.services.subagents.view import build_views
from tests.unit.server.services.subagents._fakes import USER, World

pytestmark = pytest.mark.asyncio


def _oauth_model(*, plan_gated: bool = False) -> tuple[str, str, list[str]]:
    """A manifest model served only on an OAuth account, its provider and the
    plans it is offered on."""
    mc = LLM.get_model_config()
    for name, info in mc.llm_config.items():
        provider = info.get("provider")
        if mc.get_provider_info(provider).get("access_type") != "oauth":
            continue
        if bool(info.get("oauth_plans")) == plan_gated:
            return name, provider, info.get("oauth_plans") or []
    pytest.skip("the manifest has no such OAuth-served model")


def _keyed_model() -> tuple[str, str, str]:
    """A manifest model served on an API key, its provider and the key's variable."""
    mc = LLM.get_model_config()
    for name, info in mc.llm_config.items():
        provider = info.get("provider")
        if mc.get_provider_info(provider).get("access_type") in ("oauth", "local"):
            continue
        env_key = LLM(name).env_key
        if env_key:
            return name, provider, env_key
    pytest.skip("the manifest has no model served on an API key")


@pytest.fixture
def accounts(monkeypatch) -> dict[str, str | None]:
    """``load_context`` over the real manifest, with the OAuth providers the
    user has connected (and each one's plan) read from this dict and every
    other read in memory."""
    connected: dict[str, str | None] = {}

    async def status(user_id: str, provider: str) -> dict | None:
        return {"plan_type": connected[provider]} if provider in connected else None

    async def no_settings(_user_id):
        return {}

    async def ungated(*_args):
        return None

    async def nothing(*_args, **_kwargs):
        return frozenset()

    async def connectors(*_args, **_kwargs):
        return (), False

    async def not_byok(_user_id):
        return False

    config = SimpleNamespace(
        subagents=None, features={}, search_api="tavily", search_depth="standard"
    )
    config.model_copy = lambda **_kwargs: config
    world = World()

    async def roster(*_args, **_kwargs):
        return dict(world.bases), tuple(world.offered)

    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    monkeypatch.setattr("src.server.database.oauth_tokens.get_oauth_connection", status)
    monkeypatch.setattr("src.server.database.api_keys.is_byok_active", not_byok)
    monkeypatch.setattr("src.server.services.llm.user_models.get_model_preference", no_settings)
    monkeypatch.setattr("src.server.services.llm.capabilities.gate_capabilities", ungated)
    monkeypatch.setattr(context, "_agent_config", lambda: config)
    monkeypatch.setattr(context, "_skill_names", nothing)
    monkeypatch.setattr(context, "_connectors", connectors)
    monkeypatch.setattr(context, "roster", roster)
    return connected


@pytest.mark.parametrize("connected", [False, True])
async def test_an_oauth_pin_is_unavailable_until_its_account_is_connected(accounts, connected):
    # With no account the turn builds no client for the role and runs the
    # subagent on the thread's model, so the pane has to say the pin won't run.
    model, provider, _ = _oauth_model()
    if connected:
        accounts[provider] = None
    assert await _flagged(model) is not connected


async def test_an_oauth_pin_on_an_account_needing_sign_in_is_unavailable(accounts, monkeypatch):
    # valid_token refuses the account, so the turn runs the main model instead.
    model, provider, _ = _oauth_model()

    async def flagged(_user_id, p):
        return {"plan_type": None, "needs_reauth_at": "2026-10-25T00:00:00Z"} if p == provider else None

    monkeypatch.setattr("src.server.database.oauth_tokens.get_oauth_connection", flagged)
    assert await _flagged(model) is True


@pytest.mark.parametrize(("plan", "runs"), [("other-plan", False), (None, True), ("listed", True)])
async def test_an_oauth_pin_runs_only_on_a_plan_it_is_offered_on(accounts, plan, runs):
    # The turn refuses a known plan off the model's list (oauth_plan_unsupported)
    # and lets an unknown one through, so the pane says the same.
    model, provider, plans = _oauth_model(plan_gated=True)
    accounts[provider] = plans[0].upper() if plan == "listed" else plan
    assert await _flagged(model) is not runs


@pytest.mark.parametrize(
    ("server_key", "own_key", "runs"),
    [(False, False, False), (True, False, True), (False, True, True)],
)
async def test_a_catalog_pin_runs_on_the_servers_key_or_the_users(
    accounts, monkeypatch, server_key, own_key, runs
):
    # With neither, the role gets no client and the turn runs the subagent on
    # the thread's model.
    model, provider, env_key = _keyed_model()
    if server_key:
        monkeypatch.setenv(env_key, "placeholder")
    else:
        monkeypatch.delenv(env_key, raising=False)

    async def byok(_user_id):
        return own_key

    async def configs(_user_id, slugs):
        return {provider: {"api_key": "placeholder"}} if own_key and provider in slugs else {}

    monkeypatch.setattr("src.server.database.api_keys.is_byok_active", byok)
    monkeypatch.setattr("src.server.database.api_keys.get_byok_configs_for_providers", configs)
    assert await _flagged(model) is not runs


@pytest.mark.parametrize(
    ("byok", "own_key", "runs"),
    [(False, False, False), (True, False, False), (True, True, True)],
)
async def test_a_custom_provider_pin_runs_only_on_its_own_key(
    accounts, monkeypatch, byok, own_key, runs
):
    # A custom provider's name picked as the model is served on that
    # provider's key or not at all, so with none the role gets no client.
    slug = "my-gateway"

    async def settings(_user_id):
        return {"custom_providers": [{"name": slug, "parent_provider": "openai"}]}

    async def byok_active(_user_id):
        return byok

    async def config(_user_id, provider):
        return {"api_key": "placeholder"} if own_key and provider == slug else None

    async def configs(_user_id, _slugs):
        return {}

    monkeypatch.setattr("src.server.services.llm.user_models.get_model_preference", settings)
    monkeypatch.setattr("src.server.database.api_keys.is_byok_active", byok_active)
    monkeypatch.setattr("src.server.database.api_keys.get_byok_config_for_provider", config)
    monkeypatch.setattr("src.server.database.api_keys.get_byok_configs_for_providers", configs)
    assert await _flagged(slug) is not runs


@pytest.mark.parametrize("connected", [False, True])
async def test_a_custom_model_on_a_subscription_runs_on_its_account(accounts, monkeypatch, connected):
    # A model the subscription lists but the manifest doesn't is saved as a
    # custom entry, and the turn runs it on the sign-in, with no key and BYOK off.
    _, provider, _ = _oauth_model()
    if connected:
        accounts[provider] = None

    async def settings(_user_id):
        return {"custom_models": [{"name": "untuned", "model_id": "untuned", "provider": provider}]}

    monkeypatch.setattr("src.server.services.llm.user_models.get_model_preference", settings)
    assert await _flagged("untuned") is not connected


async def _flagged(model: str) -> bool:
    world = World()
    world.override("research", model=model)
    views = await build_views(await context.load_context(USER), list(world.rows.values()))
    research = next(v for v in views if v["name"] == "research")
    return {"code": "model_unavailable", "detail": model} in research["problems"]


@pytest.mark.parametrize("opted_in", [False, True])
async def test_a_skill_behind_a_feature_counts_once_the_user_turns_it_on(monkeypatch, opted_in):
    # The turn's registry applies the user's own feature flags, so a skill
    # behind one the user hasn't turned on is never preloaded; the names a
    # preference may hold leave it out the same way.
    from src.config.features import FEATURES
    from ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY

    gated = next(s for s in SKILL_REGISTRY.values() if s.feature in FEATURES and not s.hidden)

    async def flags(_user_id):
        return {gated.feature: opted_in}

    async def own(_user_id):
        return [{"name": "my-notes"}]

    monkeypatch.setattr("src.server.services.features.effective_flags_for_user", flags)
    monkeypatch.setattr("src.server.database.user_skills.list_user_skills", own)

    names = await context._skill_names(USER)

    assert (gated.name in names) is opted_in
    assert "my-notes" in names
    assert "chart-annotation" not in names


@pytest.mark.parametrize("saved", [True, False])
async def test_the_main_agents_model_is_the_one_its_threads_run(monkeypatch, saved):
    # The pane compares each subagent with the main agent, so the main agent's
    # model is the account default a new thread runs: the saved one, or the
    # deployment's when none is saved.
    from src.server.services.llm import thread_model

    first, second = list(LLM.get_model_config().llm_config)[:2]
    monkeypatch.setattr(thread_model, "system_default_models", lambda: {"default_model": second})
    pref = {"preferred_model": first} if saved else {}

    values = context._main_values(pref, "tavily", "basic")

    assert values["model"] == (first if saved else second)
    assert (values["search_provider"], values["search_depth"]) == ("tavily", "basic")
