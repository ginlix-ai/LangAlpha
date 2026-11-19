"""The policy on a turn's models: every role held to it, and own access denied
resolving as if it were not there."""

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.agent.middleware.compaction.compact import _summary_fallback
from ptc_agent.config.agent import AgentConfig, CredentialSource, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from src.config.models import PolicyLayer
from src.server.services.account import Standing
from src.server.services.policy import ALLOW_ALL, merge

CONFIG = "src.server.services.llm.config"
CLIENTS = "src.server.services.llm.clients"
USER_MODELS = "src.server.services.llm.user_models"

MODELS = {"default", "background", "summary", "fetcher", "fb-1", "fb-2", "ok", "other", "cc"}


def _base(**llm) -> AgentConfig:
    fields = {
        "name": "default",
        "background": "background",
        "compaction": "summary",
        "fetch": "fetcher",
        "fallback": ["fb-1", "fb-2"],
        **llm,
    }
    return AgentConfig(
        llm=LLMConfig(**fields),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="k")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )


def _policy(**raw):
    return merge(None, PolicyLayer.model_validate(raw))


def _mc():
    mc = MagicMock()
    mc.get_model_config.side_effect = lambda name: {"provider": "openai"} if name in MODELS else None
    mc.get_provider_info.return_value = {}
    mc.get_parent_provider.return_value = "openai"
    mc.llm_config = {"x": 1}
    return mc


def _turn(policy, pref=None, *, oauth=None, byok=None):
    stack = ExitStack()
    stack.enter_context(patch(f"{CONFIG}.require_policy", new_callable=AsyncMock, return_value=policy))
    stack.enter_context(
        patch(f"{USER_MODELS}.get_model_preference", new_callable=AsyncMock, return_value=pref or {})
    )
    stack.enter_context(
        patch(f"{CLIENTS}.resolve_oauth_llm_client", oauth or AsyncMock(return_value=None))
    )
    stack.enter_context(
        patch(f"{CLIENTS}.resolve_byok_llm_client", byok or AsyncMock(return_value=None))
    )
    stack.enter_context(patch("src.llms.llm.LLM.get_model_config", return_value=_mc()))
    stack.enter_context(
        patch("src.llms.claude_code.is_claude_code_model", lambda name: name == "cc")
    )
    return stack


async def _resolve(policy, pref=None, request_model=None, *, base=None, is_byok=False, **kw):
    from src.server.services.llm.config import resolve_llm_config

    with _turn(policy, pref, **kw):
        return await resolve_llm_config(base or _base(), "u", request_model, is_byok)


@pytest.mark.asyncio
async def test_a_named_model_the_policy_refuses_is_refused():
    with pytest.raises(HTTPException) as refused:
        await _resolve(_policy(models={"allowed": ["default"]}), request_model="other")
    detail = refused.value.detail
    assert refused.value.status_code == 400
    assert (detail["type"], detail["rule"], detail["model"]) == ("policy_denied", "models.allowed", "other")


@pytest.mark.asyncio
async def test_every_role_of_a_turn_falls_to_an_allowed_model():
    """Saved choices the policy refuses read as unset, and every role the
    deployment names past it falls to the turn's model: background, summary,
    fetch, fallbacks, and the summary's own fallback built by name."""
    pref = {
        "preferred_model": "other",
        "compaction_model": "summary",
        "fallback_models": ["fb-1", "ok", "fb-2"],
    }
    config = await _resolve(_policy(models={"allowed": ["default", "ok"]}), pref)

    llm = config.llm
    assert llm.name == "default"
    assert (llm.background, llm.compaction, llm.fetch_name) == ("default", "default", "default")
    assert llm.fallback == ["ok"]
    assert config.fallback_llm_names == []  # no key builds one in this test
    # The summary's fallback, built by name when no client was resolved.
    assert llm.fallbacks_for(llm.compaction_name) == ["ok"]


@pytest.mark.asyncio
async def test_the_summary_fallback_built_by_name_reaches_only_an_allowed_model():
    config = await _resolve(_policy(models={"allowed": ["default"]}))
    with patch(
        "ptc_agent.agent.middleware.compaction.compact._at_summary_level"
    ) as by_name:
        config.credential_source = CredentialSource.PLATFORM
        assert _summary_fallback(config) is None
    by_name.assert_not_called()


@pytest.mark.asyncio
async def test_a_saved_default_the_policy_refuses_falls_through_to_the_deployments():
    config = await _resolve(_policy(models={"allowed": ["default"]}), {"preferred_model": "other"})
    assert config.llm.name == "default"


@pytest.mark.asyncio
async def test_with_nothing_allowed_left_the_turn_is_refused():
    with pytest.raises(HTTPException) as refused:
        await _resolve(_policy(models={"allowed": ["ok"]}), {"preferred_model": "other"})
    detail = refused.value.detail
    assert (detail["type"], detail["rule"]) == ("policy_denied", "models.allowed")
    assert detail["allowed"] == ["ok"]
    assert "No model" in detail["message"]


@pytest.mark.asyncio
async def test_the_first_allowed_model_served_is_the_default_where_the_deployments_is_not():
    """In the order the list was written; one the user cannot be served is
    passed over, and every role falls to the one picked."""
    with patch(f"{CONFIG}._serves", new=_serves_only({"fb-2", "ok"})):
        config = await _resolve(
            _policy(models={"allowed": ["cc", "other", "ok", "fb-2"]}), {"preferred_model": "summary"},
        )
    llm = config.llm
    assert llm.name == "ok"
    assert (llm.background, llm.compaction, llm.fetch_name) == ("ok", "ok", "ok")
    assert llm.fallback == ["fb-2"]


@pytest.mark.asyncio
async def test_the_organizations_order_picks_the_default():
    from src.config.models import PolicyLayer

    policy = merge(
        PolicyLayer.model_validate({"models": {"allowed": ["ok", "fb-2", "other"]}}),
        PolicyLayer.model_validate({"models": {"allowed": ["other", "fb-2", "ok"]}}),
    )
    assert policy.allowed_in_order == ("other", "fb-2", "ok")
    with patch(f"{CONFIG}._serves", new=_serves_only({"fb-2", "ok"})):
        assert (await _resolve(policy)).llm.name == "fb-2"


@pytest.mark.asyncio
async def test_a_deployment_key_serves_the_first_allowed_model_standalone():
    with patch(f"{CONFIG}.deployment_key", side_effect=lambda p: "k" if p == "openai" else None):
        config = await _resolve(_policy(models={"allowed": ["gone", "ok"]}))
    assert config.llm.name == "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize("on_plan", [True, False])
async def test_the_plan_still_gates_the_first_allowed_model(on_plan):
    from src.server.services import platform_client

    with (
        patch.object(platform_client, "platform_gating_active", return_value=True),
        patch(f"{CONFIG}.model_on_plan", new=AsyncMock(return_value=on_plan)),
    ):
        if on_plan:
            assert (await _resolve(_policy(models={"allowed": ["ok"]}))).llm.name == "ok"
        else:
            with pytest.raises(HTTPException) as refused:
                await _resolve(_policy(models={"allowed": ["ok"]}))
            assert "No model" in refused.value.detail["message"]


@pytest.mark.asyncio
async def test_a_named_model_the_policy_refuses_is_never_swapped():
    with (
        patch(f"{CONFIG}._serves", new=_serves_only({"ok"})),
        pytest.raises(HTTPException) as refused,
    ):
        await _resolve(_policy(models={"allowed": ["ok"]}), request_model="other")
    assert refused.value.detail["model"] == "other"


def _serves_only(names):
    async def serves(user_id, name, model_pref, **_kw):
        return name in names

    return serves


@pytest.mark.asyncio
async def test_own_api_keys_denied_resolve_as_if_there_were_none():
    byok = AsyncMock(return_value=MagicMock(name="own-key-client"))
    config = await _resolve(
        _policy(own_model_access={"api_keys": False}), is_byok=True, byok=byok,
    )
    byok.assert_not_awaited()
    assert config.credential_source not in (CredentialSource.BYOK, CredentialSource.OAUTH)


@pytest.mark.asyncio
async def test_a_custom_model_is_refused_with_own_api_keys_denied():
    pref = {"custom_models": [{"name": "mine", "model_id": "x", "provider": "openai"}]}
    with pytest.raises(HTTPException) as refused:
        await _resolve(_policy(own_model_access={"api_keys": False}), pref, "mine", is_byok=True)
    assert refused.value.detail["rule"] == "own_model_access.api_keys"


SHADOW = {"custom_models": [
    {"name": "ok", "model_id": "anything", "provider": "openai", "base_url": "https://elsewhere"},
]}


@pytest.mark.asyncio
async def test_a_custom_model_named_as_an_allowed_one_runs_the_built_in_when_named():
    """Under an allow-list a custom entry is not considered at all: the name
    is the manifest's model, never the endpoint the user pointed it at."""
    byok = AsyncMock(return_value=None)
    config = await _resolve(
        _policy(models={"allowed": ["default", "ok"]}), SHADOW, "ok", is_byok=True, byok=byok,
    )
    assert config.llm.name == "ok"
    assert "elsewhere" not in repr(byok.await_args_list)


@pytest.mark.asyncio
async def test_a_custom_model_named_as_an_allowed_one_runs_the_built_in_when_saved():
    pref = {**SHADOW, "preferred_model": "ok", "fallback_models": ["ok"]}
    config = await _resolve(_policy(models={"allowed": ["default", "ok"]}), pref, is_byok=True)
    assert config.llm.name == "ok"


@pytest.mark.asyncio
async def test_a_custom_only_name_is_refused_by_the_list_that_lacks_it():
    """Once refused naming the deployment, which set no list, and listing
    the refused name as allowed."""
    pref = {"custom_models": [{"name": "mine", "model_id": "x", "provider": "openai"}]}
    with pytest.raises(HTTPException) as refused:
        await _resolve(_policy(models={"allowed": ["ok"]}), pref, "mine", is_byok=True)
    detail = refused.value.detail
    assert (detail["rule"], detail["set_by"], detail["allowed"]) == (
        "models.allowed", "organization", ["ok"],
    )


@pytest.mark.asyncio
async def test_the_first_allowed_default_finds_the_built_in_a_custom_entry_shadows():
    """Once the custom shadow made the only allowed name unservable, and a
    turn naming no model was refused with nothing allowed."""
    with patch(f"{CONFIG}.deployment_key", side_effect=lambda p: "k" if p == "openai" else None):
        config = await _resolve(_policy(models={"allowed": ["ok"]}), SHADOW)
    assert config.llm.name == "ok"


def test_without_an_allow_list_custom_entries_are_read_as_saved():
    from src.server.services.llm.availability import builtins_only

    for policy in (ALLOW_ALL, _policy(own_model_access={"subscriptions": False})):
        assert builtins_only(policy, SHADOW) is SHADOW
    assert builtins_only(_policy(models={"allowed": ["ok"]}), SHADOW) == {}


def test_a_custom_entry_under_an_allow_list_is_refused_whatever_its_name():
    from src.server.services.llm.availability import policy_refuses

    provider = {"custom_providers": [{"name": "ok", "parent_provider": "openai"}]}
    allow = _policy(models={"allowed": ["ok"]})
    with patch("src.llms.llm.LLM.get_model_config", return_value=_mc()):
        assert policy_refuses(allow, SHADOW, "ok") == "models.allowed"
        assert policy_refuses(allow, provider, "ok") == "models.allowed"
        assert policy_refuses(allow, {}, "ok") is None
        # Without an allow-list a custom model is the own-keys rule's to judge.
        assert policy_refuses(_policy(own_model_access={"subscriptions": False}), SHADOW, "ok") is None


@pytest.mark.asyncio
async def test_own_sign_ins_denied_are_never_asked_and_claude_code_is_refused():
    oauth = AsyncMock(return_value=MagicMock(name="sign-in-client"))
    policy = _policy(own_model_access={"subscriptions": False})
    config = await _resolve(policy, oauth=oauth)
    oauth.assert_not_awaited()
    assert config.credential_source != CredentialSource.OAUTH

    with pytest.raises(HTTPException) as refused:
        await _resolve(policy, request_model="cc")
    assert refused.value.detail["rule"] == "own_model_access.subscriptions"
    # Saved, it reads as unset rather than refusing the turn.
    assert (await _resolve(policy, {"preferred_model": "cc"})).llm.name == "default"


@pytest.mark.asyncio
async def test_an_unknown_account_refuses_the_turn():
    from src.server.services.llm.config import resolve_llm_config

    with (
        patch(
            "src.server.services.account.read_standing",
            new_callable=AsyncMock,
            return_value=Standing(None, None),
        ),
        patch(f"{USER_MODELS}.get_model_preference", new_callable=AsyncMock, return_value={}),
        pytest.raises(HTTPException) as refused,
    ):
        await resolve_llm_config(_base(), "u", None, False)
    assert refused.value.status_code == 503


@pytest.mark.asyncio
async def test_no_policy_leaves_the_turn_as_it_was():
    config = await _resolve(ALLOW_ALL, {"fallback_models": ["fb-2"]})
    assert (config.llm.background, config.llm.compaction, config.llm.fetch) == (
        "background", "summary", "fetcher",
    )
    assert config.llm.fallback == ["fb-2"]
