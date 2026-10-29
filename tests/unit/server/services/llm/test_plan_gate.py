"""The plan gate on a platform deployment: a model above the user's tier is refused when the turn names it, and read as unset where it is only saved."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.services.llm.capabilities import PlanTier
from src.server.services.llm.config import gate_models_by_plan
from src.server.services.llm.thread_model import _runs_default_instead
from src.server.services.llm.user_models import ModelSource

USER = "user-1"
PLAN_TIER = 1
TIERS = {"m-free": 0, "m-paid": 1, "m-pro": 2, "m-pro-2": 2, "m-deploy": 3, "m-deploy-bg": 3}


@pytest.fixture
def platform(monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    monkeypatch.setattr("src.server.dependencies.usage_limits.platform_gating_active", lambda: True)
    fetch = AsyncMock(return_value=PLAN_TIER)
    monkeypatch.setattr("src.server.dependencies.usage_limits._fetch_platform_tier", fetch)

    custom: set[str] = set()

    async def classify(user_id, model, **_):
        if model in custom:
            return ModelSource.CUSTOM, {"provider": "p-own"}
        return ModelSource.SYSTEM, {"provider": f"p-{model}", "tier": TIERS[model]}

    mc = MagicMock()
    mc.get_provider_info.return_value = {}
    mc.get_model_config.side_effect = lambda name: {"tier": TIERS[name]} if name in TIERS else None
    walk = AsyncMock(return_value=(None, None))
    custom_key = AsyncMock(return_value=(None, None, {}))
    with (
        patch("src.server.services.llm.user_models.classify_model", classify),
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
        patch("src.server.services.llm.clients.walk_byok_candidates", walk),
        patch("src.server.services.llm.clients._resolve_custom_model_byok", custom_key),
    ):
        yield SimpleNamespace(fetch=fetch, walk=walk, custom=custom, custom_key=custom_key)


@pytest.fixture
def base_config():
    return SimpleNamespace(llm=SimpleNamespace(name="m-deploy", background="m-deploy-bg"))


async def _gate(base_config, pref, *, request_model=None, is_byok=False, slot="primary"):
    return await gate_models_by_plan(
        base_config, USER, pref, request_model,
        slot=slot, permits=PlanTier(USER), is_byok=is_byok,
    )


async def _refusal(base_config, pref, **kwargs) -> HTTPException:
    with pytest.raises(HTTPException) as exc:
        await _gate(base_config, pref, **kwargs)
    return exc.value


@pytest.mark.asyncio
async def test_a_named_model_above_the_plan_is_refused_with_a_link_to_settings(platform, base_config):
    err = await _refusal(base_config, {}, request_model="m-pro")

    assert err.status_code == 400
    assert err.detail["type"] == "model_not_on_plan"
    assert "m-pro" in err.detail["message"]
    assert err.detail["link"]["url"] == "/settings?tab=model"


@pytest.mark.asyncio
async def test_a_saved_default_above_the_plan_reads_as_unset_so_the_deployments_runs(platform, base_config):
    pref = {"preferred_model": "m-pro", "preferred_background_model": "m-pro-2"}

    view = await _gate(base_config, pref)

    assert view == {}
    assert pref == {"preferred_model": "m-pro", "preferred_background_model": "m-pro-2"}


@pytest.mark.asyncio
async def test_a_refusal_asks_the_platform_again_so_a_plan_changed_minutes_ago_counts(platform, base_config):
    platform.fetch.side_effect = [PLAN_TIER, 2]

    assert await _gate(base_config, {}, request_model="m-pro") == {}
    assert platform.fetch.await_args_list[-1].kwargs == {"recheck": True}


@pytest.mark.asyncio
async def test_saved_role_models_and_fallbacks_above_the_plan_are_absent_from_the_view(platform, base_config):
    pref = {
        "preferred_model": "m-paid",
        "preferred_background_model": "m-pro",
        "fetch_model": "m-pro",
        "compaction_model": "m-pro-2",
        "summarization_model": "m-free",
        "fallback_models": ["m-pro", "m-paid", "m-pro-2", "m-free"],
    }
    before = {**pref, "fallback_models": list(pref["fallback_models"])}

    view = await _gate(base_config, pref)

    assert view == {
        "preferred_model": "m-paid",
        "summarization_model": "m-free",
        "fallback_models": ["m-paid", "m-free"],
    }
    assert pref == before


@pytest.mark.asyncio
async def test_a_saved_value_that_is_not_a_name_is_left_alone_beside_one_above_the_plan(platform, base_config):
    pref = {
        "preferred_model": {"name": "m-pro"},
        "fetch_model": "m-pro",
        "fallback_models": [["m-pro"], "m-pro", "m-free"],
    }

    view = await _gate(base_config, pref, request_model="m-free")

    assert view == {"preferred_model": {"name": "m-pro"}, "fallback_models": [["m-pro"], "m-free"]}


@pytest.mark.asyncio
async def test_the_deployments_main_model_is_never_refused_above_the_plan(platform, base_config):
    assert await _gate(base_config, {}, request_model="m-deploy") == {}
    assert await _gate(base_config, {}, request_model="m-deploy", slot="background") == {}
    pref = {"preferred_model": "m-deploy", "fetch_model": "m-deploy", "fallback_models": ["m-deploy"]}
    assert await _gate(base_config, pref) == pref


@pytest.mark.asyncio
async def test_the_deployments_background_model_passes_only_where_it_runs_unasked(platform, base_config):
    assert await _gate(base_config, {}, request_model="m-deploy-bg", slot="background") == {}
    err = await _refusal(base_config, {}, request_model="m-deploy-bg")
    assert err.detail["type"] == "model_not_on_plan"

    pref = {
        "preferred_model": "m-deploy-bg",
        "preferred_background_model": "m-deploy-bg",
        "fetch_model": "m-deploy-bg",
        "fallback_models": ["m-deploy-bg", "m-free"],
    }
    assert await _gate(base_config, pref) == {
        "preferred_background_model": "m-deploy-bg",
        "fetch_model": "m-deploy-bg",
        "fallback_models": ["m-free"],
    }


@pytest.mark.asyncio
async def test_a_model_the_user_holds_a_key_for_is_not_refused_above_the_plan(platform, base_config):
    platform.walk.return_value = ({"key": "k"}, None)

    view = await _gate(base_config, {"preferred_model": "m-pro"}, is_byok=True)

    assert view == {"preferred_model": "m-pro"}


@pytest.mark.asyncio
async def test_a_model_above_the_plan_is_refused_when_byok_has_no_key_for_it(platform, base_config):
    err = await _refusal(base_config, {}, request_model="m-pro", is_byok=True)

    assert err.detail["type"] == "model_not_on_plan"


@pytest.mark.asyncio
async def test_a_keyless_custom_model_under_a_built_in_name_is_judged_as_the_built_in(platform, base_config):
    platform.custom.add("m-pro")

    view = await _gate(base_config, {"fetch_model": "m-pro"}, is_byok=True)

    assert view == {}


@pytest.mark.asyncio
async def test_a_custom_model_under_a_built_in_name_runs_on_its_own_key(platform, base_config):
    platform.custom.add("m-pro")
    platform.custom_key.return_value = ({"api_key": "k"}, None, {})

    view = await _gate(base_config, {"fetch_model": "m-pro"}, is_byok=True)

    assert view == {"fetch_model": "m-pro"}


@pytest.mark.asyncio
async def test_an_unknown_tier_refuses_a_model_that_needs_one_as_unavailable(platform, base_config):
    platform.fetch.return_value = None

    err = await _refusal(base_config, {}, request_model="m-paid")

    assert err.status_code == 503


@pytest.mark.asyncio
async def test_nothing_is_refused_or_dropped_when_gating_is_inactive(platform, base_config, monkeypatch):
    monkeypatch.setattr("src.server.dependencies.usage_limits.platform_gating_active", lambda: False)
    pref = {"preferred_model": "m-pro", "fallback_models": ["m-pro-2"]}

    view = await _gate(base_config, pref, request_model="m-pro")

    assert view == pref
    platform.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_plan_tier_is_fetched_once_across_checks(platform):
    permits = PlanTier(USER)

    results = [await permits(0), await permits(PLAN_TIER), await permits(PLAN_TIER + 1), await permits.tier()]

    assert results == [True, True, False, PLAN_TIER]
    platform.fetch.assert_awaited_once_with(USER)


def _refused(kind: str, status: int = 400) -> HTTPException:
    return HTTPException(status_code=status, detail={"type": kind, "message": "x"})


def test_a_service_turn_on_the_threads_model_runs_the_default_when_the_model_is_off_plan():
    assert _runs_default_instead(_refused("model_not_on_plan"), named=None, model="m-pro") is True


def test_a_turn_that_named_its_model_is_not_moved_to_the_default_when_the_model_is_off_plan():
    assert _runs_default_instead(_refused("model_not_on_plan"), named="m-pro", model="m-pro") is False
