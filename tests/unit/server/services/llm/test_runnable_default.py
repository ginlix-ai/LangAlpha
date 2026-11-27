"""A standalone turn that names no model and has none saved runs the
deployment's default only where something runs it; otherwise it asks for a
model (``model_required``) instead of failing on a missing key."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.services.llm.clients import DeploymentRoute, SubscriptionRoute
from src.server.services.llm.config import require_runnable_default

CONFIG = "src.server.services.llm.config"


def _config():
    return SimpleNamespace(llm=SimpleNamespace(name="model-deployment", background=None))


async def _check(model_pref=None, *, saved_pref=None, request_model=None, slot="primary", route=None, serves=False):
    with (
        patch(
            "src.server.services.llm.clients.credential_route",
            new_callable=AsyncMock,
            return_value=route or DeploymentRoute(model_info={"provider": "deepseek"}),
        ),
        patch(f"{CONFIG}._serves", new_callable=AsyncMock, return_value=serves),
    ):
        await require_runnable_default(
            _config(), "user-1", model_pref or {}, slot, request_model,
            saved_pref=saved_pref if saved_pref is not None else model_pref or {},
            policy=SimpleNamespace(), permits=SimpleNamespace(), is_byok=True,
        )


@pytest.fixture
def standalone(monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")


@pytest.mark.asyncio
async def test_a_default_nothing_runs_asks_for_a_model(standalone):
    with pytest.raises(HTTPException) as refused:
        await _check()
    assert refused.value.detail["type"] == "model_required"
    assert refused.value.detail["link"]["url"] == "/settings?tab=model"


@pytest.mark.asyncio
async def test_a_default_something_runs_goes_ahead(standalone):
    await _check(serves=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        {"request_model": "model-picked"},
        {"model_pref": {"preferred_model": "model-saved"}},
        # Saved, though a rule, the plan or a Claude Code default held for
        # Home set it aside for this turn: the user has a default.
        {"model_pref": {}, "saved_pref": {"preferred_model": "model-set-aside"}},
        {"slot": "background"},
        {"route": SubscriptionRoute(subscription=None, model_info={})},
    ],
    ids=["named", "saved", "set-aside", "background", "sign-in"],
)
async def test_left_to_the_rest_of_resolution(standalone, case):
    """A model the turn or the user chose, a Background job, and a default on a
    sign-in, whose own refusal says what to reconnect."""
    await _check(**case)


@pytest.mark.asyncio
async def test_a_platform_default_is_the_plans_to_serve(monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    await _check()
