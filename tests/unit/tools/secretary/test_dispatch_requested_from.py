"""A dispatch records where its turn came from for the turn that reports back.

The report-back turn starts later and from nowhere, so the dispatch keeps the
place in the record it reserves: the automation run whose sends the turn
makes, with the targets its agent was told, or the chat app the turn arrived
on.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services import automation_delivery
from src.server.services.automation_delivery import Delivery, Target
from src.server.services.report_back.handoff import keys, requested_from
from src.tools.secretary.chief_of_staff import delegate_to_analyst
from tests.unit.server.handlers.chat.redis_fakes import FakeCache

from .conftest import FakeResp, FakeSession

USER_ID = "user-1"
DISPATCHER_THREAD_ID = "cos-thread-1"
WORKSPACE_ID = "22222222-2222-2222-2222-222222222222"
DESK = Target(entry="slack:T/C", address="slack:T/C", name="#desk", ok=True)


@pytest.fixture
def cache(monkeypatch):
    c = FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: c)
    return c


async def _dispatch(configurable: dict) -> dict:
    call = {
        "name": "delegate_to_analyst",
        "args": {"question": "Value NVDA", "workspace_id": WORKSPACE_ID, "state": {}},
        "id": "call_1",
        "type": "tool_call",
    }
    with (
        patch("src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})),
        patch(
            "src.server.database.workspace.get_workspace",
            AsyncMock(return_value={"user_id": USER_ID, "name": "Semis"}),
        ),
        patch("aiohttp.ClientSession", return_value=FakeSession(FakeResp())),
    ):
        result = await delegate_to_analyst.ainvoke(
            call,
            config={
                "configurable": {
                    "user_id": USER_ID,
                    "thread_id": DISPATCHER_THREAD_ID,
                    **configurable,
                }
            },
        )
    return json.loads(result.update["messages"][0].content)


def _origin(cache, payload: dict) -> dict:
    assert payload["success"] is True
    return cache.kv[keys.ptc_origin_key(payload["thread_id"])]


@pytest.mark.asyncio
async def test_a_held_runs_hand_off_records_the_run(cache):
    run = Delivery("exec-1", [DESK])
    origin = _origin(cache, await _dispatch(automation_delivery.turn_configurable(run)))

    assert origin[requested_from.KEY] == [
        {"asked": "Value NVDA", "delivery": automation_delivery.stamp(run)}
    ]
    assert requested_from.deliveries(origin) == [run]


@pytest.mark.asyncio
async def test_a_chat_apps_hand_off_records_the_app(cache):
    origin = _origin(cache, await _dispatch({"platform": "telegram"}))

    assert origin[requested_from.KEY] == [
        {"asked": "Value NVDA", "surface": "telegram"}
    ]


@pytest.mark.asyncio
async def test_a_web_hand_off_records_only_the_request(cache):
    origin = _origin(cache, await _dispatch({"platform": "web"}))

    assert origin[requested_from.KEY] == [{"asked": "Value NVDA"}]
    assert requested_from.reminder(origin) is None
