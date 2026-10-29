"""The approval card's report-back switch reaches the hand-off it approves.

The web answers a hand-off's card with ``decisions[0].overrides.report_back``.
The request model dropped the key, so the switch did nothing; these tests run
the answer the way the server does, from the request model through the resume
payload into the dispatch.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.chat import ChatRequest, serialize_hitl_response_map
from src.server.services.report_back.handoff import keys
from src.tools.secretary.chief_of_staff import delegate_to_analyst
from tests.unit.server.handlers.chat.redis_fakes import FakeCache

from .conftest import FakeResp, FakeSession

USER_ID = "user-1"
DISPATCHER_THREAD_ID = "cos-thread-1"
WORKSPACE_ID = "22222222-2222-2222-2222-222222222222"


@pytest.fixture(autouse=True)
def _internal_service_token(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")


@pytest.fixture
def cache(monkeypatch) -> FakeCache:
    c = FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: c)
    return c


def _resume_payload(decision: dict) -> dict:
    """The resume value the graph hands the paused tool, built as the chat
    route builds it from the request body the web sends."""
    request = ChatRequest.model_validate(
        {"hitl_response": {"int-1": {"decisions": [decision]}}}
    )
    return serialize_hitl_response_map(request.hitl_response)["int-1"]


async def _approve_with(decision: dict) -> dict:
    with patch(
        "src.tools.secretary._commands.interrupt",
        return_value=_resume_payload(decision),
    ), patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "name": "Semis"}),
    ), patch("aiohttp.ClientSession", return_value=FakeSession(FakeResp())):
        result = await delegate_to_analyst.ainvoke(
            {
                "name": "delegate_to_analyst",
                "args": {"question": "q", "workspace_id": WORKSPACE_ID, "state": {}},
                "id": "call-1",
                "type": "tool_call",
            },
            config={
                "configurable": {"user_id": USER_ID, "thread_id": DISPATCHER_THREAD_ID}
            },
        )
    return json.loads(result.update["messages"][0].content)


def _watched(cache: FakeCache) -> set:
    return cache.client.sets.get(keys.watch_key(DISPATCHER_THREAD_ID), set())


@pytest.mark.asyncio
async def test_turning_report_back_off_on_the_card_dispatches_without_one(cache):
    payload = await _approve_with(
        {"type": "approve", "overrides": {"report_back": False}}
    )

    assert payload["status"] == "dispatched"
    assert payload["report_back"] is False
    # No report-back slot was reserved for the dispatching thread.
    assert _watched(cache) == set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision",
    [{"type": "approve"}, {"type": "approve", "overrides": {}}],
    ids=["no-overrides", "switch-untouched"],
)
async def test_an_approval_that_leaves_the_switch_keeps_the_report_back(
    cache, decision
):
    payload = await _approve_with(decision)

    assert payload["report_back"] is True
    assert payload["thread_id"] in _watched(cache)
