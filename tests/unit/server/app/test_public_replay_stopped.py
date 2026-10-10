"""A shared transcript keeps the Stopped chip of a turn its owner stopped.

The public route shares the owner's replay assembly, so the stop close derived
from the response row has to reach the shared stream too.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app
from tests.unit.server.services.history.replay_builders import replay_rows

pytestmark = pytest.mark.asyncio

_SHARE_TOKEN = "share_abc123"
_THREAD_ID = "44444444-4444-4444-8444-444444444444"
_RESPONSE_ID = "55555555-5555-4555-8555-555555555555"

_THREAD_BY_TOKEN = "src.server.app.share_access.get_thread_by_share_token"
_REPLAY_DATA = "src.server.app.public.get_replay_thread_data"
_TASK_DETAILS = "src.server.services.history.task_status.resolve_task_details"

_TEXT = {
    "event": "message_chunk",
    "data": {
        "agent": "model:t1",
        "id": "lc-1",
        "role": "assistant",
        "content_type": "text",
        "content": "I'll grab current yields.",
    },
}
_SEARCH = {
    "event": "tool_calls",
    "data": {
        "agent": "model:t1",
        "id": "lc-1",
        "role": "assistant",
        "tool_calls": [{"name": "web_search", "args": {}, "id": "tc-1"}],
    },
}
_CLOSE = {
    "event": "message_chunk",
    "data": {
        "agent": "model:t1",
        "id": "lc-1",
        "role": "assistant",
        "finish_reason": "stopped",
    },
}


@pytest_asyncio.fixture
async def client():
    from src.server.app.public import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def _closes(client, sse_events: list[dict], metadata: dict) -> list[dict]:
    thread = {
        "conversation_thread_id": _THREAD_ID,
        "workspace_id": "ws-1",
        "share_permissions": {},
    }
    queries = [{"turn_index": 0, "content": "long-end yields?", "metadata": {}}]
    row = {
        "turn_index": 0,
        "conversation_response_id": _RESPONSE_ID,
        "status": "cancelled",
        "metadata": metadata,
        "sse_events": sse_events,
    }
    with (
        patch(_THREAD_BY_TOKEN, new=AsyncMock(return_value=thread)),
        patch(
            _REPLAY_DATA,
            new=AsyncMock(
                return_value=replay_rows(
                    {
                        "conversation_thread_id": _THREAD_ID,
                        "latest_checkpoint_id": None,
                    },
                    queries,
                    [row],
                )
            ),
        ),
        patch(_TASK_DETAILS, new=AsyncMock(return_value={})),
    ):
        resp = await client.get(f"/api/v1/public/shared/{_SHARE_TOKEN}/replay")
        assert resp.status_code == 200
        body = resp.text

    closes = []
    for block in body.split("\n\n"):
        lines = dict(line.split(": ", 1) for line in block.splitlines() if ": " in line)
        if lines.get("event") != "message_chunk":
            continue
        data = json.loads(lines["data"])
        if data.get("finish_reason") == "stopped":
            closes.append(data)
    return closes


async def test_stop_between_messages_is_closed_on_the_shared_stream(client):
    closes = await _closes(client, [_TEXT, _SEARCH], {"cancelled_by_user": True})

    assert closes == [
        {
            "thread_id": _THREAD_ID,
            "id": "lc-1",
            "role": "assistant",
            "finish_reason": "stopped",
            "agent": "model:t1",
            "turn_index": 0,
            "response_id": _RESPONSE_ID,
        }
    ]


async def test_an_archived_close_is_not_doubled(client):
    closes = await _closes(
        client, [_TEXT, _SEARCH, _CLOSE], {"cancelled_by_user": True}
    )

    assert len(closes) == 1


async def test_a_stop_before_any_assistant_event_is_closed(client):
    closes = await _closes(client, [], {"cancelled_by_user": True})

    assert closes == [
        {
            "thread_id": _THREAD_ID,
            "role": "assistant",
            "finish_reason": "stopped",
            "turn_index": 0,
            "response_id": _RESPONSE_ID,
        }
    ]


async def test_a_system_cancel_gets_no_close(client):
    closes = await _closes(client, [_TEXT, _SEARCH], {"cancelled_by_user": False})

    assert closes == []
