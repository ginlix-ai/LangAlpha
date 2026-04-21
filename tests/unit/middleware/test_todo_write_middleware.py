"""Tests for TodoWriteMiddleware's SSE event emission.

Focuses on resilience to malformed `todos` payloads — some LLMs emit
`todos` as a stringified JSON blob (sometimes malformed) rather than a list.
The middleware must normalize non-list values to `[]` before iterating.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ptc_agent.agent.middleware.todo_operations.sse_middleware import (
    TodoWriteMiddleware,
)


def _make_request(todos, tool_name="TodoWrite", tool_call_id="call-1"):
    return SimpleNamespace(
        tool_call={
            "name": tool_name,
            "id": tool_call_id,
            "args": {"todos": todos},
        }
    )


async def _run(middleware, request):
    async def handler(_req):
        return "tool-result"

    return await middleware.awrap_tool_call(request, handler)


@pytest.fixture
def middleware():
    return TodoWriteMiddleware()


@pytest.mark.asyncio
async def test_list_todos_pass_through_with_counts(middleware):
    todos = [
        {"status": "pending"},
        {"status": "in_progress"},
        {"status": "completed"},
        {"status": "completed"},
    ]
    emitted = []

    with patch(
        "ptc_agent.agent.middleware.todo_operations.sse_middleware.get_stream_writer",
        return_value=emitted.append,
    ):
        result = await _run(middleware, _make_request(todos))

    assert result == "tool-result"
    assert len(emitted) == 1
    payload = emitted[0]["payload"]
    assert payload["todos"] == todos
    assert payload["total"] == 4
    assert payload["completed"] == 2
    assert payload["in_progress"] == 1
    assert payload["pending"] == 1


@pytest.mark.parametrize(
    "bad_todos",
    [
        '[{"status":"pending"}]',  # stringified JSON
        "not json at all",
        {"status": "pending"},  # dict instead of list
        None,
        42,
    ],
)
@pytest.mark.asyncio
async def test_non_list_todos_normalized_to_empty(middleware, bad_todos):
    emitted = []

    with patch(
        "ptc_agent.agent.middleware.todo_operations.sse_middleware.get_stream_writer",
        return_value=emitted.append,
    ):
        result = await _run(middleware, _make_request(bad_todos))

    assert result == "tool-result"
    assert len(emitted) == 1
    payload = emitted[0]["payload"]
    assert payload["todos"] == []
    assert payload["total"] == 0
    assert payload["completed"] == 0
    assert payload["in_progress"] == 0
    assert payload["pending"] == 0


@pytest.mark.asyncio
async def test_non_todowrite_tool_passes_through(middleware):
    emitted = []

    with patch(
        "ptc_agent.agent.middleware.todo_operations.sse_middleware.get_stream_writer",
        return_value=emitted.append,
    ):
        result = await _run(
            middleware, _make_request([], tool_name="SomethingElse")
        )

    assert result == "tool-result"
    assert emitted == []


@pytest.mark.asyncio
async def test_non_list_todos_logs_warning(middleware, caplog):
    emitted = []
    caplog.set_level("WARNING")

    with patch(
        "ptc_agent.agent.middleware.todo_operations.sse_middleware.get_stream_writer",
        return_value=emitted.append,
    ):
        await _run(middleware, _make_request('"not-a-list"'))

    assert any(
        "Non-list todos payload" in record.message for record in caplog.records
    )
