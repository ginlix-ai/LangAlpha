"""Tests for TodoWriteMiddleware's SSE event emission.

Adapted from the superseded PR #156. Covers:
  - Normalized-payload emission (whitelist to {content, activeForm, status}
    + lowercase status for frontend's strict-equality checks).
  - Top-level guard against non-list `todos` payloads so the failed-event
    branch's `len(todos)` call never crashes on None/int garbage.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ptc_agent.agent.middleware.todo_operations.sse_middleware import (
    TodoWriteMiddleware,
)


def _todo(content="Fetch Q3 earnings", active="Fetching Q3 earnings", status="pending"):
    return {"content": content, "activeForm": active, "status": status}


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
        _todo(status="pending"),
        _todo(status="in_progress"),
        _todo(status="completed"),
        _todo(status="completed"),
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
    assert payload["total"] == 4
    assert payload["completed"] == 2
    assert payload["in_progress"] == 1
    assert payload["pending"] == 1
    # Payload is whitelisted to the three schema fields
    assert all(set(t.keys()) == {"content", "activeForm", "status"} for t in payload["todos"])


@pytest.mark.asyncio
async def test_status_case_normalized_in_payload(middleware):
    """LLM-sent uppercase status must reach the frontend as lowercase — the
    drawer uses strict equality checks and would otherwise render as unknown."""
    todos = [_todo(status="PENDING"), _todo(status="In_Progress")]
    emitted = []

    with patch(
        "ptc_agent.agent.middleware.todo_operations.sse_middleware.get_stream_writer",
        return_value=emitted.append,
    ):
        await _run(middleware, _make_request(todos))

    payload = emitted[0]["payload"]
    assert [t["status"] for t in payload["todos"]] == ["pending", "in_progress"]
    assert payload["pending"] == 1
    assert payload["in_progress"] == 1


@pytest.mark.asyncio
async def test_legacy_fields_stripped_from_payload(middleware):
    """id/created_at/updated_at were removed from TodoItem but LLMs on cached
    schemas may still send them. They must not reach SSE or persistence."""
    todos = [{**_todo(status="pending"), "id": "legacy-1", "created_at": "2026-01-01"}]
    emitted = []

    with patch(
        "ptc_agent.agent.middleware.todo_operations.sse_middleware.get_stream_writer",
        return_value=emitted.append,
    ):
        await _run(middleware, _make_request(todos))

    payload = emitted[0]["payload"]
    assert payload["todos"] == [_todo(status="pending")]


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


@pytest.mark.asyncio
async def test_handler_exception_emits_failed_event(middleware):
    """Pydantic ValidationError (or any tool failure) must emit a failed event
    without crashing. Previously the error-path's len(todos) could crash when
    raw_todos was None/int — the top-level guard prevents that."""
    emitted = []

    async def failing_handler(_req):
        raise ValueError("simulated validation failure")

    request = _make_request(None)  # None would have crashed len() pre-guard

    with patch(
        "ptc_agent.agent.middleware.todo_operations.sse_middleware.get_stream_writer",
        return_value=emitted.append,
    ):
        with pytest.raises(ValueError):
            await middleware.awrap_tool_call(request, failing_handler)

    assert len(emitted) == 1
    assert emitted[0]["status"] == "failed"
    assert emitted[0]["payload"]["total"] == 0
    assert "simulated validation failure" in emitted[0]["payload"]["error"]
