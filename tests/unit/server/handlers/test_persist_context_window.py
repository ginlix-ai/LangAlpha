"""_persist_context_window_event appends via the atomic JSONB helper.

It must call append_sse_event (one server-side concat) rather than the old
read-modify-write that loaded every response for the thread and rewrote the
whole blob, and it must stay best-effort (never raise).
"""

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_appends_via_append_sse_event_not_full_rewrite():
    from src.server.handlers.workflow_handler import _persist_context_window_event

    append_mock = AsyncMock(return_value=True)
    # The old read-modify-write path must be gone: these would fail loudly.
    get_all = AsyncMock(side_effect=AssertionError("must not read all responses"))
    update_full = AsyncMock(side_effect=AssertionError("must not full-rewrite blob"))

    with (
        patch("src.server.database.conversation.append_sse_event", new=append_mock),
        patch(
            "src.server.database.conversation.get_responses_for_thread", new=get_all
        ),
        patch("src.server.database.conversation.update_sse_events", new=update_full),
    ):
        await _persist_context_window_event("thread-1", {"action": "summarize"})

    append_mock.assert_awaited_once()
    thread_id, event = append_mock.await_args.args
    assert thread_id == "thread-1"
    assert event["event"] == "context_window"
    assert event["data"]["action"] == "summarize"
    assert event["data"]["thread_id"] == "thread-1"


@pytest.mark.asyncio
async def test_best_effort_swallows_errors():
    from src.server.handlers.workflow_handler import _persist_context_window_event

    boom = AsyncMock(side_effect=RuntimeError("db down"))
    with patch("src.server.database.conversation.append_sse_event", new=boom):
        # Must not raise — context_window persistence is best-effort.
        await _persist_context_window_event("thread-1", {"action": "summarize"})
