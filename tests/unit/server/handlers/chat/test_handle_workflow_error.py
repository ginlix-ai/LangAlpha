"""Tests for ``handle_workflow_error`` tracker wiring.

Pins the contract that both terminal branches (max-retries-exceeded and
non-recoverable) push ``WorkflowStatus.FAILED`` to Redis via
``WorkflowTracker.mark_failed``. Without these tests a future refactor that
drops the tracker call would silently restore the original "ACTIVE for the
full TTL window" bug after a setup-error workflow dies.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat import _common


def _consume(agen):
    async def _drain():
        events = []
        async for event in agen:
            events.append(event)
        return events
    return _drain()


def _make_request():
    return SimpleNamespace(
        workspace_id="ws-1",
        locale=None,
        timezone=None,
    )


@pytest.fixture
def patch_tracker():
    """Patch WorkflowTracker.get_instance to return a recordable mock."""
    mock_tracker = MagicMock()
    mock_tracker.increment_retry_count = AsyncMock()
    mock_tracker.mark_failed = AsyncMock(return_value=True)
    with patch.object(
        _common.WorkflowTracker, "get_instance", return_value=mock_tracker
    ):
        yield mock_tracker


@pytest.mark.asyncio
async def test_max_retries_branch_marks_failed(patch_tracker):
    # Recoverable error past MAX_RETRIES → marks tracker FAILED with the
    # retry-limit error message.
    patch_tracker.increment_retry_count.return_value = 99  # > MAX_RETRIES

    err = ConnectionError("connection refused")
    handler = MagicMock()
    handler.get_tool_usage.return_value = None
    handler.get_sse_events.return_value = None

    with patch.object(_common, "release_burst_slot", new=AsyncMock()), \
         patch.object(_common, "get_max_workflow_retries", return_value=3):
        await _consume(_common.handle_workflow_error(
            e=err,
            thread_id="t-max-retry",
            user_id="u-1",
            workspace_id="ws-1",
            handler=handler,
            token_callback=None,
            persistence_service=None,
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    patch_tracker.mark_failed.assert_awaited_once()
    call = patch_tracker.mark_failed.await_args
    assert call.args[0] == "t-max-retry"
    assert "Max retries exceeded" in call.kwargs["error"]
    assert "ConnectionError" in call.kwargs["error"]


@pytest.mark.asyncio
async def test_non_recoverable_branch_marks_failed(patch_tracker):
    # Non-recoverable error (AttributeError) → marks tracker FAILED with the
    # raw "ErrorType: message" string.
    err = AttributeError("'NoneType' has no attribute 'foo'")
    handler = MagicMock()
    handler.get_tool_usage.return_value = None
    handler.get_sse_events.return_value = None
    handler._format_sse_event.return_value = "event: error\ndata: {}\n\n"

    with patch.object(_common, "release_burst_slot", new=AsyncMock()), \
         patch.object(_common, "get_max_workflow_retries", return_value=3):
        await _consume(_common.handle_workflow_error(
            e=err,
            thread_id="t-non-recov",
            user_id="u-1",
            workspace_id="ws-1",
            handler=handler,
            token_callback=None,
            persistence_service=None,
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    patch_tracker.mark_failed.assert_awaited_once()
    call = patch_tracker.mark_failed.await_args
    assert call.args[0] == "t-non-recov"
    assert call.kwargs["error"].startswith("AttributeError:")


@pytest.mark.asyncio
async def test_tracker_failure_does_not_break_error_flow(patch_tracker):
    # If tracker.mark_failed itself raises, the handler must still emit the
    # SSE error event — Redis write failures are non-fatal.
    patch_tracker.mark_failed.side_effect = RuntimeError("redis down")

    err = AttributeError("boom")
    handler = MagicMock()
    handler.get_tool_usage.return_value = None
    handler.get_sse_events.return_value = None
    sse = "event: error\ndata: {\"thread_id\": \"t-fail\"}\n\n"
    handler._format_sse_event.return_value = sse

    with patch.object(_common, "release_burst_slot", new=AsyncMock()), \
         patch.object(_common, "get_max_workflow_retries", return_value=3):
        events = await _consume(_common.handle_workflow_error(
            e=err,
            thread_id="t-fail",
            user_id="u-1",
            workspace_id="ws-1",
            handler=handler,
            token_callback=None,
            persistence_service=None,
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    assert sse in events
