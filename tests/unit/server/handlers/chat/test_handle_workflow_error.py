"""Tests for ``handle_workflow_error`` terminal wiring (v4).

Pins the contract that both terminal branches (max-retries-exceeded and
non-recoverable) terminal-write the open run through
``TurnCoordinator.finalize_turn`` — and that a finalize failure or a
deterministic protocol conflict never suppresses the client-facing SSE
error. Without these tests a future refactor that drops the finalize call
would silently restore the original "in_progress forever" zombie after a
setup-error workflow dies.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat import error_handling


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


def _run_handle(attempt_no: int = 1):
    return SimpleNamespace(
        run_id="r-1",
        attempt_no=attempt_no,
        finalized=False,
        workspace_id=None,
        user_id=None,
    )


def _handler():
    handler = MagicMock()
    handler.get_tool_usage.return_value = None
    handler.get_sse_events.return_value = None
    handler._format_sse_event.side_effect = (
        lambda ev, data: f"event: {ev}\ndata: {data}\n\n"
    )
    return handler


@pytest.fixture
def coordinator():
    """Patch TurnCoordinator.get_instance to return a recordable mock."""
    coord = AsyncMock()
    coord.finalize_turn.return_value = SimpleNamespace(run={"status": "error"})
    with patch("src.server.services.turn_lifecycle.TurnCoordinator") as coord_cls:
        coord_cls.get_instance.return_value = coord
        yield coord


@pytest.mark.asyncio
async def test_max_retries_branch_finalizes_error(coordinator):
    # Recoverable error past MAX_RETRIES → terminal-writes the run as error
    # with the retry-limit message. v4: the retry count is the run's
    # attempt_no, so the branch is driven via run_handle.attempt_no > MAX_RETRIES.
    run_handle = _run_handle(attempt_no=99)
    err = ConnectionError("connection refused")

    with patch.object(error_handling, "release_burst_slot", new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-max-retry",
            user_id="u-1",
            workspace_id="ws-1",
            handler=_handler(),
            token_callback=None,
            run_handle=run_handle,
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    coordinator.finalize_turn.assert_awaited_once()
    handle, outcome = coordinator.finalize_turn.await_args.args
    assert handle is run_handle
    assert outcome.status == "error"
    assert "Max retries exceeded" in outcome.errors[0]
    assert "ConnectionError" in outcome.errors[0]


@pytest.mark.asyncio
async def test_non_recoverable_branch_finalizes_error(coordinator):
    # Non-recoverable error (AttributeError) → terminal-writes the run with
    # the error's message.
    err = AttributeError("'NoneType' has no attribute 'foo'")

    with patch.object(error_handling, "release_burst_slot", new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-non-recov",
            user_id="u-1",
            workspace_id="ws-1",
            handler=_handler(),
            token_callback=None,
            run_handle=_run_handle(),
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    coordinator.finalize_turn.assert_awaited_once()
    _, outcome = coordinator.finalize_turn.await_args.args
    assert outcome.status == "error"
    assert outcome.errors == ["'NoneType' has no attribute 'foo'"]


@pytest.mark.asyncio
async def test_finalize_failure_does_not_break_error_flow(coordinator):
    # If the terminal write itself raises, the handler logs CRITICAL (the row
    # stays in_progress for recovery) but must still emit the SSE error event.
    coordinator.finalize_turn.side_effect = RuntimeError("db down")

    err = AttributeError("boom")
    handler = _handler()

    with patch.object(error_handling, "release_burst_slot", new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        events = await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-fail",
            user_id="u-1",
            workspace_id="ws-1",
            handler=handler,
            token_callback=None,
            run_handle=_run_handle(),
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    assert any(ev.startswith("event: error\n") for ev in events)


@pytest.mark.asyncio
async def test_external_id_conflict_branch_emits_conflict_and_skips_finalize(
    coordinator,
):
    # A cross-user (platform, external_id) create race surfaces as a clean SSE
    # error carrying error_type=external_id_conflict, and (like the admission-
    # conflict path) must NOT finalize anything as a turn failure.
    import json as _json

    from src.server.database.conversation import ExternalIdConflictError

    err = ExternalIdConflictError(platform="telegram", external_id="chat:42")

    with patch.object(error_handling, "release_burst_slot", new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        # handler=None takes the json.dumps SSE branch, easy to parse.
        events = await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-ext",
            user_id="u-1",
            workspace_id="ws-1",
            handler=None,
            token_callback=None,
            run_handle=None,
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    assert len(events) == 1
    assert events[0].startswith("event: error\n")
    payload = _json.loads(events[0].split("data: ", 1)[1].strip())
    assert payload["error_type"] == "external_id_conflict"
    assert payload["platform"] == "telegram"
    assert payload["external_id"] == "chat:42"
    # Deterministic protocol conflict — not a workflow failure.
    coordinator.finalize_turn.assert_not_awaited()
    coordinator.fail_open_run.assert_not_awaited()
