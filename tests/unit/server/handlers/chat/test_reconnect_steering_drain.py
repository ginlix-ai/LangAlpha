"""Reconnect-tail steering drain is gated on thread idleness.

Review F4 (v4 2.4c): a stale client reconnecting to an OLD terminal run
replays that run's retained stream and then reaches the steering drain. If a
NEWER run is live on the thread, that drain would consume the live run's
stamped payloads out from under SteeringMiddleware. The drain therefore runs
only when the ledger shows no active run — and fails CLOSED (no drain) when
the ledger can't be read.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.handlers.chat import reconnect_admission


def _stream_stub(events: list[str]):
    async def _gen(thread_id, run_id, last_event_id):
        for e in events:
            yield e

    return _gen


async def _collect(thread_id: str = "t-1") -> list[str]:
    return [
        e
        async for e in reconnect_admission.reconnect_to_workflow_stream(
            thread_id, run_id="run-old", last_event_id=None
        )
    ]


@pytest.mark.asyncio
async def test_drain_skipped_while_another_run_is_active():
    drain = AsyncMock(return_value="event: steering_returned\ndata: {}\n\n")
    with (
        patch.object(reconnect_admission, "stream_from_log", _stream_stub(["e1"])),
        patch.object(reconnect_admission, "drain_steering_return_event", drain),
        patch(
            "src.server.database.runs.lifecycle.get_active_run",
            AsyncMock(return_value={"conversation_response_id": "run-live"}),
        ),
    ):
        events = await _collect()

    assert events == ["e1"]
    drain.assert_not_awaited()


@pytest.mark.asyncio
async def test_drain_runs_when_thread_is_idle():
    drain = AsyncMock(return_value="event: steering_returned\ndata: {}\n\n")
    with (
        patch.object(reconnect_admission, "stream_from_log", _stream_stub(["e1"])),
        patch.object(reconnect_admission, "drain_steering_return_event", drain),
        patch(
            "src.server.database.runs.lifecycle.get_active_run",
            AsyncMock(return_value=None),
        ),
    ):
        events = await _collect()

    assert events == ["e1", "event: steering_returned\ndata: {}\n\n"]
    drain.assert_awaited_once_with("t-1")


@pytest.mark.asyncio
async def test_drain_skipped_when_ledger_unreadable():
    """Unknown ledger state must not consume: a drain on a false 'idle' loses
    a live run's steering; skipping only delays the return to end-of-run."""
    drain = AsyncMock(return_value=None)
    with (
        patch.object(reconnect_admission, "stream_from_log", _stream_stub([])),
        patch.object(reconnect_admission, "drain_steering_return_event", drain),
        patch(
            "src.server.database.runs.lifecycle.get_active_run",
            AsyncMock(side_effect=RuntimeError("db down")),
        ),
    ):
        events = await _collect()

    assert events == []
    drain.assert_not_awaited()
