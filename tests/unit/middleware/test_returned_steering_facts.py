"""Steering handed back to the user streams from the run's replay facts.

The ledger settles a task run's unread steering in the transaction that
stamps its cancel or ends it, and keeps what it gave back on the run's row,
where replay reads it. The run's end streams the same entries live.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTask
from ptc_agent.agent.middleware.background_subagent.run_executor import (
    _emit_returned_steering,
)


def _task() -> BackgroundTask:
    return BackgroundTask(
        tool_call_id="tc-9",
        task_id="abc123",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        task_run_id="run-9",
    )


_RETURNED = [{"content": "late note", "input_id": "i1", "reason": "run_ended"}]


@pytest.mark.asyncio
async def test_run_end_streams_what_the_ledger_returned():
    registry = SimpleNamespace(append_event_for_task=AsyncMock())

    await _emit_returned_steering(
        registry, _task(), {"replay_facts": {"steering_returned": _RETURNED}}
    )

    # The event names the return exactly as the fact does, plus its lane.
    args, kwargs = registry.append_event_for_task.await_args
    assert args[1]["event"] == "steering_returned"
    assert args[1]["data"] == {"agent": "task:abc123", **_RETURNED[0]}
    assert kwargs == {"terminal": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row", [{}, {"replay_facts": None}, {"replay_facts": {"steering_returned": []}}]
)
async def test_nothing_returned_streams_nothing(row):
    registry = SimpleNamespace(append_event_for_task=AsyncMock())

    await _emit_returned_steering(registry, _task(), row)

    registry.append_event_for_task.assert_not_awaited()
