"""A graceful restart stops a subagent so the next process can resume it.

Its writer is cancelled with the restart's own message, which settles the run
as a stop the ledger tells apart from a user's (its failure's error type)
with its checkpoint kept, and no cancel intent is stamped, since that would
settle it as the user's stop.
"""

from __future__ import annotations

import asyncio

import pytest

from ptc_agent.agent.middleware.background_subagent import run_executor
from ptc_agent.agent.middleware.background_subagent.outcomes import (
    Outcome,
    is_restart_stop,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    RESTART_CANCEL,
    TIME_LIMIT_CANCEL,
    BackgroundTaskRegistry,
)
from src.server.contracts.status import RESTART_STOP_ERROR_TYPE
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker


class _Ledger:
    def __init__(self) -> None:
        self.statuses: list[str] = []
        self.failures: list[dict | None] = []

    async def finalize_task_run(self, task_run_id, status, **kwargs):
        self.statuses.append(status)
        self.failures.append(kwargs.get("failure"))
        return {"applied": True, "run": {"status": status}}

    async def append_run_end(self, task_run_id, **kwargs):
        pass

    async def stamp_cancel_intent(self, *args, **kwargs):
        raise AssertionError("a restart stop must not stamp cancel intent")


async def _register(registry: BackgroundTaskRegistry, writer):
    return await registry.register(
        tool_call_id=f"tc-{id(writer)}",
        description="d",
        prompt="p",
        subagent_type="research",
        asyncio_task=writer,
    )


@pytest.mark.asyncio
async def test_a_restart_stops_every_running_subagent_resumably():
    registry = BackgroundTaskRegistry()
    ledger = registry.run_ledger = _Ledger()
    task = await _register(registry, None)
    task.task_run_id = "run-1"

    async def stuck_in_a_tool(_request):
        await asyncio.Event().wait()

    writer = asyncio.create_task(
        run_executor._run_background_task(
            task, stuck_in_a_tool, None, PerCallTokenTracker(), "Subagent",
            registry=registry,
        )
    )
    task.asyncio_task = writer
    await asyncio.sleep(0)

    stopping = await registry.stop_writers_for_restart()
    assert writer in stopping
    result = await asyncio.wait_for(writer, 2)

    assert is_restart_stop(result)
    assert ledger.statuses == ["cancelled"]
    assert ledger.failures == [Outcome.restart_stop().as_failure()]
    assert ledger.failures[0]["error_type"] == RESTART_STOP_ERROR_TYPE


@pytest.mark.asyncio
async def test_a_settled_task_still_unwinding_is_not_relabelled():
    seen: list[asyncio.CancelledError] = []

    async def unwinding() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as e:
            seen.append(e)

    registry = BackgroundTaskRegistry()
    hung = asyncio.create_task(unwinding())
    settled = await _register(registry, hung)
    settled.terminal_status = "completed"
    await asyncio.sleep(0)

    assert await registry.stop_writers_for_restart() == [hung]
    await hung
    assert len(seen) == 1 and seen[0].args != (RESTART_CANCEL,)


def test_each_of_our_stops_reads_as_its_own_outcome():
    assert Outcome.from_cancel(asyncio.CancelledError(RESTART_CANCEL)) == Outcome.restart_stop()
    timed_out = Outcome.from_cancel(asyncio.CancelledError(TIME_LIMIT_CANCEL))
    assert timed_out is not None and timed_out.local_status == "timeout"
    assert Outcome.from_cancel(asyncio.CancelledError()) is None


def test_the_restart_stop_settles_neutral_and_resumable():
    outcome = Outcome.restart_stop()
    assert outcome.status == "cancelled"
    assert outcome.error_type == RESTART_STOP_ERROR_TYPE
    assert is_restart_stop(outcome.as_result())
    assert not is_restart_stop({"error_type": "credit_stop"})
    assert not is_restart_stop(None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "returned"),
    [({"error_type": RESTART_STOP_ERROR_TYPE}, False), (None, True)],
    ids=["restart", "user"],
)
async def test_a_restart_stop_keeps_its_steers_for_the_resume(failure, returned):
    from datetime import datetime, timezone
    from unittest.mock import AsyncMock, MagicMock, patch

    from src.server.database.runs import subagent_runs

    settle = AsyncMock(return_value=[])
    run = {
        "task_run_id": "tr-1",
        "thread_id": "thread-1",
        "task_id": "abc123",
        "status": "cancelled",
        "cancel_requested_at": datetime.now(timezone.utc),
        "failure": failure,
    }
    with patch("src.server.database.runs.outbox.settle_task_run_steers", settle):
        await subagent_runs._settle_steers(MagicMock(), run)
    assert settle.await_args.kwargs["cancelled"] is returned
