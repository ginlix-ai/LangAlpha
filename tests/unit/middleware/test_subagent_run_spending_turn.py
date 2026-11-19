"""Which turn a subagent run's ledger row is spent for.

The row names the account of that turn's sender (``subagent_runs``), so the
middleware hands the ledger the run that resumes the task, else the run that
launched it, which a workflow's child, launched with no parent, carries too.
"""

from __future__ import annotations

import pytest

from tests.unit.middleware.test_subagent_run_ledger_admission import (
    FakeLedger,
    FakeOwner,
    _middleware,
    _ok_handler,
    _request,
)


@pytest.mark.asyncio
async def test_a_task_call_is_spent_for_the_turn_that_made_it():
    ledger = FakeLedger(admit="run-uuid-1")
    mw = _middleware(FakeOwner(), ledger)

    await mw.awrap_tool_call(_request({"description": "d", "prompt": "p"}), _ok_handler)

    started = ledger.started[0]
    assert started["spending_run_id"] == started["parent_run_id"] == "parent-run-1"
    await mw.registry._tasks["tc-1"].asyncio_task


@pytest.mark.asyncio
async def test_a_run_with_no_parent_is_spent_for_the_turn_that_launched_it():
    ledger = FakeLedger(admit="run-uuid-2")
    mw = _middleware(FakeOwner(), ledger)
    task = await mw.registry.register(
        tool_call_id="wf-1", description="d", prompt="p", subagent_type="general-purpose",
        run_id="turn-run-7", owner_task_id="wf",
    )

    await mw.admit_task_run(
        task, cause="init", description="d", launch_tool_call_id="wf-1", parent_run_id=None,
    )

    assert ledger.started[0]["parent_run_id"] is None
    assert ledger.started[0]["spending_run_id"] == "turn-run-7"


@pytest.mark.asyncio
async def test_the_coordinator_hands_the_spending_turn_to_the_row(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    from src.server.services.subagent_run_coordinator import SubagentRunCoordinator

    start = AsyncMock(return_value={"task_run_id": "run-1", "task_id": "abc123", "status": "in_progress"})
    monkeypatch.setattr("src.server.services.subagent_run_coordinator.sr_db.start_task_run", start)
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: MagicMock(enabled=False)
    )

    await SubagentRunCoordinator("t-1").start_task_run(
        task_id="abc123", cause="init", spending_run_id="turn-run-7"
    )

    assert start.await_args.kwargs["spending_run_id"] == "turn-run-7"
