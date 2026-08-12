"""Locks the orchestrator completion-nudge ownership rules.

The mid-turn pointer nudge announces only tasks this turn can actually
fetch AND actually owns: never when the middleware is disabled (the turn
has no TaskOutput tool — e.g. a disable_subagents notification turn),
each completion at most once (result_seen), and never for ledgered runs
(task_run_id set) — those belong to the durable outbox notifier, and a
memory-gated announcement here can only duplicate it.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.orchestrator import (
    BackgroundSubagentOrchestrator,
)


def _task(
    task_id: str = "abc123",
    *,
    completed: bool = True,
    terminal_status: str | None = "completed",
    result_seen: bool = False,
    is_turn_visible: bool = True,
    task_run_id: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        task_id=task_id,
        display_id=f"Task-{task_id}",
        subagent_type="general-purpose",
        completed=completed,
        terminal_status=terminal_status if completed else None,
        result_seen=result_seen,
        is_turn_visible=is_turn_visible,
        task_run_id=task_run_id,
        asyncio_task=None,
        result={"success": True},
        error=None,
    )


def _orchestrator(tasks: list, *, enabled: bool = True):
    mw = MagicMock()
    mw.enabled = enabled
    mw.registry = MagicMock()
    mw.registry._tasks = {t.task_id: t for t in tasks}
    return BackgroundSubagentOrchestrator(MagicMock(), mw)


class TestCheckAndGetNotification:
    @pytest.mark.asyncio
    async def test_unseen_task_is_announced(self):
        task = _task()
        orch = _orchestrator([task])
        notification = await orch.check_and_get_notification()
        assert notification is not None
        assert "Task-abc123" in notification
        assert 'TaskOutput(task_id="abc123")' in notification
        assert task.result_seen is True

    @pytest.mark.asyncio
    async def test_disabled_middleware_never_announces(self):
        task = _task()
        orch = _orchestrator([task], enabled=False)
        assert await orch.check_and_get_notification() is None
        # The task stays unseen for whoever legitimately owns it.
        assert task.result_seen is False

    @pytest.mark.asyncio
    async def test_seen_task_is_not_reannounced(self):
        task = _task(result_seen=True)
        orch = _orchestrator([task])
        assert await orch.check_and_get_notification() is None

    @pytest.mark.asyncio
    async def test_mixed_batch_announces_only_unseen(self):
        seen = _task("seenta1", result_seen=True)
        fresh = _task("fresh12")
        orch = _orchestrator([seen, fresh])
        notification = await orch.check_and_get_notification()
        assert notification is not None
        assert "fresh12" in notification
        assert "seenta1" not in notification
        assert fresh.result_seen is True

    @pytest.mark.asyncio
    async def test_ledgered_task_is_never_announced(self):
        """A run with a ledger identity is outbox-owned: the durable
        notifier arbitrates at claim time against result_delivered_at, and
        this memory-gated sweep re-announcing it is exactly the duplicate
        report-back bug. It stays unseen here — TaskOutput's ledger
        resolution marks it when the fate is actually delivered."""
        ledgered = _task("ledger1", task_run_id="run-42")
        local = _task("local01")
        orch = _orchestrator([ledgered, local])
        notification = await orch.check_and_get_notification()
        assert notification is not None
        assert "local01" in notification
        assert "ledger1" not in notification
        assert ledgered.result_seen is False

    @pytest.mark.asyncio
    async def test_workflow_owned_child_is_never_announced(self):
        """Driver-scoped children (is_turn_visible=False) report to their
        workflow driver, not the main agent — and stay unseen for it."""
        child = _task("child01", is_turn_visible=False)
        orch = _orchestrator([child])
        assert await orch.check_and_get_notification() is None
        assert child.result_seen is False
