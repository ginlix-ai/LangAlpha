"""Locks the orchestrator completion-nudge ownership rules.

The mid-turn pointer nudge announces only tasks this turn can actually
fetch: never when the middleware is disabled (the turn has no TaskOutput
tool — e.g. a disable_subagents notification turn), and never a task the
report-back outbox pipeline has claimed (it will be delivered by its own
notification turn; announcing it here would deliver it twice).
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
    result_seen: bool = False,
    report_back_claimed: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        task_id=task_id,
        display_id=f"Task-{task_id}",
        completed=completed,
        result_seen=result_seen,
        report_back_claimed=report_back_claimed,
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
    async def test_unclaimed_unseen_task_is_announced(self):
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
    async def test_claimed_task_belongs_to_the_outbox_pipeline(self):
        task = _task(report_back_claimed=True)
        orch = _orchestrator([task])
        assert await orch.check_and_get_notification() is None
        assert task.result_seen is False

    @pytest.mark.asyncio
    async def test_mixed_batch_announces_only_unclaimed(self):
        claimed = _task("claimed1", report_back_claimed=True)
        fresh = _task("fresh12")
        orch = _orchestrator([claimed, fresh])
        notification = await orch.check_and_get_notification()
        assert notification is not None
        assert "fresh12" in notification
        assert "claimed1" not in notification
        assert fresh.result_seen is True
        assert claimed.result_seen is False
