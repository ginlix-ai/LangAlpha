"""Locks the orchestrator completion-nudge ownership rules.

The mid-turn pointer nudge announces only tasks this turn can actually
fetch: never when the middleware is disabled (the turn has no TaskOutput
tool — e.g. a disable_subagents notification turn), and each completion
at most once (result_seen). Server-side report-back is outbox-owned and
never reaches this CLI-only path.
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
) -> SimpleNamespace:
    return SimpleNamespace(
        task_id=task_id,
        display_id=f"Task-{task_id}",
        completed=completed,
        result_seen=result_seen,
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
