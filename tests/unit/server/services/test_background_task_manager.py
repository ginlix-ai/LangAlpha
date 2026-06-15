"""
Tests for BackgroundTaskManager.cancel_stale_workflow and consume_workflow event passing.

Covers:
- cancel_stale_workflow no-ops for missing or completed tasks
- cancel_stale_workflow cancels RUNNING tasks
- cancel_stale_workflow handles timeout when task won't exit
- _run_workflow uses closure-captured events (not re-acquired from lock)
- Outer-task .cancel() propagates into inner consume_workflow (post-shield-removal)
- user cancel_workflow force-cancels inner_task + flushes checkpoint on explicit_cancel
- single-owner stop teardown ordering (drain before cancel_and_clear)
- wait_for_admission: fresh / running / stopping decisions
"""

import asyncio
import logging
from contextlib import suppress
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_btm() -> BackgroundTaskManager:
    """Create a BackgroundTaskManager with config calls patched out."""
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value="memory"), \
         patch("src.server.services.background_task_manager.get_redis_ttl_workflow_events", return_value=86400):
        btm = BackgroundTaskManager()
    return btm


def _make_task_info(
    thread_id: str = "thread-1",
    status: TaskStatus = TaskStatus.RUNNING,
    task: asyncio.Task | None = None,
    inner_task: asyncio.Task | None = None,
    run_id: str = "run-1",
) -> TaskInfo:
    """Create a TaskInfo with sensible defaults for testing."""
    return TaskInfo(
        thread_id=thread_id,
        run_id=run_id,
        status=status,
        created_at=datetime.now(),
        started_at=datetime.now(),
        task=task,
        inner_task=inner_task,
    )


# ---------------------------------------------------------------------------
# cancel_stale_workflow — no task
# ---------------------------------------------------------------------------

class TestCancelStaleWorkflowNoTask:

    @pytest.mark.asyncio
    async def test_cancel_stale_workflow_no_task(self, caplog):
        """cancel_stale_workflow returns False and logs no warning for missing thread."""
        btm = _make_btm()

        with caplog.at_level(logging.WARNING):
            result = await btm.cancel_stale_workflow("nonexistent")

        assert result is False
        assert "nonexistent" not in caplog.text


# ---------------------------------------------------------------------------
# cancel_stale_workflow — RUNNING
# ---------------------------------------------------------------------------

class TestCancelStaleWorkflowRunning:

    @pytest.mark.asyncio
    async def test_cancel_stale_workflow_running(self):
        """cancel_stale_workflow sets cancel_event, cancels inner_task, returns True."""
        btm = _make_btm()

        # Create mock tasks
        mock_inner = MagicMock(spec=asyncio.Task)
        mock_inner.done.return_value = False
        mock_inner.cancel = MagicMock()

        # Outer task that completes immediately when awaited
        outer_future = asyncio.get_event_loop().create_future()
        outer_future.set_result(None)

        task_info = _make_task_info(
            status=TaskStatus.RUNNING,
            task=outer_future,
            inner_task=mock_inner,
        )
        btm.tasks[("thread-1", "run-1")] = task_info

        result = await btm.cancel_stale_workflow("thread-1")

        assert result is True
        assert task_info.cancel_event.is_set()
        assert task_info.explicit_cancel is True
        mock_inner.cancel.assert_called_once()


# ---------------------------------------------------------------------------
# cancel_workflow — user stop force-cancels inner_task (immediacy)
# ---------------------------------------------------------------------------

class TestCancelWorkflowForceCancelsInner:

    @pytest.mark.asyncio
    async def test_user_cancel_force_cancels_inner_task(self):
        """cancel_workflow force-cancels a not-done inner_task immediately."""
        btm = _make_btm()

        mock_inner = MagicMock(spec=asyncio.Task)
        mock_inner.done.return_value = False
        mock_inner.cancel = MagicMock()

        task_info = _make_task_info(
            status=TaskStatus.RUNNING, inner_task=mock_inner
        )
        btm.tasks[("thread-1", "run-1")] = task_info

        result = await btm.cancel_workflow("thread-1")

        assert result is True
        assert task_info.cancel_event.is_set()
        assert task_info.explicit_cancel is True
        mock_inner.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_user_cancel_skips_done_inner_task(self):
        """A done inner_task is not re-cancelled."""
        btm = _make_btm()

        mock_inner = MagicMock(spec=asyncio.Task)
        mock_inner.done.return_value = True
        mock_inner.cancel = MagicMock()

        task_info = _make_task_info(
            status=TaskStatus.RUNNING, inner_task=mock_inner
        )
        btm.tasks[("thread-1", "run-1")] = task_info

        result = await btm.cancel_workflow("thread-1")

        assert result is True
        mock_inner.cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_system_cancel_does_not_downgrade_user_stop(self):
        """A later system cancel (user_initiated=False) must NOT clear a
        user_stop already set by the user's HTTP /cancel. Otherwise a graceful
        shutdown racing the stop teardown (before status flips off RUNNING)
        would mislabel the turn as system-cancelled."""
        btm = _make_btm()
        task_info = _make_task_info(status=TaskStatus.RUNNING)
        btm.tasks[("thread-1", "run-1")] = task_info

        # User presses Stop.
        assert await btm.cancel_workflow("thread-1", user_initiated=True) is True
        assert task_info.user_stop is True

        # Graceful shutdown fires a system cancel on the same still-RUNNING task.
        assert await btm.cancel_workflow(
            "thread-1", "run-1", user_initiated=False
        ) is True
        assert task_info.user_stop is True  # not downgraded

    @pytest.mark.asyncio
    async def test_system_only_cancel_leaves_user_stop_false(self):
        """A system-only cancel (no preceding user stop) keeps user_stop False
        so it persists cancelled_by_user=False."""
        btm = _make_btm()
        task_info = _make_task_info(status=TaskStatus.RUNNING)
        btm.tasks[("thread-1", "run-1")] = task_info

        assert await btm.cancel_workflow(
            "thread-1", "run-1", user_initiated=False
        ) is True
        assert task_info.explicit_cancel is True
        assert task_info.user_stop is False


# ---------------------------------------------------------------------------
# cancel_stale_workflow — COMPLETED (no-op)
# ---------------------------------------------------------------------------

class TestCancelStaleWorkflowCompleted:

    @pytest.mark.asyncio
    async def test_cancel_stale_workflow_completed(self):
        """cancel_stale_workflow returns False for a COMPLETED task."""
        btm = _make_btm()

        task_info = _make_task_info(status=TaskStatus.COMPLETED)
        btm.tasks[("thread-1", "run-1")] = task_info

        result = await btm.cancel_stale_workflow("thread-1")

        assert result is False
        # cancel_event should NOT have been set
        assert not task_info.cancel_event.is_set()


# ---------------------------------------------------------------------------
# cancel_stale_workflow — timeout waiting for outer task
# ---------------------------------------------------------------------------

class TestCancelStaleWorkflowTimeout:

    @pytest.mark.asyncio
    async def test_cancel_stale_workflow_timeout(self, caplog):
        """cancel_stale_workflow logs warning when outer task does not exit in time."""
        btm = _make_btm()

        mock_inner = MagicMock(spec=asyncio.Task)
        mock_inner.done.return_value = False
        mock_inner.cancel = MagicMock()

        # Outer task that never completes
        never_done = asyncio.get_event_loop().create_future()

        task_info = _make_task_info(
            status=TaskStatus.RUNNING,
            task=never_done,
            inner_task=mock_inner,
        )
        btm.tasks[("thread-1", "run-1")] = task_info

        with caplog.at_level(logging.WARNING):
            result = await btm.cancel_stale_workflow("thread-1", timeout=0.05)

        assert result is True
        assert "did not exit within" in caplog.text


# ---------------------------------------------------------------------------
# consume_workflow uses closure-captured events
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# cancel_workflow — run_id targeting (active vs latest, explicit vs implicit)
# ---------------------------------------------------------------------------

class TestCancelWorkflowRunIdTargeting:
    """``cancel_workflow(thread_id)`` without an explicit run_id must target
    the still-active run on the thread — not the most recently *created* row
    (which may already be terminal). With an explicit run_id, the cancel
    must hit exactly that key even if another run is more recent."""

    @pytest.mark.asyncio
    async def test_implicit_targets_active_not_latest_completed(self):
        """Older RUNNING + newer COMPLETED on the same thread ⇒ cancel hits
        the RUNNING one. The COMPLETED row stays untouched (no cancel_event)."""
        btm = _make_btm()

        # Older RUNNING task (created earlier)
        older_running = _make_task_info(
            status=TaskStatus.RUNNING, run_id="run-older"
        )
        older_running.created_at = datetime(2024, 1, 1, 12, 0, 0)

        # Newer COMPLETED task (created later)
        newer_completed = _make_task_info(
            status=TaskStatus.COMPLETED, run_id="run-newer"
        )
        newer_completed.created_at = datetime(2024, 1, 1, 12, 5, 0)

        btm.tasks[("thread-1", "run-older")] = older_running
        btm.tasks[("thread-1", "run-newer")] = newer_completed

        result = await btm.cancel_workflow("thread-1")

        assert result is True
        assert older_running.cancel_event.is_set()
        assert older_running.explicit_cancel is True
        # Newer terminal row must NOT have been disturbed.
        assert not newer_completed.cancel_event.is_set()
        assert newer_completed.explicit_cancel is False

    @pytest.mark.asyncio
    async def test_returns_false_when_only_terminal_runs_exist(self):
        """No live runs on the thread ⇒ cancel is a no-op + returns False."""
        btm = _make_btm()

        for status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            ti = _make_task_info(status=status, run_id=f"run-{status.value}")
            btm.tasks[("thread-1", ti.run_id)] = ti

        result = await btm.cancel_workflow("thread-1")

        assert result is False
        for ti in btm.tasks.values():
            assert not ti.cancel_event.is_set()
            assert ti.explicit_cancel is False

    @pytest.mark.asyncio
    async def test_explicit_run_id_targets_that_run_even_when_older(self):
        """A caller that passes a specific run_id wants THAT run cancelled,
        not "the most recent thing on the thread"."""
        btm = _make_btm()

        target = _make_task_info(status=TaskStatus.RUNNING, run_id="run-target")
        target.created_at = datetime(2024, 1, 1, 12, 0, 0)

        more_recent = _make_task_info(status=TaskStatus.RUNNING, run_id="run-newer")
        more_recent.created_at = datetime(2024, 1, 1, 12, 10, 0)

        btm.tasks[("thread-1", "run-target")] = target
        btm.tasks[("thread-1", "run-newer")] = more_recent

        result = await btm.cancel_workflow("thread-1", run_id="run-target")

        assert result is True
        assert target.cancel_event.is_set()
        assert target.explicit_cancel is True
        # The more-recent unrelated run must NOT be affected.
        assert not more_recent.cancel_event.is_set()
        assert more_recent.explicit_cancel is False


class TestConsumeWorkflowUsesClosureEvents:

    @pytest.mark.asyncio
    async def test_consume_workflow_uses_closure_events(self):
        """_run_workflow checks the cancel_event passed as a parameter.

        The cancel_event passed to _run_workflow is captured by the inner
        consume_workflow closure. When that event is set, the workflow
        should stop — proving the closure uses the parameter, not a fresh
        lookup from self.tasks.
        """
        btm = _make_btm()

        async def fake_workflow():
            """Async generator that yields events with a small delay."""
            for i in range(20):
                await asyncio.sleep(0.01)
                yield f"event-{i}"

        cancel_event = asyncio.Event()

        # Pre-register a RUNNING task so _run_workflow can find it
        task_info = _make_task_info(thread_id="thread-closure", status=TaskStatus.RUNNING)
        btm.tasks[("thread-closure", "run-1")] = task_info

        # Patch _mark_completed, _mark_cancelled, _mark_failed so they don't
        # try to do real persistence work
        with patch.object(btm, "_mark_completed", new_callable=AsyncMock) as mock_mark_completed, \
             patch.object(btm, "_mark_cancelled", new_callable=AsyncMock) as mock_mark_cancelled, \
             patch.object(btm, "_mark_failed", new_callable=AsyncMock), \
             patch.object(btm, "_flush_checkpoint", new_callable=AsyncMock), \
             patch.object(btm, "_teardown_subagents_on_stop", new_callable=AsyncMock):

            # Schedule setting the cancel_event after a brief delay
            async def set_cancel_after_delay():
                await asyncio.sleep(0.05)
                cancel_event.set()

            cancel_task = asyncio.create_task(set_cancel_after_delay())

            # Run the workflow — it should exit early via CancelledError
            # because cancel_event gets set after ~5 events
            with pytest.raises(asyncio.CancelledError):
                await btm._run_workflow(
                    thread_id="thread-closure",
                    run_id="run-1",
                    workflow_generator=fake_workflow(),
                    cancel_event=cancel_event,
                )

            await cancel_task

        # The workflow should NOT have consumed all 20 events — it should
        # have been cut short by the cancel_event being set.
        assert cancel_event.is_set()
        # The closure observed the cancel_event: cancellation path ran and the
        # completion path did not. Without these the test false-passes even if
        # _run_workflow ignored the event and ran to completion.
        mock_mark_cancelled.assert_awaited_once_with("thread-closure", "run-1")
        mock_mark_completed.assert_not_awaited()
        # The inner task was registered on the task_info.
        assert task_info.inner_task is not None


# ---------------------------------------------------------------------------
# Force-cancel propagates from outer task into inner consume_workflow
# ---------------------------------------------------------------------------

class TestOuterTaskCancelPropagatesToInner:

    @pytest.mark.asyncio
    async def test_outer_task_cancel_propagates_to_inner(self):
        """Cancelling the outer task that wraps _run_workflow now cancels the
        inner consume_workflow task and runs _mark_cancelled.

        Pinpoints the behavior change from removing ``asyncio.shield(inner_task)``:
        shutdown's force-cancel path (background_task_manager.py:367) and
        _cleanup_abandoned_tasks (line 432) both call ``info.task.cancel()``.
        Pre-shield-removal: inner_task kept running orphaned; post-removal:
        cancellation propagates through ``await inner_task`` and the workflow
        generator is closed cleanly.
        """
        btm = _make_btm()

        generator_closed = asyncio.Event()

        async def fake_workflow():
            try:
                for i in range(1000):
                    await asyncio.sleep(0.01)
                    yield f"event-{i}"
            finally:
                generator_closed.set()

        cancel_event = asyncio.Event()

        task_info = _make_task_info(
            thread_id="thread-outer", run_id="run-outer", status=TaskStatus.RUNNING
        )
        btm.tasks[("thread-outer", "run-outer")] = task_info

        with patch.object(btm, "_mark_completed", new_callable=AsyncMock), \
             patch.object(btm, "_mark_cancelled", new_callable=AsyncMock) as mock_mark_cancelled, \
             patch.object(btm, "_mark_failed", new_callable=AsyncMock), \
             patch.object(btm, "_flush_checkpoint", new_callable=AsyncMock), \
             patch.object(btm, "_teardown_subagents_on_stop", new_callable=AsyncMock):

            outer_task = asyncio.create_task(
                btm._run_workflow(
                    thread_id="thread-outer",
                    run_id="run-outer",
                    workflow_generator=fake_workflow(),
                    cancel_event=cancel_event,
                )
            )

            # Let the inner task spin up and register itself.
            await asyncio.sleep(0.05)
            assert task_info.inner_task is not None
            inner_task = task_info.inner_task

            # Simulate shutdown / abandoned-cleanup directly cancelling
            # the outer task — the path that previously hit the shield.
            outer_task.cancel()

            with suppress(asyncio.CancelledError):
                await outer_task

        # The cooperative cancel_event was never set — proves the cancellation
        # arrived via the outer task, not the cooperative path.
        assert not cancel_event.is_set()
        # Inner task is now done (shield removal lets the cancel propagate).
        assert inner_task.done()
        assert inner_task.cancelled()
        # _mark_cancelled ran inside the except handler.
        mock_mark_cancelled.assert_awaited_once_with("thread-outer", "run-outer")
        # The workflow generator's finally block ran — no orphaned generator.
        assert generator_closed.is_set()


# ---------------------------------------------------------------------------
# _mark_cancelled labels persistence by cancel origin (explicit_cancel)
# ---------------------------------------------------------------------------

class TestMarkCancelledUserLabeling:
    """``cancelled_by_user`` must reflect ``task_info.user_stop``, NOT
    ``explicit_cancel``.

    A user pressing Stop (HTTP /cancel) sets both explicit_cancel AND user_stop.
    System cancels — graceful shutdown (cancel_workflow with user_initiated=
    False) and stale-sandbox recovery (cancel_stale_workflow) — also set
    explicit_cancel (to gate flush+teardown) but leave user_stop False, so they
    must persist cancelled_by_user=False. Keying off explicit_cancel would
    mislabel a pod-roll or workspace eviction as a user "Stopped" turn.
    """

    async def _run_mark_cancelled(self, btm, task_info):
        """Drive _mark_cancelled's persistence path and return the persist kwargs."""
        persistence_service = MagicMock()
        persistence_service.persist_cancelled = AsyncMock(return_value="resp-id")
        task_info.metadata = {
            "workspace_id": "ws-1",
            "user_id": "user-1",
            "persistence_service": persistence_service,
        }
        btm.tasks[(task_info.thread_id, task_info.run_id)] = task_info

        mod = "src.server.services.background_task_manager"
        with patch(f"{mod}.get_token_usage_from_callback", return_value=(None, [])), \
             patch(f"{mod}.get_tool_usage_from_handler", return_value={}), \
             patch(f"{mod}.get_sse_events_from_handler", return_value=[]), \
             patch(f"{mod}.calculate_execution_time", return_value=1.0), \
             patch(f"{mod}.release_burst_slot", new_callable=AsyncMock), \
             patch(f"{mod}.WorkflowTracker") as mock_tracker_cls:
            mock_tracker_cls.get_instance.return_value.mark_cancelled = AsyncMock()
            await btm._mark_cancelled(task_info.thread_id, task_info.run_id)

        persistence_service.persist_cancelled.assert_awaited_once()
        return persistence_service.persist_cancelled.await_args.kwargs["metadata"]

    @pytest.mark.asyncio
    async def test_abandoned_cancel_persists_not_user(self):
        """Bare force-cancel (abandoned cleanup): neither flag → not user."""
        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-sys", run_id="run-sys")
        assert task_info.explicit_cancel is False
        assert task_info.user_stop is False

        persist_metadata = await self._run_mark_cancelled(btm, task_info)

        assert persist_metadata["cancelled_by_user"] is False

    @pytest.mark.asyncio
    async def test_user_cancel_persists_user(self):
        """user_stop set (HTTP /cancel) → cancelled_by_user=True."""
        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-usr", run_id="run-usr")
        task_info.explicit_cancel = True
        task_info.user_stop = True

        persist_metadata = await self._run_mark_cancelled(btm, task_info)

        assert persist_metadata["cancelled_by_user"] is True

    @pytest.mark.asyncio
    async def test_system_cancel_with_explicit_flag_not_user(self):
        """REGRESSION (C1): graceful shutdown / stale-sandbox recovery set
        explicit_cancel (to flush+teardown) but user_stop=False, so the
        interrupted turn must NOT be persisted as a user-cancelled Stop. A
        pod-roll or workspace eviction mid-stream previously wrote fake
        "Stopped" turns into chat history."""
        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-shutdown", run_id="run-sd")
        task_info.explicit_cancel = True   # shutdown/stale set this...
        assert task_info.user_stop is False  # ...but NOT user_stop

        persist_metadata = await self._run_mark_cancelled(btm, task_info)

        assert persist_metadata["cancelled_by_user"] is False


# ---------------------------------------------------------------------------
# _run_workflow stop path: flush + teardown gated on explicit_cancel
# ---------------------------------------------------------------------------

class TestStopPathFlushGating:
    """The except-CancelledError handler flushes the checkpoint and tears down
    subagents ONLY when the cancel was user-initiated (explicit_cancel)."""

    async def _drive_stop(self, btm, *, explicit: bool):
        async def fake_workflow():
            for i in range(1000):
                await asyncio.sleep(0.01)
                yield f"event-{i}"

        cancel_event = asyncio.Event()
        task_info = _make_task_info(
            thread_id="t-stop", run_id="r-stop", status=TaskStatus.RUNNING
        )
        task_info.explicit_cancel = explicit
        btm.tasks[("t-stop", "r-stop")] = task_info

        with patch.object(btm, "_mark_completed", new_callable=AsyncMock), \
             patch.object(btm, "_mark_cancelled", new_callable=AsyncMock), \
             patch.object(btm, "_mark_failed", new_callable=AsyncMock), \
             patch.object(btm, "_flush_checkpoint", new_callable=AsyncMock) as flush, \
             patch.object(btm, "_teardown_subagents_on_stop", new_callable=AsyncMock) as teardown:

            outer = asyncio.create_task(
                btm._run_workflow(
                    thread_id="t-stop",
                    run_id="r-stop",
                    workflow_generator=fake_workflow(),
                    cancel_event=cancel_event,
                )
            )
            await asyncio.sleep(0.05)
            inner = task_info.inner_task
            inner.cancel()
            with suppress(asyncio.CancelledError):
                await outer
        return flush, teardown

    @pytest.mark.asyncio
    async def test_explicit_cancel_flushes_and_tears_down(self):
        btm = _make_btm()
        flush, teardown = await self._drive_stop(btm, explicit=True)
        flush.assert_awaited_once_with("t-stop", "r-stop")
        teardown.assert_awaited_once_with("t-stop", "r-stop")

    @pytest.mark.asyncio
    async def test_system_cancel_does_not_flush_or_teardown(self):
        btm = _make_btm()
        flush, teardown = await self._drive_stop(btm, explicit=False)
        flush.assert_not_awaited()
        teardown.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flush_failure_still_marks_cancelled(self):
        """A raising _flush_checkpoint must not prevent _mark_cancelled."""
        btm = _make_btm()

        async def fake_workflow():
            for i in range(1000):
                await asyncio.sleep(0.01)
                yield f"event-{i}"

        cancel_event = asyncio.Event()
        task_info = _make_task_info(
            thread_id="t-flushfail", run_id="r-flushfail", status=TaskStatus.RUNNING
        )
        task_info.explicit_cancel = True
        btm.tasks[("t-flushfail", "r-flushfail")] = task_info

        with patch.object(btm, "_mark_completed", new_callable=AsyncMock), \
             patch.object(btm, "_mark_cancelled", new_callable=AsyncMock) as mark, \
             patch.object(btm, "_mark_failed", new_callable=AsyncMock), \
             patch.object(btm, "_flush_checkpoint", new_callable=AsyncMock,
                          side_effect=RuntimeError("flush boom")), \
             patch.object(btm, "_teardown_subagents_on_stop", new_callable=AsyncMock):

            outer = asyncio.create_task(
                btm._run_workflow(
                    thread_id="t-flushfail",
                    run_id="r-flushfail",
                    workflow_generator=fake_workflow(),
                    cancel_event=cancel_event,
                )
            )
            await asyncio.sleep(0.05)
            task_info.inner_task.cancel()
            with suppress(asyncio.CancelledError):
                await outer

        mark.assert_awaited_once_with("t-flushfail", "r-flushfail")

    @pytest.mark.asyncio
    async def test_recancel_during_teardown_still_marks_cancelled(self):
        """REGRESSION (C2): a second CancelledError landing in teardown (e.g.
        graceful shutdown force-cancelling the OUTER task while the single-owner
        teardown is mid-flight) must NOT skip _mark_cancelled. The finally +
        asyncio.shield guarantee persistence/burst-slot release/registry cleanup
        run rather than leaving half-state."""
        btm = _make_btm()

        async def fake_workflow():
            for i in range(1000):
                await asyncio.sleep(0.01)
                yield f"event-{i}"

        cancel_event = asyncio.Event()
        task_info = _make_task_info(
            thread_id="t-recancel", run_id="r-recancel", status=TaskStatus.RUNNING
        )
        task_info.explicit_cancel = True
        btm.tasks[("t-recancel", "r-recancel")] = task_info

        with patch.object(btm, "_mark_completed", new_callable=AsyncMock), \
             patch.object(btm, "_mark_cancelled", new_callable=AsyncMock) as mark, \
             patch.object(btm, "_mark_failed", new_callable=AsyncMock), \
             patch.object(btm, "_flush_checkpoint", new_callable=AsyncMock), \
             patch.object(btm, "_teardown_subagents_on_stop", new_callable=AsyncMock,
                          side_effect=asyncio.CancelledError()):

            outer = asyncio.create_task(
                btm._run_workflow(
                    thread_id="t-recancel",
                    run_id="r-recancel",
                    workflow_generator=fake_workflow(),
                    cancel_event=cancel_event,
                )
            )
            await asyncio.sleep(0.05)
            task_info.inner_task.cancel()
            with suppress(asyncio.CancelledError):
                await outer

        # Even though teardown raised CancelledError, persistence still ran.
        mark.assert_awaited_once_with("t-recancel", "r-recancel")


# ---------------------------------------------------------------------------
# Single-owner teardown ordering (decision 1A): drain BEFORE cancel_and_clear
# ---------------------------------------------------------------------------

class TestStopTeardownOrdering:

    @pytest.mark.asyncio
    async def test_drain_runs_before_cancel_and_clear(self):
        """_teardown_subagents_on_stop drains killed-subagent events and stashes
        them on metadata, and the drain happens BEFORE cancel_and_clear wipes
        the registry."""
        btm = _make_btm()

        order: list[str] = []

        task_info = _make_task_info(
            thread_id="t-order", run_id="r-order", status=TaskStatus.RUNNING
        )
        btm.tasks[("t-order", "r-order")] = task_info

        fake_registry = MagicMock()
        fake_registry.get_all_tasks = AsyncMock(return_value=["task-a"])

        async def fake_drain(thread_id, tasks):
            order.append("drain")
            return [{"event": "message_chunk", "data": {"agent": "task:x"}}]

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=fake_registry)

        async def fake_cancel_and_clear(thread_id, *, force):
            order.append("cancel_and_clear")
            return 1

        fake_store.cancel_and_clear = AsyncMock(side_effect=fake_cancel_and_clear)

        with patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ), patch.object(btm, "_drain_killed_subagent_events", side_effect=fake_drain):
            await btm._teardown_subagents_on_stop("t-order", "r-order")

        assert order == ["drain", "cancel_and_clear"]
        stashed = task_info.metadata.get("_stop_subagent_events")
        assert stashed and stashed[0]["data"]["agent"] == "task:x"

    @pytest.mark.asyncio
    async def test_drain_timeout_proceeds_without_events(self):
        """A drain that exceeds stop_drain_timeout doesn't block teardown."""
        btm = _make_btm()

        task_info = _make_task_info(
            thread_id="t-tmo", run_id="r-tmo", status=TaskStatus.RUNNING
        )
        btm.tasks[("t-tmo", "r-tmo")] = task_info

        fake_registry = MagicMock()
        fake_registry.get_all_tasks = AsyncMock(return_value=["task-a"])

        async def slow_drain(thread_id, tasks):
            await asyncio.sleep(5)
            return [{"event": "x"}]

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=fake_registry)
        fake_store.cancel_and_clear = AsyncMock(return_value=1)

        with patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ), patch.object(btm, "_drain_killed_subagent_events", side_effect=slow_drain), \
           patch(
               "src.server.services.background_task_manager.get_stop_drain_timeout",
               return_value=0.05,
           ):
            await btm._teardown_subagents_on_stop("t-tmo", "r-tmo")

        # No drained events stashed, but cancel_and_clear still ran.
        assert "_stop_subagent_events" not in task_info.metadata
        fake_store.cancel_and_clear.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_orphan_collectors_cancelled_on_stop(self):
        """Tracked orphan collectors are cancelled during teardown so they
        can't mutate the response after the stop."""
        btm = _make_btm()

        task_info = _make_task_info(
            thread_id="t-orph", run_id="r-orph", status=TaskStatus.RUNNING
        )
        btm.tasks[("t-orph", "r-orph")] = task_info

        started = asyncio.Event()

        async def long_collector():
            started.set()
            await asyncio.sleep(100)

        collector = asyncio.create_task(long_collector())
        btm._track_orphan_collector("t-orph", collector)
        await started.wait()

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=None)
        fake_store.cancel_and_clear = AsyncMock(return_value=0)

        with patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ):
            await btm._teardown_subagents_on_stop("t-orph", "r-orph")

        assert collector.cancelled()
        assert "t-orph" not in btm._orphan_collectors


# ---------------------------------------------------------------------------
# wait_for_admission decisions (decision 2A)
# ---------------------------------------------------------------------------

class TestWaitForAdmission:

    @pytest.mark.asyncio
    async def test_no_active_task_is_fresh(self):
        btm = _make_btm()
        assert await btm.wait_for_admission("t-none") == "fresh"

    @pytest.mark.asyncio
    async def test_running_task_is_running(self):
        btm = _make_btm()
        ti = _make_task_info(thread_id="t-run", status=TaskStatus.RUNNING)
        btm.tasks[("t-run", ti.run_id)] = ti
        assert await btm.wait_for_admission("t-run") == "running"

    @pytest.mark.asyncio
    async def test_cancelled_completing_within_wait_is_fresh(self):
        """An explicitly-cancelled task that finishes winding down within the
        wait → fresh turn, and no CancelledError reaches the caller."""
        btm = _make_btm()

        async def dies():
            raise asyncio.CancelledError()

        task = asyncio.ensure_future(dies())
        with suppress(asyncio.CancelledError):
            await asyncio.sleep(0)  # let it schedule
        ti = _make_task_info(thread_id="t-stop", status=TaskStatus.RUNNING, task=task)
        ti.explicit_cancel = True
        btm.tasks[("t-stop", ti.run_id)] = ti

        # Caller is unaffected by the task's CancelledError.
        state = await btm.wait_for_admission("t-stop")
        assert state == "fresh"
        # Terminal-marked task evicted so a fresh turn proceeds.

    @pytest.mark.asyncio
    async def test_cancelled_still_winding_down_is_stopping(self):
        """An explicitly-cancelled task still tearing down past the wait → 409
        'stopping'."""
        btm = _make_btm()

        never = asyncio.get_event_loop().create_future()
        ti = _make_task_info(thread_id="t-slow", status=TaskStatus.RUNNING, task=never)
        ti.explicit_cancel = True
        btm.tasks[("t-slow", ti.run_id)] = ti

        with patch(
            "src.server.services.background_task_manager.get_checkpoint_flush_timeout",
            return_value=0.01,
        ):
            state = await btm.wait_for_admission("t-slow")
        assert state == "stopping"

    @pytest.mark.asyncio
    async def test_terminal_task_is_fresh(self):
        btm = _make_btm()
        ti = _make_task_info(thread_id="t-done", status=TaskStatus.COMPLETED)
        btm.tasks[("t-done", ti.run_id)] = ti
        assert await btm.wait_for_admission("t-done") == "fresh"
