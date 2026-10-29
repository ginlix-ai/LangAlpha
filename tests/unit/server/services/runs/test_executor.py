"""
Tests for LocalRunExecutor.cancel_stale_workflow and consume_workflow event passing.

Covers:
- cancel_stale_workflow no-ops for missing or completed tasks
- cancel_stale_workflow cancels RUNNING tasks
- cancel_stale_workflow handles timeout when task won't exit
- _run_workflow uses closure-captured events (not re-acquired from lock)
- Outer-task .cancel() propagates into inner consume_workflow (post-shield-removal)
- user signal_cancel force-cancels inner_task + flushes checkpoint on explicit_cancel
- single-owner stop teardown ordering (drain before cancel_and_clear)
- wait_for_admission: fresh / running / stopping decisions
"""

import asyncio
import contextvars
import gc
import logging
import weakref
from contextlib import suppress
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.runs.executor import (
    LocalRunExecutor,
    LocalRunExecution,
    LocalRunStatus,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    RunTasksKilled,
)
from src.server.services.runs import subagent_collection
from src.server.services.runs.teardown import StopTeardown

REGISTRY_STORE_MOD = "src.server.services.background_registry_store"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_btm() -> LocalRunExecutor:
    """Create a LocalRunExecutor with config calls patched out."""
    with patch("src.server.services.runs.executor.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.runs.executor.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.runs.executor.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.runs.executor.get_cleanup_interval", return_value=60), \
         patch("src.server.services.runs.executor.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.runs.executor.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.runs.executor.get_event_storage_backend", return_value="memory"), \
         patch("src.server.services.runs.executor.get_redis_ttl_workflow_events", return_value=86400):
        btm = LocalRunExecutor()
    return btm


def _make_task_info(
    thread_id: str = "thread-1",
    status: LocalRunStatus = LocalRunStatus.RUNNING,
    task: asyncio.Task | None = None,
    inner_task: asyncio.Task | None = None,
    run_id: str = "run-1",
) -> LocalRunExecution:
    """Create a LocalRunExecution with sensible defaults for testing."""
    return LocalRunExecution(
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
            status=LocalRunStatus.RUNNING,
            task=outer_future,
            inner_task=mock_inner,
        )
        btm.executions[("thread-1", "run-1")] = task_info

        result = await btm.cancel_stale_workflow("thread-1")

        assert result is True
        assert task_info.cancel_event.is_set()
        assert task_info.explicit_cancel is True
        mock_inner.cancel.assert_called_once()


# ---------------------------------------------------------------------------
# signal_cancel — user stop force-cancels inner_task (immediacy)
# ---------------------------------------------------------------------------

class TestCancelWorkflowForceCancelsInner:

    @pytest.mark.asyncio
    async def test_user_cancel_force_cancels_inner_task(self):
        """signal_cancel force-cancels a not-done inner_task immediately."""
        btm = _make_btm()

        mock_inner = MagicMock(spec=asyncio.Task)
        mock_inner.done.return_value = False
        mock_inner.cancel = MagicMock()

        task_info = _make_task_info(
            status=LocalRunStatus.RUNNING, inner_task=mock_inner
        )
        btm.executions[("thread-1", "run-1")] = task_info

        result = await btm.signal_cancel("thread-1")

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
            status=LocalRunStatus.RUNNING, inner_task=mock_inner
        )
        btm.executions[("thread-1", "run-1")] = task_info

        result = await btm.signal_cancel("thread-1")

        assert result is True
        mock_inner.cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_system_cancel_does_not_downgrade_user_stop(self):
        """A later system cancel (user_initiated=False) must NOT clear a
        user_stop already set by the user's HTTP /cancel. Otherwise a graceful
        shutdown racing the stop teardown (before status flips off RUNNING)
        would mislabel the turn as system-cancelled."""
        btm = _make_btm()
        task_info = _make_task_info(status=LocalRunStatus.RUNNING)
        btm.executions[("thread-1", "run-1")] = task_info

        # User presses Stop.
        assert await btm.signal_cancel("thread-1", user_initiated=True) is True
        assert task_info.user_stop is True

        # Graceful shutdown fires a system cancel on the same still-RUNNING task.
        assert await btm.signal_cancel(
            "thread-1", "run-1", user_initiated=False
        ) is True
        assert task_info.user_stop is True  # not downgraded

    @pytest.mark.asyncio
    async def test_system_only_cancel_leaves_user_stop_false(self):
        """A system-only cancel (no preceding user stop) keeps user_stop False
        so it persists cancelled_by_user=False."""
        btm = _make_btm()
        task_info = _make_task_info(status=LocalRunStatus.RUNNING)
        btm.executions[("thread-1", "run-1")] = task_info

        assert await btm.signal_cancel(
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

        task_info = _make_task_info(status=LocalRunStatus.COMPLETED)
        btm.executions[("thread-1", "run-1")] = task_info

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
            status=LocalRunStatus.RUNNING,
            task=never_done,
            inner_task=mock_inner,
        )
        btm.executions[("thread-1", "run-1")] = task_info

        with caplog.at_level(logging.WARNING):
            result = await btm.cancel_stale_workflow("thread-1", timeout=0.05)

        assert result is True
        assert "did not exit within" in caplog.text


# ---------------------------------------------------------------------------
# cancel_stale_workflow
# ---------------------------------------------------------------------------

class TestCancelStaleWorkflow:
    @pytest.mark.asyncio
    async def test_stale_run_cancelled(self):
        btm = _make_btm()
        stale = _make_task_info(status=LocalRunStatus.RUNNING, run_id="run-stale")
        btm.executions[("thread-1", "run-stale")] = stale

        result = await btm.cancel_stale_workflow("thread-1")

        assert result is True
        assert stale.cancel_event.is_set()


# ---------------------------------------------------------------------------
# consume_workflow uses closure-captured events
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# signal_cancel — run_id targeting (active vs latest, explicit vs implicit)
# ---------------------------------------------------------------------------

class TestCancelWorkflowRunIdTargeting:
    """``signal_cancel(thread_id)`` without an explicit run_id must target
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
            status=LocalRunStatus.RUNNING, run_id="run-older"
        )
        older_running.created_at = datetime(2024, 1, 1, 12, 0, 0)

        # Newer COMPLETED task (created later)
        newer_completed = _make_task_info(
            status=LocalRunStatus.COMPLETED, run_id="run-newer"
        )
        newer_completed.created_at = datetime(2024, 1, 1, 12, 5, 0)

        btm.executions[("thread-1", "run-older")] = older_running
        btm.executions[("thread-1", "run-newer")] = newer_completed

        result = await btm.signal_cancel("thread-1")

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

        for status in (LocalRunStatus.COMPLETED, LocalRunStatus.FAILED, LocalRunStatus.CANCELLED):
            ti = _make_task_info(status=status, run_id=f"run-{status.value}")
            btm.executions[("thread-1", ti.run_id)] = ti

        result = await btm.signal_cancel("thread-1")

        assert result is False
        for ti in btm.executions.values():
            assert not ti.cancel_event.is_set()
            assert ti.explicit_cancel is False

    @pytest.mark.asyncio
    async def test_explicit_run_id_targets_that_run_even_when_older(self):
        """A caller that passes a specific run_id wants THAT run cancelled,
        not "the most recent thing on the thread"."""
        btm = _make_btm()

        target = _make_task_info(status=LocalRunStatus.RUNNING, run_id="run-target")
        target.created_at = datetime(2024, 1, 1, 12, 0, 0)

        more_recent = _make_task_info(status=LocalRunStatus.RUNNING, run_id="run-newer")
        more_recent.created_at = datetime(2024, 1, 1, 12, 10, 0)

        btm.executions[("thread-1", "run-target")] = target
        btm.executions[("thread-1", "run-newer")] = more_recent

        result = await btm.signal_cancel("thread-1", run_id="run-target")

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
        lookup from self.executions.
        """
        btm = _make_btm()

        async def fake_workflow():
            """Async generator that yields events with a small delay."""
            for i in range(20):
                await asyncio.sleep(0.01)
                yield f"event-{i}"

        cancel_event = asyncio.Event()

        # Pre-register a RUNNING task so _run_workflow can find it
        task_info = _make_task_info(thread_id="thread-closure", status=LocalRunStatus.RUNNING)
        btm.executions[("thread-closure", "run-1")] = task_info

        # Patch the terminal finalize + sentinel so they don't try to do real
        # persistence work. v4 folded _mark_completed/_cancelled/_failed into a
        # single _finalize_run(kind=...); the kind reveals which terminal path ran.
        with patch.object(btm, "_finalize_run", new_callable=AsyncMock) as mock_finalize, \
             patch.object(btm, "append_run_end_event", new_callable=AsyncMock), \
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
        # The closure observed the cancel_event: the cancellation finalize ran
        # (and only it). Without this the test false-passes even if
        # _run_workflow ignored the event and ran to completion (kind="stream_end").
        mock_finalize.assert_awaited_once_with(
            "thread-closure", "run-1", kind="cancelled"
        )
        # The inner task was registered on the task_info.
        assert task_info.inner_task is not None


# ---------------------------------------------------------------------------
# Force-cancel propagates from outer task into inner consume_workflow
# ---------------------------------------------------------------------------

class TestOuterTaskCancelPropagatesToInner:

    @pytest.mark.asyncio
    async def test_outer_task_cancel_propagates_to_inner(self):
        """Cancelling the outer task that wraps _run_workflow now cancels the
        inner consume_workflow task and runs the cancellation finalize.

        Pinpoints the behavior change from removing ``asyncio.shield(inner_task)``:
        shutdown's force-cancel path (executor.py:367) and
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
            thread_id="thread-outer", run_id="run-outer", status=LocalRunStatus.RUNNING
        )
        btm.executions[("thread-outer", "run-outer")] = task_info

        with patch.object(btm, "_finalize_run", new_callable=AsyncMock) as mock_finalize, \
             patch.object(btm, "append_run_end_event", new_callable=AsyncMock), \
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
        # The cancellation finalize ran inside the except handler.
        mock_finalize.assert_awaited_once_with(
            "thread-outer", "run-outer", kind="cancelled"
        )
        # The workflow generator's finally block ran — no orphaned generator.
        assert generator_closed.is_set()


# ---------------------------------------------------------------------------
# _mark_cancelled labels persistence by cancel origin (explicit_cancel)
# ---------------------------------------------------------------------------

class TestMarkCancelledUserLabeling:
    """``cancelled_by_user`` must reflect ``task_info.user_stop``, NOT
    ``explicit_cancel``.

    A user pressing Stop (HTTP /cancel) sets both explicit_cancel AND user_stop.
    System cancels — graceful shutdown (signal_cancel with user_initiated=
    False) and stale-sandbox recovery (cancel_stale_workflow) — also set
    explicit_cancel (to gate flush+teardown) but leave user_stop False, so they
    must persist cancelled_by_user=False. Keying off explicit_cancel would
    mislabel a pod-roll or workspace eviction as a user "Stopped" turn.

    v4: the labeling lives in the single ``_finalize_run`` (kind="cancelled");
    the terminal write is ``RunCoordinator.finalize_run(run_handle, outcome)``,
    so the ``cancelled_by_user`` flag is read off the ``RunOutcome.metadata``.
    """

    async def _run_finalize_cancelled(self, btm, task_info):
        """Drive _finalize_run(kind="cancelled") and return the outcome metadata."""
        run_handle = MagicMock()
        task_info.metadata = {
            "workspace_id": "ws-1",
            "user_id": "user-1",
            "run_handle": run_handle,
        }
        btm.executions[(task_info.thread_id, task_info.run_id)] = task_info

        coordinator = MagicMock()
        coordinator.finalize_run = AsyncMock()

        fin = "src.server.services.runs.finalization"
        mod = "src.server.services.runs.executor"
        with patch(f"{fin}.get_token_usage_from_callback", return_value=(None, [])), \
             patch(f"{fin}.get_tool_usage_from_handler", return_value={}), \
             patch(f"{fin}.calculate_execution_time", return_value=1.0), \
             patch(f"{mod}.release_burst_slot", new_callable=AsyncMock), \
             patch("src.server.services.runs.coordinator.RunCoordinator") as mock_coord_cls:
            mock_coord_cls.get_instance.return_value = coordinator
            await btm._finalize_run(task_info.thread_id, task_info.run_id, kind="cancelled")

        coordinator.finalize_run.assert_awaited_once()
        outcome = coordinator.finalize_run.await_args.args[1]
        return outcome.metadata

    @pytest.mark.asyncio
    async def test_abandoned_cancel_persists_not_user(self):
        """Bare force-cancel (abandoned cleanup): neither flag → not user."""
        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-sys", run_id="run-sys")
        assert task_info.explicit_cancel is False
        assert task_info.user_stop is False

        persist_metadata = await self._run_finalize_cancelled(btm, task_info)

        assert persist_metadata["cancelled_by_user"] is False

    @pytest.mark.asyncio
    async def test_user_cancel_persists_user(self):
        """user_stop set (HTTP /cancel) → cancelled_by_user=True."""
        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-usr", run_id="run-usr")
        task_info.explicit_cancel = True
        task_info.user_stop = True

        persist_metadata = await self._run_finalize_cancelled(btm, task_info)

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

        persist_metadata = await self._run_finalize_cancelled(btm, task_info)

        assert persist_metadata["cancelled_by_user"] is False


def _model_call_error(status_code):
    """A model call's exception as ModelResilienceMiddleware leaves it."""
    exc = RuntimeError("Error code: %d" % status_code)
    exc.__model_resilience__ = {
        "model": "primary",
        "attempted_models": [
            {"model": "primary", "error": "rejected", "status_code": status_code, "attempts": 1},
            {"model": "fallback", "error": "down", "status_code": 503, "attempts": 1},
        ],
    }
    return exc


class TestFailedRunRecordsItsModelCall:
    """A run that failed on its model call records the status the primary
    earned and whether the credential that made the call was the user's, so
    an automation can tell a rejected key of theirs from one of ours."""

    async def _finalize_failed(self, exc, credential_source):
        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-fail", run_id="run-fail")
        handler = MagicMock()
        handler.agent_config.credential_source = credential_source
        task_info.metadata = {
            "workspace_id": "ws-1",
            "user_id": "user-1",
            "run_handle": MagicMock(),
            "handler": handler,
        }
        btm.executions[(task_info.thread_id, task_info.run_id)] = task_info

        coordinator = MagicMock()
        coordinator.finalize_run = AsyncMock()
        fin = "src.server.services.runs.finalization"
        mod = "src.server.services.runs.executor"
        with patch(f"{fin}.get_token_usage_from_callback", return_value=(None, [])), \
             patch(f"{fin}.get_tool_usage_from_handler", return_value={}), \
             patch(f"{fin}.calculate_execution_time", return_value=1.0), \
             patch(f"{mod}.release_burst_slot", new_callable=AsyncMock), \
             patch("src.server.services.runs.coordinator.RunCoordinator") as coord_cls:
            coord_cls.get_instance.return_value = coordinator
            await btm._finalize_run(
                task_info.thread_id, task_info.run_id,
                kind="failed", error=str(exc), exc=exc,
            )
        return coordinator.finalize_run.await_args.args[1].metadata

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "credential_source, owned",
        [("byok", True), ("oauth", True), ("platform", False)],
    )
    async def test_a_rejected_model_call_names_whose_key(
        self, monkeypatch, credential_source, owned
    ):
        monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
        metadata = await self._finalize_failed(_model_call_error(401), credential_source)
        assert metadata["error_status_code"] == 401
        assert metadata["error_credential_owned"] is owned

    @pytest.mark.asyncio
    async def test_a_failure_off_the_model_call_records_nothing(self):
        # A tool's HTTP 401 carries no resilience trace.
        exc = RuntimeError("Client error '401 Unauthorized'")
        exc.status_code = 401
        metadata = await self._finalize_failed(exc, "byok")
        assert "error_status_code" not in metadata
        assert "error_credential_owned" not in metadata


class TestThrownFinalizeLeavesEntryRunning:
    """REGRESSION (F4): a THROWN finalize CAS must not stamp the local entry
    terminal. The durable row is still in_progress — stamping COMPLETED
    locally (and dropping the run_handle) would strand it: cleanup's
    dead-handle path only fail_opens entries that are still RUNNING with a
    handle attached."""

    def _wire(self, btm, task_info, *, finalize_side_effect):
        run_handle = MagicMock()
        run_handle.guard = None
        task_info.metadata = {
            "workspace_id": "ws-1",
            "user_id": "user-1",
            "run_handle": run_handle,
        }
        btm.executions[(task_info.thread_id, task_info.run_id)] = task_info

        coordinator = MagicMock()
        coordinator.finalize_run = AsyncMock(side_effect=finalize_side_effect)
        return run_handle, coordinator

    async def _run_finalize(self, btm, task_info, coordinator):
        fin = "src.server.services.runs.finalization"
        mod = "src.server.services.runs.executor"
        with patch(f"{fin}.get_token_usage_from_callback", return_value=(None, [])), \
             patch(f"{fin}.get_tool_usage_from_handler", return_value={}), \
             patch(f"{fin}.calculate_execution_time", return_value=1.0), \
             patch(f"{mod}.release_burst_slot", new_callable=AsyncMock), \
             patch("src.server.services.runs.coordinator.RunCoordinator") as coord_cls:
            coord_cls.get_instance.return_value = coordinator
            await btm._finalize_run(
                task_info.thread_id, task_info.run_id, kind="stream_end"
            )

    @pytest.mark.asyncio
    async def test_thrown_cas_keeps_running_status_and_handle(self):
        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-cas", run_id="run-cas")
        run_handle, coordinator = self._wire(
            btm, task_info, finalize_side_effect=RuntimeError("db down mid-CAS")
        )

        await self._run_finalize(btm, task_info, coordinator)

        # Row is still in_progress: the local mirror must not claim terminal.
        assert task_info.status == LocalRunStatus.RUNNING
        assert task_info.completed_at is None
        # The handle survives for cleanup's dead-handle fail_open path.
        assert task_info.metadata.get("run_handle") is run_handle
        # Waiters are NOT released on a thrown CAS: the row is unfinalized,
        # so they must hit their timeout instead of reading it as done.
        assert not task_info.persistence_complete.is_set()

    @pytest.mark.asyncio
    async def test_applied_cas_still_marks_terminal(self):
        from types import SimpleNamespace

        btm = _make_btm()
        task_info = _make_task_info(thread_id="thread-ok", run_id="run-ok")
        _, coordinator = self._wire(btm, task_info, finalize_side_effect=None)
        coordinator.finalize_run.side_effect = None
        coordinator.finalize_run.return_value = SimpleNamespace(
            applied=True, run={"status": "completed"}
        )

        await self._run_finalize(btm, task_info, coordinator)

        assert task_info.status == LocalRunStatus.COMPLETED
        assert task_info.metadata.get("run_handle") is None


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
            thread_id="t-stop", run_id="r-stop", status=LocalRunStatus.RUNNING
        )
        task_info.explicit_cancel = explicit
        btm.executions[("t-stop", "r-stop")] = task_info

        with patch.object(btm, "_finalize_run", new_callable=AsyncMock), \
             patch.object(btm, "append_run_end_event", new_callable=AsyncMock), \
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
        # The teardown fills the result the run already holds, so a finalize
        # that starts before the teardown is done still reads its claim.
        stop = btm.executions[("t-stop", "r-stop")].stop_teardown
        teardown.assert_awaited_once_with("t-stop", "r-stop", stop)

    @pytest.mark.asyncio
    async def test_system_cancel_does_not_flush_or_teardown(self):
        btm = _make_btm()
        flush, teardown = await self._drive_stop(btm, explicit=False)
        flush.assert_not_awaited()
        teardown.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flush_failure_still_marks_cancelled(self):
        """A raising _flush_checkpoint must not prevent the cancellation finalize."""
        btm = _make_btm()

        async def fake_workflow():
            for i in range(1000):
                await asyncio.sleep(0.01)
                yield f"event-{i}"

        cancel_event = asyncio.Event()
        task_info = _make_task_info(
            thread_id="t-flushfail", run_id="r-flushfail", status=LocalRunStatus.RUNNING
        )
        task_info.explicit_cancel = True
        btm.executions[("t-flushfail", "r-flushfail")] = task_info

        with patch.object(btm, "_finalize_run", new_callable=AsyncMock) as finalize, \
             patch.object(btm, "append_run_end_event", new_callable=AsyncMock), \
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

        finalize.assert_awaited_once_with(
            "t-flushfail", "r-flushfail", kind="cancelled"
        )

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
            thread_id="t-recancel", run_id="r-recancel", status=LocalRunStatus.RUNNING
        )
        task_info.explicit_cancel = True
        btm.executions[("t-recancel", "r-recancel")] = task_info

        with patch.object(btm, "_finalize_run", new_callable=AsyncMock) as finalize, \
             patch.object(btm, "append_run_end_event", new_callable=AsyncMock), \
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
        finalize.assert_awaited_once_with(
            "t-recancel", "r-recancel", kind="cancelled"
        )


# ---------------------------------------------------------------------------
# The graph's stream is closed before the executor flushes or finalizes
# ---------------------------------------------------------------------------

class _InFlightGraph:
    """LangGraph stand-in: a model call in flight, cancelled and awaited when
    its stream closes, as LangGraph's background executor does."""

    def __init__(self):
        self.call: asyncio.Task | None = None
        self.closed = False

    def astream(self, _input_state, config=None, **_kwargs):
        async def _gen():
            self.call = asyncio.ensure_future(asyncio.sleep(30))
            try:
                while True:
                    yield ((), "custom", {"type": "context_window", "action": "offload"})
                    await asyncio.sleep(0.01)
            finally:
                self.call.cancel()
                await asyncio.gather(self.call, return_exceptions=True)
                self.closed = True

        return _gen()


def _ptc_workflow(graph: _InFlightGraph):
    """A PTC turn's stream as ptc_run composes it, over the real orchestrator."""
    from ptc_agent.agent.middleware.background_subagent.orchestrator import (
        BackgroundSubagentOrchestrator,
    )
    from ptc_agent.agent.middleware.credit_gate import run_with_credit_gate
    from ptc_agent.core.project_context import ProjectContext, run_with_project
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from src.server.services.runs.sse_producer import RunSSEProducer

    producer = RunSSEProducer(thread_id="t-close", run_id="r-close")
    return run_with_project(
        ProjectContext(workspace_id="ws-close", dir_name="close"),
        run_with_credit_gate(
            None,
            DirectMCPBinding(user_id=None).drive(
                producer.stream_workflow(
                    BackgroundSubagentOrchestrator(graph, MagicMock()),
                    {},
                    {"configurable": {}},
                )
            ),
        ),
    )


class TestGraphClosedBeforeTheExecutorGoesOn:
    """Each way out of ``consume_workflow`` closes the turn's stream through
    every wrapper before the flush or finalize: work still running then could
    finish after its usage was read."""

    async def _drive(self, *, on_third_write, stop=None):
        from src.server.services.runs.stream_writer import TransportLostError

        btm = _make_btm()
        btm.enable_storage = True
        graph = _InFlightGraph()
        key = ("t-close", "r-close")
        task_info = _make_task_info(thread_id=key[0], run_id=key[1])
        btm.executions[key] = task_info
        seen: dict[str, bool] = {}
        writes = 0
        in_write = asyncio.Event()

        async def buffer(_thread_id, _run_id, _event):
            nonlocal writes
            writes += 1
            if writes == 3:
                if on_third_write == "lose transport":
                    raise TransportLostError("stream gone")
                if on_third_write == "flag a stop":
                    # Seen at the next event, with no task cancel behind it.
                    task_info.explicit_cancel = True
                    task_info.cancel_event.set()
                    return
                in_write.set()
                await asyncio.sleep(30)

        async def flush(*_args):
            seen["flush"] = graph.closed

        async def finalize(*_args, kind, **_kwargs):
            seen[kind] = graph.closed

        with patch.object(btm, "_buffer_event_redis", side_effect=buffer), \
             patch.object(btm, "_flush_checkpoint", side_effect=flush), \
             patch.object(btm, "_finalize_run", side_effect=finalize), \
             patch.object(btm, "_teardown_subagents_on_stop", new_callable=AsyncMock):
            outer = asyncio.create_task(
                btm._run_workflow(
                    key[0], key[1], _ptc_workflow(graph),
                    cancel_event=task_info.cancel_event,
                )
            )
            if stop is not None:
                await in_write.wait()
                task_info.explicit_cancel = True
                task_info.cancel_event.set()
                stop(task_info)
            with suppress(asyncio.CancelledError):
                await outer
        assert graph.call.cancelled()
        return seen

    @pytest.mark.asyncio
    async def test_a_lost_transport_closes_the_graph_before_the_finalize(self):
        seen = await self._drive(on_third_write="lose transport")
        assert seen == {"failed": True}

    @pytest.mark.asyncio
    async def test_a_stop_during_a_write_closes_the_graph_before_the_flush(self):
        seen = await self._drive(
            on_third_write="hang", stop=lambda info: info.inner_task.cancel()
        )
        assert seen == {"flush": True, "cancelled": True}

    @pytest.mark.asyncio
    async def test_a_cooperative_cancel_closes_the_graph_before_the_flush(self):
        seen = await self._drive(on_third_write="flag a stop")
        assert seen == {"flush": True, "cancelled": True}


# ---------------------------------------------------------------------------
# Single-owner teardown ordering (decision 1A): drain BEFORE cancel_run_tasks,
# everything scoped to the stopped run.
# ---------------------------------------------------------------------------

class TestStopTeardownOrdering:

    @pytest.mark.asyncio
    async def test_cancel_run_tasks_runs_before_drain(self):
        """_teardown_subagents_on_stop kills the run's tasks BEFORE draining:
        the drain's high-water is read at drain start, so a pre-kill snapshot
        would miss frames emitted between snapshot and kill — output the live
        stream already delivered. The task list is snapshotted pre-kill (the
        kill drops registry entries) and only the stopped run's tasks drain."""
        btm = _make_btm()

        order: list[str] = []

        task_info = _make_task_info(
            thread_id="t-order", run_id="r-order", status=LocalRunStatus.RUNNING
        )
        btm.executions[("t-order", "r-order")] = task_info

        own_task = MagicMock(spawned_run_id="r-order")
        foreign_task = MagicMock(spawned_run_id="r-prior")
        fake_registry = MagicMock()
        fake_registry.get_all_tasks = AsyncMock(
            return_value=[own_task, foreign_task]
        )

        drained_tasks: list = []

        async def fake_drain(thread_id, tasks):
            order.append("drain")
            drained_tasks.extend(tasks)
            return [{"event": "message_chunk", "data": {"agent": "task:x"}}]

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=fake_registry)

        async def fake_cancel_run_tasks(thread_id, run_id, *, force, claim_for):
            order.append("cancel_run_tasks")
            return RunTasksKilled(cancelled=1, claimed=[own_task])

        fake_store.cancel_run_tasks = AsyncMock(side_effect=fake_cancel_run_tasks)

        with patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ), patch.object(btm, "_drain_killed_subagent_events", side_effect=fake_drain):
            stop = StopTeardown()
            await btm._teardown_subagents_on_stop("t-order", "r-order", stop)

        assert order == ["cancel_run_tasks", "drain"]
        # Prior-turn tasks are excluded: their events belong to their own
        # response, not the stopped one.
        assert drained_tasks == [own_task]
        fake_store.cancel_run_tasks.assert_awaited_once_with(
            "t-order", "r-order", force=True, claim_for="r-order"
        )
        assert stop.events and stop.events[0]["data"]["agent"] == "task:x"
        assert stop.claimed == [own_task]

    @pytest.mark.asyncio
    async def test_drain_timeout_proceeds_without_events(self):
        """A drain that exceeds stop_drain_timeout doesn't block teardown."""
        btm = _make_btm()

        task_info = _make_task_info(
            thread_id="t-tmo", run_id="r-tmo", status=LocalRunStatus.RUNNING
        )
        btm.executions[("t-tmo", "r-tmo")] = task_info

        fake_registry = MagicMock()
        fake_registry.get_all_tasks = AsyncMock(
            return_value=[MagicMock(spawned_run_id="r-tmo")]
        )

        async def slow_drain(thread_id, tasks):
            await asyncio.sleep(5)
            return [{"event": "x"}]

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=fake_registry)
        fake_store.cancel_run_tasks = AsyncMock(
            return_value=RunTasksKilled(cancelled=1)
        )

        with patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ), patch.object(btm, "_drain_killed_subagent_events", side_effect=slow_drain), \
           patch(
               "src.server.services.runs.teardown.get_stop_drain_timeout",
               return_value=0.05,
           ):
            stop = StopTeardown()
            await btm._teardown_subagents_on_stop("t-tmo", "r-tmo", stop)

        # No drained events kept, but cancel_run_tasks still ran.
        assert stop.events == []
        fake_store.cancel_run_tasks.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_orphan_collectors_cancelled_on_stop(self):
        """The stopped run's orphan collector is cancelled during teardown so
        it can't mutate the response; a prior turn's collector — which owns a
        different response — survives."""
        btm = _make_btm()

        task_info = _make_task_info(
            thread_id="t-orph", run_id="r-orph", status=LocalRunStatus.RUNNING
        )
        btm.executions[("t-orph", "r-orph")] = task_info

        started = asyncio.Event()
        prior_started = asyncio.Event()

        async def long_collector(event):
            event.set()
            await asyncio.sleep(100)

        collector = asyncio.create_task(long_collector(started))
        prior_collector = asyncio.create_task(long_collector(prior_started))
        btm._track_orphan_collector("t-orph", "r-orph", collector)
        btm._track_orphan_collector("t-orph", "r-prior", prior_collector)
        await started.wait()
        await prior_started.wait()

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=None)
        fake_store.cancel_run_tasks = AsyncMock(
            return_value=RunTasksKilled(cancelled=0)
        )

        with patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ):
            await btm._teardown_subagents_on_stop("t-orph", "r-orph", StopTeardown())

        await asyncio.sleep(0)  # let done-callbacks run
        assert collector.cancelled()
        assert not prior_collector.done()
        assert btm._orphan_collectors.get("t-orph") == {prior_collector: "r-prior"}

        prior_collector.cancel()
        with suppress(asyncio.CancelledError):
            await prior_collector

    @pytest.mark.asyncio
    async def test_orphan_collector_bucket_cleared_on_natural_completion(self):
        """A collector that finishes without a stop drops its empty bucket — no
        unbounded empty-set leak on long-lived servers."""
        btm = _make_btm()

        async def quick_collector():
            return None

        collector = asyncio.create_task(quick_collector())
        btm._track_orphan_collector("t-nat", "r-nat", collector)
        assert "t-nat" in btm._orphan_collectors  # tracked while running

        await collector
        await asyncio.sleep(0)  # let the done-callback run

        assert "t-nat" not in btm._orphan_collectors


# ---------------------------------------------------------------------------
# The stop drain takes a lane whole or not at all
# ---------------------------------------------------------------------------

class TestStopDrainSnapshot:

    def _task(self, task_id: str, count: int) -> BackgroundTask:
        # No writer handles: the drain withholds tasks whose writers are alive.
        task = BackgroundTask(
            tool_call_id=f"tc-{task_id}",
            task_id=task_id,
            description="d",
            prompt="p",
            subagent_type="general-purpose",
            spawned_run_id="run-1",
        )
        task.captured_event_count = count
        return task

    @pytest.mark.asyncio
    async def test_no_recovered_events_carry_nothing(self):
        """A task with captured events whose XRANGE recovered nothing is
        withheld; its capture stream stays for the archive."""
        btm = _make_btm()

        async def empty_iter(thread_id, task):
            return
            yield  # pragma: no cover

        with patch(
            "src.server.services.runs.teardown.iter_subagent_events_full",
            side_effect=empty_iter,
        ):
            merged = await btm._drain_killed_subagent_events(
                "t-x", [self._task("abc", 3)]
            )

        assert merged == []

    @pytest.mark.asyncio
    async def test_partial_recovery_withholds_snapshot(self):
        """A truncated read (fewer records than attempted appends — torn
        spill prefix, mid-stream trim) is as unsafe as zero: recording the
        prefix would record part of the lane as all of it. The whole task
        is withheld."""
        btm = _make_btm()
        records = [
            {"event": "message_chunk", "data": {
                "agent": "task:abc", "id": "m1",
                "content": "partial", "content_type": "text"},
             "run": "run-1"},
        ]

        async def fake_iter(thread_id, task):
            for r in records:
                yield r

        with patch(
            "src.server.services.runs.teardown.iter_subagent_events_full",
            side_effect=fake_iter,
        ):
            merged = await btm._drain_killed_subagent_events(
                "t-x", [self._task("abc", 3)]
            )

        assert merged == []

    @pytest.mark.asyncio
    async def test_live_writer_withholds_snapshot(self):
        """A writer still unwinding past the bounded wait can append after
        this drain reads its count (the terminal steering sweep is exempt
        from the seal) — only a settled writer guarantees the count is
        final, so the task is withheld entirely."""
        btm = _make_btm()
        task = self._task("abc", 1)
        live_writer = MagicMock()
        live_writer.done.return_value = False
        task.handler_task = live_writer

        async def fake_iter(thread_id, t):
            yield {"event": "message_chunk", "data": {
                "agent": "task:abc", "id": "m1",
                "content": "x", "content_type": "text"}}

        with patch(
            "src.server.services.runs.teardown.iter_subagent_events_full",
            side_effect=fake_iter,
        ):
            merged = await btm._drain_killed_subagent_events("t-x", [task])

        assert merged == []

    @pytest.mark.asyncio
    async def test_foreign_epoch_records_are_withheld(self):
        """Records stamped with another round's run id (cross-worker resume
        reset the shared stream) neither count toward completeness nor
        archive under this round."""
        btm = _make_btm()
        records = [
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "id": "m1",
                         "content": "r2", "content_type": "text"},
                "run": "run-2",
            },
        ]

        async def fake_iter(thread_id, t):
            for r in records:
                yield r

        with patch(
            "src.server.services.runs.teardown.iter_subagent_events_full",
            side_effect=fake_iter,
        ):
            merged = await btm._drain_killed_subagent_events(
                "t-x", [self._task("abc", 1)]
            )

        assert merged == []


class TestReplayOwnedTaskEvents:

    def _task(self, count: int, response_id: str = "resp-1") -> MagicMock:
        task = MagicMock()
        task.task_id = "abc"
        task.captured_event_count = count
        task.collector_response_id = response_id
        task.spawned_run_id = response_id
        return task

    def _iter(self, records: list[dict]):
        async def fake_iter(thread_id, task):
            for r in records:
                yield r

        return fake_iter

    @pytest.mark.asyncio
    async def test_complete_recovery_appends_and_succeeds(self):
        records = [
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "content": "a"},
                "run": "resp-1",
            },
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "content": "b"},
                "run": "resp-1",
            },
        ]
        out: list = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=self._iter(records),
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "t-x", self._task(2), "resp-1", out
            )
        assert ok is True
        assert len(out) == 2

    @pytest.mark.asyncio
    async def test_incomplete_recovery_withholds_and_fails(self):
        """A stream yielding fewer records than the attempted appends (XRANGE
        failure reads as zero rows; a torn spill leaves a prefix) appends
        NOTHING — a partial archive would clear the replay cache gate and
        freeze an incomplete transcript — and returns False so cleanup
        retains the streams."""
        records = [
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "content": "a"},
                "run": "resp-1",
            },
        ]
        out: list = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=self._iter(records),
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "t-x", self._task(3), "resp-1", out
            )
        assert ok is False
        assert out == []

    @pytest.mark.asyncio
    async def test_zero_recovery_with_captures_fails(self):
        """The zero-row XRANGE failure mode: captured events exist but the
        iterator yields nothing — must NOT read as a successful no-op
        (persist would be skipped and cleanup would retire the only copy)."""
        out: list = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=self._iter([]),
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "t-x", self._task(3), "resp-1", out
            )
        assert ok is False
        assert out == []

    @pytest.mark.asyncio
    async def test_foreign_epoch_records_do_not_count(self):
        """Cross-worker resume: worker B reset the shared stream and wrote
        round-2 records; the stale round-1 collector reads exactly
        ``expected`` records, but all are foreign-stamped. They must not pad
        the completeness tally — round 1's capture is gone and its streams
        must be retained, not retired as safely archived."""
        records = [
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "content": "r2-a"},
                "run": "run-2",
            },
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "content": "r2-b"},
                "run": "run-2",
            },
        ]
        out: list = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=self._iter(records),
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "t-x", self._task(2), "resp-1", out
            )
        assert ok is False
        assert out == []

    @pytest.mark.asyncio
    async def test_surplus_eligible_records_fail_strict_equality(self):
        """More own-round records than the entry snapshot (a terminal append
        landing mid-replay) also withholds — the snapshot the caller is
        about to vouch for no longer matches what the writer produced."""
        records = [
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "content": "a"},
                "run": "resp-1",
            },
            {
                "event": "message_chunk",
                "data": {"agent": "task:abc", "content": "b"},
                "run": "resp-1",
            },
        ]
        out: list = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=self._iter(records),
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "t-x", self._task(1), "resp-1", out
            )
        assert ok is False
        assert out == []

    @pytest.mark.asyncio
    async def test_unstamped_records_rejected_for_stamped_task(self):
        """A run-stamped task's own writer stamps every record (the run id is
        set at registration, before any append) — an unstamped record on its
        stream can only be a foreign pre-stamp writer's (rolling-deploy
        resume). It must neither count nor archive, even when the counts
        happen to match."""
        records = [
            {"event": "message_chunk", "data": {"agent": "task:abc", "content": "a"}},
            {"event": "message_chunk", "data": {"agent": "task:abc", "content": "b"}},
        ]
        out: list = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=self._iter(records),
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "t-x", self._task(2), "resp-1", out
            )
        assert ok is False
        assert out == []

    @pytest.mark.asyncio
    async def test_unstamped_records_accepted_for_legacy_task(self):
        """A task with no spawned_run_id (pre-stamp writer) legitimately
        produces unstamped records — they still count and archive."""
        task = self._task(1)
        task.spawned_run_id = None
        records = [
            {"event": "message_chunk", "data": {"agent": "task:abc", "content": "a"}},
        ]
        out: list = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=self._iter(records),
        ):
            ok = await subagent_collection.replay_owned_task_events("t-x", task, "resp-1", out)
        assert ok is True
        assert len(out) == 1


class TestCleanupRetireSplit:

    def _task(self, response_id: str = "resp-1") -> MagicMock:
        task = MagicMock()
        task.task_id = "abc"
        task.tool_call_id = "tc-1"
        task.collector_response_id = response_id
        task.task_run_id = "run-1"
        task.redis_spill_lock = asyncio.Lock()
        event = asyncio.Event()
        event.set()
        task.sse_drain_complete = event
        return task

    async def _run(self, retire_streams: bool) -> tuple[AsyncMock, AsyncMock]:
        delete_mock = AsyncMock()
        registry = MagicMock()
        registry.remove_task_if_owned = AsyncMock()
        store = MagicMock()
        store.get_registry = AsyncMock(return_value=registry)
        with patch(
            "src.server.services.runs.subagent_collection.delete_task_keys_if_owned",
            new=delete_mock,
        ), patch(
            "src.server.services.runs.subagent_collection.get_cache_client",
            return_value=MagicMock(),
        ), patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=store,
        ):
            await subagent_collection.await_drain_and_cleanup_tasks(
                [self._task()], "t-x", "resp-1",
                timeout=0.01, retire_streams=retire_streams,
            )
        return delete_mock, registry.remove_task_if_owned

    @pytest.mark.asyncio
    async def test_persist_failure_still_evicts_registry_entry(self):
        """retire_streams=False (failed archive persist) keeps the Redis
        capture streams but STILL releases the registry entry — the
        in-memory entry is process-local and holding it recovers nothing;
        repeated failures on a long-lived thread would accumulate task
        objects without bound."""
        delete_keys, remove_entry = await self._run(retire_streams=False)
        delete_keys.assert_not_awaited()
        remove_entry.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_successful_persist_retires_streams(self):
        delete_keys, remove_entry = await self._run(retire_streams=True)
        delete_keys.assert_awaited_once()
        remove_entry.assert_awaited_once()


# ---------------------------------------------------------------------------
# wait_for_admission decisions (decision 2A)
# ---------------------------------------------------------------------------

# Ledger slot read behind wait_for_admission (v4 2.4c): the in_progress row
# decides admission, worker-agnostically. Patched at the source module —
# BTM resolves it as ``tl_db.get_active_run`` at call time.
_GET_ACTIVE_RUN = "src.server.database.runs.lifecycle.get_active_run"


def _live_row(run_id: str = "run-1") -> dict:
    return {"conversation_response_id": run_id, "cancel_requested_at": None}


def _stopping_row(run_id: str = "run-1") -> dict:
    return {
        "conversation_response_id": run_id,
        "cancel_requested_at": "2026-07-14T00:00:00+00:00",
    }


class TestWaitForAdmission:
    """Ledger-driven admission (v4 2.4c): the thread's in_progress row decides;
    the local registry is consulted only to await a stopping run's teardown
    when this worker hosts it."""

    @pytest.mark.asyncio
    async def test_no_slot_row_is_fresh(self):
        btm = _make_btm()
        with patch(_GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=None):
            assert await btm.wait_for_admission("t-none") == ("fresh", None)

    @pytest.mark.asyncio
    async def test_live_row_is_running_with_row(self):
        """A live row means running regardless of which worker hosts the run;
        the row rides back so the caller can run-stamp its steer."""
        btm = _make_btm()
        row = _live_row()
        with patch(_GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=row):
            assert await btm.wait_for_admission("t-run") == ("running", row)

    @pytest.mark.asyncio
    async def test_stopping_local_teardown_within_wait_is_fresh(self):
        """Stopping row hosted by THIS worker: the local task finishes winding
        down within the wait and the slot clears → fresh; the task's
        CancelledError never reaches the caller."""
        btm = _make_btm()

        async def dies():
            raise asyncio.CancelledError()

        task = asyncio.ensure_future(dies())
        with suppress(asyncio.CancelledError):
            await asyncio.sleep(0)  # let it schedule
        ti = _make_task_info(thread_id="t-stop", status=LocalRunStatus.RUNNING, task=task)
        btm.executions[("t-stop", ti.run_id)] = ti

        with patch(
            _GET_ACTIVE_RUN,
            new_callable=AsyncMock,
            side_effect=[_stopping_row(ti.run_id), None],
        ):
            assert await btm.wait_for_admission("t-stop") == ("fresh", None)

    @pytest.mark.asyncio
    async def test_stopping_still_winding_down_is_stopping(self):
        """Teardown outlives the wait → 409 'stopping' (never start a second
        writer while the checkpoint flush may still be running)."""
        btm = _make_btm()

        never = asyncio.get_event_loop().create_future()
        ti = _make_task_info(thread_id="t-slow", status=LocalRunStatus.RUNNING, task=never)
        btm.executions[("t-slow", ti.run_id)] = ti
        row = _stopping_row(ti.run_id)

        with patch(
            "src.server.services.runs.admission.get_checkpoint_flush_timeout",
            return_value=0.01,
        ), patch(_GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=row):
            assert await btm.wait_for_admission("t-slow") == ("stopping", row)

    @pytest.mark.asyncio
    async def test_stopping_foreign_run_polls_slot_until_cleared(self):
        """No local task (the run lives on a peer): the wait polls the slot;
        the peer's finalize clearing it within the window → fresh."""
        btm = _make_btm()
        with patch(
            _GET_ACTIVE_RUN,
            new_callable=AsyncMock,
            side_effect=[_stopping_row(), None, None],
        ):
            assert await btm.wait_for_admission("t-peer") == ("fresh", None)

    @pytest.mark.asyncio
    async def test_new_run_raced_in_while_stopping_drained(self):
        """The stopped run drained and a NEW run took the slot during the
        wait → running with the new row, so the caller steers the winner."""
        btm = _make_btm()
        new_row = _live_row("r-new")
        with patch(
            _GET_ACTIVE_RUN,
            new_callable=AsyncMock,
            side_effect=[_stopping_row("r-old"), None, new_row],
        ):
            assert await btm.wait_for_admission("t-race") == ("running", new_row)

    @pytest.mark.asyncio
    async def test_local_registry_not_the_decision_source(self):
        """A stale local entry is irrelevant — the ledger decides."""
        btm = _make_btm()
        ti = _make_task_info(thread_id="t-done", status=LocalRunStatus.COMPLETED)
        btm.executions[("t-done", ti.run_id)] = ti
        with patch(_GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=None):
            assert await btm.wait_for_admission("t-done") == ("fresh", None)


class TestStartWorkflowHandoffCancelIntent:
    """A /cancel in the START → start_run gap stamps durable intent on
    the run row but finds no local LocalRunExecution to signal (the QUEUED placeholder
    that used to fill this gap is gone — the in_progress row is the only
    pre-registration identity). start_run must re-read the row after
    registering the task and re-derive the local cancel signal, so the run
    tears down as a user cancel instead of running to completion."""

    @pytest.mark.asyncio
    async def test_durable_intent_found_at_handoff_signals_cancel(self):
        btm = _make_btm()
        blocker = asyncio.Event()

        async def gen():
            await blocker.wait()
            yield {}

        run_handle = MagicMock(run_id="run-gap", guard=None)
        row = {
            "conversation_response_id": "run-gap",
            "status": "in_progress",
            "cancel_requested_at": "2026-01-01T00:00:00Z",
        }
        with patch(
            "src.server.database.runs.lifecycle.get_run",
            new_callable=AsyncMock,
            return_value=row,
        ), patch.object(btm, "_finalize_run", new=AsyncMock()):
            ti = await btm.start_run(
                thread_id="t-gap",
                run_id="run-gap",
                workflow_generator=gen(),
                metadata={"user_id": "u-1", "run_handle": run_handle},
            )
            try:
                assert ti.cancel_event.is_set()
                assert ti.user_stop is True
            finally:
                blocker.set()
                if ti.task and not ti.task.done():
                    with suppress(asyncio.CancelledError, asyncio.TimeoutError):
                        await asyncio.wait_for(ti.task, timeout=2)

    @pytest.mark.asyncio
    async def test_no_intent_leaves_run_unsignalled(self):
        """Negative case: an in_progress row without intent must not signal —
        the recheck is a cancel relay, not a liveness probe."""
        btm = _make_btm()

        async def gen():
            yield {}

        run_handle = MagicMock(run_id="run-clean", guard=None)
        row = {
            "conversation_response_id": "run-clean",
            "status": "in_progress",
            "cancel_requested_at": None,
        }
        with patch(
            "src.server.database.runs.lifecycle.get_run",
            new_callable=AsyncMock,
            return_value=row,
        ):
            ti = await btm.start_run(
                thread_id="t-clean",
                run_id="run-clean",
                workflow_generator=gen(),
                metadata={"user_id": "u-1", "run_handle": run_handle},
            )
        try:
            assert not ti.cancel_event.is_set()
            assert ti.status == LocalRunStatus.RUNNING
        finally:
            if ti.task and not ti.task.done():
                ti.task.cancel()
                with suppress(asyncio.CancelledError):
                    await ti.task


class TestHandoffUnderCallerCancellation:
    """The caller is a chat stream task that a client disconnect cancels at
    every await (an AnyIO scope). Once start_run has registered the run, the
    run is the executor's: a cancel landing on start_run's own awaits must not
    hand it back to the stream's death path, skip the session-loss abort, or
    lose a Stop that arrived before registration."""

    @staticmethod
    async def _start_cancelled(btm, run_handle, row, scope=None):
        async def gen():
            await asyncio.Event().wait()
            yield {}

        async def _get_run(_run_id):
            await asyncio.sleep(0)
            return row

        import anyio

        # Instance-level, so the workflow's own teardown in _stop stays mocked.
        btm._finalize_run = AsyncMock()
        with patch(
            "src.server.database.runs.lifecycle.get_run", side_effect=_get_run
        ):
            with anyio.CancelScope() as cancel:
                cancel.cancel()
                try:
                    await btm.start_run(
                        thread_id="t-1",
                        run_id="run-1",
                        workflow_generator=gen(),
                        metadata={"user_id": "u-1", "run_handle": run_handle},
                        on_registered=(
                            scope.transfer_to_executor if scope is not None else None
                        ),
                    )
                except asyncio.CancelledError:
                    # The chat handlers' death path.
                    if scope is not None and scope.slot_owned:
                        await scope.fail_open("client disconnected during setup")
                    raise
            for _ in range(5):
                await asyncio.sleep(0)
        return btm.executions[("t-1", "run-1")]

    @staticmethod
    async def _stop(ti):
        ti.task.cancel()
        with suppress(asyncio.CancelledError):
            await ti.task

    @pytest.mark.asyncio
    async def test_a_registered_run_is_not_settled_by_the_stream_death_path(self):
        from src.server.services.runs.admission import RunScope

        btm = _make_btm()
        run_handle = MagicMock(run_id="run-1", guard=None)
        scope = RunScope(user_id="u-1", burst_slot_id="s-1")
        scope.attach_run(run_handle)
        coordinator = MagicMock(fail_open_run=AsyncMock())
        row = {"status": "in_progress", "cancel_requested_at": None}

        with patch(
            "src.server.services.runs.coordinator.RunCoordinator.get_instance",
            return_value=coordinator,
        ), patch(
            "src.server.dependencies.usage_limits.release_burst_slot",
            new=AsyncMock(),
        ) as release:
            ti = await self._start_cancelled(btm, run_handle, row, scope)
        try:
            assert ti.task is not None and not ti.task.done()
            coordinator.fail_open_run.assert_not_awaited()
            release.assert_not_awaited()
            assert not scope.slot_owned
        finally:
            await self._stop(ti)

    @pytest.mark.asyncio
    async def test_a_stop_found_at_handoff_survives_the_callers_cancel(self):
        btm = _make_btm()
        run_handle = MagicMock(run_id="run-1", guard=None)
        row = {"status": "in_progress", "cancel_requested_at": "2026-01-01T00:00:00Z"}

        ti = await self._start_cancelled(btm, run_handle, row)
        try:
            assert ti.cancel_event.is_set()
            assert ti.user_stop is True
        finally:
            await self._stop(ti)

    @pytest.mark.asyncio
    async def test_session_loss_abort_is_wired_before_the_first_await(self):
        btm = _make_btm()
        run_handle = MagicMock(run_id="run-1")
        row = {"status": "in_progress", "cancel_requested_at": None}

        ti = await self._start_cancelled(btm, run_handle, row)
        try:
            run_handle.guard.attach_abort.assert_called_once()
        finally:
            await self._stop(ti)


# ---------------------------------------------------------------------------
# Compaction admission guard
# ---------------------------------------------------------------------------


class TestMutationAdmissionGuard:
    """wait_for_admission holds a new turn until any in-progress thread
    mutation (auto Tier-2 summarize window or manual /compact|/offload|delete)
    finishes — now delegated to ThreadMutationRunner (v4 2.4) so the hold
    covers ops owned by OTHER workers, not just this process."""

    RUNNER = (
        "src.server.services.thread_mutation.ThreadMutationRunner.get_instance"
    )

    def _runner(self, mutating: bool, idle_within_wait: bool = True):
        runner = MagicMock()
        runner.is_mutating = AsyncMock(return_value=mutating)
        runner.wait_until_idle = AsyncMock(return_value=idle_within_wait)
        return runner

    @pytest.mark.asyncio
    async def test_admission_waits_then_returns_running(self):
        """A running turn that is compacting → admission waits out the
        mutation, then the ledger scan returns 'running' so the caller
        steers."""
        btm = _make_btm()
        row = _live_row()
        runner = self._runner(mutating=True, idle_within_wait=True)

        with patch(self.RUNNER, return_value=runner), patch(
            _GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=row
        ):
            result = await btm.wait_for_admission("thread-1")

        assert result == ("running", row)
        runner.wait_until_idle.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_admission_waits_then_returns_fresh(self):
        """Manual mutation (no slot row) → once the runner reports idle the
        admission returns 'fresh' so a new turn starts."""
        btm = _make_btm()
        runner = self._runner(mutating=True, idle_within_wait=True)

        with patch(self.RUNNER, return_value=runner), patch(
            _GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=None
        ):
            result = await btm.wait_for_admission("thread-1")

        assert result == ("fresh", None)

    @pytest.mark.asyncio
    async def test_admission_timeout_returns_compacting(self):
        """Mutation still live past the wait window → 'compacting' (→ 409)."""
        btm = _make_btm()
        runner = self._runner(mutating=True, idle_within_wait=False)

        with patch(self.RUNNER, return_value=runner):
            result = await btm.wait_for_admission("thread-1")

        assert result == ("compacting", None)

    @pytest.mark.asyncio
    async def test_admission_wait_floored_at_compaction_timeout(self):
        """Admission must not 409 a healthy compaction before its call budget
        self-terminates: the wait passed to the runner is floored at
        compaction_timeout + margin even when the configured admission
        timeout is shorter."""
        btm = _make_btm()
        runner = self._runner(mutating=True, idle_within_wait=True)

        with patch(
            "src.server.services.runs.admission.COMPACTION_ADMISSION_MARGIN_S",
            1.0,
        ), patch(
            "src.server.services.runs.admission."
            "get_admission_compaction_wait_timeout",
            return_value=0.05,  # would 409 almost immediately WITHOUT the floor
        ), patch(
            "src.server.services.runs.admission.get_compaction_timeout",
            return_value=0.5,  # floor: 0.5 + 1.0 margin governs
        ), patch(self.RUNNER, return_value=runner), patch(
            _GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=None
        ):
            await btm.wait_for_admission("thread-1")

        runner.wait_until_idle.assert_awaited_once_with("thread-1", timeout=1.5)

    @pytest.mark.asyncio
    async def test_admission_composes_mutation_wait_then_stop_drain(self):
        """One admission call can lawfully consume BOTH waits in sequence —
        the compaction backstop, then the stop-drain when the freed slot
        carries cancel intent (a user cancel landing near a compaction
        window's close chains them). report_back leases.admission_hold_bound()
        SUMS the two bounds for exactly this path (Codex round-4 F1); this
        pin keeps the sequential control flow honest against that sum."""
        btm = _make_btm()
        runner = self._runner(mutating=True, idle_within_wait=True)
        ledger = AsyncMock(
            side_effect=[_stopping_row("r-stop"), None, None]
        )

        with patch(self.RUNNER, return_value=runner), patch(
            _GET_ACTIVE_RUN, ledger
        ):
            result = await btm.wait_for_admission("thread-1")

        assert result == ("fresh", None)
        runner.wait_until_idle.assert_awaited_once()  # compaction wait ran
        # slot read (stopping) → stop-drain poll → post-drain re-read: the
        # stopping wait ran IN THE SAME CALL after the mutation wait.
        assert ledger.await_count >= 3

    @pytest.mark.asyncio
    async def test_no_mutation_admits_normally(self):
        """No mutation in progress → admission falls straight through to the
        ledger scan without waiting on the runner."""
        btm = _make_btm()
        runner = self._runner(mutating=False)

        with patch(self.RUNNER, return_value=runner), patch(
            _GET_ACTIVE_RUN, new_callable=AsyncMock, return_value=None
        ):
            result = await btm.wait_for_admission("thread-1")

        assert result == ("fresh", None)
        runner.wait_until_idle.assert_not_awaited()


# ---------------------------------------------------------------------------
# _clear_report_back_watch — terminal runs clear the report-back watch
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Terminal run_end frame (RUN_END_EVENT_TYPE)
# ---------------------------------------------------------------------------

class _FakePipeline:
    """Captures xadd/expire calls issued inside ``async with pipeline()``."""

    def __init__(self, calls: list, fail: bool = False):
        self.calls = calls
        self.fail = fail

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def xadd(self, key, fields, **kwargs):
        self.calls.append(("xadd", key, fields))

    def expire(self, key, ttl):
        self.calls.append(("expire", key, ttl))

    async def execute(self):
        if self.fail:
            raise ConnectionError("redis down")
        return []


def _sentinel_cache(
    calls: list,
    fail: bool = False,
    gate_acquired: bool = True,
    gate_exists: bool = False,
) -> MagicMock:
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.exists = AsyncMock(
        side_effect=lambda *a: (
            calls.append(("exists",) + a) or (1 if gate_exists else 0)
        )
    )
    cache.client.xadd = AsyncMock(
        side_effect=lambda key, fields, **kw: calls.append(
            ("xadd", key, fields)
        )
    )
    cache.client.set = AsyncMock(
        side_effect=lambda *a, **kw: (
            calls.append(("set", a, kw)) or (True if gate_acquired else None)
        )
    )
    cache.client.delete = AsyncMock(
        side_effect=lambda *a: calls.append(("delete",) + a)
    )
    cache.client.pipeline = MagicMock(
        return_value=_FakePipeline(calls, fail=fail)
    )
    return cache


class TestRunEndCountsSettledSubagents:
    """A collected run's settled subagents are billed before its run_end, so
    the token totals the frame carries count them; the collector gets the
    claimed tasks and bills whatever settles later."""

    @staticmethod
    def _task(task_id, *, spawned_run_id="run-1", owner_task_id=None):
        return BackgroundTask(
            tool_call_id=f"tc-{task_id}",
            task_id=task_id,
            description="d",
            prompt="p",
            subagent_type="workflow" if task_id.startswith("wf") else "research",
            spawned_run_id=spawned_run_id,
            owner_task_id=owner_task_id,
        )

    @staticmethod
    async def _finished_writer():
        async def body():
            return {"success": True, "result": "ok"}

        writer = asyncio.create_task(body())
        await writer
        return writer

    @pytest.mark.asyncio
    async def test_bills_only_tasks_whose_usage_is_final(self):
        from ptc_agent.agent.middleware.background_subagent.registry import (
            BackgroundTaskRegistry,
        )

        gate = asyncio.Event()

        async def running():
            await gate.wait()
            return {"success": True, "result": "late"}

        registry = BackgroundTaskRegistry(thread_id="t-1")
        done = self._task("done")
        done.per_call_records = [{"usage": {"input_tokens": 5}}]
        done.asyncio_task = await self._finished_writer()
        live = self._task("live")
        live.asyncio_task = asyncio.create_task(running())
        # A running workflow's settled child: its driver still reads the
        # child's records, so billing must wait for the collector.
        wf_live = self._task("wf_live")
        wf_live.asyncio_task = asyncio.create_task(running())
        child_of_live = self._task("c_live", owner_task_id="wf_live")
        child_of_live.per_call_records = [{"usage": {"input_tokens": 7}}]
        child_of_live.asyncio_task = await self._finished_writer()
        # A settled workflow's child registered after the sweep's run.
        wf_done = self._task("wf_done")
        wf_done.captured_event_count = 1
        wf_done.asyncio_task = await self._finished_writer()
        child_of_done = self._task(
            "c_done", spawned_run_id="run-0", owner_task_id="wf_done"
        )
        child_of_done.per_call_records = [{"usage": {"input_tokens": 9}}]
        child_of_done.asyncio_task = await self._finished_writer()
        for t in (done, live, wf_live, child_of_live, wf_done, child_of_done):
            registry._tasks[t.tool_call_id] = t

        bg_store = MagicMock()
        bg_store.get_registry = AsyncMock(return_value=registry)
        with patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=bg_store,
        ), patch.object(
            subagent_collection, "persist_subagent_usage", new_callable=AsyncMock
        ) as persist:
            tasks = await subagent_collection.claim_turn_subagents(
                "t-1", "run-1", {"is_byok": False}, "ws-1", "user-1"
            )

        billed = persist.await_args.args[1]
        assert {t.task_id for t in billed} == {"done", "wf_done", "c_done"}
        assert {t.task_id for t in tasks} == {
            "done", "live", "wf_live", "c_live", "wf_done", "c_done",
        }
        gate.set()
        await asyncio.gather(live.asyncio_task, wf_live.asyncio_task)

    @pytest.mark.asyncio
    async def test_a_failed_bill_still_hands_the_tasks_to_the_collector(self):
        task = self._task("done")
        registry = MagicMock()
        registry.claim_run_subagents = AsyncMock(return_value=[task])
        bg_store = MagicMock()
        bg_store.get_registry = AsyncMock(return_value=registry)
        with patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=bg_store,
        ), patch.object(
            subagent_collection,
            "persist_subagent_usage",
            new_callable=AsyncMock,
            side_effect=RuntimeError("db down"),
        ):
            tasks = await subagent_collection.claim_turn_subagents(
                "t-1", "run-1", {}, "ws-1", "user-1"
            )

        assert tasks == [task]

    @staticmethod
    async def _post_terminal(btm, *, kind, status):
        from src.server.services.runs.finalization import FinalizeVerdict

        await btm._apply_post_terminal_effects(
            "t-1",
            "run-1",
            FinalizeVerdict(applied=True, status=status),
            kind=kind,
            stop=None,
            carried_events=[],
            task_info=MagicMock(),
            completion_callback=None,
            execution_time=1.0,
            interrupt_reason=None,
            error=None,
            metadata={"is_byok": False},
            workspace_id="ws-1",
            user_id="user-1",
        )

    @pytest.mark.asyncio
    async def test_claims_and_bills_before_run_end_then_collects(self):
        btm = _make_btm()
        claimed = [MagicMock()]
        order: list = []
        btm.append_run_end_event = AsyncMock(
            side_effect=lambda *a, **k: order.append("run_end")
        )
        with patch.object(
            subagent_collection,
            "claim_turn_subagents",
            new_callable=AsyncMock,
            side_effect=lambda *a, **k: order.append("claim") or claimed,
        ), patch.object(
            subagent_collection,
            "spawn_subagent_collector",
            side_effect=lambda *a, **k: order.append("collect"),
        ) as spawn:
            await self._post_terminal(btm, kind="stream_end", status="completed")

        assert order == ["claim", "run_end", "collect"]
        assert spawn.call_args.args[2] is claimed

    @pytest.mark.asyncio
    async def test_a_failed_run_claims_nothing_before_run_end(self):
        btm = _make_btm()
        btm.append_run_end_event = AsyncMock()
        with patch.object(
            subagent_collection, "claim_turn_subagents", new_callable=AsyncMock
        ) as claim, patch.object(
            subagent_collection, "end_run_subagents", new_callable=AsyncMock
        ):
            await self._post_terminal(btm, kind="failed", status="error")

        claim.assert_not_awaited()
        btm.append_run_end_event.assert_awaited_once()


class TestAppendRunEndEvent:

    @pytest.fixture(autouse=True)
    def totals(self):
        # Unit tests have no pool; a failed read leaves the totals off.
        with patch(
            "src.server.database.conversation.usage.get_thread_token_totals",
            new_callable=AsyncMock,
            return_value=None,
        ) as read:
            yield read

    @pytest.fixture(autouse=True)
    def returned(self):
        with patch(
            "src.server.services.runs.stream_writer.steering_returned_frame",
            new_callable=AsyncMock,
            return_value=None,
        ) as read:
            yield read

    @pytest.mark.asyncio
    async def test_returned_chat_app_steers_close_the_stream_ahead_of_run_end(
        self, returned
    ):
        """A gateway stops reading at ``run_end``, so the steers it must
        resubmit come before it, once, for every reader of the stream."""
        btm = _make_btm()
        btm.event_storage_backend = "redis"
        returned.return_value = "event: steering_returned\ndata: {}\n\n"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls),
        ):
            await btm.append_run_end_event("t-1", "r-1", "completed")

        returned.assert_awaited_once_with("t-1", "r-1")
        frames = [c[2][b"event"].decode("utf-8") for c in calls if c[0] == "xadd"]
        assert [f.split("\n", 1)[0] for f in frames] == [
            "event: steering_returned",
            "event: run_end",
        ]

    @pytest.mark.asyncio
    async def test_a_lost_gate_writes_no_returned_steers(self, returned):
        btm = _make_btm()
        btm.event_storage_backend = "redis"
        returned.return_value = "event: steering_returned\ndata: {}\n\n"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls, gate_acquired=False),
        ):
            await btm.append_run_end_event("t-1", "r-1", "completed")

        assert not [c for c in calls if c[0] == "xadd"]

    @pytest.mark.asyncio
    async def test_terminal_frame_carries_the_threads_token_totals(self, totals):
        """Read after the finalize committed the run's usage row, so the
        client takes the count over the calls its stream reported."""
        import json as _json

        btm = _make_btm()
        btm.event_storage_backend = "redis"
        totals.return_value = {"input_tokens": 1460, "output_tokens": 20}
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls),
        ):
            await btm.append_run_end_event("t-1", "r-1", "completed")

        totals.assert_awaited_once_with("t-1")
        wire = next(c for c in calls if c[0] == "xadd")[2][b"event"].decode("utf-8")
        assert _json.loads(wire.split("data: ", 1)[1])["token_totals"] == {
            "input_tokens": 1460,
            "output_tokens": 20,
        }

    @pytest.mark.asyncio
    async def test_no_truth_frame_reads_no_totals(self, totals):
        btm = _make_btm()
        btm.event_storage_backend = "redis"

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache([]),
        ):
            await btm.append_run_end_event(
                "t-1", "r-1", None, error_frame={"error": "boom"}
            )

        totals.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_appends_sse_wire_frame_with_outcome_and_ttl(self):
        import json as _json

        btm = _make_btm()
        btm.event_storage_backend = "redis"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls),
        ):
            await btm.append_run_end_event("t-1", "r-1", "completed")

        # Terminal retention stamp first — unconditional, both emitters,
        # because active streams carry no TTL and the attach-grace clock
        # starts here. Then the exactly-once gate (SET NX), then the frame.
        assert calls[0] == ("expire", "workflow:stream:t-1:r-1", btm.redis_event_ttl)
        assert calls[1] == (
            "expire",
            "workflow:events:meta:t-1:r-1",
            btm.redis_event_ttl,
        )
        assert calls[2][0] == "set"
        assert calls[2][1][0] == "workflow:run_end_gate:t-1:r-1"
        assert calls[2][2].get("nx") is True
        assert calls[3][0] == "xadd"
        assert calls[3][1] == "workflow:stream:t-1:r-1"
        # Pre-rendered SSE wire string — the consumer passes it through and
        # close-matches on the "event: run_end" prefix.
        wire = calls[3][2][b"event"].decode("utf-8")
        assert wire.startswith("event: run_end\ndata: ")
        assert wire.endswith("\n\n")
        assert _json.loads(wire.split("data: ", 1)[1]) == {
            "thread_id": "t-1",
            "run_id": "r-1",
            "outcome": "completed",
        }
        # TTL refreshed so a run_end landing on an expired key can't leak it.
        assert ("expire", "workflow:stream:t-1:r-1", btm.redis_event_ttl) in calls

    @pytest.mark.asyncio
    async def test_run_end_gate_blocks_duplicate_emitter(self):
        btm = _make_btm()
        btm.event_storage_backend = "redis"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls, gate_acquired=False),
        ):
            await btm.append_run_end_event("t-1", "r-1", "completed")

        # Gate held by another emitter (owner vs recovery scanner racing
        # after the same finalize CAS) — no second run_end frame.
        assert not any(c[0] == "xadd" for c in calls)

    @pytest.mark.asyncio
    async def test_run_end_gate_kept_on_ambiguous_pipeline_failure(self):
        btm = _make_btm()
        btm.event_storage_backend = "redis"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls, fail=True),
        ):
            await btm.append_run_end_event("t-1", "r-1", "error")

        # A failed pipeline is ambiguous (the XADD may have landed with its
        # reply lost) — the gate is NEVER released, choosing at-most-once
        # over retryability. A missing frame falls back to the consumer's
        # terminal handshake.
        assert not any(c[0] == "delete" for c in calls)

    @pytest.mark.asyncio
    async def test_winner_emits_error_frame_before_run_end(self):
        """The gate winner writes the whole closing story: the caller's
        error frame first, then run_end — one pipeline, exactly once."""
        import json as _json

        btm = _make_btm()
        btm.event_storage_backend = "redis"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls),
        ):
            await btm.append_run_end_event(
                "t-1", "r-1", "error", error_frame={"error": "worker_lost"}
            )

        xadds = [c for c in calls if c[0] == "xadd"]
        assert len(xadds) == 2
        first = xadds[0][2][b"event"].decode("utf-8")
        assert first.startswith("event: error\ndata: ")
        assert _json.loads(first.split("data: ", 1)[1]) == {
            "error": "worker_lost"
        }
        second = xadds[1][2][b"event"].decode("utf-8")
        assert second.startswith("event: run_end\ndata: ")

    @pytest.mark.asyncio
    async def test_gate_loser_emits_no_error_frame(self):
        """A lost gate suppresses the ENTIRE closing story — the winner
        already told it; a trailing error frame would double-report."""
        btm = _make_btm()
        btm.event_storage_backend = "redis"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls, gate_acquired=False),
        ):
            await btm.append_run_end_event(
                "t-1", "r-1", "error", error_frame={"error": "worker_lost"}
            )

        assert not any(c[0] == "xadd" for c in calls)

    @pytest.mark.asyncio
    async def test_no_truth_appends_error_frame_without_claiming_gate(self):
        """outcome=None (no durable terminal established): the error frame
        informs attached clients, but no run_end appears, the gate stays
        unclaimed for a later real finalize, and no retention TTL is stamped
        (the row may still be in_progress)."""
        btm = _make_btm()
        btm.event_storage_backend = "redis"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls),
        ):
            await btm.append_run_end_event(
                "t-1", "r-1", None, error_frame={"error": "boom"}
            )

        assert not any(c[0] == "set" for c in calls)
        assert not any(c[0] == "expire" for c in calls)
        xadds = [c for c in calls if c[0] == "xadd"]
        assert len(xadds) == 1
        assert xadds[0][2][b"event"].decode("utf-8").startswith("event: error\n")

    @pytest.mark.asyncio
    async def test_no_truth_suppressed_once_transport_closed(self):
        """outcome=None after a real emitter claimed the gate: the stream
        already ended in run_end — nothing may trail it."""
        btm = _make_btm()
        btm.event_storage_backend = "redis"
        calls: list = []

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache(calls, gate_exists=True),
        ):
            await btm.append_run_end_event(
                "t-1", "r-1", None, error_frame={"error": "boom"}
            )

        assert not any(c[0] == "xadd" for c in calls)

    @pytest.mark.asyncio
    async def test_append_failure_swallowed(self):
        btm = _make_btm()
        btm.event_storage_backend = "redis"

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client",
            return_value=_sentinel_cache([], fail=True),
        ):
            # Must not raise — the finalize effects region calls this
            # best-effort (consumers still close via the terminal handshake).
            await btm.append_run_end_event("t-1", "r-1", "error")

    @pytest.mark.asyncio
    async def test_noop_when_backend_not_redis(self):
        btm = _make_btm()  # event_storage_backend == "memory"

        with patch(
            "src.server.services.runs.stream_writer.get_cache_client"
        ) as mock_get:
            await btm.append_run_end_event("t-1", "r-1", "completed")

        mock_get.assert_not_called()


class TestNoPreFinalizeRunEndOnTerminalFlavors:
    """1.5 (I6): _run_workflow itself never writes ``run_end`` — the frame is
    written inside ``_finalize_run``'s effects region, AFTER the CAS commits,
    carrying the adopted status. A pre-finalize write would let a consumer see
    ``run_end`` for a run whose durable row isn't terminal yet. The finalize
    ``kind`` names the terminal flavor."""

    def _btm_with_task(self, thread_id: str, run_id: str):
        btm = _make_btm()
        btm.enable_storage = True
        btm.executions[(thread_id, run_id)] = _make_task_info(
            thread_id=thread_id, run_id=run_id, status=LocalRunStatus.RUNNING
        )
        return btm

    @pytest.mark.asyncio
    async def test_completed_writes_no_run_end_before_finalize(self):
        btm = self._btm_with_task("t-comp", "r-1")
        order: list[str] = []

        async def fake_workflow():
            yield "id: 1\nevent: x\ndata: a\n\n"
            yield "id: 2\nevent: x\ndata: b\n\n"

        async def record_buffer(thread_id, run_id, event):
            order.append(f"event:{event.splitlines()[0]}")

        async def record_run_end(thread_id, run_id, outcome):
            order.append(f"run_end:{outcome}")

        async def record_finalize(thread_id, run_id, *, kind, error=None, exc=None):
            order.append(f"finalize:{kind}")

        with patch.object(btm, "_buffer_event_redis", side_effect=record_buffer), \
             patch.object(btm, "append_run_end_event", side_effect=record_run_end), \
             patch.object(btm, "_finalize_run", side_effect=record_finalize):
            await btm._run_workflow(
                thread_id="t-comp",
                run_id="r-1",
                workflow_generator=fake_workflow(),
                cancel_event=asyncio.Event(),
            )

        assert order == ["event:id: 1", "event:id: 2", "finalize:stream_end"]

    @pytest.mark.asyncio
    async def test_failed_writes_no_run_end_before_finalize(self):
        btm = self._btm_with_task("t-fail", "r-1")
        order: list[str] = []

        async def failing_workflow():
            yield "id: 1\nevent: x\ndata: a\n\n"
            raise RuntimeError("boom")

        async def record_buffer(thread_id, run_id, event):
            order.append("event")

        async def record_run_end(thread_id, run_id, outcome):
            order.append(f"run_end:{outcome}")

        async def record_finalize(thread_id, run_id, *, kind, error=None, exc=None):
            order.append(f"finalize:{kind}")

        with patch.object(btm, "_buffer_event_redis", side_effect=record_buffer), \
             patch.object(btm, "append_run_end_event", side_effect=record_run_end), \
             patch.object(btm, "_finalize_run", side_effect=record_finalize):
            await btm._run_workflow(
                thread_id="t-fail",
                run_id="r-1",
                workflow_generator=failing_workflow(),
                cancel_event=asyncio.Event(),
            )

        assert order == ["event", "finalize:failed"]

    @pytest.mark.asyncio
    async def test_cancelled_writes_no_run_end_before_finalize(self):
        btm = self._btm_with_task("t-canc", "r-1")
        order: list[str] = []
        cancel_event = asyncio.Event()

        async def fake_workflow():
            for i in range(100):
                await asyncio.sleep(0.01)
                yield f"id: {i}\nevent: x\ndata: a\n\n"

        async def record_run_end(thread_id, run_id, outcome):
            order.append(f"run_end:{outcome}")

        async def record_finalize(thread_id, run_id, *, kind, error=None, exc=None):
            order.append(f"finalize:{kind}")

        async def set_cancel():
            await asyncio.sleep(0.03)
            cancel_event.set()

        with patch.object(btm, "_buffer_event_redis", new_callable=AsyncMock), \
             patch.object(btm, "append_run_end_event", side_effect=record_run_end), \
             patch.object(btm, "_finalize_run", side_effect=record_finalize), \
             patch.object(btm, "_flush_checkpoint", new_callable=AsyncMock), \
             patch.object(btm, "_teardown_subagents_on_stop", new_callable=AsyncMock):
            setter = asyncio.create_task(set_cancel())
            with pytest.raises(asyncio.CancelledError):
                await btm._run_workflow(
                    thread_id="t-canc",
                    run_id="r-1",
                    workflow_generator=fake_workflow(),
                    cancel_event=cancel_event,
                )
            await setter

        assert order == ["finalize:cancelled"]


# ---------------------------------------------------------------------------
# await_drain_and_cleanup_tasks: spill-lock-serialized, claim-fenced deletes
# ---------------------------------------------------------------------------

class TestCleanupSpillLockFence:
    """Cleanup's Redis deletes are serialized on the task's spill lock and
    re-check the collector claim INSIDE it: a resume that steals the task
    while cleanup waits on the lock has already re-deleted the keys and may
    have written round-2 events — a late cleanup delete would erase them."""

    def _task(self, claim: str | None):
        from ptc_agent.agent.middleware.background_subagent.registry import (
            BackgroundTask,
        )

        task = BackgroundTask(
            tool_call_id="tc-1",
            task_id="abc123",
            description="d",
            prompt="p",
            subagent_type="general-purpose",
            agent_id="general-purpose:x",
            terminal_status="completed",
        )
        task.collector_response_id = claim
        task.sse_drain_complete.set()
        return task

    def _cache(self):
        cache = MagicMock()
        cache.enabled = True
        cache.client = MagicMock()
        cache.client.eval = AsyncMock(return_value=3)
        return cache

    @pytest.mark.asyncio
    async def test_steal_while_waiting_on_spill_lock_skips_deletes(self):
        task = self._task("run-old")
        cache = self._cache()

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=None)

        await task.redis_spill_lock.acquire()  # resume holds the lock
        with patch(
            "src.server.services.runs.subagent_collection.get_cache_client",
            return_value=cache,
        ), patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ):
            cleanup = asyncio.create_task(
                subagent_collection.await_drain_and_cleanup_tasks([task], "thread-x", "run-old")
            )
            for _ in range(10):  # let cleanup reach and block on the lock
                await asyncio.sleep(0)
            task.collector_response_id = None  # the steal
            task.redis_spill_lock.release()
            await cleanup

        # Fenced before the conditional delete is even attempted.
        assert cache.client.eval.await_count == 0

    @pytest.mark.asyncio
    async def test_owned_task_conditional_delete_under_the_lock(self):
        task = self._task("run-old")
        locked_during_eval: list[bool] = []

        cache = self._cache()

        async def _eval(*args):
            locked_during_eval.append(task.redis_spill_lock.locked())
            return 3

        cache.client.eval = AsyncMock(side_effect=_eval)

        fake_store = MagicMock()
        fake_store.get_registry = AsyncMock(return_value=None)

        with patch(
            "src.server.services.runs.subagent_collection.get_cache_client",
            return_value=cache,
        ), patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=fake_store,
        ):
            await subagent_collection.await_drain_and_cleanup_tasks([task], "thread-x", "run-old")

        assert locked_during_eval == [True]
        # One atomic script call: meta hash checked as owner key, the
        # captured-events key deleted only if the owner check passes,
        # caller's run as the sole argument.
        args = cache.client.eval.await_args.args
        assert args[1] == 2
        assert list(args[2:4]) == [
            "subagent:meta:thread-x:abc123",
            "subagent:events:thread-x:abc123",
        ]
        assert args[4] == "run-old"


# ---------------------------------------------------------------------------
# replay_owned_task_events: per-record claim recheck across the XRANGE await
# ---------------------------------------------------------------------------

class TestReplayOwnershipRecheck:
    """The replay iterator awaits Redis between yields: a resume can steal the
    task mid-iteration and write round-2 records with reused seq numbers — a
    record yielded after the steal must never archive under round 1."""

    @pytest.mark.asyncio
    async def test_records_after_steal_are_dropped(self):
        from ptc_agent.agent.middleware.background_subagent.registry import (
            BackgroundTask,
        )

        task = BackgroundTask(
            tool_call_id="tc-1",
            task_id="abc123",
            description="d",
            prompt="p",
            subagent_type="general-purpose",
            agent_id="general-purpose:x",
            terminal_status="completed",
        )
        task.collector_response_id = "run-1"

        async def _iter(thread_id, t):
            yield {"event": "message_chunk", "data": {"content": "ROUND-1"}}
            t.collector_response_id = None  # the steal, mid-XRANGE
            yield {"event": "message_chunk", "data": {"content": "ROUND-2"}}

        out: list[dict] = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=_iter,
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "thread-x", task, "run-1", out
            )

        # A steal aborts the whole replay: even pre-steal records are
        # withheld (a partial round-1 archive would clear the replay cache
        # gate on an incomplete transcript) — the resume owns the archive.
        assert ok is False
        assert out == []

    @pytest.mark.asyncio
    async def test_cross_worker_stamped_records_are_dropped(self):
        """A resume on ANOTHER worker never touches this process's claim: the
        stale collector's local token still matches, so the per-record claim
        recheck passes. The record's own run stamp is the durable fence —
        round-2 records carry round-2's id and must be dropped; unstamped
        (legacy-writer) records and own-round records pass."""
        from ptc_agent.agent.middleware.background_subagent.registry import (
            BackgroundTask,
        )

        task = BackgroundTask(
            tool_call_id="tc-1",
            task_id="abc123",
            description="d",
            prompt="p",
            subagent_type="general-purpose",
            agent_id="general-purpose:x",
            terminal_status="completed",
        )
        task.collector_response_id = "run-1"  # worker A's claim, never stolen
        # Round 1 attempted exactly its own two appends; the foreign record
        # must be dropped WITHOUT counting toward this tally.
        task.captured_event_count = 2

        async def _iter(thread_id, t):
            yield {"event": "message_chunk", "data": {"content": "LEGACY"}}
            yield {
                "event": "message_chunk",
                "data": {"content": "OWN"},
                "run": "run-1",
            }
            yield {
                "event": "message_chunk",
                "data": {"content": "ROUND-2"},
                "run": "run-2",
            }

        out: list[dict] = []
        with patch(
            "src.server.services.runs.subagent_collection.iter_subagent_events_full",
            side_effect=_iter,
        ):
            ok = await subagent_collection.replay_owned_task_events(
                "thread-x", task, "run-1", out
            )

        assert ok is True
        contents = [e["data"]["content"] for e in out]
        assert contents == ["LEGACY", "OWN"]


# ---------------------------------------------------------------------------
# A finished run must not pin the request's context
# ---------------------------------------------------------------------------

class _RequestScoped:
    pass


_request_var: "contextvars.ContextVar[_RequestScoped | None]" = contextvars.ContextVar(
    "test_request_scoped", default=None
)


class TestFinishedRunReleasesContext:

    @pytest.mark.asyncio
    async def test_finished_run_drops_task_and_its_context(self):
        """The entry outlives the run by the result TTL; the task (and the
        request context it copied) must not, or every object that context
        references stays alive for that long."""
        btm = _make_btm()

        async def fake_workflow():
            yield "event-0"

        async def handler() -> LocalRunExecution:
            _request_var.set(_RequestScoped())
            return await btm.start_run("thread-ctx", "run-1", fake_workflow())

        with patch.object(btm, "_finalize_run", new_callable=AsyncMock):
            info = await asyncio.create_task(handler(), context=contextvars.Context())
            scoped = weakref.ref(info.task.get_context()[_request_var])
            await asyncio.wait({info.task})
            await asyncio.sleep(0)

        assert btm.executions[("thread-ctx", "run-1")] is info
        assert info.task is None
        gc.collect()
        assert scoped() is None
