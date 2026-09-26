"""
Tests for AutomationScheduler service.

Tests the scheduling lifecycle: singleton, start/shutdown, polling logic,
cron calculation, and task dispatch.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.automation_executor import ABANDONED_AFTER_SECONDS
from src.server.services.automation_scheduler import AutomationScheduler
from src.server.services.automation_settlement import INTERRUPTED_ERROR, Outcome

_MOD = "src.server.services.automation_scheduler"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_automation(
    automation_id=None,
    trigger_type="cron",
    cron_expression="0 9 * * *",
    timezone_name="UTC",
    **overrides,
):
    data = {
        "automation_id": automation_id or str(uuid.uuid4()),
        "user_id": "user-1",
        "name": "Test Automation",
        "trigger_type": trigger_type,
        "cron_expression": cron_expression,
        "timezone": timezone_name,
        "agent_mode": "flash",
        "instruction": "Do something",
        "workspace_id": None,
        "_execution_id": str(uuid.uuid4()),
    }
    data.update(overrides)
    return data


def _quiet_sweep(mock_auto_db):
    """Nothing left unsettled, so a poll's sweep is a no-op."""
    mock_auto_db.settle_legacy_executions = AsyncMock(return_value=0)
    mock_auto_db.list_abandoned_executions = AsyncMock(return_value=[])


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

class TestSingleton:
    """Test AutomationScheduler singleton pattern."""

    def teardown_method(self):
        AutomationScheduler._instance = None

    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    def test_get_instance_creates_singleton(self, mock_executor_cls):
        mock_executor_cls.get_instance.return_value = MagicMock()
        instance = AutomationScheduler.get_instance()
        assert instance is not None
        assert isinstance(instance, AutomationScheduler)

    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    def test_get_instance_returns_same_instance(self, mock_executor_cls):
        mock_executor_cls.get_instance.return_value = MagicMock()
        first = AutomationScheduler.get_instance()
        second = AutomationScheduler.get_instance()
        assert first is second


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

class TestInit:
    """Test AutomationScheduler initialization."""

    def teardown_method(self):
        AutomationScheduler._instance = None

    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    def test_init_sets_server_id(self, mock_executor_cls):
        mock_executor_cls.get_instance.return_value = MagicMock()
        scheduler = AutomationScheduler()
        assert scheduler.server_id is not None
        assert len(scheduler.server_id) > 0

    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    def test_init_empty_running_tasks(self, mock_executor_cls):
        mock_executor_cls.get_instance.return_value = MagicMock()
        scheduler = AutomationScheduler()
        assert len(scheduler._running_tasks) == 0
        assert scheduler._poll_task is None


# ---------------------------------------------------------------------------
# start / shutdown
# ---------------------------------------------------------------------------

class TestLifecycle:
    """Test start and shutdown."""

    def teardown_method(self):
        AutomationScheduler._instance = None

    @pytest.mark.asyncio
    @patch("src.server.services.automation_scheduler.auto_db")
    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    async def test_start_creates_poll_task(self, mock_executor_cls, mock_auto_db):
        mock_executor_cls.get_instance.return_value = MagicMock()

        scheduler = AutomationScheduler()
        await scheduler.start()

        try:
            assert scheduler._poll_task is not None
            assert not scheduler._shutdown_event.is_set()
        finally:
            await scheduler.shutdown()

    @pytest.mark.asyncio
    @patch("src.server.services.automation_scheduler.auto_db")
    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    async def test_shutdown_stops_poll_task(self, mock_executor_cls, mock_auto_db):
        mock_executor_cls.get_instance.return_value = MagicMock()

        scheduler = AutomationScheduler()
        await scheduler.start()

        poll_task = scheduler._poll_task
        await scheduler.shutdown()

        assert scheduler._shutdown_event.is_set()
        assert poll_task.done() or poll_task.cancelled()

    @pytest.mark.asyncio
    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    async def test_shutdown_without_start(self, mock_executor_cls):
        mock_executor_cls.get_instance.return_value = MagicMock()

        scheduler = AutomationScheduler()
        # Should not raise
        await scheduler.shutdown()
        assert scheduler._shutdown_event.is_set()


# ---------------------------------------------------------------------------
# _poll_once
# ---------------------------------------------------------------------------

class TestPollOnce:
    """Test single polling iteration."""

    def teardown_method(self):
        AutomationScheduler._instance = None

    @pytest.mark.asyncio
    @patch("src.server.services.automation_scheduler.auto_db")
    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    async def test_poll_once_no_due_automations(self, mock_executor_cls, mock_auto_db):
        mock_executor = MagicMock()
        mock_executor_cls.get_instance.return_value = mock_executor
        mock_auto_db.claim_due_automations = AsyncMock(return_value=[])
        _quiet_sweep(mock_auto_db)

        scheduler = AutomationScheduler()
        await scheduler._poll_once()
        await scheduler._sweep_task

        mock_auto_db.claim_due_automations.assert_awaited_once()
        mock_auto_db.list_abandoned_executions.assert_awaited_once()
        assert len(scheduler._running_tasks) == 0

    @pytest.mark.asyncio
    @patch("src.server.services.automation_scheduler.auto_db")
    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    async def test_poll_once_dispatches_cron_automation(self, mock_executor_cls, mock_auto_db):
        mock_executor = AsyncMock()
        mock_executor_cls.get_instance.return_value = mock_executor

        automation = _make_automation(trigger_type="cron", cron_expression="0 9 * * *")
        mock_auto_db.claim_due_automations = AsyncMock(return_value=[automation])
        mock_auto_db.update_automation_next_run = AsyncMock()
        _quiet_sweep(mock_auto_db)

        scheduler = AutomationScheduler()
        await scheduler._poll_once()

        # Should calculate next run for cron type
        mock_auto_db.update_automation_next_run.assert_awaited_once()

        # Should have created a task
        # Wait briefly for the task to be registered
        await asyncio.sleep(0.01)
        # The task might already be done if executor is async mock,
        # but it was added to running_tasks at some point
        mock_executor.execute.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.server.services.automation_scheduler.auto_db")
    @patch("src.server.services.automation_scheduler.AutomationExecutor")
    async def test_poll_once_skips_next_run_for_once_type(self, mock_executor_cls, mock_auto_db):
        mock_executor = AsyncMock()
        mock_executor_cls.get_instance.return_value = mock_executor

        automation = _make_automation(
            trigger_type="once", cron_expression=None
        )
        mock_auto_db.claim_due_automations = AsyncMock(return_value=[automation])
        mock_auto_db.update_automation_next_run = AsyncMock()
        _quiet_sweep(mock_auto_db)

        scheduler = AutomationScheduler()
        await scheduler._poll_once()

        # Should NOT calculate next_run for one-time type
        mock_auto_db.update_automation_next_run.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.auto_db")
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_a_failed_reschedule_still_runs_the_whole_batch(
        self, mock_executor_cls, mock_auto_db
    ):
        """Every row is claimed before the loop: one left undispatched would
        sit until the sweep failed it as interrupted."""
        mock_executor = AsyncMock()
        mock_executor_cls.get_instance.return_value = mock_executor
        first, second = _make_automation(), _make_automation()
        mock_auto_db.claim_due_automations = AsyncMock(return_value=[first, second])
        mock_auto_db.update_automation_next_run = AsyncMock(
            side_effect=[RuntimeError("db blip"), None]
        )
        _quiet_sweep(mock_auto_db)

        scheduler = AutomationScheduler()
        await scheduler._poll_once()
        await asyncio.gather(*scheduler._running_tasks, scheduler._sweep_task)

        assert mock_auto_db.update_automation_next_run.await_count == 2
        assert [c.args[1] for c in mock_executor.execute.await_args_list] == [
            first["_execution_id"],
            second["_execution_id"],
        ]
        mock_auto_db.list_abandoned_executions.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.auto_db")
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_poll_claims_before_it_sweeps(self, mock_executor_cls, mock_auto_db):
        mock_executor_cls.get_instance.return_value = AsyncMock()
        calls = []
        mock_auto_db.claim_due_automations = AsyncMock(
            side_effect=lambda **_: calls.append("claim") or []
        )
        mock_auto_db.settle_legacy_executions = AsyncMock(
            side_effect=lambda *_: calls.append("sweep") or 0
        )
        mock_auto_db.list_abandoned_executions = AsyncMock(return_value=[])

        scheduler = AutomationScheduler()
        await scheduler._poll_once()
        await scheduler._sweep_task

        assert calls == ["claim", "sweep"]

    @pytest.mark.asyncio
    @patch(f"{_MOD}.auto_db")
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_a_slow_sweep_does_not_hold_up_the_next_claim(
        self, mock_executor_cls, mock_auto_db
    ):
        mock_executor_cls.get_instance.return_value = AsyncMock()
        release = asyncio.Event()
        sweeps = []

        async def slow_sweep(*_):
            sweeps.append(1)
            await release.wait()
            return 0

        mock_auto_db.claim_due_automations = AsyncMock(return_value=[])
        mock_auto_db.settle_legacy_executions = AsyncMock(side_effect=slow_sweep)
        mock_auto_db.list_abandoned_executions = AsyncMock(return_value=[])
        scheduler = AutomationScheduler()

        await scheduler._poll_once()
        await asyncio.sleep(0)
        await scheduler._poll_once()

        assert mock_auto_db.claim_due_automations.await_count == 2
        assert sweeps == [1]
        release.set()
        await scheduler._sweep_task


# ---------------------------------------------------------------------------
# _sweep_abandoned_firings
# ---------------------------------------------------------------------------


def _abandoned_row(automation_id):
    return {
        "automation_execution_id": str(uuid.uuid4()),
        "automation_id": automation_id,
        "status": "running",
        "conversation_thread_id": None,
        "conversation_response_id": None,
        "user_id": "user-1",
        "workspace_id": None,
    }


class TestSweep:
    """Settling firings whose process went away."""

    def teardown_method(self):
        AutomationScheduler._instance = None

    @pytest.mark.asyncio
    @patch(f"{_MOD}.settle_abandoned")
    @patch(f"{_MOD}.auto_db")
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_settles_each_abandoned_firing_of_a_live_automation(
        self, mock_executor_cls, mock_auto_db, mock_settle
    ):
        mock_executor_cls.get_instance.return_value = MagicMock()
        kept, gone = _abandoned_row("auto-kept"), _abandoned_row("auto-gone")
        automation = _make_automation(automation_id="auto-kept")
        mock_auto_db.settle_legacy_executions = AsyncMock(return_value=2)
        mock_auto_db.list_abandoned_executions = AsyncMock(return_value=[kept, gone])
        mock_auto_db.get_automation = AsyncMock(
            side_effect=lambda aid, uid: automation if aid == "auto-kept" else None
        )
        mock_settle.side_effect = AsyncMock(return_value=Outcome.INTERRUPTED)

        await AutomationScheduler()._sweep_abandoned_firings()

        mock_auto_db.settle_legacy_executions.assert_awaited_once_with(INTERRUPTED_ERROR)
        mock_auto_db.list_abandoned_executions.assert_awaited_once_with(
            ABANDONED_AFTER_SECONDS
        )
        mock_settle.assert_awaited_once_with(automation, kept, ABANDONED_AFTER_SECONDS)

    @pytest.mark.asyncio
    @patch(f"{_MOD}.settle_abandoned")
    @patch(f"{_MOD}.auto_db")
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_one_failing_row_does_not_stop_the_rest(
        self, mock_executor_cls, mock_auto_db, mock_settle
    ):
        mock_executor_cls.get_instance.return_value = MagicMock()
        rows = [_abandoned_row("auto-1"), _abandoned_row("auto-1")]
        mock_auto_db.settle_legacy_executions = AsyncMock(return_value=0)
        mock_auto_db.list_abandoned_executions = AsyncMock(return_value=rows)
        mock_auto_db.get_automation = AsyncMock(return_value=_make_automation())
        mock_settle.side_effect = [RuntimeError("db blip"), None]

        await AutomationScheduler()._sweep_abandoned_firings()

        assert mock_settle.await_count == 2

    @pytest.mark.asyncio
    @patch(f"{_MOD}.settle_abandoned")
    @patch(f"{_MOD}.auto_db")
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_a_failed_listing_never_raises(
        self, mock_executor_cls, mock_auto_db, mock_settle
    ):
        mock_executor_cls.get_instance.return_value = MagicMock()
        mock_auto_db.settle_legacy_executions = AsyncMock(return_value=0)
        mock_auto_db.list_abandoned_executions = AsyncMock(
            side_effect=RuntimeError("db down")
        )

        await AutomationScheduler()._sweep_abandoned_firings()

        mock_settle.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_shutdown_ends_waits_and_start_allows_them(self, mock_executor_cls):
        executor = MagicMock()
        mock_executor_cls.get_instance.return_value = executor
        scheduler = AutomationScheduler()

        with patch.object(scheduler, "_poll_loop", new=AsyncMock()):
            await scheduler.start()
            executor.resume_waiting.assert_called_once()
            await scheduler.shutdown()
        executor.stop_waiting.assert_called_once()


# ---------------------------------------------------------------------------
# _calculate_next_run
# ---------------------------------------------------------------------------

class TestCalculateNextRun:
    """Test cron next-run calculation."""

    def test_calculates_utc_datetime(self):
        result = AutomationScheduler._calculate_next_run("0 9 * * *", "UTC")
        assert result.tzinfo is not None
        assert result > datetime.now(timezone.utc)

    def test_handles_invalid_timezone(self):
        # Should fall back to UTC without raising
        result = AutomationScheduler._calculate_next_run(
            "0 9 * * *", "Invalid/Timezone"
        )
        assert result.tzinfo is not None
        assert result > datetime.now(timezone.utc)

    def test_calculate_first_run_delegates(self):
        result = AutomationScheduler.calculate_first_run("*/5 * * * *", "UTC")
        assert result.tzinfo is not None
        assert result > datetime.now(timezone.utc)

    def test_non_utc_timezone_converts_to_utc(self):
        result = AutomationScheduler._calculate_next_run(
            "0 9 * * *", "America/New_York"
        )
        assert result.tzinfo is not None
        # The result should be a valid UTC datetime
        assert result > datetime.now(timezone.utc)
