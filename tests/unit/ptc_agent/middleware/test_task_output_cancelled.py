"""TaskOutput miss-path contract: durable archive first, cancelled last.

A missing registry entry does not mean the result is gone — it may be
evicted, wiped by a stop, lost to a restart, or held by another worker while
the subagent's answer sits in its ``task:{id}`` checkpoint namespace. The
tool must recover from the archive first, and only report "cancelled by a
user stop" (never "not found") when the archive has nothing either.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent import tools as tools_module
from ptc_agent.agent.middleware.background_subagent.outcomes import (
    CREDIT_STOP_ERROR_TYPE,
)
from ptc_agent.agent.middleware.background_subagent.tools import (
    _delivered_result_text,
    create_task_output_tool,
)


def _middleware(resolved: str | None) -> MagicMock:
    middleware = MagicMock()
    registry = MagicMock()
    # Explicitly ledger-less: an auto-MagicMock attribute is truthy but not
    # awaitable, so the miss path would reach status=None by raising into
    # _missing_task_reply's except arm instead of taking the no-ledger route
    # these tests mean to pin.
    registry.run_ledger = None
    registry.get_by_task_id = AsyncMock(return_value=None)
    registry.resolve_result_text = AsyncMock(return_value=resolved)
    middleware.registry = registry
    return middleware


@pytest.mark.asyncio
async def test_missing_task_recovers_from_durable_archive():
    tool = create_task_output_tool(_middleware("Paris is the capital."))

    result = await tool.coroutine(task_id="k7Xm2p")

    assert "Paris is the capital." in result
    assert "recovered from the durable archive" in result
    assert "cancelled" not in result.lower()


@pytest.mark.asyncio
async def test_missing_task_with_empty_archive_reports_no_recorded_outcome():
    """No ledger row and nothing archived is an unknown fate, not a stop —
    naming a cause the code cannot know tells the agent the wrong thing about
    whether re-dispatching is safe."""
    tool = create_task_output_tool(_middleware(None))

    result = await tool.coroutine(task_id="k7Xm2p")

    assert "outcome was never recorded" in result.lower()
    assert "cancelled" not in result.lower()
    assert "not found" not in result.lower()


@pytest.mark.asyncio
async def test_archive_recovery_is_scoped_to_the_ledgers_latest_run():
    """The archive is written per run, and only the ledger knows which run is
    this task's latest — reading it unscoped would hand a re-dispatched task
    its predecessor's answer."""
    ledger = MagicMock()
    ledger.get_latest_run = AsyncMock(
        return_value={"status": "completed", "task_run_id": "run-7"}
    )
    ledger.mark_result_delivered = AsyncMock()
    middleware = _middleware("archived text")
    middleware.registry.run_ledger = ledger

    reply = await create_task_output_tool(middleware).coroutine(task_id="k7Xm2p")

    assert "archived text" in reply
    middleware.registry.resolve_result_text.assert_awaited_once_with(
        "k7Xm2p", "run-7"
    )
    # The archive delivery IS a delivery — nothing further is owed.
    ledger.mark_result_delivered.assert_awaited_once_with("run-7")


@pytest.mark.asyncio
async def test_a_failed_run_that_archived_a_result_still_reports_it():
    """A worker lost between the archive write and the ledger CAS leaves a
    readable result under an error verdict. The archive is written only by a
    run that produced one, so its presence outranks a verdict the run never
    got to overwrite — otherwise finished work reports as lost."""
    ledger = MagicMock()
    ledger.get_latest_run = AsyncMock(
        return_value={
            "status": "error",
            "task_run_id": "run-7",
            "failure": {"error": "worker_lost", "error_type": "worker_lost"},
        }
    )
    ledger.mark_result_delivered = AsyncMock()
    middleware = _middleware(None)
    middleware.registry.run_ledger = ledger
    middleware.registry.resolve_archived_result_text = AsyncMock(
        return_value="archived workflow summary"
    )

    reply = await create_task_output_tool(middleware).coroutine(task_id="k7Xm2p")

    assert "archived workflow summary" in reply
    assert "failed and produced no result" not in reply
    middleware.registry.resolve_archived_result_text.assert_awaited_once_with(
        "k7Xm2p", "run-7"
    )
    # The permissive resolver must never be consulted under a failed verdict:
    # it would derive the run's mid-work transcript into a fake answer.
    middleware.registry.resolve_result_text.assert_not_awaited()
    ledger.mark_result_delivered.assert_awaited_once_with("run-7")


@pytest.mark.asyncio
async def test_a_failed_run_with_no_archive_still_reports_failure():
    """The honest-failure floor: nothing archived means nothing to report but
    the failure itself — and handing the agent that fate IS the delivery
    (stamped), so the failure-leg report-back job drops at claim time."""
    ledger = MagicMock()
    ledger.get_latest_run = AsyncMock(
        return_value={
            "status": "error",
            "task_run_id": "run-9",
            "failure": {"error": "boom"},
        }
    )
    ledger.mark_result_delivered = AsyncMock()
    middleware = _middleware("mid-work transcript text")
    middleware.registry.run_ledger = ledger
    middleware.registry.resolve_archived_result_text = AsyncMock(return_value=None)

    reply = await create_task_output_tool(middleware).coroutine(task_id="k7Xm2p")

    assert "failed and produced no result" in reply
    assert "boom" in reply
    assert "mid-work transcript text" not in reply
    ledger.mark_result_delivered.assert_awaited_once_with("run-9")


@pytest.mark.asyncio
async def test_a_stopped_task_counts_as_delivered(monkeypatch):
    """The stop notice IS the delivery. Left unmarked, the task stays
    completed-but-unseen and the orchestrator announces it as finished work,
    so the agent reports the same stop a second time — that pass reads the
    ledger, which is not the turn that saw the stop."""
    task = SimpleNamespace(
        task_id="k7Xm2p",
        display_id="Task-k7Xm2p",
        subagent_type="workflow",
        completed=False,
        terminal_status=None,
        asyncio_task=None,
        result=None,
        result_seen=False,
        last_checked_at=0.0,
    )

    async def _stopped_mid_wait(*_args, **_kwargs):
        # A stop stamps the task and builds its payload in one step
        # (``BackgroundTask.mark_cancelled``), so a wait can only report a
        # stop for a task already settled as cancelled.
        task.completed = True
        task.terminal_status = "cancelled"
        task.result = {"success": False, "status": "cancelled", "error": "Cancelled"}
        return task.result

    registry = MagicMock()
    registry.run_ledger = None
    registry.get_by_task_id = AsyncMock(return_value=task)
    registry.wait_for_specific = AsyncMock(side_effect=_stopped_mid_wait)
    registry.mark_result_delivered = AsyncMock()
    middleware = MagicMock()
    middleware.registry = registry
    monkeypatch.setattr(tools_module, "get_config", lambda: {"configurable": {}})
    monkeypatch.setattr(
        tools_module.utils, "build_message_checker", AsyncMock(return_value=None)
    )

    reply = await create_task_output_tool(middleware).coroutine(
        task_id="k7Xm2p", timeout=5
    )

    assert "was stopped by the user" in reply
    assert task.result_seen is True
    registry.mark_result_delivered.assert_awaited_once_with(task)


# ---------------------------------------------------------------------------
# Delivery derivation: durable checkpoint answer first, in-memory fallback
# ---------------------------------------------------------------------------


def _completed_task(result) -> SimpleNamespace:
    return SimpleNamespace(task_id="k7Xm2p", task_run_id="run-1", result=result)


class TestDeliveredResultText:
    @pytest.mark.asyncio
    async def test_durable_answer_wins_for_successful_results(self):
        registry = MagicMock()
        registry.resolve_result_text = AsyncMock(return_value="archived answer")
        task = _completed_task({"success": True, "result": "in-memory answer"})
        assert await _delivered_result_text(registry, task) == "archived answer"

    @pytest.mark.asyncio
    async def test_falls_back_to_memory_when_archive_empty(self):
        registry = MagicMock()
        registry.resolve_result_text = AsyncMock(return_value=None)
        task = _completed_task({"success": True, "result": "in-memory answer"})
        assert await _delivered_result_text(registry, task) == "in-memory answer"

    @pytest.mark.asyncio
    async def test_failures_never_consult_the_archive(self):
        registry = MagicMock()
        registry.resolve_result_text = AsyncMock(return_value="stale partial")
        task = _completed_task({"success": False, "error": "boom"})
        text = await _delivered_result_text(registry, task)
        assert "boom" in text
        registry.resolve_result_text.assert_not_awaited()


class TestRegistryResolveResultText:
    @pytest.mark.asyncio
    async def test_no_resolver_means_none(self):
        registry = BackgroundTaskRegistry(thread_id="")
        assert await registry.resolve_result_text("k7Xm2p") is None

    @pytest.mark.asyncio
    async def test_resolver_errors_degrade_to_none(self):
        registry = BackgroundTaskRegistry(thread_id="t1")
        registry.result_resolver = AsyncMock(side_effect=RuntimeError("db down"))
        assert await registry.resolve_result_text("k7Xm2p") is None


def _ledger_middleware(run: dict) -> MagicMock:
    """A registry whose ONLY authority is the durable ledger — the shape every
    worker but the one holding the task sees."""
    middleware = MagicMock()
    registry = MagicMock()
    ledger = MagicMock()
    ledger.get_latest_run = AsyncMock(return_value=run)
    ledger.mark_result_delivered = AsyncMock(return_value=None)
    registry.run_ledger = ledger
    registry.get_by_task_id = AsyncMock(return_value=None)
    registry.resolve_result_text = AsyncMock(return_value=None)
    registry.resolve_archived_result_text = AsyncMock(return_value=None)
    middleware.registry = registry
    return middleware


@pytest.mark.asyncio
async def test_a_credit_stopped_task_reads_as_resumable_from_the_ledger():
    """The advice must not depend on which worker answers.

    A credit stop spells itself 'cancelled' exactly like a timeout kill does,
    so the generic cancelled copy ("cannot be resumed") would talk the model
    out of the one path this stop is designed to leave open. The live task
    already branches on it; the ledger is what answers on every worker but the
    one still holding the task in memory, which multi-worker makes a coin flip.
    """
    run = {
        "status": "cancelled",
        "task_run_id": "run-1",
        "failure": {"error_type": CREDIT_STOP_ERROR_TYPE, "error": "out of credits"},
    }
    tool = create_task_output_tool(_ledger_middleware(run))

    result = await tool.coroutine(task_id="k7Xm2p")

    assert "ran out of credits" in result
    assert 'Task(action="resume"' in result
    assert "cannot be resumed" not in result


@pytest.mark.asyncio
async def test_an_ordinary_cancel_still_reads_as_unresumable():
    """The complement, and the reason the branch is on the error type rather
    than on the status: telling the model to resume a timeout kill would be
    the opposite error, made just as silently."""
    run = {
        "status": "cancelled",
        "task_run_id": "run-1",
        "failure": {"error": "killed by timeout"},
    }
    tool = create_task_output_tool(_ledger_middleware(run))

    result = await tool.coroutine(task_id="k7Xm2p")

    assert "cannot be resumed" in result
    assert 'Task(action="resume"' not in result
