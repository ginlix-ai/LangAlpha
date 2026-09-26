"""How a firing moves through its turn: admission, the line for a busy thread,
and each way it ends.

Locks the executor's side of the settlement contract: a turn that loses its
thread before admission gets back in line and runs as the automation now
stands; a wait ends skipped when the automation was paused or the server
stops, and survives a failed look; once its run reached the ledger, the
firing is that run's to settle (``automation_settlement``), however the drain
here ends.
"""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.runs.lifecycle import RunSlotBusyError
from src.server.handlers.chat.admission_gate import (
    ADMISSION_CONFLICT_CODES,
    admission_conflict_detail,
)
from src.server.services import automation_executor as executor_mod
from src.server.services.automation_executor import AutomationExecutor
from src.server.services.automation_settlement import INTERRUPTED_ERROR
from src.server.services.writer_guard import WriterGuardUnavailable

_MOD = "src.server.services.automation_executor"
_USER = "user-1"
_EXEC = "exec-1"


def test_busy_codes_are_the_admission_gates_thread_busy_answers():
    for state in ("running", "stopping", "compacting"):
        assert admission_conflict_detail(state)["code"] in executor_mod._THREAD_BUSY_CODES
    assert executor_mod._THREAD_BUSY_CODES <= ADMISSION_CONFLICT_CODES
    # A steer probe's refusal is not a busy thread.
    assert "not_running" not in executor_mod._THREAD_BUSY_CODES


def _automation(**overrides):
    return {
        "automation_id": "auto-1",
        "user_id": _USER,
        "name": "",
        "agent_mode": "flash",
        "instruction": "Summarize the market",
        "trigger_type": "cron",
        "workspace_id": None,
        "thread_strategy": "new",
        "conversation_thread_id": None,
        "llm_model": None,
        "additional_context": None,
        "trigger_config": None,
        "status": "active",
        **overrides,
    }


def _run(run_id, status="completed", **metadata):
    return {"conversation_response_id": run_id, "status": status, "metadata": metadata}


@contextmanager
def _firing(turns, *, runs=None, busy=(), fresh=None, is_byok=(False,)):
    """Patch everything a firing touches.

    ``turns`` stand in for successive astream calls. ``runs`` maps a turn's
    index to what the ledger says of its run (in progress by default).
    ``busy`` is what successive idle checks of the thread answer.
    """
    runs = runs or {}
    db = MagicMock()
    db.transition_execution = AsyncMock(return_value={"conversation_thread_id": None})
    db.settle_execution = AsyncMock(
        return_value={
            "conversation_thread_id": None,
            "conversation_response_id": None,
            "settled_from": "running",
        }
    )
    db.record_delivery = AsyncMock()
    db.update_automation = AsyncMock()
    db.get_execution_status = AsyncMock(return_value="waiting")
    db.has_earlier_waiting_execution = AsyncMock(return_value=False)
    db.get_automation = AsyncMock(return_value=fresh)
    run_ids: list[str] = []

    def astream(**kwargs):
        run_ids.append(kwargs["run_id"])
        return turns[len(run_ids) - 1](**kwargs)

    async def get_run(run_id):
        index = run_ids.index(run_id)
        run = runs.get(index, "in_progress")
        if run is None or isinstance(run, dict):
            return run
        return _run(run_id, run)

    busy_answers = iter(busy)
    fx = SimpleNamespace(
        db=db,
        run_ids=run_ids,
        credit=AsyncMock(),
        byok=AsyncMock(side_effect=list(is_byok) * 4),
        started=AsyncMock(return_value=None),
        settled_webhook=AsyncMock(return_value=None),
        announce=AsyncMock(),
        astream=MagicMock(side_effect=astream),
    )
    with (
        patch(f"{_MOD}.auto_db", new=db),
        patch("src.server.services.automation_settlement.auto_db", new=db),
        patch(f"{_MOD}.is_byok_active", new=fx.byok),
        patch(f"{_MOD}.has_any_oauth_token", new=AsyncMock(return_value=False)),
        patch(f"{_MOD}.enforce_credit_limit", new=fx.credit),
        patch(
            f"{_MOD}.get_or_create_flash_workspace",
            new=AsyncMock(return_value={"workspace_id": "ws-1"}),
        ),
        patch(f"{_MOD}.fire_webhook", new=fx.started),
        patch("src.server.services.automation_settlement.fire_webhook", new=fx.settled_webhook),
        patch(f"{_MOD}.announce_wait", new=fx.announce),
        patch("src.server.services.automation_settlement.announce_wait", new=AsyncMock()),
        patch(f"{_MOD}._thread_busy", new=AsyncMock(side_effect=lambda _: next(busy_answers, False))),
        patch(f"{_MOD}._WAIT_POLL_SECONDS", new=0),
        patch("src.server.database.runs.lifecycle.get_run", new=AsyncMock(side_effect=get_run)),
        patch("src.server.handlers.chat.astream_flash_workflow", new=fx.astream),
        patch("src.server.handlers.chat.astream_ptc_workflow", new=fx.astream),
    ):
        yield fx


async def _streams(**_):
    yield "event: message_chunk\ndata: {}\n\n"


def _loses(exc):
    async def turn(**_):
        raise exc
        yield  # an async generator, like the workflows

    return turn


def _settled(fx):
    return fx.db.settle_execution.await_args.kwargs


def _left_to_run(fx):
    """The run the firing was left to, which settles nothing here."""
    fx.db.settle_execution.assert_not_awaited()
    linked = [
        c.kwargs["conversation_response_id"]
        for c in fx.db.transition_execution.await_args_list
        if c.kwargs.get("conversation_response_id")
    ]
    assert len(set(linked)) == 1
    return linked[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lost",
    [
        RunSlotBusyError("thread-1"),
        HTTPException(status_code=409, detail=admission_conflict_detail("running")),
    ],
    ids=["run_slot", "busy_409"],
)
async def test_a_turn_that_loses_its_thread_runs_as_the_automation_now_stands(lost):
    fresh = _automation(instruction="Summarize the week")
    with _firing([_loses(lost), _streams], fresh=fresh, is_byok=(False, True)) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert fx.astream.call_count == 2
    second = fx.astream.call_args_list[1].kwargs
    assert second["user_input"] == "Summarize the week"
    # Credentials and the credit gate are read again after the wait.
    assert second["is_byok"] is True
    assert fx.credit.await_count == 2
    waited = [c.kwargs["to"] for c in fx.db.transition_execution.call_args_list]
    assert "waiting" in waited
    assert _left_to_run(fx) == fx.run_ids[1]


@pytest.mark.asyncio
async def test_a_wait_ends_skipped_when_the_automation_was_paused():
    lost = HTTPException(status_code=409, detail=admission_conflict_detail("running"))
    with _firing([_loses(lost)], fresh=_automation(status="paused")) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert fx.astream.call_count == 1
    assert _settled(fx)["to"] == "skipped"
    assert _settled(fx)["skip_reason"] == "user"
    assert _settled(fx)["from_statuses"] == ("waiting",)


@pytest.mark.asyncio
async def test_a_manual_run_of_a_paused_automation_runs_after_its_wait():
    paused = _automation(status="paused")
    lost = HTTPException(status_code=409, detail=admission_conflict_detail("running"))
    with _firing([_loses(lost), _streams], fresh=paused) as fx:
        await AutomationExecutor().execute(paused, _EXEC)

    assert fx.astream.call_count == 2
    assert _left_to_run(fx) == fx.run_ids[1]


@pytest.mark.asyncio
async def test_a_failed_look_keeps_the_firing_waiting():
    pinned = _automation(thread_strategy="continue", conversation_thread_id="thread-1")
    with _firing([_streams], busy=(True,), fresh=pinned) as fx:
        fx.db.get_execution_status.side_effect = [RuntimeError("db blip"), "waiting"]
        await AutomationExecutor().execute(pinned, _EXEC)

    assert fx.astream.call_count == 1
    assert _left_to_run(fx) == fx.run_ids[0]


@pytest.mark.asyncio
async def test_shutdown_ends_a_wait_promptly():
    pinned = _automation(thread_strategy="continue", conversation_thread_id="thread-1")
    executor = AutomationExecutor()
    executor.stop_waiting()
    with _firing([_streams], busy=(True,) * 100, fresh=pinned) as fx:
        with patch(f"{_MOD}._WAIT_POLL_SECONDS", new=60):
            await asyncio.wait_for(executor.execute(pinned, _EXEC), timeout=5)

    fx.astream.assert_not_called()
    assert _settled(fx)["to"] == "skipped"
    assert _settled(fx)["skip_reason"] == "interrupted"


@pytest.mark.asyncio
async def test_a_turn_stopped_before_its_first_event_is_left_to_its_run():
    """Seen only cancelled at admission, and still the firing's own turn:
    its run settles it as the stop it was."""
    stopped = _run("run-1", "cancelled", cancelled_by_user=True)
    with _firing([_streams], runs={0: stopped}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _left_to_run(fx) == fx.run_ids[0]
    fx.started.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("run", ["completed", "error"])
async def test_a_run_already_over_at_admission_is_not_announced(run):
    """Its terminal notice may already be out from the settle job, and a
    start behind it would leave a channel showing a run that ended."""
    with _firing([_streams], runs={0: run}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _left_to_run(fx) == fx.run_ids[0]
    fx.started.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unavailable_writer_guard_is_ours():
    with _firing([_loses(WriterGuardUnavailable("budget"))], runs={0: None}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _settled(fx)["to"] == "failed"
    assert _settled(fx)["strike"] is None
    # Never admitted, so no run is named.
    assert _settled(fx)["conversation_response_id"] is None
    fx.started.assert_not_awaited()


def _breaks_after_admission(**_):
    async def turn():
        yield "event: message_chunk\ndata: {}\n\n"
        raise ConnectionError("stream read reset")

    return turn()


@pytest.mark.asyncio
@pytest.mark.parametrize("run", ["in_progress", "completed", "error"])
async def test_a_drain_that_breaks_after_admission_ends_as_the_run_does(run):
    """The run lives in the run executor: losing its stream is this task's
    failure, never a strike of its own."""
    with _firing([_breaks_after_admission], runs={0: run}) as fx:
        await AutomationExecutor().execute(_automation(), _EXEC)

    assert _left_to_run(fx) == fx.run_ids[0]


def _held_turn(*, admitted: bool):
    """A turn that never ends, after or before its run was admitted."""

    async def turn(**_):
        if admitted:
            yield "event: message_chunk\ndata: {}\n\n"
        await asyncio.Event().wait()
        yield "never"

    return turn


@pytest.mark.asyncio
async def test_a_stop_after_admission_leaves_the_firing_to_its_run():
    with _firing([_held_turn(admitted=True)]) as fx:
        task = asyncio.create_task(AutomationExecutor().execute(_automation(), _EXEC))
        while not fx.started.await_count:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert _left_to_run(fx) == fx.run_ids[0]


@pytest.mark.asyncio
async def test_a_stop_before_admission_interrupts_the_firing():
    with _firing([_held_turn(admitted=False)], runs={0: None}) as fx:
        task = asyncio.create_task(AutomationExecutor().execute(_automation(), _EXEC))
        while not fx.astream.call_count:
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert _settled(fx)["to"] == "failed"
    assert _settled(fx)["error_message"] == INTERRUPTED_ERROR
    assert _settled(fx)["conversation_response_id"] is None
