"""An automation firing a restart stopped ends with the turn that takes it
over, and delivers that turn's answer, as an uninterrupted firing ends with
its own run. When no turn takes it over it ends interrupted.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.contracts.status import RESTART_INTERRUPTED_META, RESTART_RESUME_TTL
from src.server.database.runs.hook_jobs import build_finalize_jobs_from_run_row
from src.server.services import automation_settlement
from src.server.services.automation_settlement import Outcome, ledger_outcome
from src.server.services.report_back.handoff import carry

_LATEST = "src.server.database.runs.lifecycle.get_latest_attempt"
_FIRING = {"automation_execution_id": "exec-1", "automation_id": "auto-1"}


def _stopped(*, ago=timedelta(seconds=5), **meta):
    return {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "thread-1",
        "status": "cancelled",
        "cancel_requested_at": datetime.now(timezone.utc) - ago,
        "metadata": {RESTART_INTERRUPTED_META: True, "user_id": "u-1", **_FIRING, **meta},
    }


def test_the_stopped_run_settles_nothing_and_its_firing_waits():
    jobs = build_finalize_jobs_from_run_row(_stopped())("cancelled")
    assert "automation_settle" not in [j.hook_type for j in jobs]
    assert ledger_outcome(_stopped()) is None


def test_a_firing_no_resume_can_reach_any_more_reads_as_interrupted():
    assert ledger_outcome(_stopped(ago=RESTART_RESUME_TTL + timedelta(minutes=1))) is (
        Outcome.INTERRUPTED
    )


@pytest.mark.asyncio
async def test_the_turn_that_takes_over_finishes_the_firing():
    request = SimpleNamespace(query_type="system", retry_of_run_id=None)
    with patch(_LATEST, AsyncMock(return_value=_stopped())):
        firing = await carry.carried_firing(request, "thread-1")
    assert firing == _FIRING
    row = {
        "conversation_response_id": "run-2",
        "conversation_thread_id": "thread-1",
        "metadata": {"user_id": "u-1", **firing},
    }
    (job,) = [
        j
        for j in build_finalize_jobs_from_run_row(row)("completed")
        if j.hook_type == "automation_settle"
    ]
    assert job.payload["execution_id"] == "exec-1"


@pytest.mark.asyncio
async def test_a_users_message_after_the_stop_is_their_own_turn():
    request = SimpleNamespace(query_type=None, retry_of_run_id=None)
    latest = AsyncMock(return_value=_stopped())
    with patch(_LATEST, latest):
        assert await carry.carried_firing(request, "thread-1") == {}
    latest.assert_not_awaited()


@pytest.mark.asyncio
async def test_nothing_is_carried_past_an_ordinary_stop():
    request = SimpleNamespace(query_type="system", retry_of_run_id=None)
    stop = _stopped()
    stop["metadata"].pop(RESTART_INTERRUPTED_META)
    with patch(_LATEST, AsyncMock(return_value=stop)):
        assert await carry.carried_firing(request, "thread-1") == {}


@pytest.mark.asyncio
async def test_a_retry_of_the_stopped_input_takes_the_firing_over():
    """A turn cut before its input reached the graph runs again through
    /retry, and the firing ends as that attempt does."""
    request = SimpleNamespace(query_type=None, retry_of_run_id="run-1")
    get_run = AsyncMock(return_value=_stopped())
    with patch("src.server.database.runs.lifecycle.get_run", get_run):
        assert await carry.carried_firing(request, "thread-1") == _FIRING
    get_run.assert_awaited_once_with("run-1")


@pytest.mark.asyncio
async def test_a_held_delivery_goes_with_the_firing():
    from src.server.services import automation_delivery

    delivery = automation_delivery.Delivery(id="exec-1", targets=[])
    stopped = _stopped(**automation_delivery.run_metadata(delivery))
    request = SimpleNamespace(query_type="system", retry_of_run_id=None, hitl_response=None)
    held = SimpleNamespace(held=True)
    with (
        patch(_LATEST, AsyncMock(return_value=stopped)),
        patch.object(automation_delivery, "read_stamp", return_value=held),
        patch.object(automation_delivery, "turn_configurable", return_value={"x": 1}) as conf,
    ):
        assert await carry.carried_delivery(request, "thread-1") == {"x": 1}
    conf.assert_called_once()


@pytest.mark.asyncio
async def test_a_firing_no_turn_took_over_ends_interrupted():
    row = {
        "automation_execution_id": "exec-1",
        "automation_id": "auto-1",
        "conversation_thread_id": "thread-1",
        "user_id": "u-1",
        "workspace_id": "ws-1",
    }
    settle = AsyncMock(return_value=True)
    with (
        patch.object(
            automation_settlement.exec_db,
            "get_running_execution_for_run",
            AsyncMock(return_value=row),
        ),
        patch.object(
            automation_settlement.exec_db, "get_settling_run", AsyncMock(return_value=_stopped())
        ),
        patch.object(
            automation_settlement.auto_db,
            "get_automation",
            AsyncMock(return_value={"automation_id": "auto-1"}),
        ),
        patch.object(automation_settlement, "settle", settle),
    ):
        user_turn = {"conversation_response_id": "run-2", "metadata": {}}
        with patch(_LATEST, AsyncMock(return_value=user_turn)):
            await automation_settlement.settle_unresumed("run-1")
        args, kwargs = settle.await_args
        assert args[1:] == ("exec-1", Outcome.INTERRUPTED)
        assert kwargs["run_id"] == "run-1"
        # A newer turn that took the firing over settles it, though it has
        # not relinked it yet.
        settle.reset_mock()
        taker = {"conversation_response_id": "run-2", "metadata": {**_FIRING}}
        with patch(_LATEST, AsyncMock(return_value=taker)):
            await automation_settlement.settle_unresumed("run-1")
        settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_firing_a_turn_took_over_is_left_to_it():
    settle = AsyncMock()
    with (
        patch.object(
            automation_settlement.exec_db,
            "get_running_execution_for_run",
            AsyncMock(return_value=None),
        ),
        patch.object(automation_settlement, "settle", settle),
    ):
        await automation_settlement.settle_unresumed("run-1")
    settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_retry_of_the_stopped_input_sends_for_its_held_run():
    from src.server.services import automation_delivery

    request = SimpleNamespace(query_type=None, retry_of_run_id="run-1", hitl_response=None)
    get_run = AsyncMock(return_value=_stopped())
    with (
        patch("src.server.database.runs.lifecycle.get_run", get_run),
        patch.object(automation_delivery, "read_stamp", return_value=SimpleNamespace(held=True)),
        patch.object(automation_delivery, "turn_configurable", return_value={"x": 1}),
    ):
        assert await carry.carried_delivery(request, "thread-1") == {"x": 1}
    get_run.assert_awaited_once_with("run-1")


_LATEST_ATTEMPT = "src.server.database.runs.lifecycle.get_latest_attempt"
_ACTIVE = "src.server.database.runs.lifecycle.get_active_run"


@pytest.mark.asyncio
async def test_a_pinned_thread_waits_for_the_resume_of_its_stopped_turn():
    """A firing on a pinned thread lines up behind the resume, so it never
    takes the resume's place; once the resume is declined or too late, the
    thread is free."""
    from src.server.services import automation_executor

    mutating = AsyncMock(return_value=False)
    runner = SimpleNamespace(is_mutating=mutating)
    with (
        patch(_ACTIVE, AsyncMock(return_value=None)),
        patch(
            "src.server.services.thread_mutation.ThreadMutationRunner.get_instance",
            return_value=runner,
        ),
    ):
        with patch(_LATEST_ATTEMPT, AsyncMock(return_value=_stopped())):
            assert await automation_executor._thread_busy("thread-1") is True
        late = _stopped(ago=RESTART_RESUME_TTL + timedelta(minutes=1))
        with patch(_LATEST_ATTEMPT, AsyncMock(return_value=late)):
            assert await automation_executor._thread_busy("thread-1") is False
        declined = _stopped()
        declined["metadata"].pop(RESTART_INTERRUPTED_META)
        with patch(_LATEST_ATTEMPT, AsyncMock(return_value=declined)):
            assert await automation_executor._thread_busy("thread-1") is False


@pytest.mark.asyncio
async def test_a_draining_process_hands_a_firing_on_with_its_thread():
    from src.server.services import automation_executor

    from src.server.database.runs.hook_jobs import AUTOMATION_HANDOFF_HOOK

    hand = AsyncMock(return_value=True)
    with patch.object(automation_executor.exec_db, "hand_off_execution", hand):
        await automation_executor.hand_off(
            {"automation_id": "auto-1", "user_id": "u-1"}, "exec-1", "thread-1",
            from_status="running",
        )
    kwargs = hand.await_args.kwargs
    assert hand.await_args.args == ("exec-1",)
    assert (kwargs["from_statuses"], kwargs["thread_id"]) == (("running",), "thread-1")
    assert kwargs["job"].hook_type == AUTOMATION_HANDOFF_HOOK
    assert kwargs["job"].payload == {
        "execution_id": "exec-1", "automation_id": "auto-1", "user_id": "u-1"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("draining", "age"),
    [(False, timedelta(seconds=5)), (True, timedelta(seconds=5)), (False, timedelta(minutes=6))],
    ids=["next_process", "still_draining", "stale"],
)
async def test_the_next_process_starts_a_handed_firing(draining, age):
    from src.server.services import automation_executor
    from src.server.services.automation_scheduler import AutomationScheduler

    job = {
        "conversation_thread_id": "thread-1",
        "created_at": datetime.now(timezone.utc) - age,
        "payload": {"execution_id": "exec-1", "automation_id": "auto-1", "user_id": "u-1"},
    }
    scheduler = SimpleNamespace(dispatch=MagicMock())
    with (
        patch("src.server.services.runs.restart_drain.draining", return_value=draining),
        patch.object(
            automation_executor.auto_db,
            "get_automation",
            AsyncMock(return_value={"automation_id": "auto-1"}),
        ),
        patch.object(AutomationScheduler, "get_instance", return_value=scheduler),
    ):
        if draining:
            with pytest.raises(RuntimeError):
                await automation_executor.start_handed_off(job)
        else:
            await automation_executor.start_handed_off(job)
    if draining or age > timedelta(minutes=5):
        # A stale hand-off is the sweep's to settle.
        scheduler.dispatch.assert_not_called()
    else:
        args, _ = scheduler.dispatch.call_args
        assert args == ({"automation_id": "auto-1"}, "exec-1")

