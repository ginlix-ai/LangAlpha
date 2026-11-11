"""Whether the work a graceful restart stopped is still owed a resume, what
the agent is told, and how the job leaves the thread's parked reports.

Every verdict reads the ledger: the anchor run the job names, the thread's
latest run, and the subagent tasks whose latest run a restart stopped.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.contracts.status import RESTART_INTERRUPTED_META, RESTART_RESUME_TTL
from src.server.services.report_back import notify_turn, restart
from src.server.services.report_back.notify_turn import NotOwed
from src.server.services.runs.pending_input import PENDING_INPUT_KEY

_STATE = "src.server.database.runs.restart_resume.read_resume_state"
_TASKS = "src.server.database.runs.restart_resume.list_restart_stopped_tasks"
_LATEST = "src.server.database.runs.lifecycle.get_latest_attempt"
_UNSETTLED = "src.server.database.runs.restart_resume.has_unsettled_tasks"
_STARTED = datetime(2026, 1, 1, tzinfo=timezone.utc)
_TASK = {"task_id": "abc123", "subagent_type": "research", "description": "Pull  the\n10-K"}


def _job(*, age=timedelta(minutes=1)):
    return {
        "hook_outbox_id": "job-1",
        "attempts": 1,
        "conversation_thread_id": "thread-1",
        "created_at": datetime.now(timezone.utc) - age,
        "payload": {"run_id": "run-1"},
    }


def _state(*, latest="run-1", status="cancelled", no_turn=False, **meta):
    return {
        "user_id": "u-1",
        "workspace_id": "ws-1",
        "no_turn": no_turn,
        "latest_run_id": latest,
        "status": status,
        "metadata": {RESTART_INTERRUPTED_META: True, **meta},
        "created_at": _STARTED,
    }


async def _verdict(state, tasks=(), job=None, *, card_waits=False, unsettled=False):
    with (
        patch(_STATE, AsyncMock(return_value=state)),
        patch(_TASKS, AsyncMock(return_value=list(tasks))),
        patch(_UNSETTLED, AsyncMock(return_value=unsettled)),
        patch.object(notify_turn, "_card_waits", AsyncMock(return_value=card_waits)),
    ):
        return await restart.resume_verdict(job or _job())


@pytest.mark.asyncio
async def test_an_interrupted_turn_is_resumed_in_a_hidden_turn():
    verdict = await _verdict(_state())
    assert isinstance(verdict, restart.OwedResume)
    assert (verdict.user_id, verdict.workspace_id) == ("u-1", "ws-1")
    assert verdict.interrupted and not verdict.tasks
    assert verdict.retry_run_id is None
    assert verdict.report_source is None


@pytest.mark.asyncio
async def test_input_that_never_reached_the_graph_runs_again():
    verdict = await _verdict(_state(**{PENDING_INPUT_KEY: {"checkpoint_id": "cp-1"}}))
    assert verdict.retry_run_id == "run-1"


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [{"resend": True}, {"context": True}])
async def test_input_only_the_client_holds_is_not_resumed(held):
    # /retry would refuse it for want of what the client held.
    verdict = await _verdict(_state(**{PENDING_INPUT_KEY: held}))
    assert isinstance(verdict, NotOwed)


@pytest.mark.asyncio
async def test_a_turn_the_user_stopped_offers_back_none_of_its_subagents():
    # A user's stop leaves no restart stamp.
    stopped = _state(cancelled_by_user=True)
    stopped["metadata"].pop(RESTART_INTERRUPTED_META)
    own = {**_TASK, "parent_run_id": "run-1"}
    assert isinstance(await _verdict(stopped, tasks=[own]), NotOwed)
    # A subagent an earlier turn left running is not the stop's to end.
    earlier = {**_TASK, "parent_run_id": "run-0"}
    verdict = await _verdict(stopped, tasks=[own, earlier])
    assert not verdict.interrupted
    assert [task.task_id for task in verdict.tasks] == ["abc123"]
    assert len(verdict.tasks) == 1


@pytest.mark.asyncio
async def test_a_thread_waiting_on_a_card_is_left_to_the_user():
    assert isinstance(await _verdict(_state(), card_waits=True), NotOwed)
    state = _state(status="interrupted")
    assert isinstance(await _verdict(state, tasks=[_TASK]), NotOwed)


@pytest.mark.asyncio
async def test_subagents_a_finished_turn_left_running_are_offered_back():
    state = _state(status="completed")
    state["metadata"] = {}
    verdict = await _verdict(state, tasks=[{**_TASK, "parent_run_id": "run-0"}])
    assert not verdict.interrupted
    assert verdict.tasks == (restart.StoppedTask(**_TASK),)


@pytest.mark.asyncio
async def test_a_run_not_ended_yet_is_asked_about_again():
    # Its finalize writes this same job, which an ack now would drop.
    verdict = await _verdict(_state(status="in_progress"), tasks=[_TASK])
    assert isinstance(verdict, notify_turn.NotYet)


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [{"resend": True}, {"context": True}, {}])
async def test_a_firing_turn_stopped_before_its_input_ran_fires_again(held):
    state = _state(
        **{PENDING_INPUT_KEY: held}, automation_execution_id="exec-1", automation_id="auto-1"
    )
    assert isinstance(await _verdict(state), restart.FiringRunsAgain)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "job"),
    [
        (None, None),
        (_state(no_turn=True), None),
        (_state(latest="run-2"), None),
        (_state(), _job(age=RESTART_RESUME_TTL + timedelta(minutes=1))),
    ],
    ids=["thread_gone", "archived_or_deleted", "newer_turn", "too_late"],
)
async def test_nothing_is_owed(state, job):
    verdict = await _verdict(state, job=job)
    assert isinstance(verdict, NotOwed)
    assert verdict.wake is False


def _message(interrupted, tasks=()):
    return restart.OwedResume(
        user_id="u-1",
        workspace_id="ws-1",
        interrupted=interrupted,
        tasks=tuple(restart.StoppedTask(**task) for task in tasks),
    ).message(steered=False)


def test_the_resume_message_for_an_interrupted_turn():
    assert _message(True) == (
        "<system>\n"
        "Your previous turn was paused by a routine deployment before it "
        "finished. Continue where you left off. Before repeating a tool call "
        "from that turn, check whether it already took effect.\n"
        "</system>"
    )


def test_the_resume_message_lists_the_stopped_subagents():
    message = _message(True, [_TASK, {"task_id": "def456"}])
    assert message == (
        "<system>\n"
        "Your previous turn was paused by a routine deployment before it "
        "finished. Continue where you left off. Before repeating a tool call "
        "from that turn, check whether it already took effect.\n"
        "The deployment also paused these background subagents before they finished:\n"
        "- Task-abc123 (research): Pull the 10-K\n"
        "- Task-def456 (subagent)\n"
        "Their prior work is preserved. Resume each one still needed with "
        'Task(action="resume", task_id=...).\n'
        "</system>"
    )
    assert _message(False, [_TASK]).splitlines()[1] == (
        "A routine deployment paused these background subagents before they finished:"
    )


@pytest.mark.asyncio
async def test_a_subagent_still_settling_its_stop_is_waited_for():
    fresh = _job(age=timedelta(seconds=5))
    verdict = await _verdict(_state(), job=fresh, unsettled=True)
    assert isinstance(verdict, notify_turn.NotYet)
    # Past the grace, one still running died with the process that stopped it.
    assert isinstance(await _verdict(_state(), unsettled=True), restart.OwedResume)


@pytest.mark.asyncio
@pytest.mark.parametrize("tasks", [(), (_TASK,)], ids=["alone", "with_subagents"])
async def test_a_retry_resume_posts_the_retry_route(tasks):
    owed = restart.OwedResume(
        user_id="u-1",
        workspace_id="ws-1",
        interrupted=True,
        tasks=tuple(restart.StoppedTask(**task) for task in tasks),
        retry_run_id="run-1",
    )
    pipeline = notify_turn.ReportPipeline(
        **{**restart.RESTART_PIPELINE.__dict__, "verdict": AsyncMock(return_value=owed)}
    )
    post = AsyncMock(return_value=("deleted", None))
    with (
        patch(_LATEST, AsyncMock(return_value=None)),
        patch.object(notify_turn, "park_while_thread_busy", AsyncMock(return_value=False)),
        patch.object(notify_turn, "post_notification_turn", post),
    ):
        await notify_turn.run_report_back(_job(), pipeline)
    kwargs = post.await_args.kwargs
    assert kwargs["route"] == "retry"
    body = dict(kwargs["body"])
    directives = body.pop("additional_context", [])
    assert body == {
        "run_id": "run-1",
        "workspace_id": "ws-1",
        "request_key": pipeline.request_key("job-1"),
    }
    # The input runs again as it was sent, so the stopped subagents ride
    # beside it.
    assert [d["type"] for d in directives] == ["directive"] * len(tasks)
    if tasks:
        assert directives[0]["content"].startswith("A routine deployment paused")
        assert "Task-abc123 (research)" in directives[0]["content"]


@pytest.mark.asyncio
async def test_a_resume_acks_once_its_turn_is_admitted():
    """Held through the resumed turn, the resumes of a restart that stopped
    many threads would queue past their TTL."""
    owed = restart.OwedResume(user_id="u-1", workspace_id="ws-1", interrupted=True)
    pipeline = notify_turn.ReportPipeline(
        **{**restart.RESTART_PIPELINE.__dict__, "verdict": AsyncMock(return_value=owed)}
    )
    terminal = AsyncMock()
    with (
        patch(_LATEST, AsyncMock(return_value=None)),
        patch.object(notify_turn, "post_notification_turn", AsyncMock(return_value=("ok", "rb-1"))),
        patch("src.server.database.runs.outbox.merge_job_payload", AsyncMock()),
        patch.object(notify_turn, "await_run_terminal", terminal),
    ):
        assert await notify_turn.run_report_back(_job(), pipeline) is None
    terminal.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_draining_process_hands_the_job_back():
    run = AsyncMock()
    with (
        patch("src.server.services.runs.restart_drain.draining", return_value=True),
        patch.object(restart, "run_report_back", run),
    ):
        with pytest.raises(RuntimeError):
            await restart.execute_restart_resume(_job())
    run.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("settled", [True, False], ids=["declined", "newer_run"])
@pytest.mark.parametrize("refire", [False, True], ids=["resume", "firing"])
async def test_a_declined_resume_settles_the_stopped_run(settled, refire):
    settle = AsyncMock(return_value=settled)
    unresumed = AsyncMock()
    drainer = MagicMock()
    declined = (restart.FiringRunsAgain if refire else NotOwed)("why", wake=False)
    with (
        patch("src.server.services.runs.restart_drain.draining", return_value=False),
        patch.object(restart, "run_report_back", AsyncMock(return_value=declined)),
        patch("src.server.database.runs.restart_resume.settle_declined", settle),
        patch("src.server.services.automation_settlement.settle_unresumed", unresumed),
        patch(
            "src.server.services.hook_outbox.HookOutboxDrainer.get_instance",
            return_value=drainer,
        ),
    ):
        await restart.execute_restart_resume(_job())
    settle.assert_awaited_once_with("thread-1", "run-1", refire=refire)
    assert drainer.nudge.call_count == int(settled)
    # The decline's own jobs settle the firing; a user's newer turn leaves
    # it to end interrupted here.
    assert unresumed.await_count == int(not settled)


@pytest.mark.asyncio
async def test_a_resume_that_ran_or_comes_back_settles_nothing():
    # The turn that took it over settles at its own end, and a parked or
    # deferred job runs again.
    settle = AsyncMock()
    unresumed = AsyncMock()
    with (
        patch("src.server.services.runs.restart_drain.draining", return_value=False),
        patch.object(restart, "run_report_back", AsyncMock(return_value=None)),
        patch("src.server.database.runs.restart_resume.settle_declined", settle),
        patch("src.server.services.automation_settlement.settle_unresumed", unresumed),
    ):
        await restart.execute_restart_resume(_job())
    settle.assert_not_awaited()
    unresumed.assert_not_awaited()


_HANDED_OFF = {
    "user_id": "u-1",
    "origin_home_thread_id": "home-1",
    "origin_dispatch_gen": "gen-1",
}


@pytest.mark.asyncio
async def test_settling_a_declined_stop_writes_what_its_finalize_held_back(
    mock_db_connection, mock_cursor
):
    from src.server.database.runs import restart_resume as rr_db

    mock_cursor.fetchone.return_value = {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "thread-1",
        "status": "cancelled",
        "run_seq": 3,
        "interrupt_reason": None,
        "metadata": {RESTART_INTERRUPTED_META: True, **_HANDED_OFF},
    }
    release = AsyncMock(return_value=1)
    enqueue = AsyncMock()
    with (
        patch.object(rr_db, "release_deferred_notifications", release),
        patch.object(rr_db, "enqueue_hooks", enqueue),
    ):
        assert await rr_db.settle_declined("thread-1", "run-1") is True
    release.assert_awaited_once()
    # It becomes an ordinary stop, which no later turn carries on.
    unstamp = mock_cursor.execute.await_args_list[-1].args
    assert "metadata - %s::text" in unstamp[0]
    assert unstamp[1] == (RESTART_INTERRUPTED_META, "run-1")
    jobs = enqueue.await_args.kwargs["jobs"]
    assert [j.hook_type for j in jobs] == ["report_back"]
    assert jobs[0].payload["final_status"] == "cancelled"


@pytest.mark.asyncio
async def test_a_firing_turn_declined_before_its_input_ran_is_handed_to_run_again(
    mock_db_connection, mock_cursor
):
    from src.server.database.runs import restart_resume as rr_db

    mock_cursor.fetchone.return_value = {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "thread-1",
        "status": "cancelled",
        "run_seq": 3,
        "interrupt_reason": None,
        "metadata": {
            RESTART_INTERRUPTED_META: True,
            "user_id": "u-1",
            "automation_execution_id": "exec-1",
            "automation_id": "auto-1",
        },
    }
    mock_cursor.rowcount = 1
    enqueue = AsyncMock()
    with (
        patch.object(rr_db, "release_deferred_notifications", AsyncMock()),
        patch.object(rr_db, "enqueue_hooks", enqueue),
    ):
        assert await rr_db.settle_declined("thread-1", "run-1", refire=True) is True
    sql, params = mock_cursor.execute.await_args_list[-1].args
    assert "SET status = 'pending'" in sql and params == ("exec-1", "run-1")
    types = [j.hook_type for j in enqueue.await_args.kwargs["jobs"]]
    assert "automation_settle" not in types and "automation_handoff" in types


@pytest.mark.asyncio
async def test_a_thread_holds_its_reports_for_the_resume():
    stopped = _state()
    stopped["cancel_requested_at"] = datetime.now(timezone.utc)
    with patch.object(notify_turn, "_card_waits", AsyncMock(return_value=False)):
        assert await notify_turn.holds_notifications("thread-1", stopped) is True
        stopped["cancel_requested_at"] -= RESTART_RESUME_TTL + timedelta(minutes=1)
        assert await notify_turn.holds_notifications("thread-1", stopped) is False


@pytest.mark.asyncio
async def test_a_resume_asks_again_by_the_clock_instead_of_parking():
    """The thread is held for the resume, so nothing ending on it releases a
    parked one: a run not ended yet, or a refused POST, asks again later."""
    owed = restart.OwedResume(user_id="u-1", workspace_id="ws-1", interrupted=True)
    waits = AsyncMock(return_value=True)
    for verdict, post_outcome in (
        (notify_turn.NotYet("not yet", wait_s=15), None),
        (owed, ("cap", None)),
    ):
        pipeline = notify_turn.ReportPipeline(
            **{**restart.RESTART_PIPELINE.__dict__, "verdict": AsyncMock(return_value=verdict)}
        )
        park = AsyncMock(return_value=True)
        with (
            patch(_LATEST, AsyncMock(return_value=None)),
            patch.object(notify_turn, "park_while_thread_busy", park),
            patch.object(notify_turn, "post_notification_turn", AsyncMock(return_value=post_outcome)),
            patch("src.server.database.runs.outbox.wait_for_room", waits),
        ):
            assert await notify_turn.run_report_back(_job(), pipeline) is None
        park.assert_not_awaited()
    assert waits.await_count == 2


@pytest.mark.asyncio
async def test_a_newer_run_settles_the_thread_itself(mock_db_connection, mock_cursor):
    from src.server.database.runs import restart_resume as rr_db

    mock_cursor.fetchone.return_value = {"conversation_response_id": "run-2"}
    release = AsyncMock()
    with patch.object(rr_db, "release_deferred_notifications", release):
        assert await rr_db.settle_declined("thread-1", "run-1") is False
    release.assert_not_awaited()
