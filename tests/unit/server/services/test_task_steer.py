"""Handing parked task steers to the task's live run (``task_steer._divert``).

A steer parked to resume its task goes into the task's live run instead when
one exists. The hand-off is gated on that run's row like admission, so a run
ending at the same moment makes it miss. A miss is read again rather than
taken as done: acking a row still under the caller's fence would close it
with no outcome, a lost steer. A task that keeps restarting under the
hand-off raises after ``_DIVERT_LAPS`` laps, so the drainer nacks and tries
later.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.home_workspace import get_home_workspace_id
from src.server.services.claude_code.hosted import OnCloud, OnOwn, Refused
from src.server.services.llm.claude_code import Admitted
from src.server.services.report_back import task_steer
from src.server.services.report_back.notify_turn import TURN_ROOM_WAIT_S

_OUTBOX = "src.server.database.runs.outbox"
_ACTIVE = "src.server.database.runs.subagent_runs.get_active_task_run"
_FENCE = ("claimed", 2)


def _job(n: int) -> dict:
    return {
        "hook_outbox_id": f"j-{n}",
        "payload": {"task_id": "k7Xm2p", "content": f"note {n}", "input_id": f"i-{n}"},
    }


def _run(task_run_id: str, *, stopped: bool = False) -> dict:
    return {
        "task_run_id": task_run_id,
        "cancel_requested_at": "2026-01-01T00:00:00Z" if stopped else None,
    }


class _Outbox:
    """The three outbox writes ``_divert`` makes, scripted per lap."""

    def __init__(self, moved: list[list[str]], ours: list[list[str]]) -> None:
        self.retarget = AsyncMock(side_effect=moved)
        self.fenced = AsyncMock(side_effect=ours)
        self.settle = AsyncMock(return_value=[])

    def patches(self):
        return (
            patch(f"{_OUTBOX}.retarget_task_steers", new=self.retarget),
            patch(f"{_OUTBOX}.fenced_task_steers", new=self.fenced),
            patch(f"{_OUTBOX}.settle_task_steers", new=self.settle),
        )

    def run_ids(self) -> list[str]:
        return [c.kwargs["task_run_id"] for c in self.retarget.await_args_list]


async def _divert(outbox: _Outbox, runs: list, jobs: list[dict], *, unresumable=None):
    retarget, fenced, settle = outbox.patches()
    with (
        retarget,
        fenced,
        settle,
        patch(_ACTIVE, new=AsyncMock(side_effect=runs)),
        patch.object(
            task_steer, "unresumable", new=AsyncMock(return_value=unresumable)
        ),
    ):
        return await task_steer._divert("t-1", "k7Xm2p", jobs, fence=_FENCE)


@pytest.mark.asyncio
async def test_a_hand_off_the_live_run_took_is_done():
    outbox = _Outbox(moved=[["j-1", "j-2"]], ours=[[]])

    assert await _divert(outbox, [_run("tr-1")], [_job(1), _job(2)]) is True

    outbox.retarget.assert_awaited_once_with(
        ["j-1", "j-2"],
        thread_id="t-1",
        task_id="k7Xm2p",
        task_run_id="tr-1",
        fence=_FENCE,
    )
    outbox.settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_run_ending_under_the_hand_off_reads_the_task_again():
    """The first run ended before the hand-off; the task's next run takes it."""
    outbox = _Outbox(moved=[[], ["j-1"]], ours=[["j-1"], []])

    assert await _divert(outbox, [_run("tr-1"), _run("tr-2")], [_job(1)]) is True

    assert outbox.run_ids() == ["tr-1", "tr-2"]
    outbox.settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_miss_with_no_run_left_goes_on_to_resume():
    outbox = _Outbox(moved=[[]], ours=[["j-1"]])

    assert await _divert(outbox, [_run("tr-1"), None], [_job(1)]) is False

    outbox.settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_the_rows_still_under_the_fence_carry_on():
    """j-1 went to the live run; j-2 missed and is still ours, and with the
    task now unable to resume it alone comes back."""
    outbox = _Outbox(moved=[["j-1"]], ours=[["j-2"]])

    handled = await _divert(
        outbox, [_run("tr-1"), None], [_job(1), _job(2)], unresumable="stopped"
    )

    assert handled is True
    outbox.fenced.assert_awaited_once_with(["j-2"], fence=_FENCE)
    outbox.settle.assert_awaited_once_with(["j-2"], fence=_FENCE)


@pytest.mark.asyncio
async def test_a_missed_row_someone_else_holds_is_theirs():
    outbox = _Outbox(moved=[[]], ours=[[]])

    assert await _divert(outbox, [_run("tr-1")], [_job(1)]) is True

    outbox.retarget.assert_awaited_once()
    outbox.settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_stopped_run_is_never_handed_steers():
    outbox = _Outbox(moved=[], ours=[])

    handled = await _divert(
        outbox, [_run("tr-1", stopped=True)], [_job(1)], unresumable="stopped"
    )

    assert handled is True
    outbox.retarget.assert_not_awaited()
    outbox.settle.assert_awaited_once_with(["j-1"], fence=_FENCE)


@pytest.mark.asyncio
async def test_a_task_that_keeps_ending_under_the_hand_off_raises():
    laps = task_steer._DIVERT_LAPS
    outbox = _Outbox(moved=[[]] * laps, ours=[["j-1"]] * laps)
    runs = [_run(f"tr-{n}") for n in range(laps)]

    with pytest.raises(RuntimeError):
        await _divert(outbox, runs, [_job(1)])

    assert outbox.run_ids() == [f"tr-{n}" for n in range(laps)]
    # Neither closed nor counted handled: the row stays under its fence.
    outbox.settle.assert_not_awaited()


# ---------------------------------------------------------------------------
# What the callers do with the raise
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_executor_lets_the_raise_through_to_nack():
    """Returning would ack a row still claimed with no outcome."""
    job = {
        "hook_outbox_id": "j-1",
        "attempts": 2,
        "idempotency_key": "steer:t-1:k7Xm2p:i-1",
        "conversation_thread_id": "t-1",
        "ordering_key": "t-1:steer",
    }
    row = {"status": "claimed", "attempts": 2, "payload": {"task_id": "k7Xm2p"}}
    divert = AsyncMock(side_effect=RuntimeError("kept ending"))
    with (
        patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock(return_value=row)),
        patch.object(task_steer, "_divert", new=divert),
    ):
        with pytest.raises(RuntimeError):
            await task_steer.execute_task_steer(job)

    assert divert.await_args.kwargs == {"fence": _FENCE}


@pytest.mark.asyncio
async def test_a_live_turn_leaves_the_rows_parked_for_its_next_take():
    jobs = [_job(1)]
    hold = AsyncMock(return_value=["j-1"])
    with (
        patch(f"{_OUTBOX}.list_resumable_task_steers", new=AsyncMock(return_value=jobs)),
        patch(f"{_OUTBOX}.hold_jobs", new=hold),
        patch.object(
            task_steer, "_divert", new=AsyncMock(side_effect=RuntimeError("kept ending"))
        ),
    ):
        assert await task_steer.take_resumes("t-1", "run-A") == []

    hold.assert_not_awaited()


# ---------------------------------------------------------------------------
# Out of attempts: the ceiling settles the row (``outbox._exhaust_steers``)
# ---------------------------------------------------------------------------

_NOTIFY = "src.server.services.report_back.notify_turn"


def _claimed_job(attempts: int) -> tuple[dict, dict]:
    job = {
        "hook_outbox_id": "j-1",
        "attempts": attempts,
        "idempotency_key": "steer:t-1:k7Xm2p:i-1",
        "conversation_thread_id": "t-1",
        "ordering_key": "t-1:steer",
    }
    payload = {
        "target": "task",
        "sender": "user",
        "task_id": "k7Xm2p",
        "task_run_id": "tr-1",
        "user_id": "u-1",
        "workspace_id": "w-1",
    }
    return job, {"status": "claimed", "attempts": attempts, "payload": payload}


@pytest.mark.asyncio
async def test_the_claim_before_the_ceiling_is_still_tried():
    """No early give-up: the ceiling itself settles the row now."""
    job, row = _claimed_job(4)
    busy = AsyncMock(return_value=True)
    settle = AsyncMock()
    with (
        patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock(return_value=row)),
        patch.object(task_steer, "_divert", new=AsyncMock(return_value=False)),
        patch.object(task_steer, "_settle", new=settle),
        patch(f"{_NOTIFY}.park_while_thread_busy", new=busy),
    ):
        await task_steer.execute_task_steer(job)

    busy.assert_awaited_once()
    settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_repark_at_the_ceiling_refreshes_the_launching_turn():
    job, row = _claimed_job(5)
    refresh = AsyncMock()
    with (
        patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock(return_value=row)),
        patch(f"{_OUTBOX}.defer_claimed_job", new=AsyncMock(return_value="exhausted")),
        patch.object(task_steer, "_divert", new=AsyncMock(return_value=False)),
        patch.object(task_steer, "refresh_exhausted", new=refresh),
        patch(f"{_NOTIFY}.park_while_thread_busy", new=AsyncMock(return_value=False)),
        patch(
            f"{_NOTIFY}.post_notification_turn",
            new=AsyncMock(return_value=("cap", None)),
        ),
    ):
        await task_steer.execute_task_steer(job)

    refresh.assert_awaited_once()
    assert refresh.await_args.args[0]["hook_outbox_id"] == "j-1"


@pytest.mark.asyncio
async def test_no_room_for_another_claude_code_turn_asks_again_by_the_clock():
    job, row = _claimed_job(5)
    defer = AsyncMock(return_value="exhausted")
    room = AsyncMock(return_value=True)
    post = AsyncMock(return_value=("full", None))
    with (
        patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock(return_value=row)),
        patch(f"{_OUTBOX}.defer_claimed_job", new=defer),
        patch(f"{_OUTBOX}.wait_for_room", new=room),
        patch.object(task_steer, "_divert", new=AsyncMock(return_value=False)),
        patch(f"{_NOTIFY}.park_while_thread_busy", new=AsyncMock(return_value=False)),
        patch(f"{_NOTIFY}.post_notification_turn", new=post),
    ):
        await task_steer.execute_task_steer(job)

    assert post.await_args.kwargs["room_fails_fast"]
    room.assert_awaited_once_with("j-1", attempts=5, wait_s=TURN_ROOM_WAIT_S)
    defer.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_steer_waits_for_room_without_posting_a_turn_or_spending_one():
    """A turn posted while the computer is full would only be refused the
    resume, so the steer parks until a turn ends there, short of the attempts
    ceiling, with its turns unspent."""
    job, row = _claimed_job(2)
    post = AsyncMock(return_value=("posted", "run-1"))
    defer = AsyncMock(return_value="pending")
    merge = AsyncMock()
    with (
        patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock(return_value=row)),
        patch(f"{_OUTBOX}.defer_claimed_job", new=defer),
        patch(f"{_OUTBOX}.merge_job_payload", new=merge),
        patch.object(task_steer, "_divert", new=AsyncMock(return_value=False)),
        patch.object(task_steer, "_computer_full", new=AsyncMock(return_value=True)),
        patch(f"{_NOTIFY}.park_while_thread_busy", new=AsyncMock(return_value=False)),
        patch(f"{_NOTIFY}.post_notification_turn", new=post),
    ):
        await task_steer.execute_task_steer(job)

    post.assert_not_awaited()
    merge.assert_not_awaited()
    # By its own clock: on an idle thread the safety net would hand it back
    # at once, spending its attempts before any room frees up.
    assert defer.await_args.kwargs["wait_s"] == 2 * task_steer._ROOM_WAIT_S


@pytest.mark.asyncio
async def test_the_wait_for_a_full_computer_doubles_on_tries_spent_not_on_claims():
    # Claims spent waiting for room among the user's Claude Code turns
    # (``waited``) don't lengthen it.
    job, row = _claimed_job(7)
    row = {**row, "payload": {**row["payload"], "waited": 5}}
    defer = AsyncMock(return_value="pending")
    with (
        patch(f"{_OUTBOX}.get_steer_job", new=AsyncMock(return_value=row)),
        patch(f"{_OUTBOX}.defer_claimed_job", new=defer),
        patch.object(task_steer, "_divert", new=AsyncMock(return_value=False)),
        patch.object(task_steer, "_computer_full", new=AsyncMock(return_value=True)),
        patch(f"{_NOTIFY}.park_while_thread_busy", new=AsyncMock(return_value=False)),
    ):
        await task_steer.execute_task_steer(job)

    assert defer.await_args.kwargs["wait_s"] == 2 * task_steer._ROOM_WAIT_S


_PIN_RUNS = Admitted(saved="saved")
USER_MODELS = "src.server.services.llm.user_models"
_HOME = get_home_workspace_id("user-1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("on_claude_code", "live", "cap", "full", "thread_model", "pin", "rows", "pinned", "workspace"),
    [
        pytest.param(True, 2, 4, False, "claude-sonnet-5-5-code", _PIN_RUNS, True, True, "ws-1", id="room-for-both"),
        pytest.param(True, 3, 4, True, "claude-sonnet-5-5-code", _PIN_RUNS, True, True, "ws-1", id="room-for-the-turn-only"),
        pytest.param(True, 3, 4, False, "gpt-6.1-terra", _PIN_RUNS, True, True, "ws-1", id="room-for-the-run-on-a-thread-of-another-model"),
        pytest.param(True, 4, 4, True, "gpt-6.1-terra", _PIN_RUNS, True, True, "ws-1", id="no-room-on-a-thread-of-another-model"),
        pytest.param(True, 3, 4, True, None, _PIN_RUNS, True, True, "ws-1", id="a-thread-on-the-account-default"),
        pytest.param(False, 4, 4, False, "claude-sonnet-5-5-code", _PIN_RUNS, True, True, "ws-1", id="not-claude-code"),
        pytest.param(True, 4, None, False, "claude-sonnet-5-5-code", _PIN_RUNS, True, True, "ws-1", id="size-unknown"),
        pytest.param(
            True, 0, 1, False, "claude-sonnet-5-5-code", _PIN_RUNS, True, True, "ws-1",
            id="a-computer-that-never-fits-the-turn-and-the-run-posts-it",
        ),
        pytest.param(
            True, 4, 4, False, "gpt-6.1-terra", None, True, True, "ws-1",
            id="a-pin-that-runs-on-the-thread-model-now",
        ),
        pytest.param(
            True, 4, 4, False, "gpt-6.1-terra", _PIN_RUNS, False, True, "ws-1",
            id="claude-code-off-for-the-user-now",
        ),
        pytest.param(
            True, 4, 4, False, "gpt-6.1-terra", _PIN_RUNS, True, False, "ws-1",
            id="a-subagent-whose-pin-was-changed-since",
        ),
        pytest.param(
            True, 4, 4, True, "gpt-6.1-terra", _PIN_RUNS, True, True, get_home_workspace_id("user-1"),
            id="the-chief-of-staffs-pin-on-claude-code-takes-a-cli-too",
        ),
    ],
)
async def test_a_computer_is_full_for_a_resume_without_room_for_the_turn_and_the_run(
    on_claude_code, live, cap, full, thread_model, pin, rows, pinned, workspace
):
    """The posted turn takes a CLI of its own unless the thread names another
    model; a Claude Code pin's resume on a thread of another model takes one
    for the run alone, and none where the pin runs on the thread's model now.
    A thread naming none runs the account default, a Claude Code one here."""
    runs = "src.server.database.runs.subagent_runs"
    with (
        patch(f"{USER_MODELS}.get_model_preference", new=AsyncMock(return_value={"preferred_model": "claude-sonnet-5-5-code"})),
        patch("src.server.services.claude_code.flow.rows_on", new=AsyncMock(return_value=rows)),
        patch("src.server.services.llm.claude_code.admitted_there", new=AsyncMock(return_value=pin)),
        patch("src.server.services.llm.subagents.pinned_to_claude_code", new=AsyncMock(return_value=pinned)) as compiled,
        patch(f"{runs}.get_task", new=AsyncMock(return_value={"subagent_type": "research"})),
        patch(
            "src.server.database.conversation.threads_read.get_thread_auth_meta",
            new=AsyncMock(return_value={"llm_model": thread_model}),
        ),
        patch(f"{runs}.ran_on_claude_code", new=AsyncMock(return_value=on_claude_code)),
        patch(f"{runs}.live_clis", new=AsyncMock(return_value=live)),
        patch(
            "src.server.services.claude_code.hosted.cli_placement",
            new=AsyncMock(return_value=OnCloud(None, {"computer_id": "c-1"})),
        ),
        patch("src.server.services.claude_code.hosted.cli_cap", lambda computer: cap),
    ):
        assert await task_steer._computer_full("t-1", "k7Xm2p", "user-1", workspace) is full
    if compiled.await_count:
        role = "analyst" if workspace == "ws-1" else "chief_of_staff"
        compiled.assert_awaited_once_with("user-1", "research", role)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("held", "default", "workspace", "cos_runs", "full"),
    [
        pytest.param(None, "claude-sonnet-5-5-code", _HOME, None, False, id="home-on-a-claude-code-default-it-does-not-run"),
        pytest.param(None, "claude-sonnet-5-5-code", _HOME, object(), True, id="home-on-a-claude-code-default-it-runs"),
        pytest.param("model-gone", "claude-sonnet-5-5-code", _HOME, None, False, id="home-whose-model-left-the-catalog"),
        pytest.param(None, "claude-sonnet-5-5-code", "ws-1", None, True, id="an-analyst-on-a-claude-code-default"),
        pytest.param(None, "gpt-6.1-terra", "ws-1", None, False, id="an-analyst-on-another-default"),
        pytest.param(None, "gpt-6.1-terra", _HOME, object(), False, id="home-on-another-default"),
    ],
)
async def test_a_thread_naming_no_model_is_judged_on_the_default_its_posted_turn_starts_on(
    held, default, workspace, cos_runs, full
):
    """The posted turn names no model, so it runs the account default, which
    the Chief of Staff runs only where ``chief_of_staff_default_runs`` lets
    it; elsewhere it starts on the deployment's, and only a Claude Code pin's
    resume takes a CLI."""
    runs = "src.server.database.runs.subagent_runs"
    cos = AsyncMock(return_value=cos_runs)
    with (
        patch(f"{USER_MODELS}.get_model_preference", new=AsyncMock(return_value={"preferred_model": default})),
        patch("src.server.services.llm.claude_code.chief_of_staff_default_runs", new=cos),
        patch("src.server.services.claude_code.flow.rows_on", new=AsyncMock(return_value=True)),
        patch("src.server.services.llm.claude_code.admitted_there", new=AsyncMock(return_value=_PIN_RUNS)),
        patch("src.server.services.llm.subagents.pinned_to_claude_code", new=AsyncMock(return_value=True)),
        patch(f"{runs}.get_task", new=AsyncMock(return_value={"subagent_type": "research"})),
        patch(
            "src.server.database.conversation.threads_read.get_thread_auth_meta",
            new=AsyncMock(return_value={"llm_model": held}),
        ),
        patch(f"{runs}.ran_on_claude_code", new=AsyncMock(return_value=True)),
        patch(f"{runs}.live_clis", new=AsyncMock(return_value=3)),
        patch(
            "src.server.services.claude_code.hosted.cli_placement",
            new=AsyncMock(return_value=OnCloud(None, {"computer_id": "c-1"})),
        ),
        patch("src.server.services.claude_code.hosted.cli_cap", lambda computer: 4),
    ):
        assert await task_steer._computer_full("t-1", "k7Xm2p", "user-1", workspace) is full
    if workspace == _HOME and default.endswith("-code"):
        cos.assert_awaited_once_with("user-1", workspace)
    else:
        cos.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_resume_is_held_for_room_where_its_cli_would_run_now():
    """Counted, and capped, on the computer admission places the resume's CLI
    on today, the own computer it picks for the workspace, whatever computer
    the task's last run used; a placement that refuses the turn posts it,
    and the turn says why."""
    runs = "src.server.database.runs.subagent_runs"
    studio = {"computer_id": "c-9", "kind": "ssh"}
    live = AsyncMock(return_value=1)
    placement = AsyncMock(return_value=OnOwn(studio, picked=True))
    with (
        patch(f"{USER_MODELS}.get_model_preference", new=AsyncMock(return_value={"preferred_model": "claude-sonnet-5-5-code"})),
        patch("src.server.services.claude_code.flow.rows_on", new=AsyncMock(return_value=True)),
        patch(f"{runs}.ran_on_claude_code", new=AsyncMock(return_value=True)),
        patch(f"{runs}.live_clis", new=live),
        patch("src.server.database.conversation.threads_read.get_thread_auth_meta", new=AsyncMock(return_value={})),
        patch("src.server.services.claude_code.hosted.cli_placement", new=placement),
        patch("src.server.services.claude_code.hosted.cli_cap", lambda computer: 2 if computer is studio else 8),
    ):
        assert await task_steer._computer_full("t-1", "k7Xm2p", "user-1", "ws-1") is True
        live.assert_awaited_once_with("c-9")
        placement.assert_awaited_once_with("user-1", "ws-1", token=True)
        placement.return_value = Refused("cli_computer_offline", {"name": "Studio"})
        assert await task_steer._computer_full("t-1", "k7Xm2p", "user-1", "ws-1") is False


# Handing a resume's steers to the run it admitted
#
# The run the resume admits reads them and confirms them itself. A hand-off
# that fails or moves nothing refuses the resume, so the launching run still
# holds them, gives them back, and counts the refusal, as for any refused
# resume.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_hand_off_moves_the_rows_onto_the_resumed_run():
    hand_off = AsyncMock(return_value=["j-1"])
    with patch(f"{_OUTBOX}.hand_off_task_resumes", new=hand_off):
        moved = await task_steer.hand_off_resumes(
            "t-1", "run-A", "k7Xm2p", "tr-2", ["j-1"]
        )

    assert moved == ["j-1"]
    hand_off.assert_awaited_once_with(
        ["j-1"], run_id="run-A", thread_id="t-1", task_id="k7Xm2p", task_run_id="tr-2"
    )


async def _resume_through_the_inbox(
    hand_off: AsyncMock, *, busy: bool = False, final: bool = False, settle: AsyncMock | None = None
) -> AsyncMock:
    """A main turn's resume of k7Xm2p with the server inbox, the Task call
    refusing as ``_handle_resume`` does when the hand-off fails. Returns the
    refusal-counting release."""
    from types import SimpleNamespace

    from langchain_core.messages import ToolMessage

    from ptc_agent.agent.middleware.background_subagent.context import (
        hand_off_steer_resume,
        steer_resume_busy,
        steer_resume_final,
    )
    from ptc_agent.agent.middleware.steering import (
        SteeringMiddleware,
        TaskResume,
        _resume_call,
    )
    from src.server.services.report_back.steer import SteeredReportInbox

    call = _resume_call(
        [TaskResume(task_id="k7Xm2p", job_ids=["j-1"], entries=[_job(1)["payload"]])],
        "run-A",
    )
    request = SimpleNamespace(
        tool_call=call.tool_calls[0],
        state={"messages": [call]},
        runtime=SimpleNamespace(config={"configurable": {"thread_id": "t-1"}}),
    )

    async def task_call(_request):
        if busy:
            steer_resume_busy("k7Xm2p")
        elif final:
            steer_resume_final("k7Xm2p")
        elif await hand_off_steer_resume("k7Xm2p", "tr-2"):
            raise AssertionError("the hand-off should have failed")
        return ToolMessage(content="Error", tool_call_id="c", status="error")

    release = AsyncMock(return_value=(["j-1"], []))
    with (
        patch(f"{_OUTBOX}.hand_off_task_resumes", new=hand_off),
        patch(f"{_OUTBOX}.list_steer_jobs", new=AsyncMock(return_value=[_job(1)])),
        patch(f"{_OUTBOX}.release_task_resumes", new=release),
        patch(f"{_OUTBOX}.confirm_jobs", new=AsyncMock()) as confirm,
        patch.object(task_steer, "_divert", new=AsyncMock(return_value=False)),
        patch.object(task_steer, "_settle", new=settle or AsyncMock()),
    ):
        await SteeringMiddleware(SteeredReportInbox()).awrap_tool_call(
            request, task_call
        )
    confirm.assert_not_awaited()
    return release


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "refreshed"),
    [
        ({"target": "task", "task_run_id": "tr-1"}, True),
        ({"target": "task", "sender": "chief_of_staff", "task_run_id": "tr-1"}, False),
        ({"target": "thread", "task_run_id": "tr-1"}, False),
    ],
)
async def test_only_a_users_returned_task_steer_refreshes(payload, refreshed):
    refresh = AsyncMock()
    with patch.object(task_steer, "_refresh_launching_turns", new=refresh):
        await task_steer.refresh_exhausted({"hook_outbox_id": "j-1", "payload": payload})

    assert refresh.await_count == (1 if refreshed else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "hand_off",
    [
        pytest.param(AsyncMock(return_value=[]), id="moved-nothing"),
        pytest.param(AsyncMock(side_effect=RuntimeError("db down")), id="raised"),
    ],
)
async def test_a_failed_hand_off_refuses_the_resume_and_counts_it(hand_off):
    release = await _resume_through_the_inbox(hand_off)

    release.assert_awaited_once_with(
        ["j-1"],
        run_id="run-A",
        max_refusals=task_steer.MAX_RESUME_REFUSALS,
        counted=True,
    )


@pytest.mark.asyncio
async def test_a_resume_refused_for_as_long_as_the_task_lasts_settles_its_steers():
    """Another model wrote the task's last reply, so no later turn can resume
    it on Claude Code: its steers are closed now rather than offered to the
    next turn, which would only be refused again."""
    settle = AsyncMock()
    release = await _resume_through_the_inbox(AsyncMock(), final=True, settle=settle)

    settle.assert_awaited_once()
    assert settle.await_args.kwargs["fence"] == ("held", "run-A")
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_resume_refused_while_the_computer_is_busy_gives_the_steers_back_uncounted():
    """The refusal lifts once a CLI on the computer ends, so a full computer
    never uses up the refusals the steers get."""
    hand_off = AsyncMock(return_value=["j-1"])
    release = await _resume_through_the_inbox(hand_off, busy=True)

    hand_off.assert_not_awaited()
    release.assert_awaited_once_with(
        ["j-1"],
        run_id="run-A",
        max_refusals=task_steer.MAX_RESUME_REFUSALS,
        counted=False,
    )
