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

from src.server.services.report_back import task_steer

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


async def _resume_through_the_inbox(hand_off: AsyncMock) -> AsyncMock:
    """A main turn's resume of k7Xm2p with the server inbox, the Task call
    refusing as ``_handle_resume`` does when the hand-off fails. Returns the
    refusal-counting release."""
    from types import SimpleNamespace

    from langchain_core.messages import ToolMessage

    from ptc_agent.agent.middleware.background_subagent.context import (
        hand_off_steer_resume,
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
        if await hand_off_steer_resume("k7Xm2p", "tr-2"):
            raise AssertionError("the hand-off should have failed")
        return ToolMessage(content="Error", tool_call_id="c", status="error")

    release = AsyncMock(return_value=(["j-1"], []))
    with (
        patch(f"{_OUTBOX}.hand_off_task_resumes", new=hand_off),
        patch(f"{_OUTBOX}.list_steer_jobs", new=AsyncMock(return_value=[_job(1)])),
        patch(f"{_OUTBOX}.release_task_resumes", new=release),
        patch(f"{_OUTBOX}.confirm_jobs", new=AsyncMock()) as confirm,
        patch.object(task_steer, "_divert", new=AsyncMock(return_value=False)),
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
        ["j-1"], run_id="run-A", max_refusals=task_steer.MAX_RESUME_REFUSALS
    )
