"""SteeredReportInbox: which owed reports a live turn takes in, the hold that
makes the taking exclusive, and the confirm that closes them once the message
carrying them is checkpointed."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.steering import SteeredReport
from src.server.services.report_back import steer
from src.server.services.report_back.notify_turn import NotOwed

_OUTBOX = "src.server.database.runs.outbox"
_BY_KEY = "src.server.database.runs.lifecycle.find_run_by_request_key"
_WAKE = "src.server.services.report_back.subagent.publish_cleared_wake"


@pytest.fixture(autouse=True)
def _no_admitted_turn():
    with patch(_BY_KEY, new=AsyncMock(return_value=None)) as by_key:
        yield by_key


@pytest.fixture(autouse=True)
def _no_live_hand_off():
    with patch(
        "src.server.database.runs.outbox.has_live_hand_off",
        new=AsyncMock(return_value=False),
    ) as live:
        yield live


def _job(n: int, hook_type: str = "task_report_back") -> dict:
    return {"hook_outbox_id": f"job-{n}", "hook_type": hook_type, "payload": {}}


def _report(n: int) -> SteeredReport:
    return SteeredReport(
        job_id=f"job-{n}",
        content=f"report {n}",
        shown={"report_source": {"kind": "task", "task_id": str(n)}},
    )


async def _take(jobs, decisions, held, *, closed=(), hold=None, close=None):
    """Run a take with per-job decisions (keyed by call order), the ids the
    hold reports as won, and the ids the unowed close reports as won."""
    hold = hold or AsyncMock(return_value=held)
    close = close or AsyncMock(return_value=list(closed))
    wake = AsyncMock()
    with (
        patch(f"{_OUTBOX}.list_steerable_report_jobs", new=AsyncMock(return_value=jobs)),
        patch(f"{_OUTBOX}.hold_steered_report_jobs", new=hold),
        patch(f"{_OUTBOX}.close_unowed_report_jobs", new=close),
        patch.object(steer, "_task_offer", new=AsyncMock(side_effect=decisions)),
        patch.object(steer, "_analyst_offer", new=AsyncMock(side_effect=decisions)),
        patch(_WAKE, new=wake),
    ):
        reports = await steer.SteeredReportInbox().take(
            "t-1", "run-A", kinds=("report",)
        )
    return reports, hold, close, wake


@pytest.mark.asyncio
async def test_holds_reports_it_hands_over_and_closes_unowed_jobs():
    jobs = [_job(1), _job(2), _job(3), _job(4)]
    decisions = [_report(1), NotOwed("delivered"), None, _report(4)]

    reports, hold, close, wake = await _take(
        jobs, decisions, held=["job-1"], closed=["job-2"]
    )

    # job-3 (left to its executor) is neither held nor closed; job-4 was lost
    # to its executor between the read and the hold.
    hold.assert_awaited_once_with(["job-1", "job-4"], run_id="run-A")
    close.assert_awaited_once_with(["job-2"])
    assert reports == [_report(1)]
    wake.assert_awaited_once_with("t-1")


@pytest.mark.asyncio
async def test_held_reports_keep_oldest_first_order():
    reports, *_ = await _take(
        [_job(1), _job(2)], [_report(1), _report(2)], held=["job-2", "job-1"]
    )

    assert [r.job_id for r in reports] == ["job-1", "job-2"]


@pytest.mark.asyncio
async def test_jobs_are_decided_concurrently():
    """Each decision reads the ledger and Redis; a turn waits on all of them
    before its model call, so they must not queue behind each other."""
    second_started = asyncio.Event()

    async def first(job):
        await asyncio.wait_for(second_started.wait(), timeout=1)
        return _report(1)

    async def second(job):
        second_started.set()
        return _report(2)

    offers = iter([first, second])

    async def _offer(job):
        return await next(offers)(job)

    with (
        patch(
            f"{_OUTBOX}.list_steerable_report_jobs",
            new=AsyncMock(return_value=[_job(1), _job(2)]),
        ),
        patch(
            f"{_OUTBOX}.hold_steered_report_jobs",
            new=AsyncMock(return_value=["job-1", "job-2"]),
        ),
        patch(f"{_OUTBOX}.close_unowed_report_jobs", new=AsyncMock(return_value=[])),
        patch.object(steer, "_task_offer", new=_offer),
        patch(_WAKE, new=AsyncMock()),
    ):
        reports = await steer.SteeredReportInbox().take("t-1", "run-A", kinds=("report",))

    assert [r.job_id for r in reports] == ["job-1", "job-2"]


@pytest.mark.asyncio
async def test_a_job_whose_read_fails_is_left_to_its_executor():
    reports, hold, close, _ = await _take(
        [_job(1), _job(2)], [RuntimeError("boom"), _report(2)], held=["job-2"]
    )

    hold.assert_awaited_once_with(["job-2"], run_id="run-A")
    close.assert_awaited_once_with([])
    assert reports == [_report(2)]


@pytest.mark.asyncio
async def test_listing_failure_takes_nothing():
    with patch(
        f"{_OUTBOX}.list_steerable_report_jobs",
        new=AsyncMock(side_effect=RuntimeError("db down")),
    ):
        assert await steer.SteeredReportInbox().take("t-1", "run-A", kinds=("report",)) == []


@pytest.mark.asyncio
async def test_hold_failure_takes_nothing_but_still_closes_unowed_jobs():
    hold = AsyncMock(side_effect=RuntimeError("db down"))

    reports, _, close, _ = await _take(
        [_job(1), _job(2)], [_report(1), NotOwed("delivered")], held=[], hold=hold,
        closed=["job-2"],
    )

    assert reports == []
    close.assert_awaited_once_with(["job-2"])


@pytest.mark.asyncio
async def test_close_failure_keeps_the_reports_already_held():
    """The hold has committed; dropping its reports here would park them
    until the turn ends, unseen by the turn that holds them."""
    close = AsyncMock(side_effect=RuntimeError("db down"))

    reports, *_ = await _take(
        [_job(1), _job(2)], [_report(1), NotOwed("delivered")], held=["job-1"],
        close=close,
    )

    assert reports == [_report(1)]


_OVERTAKING = "src.server.services.report_back.handoff.executor.overtaking_run"


def _held_analyst(n: int, run_id: str) -> dict:
    return {
        "hook_outbox_id": f"job-{n}",
        "hook_type": "report_back",
        "run_id": run_id,
        "payload": {"ptc_thread_id": f"ptc-{n}"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("release_fails", [False, True])
async def test_a_report_a_newer_run_took_over_after_the_read_is_given_back(
    release_fails,
):
    # job-1's analyst thread started a newer run between the offer's read and
    # the hold; job-2's did not.
    jobs = [_held_analyst(1, "run-old"), _held_analyst(2, "run-2")]
    newer = {"ptc-1": "run-newer", "ptc-2": None}
    overtaking = AsyncMock(side_effect=lambda _run, ptc: newer[ptc])
    release = AsyncMock(
        side_effect=RuntimeError("db") if release_fails else None,
        return_value=["job-1"],
    )
    with (
        patch(_OVERTAKING, new=overtaking),
        patch(f"{_OUTBOX}.release_steered_report_jobs", new=release),
    ):
        reports, *_ = await _take(
            jobs, [_report(1), _report(2)], held=["job-1", "job-2"]
        )

    release.assert_awaited_once_with(["job-1"], run_id="run-A")
    # A failed release still keeps it out: the run's finalize releases it.
    assert reports == [_report(2)]


@pytest.mark.asyncio
async def test_a_held_report_whose_thread_cannot_be_read_again_is_given_back():
    release = AsyncMock(return_value=["job-1"])
    with (
        patch(_OVERTAKING, new=AsyncMock(side_effect=RuntimeError("db"))),
        patch(f"{_OUTBOX}.release_steered_report_jobs", new=release),
    ):
        reports, *_ = await _take(
            [_held_analyst(1, "run-old")], [_report(1)], held=["job-1"]
        )

    release.assert_awaited_once_with(["job-1"], run_id="run-A")
    assert reports == []


@pytest.mark.asyncio
async def test_confirm_closes_the_runs_held_reports_and_never_raises():
    # No alert among them: nothing to stamp ahead of the close.
    no_alerts = patch(f"{_OUTBOX}.held_notice_jobs", new=AsyncMock(return_value=[]))
    confirm = AsyncMock(return_value=["job-1"])
    with no_alerts, patch(f"{_OUTBOX}.confirm_jobs", new=confirm):
        await steer.SteeredReportInbox().confirm("run-A", ["job-1"])
    confirm.assert_awaited_once_with(["job-1"], owner=("run", "run-A"))

    with no_alerts, patch(
        f"{_OUTBOX}.confirm_jobs",
        new=AsyncMock(side_effect=RuntimeError("db down")),
    ):
        await steer.SteeredReportInbox().confirm("run-A", ["job-1"])


# ---------------------------------------------------------------------------
# _task_offer: the executor's verdict, behind the steer's own pre-checks
# ---------------------------------------------------------------------------

_SR = "src.server.database.runs.subagent_runs.get_task_run"
_TL = "src.server.database.runs.lifecycle.get_run"
_STOPPED_AT = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _task_job(**payload) -> dict:
    base = {
        "task_run_id": "tr-1",
        "task_id": "abc123",
        "display_id": "Task-abc123",
        "subagent_type": "research",
        "description": "look things up",
    }
    base.update(payload)
    return {"hook_outbox_id": "job-1", "hook_type": "task_report_back", "payload": base}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "run_row",
    [None, {"result_delivered_at": "2026-01-01T00:00:00Z"}],
    ids=["run-row-gone", "result-delivered"],
)
async def test_task_offer_closes_a_report_owed_nothing(run_row):
    with patch(_SR, new=AsyncMock(return_value=run_row)):
        assert isinstance(await steer._task_offer(_task_job()), NotOwed)


@pytest.mark.asyncio
async def test_task_offer_closes_a_report_its_stopped_turn_covered():
    run_row = {
        "result_delivered_at": None,
        "parent_run_id": "run-P",
        "finalized_at": _STOPPED_AT - timedelta(seconds=30),
    }
    parent = {
        "status": "cancelled",
        "cancel_requested_at": _STOPPED_AT,
        "metadata": {"cancelled_by_user": True},
    }
    with (
        patch(_SR, new=AsyncMock(return_value=run_row)),
        patch(_TL, new=AsyncMock(return_value=parent)) as get_run,
    ):
        assert isinstance(await steer._task_offer(_task_job()), NotOwed)
    get_run.assert_awaited_once_with("run-P")


@pytest.mark.asyncio
async def test_task_offer_leaves_an_unlabelled_job_to_the_executor():
    assert await steer._task_offer(_task_job(task_run_id=None)) is None
    assert await steer._task_offer(_task_job(task_id=None)) is None


@pytest.mark.asyncio
async def test_task_offer_leaves_a_report_whose_turn_was_admitted(_no_admitted_turn):
    _no_admitted_turn.return_value = {"conversation_response_id": "run-R"}
    task_run = AsyncMock()
    with patch(_SR, new=task_run):
        assert await steer._task_offer(_task_job()) is None
    task_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_offer_steers_an_owed_report():
    run_row = {"result_delivered_at": None, "parent_run_id": None}
    with patch(_SR, new=AsyncMock(return_value=run_row)):
        offer = await steer._task_offer(_task_job())

    assert offer.job_id == "job-1"
    assert offer.content == (
        "<system>\n"
        "Background subagent Task-abc123 (research) completed while you were "
        "working.\n"
        "Task description: look things up\n"
        "</system>"
    )
    assert offer.shown["report_source"]["kind"] == "task"


# ---------------------------------------------------------------------------
# _analyst_offer
# ---------------------------------------------------------------------------

_CACHE = "src.utils.cache.redis_cache.get_cache_client"
_DELIVERIES = "src.server.services.report_back.handoff.requested_from.deliveries"


def _analyst_cache(origin: dict, *, member: bool = True) -> MagicMock:
    cache = MagicMock()
    cache.get_strict = AsyncMock(
        side_effect=lambda key: None if key.startswith("handoff_rb_run:") else origin
    )
    cache.client.sismember = AsyncMock(return_value=member)
    return cache


def _analyst_job(**over) -> dict:
    return {
        "hook_outbox_id": "job-1",
        "hook_type": "report_back",
        "payload": {"ptc_thread_id": "ptc-1", "final_status": "completed"},
        **over,
    }


def _origin(**over) -> dict:
    base = {
        "origin": "home",
        "report_back": True,
        "home_thread_id": "t-1",
        "user_id": "u-1",
    }
    base.update(over)
    return base


@pytest.mark.asyncio
async def test_analyst_offer_steers_an_owed_report():
    with patch(_CACHE, return_value=_analyst_cache(_origin())):
        offer = await steer._analyst_offer(_analyst_job(), "t-1")

    assert offer.job_id == "job-1"
    assert offer.content == (
        "<system>\n"
        "The analysis you dispatched (thread ptc-1 in a workspace) has completed.\n"
        "</system>"
    )
    assert offer.shown["report_source"] == {
        "kind": "analyst",
        "thread_id": "ptc-1",
        "outcome": "completed",
    }


@pytest.mark.asyncio
async def test_analyst_offer_labels_the_run_it_reports():
    """A steered report labels its row as its own turn would: how the
    reported run ended, the runs it continued, and the hand-offs they took
    in as steers."""
    job = _analyst_job(
        run_id="run-1",
        payload={
            "ptc_thread_id": "ptc-1",
            "final_status": "error",
        },
    )
    with (
        patch(_CACHE, return_value=_analyst_cache(_origin())),
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt",
            new=AsyncMock(return_value={"conversation_response_id": "run-1"}),
        ),
        patch(
            "src.server.database.runs.lifecycle.get_continued_runs",
            new=AsyncMock(return_value=["run-0", "run-1"]),
        ),
        patch(
            f"{_OUTBOX}.list_taken_hand_offs",
            # Taken in by one of the runs the report names, on its thread.
            new=AsyncMock(
                side_effect=lambda thread, runs: (
                    ["steer-1"] if (thread, runs) == ("ptc-1", ["run-0", "run-1"])
                    else []
                )
            ),
        ),
    ):
        offer = await steer._analyst_offer(job, "t-1")

    assert offer.shown["report_source"] == {
        "kind": "analyst",
        "thread_id": "ptc-1",
        "outcome": "error",
        "run_ids": ["run-0", "run-1"],
        "steer_ids": ["steer-1"],
    }


@pytest.mark.asyncio
async def test_analyst_offer_leaves_automation_reports_to_the_executor():
    with (
        patch(_CACHE, return_value=_analyst_cache(_origin())),
        patch(_DELIVERIES, return_value=[object()]),
    ):
        assert await steer._analyst_offer(_analyst_job(), "t-1") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("origin", "member"),
    [(_origin(home_thread_id="t-other"), True), (_origin(), False), (None, True)],
    ids=["owed-to-another-thread", "pair-already-cleared", "origin-gone"],
)
async def test_analyst_offer_leaves_what_is_not_owed_here(origin, member):
    with patch(_CACHE, return_value=_analyst_cache(origin, member=member)):
        assert await steer._analyst_offer(_analyst_job(), "t-1") is None


@pytest.mark.asyncio
async def test_analyst_offer_leaves_an_outcome_it_has_no_words_for():
    job = _analyst_job(payload={"ptc_thread_id": "ptc-1", "final_status": "weird"})
    with patch(_CACHE, return_value=_analyst_cache(_origin())):
        assert await steer._analyst_offer(job, "t-1") is None


@pytest.mark.asyncio
async def test_analyst_offer_leaves_a_report_whose_summary_run_started(_no_admitted_turn):
    _no_admitted_turn.return_value = {"conversation_response_id": "run-R"}
    with patch(_CACHE, return_value=_analyst_cache(_origin())):
        assert await steer._analyst_offer(_analyst_job(), "t-1") is None


@pytest.mark.asyncio
async def test_analyst_offer_leaves_a_report_its_executor_is_posting():
    from src.server.services.report_back.handoff.executor import analyst_request_key

    cache = _analyst_cache(_origin())
    cache.get_strict = AsyncMock(
        side_effect=lambda key: (
            {"request_key": analyst_request_key("job-1")}
            if key.startswith("handoff_rb_run:")
            else _origin()
        )
    )
    with patch(_CACHE, return_value=cache):
        assert await steer._analyst_offer(_analyst_job(), "t-1") is None


@pytest.mark.asyncio
async def test_analyst_offer_leaves_a_report_a_newer_run_took_over():
    latest = AsyncMock(return_value={"conversation_response_id": "run-newer"})
    with (
        patch(_CACHE, return_value=_analyst_cache(_origin())),
        patch("src.server.database.runs.lifecycle.get_latest_attempt", new=latest),
    ):
        assert await steer._analyst_offer(_analyst_job(run_id="run-old"), "t-1") is None
    latest.assert_awaited_once_with("ptc-1")


@pytest.mark.asyncio
async def test_analyst_offer_carries_where_the_work_was_asked_for_inline():
    """Its own turn sends the reminder as additional context; a steer has no
    such field, so the message carries it as the request path renders it."""
    from src.server.models.additional_context import DirectiveContext
    from src.server.utils.directive_context import build_directive_reminder

    with (
        patch(_CACHE, return_value=_analyst_cache(_origin())),
        patch(
            "src.server.services.report_back.handoff.requested_from.reminder",
            return_value="Asked in a Slack conversation.",
        ),
    ):
        offer = await steer._analyst_offer(_analyst_job(), "t-1")

    assert offer.content == (
        "<system>\n"
        "The analysis you dispatched (thread ptc-1 in a workspace) has completed.\n"
        "</system>"
    ) + build_directive_reminder(
        [DirectiveContext(content="Asked in a Slack conversation.")]
    )
