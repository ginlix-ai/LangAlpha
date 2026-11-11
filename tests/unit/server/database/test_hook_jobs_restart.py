"""What a turn a graceful restart cancelled leaves for the next process.

The executor's finalize stamps the turn, the decision table turns the stamp
into one ``restart_resume`` job, and the endings that would close the work
(a hand-off's report, a summary's release) wait for the turn that resumes it.
"""

import pytest

from src.server.contracts.status import RESTART_INTERRUPTED_META, is_restart_interrupted
from src.server.database.runs.hook_jobs import (
    build_finalize_jobs,
    RESTART_HELD_HOOKS,
    build_finalize_jobs_from_run_row,
    restart_held_jobs,
)
from src.server.services.runs.finalization import assemble_finalize_artifacts


def _jobs(final_status, **kw):
    args = dict(
        run_id="run-1",
        thread_id="thread-1",
        user_id="u-1",
        workspace_id="ws-1",
        restart_interrupted=True,
    )
    args.update(kw)
    return build_finalize_jobs(**args)(final_status)


def _types(jobs):
    return sorted(j.hook_type for j in jobs)


def test_a_restart_cancel_owes_one_resume_off_the_report_chain():
    (job,) = [j for j in _jobs("cancelled") if j.hook_type == "restart_resume"]
    assert job.idempotency_key == "run-1:restart_resume"
    # Parked reports on the thread's chain wait for the resume, so it must
    # not queue behind them.
    assert job.ordering_key == "restart_resume:thread-1"
    assert job.payload == {"run_id": "run-1"}


@pytest.mark.parametrize("status", ["completed", "error", "interrupted"])
def test_only_a_cancel_owes_a_resume(status):
    assert "restart_resume" not in _types(_jobs(status))


def test_an_ordinary_cancel_owes_no_resume():
    assert "restart_resume" not in _types(_jobs("cancelled", restart_interrupted=False))


def test_a_handed_off_turn_reports_once_its_resume_ends():
    kw = dict(dispatcher_thread_id="home-1", origin_dispatch_gen="gen-1")
    assert "report_back" in _types(_jobs("cancelled", restart_interrupted=False, **kw))
    assert "report_back" not in _types(_jobs("cancelled", **kw))


def test_a_summary_keeps_its_pair_for_the_turn_that_resumes_it():
    kw = dict(report_back_ptc_thread_id="ptc-9", dispatcher_thread_id="home-1")
    assert "watch_clear" in _types(_jobs("cancelled", restart_interrupted=False, **kw))
    assert "watch_clear" not in _types(_jobs("cancelled", **kw))


def _row(**meta):
    return {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "thread-1",
        "status": "cancelled",
        "metadata": {"user_id": "u-1", **meta},
    }


def test_the_row_stamp_owes_the_resume():
    jobs = build_finalize_jobs_from_run_row(_row(**{RESTART_INTERRUPTED_META: True}))
    assert "restart_resume" in _types(jobs("cancelled"))
    assert "restart_resume" not in _types(build_finalize_jobs_from_run_row(_row())("cancelled"))


def test_what_a_restart_holds_back_is_what_an_ordinary_stop_owes_beyond_it():
    row = _row(
        **{RESTART_INTERRUPTED_META: True},
        origin_home_thread_id="home-1",
        origin_dispatch_gen="gen-1",
        automation_execution_id="exec-1",
        automation_id="auto-1",
    )
    restarted = set(_types(build_finalize_jobs_from_run_row(row)("cancelled")))
    ordinary = set(_types(build_finalize_jobs_from_run_row(row, as_ordinary_stop=True)("cancelled")))
    assert restarted - {"restart_resume"} == ordinary - RESTART_HELD_HOOKS
    assert _types(restart_held_jobs(row)) == ["automation_settle", "report_back"]


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"status": "cancelled", "metadata": {RESTART_INTERRUPTED_META: True}}, True),
        ({"status": "error", "metadata": {RESTART_INTERRUPTED_META: True}}, False),
        ({"status": "cancelled", "metadata": {}}, False),
        ({"status": "cancelled", "metadata": None}, False),
        (None, False),
    ],
)
def test_is_restart_interrupted(row, expected):
    assert is_restart_interrupted(row) is expected


async def _persist_metadata(*, status, cancelled_by_user, restart):
    outcome = await assemble_finalize_artifacts(
        ("thread-1", "run-1"),
        metadata={},
        status=status,
        phase="",
        interrupt_reason=None,
        pause_message=None,
        error=None,
        handler=None,
        cancelled_by_user=cancelled_by_user,
        workspace_id=None,
        user_id=None,
        restart=restart,
    )
    return outcome.metadata


@pytest.mark.asyncio
async def test_the_finalize_stamps_a_restart_cancel():
    meta = await _persist_metadata(status="cancelled", cancelled_by_user=False, restart=True)
    assert meta[RESTART_INTERRUPTED_META] is True
    assert meta["cancelled_by_user"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "cancelled_by_user", "restart"),
    [
        ("cancelled", False, False),
        ("completed", False, True),
    ],
    ids=["shutdown_without_restart", "finished_before_the_cancel"],
)
async def test_nothing_else_is_stamped(status, cancelled_by_user, restart):
    meta = await _persist_metadata(
        status=status, cancelled_by_user=cancelled_by_user, restart=restart
    )
    assert RESTART_INTERRUPTED_META not in meta
