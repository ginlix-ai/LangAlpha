"""Which settled runs owe the user's phones a push, and when it is due."""

import pytest

from src.server.database.runs.hook_jobs import (
    PUSH_SEEN_GRACE_SECONDS,
    WAKEUP_QUIET_META,
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
)
from src.server.contracts.status import TERMINAL_STATUSES


def _push(final_status, **kw):
    args = dict(
        run_id="run-1",
        thread_id="thread-1",
        user_id="u-1",
        workspace_id="ws-1",
        run_seq=4,
        push_enabled=True,
    )
    args.update(kw)
    jobs = [j for j in build_finalize_jobs(**args)(final_status) if j.hook_type == "push"]
    assert len(jobs) <= 1
    return jobs[0] if jobs else None


def test_a_finished_turn_pushes_after_the_seen_grace():
    job = _push("completed")
    assert job.idempotency_key == "run-1:push"
    assert job.ordering_key is None
    assert job.delay_seconds == PUSH_SEEN_GRACE_SECONDS == 8.0
    assert job.payload == {
        "user_id": "u-1",
        "thread_id": "thread-1",
        "workspace_id": "ws-1",
        "run_id": "run-1",
        "run_seq": 4,
        "status": "completed",
        "interrupt_reason": None,
        "automation_id": None,
        "report_back": False,
    }
    assert _push("error").delay_seconds == PUSH_SEEN_GRACE_SECONDS


def test_a_run_waiting_on_the_user_pushes_at_once():
    job = _push("interrupted", interrupt_reason="approval_required")
    assert job.delay_seconds is None
    assert job.payload["status"] == "interrupted"
    assert job.payload["interrupt_reason"] == "approval_required"


def test_the_raw_status_is_carried_not_the_public_one():
    assert _push("error").payload["status"] == "error"


@pytest.mark.parametrize("status", TERMINAL_STATUSES)
def test_nothing_is_pushed_while_push_is_off(status):
    assert _push(status, push_enabled=False) is None


def test_no_owner_no_push():
    assert _push("completed", user_id=None) is None


def test_a_stop_never_pushes():
    assert _push("cancelled") is None


def test_a_superseded_automation_run_does_not_push():
    assert (
        _push(
            "completed",
            automation_execution_id="exec-1",
            automation_id="auto-1",
            superseded=True,
        )
        is None
    )
    job = _push("completed", automation_execution_id="exec-1", automation_id="auto-1")
    assert job.payload["automation_id"] == "auto-1"


def test_a_quiet_check_that_changed_nothing_does_not_push():
    assert _push("completed", quiet_unchanged=True) is None
    # The seen pointer a quiet settle carried says the same.
    assert _push("completed", last_seen_run_seq=4) is None


def test_a_dispatched_analysts_ending_leaves_the_push_to_its_report_back():
    for status in ("completed", "error"):
        assert _push(status, dispatcher_thread_id="cos-1") is None, status
    # Waiting on the user, it has no report coming.
    assert _push("interrupted", dispatcher_thread_id="cos-1") is not None


def test_the_report_back_summary_on_the_chief_of_staff_thread_pushes():
    job = _push(
        "completed",
        thread_id="cos-1",
        report_back_ptc_thread_id="ptc-1",
    )
    assert job.payload["report_back"] is True


@pytest.mark.parametrize(
    "platform, pushes",
    [
        (None, True),
        ("web", True),
        ("market_view:AAPL", True),
        ("telegram", False),
        ("slack", False),
        ("imessage", False),
    ],
)
def test_a_turn_on_a_chat_app_is_answered_there(platform, pushes):
    assert (_push("completed", surface_platform=platform) is not None) is pushes


def _row(**meta):
    return {
        "conversation_response_id": "run-7",
        "conversation_thread_id": "thread-7",
        "run_seq": 7,
        "metadata": {"user_id": "u-7", "workspace_id": "ws-7", **meta},
    }


def _types(jobs):
    return {j.hook_type for j in jobs}


def test_the_run_row_factory_follows_the_configuration(monkeypatch):
    from src.config import env

    monkeypatch.setattr(env, "APNS_ENABLED", False)
    assert "push" not in _types(build_finalize_jobs_from_run_row(_row())("completed"))
    monkeypatch.setattr(env, "APNS_ENABLED", True)
    assert "push" in _types(build_finalize_jobs_from_run_row(_row())("completed"))


def test_the_run_row_factory_reads_the_start_stamps(monkeypatch):
    from src.config import env

    monkeypatch.setattr(env, "APNS_ENABLED", True)
    chat_app = _row(surface={"platform": "telegram", "rules": "plain text"})
    assert "push" not in _types(build_finalize_jobs_from_run_row(chat_app)("completed"))
    quiet = _row(**{WAKEUP_QUIET_META: {"changed": False}})
    assert "push" not in _types(build_finalize_jobs_from_run_row(quiet)("completed"))
    changed = _row(**{WAKEUP_QUIET_META: {"changed": True}})
    assert "push" in _types(build_finalize_jobs_from_run_row(changed)("completed"))
    analyst = _row(origin_home_thread_id="cos-7")
    assert "push" not in _types(build_finalize_jobs_from_run_row(analyst)("completed"))
