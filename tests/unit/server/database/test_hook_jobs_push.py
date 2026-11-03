"""Which settled runs are offered to the push executor, and when the push is
due. Whether one reaches a phone is the executor's call (``test_notify``)."""

import pytest

from src.server.database.runs.hook_jobs import (
    PUSH_SEEN_GRACE_SECONDS,
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
    assert job.ordering_key == "push:thread-1"
    assert job.delay_seconds == PUSH_SEEN_GRACE_SECONDS == 8.0
    assert job.payload == {
        "user_id": "u-1",
        "thread_id": "thread-1",
        "workspace_id": "ws-1",
        "run_id": "run-1",
        "run_seq": 4,
        "status": "completed",
        "interrupt_reason": None,
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
    assert _push("completed", automation_execution_id="exec-1", automation_id="auto-1")


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
