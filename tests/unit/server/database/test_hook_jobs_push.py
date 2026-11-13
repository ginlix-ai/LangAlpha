"""Which settled runs are offered as news, to the desktop through the user's
feed (``user_notification``) and to the phones (``push``), and when each is
due. Whether one is news is the executors' call (``test_notify``)."""

import pytest

from src.server.database.runs.hook_jobs import (
    SETTLE_NEWS_GRACE_SECONDS,
    USER_NOTIFICATION_HOOK,
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
)
from src.server.contracts.status import TERMINAL_STATUSES

NEWS_HOOKS = ("push", USER_NOTIFICATION_HOOK)


def _job(hook_type, final_status, **kw):
    args = dict(
        run_id="run-1",
        thread_id="thread-1",
        user_id="u-1",
        workspace_id="ws-1",
        run_seq=4,
        push_enabled=True,
    )
    args.update(kw)
    jobs = [
        j for j in build_finalize_jobs(**args)(final_status) if j.hook_type == hook_type
    ]
    assert len(jobs) <= 1
    return jobs[0] if jobs else None


def _push(final_status, **kw):
    return _job("push", final_status, **kw)


def _notice(final_status, **kw):
    return _job(USER_NOTIFICATION_HOOK, final_status, **kw)


@pytest.mark.parametrize("hook_type", NEWS_HOOKS)
def test_a_finished_turn_is_offered_after_the_seen_grace(hook_type):
    job = _job(hook_type, "completed")
    assert job.idempotency_key == f"run-1:{hook_type}"
    assert job.ordering_key == f"{hook_type}:thread-1"
    assert job.delay_seconds == SETTLE_NEWS_GRACE_SECONDS == 8.0
    assert job.payload == {
        "user_id": "u-1",
        "thread_id": "thread-1",
        "workspace_id": "ws-1",
        "run_id": "run-1",
        "run_seq": 4,
        "status": "completed",
        "interrupt_reason": None,
    }
    assert _job(hook_type, "error").delay_seconds == SETTLE_NEWS_GRACE_SECONDS


def test_the_desktop_notice_and_the_push_are_separate_chains():
    """A slow APNs holds back only the push: the notice is its own job, on
    its own per-thread ordering key, outside the push's quota."""
    push, notice = _push("completed"), _notice("completed")
    assert push.ordering_key != notice.ordering_key
    assert push.payload == notice.payload
    assert push.payload is not notice.payload


@pytest.mark.parametrize("hook_type", NEWS_HOOKS)
def test_a_run_waiting_on_the_user_is_offered_at_once(hook_type):
    job = _job(hook_type, "interrupted", interrupt_reason="approval_required")
    assert job.delay_seconds is None
    assert job.payload["status"] == "interrupted"
    assert job.payload["interrupt_reason"] == "approval_required"


def test_the_raw_status_is_carried_not_the_public_one():
    assert _push("error").payload["status"] == "error"
    assert _notice("error").payload["status"] == "error"


@pytest.mark.parametrize("status", TERMINAL_STATUSES)
def test_nothing_is_pushed_while_push_is_off(status):
    assert _push(status, push_enabled=False) is None


@pytest.mark.parametrize("status", sorted(set(TERMINAL_STATUSES) - {"cancelled"}))
def test_the_desktop_notice_is_offered_with_push_off(status):
    assert _notice(status, push_enabled=False) is not None


@pytest.mark.parametrize("hook_type", NEWS_HOOKS)
def test_no_owner_no_news(hook_type):
    assert _job(hook_type, "completed", user_id=None) is None


@pytest.mark.parametrize("hook_type", NEWS_HOOKS)
def test_a_stop_is_never_news(hook_type):
    assert _job(hook_type, "cancelled") is None


@pytest.mark.parametrize("hook_type", NEWS_HOOKS)
def test_a_superseded_automation_run_is_not_news(hook_type):
    firing = dict(automation_execution_id="exec-1", automation_id="auto-1")
    assert _job(hook_type, "completed", superseded=True, **firing) is None
    assert _job(hook_type, "completed", **firing)


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
    types = _types(build_finalize_jobs_from_run_row(_row())("completed"))
    assert "push" not in types and USER_NOTIFICATION_HOOK in types
    monkeypatch.setattr(env, "APNS_ENABLED", True)
    types = _types(build_finalize_jobs_from_run_row(_row())("completed"))
    assert {"push", USER_NOTIFICATION_HOOK} <= types
