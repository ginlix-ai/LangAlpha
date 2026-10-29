"""A wake turn that ended quietly with nothing changed leaves the thread as
seen as it was, and says so on the feed event its settle publishes."""

import pytest

from src.server.database.runs.hook_jobs import (
    WAKEUP_QUIET_META,
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
    is_unchanged_quiet,
)


@pytest.mark.parametrize(
    "meta, expected",
    [
        ({WAKEUP_QUIET_META: {"changed": False}}, True),
        ({WAKEUP_QUIET_META: {"changed": True}}, False),
        ({WAKEUP_QUIET_META: {}}, False),
        ({WAKEUP_QUIET_META: "quiet"}, False),
        ({}, False),
    ],
)
def test_only_a_quiet_check_that_changed_nothing_is_unchanged_quiet(meta, expected):
    assert is_unchanged_quiet(meta) is expected


def _feed(jobs):
    [feed] = [j for j in jobs if j.hook_type == "user_feed"]
    return feed.payload


def _run_row(**meta):
    return {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "thread-1",
        "run_seq": 7,
        "metadata": {"user_id": "u_test", **meta},
    }


def test_the_feed_event_carries_the_seen_pointer_a_quiet_settle_kept():
    jobs = build_finalize_jobs_from_run_row(_run_row(), last_seen_run_seq=7)(
        "completed"
    )
    assert _feed(jobs)["last_seen_run_seq"] == 7
    assert _feed(jobs)["run_seq"] == 7


def test_a_settle_that_left_the_pointer_publishes_none():
    jobs = build_finalize_jobs_from_run_row(_run_row())("completed")
    assert "last_seen_run_seq" not in _feed(jobs)


def test_the_decision_table_passes_the_pointer_through_unchanged():
    jobs = build_finalize_jobs(
        run_id="run-1",
        thread_id="thread-1",
        user_id="u_test",
        run_seq=7,
        last_seen_run_seq=0,
    )("completed")
    # Zero is a pointer too, not an absent one.
    assert _feed(jobs)["last_seen_run_seq"] == 0
