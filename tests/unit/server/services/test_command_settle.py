"""One settling policy for every end of a background command: when it is
covered, how one read of its job is judged, and when its end is reported."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ptc_agent.core.sandbox.jobs import JobState
from src.server.database.runs.command_runs import Outcome
from src.server.services.commands import settle as s

NOW = datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc)


def _row(**over):
    row = {"command_id": "bg-000000000001", "handed_off_at": NOW - timedelta(minutes=10),
           "stopped_by": None}
    row.update(over)
    return row


def _state(**over):
    state = dict(found=True, exit_code=None, vanished=False, tail="")
    state.update(over)
    return JobState(**state)


# --- coverage ---------------------------------------------------------------


def test_a_live_turn_or_task_covers_with_no_end():
    row = _row(handed_off_at=NOW - timedelta(hours=5))
    assert s.hour_ends_at(row, s.Coverage(live=True)) is None
    assert s.is_covered(row, s.Coverage(live=True), NOW)


# Handed off before the thread's work last ended, so the hour runs from that.
EARLY = _row(handed_off_at=NOW - timedelta(hours=3))


def test_a_wakeup_due_inside_the_hour_carries_the_command_an_hour_past_its_fire():
    last_end = NOW - timedelta(minutes=30)  # the hour runs out at NOW+30m
    fire = NOW + timedelta(minutes=20)
    cov = s.Coverage(last_end=last_end, next_fire=fire)
    assert s.hour_ends_at(EARLY, cov) == fire + s.AFTER_TURN
    # Due exactly as the hour runs out still covers.
    at_end = s.Coverage(last_end=last_end, next_fire=last_end + s.AFTER_TURN)
    assert s.hour_ends_at(EARLY, at_end) == last_end + 2 * s.AFTER_TURN


def test_a_fire_running_late_still_finds_the_command_covered():
    # Due 5 minutes ago, inside the hour, and not fired yet.
    cov = s.Coverage(last_end=NOW - timedelta(minutes=58), next_fire=NOW - timedelta(minutes=5))
    assert s.is_covered(EARLY, cov, NOW + timedelta(minutes=10))


def test_a_wakeup_due_after_the_hour_covers_nothing():
    last_end = NOW - timedelta(minutes=30)
    for fire in (NOW + timedelta(minutes=31), NOW + timedelta(days=7)):
        cov = s.Coverage(last_end=last_end, next_fire=fire)
        assert s.hour_ends_at(EARLY, cov) == last_end + s.AFTER_TURN
        assert not s.is_covered(EARLY, cov, NOW + timedelta(minutes=30))


def test_after_a_fire_the_hour_renews_from_it_and_the_next_fire_is_judged_against_that():
    # A 50-minute cron: fired 10 minutes ago, next due in 40.
    fired = NOW - timedelta(minutes=10)
    cov = s.Coverage(last_end=fired, next_fire=fired + timedelta(minutes=50))
    assert s.hour_ends_at(EARLY, cov) == fired + timedelta(minutes=50) + s.AFTER_TURN
    # A daily cron that just fired covers its hour and no more.
    daily = s.Coverage(last_end=fired, next_fire=fired + timedelta(days=1))
    assert s.hour_ends_at(EARLY, daily) == fired + s.AFTER_TURN


def test_the_hour_runs_from_the_threads_last_work_or_the_hand_off_whichever_is_later():
    row = _row(handed_off_at=NOW - timedelta(hours=2))
    cov = s.Coverage(last_end=NOW - timedelta(minutes=30))
    assert s.hour_ends_at(row, cov) == NOW + timedelta(minutes=30)
    assert s.is_covered(row, cov, NOW)
    assert s.hour_ends_at(_row(handed_off_at=NOW), s.Coverage(last_end=NOW - timedelta(hours=3))) == NOW + s.AFTER_TURN
    stale = s.Coverage(last_end=NOW - timedelta(minutes=61))
    assert not s.is_covered(_row(handed_off_at=NOW - timedelta(hours=2)), stale, NOW)


# --- one read, judged ---------------------------------------------------------

LATE = {"handed_off_at": NOW - timedelta(hours=2)}
STOPPED = {"vanished": True, "stopped": True}


@pytest.mark.parametrize(
    "row_over, state, kw, expected",
    [
        # A stop wins over an end it races: its stopper asked for nothing more.
        ({"stopped_by": "agent"}, _state(exit_code=0), {}, Outcome("stopped", stopped_by="agent")),
        ({"stopped_by": "archive"}, s.GONE, {}, Outcome("stopped", stopped_by="archive")),
        ({"stopped_by": "user"}, _state(**STOPPED), {}, Outcome("stopped", stopped_by="user")),
        # The agent is told of the user's stop and the hour's: a job that
        # exited first ends as it did, never as stopped.
        ({"stopped_by": "user"}, _state(exit_code=0), {}, Outcome("completed", exit_code=0)),
        ({"stopped_by": "after_turn_limit"}, _state(exit_code=0), {}, Outcome("completed", exit_code=0)),
        ({"stopped_by": "after_turn_limit"}, _state(exit_code=2), {"up": False}, Outcome("failed", "exit", 2, up=False)),
        ({"stopped_by": "after_turn_limit"}, _state(**STOPPED), {}, Outcome("failed", "after_turn_limit")),
        ({"stopped_by": "after_turn_limit"}, s.GONE, {}, Outcome("failed", "after_turn_limit")),
        ({}, _state(exit_code=0), {}, Outcome("completed", exit_code=0)),
        ({}, _state(exit_code=2), {}, Outcome("failed", "exit", 2)),
        # An end read on a computer a stop took since reports at the next turn.
        ({}, _state(exit_code=143), {"up": False}, Outcome("failed", "exit", 143, up=False)),
        # Gone with no exit while its computer runs: killed from outside,
        # unless a stop since it started took it, or its record went with a
        # rebuilt computer.
        ({}, _state(found=False), {}, Outcome("failed", "lost")),
        ({}, _state(vanished=True), {}, Outcome("failed", "vanished")),
        ({"started_at": NOW - timedelta(minutes=10)}, _state(vanished=True),
         {"computer_stopped_at": NOW - timedelta(minutes=5)}, Outcome("failed", "lost")),
        ({"started_at": NOW - timedelta(minutes=10)}, _state(vanished=True),
         {"computer_stopped_at": NOW - timedelta(hours=1)}, Outcome("failed", "vanished")),
        # Lost from a computer no longer up: judged as the computer's, later.
        ({}, _state(vanished=True), {"up": False}, s.WATCH),
        # A stop reached it and nothing on the row says who: it simply stopped.
        ({}, _state(**STOPPED), {}, Outcome("stopped")),
        (LATE, _state(), {"hour_applies": True}, s.STOP_AFTER_TURN),
        # An always-on computer has no hour.
        (LATE, _state(), {"hour_applies": False}, s.WATCH),
        # A wakeup due inside the hour keeps it; one due after does not.
        ({"handed_off_at": NOW - timedelta(minutes=50)}, _state(),
         {"hour_applies": True, "cov": s.Coverage(next_fire=NOW + timedelta(minutes=5))}, s.WATCH),
        (LATE, _state(), {"hour_applies": True, "cov": s.Coverage(next_fire=NOW + timedelta(hours=1))},
         s.STOP_AFTER_TURN),
        ({}, _state(), {"hour_applies": True}, s.WATCH),
        # A computer gone: its jobs went with it, reported at the next turn.
        ({}, s.GONE, {"hour_applies": True}, Outcome("failed", "lost", up=False)),
        ({"handed_off_at": NOW - timedelta(hours=3)}, s.GONE, {"hour_applies": True},
         Outcome("failed", "after_turn_limit", up=False)),
        ({"handed_off_at": NOW - timedelta(hours=3)}, s.GONE, {"hour_applies": False},
         Outcome("failed", "lost", up=False)),
        # Judged when the computer stopped, inside the hour, not when it was noticed.
        (LATE, s.GONE, {"hour_applies": True, "now": NOW - timedelta(minutes=70)},
         Outcome("failed", "lost", up=False)),
    ],
)
def test_decide(row_over, state, kw, expected):
    kw = {"up": True, "now": NOW, **kw}
    assert s.decide(_row(**row_over), state, **kw) == expected


@pytest.mark.parametrize(
    "outcome, report",
    [
        (Outcome("completed", exit_code=0), "now"),
        (Outcome("failed", "lost"), "now"),
        (Outcome("failed", "exit", 1, up=False), "next_turn"),
        (Outcome("failed", "after_turn_limit"), "next_turn"),
        (Outcome("stopped", stopped_by="user"), "next_turn"),
        (Outcome("stopped", stopped_by="agent"), "none"),
        (Outcome("stopped", stopped_by="archive"), "none"),
        (Outcome("stopped"), "none"),
    ],
)
def test_when_an_end_is_reported(outcome, report):
    """A turn of its own would start a stopped computer again, so an end read
    off one waits for the thread's next turn. Of the stops only the user's is
    reported, at that next turn: the agent made its own, and an archive leaves
    no turn to tell."""
    assert outcome.report == report


@pytest.mark.parametrize(
    "args",
    [
        ("running",),
        ("failed",),
        ("completed", "exit"),
        ("failed", "timeout"),
        ("cancelled",),
        ("completed", None, 0, "user"),
    ],
)
def test_an_outcome_the_ledger_cannot_hold_is_refused(args):
    with pytest.raises(ValueError):
        Outcome(*args)


def test_only_an_exit_137_names_a_sandbox_to_count_against():
    computer = {"kind": "docker", "provider_ref": "sbx-1"}
    assert s.oom_kill_on(computer, 137) == ("docker", "sbx-1")
    assert s.oom_kill_on(computer, 1) is None
    assert s.oom_kill_on(computer, None) is None
    assert s.oom_kill_on({**computer, "provider_ref": None}, 137) is None
