"""Starts and stops paired from the events the server writes.

Each event is written with no read of the one before, so what a start or
stop was, and how it ended, rests entirely on the pairing. The events here
are written by the server's own functions, so the two halves stay in step.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts.ops import computer_events as script
from src.observability import event_log
from src.server.services.computer_usage import events
from src.server.services.computer_usage.causes import computer_cause


@pytest.fixture
def log_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("EVENT_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(event_log, "_pruned_on", {})
    return tmp_path


def _computer(status, **extra):
    return {
        "computer_id": "c1", "user_id": "u1", "kind": "daytona",
        "resource_tier": "standard", "provider_ref": "sb-1", "status": status, **extra,
    }


def _status(status, *, reason=None, **extra):
    events.note_status(_computer(status, **extra), status, reason=reason)


def _paired(log_dir):
    event_log.flush()
    return script.pair_transitions(script.read_events(log_dir, "computer-events"))


def test_a_start_carries_its_cause_and_a_repeated_claim_is_one_start(log_dir):
    with computer_cause("warm", client="desktop", detail="composer"):
        _status("starting")
        _status("starting")
        events.note_start_path("c1", "create")
        _status("running")

    [start] = _paired(log_dir)
    assert (start["action"], start["cause"], start["client"], start["detail"]) == (
        "start", "warm", "desktop", "composer"
    )
    assert (start["path"], start["outcome"], start["tracked"]) == ("create", "ok", True)
    assert start["duration_ms"] >= 0


def test_a_revert_is_a_failure_only_when_the_start_reported_one(log_dir):
    _status("starting")
    _status("stopped")
    _status("starting")
    _status("stopped")
    events.note_start_failure("c1", "not_connected")  # races the revert

    released, failed = _paired(log_dir)
    assert released["outcome"] == "released"
    assert (failed["outcome"], failed["failure_code"]) == ("failed", "not_connected")


def test_a_claim_on_a_running_computer_handed_back_started_nothing(log_dir):
    _status("running")
    with computer_cause("rebuild"):
        _status("starting")
    _status("running")

    [start] = _paired(log_dir)
    assert (start["cause"], start["outcome"], start["from_status"]) == (
        "rebuild", "released", "running"
    )


def test_a_stop_takes_its_writers_reason_idle_time_and_step_timings(log_dir):
    idle_since = datetime.now(timezone.utc) - timedelta(minutes=50)
    with computer_cause("user"):
        _status("stopping", last_activity_at=idle_since, reason="idle")
    events.note_stop_phases("c1", backup_ms=120, provider_ms=900)
    _status("stopped")

    [stop] = _paired(log_dir)
    assert (stop["action"], stop["cause"], stop["outcome"]) == ("stop", "idle", "ok")
    assert (stop["backup_ms"], stop["provider_ms"]) == (120, 900)
    assert 50 * 60 - 5 <= stop["idle_s"] <= 50 * 60 + 60


def test_a_stop_that_found_new_work_is_aborted(log_dir):
    _status("stopping")
    _status("running")
    [stop] = _paired(log_dir)
    assert stop["outcome"] == "aborted"


def test_a_provider_report_fills_the_claimed_start_or_stands_alone(log_dir):
    report = SimpleNamespace(
        sandbox_id="sb-1", provider_kind="daytona", state_before="archiving",
        started=True, start_ms=61000,
    )
    with computer_cause("turn", client="browser"):
        _status("starting")
        events._on_runtime_start(report)
    _status("running")
    events._on_runtime_start(SimpleNamespace(**{**vars(report), "state_before": "stopped", "start_ms": 800}))
    # A reconnect that only waited on another caller's start reports nothing.
    events._on_runtime_start(SimpleNamespace(**{**vars(report), "started": False}))

    claimed, untracked = _paired(log_dir)
    assert (claimed["path"], claimed["provider_ms"], claimed["outcome"]) == ("archived", 61000, "ok")
    assert (untracked["tracked"], untracked["path"], untracked["cause"]) == (False, "stopped", "unknown")


def test_a_machine_is_online_from_its_first_connection_to_its_disconnect(log_dir):
    for change, tunnel in [
        ("connected", "t1"), ("connected", "t1"), ("disconnected", "t1"),
        ("connected", "t1"), ("connected", "t2"), ("gateway_stop", None),
    ]:
        event_log.write_event("tunnel-events", change, tunnel_id=tunnel)

    event_log.flush()
    sessions = script.tunnel_sessions(script.read_events(log_dir, "tunnel-events"))
    assert [how for _, _, how in sessions["t1"]] == ["disconnected", "gateway_stop"]
    assert [how for _, _, how in sessions["t2"]] == ["gateway_stop"]
