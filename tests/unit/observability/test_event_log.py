"""The event files: off until a directory is named, one line per event, kept
for a bounded number of days, and never an error for the caller."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from unittest.mock import patch

from src.observability import event_log


@pytest.fixture
def log_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("EVENT_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(event_log, "_pruned_on", {})
    return tmp_path


def _lines(directory, stream):
    event_log.flush()
    [path] = directory.glob(f"{stream}-*.jsonl")
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_nothing_is_written_without_a_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("EVENT_LOG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    event_log.write_event("s", "e", a=1)
    event_log.flush()
    assert list(tmp_path.iterdir()) == []


def test_each_event_is_one_line_without_its_empty_fields(log_dir):
    at = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)
    event_log.write_event("s", "first", a=1, b=None, at=at)
    event_log.write_event("s", "second")

    first, second = _lines(log_dir, "s")
    assert (first["event"], first["a"], first["at"]) == ("first", 1, at.isoformat())
    assert "b" not in first
    assert second["event"] == "second" and second["ts"] >= first["ts"]


def test_lines_land_in_the_order_they_were_queued(log_dir):
    for n in range(200):
        event_log.write_event("s", "e", n=n)
    assert [line["n"] for line in _lines(log_dir, "s")] == list(range(200))


def test_files_past_the_kept_days_are_removed(log_dir, monkeypatch):
    monkeypatch.setenv("EVENT_LOG_KEEP_DAYS", "7")
    today = datetime.now(timezone.utc)
    old = log_dir / f"s-{(today - timedelta(days=8)):%Y-%m-%d}.jsonl"
    recent = log_dir / f"s-{(today - timedelta(days=6)):%Y-%m-%d}.jsonl"
    other = log_dir / f"t-{(today - timedelta(days=30)):%Y-%m-%d}.jsonl"
    for path in (old, recent, other):
        path.write_text("")

    event_log.write_event("s", "e")
    event_log.flush()

    assert not old.exists() and recent.exists()
    assert other.exists()  # another stream prunes its own


def test_a_directory_that_cannot_be_written_is_not_the_callers_error(tmp_path, monkeypatch):
    blocked = tmp_path / "file"
    blocked.write_text("")
    monkeypatch.setenv("EVENT_LOG_DIR", str(blocked / "events"))
    monkeypatch.setattr(event_log, "_pruned_on", {})
    monkeypatch.setattr(event_log, "_warned_write", False)
    with patch.object(event_log.logger, "warning") as warning:
        event_log.write_event("s", "e")
        event_log.write_event("s", "e")
        event_log.flush()
    warning.assert_called_once()  # visible, but not a line per event


def test_a_directory_removed_mid_day_is_made_again(log_dir):
    event_log.write_event("s", "first")
    event_log.flush()
    for path in log_dir.iterdir():
        path.unlink()
    log_dir.rmdir()

    event_log.write_event("s", "second")
    assert [line["event"] for line in _lines(log_dir, "s")] == ["second"]


def test_a_full_queue_drops_the_line_rather_than_waiting(log_dir, monkeypatch):
    import queue

    monkeypatch.setattr(event_log, "_pending", queue.Queue(1))
    monkeypatch.setattr(event_log, "_ensure_writer", lambda: None)  # no drain
    monkeypatch.setattr(event_log, "_warned_full", False)
    with patch.object(event_log.logger, "warning") as warning:
        event_log.write_event("s", "kept")
        event_log.write_event("s", "dropped")  # returns, does not block
        event_log.write_event("s", "dropped")
    assert event_log._pending.qsize() == 1
    warning.assert_called_once()  # once per process, not per line


def test_an_old_file_that_cannot_be_removed_costs_no_line(log_dir, monkeypatch):
    from pathlib import Path

    old = log_dir / "s-2000-01-01.jsonl"
    old.write_text("")

    def refuse(self, missing_ok=False):
        raise PermissionError("read-only")

    monkeypatch.setattr(Path, "unlink", refuse)
    event_log.write_event("s", "e")
    event_log.flush()
    monkeypatch.undo()
    [today] = [p for p in log_dir.glob("s-*.jsonl") if p != old]
    assert today.read_text().count("\n") == 1 and old.exists()


def test_a_bounded_flush_returns_while_the_disk_stalls(log_dir, monkeypatch):
    import threading
    import time

    stalled = threading.Event()
    monkeypatch.setattr(event_log, "_append", lambda *a: stalled.wait(5))
    event_log.write_event("s", "e")
    started = time.monotonic()
    event_log.flush(timeout=0.2)
    assert time.monotonic() - started < 2
    stalled.set()
    event_log.flush()
