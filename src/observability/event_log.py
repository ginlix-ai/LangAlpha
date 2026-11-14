"""Append-only JSON Lines files, for measurement the database need not hold.

Off unless ``EVENT_LOG_DIR`` names a directory, which needs to outlive the
process (a mounted volume in a container) for the record to be worth reading.
Each event is one line in ``<stream>-<UTC date>.jsonl``, written by one
``os.write`` on a file opened for append. That makes every worker process,
and the tunnel gateway in its own, safe to append to the same day's file
without a lock: a line this short is never interleaved with another on a
local filesystem. Files older than ``EVENT_LOG_KEEP_DAYS`` (default 90) are
removed the first time each process writes on a new day.

The caller only queues the line: one writer thread per process does the file
work, so a slow disk never blocks the event loop, and lines keep the order
they were queued in. Never raises; a measurement that cannot be written is
dropped.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import queue
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEEP_DAYS = 90

# Lines a stalled disk may hold back before new ones are dropped, so the
# queue never grows with traffic.
_MAX_PENDING = 10_000

# How long exit waits for queued lines.
_EXIT_FLUSH_S = 5.0

# The day each stream last pruned on, so a process prunes once a day.
# Housekeeping only: nothing reads it as state, and every process keeps its own.
_pruned_on: dict[str, str] = {}

# Lines waiting for the writer thread: (directory, stream, line, when).
_pending: queue.Queue[tuple[str, str, str, datetime]] = queue.Queue(_MAX_PENDING)
_writer: threading.Thread | None = None
_writer_lock = threading.Lock()
_warned_full = False
_warned_write = False


def write_event(stream: str, event: str, **fields: Any) -> None:
    """Queue *event* with *fields* (None fields left out) for *stream*'s file."""
    directory = os.getenv("EVENT_LOG_DIR")
    if not directory:
        return
    now = datetime.now(timezone.utc)
    record = {"ts": now.isoformat(timespec="milliseconds"), "event": event}
    record.update((key, value) for key, value in fields.items() if value is not None)
    try:
        line = json.dumps(record, default=_plain, separators=(",", ":")) + "\n"
        _ensure_writer()
        _pending.put_nowait((directory, stream, line, now))
    except queue.Full:
        _note_dropped(stream)
    except Exception as e:
        logger.debug(f"Event {stream}/{event} not written: {type(e).__name__}: {e}")


def _note_dropped(stream: str) -> None:
    """Warn once per process that lines are being dropped, so a gap in the
    files has a cause someone can find."""
    global _warned_full
    if not _warned_full:
        _warned_full = True
        logger.warning(
            f"Event log queue full ({_MAX_PENDING} lines); dropping {stream} "
            "lines until the writer catches up"
        )


def flush(timeout: float | None = None) -> None:
    """Wait until every queued line is written (or dropped), or *timeout* passes."""
    if _writer is None or not _writer.is_alive():
        return
    if timeout is None:
        _pending.join()
        return
    waiter = threading.Thread(target=_pending.join, daemon=True)
    waiter.start()
    waiter.join(timeout)


def _ensure_writer() -> None:
    global _writer
    if _writer is not None and _writer.is_alive():
        return
    with _writer_lock:
        if _writer is None or not _writer.is_alive():
            _writer = threading.Thread(
                target=_drain, args=(_pending,), name="event-log", daemon=True
            )
            _writer.start()


def _drain(pending: queue.Queue[tuple[str, str, str, datetime]]) -> None:
    global _warned_full, _warned_write
    while True:
        directory, stream, line, now = pending.get()
        try:
            _append(directory, stream, line, now)
            _warned_write = False
        except Exception as e:
            # Warned once per failing spell, so a misconfigured directory is
            # visible without a line per event.
            message = f"Event line for {stream} not written to {directory}: {type(e).__name__}: {e}"
            if _warned_write:
                logger.debug(message)
            else:
                _warned_write = True
                logger.warning(message)
        finally:
            pending.task_done()
            if pending.empty():
                _warned_full = False  # a later stall warns again


def _append(directory: str, stream: str, line: str, now: datetime) -> None:
    day = now.strftime("%Y-%m-%d")
    if _pruned_on.get(stream) != day:
        os.makedirs(directory, exist_ok=True)
        _prune(Path(directory), stream, now)
        _pruned_on[stream] = day
    path = os.path.join(directory, f"{stream}-{day}.jsonl")
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
    try:
        fd = os.open(path, flags, 0o644)
    except FileNotFoundError:
        # The directory went away since the day's first line (a remount).
        os.makedirs(directory, exist_ok=True)
        fd = os.open(path, flags, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)


# Bounded: a stalled disk must not hang the process's exit.
atexit.register(flush, _EXIT_FLUSH_S)


def _plain(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _prune(directory: Path, stream: str, now: datetime) -> None:
    try:
        keep_days = int(os.getenv("EVENT_LOG_KEEP_DAYS") or _DEFAULT_KEEP_DAYS)
    except ValueError:
        keep_days = _DEFAULT_KEEP_DAYS
    oldest = (now - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    for path in directory.glob(f"{stream}-*.jsonl"):
        # The name carries the date, so names sort as days do.
        if path.stem[len(stream) + 1 :] < oldest:
            try:
                path.unlink(missing_ok=True)
            except OSError as e:
                # Left for tomorrow's prune; it must not cost today's lines.
                logger.debug(f"Old event file {path.name} not removed: {e}")
