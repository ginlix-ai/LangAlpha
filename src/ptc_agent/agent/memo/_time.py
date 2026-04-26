"""ISO-8601 timestamp helpers for memo rows.

Microsecond resolution. Burst regenerate (multiple writes inside the same
wall-clock second) needs distinct timestamps so the ``modified_at`` CAS in
``_merge_metadata`` / ``_mark_failed`` keeps distinguishing snapshots.
"""

from __future__ import annotations

from datetime import UTC, datetime


def now_iso() -> str:
    """Current UTC time as ``YYYY-MM-DDTHH:MM:SS.ffffffZ``."""
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
