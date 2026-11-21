"""How sharply the live view draws the page in a browser starting now: as chosen
for the computer, which outlives any one browser, or by the computer's size.
The open browser keeps the quality it started with (``launch``)."""

from __future__ import annotations

import os
from pathlib import Path

from .protocol import (
    RETINA_BY_DEFAULT,
    RETINA_MIN_CPUS,
    STREAM_QUALITIES,
    StreamQualityName,
    state_file,
    write_state,
)

_FILE = "quality"
_CGROUP = Path("/sys/fs/cgroup")


def cpus() -> float:
    """The CPUs the computer may use: its own limit where it has one, as a
    container does, else every CPU the machine has."""
    quota = _quota()
    return quota if quota is not None else float(os.cpu_count() or 1)


def _quota() -> float | None:
    """The computer's CPU limit under cgroup v2, else v1, or None without one."""
    try:
        quota, period = (_CGROUP / "cpu.max").read_text().split()
    except (OSError, ValueError):
        try:
            quota = (_CGROUP / "cpu/cpu.cfs_quota_us").read_text().strip()
            period = (_CGROUP / "cpu/cpu.cfs_period_us").read_text().strip()
        except OSError:
            return None
    try:
        # v2 says ``max`` for no limit, v1 ``-1``.
        return int(quota) / int(period) if quota not in ("max", "-1") else None
    except (ValueError, ZeroDivisionError):
        return None


def choose(name: str) -> bool:
    """Keep *name* as the computer's choice, or forget it for ``default``;
    whether it was one of the qualities."""
    if name == "default":
        state_file(_FILE).unlink(missing_ok=True)
        return True
    if name not in STREAM_QUALITIES:
        return False
    write_state(_FILE, name)
    return True


def _choice() -> StreamQualityName | None:
    try:
        value = state_file(_FILE).read_text().strip()
    except OSError:
        return None
    return value if value in STREAM_QUALITIES else None


def chosen() -> StreamQualityName:
    """The quality a browser starting now gets: the one chosen for the computer,
    or by its size when none is, and never one its CPUs cannot keep up with."""
    count = cpus()
    name = _choice() or ("retina" if count >= RETINA_BY_DEFAULT else "standard")
    if name == "retina" and count < RETINA_MIN_CPUS:
        return "sharp"
    return name
