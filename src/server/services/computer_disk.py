"""How full a computer's disk is: levels, the sandbox command, and its parsing.

Every workspace on a computer shares one disk, so the reading belongs to the
computer. ``df`` on the computer's root is the whole answer: the sandbox's
writable layer is one filesystem sized to the tier's disk, and the image layers
beneath it do not count against it. Levels are set by free bytes alone, because
what fails at the edge is an absolute amount (an install, a download, a temp
file), not a percentage of a disk whose size depends on the tier.

Pure functions only; taking a reading is ``ComputerManager.refresh_computer_disk``.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from src.server.models.computer import ComputerDisk, DiskLevel, WorkspaceStorage

_KIB = 1024
_MB = 1024 * 1024

# Free space under which each level starts. Tuned by hand; see the level table
# in the UI for what each one does.
NOTICE_FREE_BYTES = 250 * _MB
WARNING_FREE_BYTES = 100 * _MB
CRITICAL_FREE_BYTES = 50 * _MB

# The agent is told only at critical. Ordinary work (scripts, tables, charts,
# reports) still fits in 50 MB; what does not is an install or a download,
# which is what the notice steers it away from. Earlier, it is noise.
_AGENT_NOTICE_LEVELS = ("critical",)

# A turn is the thing that fills the disk, but a burst of short turns should
# not cost one exec each.
TURN_MEASURE_MIN_INTERVAL_SECONDS = 60

# A folder breakdown is a du over the whole root on the live machine, and the
# storage panel asks for one on every open. Folder sizes move at the pace of
# a turn, so a breakdown this young is served again instead of measured.
STORAGE_BREAKDOWN_MAX_AGE_SECONDS = 60

_DF_TIMEOUT = 10
_DU_TIMEOUT = 20
_SPLIT = "---du---"


def disk_breakdown_key(computer_id: str) -> str:
    """Redis key of a computer's last folder breakdown, shared by every worker."""
    return f"computer:disk-breakdown:{computer_id}"


def disk_level(free_bytes: int) -> DiskLevel:
    if free_bytes < CRITICAL_FREE_BYTES:
        return "critical"
    if free_bytes < WARNING_FREE_BYTES:
        return "warning"
    if free_bytes < NOTICE_FREE_BYTES:
        return "notice"
    return "healthy"


def _reading_is_current(computer: Mapping[str, Any]) -> bool:
    """Whether the stored reading was taken on the sandbox the machine has now.

    A spec change or a recovery replaces the sandbox without measuring the
    new one, and the old reading (its total, its fullness) says nothing about
    it: until the next measurement there is no reading, not a wrong one.
    """
    return computer.get("disk_sandbox_ref") == computer.get("provider_ref")


def disk_from_row(computer: Mapping[str, Any]) -> Optional[ComputerDisk]:
    if not _reading_is_current(computer):
        return None
    total = computer.get("disk_total_bytes")
    used = computer.get("disk_used_bytes")
    free = computer.get("disk_free_bytes")
    measured_at = computer.get("disk_measured_at")
    if total is None or used is None or free is None or measured_at is None:
        return None
    return ComputerDisk(
        used_bytes=int(used),
        total_bytes=int(total),
        free_bytes=int(free),
        measured_at=measured_at,
        level=disk_level(int(free)),
    )


def low_disk_free_mb(computer: Optional[Mapping[str, Any]]) -> Optional[int]:
    """Free megabytes to tell the agent, or None while the disk is not low enough to matter."""
    disk = disk_from_row(computer) if computer else None
    if disk is None or disk.level not in _AGENT_NOTICE_LEVELS:
        return None
    return disk.free_bytes // _MB


def disk_is_known(computer: Optional[Mapping[str, Any]]) -> bool:
    """Whether the row holds a current reading, so "not low" is an observation."""
    return bool(computer) and disk_from_row(computer) is not None


def measured_within(computer: Mapping[str, Any], seconds: float) -> bool:
    measured_at = computer.get("disk_measured_at")
    if not isinstance(measured_at, datetime) or not _reading_is_current(computer):
        return False
    if measured_at.tzinfo is None:
        measured_at = measured_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - measured_at).total_seconds() < seconds


@dataclass(frozen=True)
class DiskReading:
    total_bytes: int
    used_bytes: int
    free_bytes: int


def parse_df(stdout: str) -> Optional[DiskReading]:
    """Parse ``df -Pk``: a header, then filesystem, size, used, available
    (KiB), capacity and mount point."""
    for line in stdout.strip().splitlines()[1:]:
        parts = line.split()
        # A filesystem name or mount point may hold spaces, so the numbers are
        # the three that the capacity column follows.
        for i in range(len(parts) - 3):
            numbers = parts[i : i + 3]
            if all(p.isdigit() for p in numbers) and parts[i + 3].endswith("%"):
                total, used, free = (int(p) * _KIB for p in numbers)
                return DiskReading(total_bytes=total, used_bytes=used, free_bytes=free)
    return None


def parse_du(stdout: str, root_dir: str) -> Dict[str, int]:
    """Parse ``du -sk`` of each top-level folder into folder name -> bytes."""
    prefix = root_dir.rstrip("/") + "/"
    sizes: Dict[str, int] = {}
    for line in stdout.strip().splitlines():
        size, _, path = line.partition("\t")
        if not size.strip().isdigit() or not path.startswith(prefix):
            continue
        name = path[len(prefix):].strip("/")
        if name and "/" not in name:
            sizes[name] = int(size.strip()) * _KIB
    return sizes


def disk_command(root_dir: str, *, breakdown: bool) -> tuple[str, int]:
    """One shell command for the reading, and the exec timeout it needs."""
    root = shlex.quote(root_dir.rstrip("/") or "/")
    # KiB and POSIX columns, which BSD df (a macOS machine) reads as GNU does.
    df = f"df -Pk {root}"
    if not breakdown:
        return df, _DF_TIMEOUT
    # Real directories only, on the root's filesystem: an agent's symlink to
    # / would otherwise make every breakdown walk the whole machine. The du
    # gets its own deadline inside the exec, so a slow walk still returns the
    # df line ahead of it instead of losing both to the exec timeout. macOS
    # has no timeout(1), so perl's alarm stands in for it there.
    # Allocated blocks, not apparent size: that is what df counts as used, so
    # a sparse or preallocated file cannot push the folders past it and zero
    # out 'other'. One du for every folder counts a hard link only once.
    deadline = _DU_TIMEOUT - 2
    bound = (
        f"if command -v timeout >/dev/null 2>&1; then set -- timeout {deadline}; "
        "elif command -v perl >/dev/null 2>&1; then "
        f"set -- perl -e 'alarm shift; exec @ARGV' {deadline}; "
        "else set --; fi"
    )
    du = f'find {root} -mindepth 1 -maxdepth 1 -type d -exec "$@" du -skx {{}} +'
    return (
        f"{df}; echo {_SPLIT}; {bound}; {du} 2>/dev/null || true",
        _DF_TIMEOUT + _DU_TIMEOUT,
    )


def parse_disk_output(
    stdout: str, root_dir: str, *, breakdown: bool
) -> tuple[Optional[DiskReading], Dict[str, int]]:
    df_out, _, du_out = stdout.partition(_SPLIT)
    return parse_df(df_out), parse_du(du_out, root_dir) if breakdown else {}


def storage_breakdown(
    computer: Mapping[str, Any],
    workspaces: List[Dict[str, Any]],
    sizes: Dict[str, int],
) -> tuple[List[WorkspaceStorage], int]:
    """Map folder sizes onto workspaces; whatever the folders don't hold is 'other'."""
    rows = [
        WorkspaceStorage(
            workspace_id=str(ws["workspace_id"]),
            name=ws.get("name") or "",
            dir_name=ws["dir_name"],
            bytes=sizes[ws["dir_name"]],
        )
        for ws in workspaces
        if ws.get("dir_name") and ws["dir_name"] in sizes
    ]
    rows.sort(key=lambda r: r.bytes, reverse=True)
    used = int(computer.get("disk_used_bytes") or 0)
    other = max(0, used - sum(r.bytes for r in rows))
    return rows, other
