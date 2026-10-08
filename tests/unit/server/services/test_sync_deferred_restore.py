"""Evicted tool results the deferred restore has not brought back yet.

A restore places ``.agents/large_tool_results/`` after the rest of the folder,
so a sync between the two passes sees those results missing. Read as a
deletion, the prune would remove the only record of each one; the rows are
kept until the second pass writes its marker, which the scan reports and the
manifest never stores.

The scan itself runs (only the sandbox's answer is canned), since dropping the
marker from the entries and noting it is ``scan_workspace``'s half of the gate.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from src.server.services.persistence import backup, transfer
from src.server.services.persistence.transfer import DEFERRED_MARKER

WS = "ws-deferred-restore"
LAYOUT = SandboxLayout.for_root("/home/workspace").for_workspace("deferred-ab12")
CLOCK = datetime(2026, 9, 3, 12, 0, 0, tzinfo=timezone.utc)
MTIME_NS = 1_700_000_000_000_000_000

RESULTS_DIR = WorkspaceLayout.LARGE_TOOL_RESULTS_DIR
THREAD_DIR = f"{RESULTS_DIR}/a1b2c3d4"
EVICTED = f"{THREAD_DIR}/call_1.txt"
NOTE = WorkspaceLayout.scratchpad_subdir("a1b2c3d4", "note", "plan.md")
GONE = "reports/gone.txt"


def _file(path: str, sha: str, size: int = 10) -> dict:
    return {"path": path, "kind": "file", "size": size, "mtime_ns": MTIME_NS, "mode": 0o644, "sha256": sha}


def _dir(path: str) -> dict:
    return {"path": path, "kind": "dir", "size": 0, "mtime_ns": MTIME_NS, "mode": 0o755}


def _row(entry: dict) -> dict:
    return {
        "kind": entry["kind"],
        "file_size": entry["size"],
        "mtime_ns": entry["mtime_ns"],
        "content_hash": entry.get("sha256"),
        "permissions": f"{entry['mode']:04o}",
        "symlink_target": None,
    }


class _Manifest:
    """One workspace's ``workspace_files`` rows: the prune deletes every row
    the keep set leaves out."""

    def __init__(self, entries: list[dict]) -> None:
        self.rows = {e["path"]: _row(e) for e in entries}

    async def read(self, workspace_id, conn=None):
        return {path: dict(row) for path, row in self.rows.items()}

    async def prune(self, workspace_id, keep, *, walked_dir_name, untouched_since, conn=None):
        assert walked_dir_name == LAYOUT.dir_name
        gone = [path for path in self.rows if path not in keep]
        for path in gone:
            del self.rows[path]
        return len(gone)

    async def upsert(self, workspace_id, rows, conn=None):
        self.rows.update((row["file_path"], row) for row in rows)
        return len(rows)

    async def delete(self, workspace_id, paths, conn=None):
        for path in paths:
            self.rows.pop(path, None)
        return len(paths)


def _manifest() -> _Manifest:
    return _Manifest([
        _file("notes.txt", "n"),
        _dir(".agents"),
        _dir(RESULTS_DIR),
        _dir(THREAD_DIR),
        _file(EVICTED, "e"),
        _file(NOTE, "p"),
        _file(GONE, "g"),
    ])


async def _sync(manifest: _Manifest, entries: list[dict]):
    """One pass over a sandbox whose scan lists ``entries``."""

    @asynccontextmanager
    async def _lock(_workspace_id):
        yield None

    listing = {
        "entries": entries,
        "oversized": [],
        "errors": [],
        "hashed": 0,
        "reused": len(entries),
        "started_ns": MTIME_NS,
        "boot_id": "boot-a",
        "clock_offset_ns": 0,
    }
    with (
        patch.object(transfer, "run_transfer_op", new=AsyncMock(return_value=listing)),
        patch.object(backup, "workspace_sync_lock", _lock),
        patch.object(backup, "manifest_clock", new=AsyncMock(return_value=CLOCK)),
        patch.object(backup, "is_storage_enabled", return_value=False),
        patch.object(backup, "files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch.object(backup, "get_file_metadata_for_sync", new=manifest.read),
        patch.object(backup, "delete_removed_files", new=manifest.prune),
        patch.object(backup, "bulk_upsert_files", new=manifest.upsert),
        patch.object(backup, "delete_file_rows", new=manifest.delete),
        patch.object(backup, "bulk_update_file_stamps", new=AsyncMock()),
        # Every file the scan lists is one the manifest already holds.
        patch.object(backup, "_persist_inline", new=AsyncMock(return_value=(0, []))) as inline,
        patch.object(backup, "get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch.object(backup, "set_files_scan_mark", new=AsyncMock()) as mark,
    ):
        result = await backup.sync_to_db(WS, SimpleNamespace(sandbox_id="sb-1"), layout=LAYOUT)
    inline.assert_not_awaited()
    return result, mark


@pytest.mark.asyncio
async def test_evicted_results_missing_before_the_marker_keep_their_rows():
    manifest = _manifest()

    result, mark = await _sync(manifest, [_file("notes.txt", "n"), _dir(".agents")])

    assert {RESULTS_DIR, THREAD_DIR, EVICTED} <= set(manifest.rows)
    # Everything else still prunes, a checkpoint note too: it came with the
    # first pass, so one missing now is one the turn deleted.
    assert GONE not in manifest.rows and NOTE not in manifest.rows
    assert result.deleted == 2
    # A withheld prune is one the next pass has to repeat, which a recorded
    # scan mark would let the sweep skip.
    assert result.pruned is False
    mark.assert_not_awaited()


@pytest.mark.asyncio
async def test_once_the_marker_is_listed_missing_results_prune_and_the_marker_is_no_row():
    manifest = _manifest()

    result, mark = await _sync(
        manifest,
        [
            _file("notes.txt", "n"),
            _dir(".agents"),
            _dir(RESULTS_DIR),
            _file(DEFERRED_MARKER, "m", size=0),
        ],
    )

    assert set(manifest.rows) == {"notes.txt", ".agents", RESULTS_DIR}
    assert result.deleted == 4
    assert result.pruned is True
    mark.assert_awaited_once()
