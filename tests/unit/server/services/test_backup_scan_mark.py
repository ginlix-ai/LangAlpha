"""The scan mark: what a clean sync vouches for, and when it may be recorded.

A later sweep skips a project whose files are all older than its mark, so a
mark recorded after a pass that left a savable file behind turns that file
into one no routine backup ever retries. These pin the recording gate in
``sync_to_db``, the mark's shape, and when a stored mark is trusted.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout
from src.server.services.persistence import backup, transfer
from src.server.services.persistence.transfer import (
    SCAN_MARK_MARGIN_NS,
    ScanEntry,
    ScanResult,
    ScanMark,
    ScanRules,
    SweepTarget,
    TransferRuntimeError,
    sweep_projects,
)

WS = "ws-scan-mark"
STARTED_NS = 1_750_000_000_000_000_000
BOOT = "boot-a"
OFFSET = 1_700_000_000_000_000_000
SANDBOX_ID = "sb-1"
RULES = ScanRules.of(None)
RULES_CAP = 100 * 1024 * 1024
LAYOUT = SandboxLayout.for_root("/home/workspace").for_workspace("mark-ab12")
LOCK_CONN = object()


def _scan(
    *,
    entries=(),
    oversized=(),
    errors=(),
    started_ns=STARTED_NS,
    boot_id=BOOT,
    clock_offset_ns=OFFSET,
):
    return ScanResult(
        entries=list(entries),
        oversized=list(oversized),
        errors=list(errors),
        hashed=0,
        reused=0,
        started_ns=started_ns,
        boot_id=boot_id,
        clock_offset_ns=clock_offset_ns,
    )


# --- ScanMark -------------------------------------------------------------


def test_mark_sits_a_margin_before_the_scan_started():
    mark = ScanMark.of(_scan(), SimpleNamespace(sandbox_id=SANDBOX_ID), RULES)
    assert mark.as_json() == {
        "ns": STARTED_NS - SCAN_MARK_MARGIN_NS,
        "boot_id": BOOT,
        "sandbox_id": SANDBOX_ID,
        "rules": RULES.fingerprint,
        "offset_ns": OFFSET,
    }


@pytest.mark.parametrize(
    "scan,sandbox",
    [
        # A runtime that predates the sweep reports no start time.
        (_scan(started_ns=None), SimpleNamespace(sandbox_id=SANDBOX_ID)),
        (_scan(boot_id=None), SimpleNamespace(sandbox_id=SANDBOX_ID)),
        # Nor a clock offset: the sweep could not tell a stepped-back clock.
        (_scan(clock_offset_ns=None), SimpleNamespace(sandbox_id=SANDBOX_ID)),
        # Without an id the mark could belong to any sandbox.
        (_scan(), SimpleNamespace(sandbox_id=None)),
        (_scan(), SimpleNamespace()),
    ],
)
def test_no_mark_when_the_scan_cannot_vouch(scan, sandbox):
    assert ScanMark.of(scan, sandbox, RULES) is None


def _stored(**overrides):
    return {
        "ns": 123,
        "boot_id": BOOT,
        "sandbox_id": SANDBOX_ID,
        "rules": RULES.fingerprint,
        "offset_ns": OFFSET,
        **overrides,
    }


def test_a_stored_mark_is_trusted_only_on_the_sandbox_that_wrote_it():
    """A recreated machine holds a restore; its change times say nothing about
    what the old sandbox's mark vouched for."""
    stored = _stored()
    assert ScanMark.trusted(stored, SANDBOX_ID, RULES) == ScanMark(
        123, BOOT, SANDBOX_ID, RULES.fingerprint, OFFSET
    )
    assert ScanMark.trusted(stored, "sb-after-recreate", RULES) is None
    assert ScanMark.trusted(stored, None, RULES) is None
    assert ScanMark.trusted(None, SANDBOX_ID, RULES) is None


@pytest.mark.parametrize(
    "stored",
    [
        _stored(sandbox_id=None),
        _stored(ns="123"),
        _stored(boot_id=""),
        _stored(offset_ns="1700"),
        _stored(offset_ns=1.5),
    ],
)
def test_a_malformed_stored_mark_is_not_trusted(stored):
    stored = {k: v for k, v in stored.items() if v is not None}
    assert ScanMark.trusted(stored, SANDBOX_ID, RULES) is None


# --- ScanRules: the mark vouches only for what its scan could admit -------


def test_rules_fingerprint_follows_the_cap_and_the_exclusions():
    """The same build and cap fingerprint alike; a cap change or an exclusion
    change does not. Only the fingerprint travels, so it must be stable."""
    assert ScanRules.of(None) == ScanRules.of(None)
    assert ScanRules.of(None).fingerprint != ScanRules.of(RULES_CAP).fingerprint
    assert len(RULES.fingerprint) == 16
    with patch.object(transfer, "EXCLUDE_BASENAMES", frozenset()):
        narrowed = ScanRules.of(None)
    assert narrowed.fingerprint != ScanRules.of(None).fingerprint


def test_a_mark_taken_under_other_rules_is_not_trusted():
    """Object storage switched on, or an exclusion dropped, makes files eligible
    that never changed; only a full sync finds them, so the old mark must go."""
    under_cap = ScanRules.of(RULES_CAP)
    assert ScanMark.trusted(_stored(), SANDBOX_ID, under_cap) is None
    assert ScanMark.trusted(_stored(rules=under_cap.fingerprint), SANDBOX_ID, RULES) is None
    assert ScanMark.trusted(_stored(rules=under_cap.fingerprint), SANDBOX_ID, under_cap) is not None


def test_a_mark_from_before_marks_carried_rules_is_not_trusted():
    """Whatever rules it was taken under, nothing says they are today's."""
    stored = _stored()
    del stored["rules"]
    assert ScanMark.trusted(stored, SANDBOX_ID, RULES) is None


def test_a_mark_from_before_marks_carried_a_clock_offset_is_not_trusted():
    """Without an offset the sweep cannot tell whether the wall clock stepped
    back since the mark; the project syncs in full once and a new mark takes over."""
    stored = _stored()
    del stored["offset_ns"]
    assert ScanMark.trusted(stored, SANDBOX_ID, RULES) is None


# --- the wire: scan_workspace and sweep_projects -------------------------


@pytest.mark.asyncio
async def test_scan_workspace_carries_start_time_and_boot():
    run = AsyncMock(
        return_value={
            "entries": [],
            "started_ns": STARTED_NS,
            "boot_id": BOOT,
            "clock_offset_ns": OFFSET,
        }
    )
    with patch.object(transfer, "run_transfer_op", run):
        scan = await transfer.scan_workspace(MagicMock(), {}, max_file_bytes=None, layout=LAYOUT)
    assert (scan.started_ns, scan.boot_id, scan.clock_offset_ns) == (STARTED_NS, BOOT, OFFSET)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "out",
    [
        {},
        {"started_ns": "123", "boot_id": "", "clock_offset_ns": "1"},
        {"started_ns": 1.5, "clock_offset_ns": 1.5},
    ],
)
async def test_scan_workspace_from_an_older_runtime_has_no_mark_material(out):
    run = AsyncMock(return_value={"entries": [], **out})
    with patch.object(transfer, "run_transfer_op", run):
        scan = await transfer.scan_workspace(MagicMock(), {}, max_file_bytes=None, layout=LAYOUT)
    assert scan.started_ns is None and scan.boot_id is None and scan.clock_offset_ns is None


@pytest.mark.asyncio
async def test_sweep_projects_sends_the_backup_exclusions_and_parses_the_result():
    targets = [
        SweepTarget("w1", "/home/workspace/a", None),
        SweepTarget("w2", "/home/workspace/b", ScanMark(123, BOOT, SANDBOX_ID, RULES.fingerprint, OFFSET)),
    ]
    run = AsyncMock(
        return_value={
            "changed": ["w1"],
            "unchanged": [],
            "missing": [],
            "visited": 3,
            "walk_ms": 1,
        }
    )
    with patch.object(transfer, "run_transfer_op", run):
        result = await sweep_projects(MagicMock(), targets)
    (_, op, spec), kwargs = run.await_args
    assert op == "sweep"
    # The sandbox id and the rules stay on the server: the runtime needs only
    # time, boot and the clock offset that lets it see a step back.
    assert spec["projects"] == [
        {"key": "w1", "root": "/home/workspace/a", "mark": None},
        {
            "key": "w2",
            "root": "/home/workspace/b",
            "mark": {"ns": 123, "boot_id": BOOT, "offset_ns": OFFSET},
        },
    ]
    # Parity with the scan comes from sending the very same exclusions.
    for key, value in transfer.exclusion_spec(None).items():
        assert spec[key] == value
    assert kwargs["timeout_s"] == transfer.SWEEP_TIMEOUT_S
    assert (result.changed, result.unchanged, result.missing, result.visited) == (
        ["w1"],
        [],
        [],
        3,
    )


@pytest.mark.asyncio
async def test_sweep_without_a_verdict_raises():
    """A runtime that predates the op answers with an error, not a verdict;
    reading that as "nothing changed" would skip every project."""
    run = AsyncMock(return_value={"error": "unknown op: sweep"})
    with patch.object(transfer, "run_transfer_op", run):
        with pytest.raises(TransferRuntimeError):
            await sweep_projects(MagicMock(), [])


# --- the recording gate in sync_to_db ------------------------------------


@pytest.fixture(autouse=True)
def _db():
    @asynccontextmanager
    async def _lock(_workspace_id):
        yield LOCK_CONN

    clock = datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)
    with (
        patch.object(backup, "workspace_sync_lock", _lock),
        patch.object(backup, "manifest_clock", new=AsyncMock(return_value=clock)),
        patch.object(backup, "get_file_metadata_for_sync", new=AsyncMock(return_value={})),
        patch.object(backup, "files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch.object(backup, "workspace_owner", new=AsyncMock(return_value="user-1")),
        patch.object(backup, "delete_removed_files", new=AsyncMock(return_value=0)),
        patch.object(backup, "get_workspace_total_size", new=AsyncMock(return_value=0)),
    ):
        yield


def _sandbox(sandbox_id=SANDBOX_ID):
    sandbox = MagicMock()
    sandbox.working_dir = "/home/workspace"
    sandbox.sandbox_id = sandbox_id
    return sandbox


async def _sync(scan, sandbox=None, cap=None):
    setter = AsyncMock()
    with (
        patch.object(backup, "scan_workspace", new=AsyncMock(return_value=scan)),
        patch.object(backup, "set_files_scan_mark", new=setter),
        patch.object(backup, "scan_cap_bytes", return_value=cap),
    ):
        result = await backup.sync_to_db(WS, sandbox or _sandbox(), layout=LAYOUT)
    return result, setter


def _expected_mark(rules=RULES):
    return {
        "ns": STARTED_NS - SCAN_MARK_MARGIN_NS,
        "boot_id": BOOT,
        "sandbox_id": SANDBOX_ID,
        "rules": rules.fingerprint,
        "offset_ns": OFFSET,
    }


@pytest.mark.asyncio
async def test_clean_pass_records_the_mark_under_the_lock():
    result, setter = await _sync(_scan())
    assert result.errors == 0
    setter.assert_awaited_once_with(WS, _expected_mark(), conn=LOCK_CONN)


@pytest.mark.asyncio
async def test_pass_that_writes_rows_records_the_mark():
    entry = ScanEntry("reports", "dir", 0, 1, 0o755, None, None, None)
    with patch.object(backup, "bulk_upsert_files", new=AsyncMock(return_value=1)) as upsert:
        result, setter = await _sync(_scan(entries=[entry]))
    upsert.assert_awaited_once()
    assert result.synced == 1
    setter.assert_awaited_once()


@pytest.mark.asyncio
async def test_unreadable_entry_holds_the_mark_back():
    """The next pass may read it; a mark would stop the sweep from asking."""
    result, setter = await _sync(
        _scan(errors=[{"path": "locked", "error": "Permission denied", "errno": 13}])
    )
    assert result.errors == 1
    setter.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_root_holds_the_mark_back():
    result, setter = await _sync(
        _scan(errors=[{"path": ".", "error": "No such file", "errno": 2}])
    )
    assert result.root_missing and result.errors == 0
    setter.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flag",
    [
        pytest.param(AsyncMock(return_value=True), id="restore-incomplete"),
        pytest.param(AsyncMock(side_effect=RuntimeError("pool exhausted")), id="flag-unreadable"),
    ],
)
async def test_a_pass_that_withheld_its_prune_holds_the_mark_back(flag):
    """Once the flag clears without a write, only a rescan prunes the files
    deleted meanwhile; a mark would let the sweep skip that rescan for good."""
    with patch.object(backup, "files_restore_incomplete", new=flag):
        result, setter = await _sync(_scan())
    assert result.errors == 0 and not result.pruned
    setter.assert_not_awaited()


@pytest.mark.asyncio
async def test_files_unsaved_for_good_do_not_hold_the_mark_back():
    """Too large or a path too long is refused by every later pass too, so
    withholding the mark would only rescan the project on every sweep."""
    result, setter = await _sync(
        _scan(
            oversized=[{"path": "big.bin", "size": 10**12}],
            errors=[{"path": "deep/" * 3, "error": "File name too long", "errno": 36}],
        )
    )
    assert result.errors == 0 and result.oversized == 2
    setter.assert_awaited_once_with(WS, _expected_mark(), conn=LOCK_CONN)


@pytest.mark.asyncio
async def test_file_vanishing_mid_scan_does_not_hold_the_mark_back():
    """Deleted between listing and read is absent for the right reason."""
    _, setter = await _sync(
        _scan(errors=[{"path": "tmp.txt", "error": "No such file", "errno": 2}])
    )
    setter.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_transfer_holds_the_mark_back():
    """An upload that failed leaves a savable file behind, like a read error."""
    entry = ScanEntry("new.bin", "file", 10, 1, 0o644, "abc", None, True)
    unsaved = [backup.UnsavedFile("new.bin", "failed", 10)]
    with (
        patch.object(backup, "is_storage_enabled", return_value=False),
        patch.object(backup, "_persist_inline", new=AsyncMock(return_value=(0, unsaved))),
    ):
        result, setter = await _sync(_scan(entries=[entry]))
    assert result.errors == 1
    setter.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_mark_to_record_without_a_sandbox_id():
    _, setter = await _sync(_scan(), sandbox=_sandbox(sandbox_id=None))
    setter.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_mark_write_does_not_fail_a_sync_that_saved_everything():
    """The mark is only a hint: without it the next sweep reads the project as
    changed, which is safe. Raising would tell a strict caller that saved
    files were unsaved."""
    setter = AsyncMock(side_effect=RuntimeError('column "files_scan_mark" does not exist'))
    with (
        patch.object(backup, "scan_workspace", new=AsyncMock(return_value=_scan())),
        patch.object(backup, "set_files_scan_mark", new=setter),
        patch.object(backup, "scan_cap_bytes", return_value=None),
    ):
        result = await backup.sync_to_db(WS, _sandbox(), layout=LAYOUT)
    setter.assert_awaited_once()
    assert result.errors == 0 and result.scan_mark.as_json() == _expected_mark()


@pytest.mark.asyncio
async def test_the_recorded_mark_carries_the_rules_the_scan_ran_under():
    """The cap the scan was given is the cap the mark names: a sweep under a
    different cap then sees a foreign mark and syncs the project in full."""
    _, setter = await _sync(_scan(), cap=RULES_CAP)
    setter.assert_awaited_once_with(WS, _expected_mark(ScanRules.of(RULES_CAP)), conn=LOCK_CONN)
