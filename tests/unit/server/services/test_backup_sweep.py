"""The post-turn backup: which projects on the machine get mirrored.

The sweep only chooses; ``_mirror_projects`` does the mirroring through the
same loop and fence as a teardown's backup. What these pin is the choice and
the fence: every project the sweep cannot vouch for is synced, each is fenced
by the machine's durable ref, and nothing that goes wrong with the sweep
leaves the turn's own project unsaved.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout
from src.server.services.computer_manager import _machine_backup
from src.server.services.computer_manager._machine_backup import MachineBackupMixin
from src.server.services.computer_manager._sessions import SessionCacheMixin
from src.server.services.persistence import backup
from src.server.services.persistence.sync_result import BackupIncomplete
from src.server.services.persistence.transfer import (
    ScanMark,
    ScanResult,
    ScanRules,
    SweepResult,
)
from src.server.services.workspace_layout import layout_from_binding

COMPUTER = "comp-1"
SANDBOX_ID = "sb-live"
DURABLE_REF = "sb-durable"
ROOT = "/home/workspace"
RULES = ScanRules.of(None)
OFFSET = 1_700_000_000_000_000_000
MARK = {
    "ns": 123,
    "boot_id": "boot-a",
    "sandbox_id": SANDBOX_ID,
    "rules": RULES.fingerprint,
    "offset_ns": OFFSET,
}


class _Manager(MachineBackupMixin):
    _session_sandbox_id = staticmethod(SessionCacheMixin._session_sandbox_id)

    def __init__(self, turn_project, cached=None):
        self._cached = cached
        self.config = SimpleNamespace(filesystem=SimpleNamespace(working_directory=ROOT))
        self.resolve_binding = AsyncMock(
            return_value=SimpleNamespace(
                workspace_id=turn_project,
                computer_id=COMPUTER,
                root_dir=ROOT,
                provider_ref=DURABLE_REF,
                files_backup=True,
            )
        )
        self.backup_project_files = AsyncMock(return_value=True)

    def _cached_session(self, computer_id):
        return self._cached

    async def _project_layout(self, workspace_id, computer_id, *, dir_name=None, root=None):
        return layout_from_binding(
            workspace_id, {"dir_name": dir_name, "computer_id": computer_id}, root=root
        )


def _session(sandbox_id=SANDBOX_ID):
    return SimpleNamespace(sandbox=SimpleNamespace(sandbox_id=sandbox_id))


def _row(ws, mark=MARK, dir_name="default"):
    return {
        "workspace_id": ws,
        "dir_name": f"{ws}-dir" if dir_name == "default" else dir_name,
        "files_scan_mark": mark,
    }


def _verdict(changed=(), unchanged=(), missing=()):
    return SweepResult(list(changed), list(unchanged), list(missing), visited=0, walk_ms=0)


async def _run(rows, *, turn="w2", session="default", cached=None, sweep=None, marks=None):
    session = _session() if session == "default" else session
    mgr = _Manager(turn, cached=cached)
    sweeper = (
        AsyncMock(side_effect=sweep)
        if isinstance(sweep, Exception)
        else AsyncMock(return_value=sweep or _verdict())
    )
    with (
        patch.object(
            _machine_backup,
            "get_scan_marks_for_computer",
            new=marks or AsyncMock(return_value=rows),
        ),
        patch.object(_machine_backup, "sweep_projects", new=sweeper),
        patch.object(_machine_backup, "scan_cap_bytes", return_value=None),
    ):
        await mgr.backup_changed_projects(turn, session=session)
    return mgr, sweeper


def _synced(mgr):
    return [c.args[0] for c in mgr.backup_project_files.await_args_list]


def _swept(sweeper):
    return sweeper.await_args.args[1]


@pytest.mark.asyncio
async def test_the_turns_project_is_swept_first():
    _, sweeper = await _run([_row("w1"), _row("w2"), _row("w3")], turn="w2")
    assert [t.key for t in _swept(sweeper)] == ["w2", "w1", "w3"]
    assert _swept(sweeper)[0].root == f"{ROOT}/w2-dir"


@pytest.mark.asyncio
async def test_only_changed_projects_are_synced_fenced_by_the_machine():
    """The durable ref, not each project's shadow column: a sibling row that
    lags behind the machine must not skip its own mirror."""
    rows = [_row("w1"), _row("w2"), _row("w3"), _row("w4")]
    mgr, _ = await _run(
        rows, sweep=_verdict(changed=["w2", "w3"], unchanged=["w1"], missing=["w4"])
    )
    assert _synced(mgr) == ["w2", "w3"]
    for call in mgr.backup_project_files.await_args_list:
        assert call.kwargs["expected_sandbox_id"] == DURABLE_REF
        # The folder the plan resolved is reused, not read again.
        assert call.kwargs["layout"].workspace == f"{ROOT}/{call.args[0]}-dir"
        # Each project's pass reads the switch again, as the owner may turn
        # backups off while the sweep is under way.
        assert call.kwargs["routine"] is True


@pytest.mark.asyncio
async def test_a_folder_a_settle_moved_during_the_sweep_is_still_mirrored():
    """The run is over, so a settle on another worker may move a folder after
    its path was read: it sweeps as missing, and left out of the mirror, the
    turn's edits wait for the next backup. A folder missing with its row
    unchanged is not on the disk to mirror."""
    before = [_row("w1"), _row("w2"), _row("w3"), _row("w4")]
    after = [_row("w1"), _row("w2", dir_name="_internal/moving/w2"),
             _row("w3", dir_name="Moved"), _row("w4")]
    mgr, _ = await _run(
        before,
        sweep=_verdict(changed=["w1"], missing=["w2", "w3", "w4"]),
        marks=AsyncMock(side_effect=[before, after]),
    )
    # A staged folder too: the mirror waits for its hold and reads it again.
    assert _synced(mgr) == ["w1", "w2", "w3"]


@pytest.mark.asyncio
async def test_only_a_mark_this_sandbox_wrote_reaches_the_sweep():
    rows = [_row("w1"), _row("w2", mark={**MARK, "sandbox_id": "sb-before-recreate"})]
    _, sweeper = await _run(rows, turn="w1")
    assert [t.mark for t in _swept(sweeper)] == [
        ScanMark(123, "boot-a", SANDBOX_ID, RULES.fingerprint, OFFSET),
        None,
    ]


@pytest.mark.asyncio
async def test_a_mark_from_other_rules_or_none_is_not_handed_to_the_sweep():
    """A deploy that admits more files leaves marks that vouch for fewer; each
    such project is walked as changed and synced in full once."""
    rows = [
        _row("w1", mark={**MARK, "rules": ScanRules.of(1).fingerprint}),
        _row("w2", mark={k: v for k, v in MARK.items() if k != "rules"}),
        # Written before marks carried a clock offset.
        _row("w4", mark={k: v for k, v in MARK.items() if k != "offset_ns"}),
        _row("w3"),
    ]
    _, sweeper = await _run(rows, turn="w3")
    assert [(t.key, t.mark is None) for t in _swept(sweeper)] == [
        ("w3", False),
        ("w1", True),
        ("w2", True),
        ("w4", True),
    ]


@pytest.mark.asyncio
async def test_a_project_with_no_folder_is_synced_without_a_sweep():
    """The sync is what knows how to handle a project naming no folder."""
    rows = [_row("w1"), _row("w2", dir_name=None)]
    mgr, sweeper = await _run(rows, turn="w1", sweep=_verdict(unchanged=["w1"]))
    assert [t.key for t in _swept(sweeper)] == ["w1"]
    assert _synced(mgr) == ["w2"]


@pytest.mark.asyncio
async def test_the_cached_session_is_used_when_none_is_passed():
    mgr, sweeper = await _run(
        [_row("w1")], turn="w1", session=None, cached=_session(), sweep=_verdict(changed=["w1"])
    )
    assert _synced(mgr) == ["w1"]
    assert _swept(sweeper)[0].mark is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("session", [None, SimpleNamespace(sandbox=None)])
async def test_without_a_sandbox_only_the_turns_project_is_handed_on(session):
    """``backup_project_files`` owns the no-session case: it skips and logs."""
    marks = AsyncMock()
    mgr, sweeper = await _run([], turn="w1", session=session, marks=marks)
    assert _synced(mgr) == ["w1"]
    sweeper.assert_not_awaited()
    marks.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marks,sweep",
    [
        pytest.param(AsyncMock(side_effect=RuntimeError("pool exhausted")), None, id="plan"),
        pytest.param(None, RuntimeError("exec timed out"), id="sweep"),
    ],
)
async def test_a_failed_plan_or_sweep_mirrors_the_turns_project_only(marks, sweep):
    session = _session()
    mgr, _ = await _run(
        [_row("w1"), _row("w2")], turn="w2", session=session, marks=marks, sweep=sweep
    )
    assert _synced(mgr) == ["w2"]
    call = mgr.backup_project_files.await_args
    assert call.kwargs["session"] is session
    assert call.kwargs["expected_sandbox_id"] == DURABLE_REF


@pytest.mark.asyncio
async def test_a_machine_whose_owner_turned_backups_off_is_neither_swept_nor_mirrored():
    """Off keeps the last backup as it was, which a pass after the turn would
    overwrite with the folder as it is now."""
    mgr = _Manager("w2")
    mgr.resolve_binding.return_value.files_backup = False
    marks = AsyncMock(return_value=[_row("w1"), _row("w2")])
    sweeper = AsyncMock(return_value=_verdict(changed=["w1", "w2"]))
    with (
        patch.object(_machine_backup, "get_scan_marks_for_computer", new=marks),
        patch.object(_machine_backup, "sweep_projects", new=sweeper),
    ):
        await mgr.backup_changed_projects("w2", session=_session())

    assert _synced(mgr) == []
    marks.assert_not_awaited()
    sweeper.assert_not_awaited()


# --- a computer the Chief of Staff reached from Home ------------------------


async def _run_computer(
    rows,
    *,
    live,
    sweep=None,
    files_backup=True,
    live_ids=(),
    cached=None,
    status="running",
    unmirrored=(),
):
    mgr = _Manager("", cached=cached)
    mgr.backup_project_files = AsyncMock(side_effect=lambda ws, **_: ws not in unmirrored)
    binding = mgr.resolve_binding.return_value
    binding.workspace_id = ""
    binding.files_backup = files_backup
    row = {"computer_id": COMPUTER, "provider_ref": DURABLE_REF, "status": status}
    mgr._binding_from_computer = MagicMock(return_value=binding)
    mgr.cached_session_for_computer = MagicMock(return_value=live)
    mgr._start_machine = AsyncMock()
    sweeper = (
        AsyncMock(side_effect=sweep)
        if isinstance(sweep, Exception)
        else AsyncMock(return_value=sweep or _verdict())
    )
    with (
        patch("src.server.database.computer.get_computer", new=AsyncMock(return_value=row)),
        patch.object(_machine_backup, "get_scan_marks_for_computer", new=AsyncMock(return_value=rows)),
        patch.object(
            _machine_backup, "get_live_workspace_ids_for_computer", new=AsyncMock(return_value=list(live_ids))
        ),
        patch.object(_machine_backup, "sweep_projects", new=sweeper),
        patch.object(_machine_backup, "scan_cap_bytes", return_value=None),
    ):
        done = await mgr.backup_computer_changes(COMPUTER)
    # The machine's start path may rebuild or replace it: a backup never takes it.
    mgr._start_machine.assert_not_awaited()
    return mgr, sweeper, done


@pytest.mark.asyncio
async def test_a_reached_computer_mirrors_what_changed_on_this_workers_live_session():
    mgr, sweeper, done = await _run_computer(
        [_row("w1"), _row("w2")], live=_session(), sweep=_verdict(changed=["w2"], unchanged=["w1"])
    )
    assert done
    assert [t.key for t in _swept(sweeper)] == ["w1", "w2"]
    assert _synced(mgr) == ["w2"]


@pytest.mark.asyncio
async def test_a_reached_computer_with_a_project_left_unmirrored_stays_owed():
    """No turn there sweeps it again, so the mirror is tried again rather than
    counted done."""
    with pytest.raises(BackupIncomplete):
        await _run_computer(
            [_row("w1"), _row("w2")],
            live=_session(),
            sweep=_verdict(changed=["w1", "w2"]),
            unmirrored={"w1"},
        )


@pytest.mark.asyncio
async def test_a_worker_without_a_live_session_there_owes_the_mirror():
    """Not even through a session it cached once: a stopped computer's handle
    can reconnect, and the start path can rebuild it from the backup this
    would have brought up to date. The worker that holds it mirrors."""
    mgr, sweeper, done = await _run_computer([_row("w1")], live=None, cached=_session())
    assert not done
    assert _synced(mgr) == []
    sweeper.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_stopped_computer_owes_nothing_though_a_handle_still_looks_live():
    """Its stop mirrored it, and a handle another worker's stop left behind
    would reconnect to it."""
    mgr, sweeper, done = await _run_computer([_row("w1")], live=_session(), status="stopped")
    assert done
    mgr.cached_session_for_computer.assert_not_called()
    sweeper.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_reached_computer_whose_owner_turned_backups_off_is_left_as_it_is():
    mgr, sweeper, done = await _run_computer([_row("w1")], live=_session(), files_backup=False)
    assert done
    assert _synced(mgr) == []
    sweeper.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_sweep_of_a_reached_computer_mirrors_every_project_there():
    """It holds no project of the turn's to fall back on, so a fallback to that
    left every change there unmirrored."""
    mgr, _, _ = await _run_computer(
        [_row("w1"), _row("w2")], live=_session(), sweep=RuntimeError("exec timed out"),
        live_ids=["w1", "w2"],
    )
    assert _synced(mgr) == ["w1", "w2"]


# --- both paths derive the rules the same way -----------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("blobs_on", [False, True])
async def test_the_sweep_trusts_the_mark_the_sync_recorded_under_the_same_deployment(blobs_on):
    """The sync records a mark for the cap it scanned under; the sweep asks
    whether that mark holds for the cap a sync would use now. Both resolve
    the cap from the same sandbox and storage setting, unpatched here, so a
    drift between the two derivations shows up as a mark never trusted."""
    sandbox = MagicMock()
    sandbox.working_dir = ROOT
    sandbox.sandbox_id = SANDBOX_ID
    layout = SandboxLayout.for_root(ROOT).for_workspace("w1-dir")
    scan = ScanResult(
        [], [], [], 0, 0, started_ns=10**18, boot_id="boot-a", clock_offset_ns=OFFSET
    )
    setter = AsyncMock()

    @asynccontextmanager
    async def _lock(_workspace_id):
        yield object()

    clock = datetime(2026, 9, 25, tzinfo=timezone.utc)
    with (
        patch.object(backup, "is_storage_enabled", return_value=blobs_on),
        patch.object(_machine_backup, "is_storage_enabled", return_value=blobs_on),
        patch.object(backup, "workspace_sync_lock", _lock),
        patch.object(backup, "manifest_clock", new=AsyncMock(return_value=clock)),
        patch.object(backup, "get_file_metadata_for_sync", new=AsyncMock(return_value={})),
        patch.object(backup, "files_restore_incomplete", new=AsyncMock(return_value=False)),
        patch.object(backup, "workspace_owner", new=AsyncMock(return_value="user-1")),
        patch.object(backup, "delete_removed_files", new=AsyncMock(return_value=0)),
        patch.object(backup, "get_workspace_total_size", new=AsyncMock(return_value=0)),
        patch.object(backup, "scan_workspace", new=AsyncMock(return_value=scan)),
        patch.object(backup, "set_files_scan_mark", new=setter),
    ):
        await backup.sync_to_db("w1", sandbox, layout=layout)
        recorded = setter.await_args.args[1]

        mgr = _Manager("w1")
        session = SimpleNamespace(sandbox=sandbox)
        sweeper = AsyncMock(return_value=_verdict(unchanged=["w1"]))
        with (
            patch.object(
                _machine_backup,
                "get_scan_marks_for_computer",
                new=AsyncMock(return_value=[_row("w1", mark=recorded)]),
            ),
            patch.object(_machine_backup, "sweep_projects", new=sweeper),
        ):
            await mgr.backup_changed_projects("w1", session=session)

    assert "rules" in recorded
    assert _swept(sweeper)[0].mark == ScanMark(
        recorded["ns"], "boot-a", SANDBOX_ID, recorded["rules"], OFFSET
    )
    assert _synced(mgr) == []
