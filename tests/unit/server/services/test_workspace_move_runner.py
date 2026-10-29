"""The move runner where it stops short: a lost row, and a folder to put back.

A runner that loses its row mid-backup lets go of the source at once rather
than when the backup ends. A rollback restores from the mirror only once the
folder cannot come back, because the mirror lacks the folder's .git and
dependencies; until then the row stays staged and the sweep tries again.
"""

import asyncio
import json
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.computer_manager import ComputerManager
from src.server.services.computer_errors import ComputerBusyError
from src.server.services.computer_manager._move import _PUT_BACK_GIVE_UP_S, _folder_held
from src.server.services.persistence.transfer import SYNC_MARKER_NAME
from src.server.services.workspace_move import MoveNoRoom, move_error, move_from_row
from tests.unit.restored_overlay import archive, python3_on_path

_MOVE = "src.server.services.computer_manager._move"
MOVE_ID = "66666666-6666-4666-8666-666666666666"
SOURCE = "77777777-7777-4777-8777-777777777777"
ROOT = "/data/root"
AWAY = f"_internal/leftovers/move-{MOVE_ID}"


def _staged(age_s=60, **overrides):
    """Staged ``age_s`` ago, and claimed long before: a long backup sits between."""
    staged_at = datetime.now(timezone.utc) - timedelta(seconds=age_s)
    row = {
        "move_id": MOVE_ID,
        "runner_id": "runner-1",
        "user_id": "user-1",
        "workspace_id": "ws-1",
        "source_computer_id": SOURCE,
        "source_folder": "Research",
        "leftovers_path": AWAY,
        "phase": "staged",
        "created_at": staged_at - timedelta(seconds=2 * _PUT_BACK_GIVE_UP_S),
        "staged_at": staged_at,
    }
    row.update(overrides)
    return row


def _computer(status="running"):
    return {
        "computer_id": SOURCE,
        "user_id": "user-1",
        "kind": "docker",
        "status": status,
        "provider_ref": "sb-old",
        "root_dir": ROOT,
    }


def _manager():
    return ComputerManager(SimpleNamespace(sandbox=SimpleNamespace(provider="docker")))


@pytest.fixture
def rollback():
    """A manager whose put-back answers as the test sets it, and the marks it makes."""
    manager = _manager()
    state = SimpleNamespace(outcome="back")

    async def put_back(row):
        return state.outcome

    manager._put_folder_back = put_back
    with (
        patch(f"{_MOVE}.finish_workspace_move", AsyncMock()) as finish,
        patch(f"{_MOVE}.flag_rollback_restore", AsyncMock()) as flag,
    ):
        yield SimpleNamespace(manager=manager, state=state, finish=finish, flag=flag)


@pytest.mark.asyncio
async def test_a_folder_put_back_ends_the_move_with_nothing_left(rollback):
    await rollback.manager._roll_back_move(_staged(), {"code": "busy"})

    rollback.flag.assert_not_awaited()
    rollback.finish.assert_awaited_once_with(
        MOVE_ID, "runner-1", "failed", error={"code": "busy"}, leftovers_path=None
    )


@pytest.mark.asyncio
async def test_a_put_back_that_can_succeed_later_leaves_the_move_staged(rollback):
    rollback.state.outcome = "later"

    await rollback.manager._roll_back_move(_staged(), {"code": "busy"})

    rollback.flag.assert_not_awaited()
    rollback.finish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "age_s"),
    [("never", 60), ("later", _PUT_BACK_GIVE_UP_S + 60)],
    ids=["cannot", "gave-up"],
)
async def test_the_mirror_restores_only_a_folder_that_cannot_come_back(
    rollback, outcome, age_s
):
    rollback.state.outcome = outcome

    await rollback.manager._roll_back_move(_staged(age_s), {"code": "busy"})

    rollback.flag.assert_awaited_once_with(MOVE_ID, "ws-1")
    rollback.finish.assert_awaited_once_with(
        MOVE_ID, "runner-1", "failed", error={"code": "busy"}
    )


@pytest.fixture
def put_back():
    """A manager whose source sandbox runs the put-back script with a set exit code."""
    manager = _manager()
    state = SimpleNamespace(exit_code=0, computers=[_computer()])
    runtime = MagicMock()
    runtime.exec = AsyncMock(
        side_effect=lambda command: SimpleNamespace(exit_code=state.exit_code)
    )

    @asynccontextmanager
    async def detached(provider_ref, *, binding=None):
        yield runtime

    @asynccontextmanager
    async def folders_lock(computer_id):
        yield object()

    manager._detached_runtime = detached
    manager.wake_computer = AsyncMock()
    clear = AsyncMock(return_value=None)
    with (
        patch(f"{_MOVE}.get_computer", AsyncMock(side_effect=lambda _: state.computers.pop(0))),
        patch(f"{_MOVE}.workspace_folders_lock", folders_lock),
        patch(f"{_MOVE}.hold_workspace_folder", AsyncMock(return_value=True)),
        patch(f"{_MOVE}._clear_move_folder", clear),
    ):
        yield SimpleNamespace(manager=manager, state=state, runtime=runtime, clear=clear)


@pytest.mark.asyncio
async def test_a_stopped_source_is_started_to_take_its_folder_back(put_back):
    put_back.state.computers = [_computer("stopped"), _computer()]

    assert await put_back.manager._put_folder_back(_staged()) == "back"

    put_back.manager.wake_computer.assert_awaited_once_with(SOURCE, user_id="user-1")
    put_back.clear.assert_awaited_once_with(
        put_back.runtime, ROOT, f"move-{MOVE_ID}", "Research", "finish"
    )
    (command,) = put_back.runtime.exec.await_args.args
    assert f"mv {ROOT}/{AWAY} {ROOT}/Research" in command


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("computers", "exit_code", "outcome"),
    [
        ([_computer("deleted")], 0, "never"),
        ([_computer()], 3, "never"),
        ([_computer()], 1, "later"),
        ([_computer("stopped"), _computer("stopped")], 0, "later"),
    ],
    ids=["computer-gone", "name-taken", "exec-failed", "did-not-start"],
)
async def test_a_put_back_says_whether_trying_again_can_help(
    put_back, computers, exit_code, outcome
):
    put_back.state.computers = computers
    put_back.state.exit_code = exit_code

    assert await put_back.manager._put_folder_back(_staged()) == outcome


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("aside", "under_name", "outcome"),
    [(True, False, "back"), (False, True, "back"), (True, True, "never"), (False, False, "never")],
    ids=["set-aside", "never-left", "name-taken", "gone-from-both"],
)
async def test_a_put_back_reads_where_the_folder_is(
    put_back, tmp_path, aside, under_name, outcome
):
    """The script itself, run by sh: the row records the leftovers before the
    folder leaves, so a folder still under its name never left, and only a
    ``never`` restores the mirror over it."""
    put_back.state.computers = [{**_computer(), "root_dir": str(tmp_path)}]
    put_back.runtime.exec.side_effect = lambda command: SimpleNamespace(
        exit_code=subprocess.run(["sh", "-c", command]).returncode
    )
    if aside:
        (tmp_path / AWAY).mkdir(parents=True)
        (tmp_path / AWAY / "notes.txt").write_text("aside")
    if under_name:
        (tmp_path / "Research").mkdir()

    assert await put_back.manager._put_folder_back(_staged()) == outcome
    if aside and not under_name:
        assert (tmp_path / "Research" / "notes.txt").read_text() == "aside"


@pytest.mark.asyncio
async def test_a_runner_that_loses_its_row_stops_its_backup_at_once():
    """Held under the source's decision lock, a backup left running would keep
    a new move or a spec change on that computer waiting until it ended."""
    manager = _manager()
    started = asyncio.Event()

    async def backup(*args, **kwargs):
        started.set()
        await asyncio.sleep(3600)

    manager.backup_project_files = backup
    manager._set_folder_aside = AsyncMock()
    lost = asyncio.Event()
    move = _staged(phase="preparing")
    with (
        patch(f"{_MOVE}.advance_workspace_move", AsyncMock(return_value={"phase": "backing_up"})),
        patch(f"{_MOVE}.get_computer", AsyncMock(return_value=_computer())),
    ):
        task = asyncio.create_task(
            manager._back_up_and_set_aside(move, SimpleNamespace(sandbox=MagicMock()), lost)
        )
        await started.wait()
        lost.set()
        assert await asyncio.wait_for(task, timeout=1) is None
    manager._set_folder_aside.assert_not_awaited()


@pytest.fixture
def folder_hold():
    """The folder lock always free, and the folder held as the test sets it."""
    state = SimpleNamespace(holds=[], tries=0)

    @asynccontextmanager
    async def folders_lock(computer_id):
        yield object()

    async def hold(conn, workspace_id):
        state.tries += 1
        return state.holds.pop(0) if state.holds else False

    with (
        patch(f"{_MOVE}.workspace_folders_lock", folders_lock),
        patch(f"{_MOVE}.hold_workspace_folder", hold),
        patch(f"{_MOVE}._FOLDER_HOLD_RETRY_S", 0),
    ):
        yield state


@pytest.mark.asyncio
async def test_setting_aside_waits_out_the_backup_after_a_turn(folder_hold):
    """A turn's backup holds the folder as it ends; a move right after it waits."""
    folder_hold.holds = [False, False, True]

    async with _folder_held(SOURCE, "ws-1") as conn:
        assert conn is not None
    assert folder_hold.tries == 3


@pytest.mark.asyncio
async def test_a_folder_held_past_the_wait_fails_the_move_busy(folder_hold):
    with patch(f"{_MOVE}._FOLDER_HOLD_WAIT_S", 0):
        with pytest.raises(ComputerBusyError, match="a file change holds the folder"):
            async with _folder_held(SOURCE, "ws-1"):
                pass


def test_an_incomplete_backup_keeps_its_own_sentence_and_files():
    error = {
        "code": "backup_incomplete",
        "message": "2 files couldn't be saved, so nothing was moved.",
        "files": [{"path": "a.bin", "reason": "unreadable", "size": 12}],
    }
    row = {
        **_staged(),
        "target_computer_id": "88888888-8888-4888-8888-888888888888",
        "make_primary": False,
        "phase": "failed",
        "error": error,
        "stale": False,
    }

    assert move_from_row(row).error.model_dump() == error


class _LocalShell:
    """Runs the commands the runner builds in a local shell."""

    async def upload_file(self, content, dest_path):
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
        Path(dest_path).write_bytes(content)

    async def exec(self, command, timeout=None):
        done = subprocess.run(
            ["/bin/sh", "-c", command], capture_output=True, text=True, timeout=timeout
        )
        return SimpleNamespace(exit_code=done.returncode, stdout=done.stdout, stderr=done.stderr)


@pytest.mark.asyncio
async def test_a_put_back_finishes_the_delete_a_copy_aside_left(tmp_path, monkeypatch):
    """The folder was copied aside and its delete where it is cut short: both
    paths exist. The copy is whole and the folder is not, so what is left of
    the folder goes and the copy comes back, rather than the mirror, which
    lacks .git and dependencies."""
    root = tmp_path / "root"
    (root / "Research").mkdir(parents=True)
    (root / "Research" / "left.txt").write_text("not yet deleted")
    copy = root / AWAY
    (copy / ".git").mkdir(parents=True)
    (copy / ".git" / "HEAD").write_text("ref")
    (copy / "left.txt").write_text("not yet deleted")
    journal = root / "_internal" / "moving" / "landed.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(json.dumps(
        {f"c:move-{MOVE_ID}": ["Research", copy.lstat().st_ino, AWAY]}
    ))
    python3_on_path(monkeypatch, tmp_path / "bin", archive(root))
    manager = _manager()

    @asynccontextmanager
    async def detached(provider_ref, *, binding=None):
        yield _LocalShell()

    @asynccontextmanager
    async def folders_lock(computer_id):
        yield object()

    manager._detached_runtime = detached
    computer = {**_computer(), "root_dir": str(root)}
    with (
        patch(f"{_MOVE}.get_computer", AsyncMock(return_value=computer)),
        patch(f"{_MOVE}.workspace_folders_lock", folders_lock),
        patch(f"{_MOVE}.hold_workspace_folder", AsyncMock(return_value=True)),
    ):
        assert await manager._put_folder_back(_staged()) == "back"

    assert (root / "Research" / ".git" / "HEAD").read_text() == "ref"
    assert not copy.exists()
    assert json.loads(journal.read_text()) == {}


@pytest.mark.asyncio
async def test_a_folder_with_no_room_for_its_copy_is_deleted_after_its_backup(
    tmp_path, monkeypatch
):
    """Restored from its archive, the source refuses to rename the folder
    aside, and its disk has no room for a copy. The backup is all that is
    kept, so it is taken again under the hold, which a file change begun
    before the move no longer has, and a rollback is flagged to restore from
    it before anything goes. Nothing is left behind."""
    root = tmp_path / "root"
    folder = root / "Research"
    (folder / "src").mkdir(parents=True)
    (folder / "src" / "a.py").write_text("x")
    (folder / SYNC_MARKER_NAME).write_text("synced")
    python3_on_path(monkeypatch, tmp_path / "bin", archive(root), full=True)
    seen = []

    async def back_up(**kwargs):
        seen.append(("backup", kwargs, folder.exists()))
        return True

    flag = AsyncMock(side_effect=lambda *_: seen.append(("flag", folder.exists())))
    advance = AsyncMock(return_value={"phase": "staged", "leftovers_path": None})

    @asynccontextmanager
    async def held(computer_id, workspace_id):
        yield object()

    with (
        patch(f"{_MOVE}._folder_held", held),
        patch(f"{_MOVE}.get_workspace_dir_name", AsyncMock(return_value="Research")),
        patch(f"{_MOVE}.stage_workspace_move", AsyncMock(return_value=_staged())),
        patch(f"{_MOVE}.flag_rollback_restore", flag),
        patch(f"{_MOVE}.advance_workspace_move", advance),
    ):
        staged = await _manager()._set_folder_aside(
            _staged(), _LocalShell(), str(root), back_up, asyncio.Event(),
            owner_machine=False,
        )

    assert staged == {"phase": "staged", "leftovers_path": None}
    flag.assert_awaited_once_with(MOVE_ID, "ws-1")
    assert seen == [("backup", {"held_folder": "Research"}, True), ("flag", True)]
    advance.assert_awaited_once_with(MOVE_ID, "runner-1", "staged", leftovers_path=None)
    assert not folder.exists()
    assert not (root / AWAY).exists()


@pytest.mark.asyncio
async def test_a_folder_with_no_room_on_its_owners_machine_stays_and_fails_the_move(
    tmp_path, monkeypatch
):
    """The same disk on a machine its owner brought: the backup lacks .git and
    dependencies that may exist nowhere else, so the folder is never cut down
    to it. It stays whole under its name, with its sync marker, nothing of the
    copy is left, and the move fails with its own sentence."""
    root = tmp_path / "root"
    folder = root / "Research"
    (folder / ".git").mkdir(parents=True)
    (folder / ".git" / "HEAD").write_text("ref")
    (folder / SYNC_MARKER_NAME).write_text("synced")
    python3_on_path(monkeypatch, tmp_path / "bin", archive(root), full=True)
    back_up = AsyncMock(return_value=True)
    flag = AsyncMock()

    @asynccontextmanager
    async def held(computer_id, workspace_id):
        yield object()

    with (
        patch(f"{_MOVE}._folder_held", held),
        patch(f"{_MOVE}.get_workspace_dir_name", AsyncMock(return_value="Research")),
        patch(f"{_MOVE}.stage_workspace_move", AsyncMock(return_value=_staged())),
        patch(f"{_MOVE}.flag_rollback_restore", flag),
        pytest.raises(MoveNoRoom) as raised,
    ):
        await _manager()._set_folder_aside(
            _staged(), _LocalShell(), str(root), back_up, asyncio.Event(),
            owner_machine=True,
        )

    back_up.assert_not_awaited()
    flag.assert_not_awaited()
    assert (folder / ".git" / "HEAD").read_text() == "ref"
    assert (folder / SYNC_MARKER_NAME).exists()
    assert not (root / AWAY).exists()
    assert not any((root / "_internal" / "leftovers").glob("*.copying"))
    assert move_error(raised.value)["code"] == "no_room"


# --- a move from the backup, off a machine its owner brought ----------------


@pytest.fixture
def from_backup(folder_hold):
    """A move that lays the workspace on its target from the backup alone."""
    folder_hold.holds = [True]
    manager = _manager()

    @asynccontextmanager
    async def heartbeat(move_id, runner_id, source_id):
        yield asyncio.Event()

    @asynccontextmanager
    async def decision_lock(computer_id):
        yield True

    manager._move_heartbeat = heartbeat
    manager._machine_decision_lock = decision_lock
    manager._wake_move_source = AsyncMock()
    manager.backup_project_files = AsyncMock()
    manager._back_up_and_set_aside = AsyncMock()
    manager._commit_move = AsyncMock(return_value={"move_id": MOVE_ID, "phase": "committed"})
    manager._finish_committed_move = AsyncMock()
    manager._abort_move = AsyncMock()
    staged = _staged(leftovers_path=None, from_backup=True)
    with (
        patch(f"{_MOVE}.advance_workspace_move", AsyncMock(return_value={"phase": "backing_up"})),
        patch(f"{_MOVE}.get_workspace_dir_name", AsyncMock(return_value="Research")) as folder,
        patch(f"{_MOVE}.stage_workspace_move", AsyncMock(return_value=staged)) as stage,
    ):
        yield SimpleNamespace(manager=manager, folder=folder, stage=stage, staged=staged)


@pytest.mark.asyncio
async def test_a_move_from_the_backup_never_reaches_the_machine_it_leaves(from_backup):
    """That machine is unreachable, which is why its owner moves from the
    backup: no wake, no backup of it, and its folder stays where it is, so a
    rollback has nothing to put back."""
    manager = from_backup.manager
    await manager._run_move(_staged(phase="preparing", from_backup=True))

    manager._wake_move_source.assert_not_awaited()
    manager.backup_project_files.assert_not_awaited()
    manager._back_up_and_set_aside.assert_not_awaited()
    from_backup.stage.assert_awaited_once_with(
        MOVE_ID, "runner-1", source_folder="Research", leftovers_path=None
    )
    manager._commit_move.assert_awaited_once_with(from_backup.staged)
    manager._finish_committed_move.assert_awaited_once_with(
        {"move_id": MOVE_ID, "phase": "committed"}
    )
    manager._abort_move.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("folder", [None, f"_internal/moving/{MOVE_ID}"], ids=["none", "staged"])
async def test_a_move_from_the_backup_waits_for_a_folder_mid_move(from_backup, folder):
    """The staged row records the folder the workspace left by name, which a
    folder not at its name cannot give."""
    from_backup.folder.return_value = folder

    with pytest.raises(ComputerBusyError, match="mid-move"):
        await from_backup.manager._stage_from_backup(_staged(phase="preparing", from_backup=True))

    from_backup.stage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("left", [False, True], ids=["moved", "left-behind"])
async def test_a_machine_left_behind_is_not_dialled_to_drop_its_grants(left):
    """It went unreachable, so the dial would only wait out its timeout; the
    database already denies the grants it held."""
    manager = _manager()
    manager._refresh_source_grants = AsyncMock()
    target = {**_computer(), "computer_id": "target-1"}
    with (
        patch(f"{_MOVE}.get_computer", AsyncMock(side_effect=[target, _computer()])),
        patch(f"{_MOVE}.publish_workspace_binding_change", AsyncMock()),
        patch("src.server.services.preview_capabilities.revoke", AsyncMock()),
        patch(f"{_MOVE}.finish_workspace_move", AsyncMock()) as finish,
    ):
        await manager._finish_committed_move(
            _staged(phase="committed", target_computer_id="target-1", from_backup=left)
        )

    assert manager._refresh_source_grants.await_count == (0 if left else 1)
    finish.assert_awaited_once_with(MOVE_ID, "runner-1", "done")
