"""The sweep acting on a folder a workspace move left on its old computer.

The listing decides what each row needs (pinned on Postgres in
``tests/integration/test_workspace_move_db.py``); these pin that the sweep
does only that. A row whose computer is gone, or whose files just came back,
is a mark and never a sandbox call. A removal runs only on a computer already
running, under its folder lock, on a path only a move sets aside, and is
marked only once the folder is gone. A folder on its owner's machine is never
removed: the sweep marks it done and leaves it there. A folder a move from the
backup left is set aside in a loop of its own, a claimed row at a time, so a
machine that never answers holds up neither idle reaping nor the next row.
"""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.workspace_move import LEFT_FOLDER_RETRY_S
from src.server.services.computer_manager import ComputerManager
from src.server.services.computer_manager._folders import _SCRIPT_LIMIT_S, _SCRIPT_START_S
from src.server.services.computer_manager._move import (
    _LEFT_FOLDER_TRY_S,
    _LEFTOVERS_RM_TIMEOUT_S,
)
from src.server.services.workspace_manager import WorkspaceManager

_MOVE = "src.server.services.computer_manager._move"
MOVE_ID = "66666666-6666-4666-8666-666666666666"
SOURCE = "77777777-7777-4777-8777-777777777777"
ROOT = "/data/root"
AWAY = f"_internal/leftovers/move-{MOVE_ID}"


def _row(action, **overrides):
    row = {
        "move_id": MOVE_ID,
        "source_computer_id": SOURCE,
        "leftovers_path": AWAY,
        "finished_at": None,
        "source_kind": "docker",
        "source_root_dir": ROOT,
        "action": action,
    }
    row.update(overrides)
    return row


def _computer(**overrides):
    computer = {
        "computer_id": SOURCE,
        "user_id": "user-1",
        "kind": "docker",
        "status": "running",
        "provider_ref": "sb-old",
        "root_dir": ROOT,
    }
    computer.update(overrides)
    return computer


@pytest.fixture
def sweep():
    """A manager whose sandbox, folder lock and marks record what they were asked, in order."""
    events = []
    state = SimpleNamespace(lock_conn=object(), exit_code=0)
    runtime = MagicMock()

    async def run(command, timeout=None):
        events.append(("exec", command, timeout))
        return SimpleNamespace(exit_code=state.exit_code, stderr="rm: refused")

    runtime.exec = AsyncMock(side_effect=run)

    @asynccontextmanager
    async def detached(provider_ref, *, binding=None):
        events.append(("runtime", provider_ref))
        yield runtime

    @asynccontextmanager
    async def folders_lock(computer_id, *, wait_s=None):
        events.append(("lock", computer_id, wait_s))
        yield state.lock_conn
        events.append(("unlock", computer_id))

    async def removed(move_id):
        events.append(("removed", move_id))

    manager = ComputerManager(SimpleNamespace(sandbox=SimpleNamespace(provider="docker")))
    manager._detached_runtime = detached
    with (
        patch(f"{_MOVE}.get_computer", AsyncMock(return_value=_computer())) as computer,
        patch(f"{_MOVE}.workspace_folders_lock", folders_lock),
        patch(f"{_MOVE}.mark_leftovers_removed", AsyncMock(side_effect=removed)) as gone,
        patch(f"{_MOVE}.mark_move_restored", AsyncMock()) as restored,
    ):
        yield SimpleNamespace(
            manager=manager,
            runtime=runtime,
            events=events,
            state=state,
            get_computer=computer,
            mark_removed=gone,
            mark_restored=restored,
        )


@pytest.mark.asyncio
async def test_a_folder_gone_with_its_computer_is_marked_without_a_sandbox(sweep):
    await sweep.manager._reap_move_leftovers(_row("source_gone"))

    sweep.mark_removed.assert_awaited_once_with(MOVE_ID)
    sweep.mark_restored.assert_not_awaited()
    sweep.get_computer.assert_not_awaited()
    sweep.runtime.exec.assert_not_awaited()
    assert sweep.events == [("removed", MOVE_ID)]


@pytest.mark.asyncio
async def test_files_back_in_place_start_the_wait_without_a_sandbox(sweep):
    await sweep.manager._reap_move_leftovers(_row("restored"))

    sweep.mark_restored.assert_awaited_once_with(MOVE_ID)
    sweep.mark_removed.assert_not_awaited()
    sweep.get_computer.assert_not_awaited()
    sweep.runtime.exec.assert_not_awaited()
    assert sweep.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "Research",
        "_internal/leftovers/ws-a",
        "_internal/leftovers",
        f"{ROOT}/_internal/leftovers/move-{MOVE_ID}",
    ],
    ids=["live-folder", "delete-leftovers", "leftovers-root", "absolute"],
)
async def test_a_removal_outside_a_moves_leftovers_is_left_alone(sweep, path):
    await sweep.manager._reap_move_leftovers(_row("remove", leftovers_path=path))

    sweep.get_computer.assert_not_awaited()
    sweep.runtime.exec.assert_not_awaited()
    sweep.mark_removed.assert_not_awaited()
    sweep.mark_restored.assert_not_awaited()
    assert sweep.events == []


@pytest.mark.asyncio
async def test_a_removal_runs_under_the_folder_lock_then_marks_the_row(sweep):
    await sweep.manager._reap_move_leftovers(_row("remove"))

    sweep.get_computer.assert_awaited_once_with(SOURCE)
    rm = ("exec", f"rm -rf {ROOT}/{AWAY}", _LEFTOVERS_RM_TIMEOUT_S)
    assert ("runtime", "sb-old") in sweep.events
    # Never waited for: a holder elsewhere is likely removing the same folder.
    assert [e for e in sweep.events if e[0] != "runtime"] == [
        ("lock", SOURCE, 0),
        rm,
        ("unlock", SOURCE),
        ("removed", MOVE_ID),
    ]
    sweep.mark_restored.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "computer",
    [
        _computer(status="stopped"),
        _computer(status="starting"),
        _computer(provider_ref=None),
        None,
    ],
    ids=["stopped", "starting", "no-sandbox", "row-gone"],
)
async def test_a_removal_waits_for_a_computer_already_running(sweep, computer):
    """No computer is started to clear a folder; the row stays listed for later."""
    sweep.get_computer.return_value = computer

    await sweep.manager._reap_move_leftovers(_row("remove"))

    sweep.runtime.exec.assert_not_awaited()
    sweep.mark_removed.assert_not_awaited()
    assert sweep.events == []


@pytest.mark.asyncio
async def test_a_busy_folder_lock_defers_the_removal(sweep):
    sweep.state.lock_conn = None

    await sweep.manager._reap_move_leftovers(_row("remove"))

    sweep.runtime.exec.assert_not_awaited()
    sweep.mark_removed.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_removal_is_not_marked(sweep):
    sweep.state.exit_code = 1

    with pytest.raises(RuntimeError):
        await sweep.manager._reap_move_leftovers(_row("remove"))

    sweep.runtime.exec.assert_awaited_once()
    sweep.mark_removed.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["restored", "remove"])
async def test_a_folder_on_its_owners_machine_is_kept_and_marked_done(sweep, action):
    """What the mirror left out exists nowhere else, so nothing reaches the machine."""
    await sweep.manager._reap_move_leftovers(_row(action, source_kind="ssh"))

    sweep.mark_removed.assert_awaited_once_with(MOVE_ID)
    sweep.mark_restored.assert_not_awaited()
    sweep.get_computer.assert_not_awaited()
    sweep.runtime.exec.assert_not_awaited()
    assert sweep.events == [("removed", MOVE_ID)]


# --- a folder a move from the backup left under its name -------------------


def _left(**overrides):
    row = {
        "move_id": MOVE_ID,
        "source_computer_id": SOURCE,
        "source_folder": "Research",
        "source_gone": False,
    }
    row.update(overrides)
    return row


@pytest.fixture
def left(sweep):
    """The sweep on an owner's machine, with the name's release and the
    set-aside recorded beside the lock and the sandbox."""
    sweep.get_computer.return_value = _computer(kind="ssh")
    state = SimpleNamespace(taken=False, moved=AWAY)

    async def release(move_id, moved):
        sweep.events.append(("release", move_id, moved))

    async def clear(runtime, root, tombstone, folder, stuck):
        sweep.events.append(("clear", root, tombstone, folder, stuck))
        return state.moved

    async def taken(conn, computer_id, folder):
        assert conn is sweep.state.lock_conn
        return state.taken

    with (
        patch(f"{_MOVE}.set_left_folder_aside", AsyncMock(side_effect=release)) as released,
        patch(f"{_MOVE}.left_folder_taken", taken),
        patch(f"{_MOVE}._clear_move_folder", AsyncMock(side_effect=clear)) as cleared,
    ):
        yield SimpleNamespace(sweep=sweep, state=state, released=released, cleared=cleared)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        _left(source_gone=True),
        _left(source_folder=None),
        _left(source_folder=f"_internal/leftovers/move-{MOVE_ID}"),
    ],
    ids=["computer-gone", "no-folder", "not-at-a-name"],
)
async def test_a_name_with_no_folder_to_set_aside_is_released_without_a_sandbox(left, row):
    """Nothing can sit under it, so holding it would only keep the name from
    a workspace made or moved there later."""
    await left.sweep.manager._set_left_folder_aside(row)

    assert left.sweep.events == [("release", MOVE_ID, None)]
    left.sweep.get_computer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("why", ["stopped", "no-sandbox", "lock-busy"])
async def test_the_name_stays_held_until_the_machine_runs_and_its_lock_is_free(left, why):
    """None is started for this, and a settle holding the folders is not
    waited on; a release before the set-aside would let a workspace bound to
    the name trust the stale copy's sync marker."""
    if why == "stopped":
        left.sweep.get_computer.return_value = _computer(kind="ssh", status="stopped")
    elif why == "no-sandbox":
        left.sweep.get_computer.return_value = _computer(kind="ssh", provider_ref=None)
    else:
        left.sweep.state.lock_conn = None

    await left.sweep.manager._set_left_folder_aside(_left())

    left.released.assert_not_awaited()
    left.cleared.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("moved", [AWAY, None], ids=["set-aside", "nothing-there"])
async def test_a_left_folder_is_copied_aside_under_the_lock_then_its_name_released(left, moved):
    """Kept, as every move's leftovers are on the owner's machine: the backup
    lacks the folder's .git and dependencies."""
    left.state.moved = moved

    await left.sweep.manager._set_left_folder_aside(_left())

    assert left.sweep.events == [
        ("runtime", "sb-old"),
        ("lock", SOURCE, 0),
        ("clear", ROOT, f"move-{MOVE_ID}", "Research", "copy"),
        ("unlock", SOURCE),
        ("release", MOVE_ID, moved),
    ]


@pytest.mark.asyncio
async def test_a_name_another_workspace_took_is_released_with_nothing_moved(left):
    """The folder under it now is that workspace's, not the one the move left."""
    left.state.taken = True

    await left.sweep.manager._set_left_folder_aside(_left())

    left.cleared.assert_not_awaited()
    left.released.assert_awaited_once_with(MOVE_ID, None)


@pytest.mark.asyncio
async def test_a_folder_with_no_room_for_its_copy_keeps_its_name_held(left):
    """The copy is all that would keep it once moved; the next sweep tries again."""
    from src.server.services.computer_manager._move import _NO_ROOM

    left.state.moved = _NO_ROOM

    await left.sweep.manager._set_left_folder_aside(_left())

    left.cleared.assert_awaited_once()
    left.released.assert_not_awaited()


# --- the left-folder pass, apart from the cleanup loop -----------------------


def _manager(cls=ComputerManager, **kwargs):
    return cls(SimpleNamespace(sandbox=SimpleNamespace(provider="docker")), **kwargs)


@pytest.mark.asyncio
async def test_the_pass_tries_each_claimed_row_until_none_is_due():
    """A row that fails is logged and left for its next window; the next
    claimed row is tried all the same."""
    manager = _manager()
    tried = []

    async def try_row(row):
        tried.append(row["move_id"])
        if row["move_id"] == "dark":
            raise RuntimeError("the machine did not answer")

    manager._set_left_folder_aside = try_row
    claim = AsyncMock(side_effect=[[_left(move_id="dark")], [_left(move_id="next")], []])

    with patch(f"{_MOVE}.claim_folders_left_in_place", claim):
        await manager.set_aside_left_folders()

    assert tried == ["dark", "next"]
    assert claim.await_count == 3


@pytest.mark.asyncio
async def test_a_try_past_its_bound_is_cut_short_for_the_next_row():
    """A machine that answers slowly on purpose keeps one row's try, not the pass."""
    manager = _manager()
    tried, cut = [], []

    async def try_row(row):
        tried.append(row["move_id"])
        if row["move_id"] == "slow":
            try:
                await asyncio.Event().wait()
            finally:
                cut.append(row["move_id"])

    manager._set_left_folder_aside = try_row
    claim = AsyncMock(side_effect=[[_left(move_id="slow")], [_left(move_id="next")], []])

    with (
        patch(f"{_MOVE}.claim_folders_left_in_place", claim),
        patch(f"{_MOVE}._LEFT_FOLDER_TRY_S", 0.01),
    ):
        await manager.set_aside_left_folders()

    assert tried == ["slow", "next"]
    assert cut == ["slow"]


def test_a_try_ends_before_its_row_is_due_again():
    """A try cut short still outlasts the pass it cancelled; both end inside
    the retry, so no other worker claims the row while this one is at it."""
    assert _LEFT_FOLDER_TRY_S + _SCRIPT_LIMIT_S + _SCRIPT_START_S < LEFT_FOLDER_RETRY_S


@pytest.mark.asyncio
async def test_the_pass_claims_nothing_once_shutting_down():
    manager = _manager()
    manager._shutdown = True
    claim = AsyncMock(return_value=[_left()])

    with patch(f"{_MOVE}.claim_folders_left_in_place", claim):
        await manager.set_aside_left_folders()

    claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_reaping_moves_never_dials_a_left_folder():
    """The serial cleanup loop runs it ahead of idle reaping."""
    manager = _manager()
    claim = AsyncMock(return_value=[_left()])

    with (
        patch(f"{_MOVE}.fail_stale_early_moves", AsyncMock(return_value=0)),
        patch(f"{_MOVE}.take_over_stale_moves", AsyncMock(return_value=[])),
        patch(f"{_MOVE}.list_moves_with_leftovers", AsyncMock(return_value=[])),
        patch(f"{_MOVE}.claim_folders_left_in_place", claim),
    ):
        await manager.reap_workspace_moves()

    claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_pass_that_never_returns_holds_up_no_idle_reaping():
    """Each loop runs on its own, and shutdown cancels both."""
    manager = _manager(WorkspaceManager, cleanup_interval=0.01)
    swept, reaped, cancelled = asyncio.Event(), asyncio.Event(), []

    async def hang():
        swept.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    manager.set_aside_left_folders = AsyncMock(side_effect=hang)
    manager.reap_pending_workspace_folder_cleanups = AsyncMock()
    manager.reap_workspace_moves = AsyncMock()
    manager.cleanup_idle_workspaces = AsyncMock(side_effect=lambda: reaped.set())
    manager.reap_stuck_starting_workspaces = AsyncMock()
    manager.reap_retirement_pending_computers = AsyncMock()

    await manager.start_cleanup_task()
    try:
        await asyncio.wait_for(swept.wait(), 2)
        reaped.clear()
        # Another round of idle reaping, while the pass is still stuck.
        await asyncio.wait_for(reaped.wait(), 2)
    finally:
        await manager.shutdown()

    assert manager.set_aside_left_folders.await_count == 1
    assert cancelled == [True]
    assert manager._cleanup_task is None
    assert manager._left_folders_task is None
