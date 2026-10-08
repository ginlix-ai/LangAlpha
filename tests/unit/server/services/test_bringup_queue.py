"""A computer's bring-up works through its workspaces one at a time.

The order is the contract: a workspace someone opened goes first, both steps;
every other workspace's evicted results come back before any transcript sync,
since a result is the user's data and a transcript can wait; a workspace whose
sync lock is held goes to the back, after a pause, instead of holding up the
rest. Each restore reads the workspace's folder when it runs, so one that
moved while queued is restored where it landed, and one still moving is left
to its next acquisition, which the dropped attach key lets queue it again.
"""

from __future__ import annotations

import asyncio
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from src.server.database.conversation import ThreadPrefixes
from src.server.database.workspace_file import WorkspaceSyncBusy
from src.server.database.workspace_folders import WorkspaceFolderMoving
from src.server.services.computer_manager import _bringup
from src.server.services.computer_manager._bringup import BringUpMixin
from src.server.services.computer_manager._sessions import SessionCacheMixin

pytestmark = pytest.mark.asyncio

ROOT = "/home/workspace"


def _layout(name: str):
    return SandboxLayout.for_root(ROOT).for_workspace(name)


class _Manager(SessionCacheMixin, BringUpMixin):
    def __init__(self) -> None:
        self._machines = {}
        self._projects_attached: set[tuple[str, str | None]] = set()


@pytest.fixture
def manager():
    return _Manager()


@pytest.fixture
def steps():
    """Record each step; ``busy[ws]`` counts lock waits still to time out,
    ``folders[ws]`` is where the workspace's folder is when a restore reads it
    (its own name unless set, None while a settle is moving it), a
    workspace in ``short`` restores with an error left over, one in
    ``unpruned`` keeps a dead thread dir its prune could not remove, and
    ``unsynced[ws]`` says how its sync left a thread behind."""
    order: list[str] = []
    busy: dict[str, int] = {}
    short: set[str] = set()
    unpruned: set[str] = set()
    folders: dict[str, str | None] = {}
    unsynced: dict[str, str] = {}

    @asynccontextmanager
    async def held(workspace_id, root):
        folder = folders.get(workspace_id, workspace_id)
        if folder is None:
            raise WorkspaceFolderMoving(workspace_id)
        yield _layout(folder)

    listing, inventory = object(), object()
    live: dict[str, ThreadPrefixes] = {}
    #: What a delete or archive during a workspace's pass leaves its threads at.
    moved: dict[str, ThreadPrefixes] = {}

    def kept_of(workspace_id):
        default = frozenset({f"live-{workspace_id}"})
        return live.get(workspace_id, ThreadPrefixes(all=default, open=default))

    async def inventory_of(runtime, layout):
        order.append(f"inventory:{layout.dir_name}")
        return listing, inventory

    async def prune(runtime, layout, workspace_id, *, listing=None):
        if listing is None:
            order.append(f"reprune:{workspace_id}")
        return kept_of(workspace_id), workspace_id not in unpruned

    async def live_threads(workspace_id):
        return kept_of(workspace_id)

    async def restore(
        workspace_id, sandbox, *, layout, kept: ThreadPrefixes, lock_wait, inventory
    ):
        assert kept == kept_of(workspace_id)
        await asyncio.sleep(0)
        if busy.get(workspace_id):
            busy[workspace_id] -= 1
            order.append(f"busy:{workspace_id}")
            raise WorkspaceSyncBusy("held")
        order.append(f"restore:{workspace_id}")
        if layout.dir_name != workspace_id:
            order[-1] += f"@{layout.dir_name}"
        if workspace_id in moved:
            live[workspace_id] = moved.pop(workspace_id)
        if workspace_id in short:
            return {"restored": 0, "errors": 1, "skipped": 0, "done": False}
        return {"restored": 0, "errors": 0, "skipped": 0, "done": True}

    async def sync(workspace_id):
        await asyncio.sleep(0)
        order.append(f"sync:{workspace_id}")
        ending = unsynced.get(workspace_id)
        if ending == "raised":
            raise RuntimeError("checkpoint read failed")
        return {"stored": 1, "failed": 1} if ending else {}

    with (
        patch.object(_bringup, "held_workspace_layout", held),
        patch.object(_bringup, "_inventory", inventory_of),
        patch.object(_bringup, "_thread_prefixes", live_threads),
        patch.object(_bringup.cache, "client", lambda: None),
        patch.object(_bringup, "_BUSY_PAUSE_S", 0.0),
        patch.object(_bringup._thread_dirs, "prune_dead_thread_dirs", prune),
        patch.object(_bringup.transcripts, "sync_workspace", sync),
        patch.object(_bringup.FilePersistenceService, "restore_deferred", restore),
    ):
        yield SimpleNamespace(
            order=order,
            busy=busy,
            folders=folders,
            live=live,
            moved=moved,
            short=short,
            unpruned=unpruned,
            unsynced=unsynced,
        )


def _steps(order: list[str]) -> list[str]:
    """The restores and syncs, without the probe each restore takes first."""
    return [step for step in order if not step.startswith("inventory:")]


class _Redis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ex=None):
        self.values[key] = value

    async def delete(self, key):
        self.values.pop(key, None)


@pytest.fixture
def sandbox():
    return SimpleNamespace(runtime=object(), sandbox_id="sandbox-1", working_dir=ROOT)


def _schedule(manager, sandbox, workspace_id: str, *, urgent: bool) -> None:
    manager._queue_bring_up("computer-1", workspace_id, sandbox, ROOT, urgent=urgent)


async def _drain(manager) -> None:
    job = manager._machine("computer-1").bring_up
    if job is not None:
        await job.task


async def test_every_restore_runs_before_any_sync(steps, manager, sandbox):
    for workspace_id in "ABC":
        _schedule(manager, sandbox, workspace_id, urgent=False)
    await _drain(manager)
    assert _steps(steps.order) == [
        "restore:A",
        "restore:B",
        "restore:C",
        "sync:A",
        "sync:B",
        "sync:C",
    ]


async def test_an_opened_workspace_jumps_the_queue_for_both_steps(
    steps, manager, sandbox
):
    for workspace_id in "ABCD":
        _schedule(manager, sandbox, workspace_id, urgent=False)
    await asyncio.sleep(0)  # A is under way
    _schedule(manager, sandbox, "C", urgent=True)
    await _drain(manager)
    assert _steps(steps.order) == [
        "restore:A",
        "restore:C",
        "sync:C",
        "restore:B",
        "restore:D",
        "sync:A",
        "sync:B",
        "sync:D",
    ]


async def test_a_busy_workspace_goes_to_the_back_and_loses_its_place(
    steps, manager, sandbox
):
    steps.busy["A"] = 1
    _schedule(manager, sandbox, "A", urgent=True)
    _schedule(manager, sandbox, "B", urgent=False)
    await _drain(manager)
    assert _steps(steps.order) == ["busy:A", "restore:B", "sync:B", "restore:A", "sync:A"]


async def test_a_busy_restore_does_not_hold_up_the_others_transcripts(
    steps, manager, sandbox
):
    """Another worker can hold a workspace's lock for minutes; the retries
    wait behind every sync already queued rather than in front of it."""
    steps.busy["B"] = 2
    for workspace_id in "ABC":
        _schedule(manager, sandbox, workspace_id, urgent=False)
    await _drain(manager)
    assert _steps(steps.order) == [
        "restore:A",
        "busy:B",
        "restore:C",
        "sync:A",
        "sync:C",
        "busy:B",
        "restore:B",
        "sync:B",
    ]


async def test_reopening_a_workspace_waiting_on_its_lock_retries_it_next(
    steps, manager, sandbox
):
    steps.busy["B"] = 1
    for workspace_id in "ABC":
        _schedule(manager, sandbox, workspace_id, urgent=False)
    while "busy:B" not in steps.order:
        await asyncio.sleep(0)
    _schedule(manager, sandbox, "B", urgent=True)
    await _drain(manager)
    assert _steps(steps.order) == [
        "restore:A",
        "busy:B",
        "restore:C",
        "restore:B",
        "sync:B",
        "sync:A",
        "sync:C",
    ]


async def test_a_lock_that_stays_busy_is_given_up_but_still_synced(
    steps, manager, sandbox
):
    """The next bring-up resumes the restore; the transcripts need not wait."""
    steps.busy["A"] = _bringup._LOCK_TRIES
    _schedule(manager, sandbox, "A", urgent=False)
    await _drain(manager)
    assert _steps(steps.order) == ["busy:A"] * _bringup._LOCK_TRIES + ["sync:A"]


async def test_a_workspace_queued_twice_runs_once(steps, manager, sandbox):
    _schedule(manager, sandbox, "A", urgent=False)
    _schedule(manager, sandbox, "B", urgent=False)
    await asyncio.sleep(0)  # A is under way
    _schedule(manager, sandbox, "A", urgent=False)
    _schedule(manager, sandbox, "B", urgent=False)
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "restore:B", "sync:A", "sync:B"]


async def test_a_finished_job_is_dropped_so_the_next_start_gets_a_new_one(
    steps, manager, sandbox
):
    machine = manager._machine("computer-1")
    _schedule(manager, sandbox, "A", urgent=False)
    first = machine.bring_up
    await first.task
    assert machine.bring_up is None

    _schedule(manager, sandbox, "A", urgent=True)
    assert machine.bring_up is not first
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "sync:A", "restore:A", "sync:A"]


async def test_a_dropped_session_takes_its_queued_work_with_it(
    steps, manager, sandbox
):
    """What was still queued loses its attach key, so the next acquisition on
    that sandbox queues it again."""
    machine = manager._machine("computer-1")
    manager._projects_attached |= {("A", "sandbox-1"), ("B", "sandbox-1")}
    for workspace_id in "AB":
        _schedule(manager, sandbox, workspace_id, urgent=False)
    job = machine.bring_up

    machine.forget_session()

    with pytest.raises(asyncio.CancelledError):
        await job.task
    assert _steps(steps.order) == []
    assert machine.bring_up is None
    assert not manager._projects_attached


async def test_an_eviction_spares_a_replacements_job(steps, manager, sandbox):
    machine = manager._machine("computer-1")
    _schedule(manager, sandbox, "A", urgent=False)
    job = machine.bring_up

    machine.cancel_bring_up("sandbox-0")

    assert machine.bring_up is job
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "sync:A"]


async def test_a_replaced_sandbox_cancels_the_old_job(steps, manager, sandbox):
    """The old sandbox's work stayed behind with it."""
    replacement = SimpleNamespace(runtime=object(), sandbox_id="sandbox-2")
    _schedule(manager, sandbox, "A", urgent=False)
    old = manager._machine("computer-1").bring_up
    _schedule(manager, replacement, "A", urgent=False)
    with pytest.raises(asyncio.CancelledError):
        await old.task
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "sync:A"]


async def test_a_resumed_sandbox_queues_every_workspace_for_its_sync(
    steps, manager, sandbox
):
    """A stop leaves each workspace attached to the same sandbox, so no later
    open looks again; the resume queues them itself, and a workspace whose
    folder is mid-move does not keep the others from theirs."""
    from src.server.services.computer_manager import _machines
    from src.server.services.computer_manager._machines import MachineLifecycleMixin

    steps.folders["B"] = None
    with patch.object(
        _machines,
        "get_live_workspace_ids_for_computer",
        AsyncMock(return_value=["A", "B", "C"]),
    ):
        await MachineLifecycleMixin._queue_reconnect_syncs(
            manager, "computer-1", sandbox
        )
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "restore:C", "sync:A", "sync:C"]


async def test_each_restore_reads_the_folder_where_it_is_when_it_runs(
    steps, manager, sandbox
):
    """A settle can rename a queued workspace's folder, and a sibling can take
    the old name; the restore goes where the workspace landed."""
    _schedule(manager, sandbox, "A", urgent=False)
    _schedule(manager, sandbox, "B", urgent=False)
    steps.folders["B"] = "B renamed"
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "restore:B@B renamed", "sync:A", "sync:B"]


async def test_a_workspace_mid_move_is_left_to_its_next_acquisition(
    steps, manager, sandbox
):
    manager._projects_attached |= {("A", "sandbox-1"), ("B", "sandbox-1")}
    steps.folders["A"] = None
    _schedule(manager, sandbox, "A", urgent=True)
    _schedule(manager, sandbox, "B", urgent=False)
    await _drain(manager)
    assert _steps(steps.order) == ["restore:B", "sync:B"]
    assert manager._projects_attached == {("B", "sandbox-1")}


async def test_a_busy_restore_pauses_without_holding_up_the_others(
    steps, manager, sandbox
):
    """The lock wait is short and the retry waits out a pause, so a backup
    holding one workspace's lock costs the others nothing; reopening the
    workspace cuts its pause short."""
    steps.busy["A"] = 1
    with patch.object(_bringup, "_BUSY_PAUSE_S", 60.0):
        _schedule(manager, sandbox, "A", urgent=False)
        _schedule(manager, sandbox, "B", urgent=False)
        while "sync:B" not in steps.order:
            await asyncio.sleep(0)
        await asyncio.sleep(0.01)
        assert _steps(steps.order) == ["busy:A", "restore:B", "sync:B"]

        _schedule(manager, sandbox, "A", urgent=True)
        await asyncio.wait_for(_drain(manager), 1)
    assert _steps(steps.order) == [
        "busy:A",
        "restore:B",
        "sync:B",
        "restore:A",
        "sync:A",
    ]


async def test_a_finished_pass_is_skipped_until_its_threads_or_sandbox_change(
    steps, manager, sandbox
):
    """Only a thread's delete or archive leaves a dir to prune, and the marker
    ends the restore, so a reconnect or a second worker finding the same
    sandbox and threads skips the probe and the restore; the sync still runs."""
    redis = _Redis()
    with patch.object(_bringup.cache, "client", lambda: redis):
        _schedule(manager, sandbox, "A", urgent=False)
        await _drain(manager)
        assert steps.order == ["inventory:A", "restore:A", "sync:A"]

        _schedule(manager, sandbox, "A", urgent=False)
        await _drain(manager)
        assert steps.order[3:] == ["sync:A"]

        # Archived: the thread stays, its scratchpad has to go.
        steps.live["A"] = ThreadPrefixes(all=frozenset({"live-A"}), open=frozenset())
        _schedule(manager, sandbox, "A", urgent=False)
        await _drain(manager)
        assert steps.order[4:] == ["inventory:A", "restore:A", "sync:A"]

        steps.live["A"] = ThreadPrefixes(all=frozenset(), open=frozenset())
        _schedule(manager, sandbox, "A", urgent=False)
        await _drain(manager)
        assert steps.order[7:] == ["inventory:A", "restore:A", "sync:A"]

        replacement = SimpleNamespace(runtime=object(), sandbox_id="sandbox-2")
        _schedule(manager, replacement, "A", urgent=False)
        await _drain(manager)
        assert steps.order[10:] == ["inventory:A", "restore:A", "sync:A"]


async def test_one_exec_lists_the_thread_dirs_and_probes_the_deferred_dir(tmp_path):
    """The split line has to come through the shell for either half to be
    read; a marker ends the probe before ``find`` runs."""
    layout = SandboxLayout.for_root(str(tmp_path)).for_workspace("My Workspace")
    Path(layout.join(WorkspaceLayout.THREADS_DIR, "abcd1234")).mkdir(parents=True)
    Path(layout.join(WorkspaceLayout.scratchpad_subdir("abcd1234", "note"))).mkdir(parents=True)
    marker = Path(layout.join(_bringup.restore.DEFERRED_MARKER))
    marker.parent.mkdir(parents=True)
    marker.write_text("done")

    class _Shell:
        async def exec(self, script):
            ran = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return SimpleNamespace(stdout=ran.stdout, exit_code=ran.returncode)

    listing, inventory = await _bringup._inventory(_Shell(), layout)

    assert listing == {
        WorkspaceLayout.THREADS_DIR: {"abcd1234"},
        WorkspaceLayout.LARGE_TOOL_RESULTS_DIR: set(),
        WorkspaceLayout.SCRATCHPAD_DIR: {"abcd1234"},
    }
    assert inventory.present is None


async def test_an_unfinished_pass_is_not_stamped(steps, manager, sandbox):
    redis = _Redis()
    steps.busy["A"] = _bringup._LOCK_TRIES
    with patch.object(_bringup.cache, "client", lambda: redis):
        _schedule(manager, sandbox, "A", urgent=False)
        await _drain(manager)
    assert redis.values == {}


async def test_a_pass_whose_prune_left_dead_dirs_is_not_stamped(steps, manager, sandbox):
    """A stamp skips the next bring-up's prune, so it would keep a deleted
    thread's dirs on the machine until another thread came or went."""
    redis = _Redis()
    steps.unpruned.add("A")
    with patch.object(_bringup.cache, "client", lambda: redis):
        _schedule(manager, sandbox, "A", urgent=False)
        await _drain(manager)
    assert "restore:A" in steps.order
    assert redis.values == {}


async def test_a_thread_archived_during_the_pass_is_pruned_again(steps, manager, sandbox):
    """Its own prune ran beside a batch that may have put its scratchpad back,
    and the stamp records the threads as they stand after."""
    redis = _Redis()
    after = ThreadPrefixes(all=frozenset({"live-A"}), open=frozenset())
    steps.moved["A"] = after
    with patch.object(_bringup.cache, "client", lambda: redis):
        _schedule(manager, sandbox, "A", urgent=False)
        await _drain(manager)
    assert _steps(steps.order)[:2] == ["restore:A", "reprune:A"]
    assert redis.values[_bringup._stamp_key("A")] == _bringup._stamp("sandbox-1", after)


@pytest.mark.parametrize("ending", ["errors", "busy"])
async def test_an_unfinished_restore_is_left_to_the_next_acquisition(
    steps, manager, sandbox, ending
):
    """On a running machine no start comes to resume it, so the attach key
    goes once the sync ran; a finished one keeps its key."""
    manager._projects_attached |= {("A", "sandbox-1"), ("B", "sandbox-1")}
    if ending == "errors":
        steps.short.add("A")
    else:
        steps.busy["A"] = _bringup._LOCK_TRIES
    _schedule(manager, sandbox, "A", urgent=False)
    _schedule(manager, sandbox, "B", urgent=False)
    await _drain(manager)
    assert {"sync:A", "sync:B"} <= set(steps.order)
    assert manager._projects_attached == {("B", "sandbox-1")}


async def test_an_acquisition_before_the_sync_does_not_keep_the_unfinished_key(
    steps, manager, sandbox
):
    """Its add folds into the queued step, so the key it records has to go
    with the step or the restore waits for the next machine start."""
    steps.short.add("A")
    _schedule(manager, sandbox, "A", urgent=False)
    while "restore:A" not in steps.order:
        await asyncio.sleep(0)
    manager._projects_attached.add(("A", "sandbox-1"))
    _schedule(manager, sandbox, "A", urgent=True)
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "sync:A"]
    assert not manager._projects_attached


@pytest.mark.parametrize("ending", ["failed", "raised"])
async def test_a_sync_that_left_a_thread_behind_is_left_to_the_next_acquisition(
    steps, manager, sandbox, ending
):
    """A thread nobody opens again has no turn end to export it, and on a
    running machine no start comes to sync it, so the attach key goes."""
    manager._projects_attached |= {("A", "sandbox-1"), ("B", "sandbox-1")}
    steps.unsynced["A"] = ending
    _schedule(manager, sandbox, "A", urgent=False)
    _schedule(manager, sandbox, "B", urgent=False)
    await _drain(manager)
    assert _steps(steps.order) == ["restore:A", "restore:B", "sync:A", "sync:B"]
    assert manager._projects_attached == {("B", "sandbox-1")}


async def test_a_thread_delete_drops_the_stamp_even_on_a_stopped_machine(manager):
    redis = _Redis()
    redis.values[_bringup._stamp_key("A")] = "sandbox-1|0"
    stopped = AsyncMock(return_value={"status": "stopped"})
    with (
        patch.object(_bringup.cache, "client", lambda: redis),
        patch.object(_bringup, "get_computer_for_workspace", stopped),
    ):
        await manager.prune_thread_dirs_if_running("A")
    assert redis.values == {}
