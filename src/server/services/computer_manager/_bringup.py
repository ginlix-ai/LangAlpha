"""Seam: a machine's background bring-up, and the dirs of deleted threads
and the scratchpads of archived ones that it prunes.

One file of the ComputerManager split; see the package __init__."""

import asyncio
import hashlib
import itertools
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from ptc_agent.core.paths import WorkspaceLayout
from src.server.database.computer import DEFAULT_ROOT_DIR, get_computer_for_workspace
from src.server.database.workspace_file import WorkspaceSyncBusy
from src.server.database.workspace_folders import WorkspaceFolderMoving
from src.server.services import transcripts
from src.server.services.computer_manager import _thread_dirs
from src.server.services.livefs import cache
from src.server.services.persistence import restore
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.workspace_layout import held_workspace_layout

if TYPE_CHECKING:
    from src.server.database.conversation import ThreadPrefixes

logger = logging.getLogger(__name__)

# A backup holds the workspace's sync lock for as long as it runs. The bring-up
# waits briefly for it, then works on the other workspaces and comes back after
# a pause, giving up on the restore after as many tries (backups keep the rows
# it guards, and the next bring-up resumes it). About as patient in all as a
# long wait, without one held lock stalling every workspace on the sandbox.
_LOCK_WAIT = "2s"
_BUSY_PAUSE_S = 15.0
_LOCK_TRIES = 12

# A pass that finished is stamped with its sandbox and the threads it judged.
# Only a thread's delete, or its archive once it is no longer in use (see
# ThreadPrefixes), leaves a dir to prune, and the deferred marker ends the
# restore for the sandbox's life, so a bring-up finding the same stamp has
# nothing to do there. Redis only saves the work: a lost stamp costs a pass.
_STAMP_TTL_S = 7 * 24 * 3600
# Splits the one exec's output: the thread dirs above, the deferred dirs below.
_INVENTORY_SPLIT = "#deferred"

# Where a workspace's next step stands in the queue, first to last.
_URGENT, _RESTORE, _SYNC, _RETRY = range(4)


@dataclass(order=True)
class _Step:
    tier: int
    seq: int
    restored: bool = field(default=False, compare=False)
    #: The restore ended short of done; once synced, the attach key goes.
    unfinished: bool = field(default=False, compare=False)
    busy: int = field(default=0, compare=False)
    #: Loop time before which a busy restore is not tried again.
    not_before: float = field(default=0.0, compare=False)


def _stamp_key(workspace_id: str) -> str:
    return f"bringup:{workspace_id}"


def _stamp(sandbox_id: Optional[str], kept: "ThreadPrefixes") -> Optional[str]:
    if not sandbox_id:
        return None
    listed = "\n".join(sorted(kept.all)) + "|" + "\n".join(sorted(kept.open))
    digest = hashlib.sha256(listed.encode()).hexdigest()
    return f"{sandbox_id}|{digest[:16]}"


async def _read_stamp(workspace_id: str) -> Optional[str]:
    client = cache.client()
    if client is None:
        return None
    try:
        value = await client.get(_stamp_key(workspace_id))
    except Exception:  # noqa: BLE001 - no stamp only costs a pass
        return None
    if value is None:
        return None
    return cache.text(value)


async def _write_stamp(workspace_id: str, stamp: Optional[str]) -> None:
    client = cache.client()
    if client is None or stamp is None:
        return
    try:
        await client.set(_stamp_key(workspace_id), stamp, ex=_STAMP_TTL_S)
    except Exception as e:  # noqa: BLE001
        logger.debug(f"Could not stamp the bring-up of workspace {workspace_id}: {e}")


async def _drop_stamp(workspace_id: str) -> None:
    client = cache.client()
    if client is None:
        return
    try:
        await client.delete(_stamp_key(workspace_id))
    except Exception as e:  # noqa: BLE001
        logger.debug(f"Could not drop the bring-up stamp of workspace {workspace_id}: {e}")


async def _thread_prefixes(workspace_id: str) -> "ThreadPrefixes":
    from src.server.database.conversation import get_workspace_thread_prefixes

    return await get_workspace_thread_prefixes(workspace_id)


async def _inventory(
    runtime: Any, layout: WorkspaceLayout
) -> tuple[Optional[_thread_dirs.ThreadDirListing], Optional[restore.DeferredInventory]]:
    """The thread dirs and the deferred dirs, in one exec; both are None when
    the output came back without the split, and each step probes for itself."""
    listed = await runtime.exec(
        f"{_thread_dirs.listing_script(layout)}\n"
        f"echo '{_INVENTORY_SPLIT}'\n"
        f"{restore.deferred_probe_script(layout)}"
    )
    lines = (listed.stdout or "").splitlines()
    if listed.exit_code != 0 or _INVENTORY_SPLIT not in lines:
        return None, None
    split = lines.index(_INVENTORY_SPLIT)
    return (
        _thread_dirs.parse_listing("\n".join(lines[:split])),
        restore.parse_deferred("\n".join(lines[split + 1 :])),
    )


class BringUp:
    """One sandbox's bring-up: each workspace's dead thread dirs and evicted
    results, then its transcripts, one workspace at a time.

    A workspace someone opened goes first, both steps; the rest restore before
    any syncs, since an evicted result is the user's data and a transcript can
    wait. A restore whose sync lock is held retries behind everything queued,
    after a pause. Work the job drops (a folder moving, a cancel) or leaves
    unfinished discards the workspace's attach key, so its next acquisition
    queues it again rather than the next machine start. Execution context
    only: each worker runs its own.
    """

    def __init__(
        self,
        sandbox: Any,
        sandbox_id: Optional[str],
        root: str,
        forget: Callable[[str], None],
    ) -> None:
        self.sandbox = sandbox
        self.sandbox_id = sandbox_id
        self.root = root
        self.pending: dict[str, _Step] = {}
        self.task: Optional[asyncio.Task] = None
        self._forget = forget
        self._seq = itertools.count()
        self._wake = asyncio.Event()

    def add(self, workspace_id: str, *, urgent: bool) -> None:
        step = self.pending.get(workspace_id)
        if step is None:
            self.pending[workspace_id] = _Step(
                _URGENT if urgent else _RESTORE, next(self._seq)
            )
        elif urgent and step.tier != _URGENT:
            self._move(step, _URGENT)
            step.not_before = 0.0
        self._wake.set()

    def cancel(self) -> None:
        for workspace_id in self.pending:
            self._forget(workspace_id)
        self.pending.clear()
        if self.task is not None:
            self.task.cancel()

    def _move(self, step: _Step, tier: int) -> None:
        step.tier, step.seq = tier, next(self._seq)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        while self.pending:
            now = loop.time()
            ready = [item for item in self.pending.items() if item[1].not_before <= now]
            if not ready:
                # Only busy restores are left, each pausing; an add wakes it.
                self._wake.clear()
                pause = min(step.not_before for step in self.pending.values()) - now
                try:
                    await asyncio.wait_for(self._wake.wait(), pause)
                except TimeoutError:
                    pass
                continue
            workspace_id, step = min(ready, key=lambda item: item[1])
            if step.restored:
                synced = await self._sync(workspace_id)
                self.pending.pop(workspace_id, None)
                if step.unfinished or not synced:
                    # Not before: an acquisition meanwhile would put the key
                    # back and its add would fold into this step.
                    self._forget(workspace_id)
            else:
                await self._restore(workspace_id, step)

    async def _unchanged(self, workspace_id: str) -> bool:
        stamp = await _read_stamp(workspace_id)
        if stamp is None or stamp.split("|", 1)[0] != self.sandbox_id:
            return False
        try:
            live = await _thread_prefixes(workspace_id)
        except Exception:  # noqa: BLE001 - the pass reads them again and reports
            return False
        return stamp == _stamp(self.sandbox_id, live)

    async def _restore(self, workspace_id: str, step: _Step) -> None:
        done = False
        try:
            if await self._unchanged(workspace_id):
                step.restored = True
                if step.tier != _URGENT:
                    self._move(step, _SYNC)
                return
            async with held_workspace_layout(workspace_id, self.root) as layout:
                if layout is None:
                    raise WorkspaceFolderMoving(workspace_id)
                listing, inventory = await _inventory(self.sandbox.runtime, layout)
                kept, pruned = await _thread_dirs.prune_dead_thread_dirs(
                    self.sandbox.runtime, layout, workspace_id, listing=listing
                )
                outcome = await FilePersistenceService.restore_deferred(
                    workspace_id,
                    self.sandbox,
                    layout=layout,
                    kept=kept,
                    lock_wait=_LOCK_WAIT,
                    inventory=inventory,
                )
                if await _thread_prefixes(workspace_id) != kept:
                    # A delete or archive during the pass pruned beside a
                    # batch that may have put its dirs back.
                    kept, pruned = await _thread_dirs.prune_dead_thread_dirs(
                        self.sandbox.runtime, layout, workspace_id
                    )
            if outcome["restored"] or outcome["errors"]:
                logger.info(f"Deferred restore for workspace {workspace_id}: {outcome}")
            done = bool(outcome.get("done"))
            if done and pruned:
                await _write_stamp(workspace_id, _stamp(self.sandbox_id, kept))
        except WorkspaceFolderMoving:
            logger.info(
                f"Deferred restore for workspace {workspace_id} found no settled "
                f"folder; its next acquisition queues it again"
            )
            self.pending.pop(workspace_id, None)
            self._forget(workspace_id)
            return
        except WorkspaceSyncBusy:
            step.busy += 1
            if step.busy < _LOCK_TRIES:
                logger.info(
                    f"Deferred restore for workspace {workspace_id} found its "
                    f"sync lock held; retrying after the others and a pause ({step.busy}/"
                    f"{_LOCK_TRIES})"
                )
                self._move(step, _RETRY)
                step.not_before = asyncio.get_running_loop().time() + _BUSY_PAUSE_S
                return
            logger.warning(
                f"Deferred restore for workspace {workspace_id} gave up after "
                f"{step.busy} busy waits; its next acquisition resumes it"
            )
        except Exception as e:
            logger.warning(f"Deferred restore failed for workspace {workspace_id}: {e}")
        step.restored = True
        step.unfinished = not done
        if step.tier != _URGENT:
            self._move(step, _SYNC)

    async def _sync(self, workspace_id: str) -> bool:
        """Whether no thread was left behind. Only its own turn end exports a
        thread again, so one nobody reopens waits on the next sync."""
        try:
            outcome = await transcripts.sync_workspace(workspace_id)
        except Exception as e:
            logger.warning(f"Transcript sync failed for workspace {workspace_id}: {e}")
            return False
        if any(outcome.values()):
            logger.info(f"Transcript sync for workspace {workspace_id}: {outcome}")
        return not outcome.get("failed")


class BringUpMixin:
    def _queue_bring_up(
        self,
        computer_id: str,
        workspace_id: str,
        sandbox: Any,
        root: Optional[str],
        *,
        urgent: bool = True,
    ) -> None:
        """Queue what a usable folder can wait for on its machine's bring-up.

        One job per sandbox works through every workspace on it, so a machine
        start with many workspaces does not run their restores side by side on
        a small sandbox. A replaced sandbox's job is cancelled: its work stayed
        behind with the sandbox.
        """
        if sandbox.runtime is None:
            return
        sandbox_id = str(sandbox.sandbox_id) if sandbox.sandbox_id else None
        machine = self._machine(computer_id)
        job = machine.bring_up
        if job is None or job.sandbox_id != sandbox_id:
            if job is not None:
                job.cancel()
            job = machine.bring_up = BringUp(
                sandbox,
                sandbox_id,
                root or DEFAULT_ROOT_DIR,
                lambda ws: self._projects_attached.discard((ws, sandbox_id)),
            )

            async def _run(job: BringUp) -> None:
                try:
                    await job.run()
                finally:
                    if machine.bring_up is job:
                        machine.bring_up = None

            job.task = asyncio.create_task(_run(job))
        job.add(workspace_id, urgent=urgent)

    def prune_thread_dirs_soon(self, workspace_id: str) -> None:
        """``prune_thread_dirs_if_running`` without holding up the thread
        delete or archive that asks for it."""
        task = asyncio.create_task(self.prune_thread_dirs_if_running(workspace_id))
        self._prune_tasks.add(task)
        task.add_done_callback(self._prune_tasks.discard)

    def prune_if_archived_soon(self, thread_id: str) -> None:
        """At the end of a run: if its thread is archived, the scratchpad the
        run kept open leaves now, not at the machine's next bring-up."""
        task = asyncio.create_task(self._prune_if_archived(thread_id))
        self._prune_tasks.add(task)
        task.add_done_callback(self._prune_tasks.discard)

    async def _prune_if_archived(self, thread_id: str) -> None:
        from src.server.database.conversation import get_thread_by_id

        try:
            thread = await get_thread_by_id(thread_id)
        except Exception as e:
            logger.warning(f"Could not read thread {thread_id} for its archive prune: {e}")
            return
        if thread and thread.get("archived_at") is not None:
            await self.prune_thread_dirs_if_running(str(thread["workspace_id"]))

    async def prune_thread_dirs_if_running(self, workspace_id: str) -> None:
        """Deleted threads' dirs and archived threads' scratchpads leave the
        machine now, if it is up.

        Never wakes a stopped machine: its next bring-up prunes what this
        misses, which the dropped stamp keeps it from skipping.
        """
        try:
            await _drop_stamp(workspace_id)
            computer = await get_computer_for_workspace(workspace_id)
            if not computer or computer.get("status") != "running":
                return
            sandbox_id = computer.get("provider_ref")
            if not sandbox_id:
                return
            async with (
                self._computer_runtime(computer, str(sandbox_id)) as runtime,
                held_workspace_layout(
                    workspace_id, computer.get("root_dir") or DEFAULT_ROOT_DIR
                ) as layout,
            ):
                if layout is not None:
                    await _thread_dirs.prune_dead_thread_dirs(runtime, layout, workspace_id)
        except Exception as e:
            logger.warning(f"Could not prune deleted thread dirs of {workspace_id}: {e}")
