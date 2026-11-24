"""Keep a computer's Drive mount as due: serving and linked into exactly its
live folders while the ``drive`` feature is on for its owner, and gone when
it is off.

Asked whenever the computer's file mount serves (a turn's acquire), off the
turn's path, and never raises: the Drive is a second daemon beside the file
mount, so a start that fails costs the Drive, not the file mount or the turn.
Due only where the file mount is (``mounts_livefs``), whose token it uses,
and never for a shared workspace's computer, which holds no one's own files.

What was last brought about is remembered in Redis per computer, so a turn
whose computer already has it asks the sandbox nothing. The memory names the
sandbox, the code, the server address and the folders, and lapses after
``RECHECK_S``, so a daemon that died unseen is started again within that; a
file mount found restarted (the sandbox restarted with it) asks again at
once. The daemon stays the truth: ``drive-start`` and ``drive-link`` are
idempotent and cheap on a computer already serving.

``converge`` only brings the sandbox to what is due and says what it did.
What this worker then believes about the mount is the ``DriveMount`` handed
to the sandbox, the one owner of that belief, which also decides when to
converge: on a tool's first ask, again after a failure's backoff, and at
once on a command that met the daemon dead.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import drive_mount, livefs_mount
from ptc_agent.core.sandbox.drive_mount import DriveDisk
from ptc_agent.core.sandbox.livefs_runtime.protocol import MountError
from src.server.database import workspace as workspace_db
from src.server.database.shared_workspaces import is_holder
from src.server.database.workspace_folders import is_top_level, workspace_folders_lock
from src.server.services import drive
from src.server.services.egress.reachability import effective_relay_base_url
from src.server.services.livefs import cache, drive_notices, outcomes
from src.server.services.livefs.tracker import sandbox_id
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

#: How long what was brought about is trusted before the sandbox is asked again.
RECHECK_S = 600
#: How long a computer is remembered to have no Drive mount, so a turn with
#: the feature off asks the sandbox nothing. What is remembered is the stop,
#: never the start: the server's refusals do not take a mount down (the
#: daemon keeps serving what it cached), so a memory lost to a flush or
#: eviction must cost one more ``drive-stop``, which is idempotent, rather
#: than skip it.
OFF_S = 86400

#: A Drive found missing is probed at most this often, as livefs's is.
_PROBE_EVERY_S = 30.0
#: A Drive that could not be brought up is tried again, by the next command
#: or file call naming it, at most this often.
_RETRY_EVERY_S = 30.0

_LOCK_WAIT_S = 1.5
_BUSY_TRIES = 10
_BUSY_PAUSE_S = 3.0


class Belief(enum.Enum):
    """What the last settled converge left this worker believing about the
    Drive mount on one sandbox."""

    #: Nothing asked yet.
    UNKNOWN = "unknown"
    SERVING = "serving"
    #: Not this computer's to have.
    OFF = "off"
    #: Due but not on disk: a failed start, or a machine that never mounts.
    UNAVAILABLE = "unavailable"

    @property
    def disk(self) -> DriveDisk:
        if self is Belief.SERVING:
            return DriveDisk.ON
        if self is Belief.OFF:
            return DriveDisk.OFF
        return DriveDisk.UNAVAILABLE


class Converged(enum.Enum):
    """What one ``converge`` did."""

    #: Already as due by the memory: the sandbox was asked nothing.
    KEPT = "kept"
    #: Started or relinked, and serving.
    SERVING = "serving"
    #: Taken down: the feature is off and the computer was not known stopped.
    STOPPED = "stopped"
    #: Not due, and never had here.
    OFF = "off"
    #: A settle or sync held the folders: nothing was asked.
    BUSY = "busy"
    FAILED = "failed"

    @property
    def belief(self) -> Belief | None:
        """What it leaves the worker believing; None leaves the belief as it
        was, since nothing was asked."""
        if self in (Converged.KEPT, Converged.SERVING):
            return Belief.SERVING
        if self in (Converged.STOPPED, Converged.OFF):
            return Belief.OFF
        if self is Converged.FAILED:
            return Belief.UNAVAILABLE
        return None


@dataclass(frozen=True)
class Convergence:
    done: Converged
    #: Link paths that already held files, and where those were moved.
    set_aside: Mapping[str, str] = field(default_factory=dict)


def _memo_key(computer_id: str) -> str:
    return f"{cache.tag(computer_id)}:drive"


def _off_key(computer_id: str) -> str:
    return f"{cache.tag(computer_id)}:drive-off"


async def wanted(user_id: str | None) -> bool:
    """Whether ``user_id``'s computers are to have the Drive at all."""
    return bool(user_id and not is_holder(user_id) and await drive.enabled(user_id))


async def due(sandbox: Any, user_id: str | None) -> bool:
    """Whether the Drive is to be mounted on ``sandbox``."""
    return bool(
        sandbox is not None
        and getattr(sandbox, "mounts_livefs", False)
        and getattr(sandbox, "runtime", None) is not None
        and await wanted(user_id)
    )


async def targets(computer_id: str, layout: SandboxLayout) -> list[str]:
    """Where the Drive is linked: the computer root and every live workspace
    folder, as the file mount links its own points."""
    found = [layout.join(SandboxLayout.DRIVE_DIR)]
    for folder in await workspace_db.get_live_workspace_folders_for_computer(
        computer_id
    ):
        dir_name = folder.get("dir_name")
        # A folder that owns the root reads the root's link; one staged
        # mid-move is linked once it lands.
        if dir_name and is_top_level(dir_name):
            found.append(layout.for_workspace(dir_name).join(SandboxLayout.DRIVE_DIR))
    return sorted(found)


async def converge(
    sandbox: Any,
    *,
    computer_id: str,
    user_id: str | None,
    provider: str,
    force: bool = False,
) -> Convergence:
    """Bring the Drive mount on ``sandbox`` to what is due, and say what was
    done. ``force`` asks the sandbox whatever was remembered."""
    computer_id = str(computer_id)
    try:
        return await _converge(sandbox, computer_id, user_id, provider, force)
    except Exception:
        logger.warning(
            "drive mount not brought up on computer %s", computer_id, exc_info=True
        )
        return Convergence(Converged.FAILED)


async def _converge(
    sandbox: Any, computer_id: str, user_id: str | None, provider: str, force: bool
) -> Convergence:
    memo = get_cache_client()
    if not await due(sandbox, user_id):
        if (
            sandbox is None
            or getattr(sandbox, "runtime", None) is None
            # Never started where the file mount is not, so nothing to stop.
            or not getattr(sandbox, "mounts_livefs", False)
        ):
            return Convergence(Converged.OFF)
        if await memo.get(_off_key(computer_id)) is not None:
            return Convergence(Converged.OFF)
        # Under the lock a start holds until it has published its memory, so
        # neither lands between the other's change and its record of it.
        async with workspace_folders_lock(computer_id, wait_s=_LOCK_WAIT_S) as folders:
            if folders is None:
                return Convergence(Converged.BUSY)
            outcome = await drive_mount.stop(sandbox)
            if not outcome.ok:
                logger.warning(
                    "drive mount not stopped on computer %s: %s %s",
                    computer_id,
                    outcome.error,
                    outcome.reason or "",
                )
                return Convergence(Converged.FAILED)
            await memo.delete(_memo_key(computer_id))
            await memo.set(_off_key(computer_id), True, OFF_S)
        logger.info("drive mount stopped on computer %s", computer_id)
        return Convergence(Converged.STOPPED)
    base_url = effective_relay_base_url(provider)
    async with workspace_folders_lock(computer_id, wait_s=_LOCK_WAIT_S) as folders:
        if folders is None:
            return Convergence(Converged.BUSY)
        wanted_now = {
            "sandbox": sandbox_id(sandbox),
            "code": livefs_mount.code_version(),
            "base_url": base_url,
            "targets": await targets(computer_id, sandbox.layout),
        }
        if not force and await memo.get(_memo_key(computer_id)) == wanted_now:
            return Convergence(Converged.KEPT)
        await memo.delete(_off_key(computer_id))
        started = await drive_mount.start(sandbox, base_url=base_url)
        if not started.ok:
            level = (
                logging.INFO
                if started.error == MountError.INSTALLING
                else logging.WARNING
            )
            logger.log(
                level,
                "drive mount not up on computer %s: %s %s",
                computer_id,
                started.error,
                started.reason or "",
            )
            return Convergence(Converged.FAILED)
        linked = await drive_mount.link(sandbox, wanted_now["targets"])
        if linked.ok:
            await memo.set(_memo_key(computer_id), wanted_now, RECHECK_S)
    if not linked.ok:
        logger.warning(
            "drive links not laid on computer %s: %s %s",
            computer_id,
            linked.error,
            linked.failed or linked.reason or "",
        )
        return Convergence(Converged.FAILED)
    if linked.set_aside:
        # Never deleted: whatever stood where a link goes is kept beside it.
        logger.warning(
            "drive links moved files aside on computer %s: %s",
            computer_id,
            linked.set_aside,
        )
    if started.started:
        logger.info("drive mount serving on computer %s", computer_id)
    return Convergence(Converged.SERVING, linked.set_aside or {})


async def forget(computer_id: str) -> None:
    """The computer left service: its sandbox and token are gone, so the
    next one starts from nothing."""
    await get_cache_client().delete(_memo_key(str(computer_id)))


class DriveRuns:
    """Every converge this worker runs for one computer
    (``MachineState.drive``), whichever sandbox object asked for it. One at
    a time: a request while one runs for the same sandbox is that one's to
    meet, unless forced; any other runs after it, since a run's answer is
    about the sandbox it ran on."""

    def __init__(self) -> None:
        self._task: asyncio.Task[Convergence] | None = None
        self._sandbox: str | None = None

    def soon(
        self,
        run: Callable[[], Awaitable[Convergence]],
        *,
        force: bool,
        sandbox: str | None = None,
    ) -> asyncio.Task[Convergence]:
        running = self._task
        if (
            running is not None
            and not running.done()
            and not force
            and sandbox == self._sandbox
        ):
            return running

        async def after_running() -> Convergence:
            if running is not None and not running.done():
                await asyncio.wait({running})
            return await run()

        self._task = asyncio.get_running_loop().create_task(after_running())
        self._sandbox = sandbox
        return self._task


class DriveMount:
    """The Drive mount on one sandbox as this worker's tools see it
    (``drive_mount.DriveHandle``), handed to the sandbox at each acquire.

    Execution context only: what the last settled converge here brought
    about, and the runs this handle asked for whose answers it has yet to
    take, each taken once and in order. A command naming the Drive waits for
    a run it must not run ahead of: any while nothing is known to serve, and
    a forced one always. The daemon stays the truth: a command whose output
    shows it dead restarts it, whatever was believed.
    """

    def __init__(
        self,
        sandbox: Any,
        computer_id: str,
        user_id: str | None,
        provider: str,
        runs: DriveRuns,
    ) -> None:
        self.sandbox = sandbox
        self.computer_id = str(computer_id)
        self.user_id = user_id
        self.provider = provider
        self.belief = Belief.UNKNOWN
        self._runs = runs
        #: Runs asked for and not yet taken, with whether a command waits
        #: for each.
        self._asked: list[tuple[asyncio.Task[Convergence], bool]] = []
        self._notes: list[str] = []
        self._probed_at = 0.0
        self._failed_at = 0.0

    def owned_by(self, computer_id: str, user_id: str | None) -> bool:
        return self.computer_id == str(computer_id) and self.user_id == user_id

    def converge_soon(self, *, force: bool = False) -> None:
        """``converge`` off the caller's path, through the computer's
        ``DriveRuns``."""
        self._ask(lambda: self._converge_past_busy(force), force=force)

    def _ask(
        self, run: Callable[[], Awaitable[Convergence]], *, force: bool
    ) -> asyncio.Task[Convergence]:
        self._take_done()
        task = self._runs.soon(run, force=force, sandbox=sandbox_id(self.sandbox))
        # One already served keeps going meanwhile, unless forced.
        waits = force or self.belief is not Belief.SERVING
        for i, (asked, waited) in enumerate(self._asked):
            if asked is task:
                self._asked[i] = (task, waited or waits)
                return task
        self._asked.append((task, waits))
        return task

    def _outstanding(self) -> list[asyncio.Task[Convergence]]:
        """The runs a command naming the Drive waits for, their answers taken
        first for those that ended."""
        self._take_done()
        return [task for task, waits in self._asked if waits]

    def _take_done(self) -> None:
        while self._asked and self._asked[0][0].done():
            task, _ = self._asked.pop(0)
            if not task.cancelled() and task.exception() is None:
                self._take(task.result())

    async def _converge(self, force: bool) -> Convergence:
        return await converge(
            self.sandbox,
            computer_id=self.computer_id,
            user_id=self.user_id,
            provider=self.provider,
            force=force,
        )

    async def _converge_past_busy(self, force: bool) -> Convergence:
        for _ in range(_BUSY_TRIES):
            found = await self._converge(force)
            if found.done is not Converged.BUSY:
                return found
            # A settle or sync is moving folders; the links wait for it.
            await asyncio.sleep(_BUSY_PAUSE_S)
        return Convergence(Converged.FAILED)

    def _take(self, found: Convergence) -> None:
        belief = found.done.belief
        if belief is None:
            return
        self.belief = belief
        if belief is Belief.UNAVAILABLE:
            self._failed_at = time.monotonic()
        for target, aside in sorted(found.set_aside.items()):
            self._notes.append(
                f"What was at {target} before the Drive was linked there is now at {aside}."
            )

    def _serving(self) -> bool:
        return not self._outstanding() and self.belief is Belief.SERVING

    def _mount_starting(self) -> bool:
        """Whether the file mount's start, handed off a turn's path, has yet
        to answer. The Drive runs on the token that start installs and would
        hold the folders it links, so nothing is asked of it meanwhile: the
        start asks the Drive itself when it answers (``_serve_livefs``)."""
        mount = getattr(self.sandbox, "livefs", None)
        return mount is not None and mount.serving is None

    async def on_disk(self, wait_s: float = 0.0) -> DriveDisk:
        mounts = getattr(self.sandbox, "mounts_livefs", False)
        if self.belief is Belief.UNKNOWN and not self._asked:
            if not mounts:
                # Never mounted here: the feature decides whether that
                # matters, read once for the sandbox's life.
                self.belief = (
                    Belief.UNAVAILABLE if await wanted(self.user_id) else Belief.OFF
                )
            elif not self._mount_starting():
                self.converge_soon()
        elif (
            self.belief is Belief.UNAVAILABLE
            and mounts
            and not self._outstanding()
            and time.monotonic() - self._failed_at >= _RETRY_EVERY_S
            and not self._mount_starting()
        ):
            # Not left down until the computer restarts: whatever stopped it
            # may have passed.
            self.converge_soon(force=True)
        starting = self._outstanding()
        if starting and wait_s > 0:
            # Waited for, never cancelled: the run is the computer's.
            await asyncio.wait(starting, timeout=wait_s)
            starting = self._outstanding()
        return DriveDisk.UNAVAILABLE if starting else self.belief.disk

    async def push_saves(self, text: str, call_id: str | None) -> bool:
        """Have the daemon send ``call_id``'s saves and wait for them
        (``drive_mount.settle``), then file every notice it holds as an
        outcome of its own call, so this command's are in the result the
        file mount's report puts together next."""
        if call_id is None or not self._serving() or not drive_mount.names_drive(text):
            return False
        taken = await drive_mount.settle(self.sandbox, call_id)
        if taken:
            await drive_notices.file(self.computer_id, self.sandbox.layout.root, taken)
        return True

    async def after(
        self,
        output: str,
        *,
        text: str = "",
        call_id: str | None = None,
        settled: bool = False,
    ) -> str:
        serving = self._serving()
        notes, self._notes = self._notes, []
        if not settled:
            unsaved = await self._unsaved(text, call_id)
            if unsaved:
                notes.append(unsaved)
        if serving and await self._dead(output):
            logger.warning(
                "drive mount found dead on computer %s; restarting", self.computer_id
            )
            # Not trusted over what the command met: ask the sandbox again,
            # through the computer's runs, after any under way.
            await get_cache_client().delete(_memo_key(self.computer_id))
            task = self._ask(lambda: self._converge(True), force=True)
            await asyncio.wait({task})
            self._take_done()
            found = (
                task.result()
                if not task.cancelled() and task.exception() is None
                else Convergence(Converged.FAILED)
            )
            if found.done.belief is Belief.SERVING:
                notes.append(
                    "The Drive had stopped and is serving again; rerun the command."
                )
            else:
                notes.append(
                    f"The Drive stopped and could not be started again ({found.done.value})."
                )
        return "\n".join(notes)

    async def _unsaved(self, text: str, call_id: str | None) -> str:
        """Saves the server never answered, which the daemon could not tell
        it about either: taken off the computer when the command named the
        Drive or the daemon has not reached the server lately. Those of this
        command are answered here; the rest are filed for their own calls."""
        if not self._serving():
            return ""
        if not drive_mount.names_drive(text) and await drive_notices.seen(
            self.computer_id
        ):
            return ""
        taken = await drive_mount.take_notices(self.sandbox)
        if not taken:
            return ""
        own = await drive_notices.file(
            self.computer_id, self.sandbox.layout.root, taken, own_call=call_id
        )
        return outcomes.describe(own)

    async def _dead(self, output: str) -> bool:
        """Whether ``output`` shows the Drive gone: its daemon dead under the
        mount, or the mount itself gone with a restart no turn saw (a missing
        Drive path, confirmed by a probe at most every ``_PROBE_EVERY_S``)."""
        if drive_mount.met(output, drive_mount.DEAD_MOUNT):
            return True
        if not drive_mount.met(output, drive_mount.MISSING):
            return False
        now = time.monotonic()
        if now - self._probed_at < _PROBE_EVERY_S:
            return False
        self._probed_at = now
        return await drive_mount.answers(self.sandbox) is False
