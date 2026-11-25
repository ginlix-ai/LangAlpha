"""The Drive mount as the host drives it: the Drive daemon's commands in a
sandbox, and the question every tool asks before a command or file call that
names the Drive. The same package and ``boot`` as the file mount, with the
Drive's private directory, so the two daemons keep separate code copies,
locks and state while sharing the token the file mount installed.
"""

from __future__ import annotations

import enum
import re
import shlex
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol

from ptc_agent.core.sandbox import mount_commands
from ptc_agent.core.sandbox.mount_commands import MountOutcome
from ptc_agent.core.sandbox.livefs_runtime import drive_protocol
from ptc_agent.core.sandbox.retry import RetryPolicy

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

_EXEC_TIMEOUT_S = 90

#: A path a command can name the Drive by: its link in a folder or at the
#: root, or the mount itself. Not ``.agents/drive.local``, where a link's
#: set-aside files go.
_NAMES_DRIVE = re.compile(
    r"(?<![\w.-])"
    + re.escape(drive_protocol.MOUNT)
    + r"(?![\w.-])|(?<![\w.-])"
    + re.escape(".agents/drive")
    + r"(?![\w.-])"
)

#: What a command meets on a Drive path whose daemon died (the mount is
#: still there, nothing answers it), and on one whose sandbox restarted
#: under it (the link points at a mount that is gone).
DEAD_MOUNT = "Transport endpoint is not connected"
MISSING = "No such file or directory"

#: Prints ``_ANSWERS`` when the Drive answers at its path; killed at 2 s, as
#: a stat on a hung daemon waits until killed.
_ANSWERS = "drive-mount-answers"
PROBE = f"timeout -s KILL 2 test -e {shlex.quote(drive_protocol.MOUNT + '/.')} && echo {_ANSWERS}"


class DriveDisk(enum.Enum):
    """Whether the user's Drive is on this computer's disk right now, as
    ``DriveHandle.on_disk`` answers it."""

    #: Mounted and serving: Bash, code and the file tools all reach it at
    #: ``.agents/drive``.
    ON = "on"
    #: Not this computer's to have (the feature is off, or the computer is a
    #: shared workspace's): ``.agents/drive`` is no path of the Drive's.
    OFF = "off"
    #: Due here but not on disk (still starting past the wait, failed to
    #: start, or a machine that never mounts it): only the file tools and
    #: ``la drive`` reach it, and a command writing there would make plain
    #: folders the Drive never sees and backups skip.
    UNAVAILABLE = "unavailable"


class DriveHandle(Protocol):
    """What the host hands a sandbox as ``sandbox.drive``: its view of the
    Drive mount on that machine, which every tool asks before a command or
    file call that names the Drive. Execution context only; the daemon is
    the truth, and a command that meets it dead has it restarted."""

    async def on_disk(self, wait_s: float = 0.0) -> DriveDisk:
        """Whether the Drive is on the sandbox's disk right now, waiting up
        to ``wait_s`` for a start under way."""
        ...

    async def push_saves(self, text: str, call_id: str | None) -> bool:
        """Before a command's result is put together: have the saves of
        ``call_id`` still on the way sent and answered, within a bound, so
        what the server refused, and what is not saved yet, is reported with
        it. Only for a command whose ``text`` names the Drive: one that does
        not pays nothing, its saves landing in the background and a refusal
        reaching the next result. Whether it settled them, for ``after``."""
        ...

    async def after(
        self,
        output: str,
        *,
        text: str = "",
        call_id: str | None = None,
        settled: bool = False,
    ) -> str:
        """What to add to a command's result once it ran: a Drive found dead
        in ``output`` is restarted and the command is asked to run again,
        files a link set aside are named, and saves the server never
        answered are reported, unless ``push_saves`` settled ``call_id``'s
        already (``text`` is the command). Empty when nothing happened."""
        ...


class NoDrive:
    """The handle of a sandbox the host gave none (a machine outside the
    product's acquire, a test): the Drive is none of its business, so
    ``.agents/drive`` is a folder like any other there."""

    async def on_disk(self, wait_s: float = 0.0) -> DriveDisk:
        return DriveDisk.OFF

    async def push_saves(self, text: str, call_id: str | None) -> bool:
        return False

    async def after(
        self,
        output: str,
        *,
        text: str = "",
        call_id: str | None = None,
        settled: bool = False,
    ) -> str:
        return ""


NO_DRIVE: DriveHandle = NoDrive()


def names_drive(text: str) -> bool:
    """Whether a command's text names a Drive path. Read from the text, so a
    path built at runtime passes, as for the file mount's own check."""
    return _NAMES_DRIVE.search(text) is not None


#: Bash and code cannot reach the Drive here right now, though the file
#: tools can: a command writing there would make plain folders the Drive
#: never sees and backups skip.
DRIVE_UNAVAILABLE = (
    "The Drive is not on this computer's disk right now, so Bash and code cannot "
    "reach `.agents/drive`; use the file tools or `la drive` for it instead."
)
#: The Drive is not this computer's to have: ``.agents/drive`` is reserved
#: for its link, so a folder made there would be set aside once it is linked.
DRIVE_NOT_HERE = (
    "The Drive is not available here: it is an experiment the user turns on in "
    "Settings, under Experiments, and it does not reach shared workspaces."
)


def drive_refusal(disk: DriveDisk) -> str | None:
    """Why a call naming ``.agents/drive`` is refused while the Drive is
    ``disk``; None when it is on disk."""
    if disk is DriveDisk.ON:
        return None
    if disk is DriveDisk.UNAVAILABLE:
        return DRIVE_UNAVAILABLE
    return DRIVE_NOT_HERE


def met(output: str, error: str) -> bool:
    """Whether ``output`` shows ``error`` on a line naming a Drive path."""
    return any(error in line and names_drive(line) for line in output.splitlines())


async def answers(sandbox: PTCSandbox) -> bool | None:
    """Whether the Drive answers at its path (None: the exec failed)."""
    return await mount_commands.probe(sandbox, PROBE, _ANSWERS)


async def _run(sandbox: PTCSandbox, action: str, args: Sequence[str] = ()) -> MountOutcome:
    return await mount_commands.run(
        sandbox, action, args, private=drive_protocol.PRIVATE, timeout_s=_EXEC_TIMEOUT_S
    )


async def _answer(
    sandbox: PTCSandbox, action: str, args: Sequence[str], retry_policy: RetryPolicy
) -> dict[str, Any] | None:
    return mount_commands.parse(
        await mount_commands.execute(
            sandbox,
            action,
            args,
            private=drive_protocol.PRIVATE,
            timeout_s=_EXEC_TIMEOUT_S,
            retry_policy=retry_policy,
        )
    )


async def start(sandbox: PTCSandbox, *, base_url: str) -> MountOutcome:
    """Make the Drive mount serve ``base_url`` with current code, on the
    token the file mount installed."""
    return await _run(sandbox, "drive-start", ["--base-url", base_url])


async def link(sandbox: PTCSandbox, targets: Sequence[str]) -> MountOutcome:
    """Link exactly ``targets`` to the Drive mount, removing the ones an
    earlier call laid that this one leaves out."""
    args: list[str] = []
    for target in targets:
        args += ["--target", target]
    return await _run(sandbox, "drive-link", args)


async def stop(sandbox: PTCSandbox) -> MountOutcome:
    """Remove the links and unmount: the Drive went off for this computer."""
    return await _run(sandbox, "drive-stop")


async def _take(
    sandbox: PTCSandbox, action: str, *args: str
) -> list[dict[str, Any]] | None:
    """The notices a command took off the computer; None when the exec
    failed. Not retried, since a take that ran and lost its answer must not
    take again."""
    try:
        answer = await _answer(sandbox, action, args, RetryPolicy.UNSAFE)
    except Exception:  # noqa: BLE001 - the notices stay for the next take
        return None
    notes = answer.get("notices") if answer is not None else None
    return notes if isinstance(notes, list) else None


async def take_notices(sandbox: PTCSandbox) -> list[dict[str, Any]] | None:
    """The daemon's notices the server has not had (``drive-notices``)."""
    return await _take(sandbox, "drive-notices")


async def settle(sandbox: PTCSandbox, call_id: str) -> list[dict[str, Any]] | None:
    """Send ``call_id``'s saves still waiting and wait for their answers
    (``drive-settle``), then take the daemon's notices, as ``take_notices``."""
    return await _take(
        sandbox,
        "drive-settle",
        "--call",
        call_id,
        "--wait",
        str(drive_protocol.SETTLE_WAIT_S),
    )


async def status(sandbox: PTCSandbox) -> dict[str, Any] | None:
    """The daemon's own account of itself (``drive-status``)."""
    return await _answer(sandbox, "drive-status", (), RetryPolicy.SAFE)
