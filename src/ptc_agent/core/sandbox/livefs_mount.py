"""The file mount as the host drives it: the daemon's own commands in a
sandbox, and each tool command run through it.

The server decides what to mount and mints the token; this only carries them
into the sandbox. The daemon's commands are idempotent, so every caller asks
for the state it wants and the daemon does whatever part of it is missing.
"""

from __future__ import annotations

import secrets
import shlex
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol

import structlog

from ptc_agent.core.paths import (
    MEMO_USER_DIR,
    MEMORY_USER_DIR,
    SandboxLayout,
    WorkspaceLayout,
)
from ptc_agent.core.sandbox import mount_commands
from ptc_agent.core.sandbox.livefs_runtime import lifecycle, protocol
from ptc_agent.core.sandbox.mount_commands import MountOutcome, code_version  # noqa: F401 - read here

if TYPE_CHECKING:
    from langchain_core.messages import AnyMessage

    from ptc_agent.agent.transcript import BorrowedFiles, TranscriptTarget, Window
    from ptc_agent.core.sandbox.drive_mount import DriveHandle
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

# Covers a cold start's wait for its mount, plus the probe and links.
_EXEC_TIMEOUT_S = lifecycle.START_TIMEOUT_S + 20

_ANSWERS = "livefs-mount-answers"
#: Prints ``_ANSWERS`` when something answers at the mount's path. Killed at
#: 2 s: a stat on a daemon that hangs waits until killed.
PROBE = f"timeout -s KILL 2 test -e {shlex.quote(protocol.MOUNT + '/.')} && echo {_ANSWERS}"

logger = structlog.get_logger(__name__)


def answered(stdout: str | None) -> bool:
    """Whether a command that ran ``PROBE`` found the mount answering."""
    return _ANSWERS in (stdout or "")


@dataclass(frozen=True)
class CallContext:
    """Who a command runs for. The mount serves the whole computer, so a save
    through it names only the call, and a file whose meaning depends on the
    conversation (a new automation's defaults and ``"current"`` thread)
    reads the rest from here."""

    workspace_id: str | None = None
    thread_id: str | None = None
    # None reads the user's stored zone, when a file needs one.
    timezone: str | None = None
    #: The turn's run, read per call (``for_call``): one context serves
    #: every run of the thread and every subagent of the turn.
    run_id: str | None = None
    #: ``main`` or the subagent's name, read per call as ``run_id`` is.
    agent: str | None = None

    def for_call(self, config: Mapping[str, Any] | None) -> CallContext:
        """This context as one tool call runs it, with the run and the agent
        its config names: the tool's metadata keeps the turn's ``run_id``,
        and a subagent's run adds its ``subagent_type``."""
        metadata = (config or {}).get("metadata") or {}
        run_id = metadata.get("run_id") or (config or {}).get("run_id")
        agent = metadata.get("subagent_type") or "main"
        return replace(self, run_id=str(run_id) if run_id else None, agent=str(agent))

    @property
    def sitting(self) -> str | None:
        """What a Drive save by this call folds into: every save one agent
        makes to one file in one run is one version, through the file tools
        and the mount alike. None outside a run."""
        if not (self.thread_id and self.run_id):
            return None
        return f"{self.thread_id}:{self.run_id}"


class MountHandle(Protocol):
    """What a tool asks of the mount around one command. The host
    implements it, since the token, the links and the save outcomes are its
    own, and hands it to the sandbox while the mount serves, or ahead of a
    start a turn did not wait for."""

    @property
    def serving(self) -> bool | None:
        """Whether the mount serves; None while the start it was handed
        ahead of has yet to answer."""

    async def started(self) -> None:
        """Wait out a start still to answer."""

    async def ready(self, workspace_id: str | None = None) -> bool:
        """Wait out a start still to answer, links still going in and a token
        that has to be replaced first; whether the mount then serves ``workspace_id``'s folder (None:
        the computer's root). A command run without it would write beside
        the files the store holds."""

    async def report(
        self, call_id: str, output: str, context: CallContext | None = None
    ) -> str:
        """Saves through the mount that failed during ``call_id`` (and any
        that failed after an earlier command in ``context``'s thread
        returned), as text for the tool result; empty when none did. An
        ``output`` showing the mount dead restarts it."""

    async def save_transcript(
        self, target: TranscriptTarget, messages: Sequence[AnyMessage], *, window: Window
    ) -> TranscriptTarget | None:
        """Store one agent's transcript from the messages in hand, ``window``
        having been trimmed from their head, where the mount serves it. The
        target as it landed, with the files it borrows from the chats it was
        forked from (``TranscriptTarget.borrowed``), or None when it did not."""


class HistoryFiles(Protocol):
    """Past threads for the file tools on a machine the mount never serves.

    The tools read them in process, through routes the host builds over the
    rows, which Bash and code do not reach. The host implements it, since the
    rows are its own.
    """

    def routes(self, sandbox: Any) -> list[Any]:
        """Read-only filesystem routes over ``sandbox``'s paths: each
        workspace folder's transcripts and the computer's thread index."""

    def serves(self, workspace_id: str | None) -> bool:
        """Whether the routes include ``workspace_id``'s transcripts, which
        a pointer at one of its threads needs."""

    async def save_transcript(
        self, target: TranscriptTarget, messages: Sequence[AnyMessage], *, window: Window
    ) -> TranscriptTarget | None:
        """As ``MountHandle.save_transcript``, for the routes to serve."""


@dataclass(frozen=True)
class TranscriptReach:
    """Where an agent's transcripts are saved for its tools to read.

    ``shell`` says Bash and code see them too, which only the mount gives:
    its links put them in the workspace folder, so a pointer at them is only
    as good as those links. The in-process routes serve the file tools alone.
    """

    home: MountHandle | HistoryFiles
    shell: bool
    #: The thread's turns before its own when it is a snapshot fork, in the
    #: transcripts of the chats it was forked from: its own agent's files
    #: number on from the last (``TranscriptTarget.borrowed``).
    borrowed: tuple[BorrowedFiles, ...] = ()


#: Files who a command runs for under its call id. The host sets one on every
#: sandbox (``PTCSandbox.call_filer``): a save through the mount reads it, and
#: so does a call the ``la`` CLI makes, on a machine whatever mounts it.
CallFiler = Callable[[str, CallContext], Awaitable[None]]


async def run_call(
    run: Callable[[str | None], Awaitable[tuple[str, dict[str, Any]]]],
    *,
    command: str,
    mount: MountHandle | None,
    drive: DriveHandle,
    filer: CallFiler | None,
    context: CallContext | None,
    paid_calls: Callable[[str], Awaitable[str]] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Run ``command`` as one call: filed with who it runs for, executed, its
    Drive saves settled, then added to its output in this order what its
    paid relay calls cost, what its saves through the file mount reported,
    and the Drive's note.

    It runs under a fresh call id when something reads one (a mount, or a
    context to file), else under none. The id is collected once ``run``
    returns, so the later saves of a command it handed off reach the next
    command's result. A cancelled run reports nothing.
    """
    filing = filer is not None and context is not None
    call_id = secrets.token_hex(8) if mount is not None or filing else None
    if filing:
        await filer(call_id, context)
    output, artifact = await run(call_id)
    # Before the mount's report collects the call's outcomes: a save the
    # Drive's server refused is filed by then.
    settled = await drive.push_saves(command, call_id)
    if call_id is not None and paid_calls is not None:
        output = _added(output, await paid_calls(call_id))
    if mount is not None:
        output = _added(output, await mount.report(call_id, output, context))
    note = await drive.after(output, text=command, call_id=call_id, settled=settled)
    return _added(output, note), artifact


def _added(output: str, more: str) -> str:
    return f"{output}\n\n{more}" if more else output


_MEMORY_MARKERS = (f"{MEMORY_USER_DIR}/", f"{WorkspaceLayout.MEMORY_DIR}/", f"{MEMO_USER_DIR}/")
_SERVED_DIRS = (
    SandboxLayout.AUTOMATIONS_DIR,
    SandboxLayout.USER_AGENTS_DIR,
    SandboxLayout.USER_PROFILE_DIR,
    SandboxLayout.WORKFLOWS_DIR,
)


def unserved_tree(mount: MountHandle | None, text: str) -> str | None:
    """The store-backed tree a command names while no mount serves it:
    "memory" for the memory and memo tiers, else the directory. The tool
    refuses such a command: it would find no files there, and a write would
    land where nothing reads it."""
    if mount is not None:
        return None
    if any(marker in text for marker in _MEMORY_MARKERS):
        return "memory"
    return next((d for d in _SERVED_DIRS if d in text), None)


async def start(
    sandbox: PTCSandbox,
    *,
    config: dict[str, Any] | None,
    base_url: str,
) -> MountOutcome:
    """Make the mount serve ``base_url``, starting the daemon if it is not
    serving current code.

    ``config`` is a new token to install; None keeps the one the sandbox
    has. It travels in the root command's environment and the daemon keeps
    it where only root can read it, which hides it from agent code only where
    that code runs as another user (see ``protocol.CONFIG_ENV``).
    """
    return await mount_commands.run(
        sandbox, "start", ["--base-url", base_url], config, timeout_s=_EXEC_TIMEOUT_S
    )


async def answers(sandbox: PTCSandbox) -> bool | None:
    """Whether the mount answers at its path (None: the exec failed). After a
    restart no reconnect saw, the links dangle and answer ENOENT, which no
    command tells apart from a missing file."""
    return await mount_commands.probe(sandbox, PROBE, _ANSWERS)


async def link(sandbox: PTCSandbox, links: Sequence[tuple[str, str]]) -> MountOutcome:
    """Link exactly ``links`` (mount path, target), removing the links an
    earlier call laid that this one leaves out."""
    args: list[str] = []
    for source, target in links:
        args += ["--link", f"{source}:{target}"]
    return await mount_commands.run(sandbox, "link", args, timeout_s=_EXEC_TIMEOUT_S)
