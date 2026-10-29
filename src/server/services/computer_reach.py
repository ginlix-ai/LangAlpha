"""The user's other computers, as Home's agent reaches one for a tool call.

The Chief of Staff runs on the computer Home is on, and its machine tools take
a ``computer`` naming another of the user's computers (see
``ptc_agent.agent.tools.computer_reach``). This resolves the name, wakes the
computer as a turn on it would, and readies its folders once per session.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import posixpath
import shlex
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any

from fastapi import HTTPException

from ptc_agent.agent.tools.computer_reach import (
    Attached,
    ComputerTarget,
    ComputerUnreachable,
    file_refusal,
)
from ptc_agent.core.paths import lexical_path
from ptc_agent.core.project_context import (
    MachinePaths,
    ProjectContext,
    context_without_project,
    project_bound,
)
from ptc_agent.core.sandbox.files import FileOverCap
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from ptc_agent.core.session import Session

from src.server.database.computer import (
    DEFAULT_ROOT_DIR,
    get_computer,
    get_computer_roster,
)
from src.server.database.workspace_binding import (
    get_computer_folders,
    get_computers_with_folders,
)
from src.server.models.computer import ComputerStatus
from src.tools.messaging.tools import ATTACH_MAX_BYTES, SEND_MAX_BYTES
from src.tools.secretary.utils import spoken_computer_name

logger = logging.getLogger(__name__)

#: A computer id spelled short enough to be ambiguous is read as a name only.
_MIN_ID_PREFIX = 8

#: How often a call stamps the computer it reaches as in use: the idle sweep
#: counts only turns on a computer's own workspaces, and minutes are its grain.
_KEEP_ALIVE_EVERY_S = 60.0

# The wakes still running, held so one runs to its end even once the turn
# that started it is gone: a start cut off midway never hands its claim back
# half built.
_wakes: set[asyncio.Task] = set()

#: Home's folder for the copies a send makes of files no workspace folder
#: holds: the messaging service fetches a file only from a workspace's folder,
#: and a file sent as a link keeps opening from here, the machine asleep or not.
SENT_DIR = "sent"

_MB = 1024 * 1024


@dataclass
class _Reached:
    """One of the user's computers as this turn has reached it."""

    #: The last wake, running or done. A call that stops waiting leaves it
    #: running, and the next call waits on it rather than starting another.
    wake: asyncio.Task[Session] | None = None
    #: The session the login home was read for, and that home.
    home: tuple[Session, str | None] | None = None
    #: When a call last counted as use of the computer.
    stamped: float = float("-inf")


@dataclass(frozen=True)
class _Copy:
    """A file a send copies into Home, read before any copy is written."""

    path: str
    name: str
    data: bytes


def _label(row: Mapping[str, Any]) -> str:
    return spoken_computer_name(row.get("name"))


def _root(row: Mapping[str, Any]) -> str:
    return str(row.get("root_dir") or DEFAULT_ROOT_DIR)


def _listing(rows: Sequence[Mapping[str, Any]]) -> str:
    return ", ".join(f'"{_label(r)}" (computer_id {r["computer_id"]})' for r in rows)


def pick_computer(rows: Sequence[Mapping[str, Any]], wanted: str) -> Mapping[str, Any]:
    """The computer ``wanted`` names among ``rows``: its id, its name in any
    case, or a prefix of its id long enough to be one. Raises
    ``ComputerUnreachable`` naming the computers there are."""
    typed = wanted.strip()
    text = typed.strip("\"'`").strip()
    key = text.lower()
    # Every reading at once: its id or a long enough prefix of one, its name
    # as typed and unquoted, and its name as `<activity>` prints it (on one
    # line, its angle brackets escaped), folded as the index folds names. A
    # name may hold quotes, "&lt;" or another computer's id, so a spelling
    # these readings match on two computers asks for the id like a shared name.
    spoken = {
        " ".join(reading.split())
        for spelling in (typed.lower(), key)
        for reading in (spelling, spelling.replace("&lt;", "<").replace("&gt;", ">"))
    }
    named = [
        r
        for r in rows
        if _label(r).lower() in spoken
        or (len(key) >= _MIN_ID_PREFIX and str(r["computer_id"]).startswith(key))
    ]
    if len(named) == 1:
        return named[0]
    if named:
        raise ComputerUnreachable(
            f'{len(named)} computers answer to "{text}". Pass the computer_id of '
            f"the one you mean: {_listing(named)}."
        )
    raise ComputerUnreachable(
        f'The user has no computer named "{text}". Their computers: {_listing(rows)}.'
    )


def _why_not(name: str, error: BaseException) -> str:
    """The agent's sentence for a computer that could not be woken."""
    if isinstance(error, HTTPException):
        # The plan's computer limit; its message is the platform's own copy.
        detail = error.detail if isinstance(error.detail, dict) else {}
        reason = str(detail.get("message") or "the user's plan does not allow it now")
        return f'Computer "{name}" could not start: {reason}'
    if isinstance(error, SSHConnectError):
        return f'Computer "{name}" is not reachable: {error.for_owner}'
    if isinstance(error, SandboxTransientError):
        return (
            f'Computer "{name}" is not reachable right now. If it is the user\'s '
            "own machine, it may be asleep or offline."
        )
    return f'Computer "{name}" could not be started. The user can check it in Computers.'


#: What a name in ``sent`` holds from its claim until its copy lands.
_CLAIM_MARK = "langalpha: copy in progress"


def _claim_script(name: str, data: bytes) -> str:
    """A command, run in Home's folder, that names the copy of ``data`` in
    ``sent``: the first of ``name``, ``name-2``, ``name-3``... already
    holding those bytes, printed as ``same <name>``, or else the first free
    one, created and printed as ``new <name>``. A link to an earlier copy
    keeps opening the file it was sent with, and a re-run of a send names the
    copy its first attempt made, so the messaging service, which keys a
    repeat on the paths, answers it as one instead of sending again.

    Created rather than listed: noclobber opens the file exclusively, so two
    sends at once, from any worker, never take one name. The claim writes
    ``_CLAIM_MARK``, which the upload replaces, so a name still claimed is no
    copy to reuse while an empty file's copy is one. It stops at a failure
    that is not the name being taken, rather than counting on. The claim is
    ``printf``, not ``:``: a special builtin whose redirection fails ends a
    POSIX shell such as dash, so the first taken name would end the script
    there."""
    stem, dot, ext = name.rpartition(".")
    if not stem:
        stem, dot, ext = name, "", ""
    digest = hashlib.sha256(data).hexdigest()
    q = shlex.quote
    return (
        f"mkdir -p {q(SENT_DIR)} && cd {q(SENT_DIR)} && set -C && f={q(name)} && n=2 && "
        f'while ! {{ printf %s {q(_CLAIM_MARK)} > "$f"; }} 2>/dev/null; do [ -e "$f" ] || exit 1; '
        f'case "$({{ sha256sum || shasum -a 256; }} < "$f" 2>/dev/null)" in '
        f'{digest}*) printf "same %s" "$f"; exit 0;; esac; '
        f'f={q(stem)}-"$n"{q(dot + ext)}; n=$((n + 1)); done && printf "new %s" "$f"'
    )


def _woke(wake: asyncio.Task[Session], live: Session | None) -> bool:
    """Whether a finished wake handed back the session still live here, which
    is then ready for a call as it stands."""
    return not wake.cancelled() and wake.exception() is None and wake.result() is live


def _log_failure(computer_id: str, wake: asyncio.Task[Session]) -> None:
    if not wake.cancelled() and (e := wake.exception()) is not None:
        logger.warning(
            "Chief of Staff could not reach computer %s: %s: %s",
            computer_id,
            type(e).__name__,
            e,
        )


class TurnComputers:
    """The user's computers as one Chief of Staff turn reaches them.

    One per build. Each call re-reads the user's computers (a rename or a new
    one lands mid-turn) and wakes the one it names, which is a row read and a
    cached handle once the computer is up; its folders are readied once per
    session it hands back.
    """

    def __init__(
        self,
        user_id: str,
        own_computer_id: str | None,
        own_session: Session,
        touched: set[str] | None = None,
    ) -> None:
        self._user_id = user_id
        self._own = str(own_computer_id) if own_computer_id else None
        self._own_session = own_session
        self._reached: dict[str, _Reached] = {}
        #: The other computers a call ran on, which the turn's end backs up as
        #: it backs up its own: nothing else mirrors what a call wrote there
        #: before that computer stops.
        self.touched = touched if touched is not None else set()

    def _record(self, computer_id: str) -> _Reached:
        return self._reached.setdefault(computer_id, _Reached())

    async def reach(self, computer: str) -> ComputerTarget | None:
        row = pick_computer(await get_computer_roster(self._user_id), computer)
        if str(row["computer_id"]) == self._own:
            return None
        target, _ = await self._at(row)
        return target

    async def _at(self, row: Mapping[str, Any]) -> tuple[ComputerTarget, dict[str, str]]:
        """``row``'s computer, awake, as a call from Home reaches it, and the
        workspaces on it by folder."""
        computer_id = str(row["computer_id"])
        own = computer_id == self._own
        session = self._own_session if own else await self._wake(computer_id, _label(row))
        folders = await get_computer_folders(computer_id)
        target = ComputerTarget(
            computer_id=computer_id,
            name=_label(row),
            session=session,
            # The root, where the folders are each a workspace's own: none is
            # this call's to fold a path into, and Home's agent knows this
            # computer only by its own paths.
            project=ProjectContext(
                workspace_id="",
                dir_name=None,
                sibling_dir_names=tuple(folders),
                machine=MachinePaths(home=await self._home(computer_id, session)),
            ),
            root=_root(row),
        )
        if not own:
            self.touched.add(computer_id)
            await self._keep_alive(computer_id, session)
        return target, folders

    async def _home(self, computer_id: str, session: Session) -> str | None:
        """The home of the user a computer's commands run as, once per session,
        which opens the whole machine to a file call; None where a file call
        would run as someone else, whose files stay held to the root and
        ``/tmp``."""
        sandbox = session.sandbox
        if sandbox is None or sandbox.runtime is None or not sandbox.files_as_command_user:
            return None
        record = self._record(computer_id)
        if record.home is not None and record.home[0] is session:
            return record.home[1]
        try:
            home = (await sandbox.runtime.login_home()) or None
        except Exception as e:  # noqa: BLE001 - held to the root, the call still runs
            logger.warning("Could not read the home of %s: %s", computer_id, e)
            return None
        record.home = (session, home)
        return home

    async def attach(self, files: Sequence[tuple[str, str]]) -> list[Attached]:
        """The files a send names, each by a computer and its whole path there.

        One inside a workspace's folder is sent from that workspace, and the
        messaging service fetches it as it does a file named without a
        computer. Anything else is copied into Home's ``sent`` folder first,
        since the service fetches only from a workspace's folder. Each of
        those is read before the first copy is written, so one that cannot go
        leaves no copy of the others. A send the service refuses afterwards
        leaves its copies, which a resend of the same file names again
        (``_claim_script``). Raises ``ComputerUnreachable`` with the agent's
        sentence when one cannot go.
        """
        rows = await get_computer_roster(self._user_id)
        targets: dict[str, tuple[ComputerTarget, dict[str, str]]] = {}
        found: list[Attached | _Copy] = []
        room = SEND_MAX_BYTES
        for computer, path in files:
            row = pick_computer(rows, computer)
            computer_id = str(row["computer_id"])
            if computer_id not in targets:
                targets[computer_id] = await self._at(row)
            item = await self._find(*targets[computer_id], path, room)
            if isinstance(item, _Copy):
                room -= len(item.data)
            found.append(item)
        return await self._copy(found)

    async def _find(
        self, target: ComputerTarget, folders: Mapping[str, str], path: str, room: int
    ) -> Attached | _Copy:
        """The file ``path`` names on ``target``: sent from the folder that
        holds it, or read to be copied, within ``room``."""
        if refused := file_refusal(target, "Read", {"file_path": path}):
            raise ComputerUnreachable(refused)
        sandbox = target.sandbox
        with project_bound(target.project):
            whole = sandbox.normalize_path(path)
        root = target.root.rstrip("/")
        for folder, workspace_id in folders.items():
            if whole.startswith(f"{root}/{folder}/"):
                return Attached(whole[len(root) + len(folder) + 2 :], workspace_id)
        cap = min(ATTACH_MAX_BYTES, room)
        try:
            data = await sandbox.adownload_file_bytes_capped(whole, lambda _: cap)
        except FileOverCap:
            limit = (
                f"{ATTACH_MAX_BYTES // _MB} MB"
                if cap == ATTACH_MAX_BYTES
                else f"the {SEND_MAX_BYTES // _MB} MB a send's files hold"
            )
            raise ComputerUnreachable(
                f'{path} on computer "{target.name}" is over {limit}; nothing was sent.'
            ) from None
        if data is None:
            raise ComputerUnreachable(f'There is no file {path} on computer "{target.name}".')
        return _Copy(path, posixpath.basename(whole) or "file", data)

    async def _copy(self, found: Sequence[Attached | _Copy]) -> list[Attached]:
        """Write the copies ``found`` holds into Home's ``sent`` folder."""
        attached: list[Attached] = []
        for item in found:
            if isinstance(item, Attached):
                attached.append(item)
                continue
            sandbox = self._own_session.sandbox
            assert sandbox is not None, "the turn's own computer has no sandbox"
            failed = ComputerUnreachable(f"{item.path} could not be copied into Home to send.")
            claimed = await sandbox.execute_bash_command(_claim_script(item.name, item.data))
            kind, _, name = claimed["stdout"].partition(" ") if claimed["success"] else ("", "", "")
            if kind not in ("same", "new") or not name:
                raise failed
            copy = lexical_path(f"{SENT_DIR}/{name}")
            if kind == "new":
                try:
                    if not await sandbox.aupload_file_bytes(copy, item.data):
                        raise failed
                except BaseException:
                    # The name is this send's alone, the turn's cancel
                    # included. Left, the claim is a file named like the copy
                    # that holds only the mark.
                    try:
                        await sandbox.execute_bash_command(f"rm -f -- {shlex.quote(copy)}")
                    except Exception as e:  # noqa: BLE001 - the send's own error is the one to raise
                        logger.warning("Could not remove the claim on %s: %s", copy, e)
                    raise
            attached.append(Attached(copy, None, len(item.data)))
        return attached

    async def elsewhere(self) -> tuple[tuple[str, str], ...]:
        rows = [
            row
            for row in await get_computer_roster(self._user_id)
            if str(row["computer_id"]) != self._own
        ]
        held = await get_computers_with_folders([str(row["computer_id"]) for row in rows])
        return tuple((_label(row), _root(row)) for row in rows if str(row["computer_id"]) in held)

    async def _wake(self, computer_id: str, name: str) -> Session:
        from src.server.services.workspace_manager import WorkspaceManager

        manager = WorkspaceManager.get_instance()
        row = await get_computer(computer_id)
        if row is not None and row.get("status") == ComputerStatus.ERROR:
            raise ComputerUnreachable(
                f'Computer "{name}" failed and cannot start until the user repairs '
                "or rebuilds it in Computers."
            )
        # Only the row says whether another worker stopped the computer since
        # the last wake; this worker's handle outlives that, and a call on it
        # would bring the machine back up past admission with the row stopped.
        running = row is not None and row.get("status") == ComputerStatus.RUNNING
        live = manager.cached_session_for_computer(row) if running else None
        record = self._record(computer_id)
        wake = record.wake
        if wake is None or (wake.done() and not _woke(wake, live)):
            # Out of the turn's project: a start lays out the bound project's
            # folder, and Home's has no place on this computer.
            wake = record.wake = asyncio.get_running_loop().create_task(
                manager.reach_computer(computer_id, user_id=self._user_id),
                context=context_without_project(),
            )
            _wakes.add(wake)
            wake.add_done_callback(_wakes.discard)
            wake.add_done_callback(partial(_log_failure, computer_id))
        try:
            return await asyncio.shield(wake)
        except Exception as e:
            raise ComputerUnreachable(_why_not(name, e)) from e

    async def in_use(self, target: ComputerTarget) -> None:
        await self._keep_alive(target.computer_id, target.session)

    async def _keep_alive(self, computer_id: str, session: Session) -> None:
        """Count the call as use of the computer, at most once a minute:
        nothing else tells the idle sweep that Home's agent is working there."""
        record = self._record(computer_id)
        now = time.monotonic()
        if now - record.stamped < _KEEP_ALIVE_EVERY_S:
            return
        record.stamped = now
        from src.server.services.workspace_manager import WorkspaceManager

        sandbox = session.sandbox
        if sandbox is None or not sandbox.sandbox_id:
            return
        try:
            await WorkspaceManager.get_instance().keep_computer_alive(
                await get_computer(computer_id), sandbox.sandbox_id, runtime=sandbox.runtime
            )
        except Exception as e:  # noqa: BLE001 - a call never fails for its bookkeeping
            logger.debug("Could not stamp computer %s as in use: %s", computer_id, e)


def turn_computers(
    user_id: str,
    own_computer_id: str | None,
    own_session: Session,
    roster: Sequence[Mapping[str, Any]],
    touched: set[str] | None = None,
) -> TurnComputers | None:
    """The reach for a Chief of Staff build, or None when ``roster``, the
    user's computers as the build read them for ``<activity>`` too, holds
    only the turn's own: then no tool is given the argument. ``touched``
    collects the other computers its calls run on (``TurnComputers.touched``)."""
    if len(roster) < 2:
        return None
    return TurnComputers(user_id, own_computer_id, own_session, touched)
