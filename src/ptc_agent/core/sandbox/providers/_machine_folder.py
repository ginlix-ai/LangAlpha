"""A computer as a folder on a machine its user already has: one brought over
SSH, or the one the server runs on.

The agent runs as the machine's user, in the computer's root folder, with the
Python runtime the bootstrap installs under ``~/.langalpha``. A computer's own
state sits beside it in ``~/.langalpha/computers/<id>/``:

- ``state``: ``running <boot key>`` or ``stopped``. Read on every command, so
  a stop in one worker reaches every other at once, and a ``running`` written
  before the machine last rebooted reads as stopped.
- ``env.sh`` and ``env.json``: the computer's env, sourced by every command.
- ``execs/``: one record per command whose process group may still be alive.
- ``sessions/``: background sessions, as ``ExecSessionRuntime`` keeps them.

The machine is the owner's, so nothing here touches anything outside that
folder and the computer's root, and stopping a computer ends only the
processes it started. ``MachineFolderRuntime`` keeps that lifecycle; a
transport supplies how a script, a framed command and a file reach the machine.
"""

from __future__ import annotations

import asyncio
import errno
import json
import posixpath
import re
import time
from abc import abstractmethod
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from typing import Any, ClassVar, Generic, TypeVar

import structlog

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.providers import _ssh_bootstrap as bootstrap
from ptc_agent.core.sandbox.providers._exec_sessions import ExecSessionRuntime
from ptc_agent.core.sandbox.providers._script_run import ScriptResult
from ptc_agent.core.sandbox.providers._ssh_root import refuse_root, root_dir_script
from ptc_agent.core.sandbox.providers._ssh_shell import (
    BOOT_KEY_SH,
    OUTPUT_CAP,
    FramedResult,
    check_env,
    computer_dir,
    computers_dir,
    delete_script,
    env_file,
    framed_script,
    layout_script,
    mark_running_script,
    read_state_script,
    stop_script,
)
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxTransientError,
)

logger = structlog.get_logger(__name__)

SANDBOX_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
# How long a state read stays good enough for a file operation. A command
# reads the state itself, so only file calls lean on this.
STATE_FRESH_S = 10.0
# Management scripts: state reads, layout, kills.
SHORT_S = 30
UPLOAD_CONCURRENCY = 8
# What the server holds of a file the machine sends, so a machine cannot fill
# the memory every other user's turns share. A whole download matches what
# the file relay accepts (RELAY_MAX_BYTES); the env file is never near its cap.
DOWNLOAD_MAX_BYTES = 256 * 1024 * 1024
ENV_MAX_BYTES = 1024 * 1024
LIST_MAX_ENTRIES = 10_000

# What reaches the machine: an SSH ``Link``, or None on the server's own.
L = TypeVar("L")


def too_large(path: str, size: int, cap: int) -> OSError:
    return OSError(
        errno.EFBIG,
        f"File too large to read here ({size} bytes; the limit is {cap})",
        path,
    )


class MachineFolderRuntime(ExecSessionRuntime, Generic[L]):
    """Runtime over one computer folder on a machine. Holds nothing that needs
    closing, so building one does no I/O."""

    # Changes when every process on the machine was ended (see BOOT_KEY_SH).
    session_boot_key_sh = f'$({BOOT_KEY_SH}echo "$__la_k")'
    exec_output_cap = OUTPUT_CAP
    says_config_home = True
    # What logs and errors call a computer of this kind, and its provider kind.
    noun: ClassVar[str]
    provider: ClassVar[str]

    def __init__(
        self, *, sandbox_id: str, working_dir: str, bootstrap_timeout: int
    ) -> None:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError(f"Invalid {self.noun} id: {sandbox_id!r}")
        self._id = sandbox_id
        self._working_dir = working_dir
        self._bootstrap_timeout = bootstrap_timeout
        self._state: RuntimeState | None = None
        self._state_read_at = 0.0
        self._state_lock = asyncio.Lock()

    # -- What the transport supplies --

    @property
    @abstractmethod
    def _host(self) -> str:
        """The machine as a sentence names it to its owner."""

    @abstractmethod
    async def _link(self) -> L:
        """What reaches the machine, for a call on a running computer."""

    @abstractmethod
    async def _lifecycle_link(self) -> L:
        """What reaches the machine, for a create, a start or a state read."""

    async def _on_live_link(self, step: Callable[[L], Awaitable[Any]]) -> Any:
        """Run a start, a state read or a create's undoing, each safe to run twice."""
        return await step(await self._lifecycle_link())

    @abstractmethod
    def _home(self, link: L) -> str: ...

    @abstractmethod
    def _user(self, link: L) -> str: ...

    @abstractmethod
    def _hold(self, link: L, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Run *coro* in the background, held until it ends."""

    @abstractmethod
    async def _script(
        self, link: L, script: str, *, timeout: float = SHORT_S, root: bool = False
    ) -> ScriptResult:
        """Run a management script to its end, a slow one raised as the
        transient failure a caller may retry."""

    @abstractmethod
    async def _send_framed(
        self, link: L, script: str, *, computer_dir: str, timeout: float, root: bool
    ) -> FramedResult:
        """Run a ``framed_script``; on timeout or cancel, end its group."""

    @abstractmethod
    async def _read_bytes(self, link: L, path: str, *, cap: int) -> bytes:
        """The file at *path*, refused past *cap* bytes.

        Raises:
            OSError: ``EFBIG``, the file is larger than *cap*.
        """

    @abstractmethod
    async def _write_private(
        self, link: L, directory: str, files: Mapping[str, bytes]
    ) -> None:
        """Write each file in *directory* whole, readable by its user alone."""

    def _install_groups(self) -> frozenset[str] | None:
        """What the runtime installs; None for the defaults."""
        return None

    async def _prepare(self, link: L) -> None:
        """Bring the machine's own setup up to date, once the root is taken."""

    def _machine_facts(self) -> dict[str, Any]:
        """What ``get_metadata`` says of the machine beside the computer."""
        return {}

    def _log_fields(self) -> dict[str, Any]:
        return {"sandbox_id": self._id}

    @property
    def _subject(self) -> str:
        """``noun`` as a sentence starts with it."""
        return self.noun[:1].upper() + self.noun[1:]

    # -- Properties --

    @property
    def id(self) -> str:
        return self._id

    @property
    def working_dir(self) -> str:
        return self._working_dir

    @property
    def image_prebuilt(self) -> bool:
        # Create and start install the runtime before anything else runs.
        return True

    @property
    def _sessions_root(self) -> str:
        # Every command's script sets __la_dir to this computer's own folder.
        return "$__la_dir/sessions"

    def _dir(self, link: L) -> str:
        return computer_dir(self._home(link), self._id)

    async def _run(
        self, link: L, script: str, *, timeout: float = SHORT_S, root: bool = False
    ) -> str:
        """A management script's stdout; raises on a nonzero exit."""
        result = await self._script(link, script, timeout=timeout, root=root)
        if result.exit_code != 0:
            detail = (result.stderr or result.stdout).decode(errors="replace").strip()
            raise RuntimeError(
                f"Command on {self._host} failed (exit {result.exit_code}): {detail}"
            )
        return result.stdout.decode(errors="replace")

    # -- Lifecycle --

    async def create(self, env: Mapping[str, str]) -> None:
        """Lay the computer out on the machine, install the runtime, mark it running.

        A create that fails after taking the root takes its folder off the
        machine again: a root whose mark names a computer with a folder is
        refused as that computer's, and the retry comes with a new id.
        """
        link = await self._lifecycle_link()
        # First: a folder the user cannot have fails before anything is
        # written, and before the install, which takes minutes. Taking it
        # makes the computer's folder, so whatever fails after is undone.
        await self._ensure_root(link)
        try:
            await self._prepare(link)
            await self._ensure_layout(link)
            await self._bootstrap(link, timeout=self._bootstrap_timeout, creating=True)
            await self._write_env(link, dict(env))
            await self._mark_running(link)
        except BaseException as e:
            # Held, so a cancelled create still takes its folder back; any
            # other failure waits for it, since a retry follows.
            cleanup = self._hold(link, self._discard())
            if not isinstance(e, asyncio.CancelledError):
                await asyncio.shield(cleanup)
            raise

    async def _discard(self) -> None:
        """Remove a computer whose create failed; best effort."""
        try:
            async with asyncio.timeout(SHORT_S):
                await self._on_live_link(
                    lambda link: self._run(link, delete_script(self._dir(link)))
                )
        except Exception as e:
            logger.warning(
                "Could not remove a computer whose create failed",
                error=str(e) or type(e).__name__,
                **self._log_fields(),
            )

    async def start(self, timeout: int = 120) -> None:
        await self._on_live_link(lambda link: self._start_on(link, timeout))

    async def _start_on(self, link: L, timeout: int) -> None:
        # Before the layout, whose folder is what keeps an unmarked root.
        await self._ensure_root(link)
        await self._prepare(link)
        has_env = await self._ensure_layout(link)
        await self._bootstrap(link, timeout=max(timeout, self._bootstrap_timeout))
        # Rendered again each start, so the lines the runtime owns follow it
        # when they change. A folder that went missing (or was never
        # finished) took its env with it: the computer starts with its own only.
        await self._write_env(link, await self._read_env(link) if has_env else {})
        await self._mark_running(link)

    async def stop(self, timeout: int = 60, *, force: bool = False) -> None:
        link = await self._link()
        await self._stop_processes(link)
        self._set_state(RuntimeState.STOPPED)

    async def delete(self) -> None:
        """Stop the computer and remove its own folder under ``~/.langalpha``.

        The root folder and everything the agent wrote there stay: they are
        the owner's files on the owner's machine. The credentials put there
        for its processes go (the data token, the connector relay's grant,
        the vault secrets, and any MCP check's copy of them), since nothing of
        this computer is left to use them.
        """
        link = await self._link()
        await self._stop_processes(link)
        await self._run(link, delete_script(self._dir(link), root=self._working_dir))
        self._set_state(RuntimeState.STOPPED)

    async def get_state(self) -> RuntimeState:
        return await self._on_live_link(self._read_state)

    async def _read_state(self, link: L) -> RuntimeState:
        """Running only when marked so on the machine's current boot.

        A missing folder reads as stopped, so a start lays it out again, and
        so does a machine rebooted since the start, so the next start runs.
        """
        out = await self._run(link, read_state_script(self._dir(link)))
        state = RuntimeState.RUNNING if out.strip() == "1" else RuntimeState.STOPPED
        self._set_state(state)
        return state

    def _set_state(self, state: RuntimeState) -> None:
        self._state = state
        self._state_read_at = time.monotonic()

    def _saw_running(self, at: float) -> None:
        """Running as of *at*, unless something seen since says otherwise."""
        if at > self._state_read_at:
            self._state = RuntimeState.RUNNING
            self._state_read_at = at

    async def _ensure_running(self) -> L:
        """What reaches the machine, once the computer reads as running.

        Commands check the state themselves; file calls cannot, so they check
        here, trusting a read from the last few seconds. Calls that find it
        stale at once share one read.
        """
        link = await self._link()
        fresh = time.monotonic() - self._state_read_at < STATE_FRESH_S
        if not (fresh and self._state is RuntimeState.RUNNING):
            seen = self._state_read_at
            async with self._state_lock:
                if self._state_read_at == seen:
                    await self._read_state(link)
        if self._state is not RuntimeState.RUNNING:
            raise self._not_running()
        return link

    def _not_running(self) -> SandboxTransientError:
        # Transient, so a caller with a reconnect path starts it.
        return SandboxTransientError(
            f"{self._subject} {self._id} is stopped, not running"
        )

    async def _ensure_layout(self, link: L) -> bool:
        """Create the computer's folder; True when its env file is already there."""
        return "has-env" in await self._run(link, layout_script(self._dir(link)))

    async def _mark_running(self, link: L) -> None:
        """Mark the computer running on this boot, starting its sessions anew.

        After a reboot nothing its records name is alive, so they are dropped,
        and the sessions of the old boot with them.
        """
        out = await self._run(link, mark_running_script(self._dir(link)))
        if "rebooted" in out:
            logger.info(
                f"{self._subject}'s machine rebooted since its last start",
                **self._log_fields(),
            )
        self._set_state(RuntimeState.RUNNING)

    async def _bootstrap(
        self, link: L, *, timeout: int, creating: bool = False
    ) -> None:
        groups = self._install_groups()
        out = await self._run(link, bootstrap.stamp_check_script(groups))
        if out.strip() == "current":
            return
        started = time.monotonic()
        logger.info(
            f"Installing the {self.noun} runtime",
            **self._log_fields(),
            **({} if groups is None else {"groups": sorted(groups)}),
        )
        result = await self._framed(
            link,
            # Waiting on another install gives up before this call does, so
            # the error says what held it.
            bootstrap.bootstrap_script(
                lock_wait_s=max(timeout // 2, timeout - 30), groups=groups
            ),
            timeout=timeout,
            computer=False,
        )
        elapsed = time.monotonic() - started
        output = result.stdout.decode(errors="replace")
        phases = bootstrap.phase_seconds(output)
        if result.timed_out or result.exit_code != 0:
            logger.warning(
                f"Installing the {self.noun} runtime failed",
                **self._log_fields(),
                exit_code=result.exit_code,
                timed_out=result.timed_out,
                phases=phases,
                failed_in=bootstrap.last_phase(output),
            )
        if result.timed_out:
            # The install keeps the layers it finished, and a failed create
            # leaves no computer to start.
            again = "Adding" if creating else "Starting"
            raise RuntimeError(
                f"Installing the Python runtime on {self._host} took longer "
                f"than {timeout // 60} minutes. {again} the computer again picks it up."
            )
        if result.exit_code != 0:
            tail = bootstrap.failure_tail(output)
            raise RuntimeError(
                f"Installing the Python runtime on {self._host} failed "
                f"(exit {result.exit_code}): {tail or result.framing_error}"
            )
        waited = phases.pop("lock", 0.0)
        phases.pop("check", None)
        if not phases:
            logger.info(
                f"Another install of the {self.noun} runtime finished it",
                **self._log_fields(),
                waited_seconds=waited,
            )
            return
        logger.info(
            f"Installed the {self.noun} runtime",
            **self._log_fields(),
            seconds=round(elapsed, 1),
            waited_seconds=waited,
            phases=phases,
        )

    async def _ensure_root(self, link: L) -> None:
        """Make or take the computer's root folder and mark it as this one's.
        See ``root_dir_script``.

        Raises:
            SSHFolderMarked: Another computer's mark is on the folder.
            SSHConnectError: ``root_dir``, the folder cannot be the user's.
        """
        result = await self._script(
            link,
            root_dir_script(
                self._working_dir,
                computers=computers_dir(self._home(link)),
                computer_id=self._id,
            ),
        )
        self._refuse_root(link, result)

    def _refuse_root(self, link: L, result: ScriptResult) -> None:
        refuse_root(
            result.exit_code,
            result.stdout,
            path=self._working_dir,
            user=self._user(link),
            host=self._host,
        )

    async def _stop_processes(self, link: L) -> str:
        return await self._run(
            link,
            stop_script(self._dir(link), root=False, computer_root=self._working_dir),
        )

    # -- Env --

    async def _read_env(self, link: L) -> dict[str, str]:
        try:
            raw = await self._read_bytes(
                link, f"{self._dir(link)}/env.json", cap=ENV_MAX_BYTES
            )
        except FileNotFoundError:
            return {}
        except OSError as e:
            if e.errno != errno.EFBIG:
                raise
            # Read as unreadable, as a malformed one is: the computer starts
            # with its own env only.
            logger.warning(
                f"{self._subject}'s env file is too large to read",
                **self._log_fields(),
            )
            return {}
        try:
            data = json.loads(raw)
        except ValueError:
            return {}
        return (
            {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}
        )

    async def replace_env(self, env: dict[str, str]) -> bool:
        link = await self._ensure_running()
        if await self._read_env(link) == env:
            return False
        await self._write_env(link, env)
        return True

    async def _write_env(self, link: L, env: dict[str, str]) -> None:
        env = check_env(env)
        script = env_file(env, self._home(link), self._working_dir)
        # Both hold the computer's env, which may carry credentials.
        await self._write_private(
            link,
            self._dir(link),
            {"env.json": json.dumps(env).encode(), "env.sh": script.encode()},
        )

    # -- Execution --

    async def _framed(
        self,
        link: L,
        command: str,
        *,
        timeout: float,
        env: Mapping[str, str] | None = None,
        root: bool = False,
        merge: bool = True,
        computer: bool = True,
    ) -> FramedResult:
        """Run *command* in a group of its own, in the computer's root, or
        with ``computer`` False outside the computer altogether, as the
        install does."""
        d = self._dir(link)
        script = framed_script(
            command,
            computer_dir=d,
            cwd=self._working_dir if computer else None,
            env=env,
            computer=computer,
            merge=merge,
            root=root,
        )
        began = time.monotonic()
        result = await self._send_framed(
            link, script, computer_dir=d, timeout=timeout, root=root
        )
        if computer and result.started:
            # It found the computer running before it began.
            self._saw_running(began)
        return result

    async def exec(
        self, command: str, timeout: int = 60, *, env: dict[str, str] | None = None
    ) -> ExecResult:
        return await self._exec(command, timeout, env=env, root=False)

    async def _exec(
        self, command: str, timeout: int, *, env: dict[str, str] | None, root: bool
    ) -> ExecResult:
        link = await self._link()
        result = await self._framed(link, command, timeout=timeout, env=env, root=root)
        if result.stopped:
            self._set_state(RuntimeState.STOPPED)
            raise self._not_running()
        stdout, stderr, exit_code = result.outcome(timeout=timeout, what="command")
        return ExecResult(stdout=stdout, stderr=stderr, exit_code=exit_code)

    # -- File I/O --

    def _upload_mode(self, dest: str) -> int | None:
        """0600 under ``_internal``, which holds the computer's credentials, so
        other accounts on the machine cannot read them; the default elsewhere."""
        internal = posixpath.join(self._working_dir, SandboxLayout.INTERNAL_DIR, "")
        return 0o600 if posixpath.normpath(dest).startswith(internal) else None

    async def download_file(self, path: str) -> bytes:
        link = await self._ensure_running()
        return await self._read_bytes(link, path, cap=DOWNLOAD_MAX_BYTES)

    # -- Preview URLs --

    async def get_preview_link(self, port: int) -> PreviewInfo:
        return await self.get_preview_url(port)

    # -- Metadata --

    async def get_metadata(self) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "id": self._id,
            "working_dir": self._working_dir,
            "provider": self.provider,
        }
        try:
            meta["state"] = (await self.get_state()).value
        except Exception:
            meta["state"] = self._state.value if self._state else "unknown"
        meta.update(self._machine_facts())
        return meta
