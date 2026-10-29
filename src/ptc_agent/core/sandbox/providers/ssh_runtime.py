"""The runtime half of the SSH provider: one computer on a machine its owner brings.

The agent runs as the SSH login, in the computer's root folder, with the
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
processes it started.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import json
import posixpath
import re
import stat
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import asyncssh
import structlog

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.providers import _ssh_bootstrap as bootstrap
from ptc_agent.core.sandbox.providers._ssh_authorized_key import (
    admits,
    authorized_key_line,
    converge_script,
)
from ptc_agent.core.sandbox.providers._exec_sessions import ExecSessionRuntime
from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget
from ptc_agent.core.sandbox.providers._ssh_shell import (
    BOOT_KEY_SH,
    SUDO_CHECK,
    check_env,
    computer_dir,
    computers_dir,
    delete_script,
    env_file,
    folder_marked_message,
    framed_script,
    layout_script,
    mark_running_script,
    read_state_script,
    root_dir_error,
    root_dir_script,
    stop_script,
)
from ptc_agent.core.sandbox.providers._ssh_transport import (
    OUTPUT_CAP,
    TRANSPORT_ERRORS,
    FramedResult,
    Link,
    ScriptLost,
    ScriptResult,
    pool,
    run_framed,
    write_atomic,
)
from ptc_agent.core.sandbox.providers.ssh_errors import (
    MACHINE_ID_PATTERN,
    SSHConnectError,
    SSHFolderMarked,
)
from ptc_agent.core.sandbox.runtime import (
    STREAM_CHUNK_BYTES,
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxHostUnavailableError,
    SandboxTransientError,
)

logger = structlog.get_logger(__name__)

SANDBOX_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
SUPPORTED_ARCHES = {
    "x86_64": "x86_64",
    "amd64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
}
SUPPORTED_OS = ("Linux", "Darwin")
# How long a state read stays good enough for a file operation. A command
# reads the state itself, so only SFTP calls lean on this.
_STATE_FRESH_S = 10.0
# Management scripts: state reads, layout, kills.
_SHORT_S = 30
_UPLOAD_CONCURRENCY = 8
# A file call that moves data may take _SHORT_S plus this long per byte, so
# a machine whose file server hangs fails the call rather than holding it.
_SFTP_MIN_RATE = 128 * 1024
# What the server holds of a file the machine sends, so a machine cannot fill
# the memory every other user's turns share. A whole download matches what
# the file relay accepts (RELAY_MAX_BYTES); the env file is never near its cap.
_DOWNLOAD_MAX_BYTES = 256 * 1024 * 1024
_ENV_MAX_BYTES = 1024 * 1024
_LIST_MAX_ENTRIES = 10_000


class _ConnectionLost(SandboxTransientError):
    """The connection died under a call."""


class SSHRuntime(ExecSessionRuntime):
    """Runtime over one computer on an SSH machine, reached through the pool.

    Holds no connection of its own: every call takes the process's pooled
    connection to the machine, dialing it if needed, so building one does no
    I/O and many runtimes of one computer cost one connection.
    """

    # Changes when every process on the machine was ended (see BOOT_KEY_SH).
    session_boot_key_sh = f'$({BOOT_KEY_SH}echo "$__la_k")'
    exec_output_cap = OUTPUT_CAP

    def __init__(
        self,
        *,
        target: SSHTarget,
        sandbox_id: str,
        working_dir: str,
        load_key: Callable[[], Awaitable[str]],
        bootstrap_timeout: int,
        login_from: tuple[str, ...] = (),
    ) -> None:
        if not SANDBOX_ID_RE.match(sandbox_id):
            raise ValueError(f"Invalid SSH computer id: {sandbox_id!r}")
        self._target = target
        self._id = sandbox_id
        self._working_dir = working_dir
        self._load_key = load_key
        self._bootstrap_timeout = bootstrap_timeout
        self._login_from = login_from
        self._state: RuntimeState | None = None
        self._state_read_at = 0.0
        self._state_lock = asyncio.Lock()

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

    async def login_home(self) -> str:
        """The login user's home, which ``~`` names on the machine."""
        link = await self._link()
        assert link.facts is not None
        return link.facts.home

    # -- Connections --

    async def _connect(self) -> Link:
        """The pooled connection, on a machine that can host a computer.

        Raises:
            SSHConnectError: As the pool raises it, or ``unsupported``.
        """
        link = await pool().link(self._target, self._load_key)
        facts = link.facts
        assert facts is not None
        if facts.os not in SUPPORTED_OS or facts.arch not in SUPPORTED_ARCHES:
            raise SSHConnectError(
                "unsupported",
                f"{self._target.host} runs {facts.os} on {facts.arch}; only Linux "
                "or macOS on x86_64 or arm64 can host a computer.",
            )
        return link

    async def _link(self) -> Link:
        """``_connect`` for a call on a running computer: an unreachable
        machine is a transient failure the reconnect path decides about."""
        try:
            return await self._connect()
        except SSHConnectError as e:
            if e.answered:
                raise
            raise SandboxTransientError(str(e)) from e

    async def _lifecycle_link(self) -> Link:
        """``_connect`` for a create, a start or a state read, which name an
        unreachable machine as such."""
        try:
            return await self._connect()
        except SSHConnectError as e:
            if e.answered:
                raise
            raise SandboxHostUnavailableError(self._id, e.for_owner) from e

    def _dir(self, link: Link) -> str:
        assert link.facts is not None
        return computer_dir(link.facts.home, self._id)

    def _lost(self, link: Link, e: BaseException) -> SandboxTransientError:
        if not link.usable or isinstance(e, asyncssh.DisconnectError):
            pool().evict(link)
        elif isinstance(e, asyncssh.ChannelOpenError):
            if link.refusals >= 2:
                # It works but keeps refusing channels: sshd holds sessions
                # of it that we no longer count.
                pool().shed(link)
            return _ConnectionLost(f"{self._target.host} refused a channel: {e}")
        return _ConnectionLost(
            f"Lost the connection to {self._target.host}: {e or type(e).__name__}"
        )

    async def _script(
        self, link: Link, script: str, *, timeout: float = _SHORT_S, root: bool = False
    ) -> ScriptResult:
        """Run a management script, a slow one or a lost connection raised as
        the transient failure a caller may retry."""
        try:
            return await link.run(script, timeout=timeout, root=root)
        except TimeoutError as e:
            raise SandboxTransientError(
                f"{self._target.host} did not finish a command within {int(timeout)} seconds"
            ) from e
        except TRANSPORT_ERRORS as e:
            raise self._lost(link, e) from e

    async def _run(
        self, link: Link, script: str, *, timeout: float = _SHORT_S, root: bool = False
    ) -> str:
        """A management script's stdout; raises on a nonzero exit."""
        result = await self._script(link, script, timeout=timeout, root=root)
        if result.exit_code != 0:
            detail = (result.stderr or result.stdout).decode(errors="replace").strip()
            raise RuntimeError(
                f"Command on {self._target.host} failed (exit {result.exit_code}): {detail}"
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
            await self._ensure_key_line(link)
            await self._ensure_layout(link)
            await self._bootstrap(link, timeout=self._bootstrap_timeout, creating=True)
            await self._write_env(link, dict(env))
            await self._mark_running(link)
        except BaseException as e:
            # Held by the link, so a cancelled create still takes its folder
            # back; any other failure waits for it, since a retry follows.
            cleanup = link.spawn(self._discard())
            if not isinstance(e, asyncio.CancelledError):
                await asyncio.shield(cleanup)
            raise

    async def _discard(self) -> None:
        """Remove a computer whose create failed; best effort, on a new
        connection when the one the create failed on died."""
        try:
            async with asyncio.timeout(_SHORT_S):
                await self._on_live_link(
                    lambda link: self._run(link, delete_script(self._dir(link)))
                )
        except Exception as e:
            logger.warning(
                "Could not remove an SSH computer whose create failed",
                target=self._target.label,
                sandbox_id=self._id,
                error=str(e) or type(e).__name__,
            )

    async def _on_live_link(self, step: Callable[[Link], Awaitable[Any]]) -> Any:
        """Run a start or a state read, again on a new connection if the pooled
        one died under it.

        A connection left idle while the machine went away reads as open until
        its keepalive gives up, and only a new dial says whether the machine
        is there. Both steps are safe to run twice.
        """
        link = await self._lifecycle_link()
        try:
            return await step(link)
        except _ConnectionLost:
            return await step(await self._lifecycle_link())

    async def start(self, timeout: int = 120) -> None:
        await self._on_live_link(lambda link: self._start_on(link, timeout))

    async def _start_on(self, link: Link, timeout: int) -> None:
        # Before the layout, whose folder is what keeps an unmarked root.
        await self._ensure_root(link)
        await self._ensure_key_line(link)
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
        pool().retire(self._target, only_if_idle=True)

    async def delete(self) -> None:
        """Stop the computer and remove its own folder under ``~/.langalpha``.

        The root folder and everything the agent wrote there stay: they are
        the owner's files on the owner's machine. The credentials put there
        for its processes go (the data token, the connector relay's grant,
        the vault secrets, and any MCP check's copy of them), since nothing of
        this computer is left to use them.

        The connection stays open for the removal's own steps after this one,
        which close it (``ssh.removal_link``).
        """
        link = await self._link()
        await self._stop_processes(link)
        await self._run(link, delete_script(self._dir(link), root=self._working_dir))
        self._set_state(RuntimeState.STOPPED)

    async def get_state(self) -> RuntimeState:
        return await self._on_live_link(self._read_state)

    async def _read_state(self, link: Link) -> RuntimeState:
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

    async def _ensure_running(self) -> Link:
        """The connection, once the computer reads as running.

        Commands check the state themselves; file calls go over SFTP, which
        cannot, so they check here, trusting a read from the last few seconds.
        Calls that find it stale at once share one read, since every state
        read on the connection takes its one control slot.
        """
        link = await self._link()
        fresh = time.monotonic() - self._state_read_at < _STATE_FRESH_S
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
        return SandboxTransientError(f"SSH computer {self._id} is stopped, not running")

    async def _ensure_layout(self, link: Link) -> bool:
        """Create the computer's folder; True when its env file is already there."""
        return "has-env" in await self._run(link, layout_script(self._dir(link)))

    async def _mark_running(self, link: Link) -> None:
        """Mark the computer running on this boot, starting its sessions anew.

        After a reboot nothing its records name is alive, so they are dropped,
        and the sessions of the old boot with them.
        """
        out = await self._run(link, mark_running_script(self._dir(link)))
        if "rebooted" in out:
            logger.info(
                "SSH computer's machine rebooted since its last start",
                target=self._target.label,
                sandbox_id=self._id,
            )
        self._set_state(RuntimeState.RUNNING)

    async def _bootstrap(
        self, link: Link, *, timeout: int, creating: bool = False
    ) -> None:
        out = await self._run(link, bootstrap.stamp_check_script())
        if out.strip() == "current":
            return
        started = time.monotonic()
        logger.info(
            "Installing the SSH computer runtime",
            target=self._target.label,
            sandbox_id=self._id,
        )
        result = await self._framed(
            link,
            # Waiting on another install gives up before this call does, so
            # the error says what held it.
            bootstrap.bootstrap_script(lock_wait_s=max(timeout // 2, timeout - 30)),
            timeout=timeout,
            computer=False,
        )
        elapsed = time.monotonic() - started
        output = result.stdout.decode(errors="replace")
        phases = bootstrap.phase_seconds(output)
        if result.timed_out or result.exit_code != 0:
            logger.warning(
                "Installing the SSH computer runtime failed",
                target=self._target.label,
                sandbox_id=self._id,
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
                f"Installing the Python runtime on {self._target.host} took longer "
                f"than {timeout // 60} minutes. {again} the computer again picks it up."
            )
        if result.exit_code != 0:
            tail = "\n".join(
                line
                for line in output.splitlines()
                if not line.startswith(bootstrap.PHASE_MARK)
            ).strip()[-2000:]
            raise RuntimeError(
                f"Installing the Python runtime on {self._target.host} failed "
                f"(exit {result.exit_code}): {tail or result.framing_error}"
            )
        waited = phases.pop("lock", 0.0)
        phases.pop("check", None)
        if not phases:
            logger.info(
                "Another install of the SSH computer runtime finished it",
                target=self._target.label,
                sandbox_id=self._id,
                waited_seconds=waited,
            )
            return
        logger.info(
            "Installed the SSH computer runtime",
            target=self._target.label,
            sandbox_id=self._id,
            seconds=round(elapsed, 1),
            waited_seconds=waited,
            phases=phases,
        )

    async def _ensure_root(self, link: Link) -> None:
        """Make or take the computer's root folder and mark it as this one's.

        Sudo makes it only where the user cannot. See ``root_dir_script``.

        Raises:
            SSHFolderMarked: Another computer's mark is on the folder, named
                as the probe names it so the owner can take the folder back.
            SSHConnectError: ``root_dir``, the folder cannot be the user's.
        """
        assert link.facts is not None
        result = await self._script(
            link,
            root_dir_script(
                self._working_dir,
                computers=computers_dir(link.facts.home),
                computer_id=self._id,
            ),
        )
        if result.exit_code is None:
            raise self._lost(
                link, ConnectionError("the command ended without a status")
            )
        if result.exit_code == 15:
            raise SandboxTransientError(
                f"{self._target.host} is still setting up another computer's folder"
            )
        if result.exit_code == 10:
            owner = result.stdout.decode(errors="replace").strip()
            if re.match(MACHINE_ID_PATTERN, owner):
                raise SSHFolderMarked(
                    folder_marked_message(self._working_dir, self._target.host),
                    owner,
                )
        error = root_dir_error(
            result.exit_code,
            path=self._working_dir,
            user=self._target.username,
            host=self._target.host,
        )
        if error is not None:
            raise error

    async def _ensure_key_line(self, link: Link) -> None:
        """Bring the machine's line for the client key to the current one.

        Only where the deployment names ``login_from``: without it the line
        admits any address, and a line the owner narrowed by hand is theirs
        to keep. A line that would not admit this very connection is left as
        it is, so a ``login_from`` missing the address the deployment now
        connects from never locks it out. Best effort: the owner may keep the
        file read only, and the computer starts all the same.
        """
        if not self._login_from:
            return
        assert link.facts is not None
        seen_from = link.facts.client_address
        if not admits(self._login_from, seen_from):
            logger.warning(
                "SSH login_from does not include the address the machine sees "
                "this deployment at; its key line was left as it is",
                target=self._target.label,
                sandbox_id=self._id,
                seen_from=seen_from or None,
            )
            return
        try:
            public_key = (
                asyncssh.import_private_key(await self._load_key())
                .export_public_key("openssh")
                .decode()
            )
            outcome = (
                await self._run(
                    link,
                    converge_script(authorized_key_line(public_key, self._login_from)),
                )
            ).strip()
        except SandboxTransientError:
            # A lost connection is the start's to retry.
            raise
        except (RuntimeError, ValueError, asyncssh.KeyImportError) as e:
            logger.warning(
                "Could not bring the SSH key line up to date",
                target=self._target.label,
                sandbox_id=self._id,
                error=str(e) or type(e).__name__,
            )
            return
        if outcome == "updated":
            logger.info(
                "Brought the SSH key line up to date",
                target=self._target.label,
                sandbox_id=self._id,
            )

    async def _stop_processes(self, link: Link) -> None:
        d = self._dir(link)
        out = await self._run(
            link, stop_script(d, root=False, computer_root=self._working_dir)
        )
        if "has-root" in out and await self._has_sudo(link):
            await self._run(link, stop_script(d, root=True), root=True)

    # -- Env --

    async def _read_env(self, link: Link) -> dict[str, str]:
        try:
            raw = await self._read_bytes(
                link, f"{self._dir(link)}/env.json", cap=_ENV_MAX_BYTES
            )
        except FileNotFoundError:
            return {}
        except OSError as e:
            if e.errno != errno.EFBIG:
                raise
            # Read as unreadable, as a malformed one is: the computer starts
            # with its own env only.
            logger.warning(
                "SSH computer's env file is too large to read",
                target=self._target.label,
                sandbox_id=self._id,
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

    async def _write_env(self, link: Link, env: dict[str, str]) -> None:
        env = check_env(env)
        facts = link.facts
        assert facts is not None
        d = self._dir(link)
        script = env_file(env, facts.home, self._working_dir)
        async with self._sftp(link, d) as sftp:
            # Both hold the computer's env, which may carry credentials.
            await write_atomic(
                sftp, f"{d}/env.json", json.dumps(env).encode(), mode=0o600
            )
            await write_atomic(sftp, f"{d}/env.sh", script.encode(), mode=0o600)

    # -- Execution --

    async def _framed(
        self,
        link: Link,
        command: str,
        *,
        timeout: float,
        env: Mapping[str, str] | None = None,
        root: bool = False,
        merge: bool = True,
        computer: bool = True,
    ) -> FramedResult:
        """Run *command* framed, dialing again once if the channel never opened.

        It runs in the computer's root, or with ``computer`` False outside the
        computer altogether, as the install does. A command that may have
        reached the machine is never sent twice.
        """
        for attempt in range(2):
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
            try:
                result = await run_framed(
                    link, script, computer_dir=d, timeout=timeout, root=root
                )
            except ScriptLost as e:
                pool().evict(link)
                raise SandboxTransientError(
                    f"Lost the connection to {self._target.host} while a command "
                    "ran; it may still be running there."
                ) from e
            except TRANSPORT_ERRORS as e:
                if attempt:
                    raise self._lost(link, e) from e
                refused = link.usable and isinstance(e, asyncssh.ChannelOpenError)
                if refused and link.refusals < 2:
                    # The connection is fine but refused a channel: sshd allows
                    # fewer sessions than we assume. A moment frees one.
                    await asyncio.sleep(0.5)
                    continue
                if refused:
                    pool().shed(link)
                else:
                    pool().evict(link)
                link = await self._link()
                continue
            if computer and result.started:
                # It found the computer running before it began.
                self._saw_running(began)
            return result
        raise AssertionError("unreachable")  # pragma: no cover

    def _outcome(
        self, result: FramedResult, *, timeout: float, what: str
    ) -> tuple[str, str, int]:
        """A framed run's stdout, stderr and exit code, as exec
        reports them.

        Raises:
            SandboxTransientError: The computer is marked stopped.
        """
        if result.stopped:
            self._set_state(RuntimeState.STOPPED)
            raise self._not_running()
        stdout = _text(result)
        stderr = result.stderr.decode(errors="replace")
        if result.timed_out and result.started:
            return (
                stdout,
                f"{stderr}timeout: the {what} ran past {timeout}s and was stopped",
                -1,
            )
        if result.timed_out:
            # The script went out but never reached the command.
            return (
                stdout,
                f"timeout: the {what} did not start within {timeout}s",
                -1,
            )
        if not result.started:
            return (
                stdout,
                result.framing_error or f"the {what} could not be started",
                result.exit_code or -1,
            )
        return stdout, stderr, -1 if result.exit_code is None else result.exit_code

    async def exec(
        self, command: str, timeout: int = 60, *, env: dict[str, str] | None = None
    ) -> ExecResult:
        return await self._exec(command, timeout, env=env, root=False)

    async def _exec(
        self, command: str, timeout: int, *, env: dict[str, str] | None, root: bool
    ) -> ExecResult:
        link = await self._link()
        result = await self._framed(link, command, timeout=timeout, env=env, root=root)
        stdout, stderr, exit_code = self._outcome(
            result, timeout=timeout, what="command"
        )
        return ExecResult(stdout=stdout, stderr=stderr, exit_code=exit_code)

    async def exec_as_root(
        self, command: str, timeout: int = 60, env: dict[str, str] | None = None
    ) -> ExecResult:
        link = await self._link()
        assert link.facts is not None
        if link.facts.uid == 0:
            return await self.exec(command, timeout, env=env)
        if not await self._has_sudo(link):
            if link.facts.sudo_needs_tty:
                raise RuntimeError(
                    f"Running this as root on {self._target.host} needs sudo, and "
                    f"sudo for {self._target.username} requires a terminal "
                    "(requiretty in sudoers), which commands here do not have."
                )
            raise RuntimeError(
                f"Running this as root on {self._target.host} needs passwordless "
                f"sudo for {self._target.username}, which is not set up."
            )
        return await self._exec(command, timeout, env=env, root=True)

    async def _has_sudo(self, link: Link) -> bool:
        facts = link.facts
        assert facts is not None
        if facts.uid == 0:
            return True
        if facts.has_sudo is None:
            result = await self._script(link, SUDO_CHECK)
            facts.has_sudo = result.exit_code == 0
            facts.sudo_needs_tty = not facts.has_sudo and b"tty" in result.stdout
        return facts.has_sudo

    # -- File I/O --

    @contextlib.asynccontextmanager
    async def _sftp(
        self, link: Link, path: str, *, timeout: float | None = _SHORT_S
    ) -> AsyncIterator[asyncssh.SFTPClient]:
        """The connection's SFTP client, its errors raised as the runtime's.

        *timeout* bounds the whole block; None leaves each call in it to
        bound its own. A channel that died or let a call run past its
        deadline is forgotten by the client it was, so the next call opens
        another while a sibling's new one is kept. One that would not open
        sheds the connection, which may be holding it half open.
        """
        sftp: asyncssh.SFTPClient | None = None
        try:
            async with link.in_use():
                sftp = await link.sftp()
                async with asyncio.timeout(timeout):
                    yield sftp
        except TimeoutError as e:
            if sftp is None:
                pool().shed(link)
            else:
                link.drop_sftp(sftp)
            raise SandboxTransientError(
                f"{self._target.host} did not answer a file call on {path} in time"
            ) from e
        except asyncssh.SFTPNoSuchFile as e:
            raise FileNotFoundError(path) from e
        except asyncssh.SFTPPermissionDenied as e:
            raise PermissionError(f"Permission denied: {path}") from e
        except (asyncssh.SFTPConnectionLost, asyncssh.SFTPNoConnection) as e:
            if sftp is not None:
                link.drop_sftp(sftp)
            raise self._lost(link, e) from e
        except TRANSPORT_ERRORS as e:
            raise self._lost(link, e) from e

    async def _read_bytes(self, link: Link, path: str, *, cap: int) -> bytes:
        """The file at *path*, refused past *cap* bytes.

        asyncssh sizes a whole read by the file's stat, so an uncapped one
        holds whatever size the machine claims, in the memory every other
        user's turns share.

        Raises:
            OSError: ``EFBIG``, the file is larger than *cap*.
        """
        async with self._sftp(link, path) as sftp:
            size = (await sftp.stat(path)).size or 0
        if size > cap:
            raise _too_large(path, size, cap)
        async with self._sftp(link, path, timeout=_transfer_s(size)) as sftp:
            async with await sftp.open(path, "rb", encoding=None) as f:
                data = await f.read(cap + 1)
        if len(data) > cap:
            raise _too_large(path, len(data), cap)
        return data

    def _upload_mode(self, dest: str) -> int | None:
        """0600 under ``_internal``, which holds the computer's credentials, so
        other accounts on the machine cannot read them; the default elsewhere."""
        internal = posixpath.join(self._working_dir, SandboxLayout.INTERNAL_DIR, "")
        return 0o600 if posixpath.normpath(dest).startswith(internal) else None

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        link = await self._ensure_running()
        timeout = _transfer_s(len(content))
        async with self._sftp(link, dest_path, timeout=timeout) as sftp:
            await write_atomic(
                sftp, dest_path, bytes(content), mode=self._upload_mode(dest_path)
            )

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        if not files:
            return
        link = await self._ensure_running()
        gate = asyncio.Semaphore(_UPLOAD_CONCURRENCY)

        async def one(source: bytes | str, dest: str) -> None:
            if isinstance(source, str):
                source = await asyncio.to_thread(_read_local, source)
            timeout = _transfer_s(len(source))
            async with gate, self._sftp(link, dest, timeout=timeout) as sftp:
                await write_atomic(sftp, dest, source, mode=self._upload_mode(dest))

        async with link.in_use():
            await asyncio.gather(*(one(src, dest) for src, dest in files))

    async def download_file(self, path: str) -> bytes:
        link = await self._ensure_running()
        return await self._read_bytes(link, path, cap=_DOWNLOAD_MAX_BYTES)

    async def download_file_stream(self, path: str) -> AsyncIterator[bytes]:
        link = await self._ensure_running()
        # Each call is bounded on its own: a deadline around the stream would
        # also run while the reader holds a chunk.
        async with self._sftp(link, path, timeout=None) as sftp:
            async with asyncio.timeout(_SHORT_S):
                f = await sftp.open(path, "rb", encoding=None)
            try:
                offset = 0
                while True:
                    async with asyncio.timeout(_transfer_s(STREAM_CHUNK_BYTES)):
                        chunk = await f.read(STREAM_CHUNK_BYTES, offset)
                    if not chunk:
                        return
                    offset += len(chunk)
                    yield chunk
            finally:
                with contextlib.suppress(Exception):
                    async with asyncio.timeout(_SHORT_S):
                        await f.close()

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        link = await self._ensure_running()
        entries: list[dict[str, Any]] = []
        async with self._sftp(link, directory) as sftp:
            # Read as the entries come, and cut off: a machine's file server
            # could send them without end.
            async with contextlib.aclosing(sftp.scandir(directory)) as names:
                async for name in names:
                    if name.filename in (".", ".."):
                        continue
                    if len(entries) == _LIST_MAX_ENTRIES:
                        logger.warning(
                            "SSH directory listing cut short",
                            target=self._target.label,
                            sandbox_id=self._id,
                            entries=_LIST_MAX_ENTRIES,
                        )
                        break
                    entries.append(_entry(name))
        return entries

    # -- Preview URLs --

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        """A loopback port on this server that reaches *port* on the machine.

        The forward belongs to this process's connection, so the URL works
        only here and only while the connection lasts; the server proxies it
        and asks again for each request.
        """
        if not 1 <= port <= 65535:
            raise ValueError(f"Invalid port: {port}")
        link = await self._link()
        try:
            async with link.in_use():
                local = await link.forward(port)
        except TRANSPORT_ERRORS as e:
            raise self._lost(link, e) from e
        return PreviewInfo(
            url=f"http://127.0.0.1:{local}",
            token=None,
            browser_ready=False,
            process_local=True,
        )

    async def get_preview_link(self, port: int) -> PreviewInfo:
        return await self.get_preview_url(port)

    # -- Metadata --

    async def get_metadata(self) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "id": self._id,
            "working_dir": self._working_dir,
            "provider": "ssh",
            "host": self._target.host,
            "port": self._target.port,
            "user": self._target.username,
        }
        try:
            meta["state"] = (await self.get_state()).value
        except Exception:
            meta["state"] = self._state.value if self._state else "unknown"
        link = pool().peek(self._target)
        if link is not None and link.facts is not None:
            meta["os"] = link.facts.os
            meta["arch"] = link.facts.arch
        return meta


def _text(result: FramedResult) -> str:
    text = result.stdout.decode(errors="replace")
    if result.stdout_bytes > OUTPUT_CAP:
        text += (
            f"\n[output truncated: kept the first {OUTPUT_CAP // (1024 * 1024)} MiB "
            f"of {result.stdout_bytes} bytes]"
        )
    return text


def _transfer_s(size: int) -> float:
    return _SHORT_S + size / _SFTP_MIN_RATE


def _too_large(path: str, size: int, cap: int) -> OSError:
    return OSError(
        errno.EFBIG,
        f"File too large to read here ({size} bytes; the limit is {cap})",
        path,
    )


def _read_local(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _entry(name: Any) -> dict[str, Any]:
    """One directory entry in the keys E2B's ``list_files`` returns."""
    attrs = name.attrs
    mode = attrs.permissions or 0
    # The longname is ls -l output: perms, links, owner, group, ...
    fields = (name.longname or "").split()
    owner = (
        fields[2]
        if len(fields) > 3
        else (str(attrs.uid) if attrs.uid is not None else "")
    )
    group = (
        fields[3]
        if len(fields) > 3
        else (str(attrs.gid) if attrs.gid is not None else "")
    )
    return {
        "name": name.filename,
        "is_dir": stat.S_ISDIR(mode),
        "size": attrs.size or 0,
        "mode": stat.S_IMODE(mode),
        "permissions": stat.filemode(mode)[1:],
        "owner": owner,
        "group": group,
        "mod_time": datetime.fromtimestamp(attrs.mtime, UTC).isoformat()
        if attrs.mtime is not None
        else None,
    }
