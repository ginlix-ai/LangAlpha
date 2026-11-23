"""The runtime half of the SSH provider: one computer on a machine its owner brings.

The computer's folder and lifecycle are ``MachineFolderRuntime``'s; this is
how they reach the machine: the process's pooled connection, its SFTP channel
for files, sudo where a command needs root, and the machine's key line.
"""

from __future__ import annotations

import asyncio
import contextlib
import stat
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping
from datetime import UTC, datetime
from typing import Any

import asyncssh
import structlog

from ptc_agent.core.sandbox.providers._machine_folder import (
    LIST_MAX_ENTRIES,
    SHORT_S,
    UPLOAD_CONCURRENCY,
    MachineFolderRuntime,
    too_large,
)
from ptc_agent.core.sandbox.providers._ssh_authorized_key import (
    admits,
    authorized_key_line,
    converge_script,
)
from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget
from ptc_agent.core.sandbox.providers._ssh_pool import pool
from ptc_agent.core.sandbox.providers._ssh_shell import (
    SUDO_CHECK,
    hold_awake_script,
    stop_script,
)
from ptc_agent.core.sandbox.providers._ssh_transport import (
    TRANSPORT_ERRORS,
    FramedResult,
    Link,
    ScriptLost,
    ScriptResult,
    run_framed,
    write_atomic,
)
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from ptc_agent.core.sandbox.runtime import (
    STREAM_CHUNK_BYTES,
    ExecResult,
    PreviewInfo,
    SandboxHostUnavailableError,
    SandboxTransientError,
)

logger = structlog.get_logger(__name__)

SUPPORTED_ARCHES = {
    "x86_64": "x86_64",
    "amd64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
}
SUPPORTED_OS = ("Linux", "Darwin")
# A file call that moves data may take SHORT_S plus this long per byte, so
# a machine whose file server hangs fails the call rather than holding it.
_SFTP_MIN_RATE = 128 * 1024


class _ConnectionLost(SandboxTransientError):
    """The connection died under a call."""


class SSHRuntime(MachineFolderRuntime[Link]):
    """Runtime over one computer on an SSH machine, reached through the pool.

    Holds no connection of its own: every call takes the process's pooled
    connection to the machine, dialing it if needed, so building one does no
    I/O and many runtimes of one computer cost one connection. A ``delete``
    leaves the connection open for the removal's own steps after it, which
    close it (``ssh.removal_link``).
    """

    noun = "SSH computer"
    provider = "ssh"

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
        super().__init__(
            sandbox_id=sandbox_id,
            working_dir=working_dir,
            bootstrap_timeout=bootstrap_timeout,
        )
        self._target = target
        self._load_key = load_key
        self._login_from = login_from

    @property
    def _host(self) -> str:
        return self._target.host

    def _home(self, link: Link) -> str:
        assert link.facts is not None
        return link.facts.home

    def _user(self, link: Link) -> str:
        return self._target.username

    def _log_fields(self) -> dict[str, Any]:
        return {"target": self._target.label, "sandbox_id": self._id}

    def _hold(self, link: Link, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        # Held by the link, so it outlives a cancelled caller.
        return link.spawn(coro)

    async def login_home(self) -> str:
        """The login user's home, which ``~`` names on the machine."""
        return self._home(await self._link())

    async def login_config_home(self) -> str:
        link = await self._link()
        assert link.facts is not None
        return link.facts.config_home

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

    async def link(self) -> Link:
        """The computer's pooled connection, for work that runs its own
        channels on it (a Claude Code turn's CLI)."""
        return await self._link()

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
        self, link: Link, script: str, *, timeout: float = SHORT_S, root: bool = False
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

    # -- Lifecycle --

    async def stop(self, timeout: int = 60, *, force: bool = False) -> None:
        await super().stop(timeout, force=force)
        pool().retire(self._target, only_if_idle=True)

    async def hold_awake(self, seconds: int) -> bool:
        """A Mac sleeps when left alone, and the turn's next call then finds it
        gone. The hold is a timed ``caffeinate``, so one the server stops
        renewing (a turn that ended, a server that died) lets go by itself."""
        link = await self._link()
        if link.facts is None or link.facts.os != "Darwin":
            return False
        await self._run(link, hold_awake_script(seconds))
        return True

    def _refuse_root(self, link: Link, result: ScriptResult) -> None:
        # Sudo makes the root only where the user cannot (``root_dir_script``),
        # and a script with no exit status lost its channel.
        if result.exit_code is None:
            raise self._lost(
                link, ConnectionError("the command ended without a status")
            )
        super()._refuse_root(link, result)

    async def _prepare(self, link: Link) -> None:
        await self._ensure_key_line(link)

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

    async def _stop_processes(self, link: Link) -> str:
        out = await super()._stop_processes(link)
        if "has-root" in out and await self._has_sudo(link):
            await self._run(link, stop_script(self._dir(link), root=True), root=True)
        return out

    # -- Execution --

    async def _send_framed(
        self, link: Link, script: str, *, computer_dir: str, timeout: float, root: bool
    ) -> FramedResult:
        """Dialing again once if the channel never opened. A command that may
        have reached the machine is never sent twice."""
        for attempt in range(2):
            try:
                return await run_framed(
                    link, script, computer_dir=computer_dir, timeout=timeout, root=root
                )
            except ScriptLost as e:
                pool().evict(e.link)
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
        raise AssertionError("unreachable")  # pragma: no cover

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
        self, link: Link, path: str, *, timeout: float | None = SHORT_S
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
            raise too_large(path, size, cap)
        async with self._sftp(link, path, timeout=_transfer_s(size)) as sftp:
            async with await sftp.open(path, "rb", encoding=None) as f:
                data = await f.read(cap + 1)
        if len(data) > cap:
            raise too_large(path, len(data), cap)
        return data

    async def _write_private(
        self, link: Link, directory: str, files: Mapping[str, bytes]
    ) -> None:
        async with self._sftp(link, directory) as sftp:
            for name, data in files.items():
                await write_atomic(sftp, f"{directory}/{name}", data, mode=0o600)

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
        gate = asyncio.Semaphore(UPLOAD_CONCURRENCY)

        async def one(source: bytes | str, dest: str) -> None:
            if isinstance(source, str):
                source = await asyncio.to_thread(_read_local, source)
            timeout = _transfer_s(len(source))
            async with gate, self._sftp(link, dest, timeout=timeout) as sftp:
                await write_atomic(sftp, dest, source, mode=self._upload_mode(dest))

        async with link.in_use():
            await asyncio.gather(*(one(src, dest) for src, dest in files))

    async def download_file_stream(self, path: str) -> AsyncIterator[bytes]:
        link = await self._ensure_running()
        # Each call is bounded on its own: a deadline around the stream would
        # also run while the reader holds a chunk.
        async with self._sftp(link, path, timeout=None) as sftp:
            async with asyncio.timeout(SHORT_S):
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
                    async with asyncio.timeout(SHORT_S):
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
                    if len(entries) == LIST_MAX_ENTRIES:
                        logger.warning(
                            "SSH directory listing cut short",
                            target=self._target.label,
                            sandbox_id=self._id,
                            entries=LIST_MAX_ENTRIES,
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

    # -- Metadata --

    def _machine_facts(self) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "host": self._target.host,
            "port": self._target.port,
            "user": self._target.username,
        }
        link = pool().peek(self._target)
        if link is not None and link.facts is not None:
            meta["os"] = link.facts.os
            meta["arch"] = link.facts.arch
        return meta


def _transfer_s(size: int) -> float:
    return SHORT_S + size / _SFTP_MIN_RATE


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
