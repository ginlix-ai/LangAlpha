"""The runtime half of the local provider: one computer on the machine the server runs on.

Its commands run as the server's own OS user, with the same scripts and the
same folders an SSH computer uses on a machine of the user's
(``MachineFolderRuntime``). What differs is the channel. A script reaches a
local bash on stdin rather than over a login, and a file call is a file call.

A login gives every command a clean environment. A child process inherits the
server's, which holds its database password, LLM and sandbox keys, relay secret
and any BYOK key, so nothing here starts one without ``machine_env``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import platform
import posixpath
import signal
import stat
import uuid
from collections import deque
from collections.abc import AsyncIterator, Coroutine, Mapping
from datetime import UTC, datetime
from typing import Any, cast

import structlog

from ptc_agent.core.sandbox.providers import _ssh_bootstrap as bootstrap
from ptc_agent.core.sandbox.providers._machine_folder import (
    LIST_MAX_ENTRIES,
    SHORT_S,
    UPLOAD_CONCURRENCY,
    MachineFolderRuntime,
    too_large,
)
from ptc_agent.core.sandbox.providers._script_run import (
    ScriptResult,
    ScriptRun,
    finish_framed,
    spawn,
)
from ptc_agent.core.sandbox.providers._ssh_root import root_dir_script
from ptc_agent.core.sandbox.providers._ssh_shell import (
    BOOT,
    OUTPUT_CAP,
    TERM_GRACE_S,
    FramedResult,
    computers_dir,
)
from ptc_agent.core.sandbox.runtime import (
    STREAM_CHUNK_BYTES,
    ExecResult,
    PreviewInfo,
    SandboxTransientError,
)

logger = structlog.get_logger(__name__)

# How the root folder scripts name this machine in a sentence for its owner.
MACHINE_NAME = "this computer"
# The selection the setup step saves; unset installs the defaults.
INSTALL_GROUPS_ENV = "LOCAL_INSTALL_GROUPS"

# All that reaches a process from the server's environment. PATH is the owner's
# so their own tools resolve; nothing here authenticates to anything but, in a
# proxy's URL, the owner's own way out, which the first-run install needs too.
_PASSED_ENV = (
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TMPDIR",
    "XDG_CACHE_HOME",
    "XDG_CONFIG_DIRS",
    "XDG_CONFIG_HOME",
    "XDG_DATA_DIRS",
    "XDG_DATA_HOME",
    "XDG_RUNTIME_DIR",
    "XDG_STATE_HOME",
    # A Node for the `la` shim where the machine has none: the desktop app's
    # own runtime, a path and not a credential.
    "LANGALPHA_NODE",
)
# A login's PATH holds these, and an app the desktop started has few of them.
_USUAL_PATH = (
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin",
    "/usr/sbin",
    "/sbin",
)


def _own_account() -> Any:
    """Imported here because Windows has no ``pwd``, and the gate beside this
    module has to import there to say no."""
    import pwd

    return pwd.getpwuid(os.getuid())


def machine_home(environ: Mapping[str, str] = os.environ) -> str:
    """The user's home by its real path, which the root folder scripts compare
    a folder's own path against."""
    home = environ.get("HOME") or _own_account().pw_dir
    return os.path.realpath(home)


def machine_env(environ: Mapping[str, str] = os.environ) -> dict[str, str]:
    """The whole environment of every process a local computer starts.

    The security seam: an allowlist, never the server's own with some removed,
    so a credential added to the server later stays out without a word here.
    """
    account = _own_account()
    env = {name: environ[name] for name in _PASSED_ENV if environ.get(name)}
    # The server's own virtualenv is on its PATH when it runs from one.
    venv = environ.get("VIRTUAL_ENV")
    own_bin = os.path.join(venv, "bin") if venv else None
    path = [p for p in environ.get("PATH", "").split(":") if p and p != own_bin]
    env.update(
        HOME=machine_home(environ),
        USER=account.pw_name,
        LOGNAME=account.pw_name,
        SHELL=environ.get("SHELL") or account.pw_shell,
        PATH=":".join(dict.fromkeys((*path, *_USUAL_PATH))),
    )
    return env


def install_groups(environ: Mapping[str, str] = os.environ) -> frozenset[str]:
    """What its owner chose to install, the defaults where they chose nothing.

    A name no group has is dropped rather than refused, so a selection saved by
    a newer build still lets a computer start.
    """
    raw = environ.get(INSTALL_GROUPS_ENV)
    if raw is None:
        return bootstrap.default_groups()
    names = {name.strip() for name in raw.split(",")}
    known = names & bootstrap.INSTALL_GROUPS.keys()
    if unknown := sorted(names - known - {""}):
        logger.warning("Ignoring unknown install groups", groups=unknown)
    return bootstrap.selected_groups(known)


class _Pipe:
    """One of a script's output pipes, read as ``Drain`` reads a stream."""

    def __init__(self) -> None:
        self._chunks: deque[bytes] = deque()
        self._end: BaseException | bool = False
        self._more = asyncio.Event()

    def feed(self, data: bytes) -> None:
        self._chunks.append(data)
        self._more.set()

    def end(self, exc: BaseException | None) -> None:
        self._end = exc or True
        self._more.set()

    async def read(self, _size: int) -> bytes:
        while not self._chunks:
            if isinstance(self._end, BaseException):
                raise self._end
            if self._end:
                return b""
            self._more.clear()
            await self._more.wait()
        return self._chunks.popleft()


class _Pipes(asyncio.SubprocessProtocol):
    """A script's stdout and stderr, and its exit."""

    def __init__(self) -> None:
        self.pipes = {1: _Pipe(), 2: _Pipe()}
        self.exited = asyncio.Event()
        self._transport: asyncio.SubprocessTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        self.pipes[fd].feed(data)

    def pipe_connection_lost(self, fd: int, exc: Exception | None) -> None:
        if fd in self.pipes:
            self.pipes[fd].end(exc)

    def process_exited(self) -> None:
        self.exited.set()

    def connection_lost(self, exc: Exception | None) -> None:
        # Exited and every pipe closed: nothing is left to give up.
        if self._transport is not None:
            self._transport.close()


class _Run(ScriptRun):
    """One script in a bash of its own, in a session of its own.

    Over a protocol of its own rather than ``asyncio.subprocess.Process``,
    whose transport it has to close to give up pipes a background child
    still holds, and which keeps that transport private.
    """

    def __init__(self, transport: asyncio.SubprocessTransport, pipes: _Pipes) -> None:
        super().__init__(pipes.pipes[1], pipes.pipes[2], OUTPUT_CAP)
        self._transport = transport
        self._pipes = pipes

    @classmethod
    async def start(cls, script: str) -> _Run:
        env = machine_env()
        transport, pipes = await asyncio.get_running_loop().subprocess_exec(
            _Pipes,
            "bash",
            "-c",
            BOOT,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            # A login starts in the home; the folder the server started in is
            # nothing of the computer's.
            cwd=env["HOME"],
            start_new_session=True,
        )
        # A cast, not an isinstance check: uvloop's pipe transports write like
        # asyncio's but are not its subclasses.
        stdin = cast(asyncio.WriteTransport, transport.get_pipe_transport(0))
        # On stdin, so a secret in its env stays out of argv, as over a login.
        stdin.write(script.encode())
        stdin.write_eof()
        return cls(transport, pipes)

    @property
    def exit_code(self) -> int | None:
        code = self._transport.get_returncode()
        return code if code is not None and code >= 0 else None

    def _exited(self) -> bool:
        return self._transport.get_returncode() is not None

    async def _closed(self) -> None:
        await self._pipes.exited.wait()

    def _close(self) -> None:
        # Its pipes close with the transport, which kills a script still running.
        self._transport.close()

    def kill(self) -> None:
        """Signal the script's own group: a framed command's is another, which
        ``end_group`` ends first, so this ends the shell around it."""
        if self._transport.get_returncode() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(self._transport.get_pid(), signal.SIGKILL)

    async def _kill_group(self, script: str, *, root: bool) -> bool:
        await _to_end(script, timeout=TERM_GRACE_S + 15)
        return True


async def _to_end(script: str, *, timeout: float) -> ScriptResult:
    """Run *script* to its end; one still running at *timeout* is ended.

    Raises:
        TimeoutError: It ran past *timeout*.
    """
    run = await _Run.start(script)
    try:
        async with asyncio.timeout(timeout):
            return await run.finish()
    except BaseException:
        run.kill()
        run.abandon()
        raise


class LocalRuntime(MachineFolderRuntime[None]):
    """Runtime over one computer on this machine, which nothing has to reach."""

    noun = "local computer"
    provider = "local"

    @property
    def _host(self) -> str:
        return MACHINE_NAME

    async def _link(self) -> None:
        return None

    async def _lifecycle_link(self) -> None:
        return None

    def _home(self, link: None) -> str:
        return machine_home()

    def _user(self, link: None) -> str:
        return _own_account().pw_name

    def _hold(self, link: None, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        return spawn(coro)

    def _install_groups(self) -> frozenset[str]:
        return install_groups()

    def _machine_facts(self) -> dict[str, Any]:
        return {"os": platform.system(), "arch": platform.machine()}

    async def login_home(self) -> str:
        return machine_home()

    async def login_config_home(self) -> str:
        return os.environ.get("XDG_CONFIG_HOME", "")

    async def _script(
        self, link: None, script: str, *, timeout: float = SHORT_S, root: bool = False
    ) -> ScriptResult:
        try:
            return await _to_end(script, timeout=timeout)
        except TimeoutError as e:
            raise SandboxTransientError(
                f"This computer did not finish a command within {int(timeout)} seconds"
            ) from e

    async def _send_framed(
        self, link: None, script: str, *, computer_dir: str, timeout: float, root: bool
    ) -> FramedResult:
        run = await _Run.start(script)
        return await finish_framed(run, computer_dir=computer_dir, timeout=timeout)

    async def exec_as_root(
        self, command: str, timeout: int = 60, env: dict[str, str] | None = None
    ) -> ExecResult:
        # Only the file mount asks, which this machine never runs, and a
        # server running as its user has no business with the owner's sudo.
        raise RuntimeError("This computer runs commands only as its own user.")

    # -- File I/O --

    def _path(self, path: str) -> str:
        # Against the home as a login's file calls resolve it, never against
        # the folder the server happened to start in.
        return posixpath.join(machine_home(), path)

    async def _read_bytes(self, link: None, path: str, *, cap: int) -> bytes:
        return await asyncio.to_thread(_read_capped, self._path(path), cap)

    async def _write_private(
        self, link: None, directory: str, files: Mapping[str, bytes]
    ) -> None:
        for name, data in files.items():
            await asyncio.to_thread(_write_atomic, f"{directory}/{name}", data, 0o600)

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        await self._ensure_running()
        path = self._path(dest_path)
        await asyncio.to_thread(
            _write_atomic, path, bytes(content), self._upload_mode(path)
        )

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        if not files:
            return
        await self._ensure_running()
        gate = asyncio.Semaphore(UPLOAD_CONCURRENCY)

        def write(source: bytes | str, path: str) -> None:
            if isinstance(source, str):
                with open(source, "rb") as f:
                    source = f.read()
            _write_atomic(path, source, self._upload_mode(path))

        async def one(source: bytes | str, dest: str) -> None:
            async with gate:
                await asyncio.to_thread(write, source, self._path(dest))

        await asyncio.gather(*(one(src, dest) for src, dest in files))

    async def download_file_stream(self, path: str) -> AsyncIterator[bytes]:
        await self._ensure_running()
        f = await asyncio.to_thread(open, self._path(path), "rb")
        try:
            while chunk := await asyncio.to_thread(f.read, STREAM_CHUNK_BYTES):
                yield chunk
        finally:
            await asyncio.to_thread(f.close)

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        await self._ensure_running()
        entries, cut = await asyncio.to_thread(_list_dir, self._path(directory))
        if cut:
            logger.warning(
                "Local directory listing cut short",
                sandbox_id=self._id,
                entries=LIST_MAX_ENTRIES,
            )
        return entries

    # -- Preview URLs --

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        """The port itself: the browser is on this machine too, and its frame
        host names that one origin, so no proxy stands between them."""
        if not 1 <= port <= 65535:
            raise ValueError(f"Invalid port: {port}")
        return PreviewInfo(url=f"http://127.0.0.1:{port}", token=None)


async def root_is_free(path: str) -> bool:
    """Whether a new computer could take *path*, by the rule its start takes it."""
    script = root_dir_script(
        path, computers=computers_dir(machine_home()), computer_id=None
    )
    return (await _to_end(script, timeout=SHORT_S)).exit_code == 0


def _read_capped(path: str, cap: int) -> bytes:
    """The file at *path*, refused past *cap* bytes, in the memory every other
    turn on this server shares."""
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        if size > cap:
            raise too_large(path, size, cap)
        data = f.read(cap + 1)
    if len(data) > cap:
        raise too_large(path, len(data), cap)
    return data


def _make_dirs(path: str) -> None:
    """Make *path* and its parents writable by the user alone, whatever the
    server's umask: the agent imports code from these folders."""
    if os.path.isdir(path):
        return
    parent = os.path.dirname(path)
    if parent and parent != path:
        _make_dirs(parent)
    with contextlib.suppress(FileExistsError):
        os.mkdir(path, 0o755)


def _existing_mode(path: str) -> int | None:
    """Followed through a link, whose own mode says nothing. No special bits: a
    setuid file that was just rewritten is not the same program."""
    try:
        return stat.S_IMODE(os.stat(path).st_mode) & 0o777
    except OSError:
        return None


def _write_atomic(path: str, data: bytes, mode: int | None = None) -> None:
    """Write *data* to *path* whole: to a temp file first, then renamed in.
    Parents are created on demand, so the common case costs no extra call.

    With no *mode*, a file being replaced keeps its own, since the rename would
    otherwise widen a 0600 file and take a script's exec bit; a new one gets 0644,
    leaving out what the server's own umask may not, as every command's
    ``umask go-w`` does.
    """
    parent, name = posixpath.split(path)
    tmp = posixpath.join(parent, f".{name}.la-{uuid.uuid4().hex[:8]}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    kept = None if mode is not None else _existing_mode(path)
    create_mode = mode if mode is not None else 0o644
    try:
        fd = os.open(tmp, flags, create_mode)
    except FileNotFoundError:
        _make_dirs(parent)
        fd = os.open(tmp, flags, create_mode)
    try:
        with os.fdopen(fd, "wb") as f:
            # After open, which the umask would have narrowed.
            if kept is not None:
                os.fchmod(f.fileno(), kept)
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _account(names: dict[int, str], key: int, lookup: Any) -> str:
    if key not in names:
        try:
            names[key] = lookup(key)
        except KeyError:
            names[key] = str(key)
    return names[key]


def _list_dir(directory: str) -> tuple[list[dict[str, Any]], bool]:
    """Up to ``LIST_MAX_ENTRIES`` entries in the keys every runtime's
    ``list_files`` returns; True when more were left out. A symbolic link reads
    as itself, as over SFTP."""
    import grp
    import pwd

    entries: list[dict[str, Any]] = []
    owners: dict[int, str] = {}
    groups: dict[int, str] = {}
    with os.scandir(directory) as found:
        for entry in found:
            if len(entries) == LIST_MAX_ENTRIES:
                return entries, True
            try:
                info = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            entries.append(
                {
                    "name": entry.name,
                    "is_dir": stat.S_ISDIR(info.st_mode),
                    "size": info.st_size,
                    "mode": stat.S_IMODE(info.st_mode),
                    "permissions": stat.filemode(info.st_mode)[1:],
                    "owner": _account(
                        owners, info.st_uid, lambda u: pwd.getpwuid(u).pw_name
                    ),
                    "group": _account(
                        groups, info.st_gid, lambda g: grp.getgrgid(g).gr_name
                    ),
                    "mod_time": datetime.fromtimestamp(info.st_mtime, UTC).isoformat(),
                }
            )
    return entries, False
