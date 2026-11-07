"""Claude Code on a cloud computer, inside the computer's own sandbox.

The CLI runs in the sandbox as it runs on a computer of the user's own
(``SSHLauncher``): Anthropic's pinned build under the computer root's internal
dir, a private config dir per turn laid out from the checkpoint and removed
after it, and the credential handed to the CLI on a descriptor by the shim,
never in an argv, an environment or a file. ``SandboxLink`` is the
sandbox's ``ShellLink``: the runtime's exec runs the scripts and a leased
stream (``leased_stream``) carries the CLI's stdio, so the SDK client, the
tools and the session mirror stay in the backend worker as for every turn.

The credential is the one the user's turns on cloud computers run on, read as
each run starts (``TokenStore``) and relayed on the CLI's stdin once the shim
asks for it. The shim is told of no token file, so one planted in the
sandbox is never read. The sandbox is the user's computer: code the agent
runs there shares the CLI's user (root on some providers), so it can read
the CLI's memory or replace what the CLI runs. Keeping the credential out of
argvs, environments and files keeps it out of logs, process lists and the
user's own files; it does not keep it from the user's own code.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import math
import posixpath
import shlex
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.claude_code import cli_install
from ptc_agent.agent.claude_code.launcher import TokenStore
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.leased_stream import LEASE_S, RENEW_S, keep_leased, leased_command, new_lease
from ptc_agent.core.sandbox.providers._ssh_transport import Machine, ScriptResult, temp_sibling

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
    from ptc_agent.core.sandbox.runtime import SandboxRuntime, StreamProcess

logger = logging.getLogger(__name__)

#: What the CLI's machine is called in what a failed start says: a sandbox
#: has no host name the user knows.
CLOUD_COMPUTER = "the cloud computer"
#: Between a script's stdout and its stderr in what exec returns, which
#: merges the two.
_STDERR_MARK = "__langalpha_stderr__"
#: The slack exec is given past a script's own timeout, so the script's
#: timeout is the one that fires.
_EXEC_SLACK_S = 5
_SCRIPT_S = 30
#: One file-API request of an upload: small enough that a slow link still
#: sends it within a provider's own per-request cap, and the worker holds one
#: at a time.
_PART_BYTES = 32 << 20


def _script_word(script: str) -> str:
    """``script`` as one word any shell decodes: exec runs under sh on some
    providers, which has no ``$'...'`` quoting for a script's newlines."""
    return f'"$(printf %s {base64.b64encode(script.encode()).decode()} | base64 -d)"'


class _Input:
    """A stream's stdin in the shape ``SSHTransport`` writes an SSH channel's:
    ``write`` queues, ``drain`` sends, ``write_eof`` ends the input. Where the
    sandbox cannot close a process's stdin, ending the input ends the process
    with TERM, which the CLI answers as it answers an EOF once its turn is
    over: it exits within about a second."""

    def __init__(self, stream: StreamProcess, link: SandboxLink) -> None:
        self._stream = stream
        self._link = link
        self._pending = bytearray()
        self._lock = asyncio.Lock()

    def write(self, data: bytes) -> None:
        self._pending += data

    async def drain(self) -> None:
        async with self._lock:
            data, self._pending = bytes(self._pending), bytearray()
            if data:
                await self._stream.write(data)

    def write_eof(self) -> None:
        self._link.spawn(self._end())

    async def _end(self) -> None:
        with contextlib.suppress(Exception):
            await self.drain()
        if not await self._stream.write_eof():
            await self._stream.terminate()


class _Channel:
    """A sandbox stream as a ``ShellProcess``, the shape ``SSHTransport`` reads."""

    def __init__(self, stream: StreamProcess, link: SandboxLink) -> None:
        self._stream = stream
        self._link = link
        self._closing = False
        self.stdin = _Input(stream, link)
        self.stdout = stream.stdout
        self.stderr = stream.stderr

    @property
    def exit_status(self) -> int | None:
        code = self._stream.returncode
        return code if code is not None and code >= 0 else None

    @property
    def exit_signal(self) -> str | None:
        """Set only where the stream could not read how the process ended; a
        signal's end reads as its 128+N status, as a shell reports it."""
        return "unknown" if self._stream.returncode == -1 else None

    async def wait_closed(self) -> None:
        await self._stream.wait()

    def is_closing(self) -> bool:
        return self._closing or self._stream.returncode is not None

    def close(self) -> None:
        self._closing = True
        self._link.spawn(self._stream.close())

    def send_signal(self, sig: str) -> None:
        self._link.spawn(self._stream.terminate() if sig == "TERM" else self._stream.kill())


class SandboxLink:
    """The ``ShellLink`` of a cloud computer: scripts on the sandbox runtime's
    exec, with a script's stderr apart from its stdout, as the scripts'
    readers parse stdout alone, and the CLI on a stream leased for
    ``lease_s`` and renewed every ``renew_s`` (``leased_stream``). Exec opens
    no channel, so there are no slots to hold: how many turns a user runs is
    bounded at admission."""

    host = CLOUD_COMPUTER
    usable = True

    def __init__(self, runtime: SandboxRuntime, *, lease_s: int = LEASE_S, renew_s: float = RENEW_S) -> None:
        self.runtime = runtime
        self.lease_s = lease_s
        self.renew_s = renew_s
        self._tasks: set[asyncio.Task[Any]] = set()

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def run(self, script: str, *, timeout: float, kill: bool = False, long: bool = False) -> ScriptResult:
        """Run ``script`` under bash and wait for it. A result whose stderr
        cannot be told apart (the exec ended before the script's own end) has
        no exit code, with all it printed, which every caller reads as failed.

        Raises:
            TimeoutError: It ran past ``timeout``.
        """
        wrapped = (
            f't=$(mktemp) || exit 125; bash -c {_script_word(script)} 2>"$t"; e=$?; '
            f'printf "\\n{_STDERR_MARK}\\n"; cat "$t"; rm -f "$t"; exit $e'
        )
        async with asyncio.timeout(timeout):
            result = await self.runtime.exec(wrapped, timeout=math.ceil(timeout) + _EXEC_SLACK_S)
        out, mark, err = result.stdout.rpartition(f"\n{_STDERR_MARK}\n")
        if not mark:
            return ScriptResult(None, b"", (result.stderr or result.stdout).encode())
        stdout = out.encode()
        return ScriptResult(result.exit_code, stdout, err.encode(), stdout_bytes=len(stdout))

    @contextlib.asynccontextmanager
    async def cli_hold(self, wait_s: float) -> AsyncIterator[Callable[[str], Awaitable[_Channel]]]:
        yield self._start

    async def _start(self, command: str) -> _Channel:
        """``command`` streaming on a lease; it keeps the pid it starts with, as an ``exec`` does."""
        lease = new_lease()
        stream = await self.runtime.open_stream(leased_command(command, lease, self.lease_s))
        self.spawn(keep_leased(self.runtime, stream, lease, lease_s=self.lease_s, renew_s=self.renew_s))
        return _Channel(stream, self)

    async def upload(self, local: str, remote: str, *, timeout: float) -> None:
        """Send a large local file to ``remote`` gzipped (``cli_install.gzipped``),
        in parts of ``_PART_BYTES``, unpacked there: about half the bytes, which
        on a cloud provider's file API is about half the time (the CLI: 21.6 s
        raw, 13.2 s gzipped on E2B). A send that fails leaves nothing at
        ``remote``.

        Raises:
            TimeoutError: It ran past ``timeout``.
        """
        parts: list[str] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            source = await asyncio.to_thread(cli_install.gzipped, Path(local))
            async with asyncio.timeout_at(deadline):
                with source.open("rb") as packed:
                    while data := await asyncio.to_thread(packed.read, _PART_BYTES):
                        parts.append(f"{remote}.gz.{len(parts)}")
                        await self.runtime.upload_files([(data, parts[-1])])
            sent = " ".join(map(shlex.quote, parts))
            result = await self.run(
                f"cat -- {sent} | gunzip -c > {shlex.quote(remote)}; e=$?; rm -f -- {sent}; exit $e",
                timeout=max(1.0, deadline - loop.time()),
            )
            if result.exit_code != 0:
                raise RuntimeError(result.stderr.decode(errors="replace").strip() or f"exit {result.exit_code}")
        except BaseException:
            await asyncio.shield(self._remove(remote, *parts))
            raise

    async def _remove(self, *paths: str) -> None:
        with contextlib.suppress(Exception):
            await self.run(f"rm -f -- {' '.join(map(shlex.quote, paths))}", timeout=_SCRIPT_S)

    async def write_files(self, files: list[tuple[str, bytes]]) -> None:
        """Write each file whole and private: uploaded beside its path, then renamed in."""
        staged = [(data, temp_sibling(path)) for path, data in files]
        parents = sorted({posixpath.dirname(path) for path, _ in files})
        made = await self.run(f"umask 077; mkdir -p -- {' '.join(map(shlex.quote, parents))}", timeout=_SCRIPT_S)
        if made.exit_code != 0:
            raise RuntimeError(made.stderr.decode(errors="replace").strip() or f"exit {made.exit_code}")
        await self.runtime.upload_files(staged)
        moves = "; ".join(
            f"chmod 600 -- {shlex.quote(tmp)} && mv -f -- {shlex.quote(tmp)} {shlex.quote(path)} || f=1"
            for (_, tmp), (path, _) in zip(staged, files, strict=True)
        )
        moved = await self.run(f"f=0; {moves}; exit $f", timeout=_SCRIPT_S)
        if moved.exit_code != 0:
            await self.run(f"rm -f -- {' '.join(shlex.quote(tmp) for _, tmp in staged)}", timeout=_SCRIPT_S)
            raise RuntimeError(moved.stderr.decode(errors="replace").strip() or f"exit {moved.exit_code}")


def sandbox_launcher(sandbox: PTCSandbox, *, identity: str, credential: TokenStore, env: dict[str, str] | None = None) -> SSHLauncher:
    """The launcher of a turn in ``sandbox``, a cloud computer's, signed in with
    what ``credential`` holds as the run starts; it reads no token file
    there. ``env`` is for a test endpoint."""
    link: SandboxLink | None = None

    async def connect() -> SandboxLink:
        nonlocal link
        await sandbox.wait_ready()
        if link is None or link.runtime is not sandbox.runtime:
            link = SandboxLink(sandbox.runtime)
        return link

    return SSHLauncher(
        Machine(connect, lambda: sandbox.working_dir), identity=identity, token_file=None, credential=credential,
        env=dict(env or {}),
    )
