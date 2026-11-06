"""A process in an E2B sandbox, started with stdin, its output pushed through callbacks.

The SDK privates this needs live here alone, as of e2b 2.53.1 (pinned in
pyproject): ``Commands._rpc``, ``Commands._create_handle``,
``Commands._envd_version`` and ``AsyncCommandHandle._stdout_chunks`` and
``_stderr_chunks``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING

import structlog
from e2b import AsyncSandbox, CommandExitException
from e2b.connection_config import KEEPALIVE_PING_HEADER, KEEPALIVE_PING_INTERVAL_SEC
from e2b.envd.client_async import as_stream
from e2b.envd.process import process_pb
from e2b.envd.utils import authentication_header
from e2b.envd.versions import ENVD_ENVD_CLOSE
from e2b.sandbox_async.commands.command_handle import AsyncCommandHandle

from ptc_agent.core.sandbox.providers._e2b_common import _USER, _exit_status
from ptc_agent.core.sandbox.providers._streams import FedStream, pid_marked, stream_pid_file
from ptc_agent.core.sandbox.runtime import ExecResult, StreamProcess

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.providers.e2b_runtime import E2BRuntime

logger = structlog.get_logger(__name__)

# How often, at most, an open stream pushes the pause deadline out. A short
# idle window renews more often, at a quarter of it, the runtime's renewal
# slack, so the deadline never comes near while the stream is open.
_KEEP_ALIVE_S = 60


def _closes_stdin(sandbox: AsyncSandbox) -> bool:
    """Whether the sandbox's envd can close a process's stdin, which an older
    template's cannot."""
    return sandbox.commands._envd_version >= ENVD_ENVD_CLOSE


async def _start_plain_bash(
    sandbox: AsyncSandbox,
    command: str,
    *,
    on_stdout: Callable[[str], None],
    on_stderr: Callable[[str], None],
    cwd: str | None = None,
) -> AsyncCommandHandle:
    """Start ``bash -c command`` with stdin, as neither a login nor an
    interactive shell.

    ``commands.run`` always starts ``/bin/bash -l -c``, and a login shell runs
    root's ``~/.bash_profile`` before the command, so a profile or rc file that
    code in the sandbox planted would run first, holding the stream's stdin.
    This stops that, and only that: commands run as root, where such code can
    still read the process's descriptors or replace bash. It is
    ``Commands._start`` of e2b 2.53.1 with only the shell's arguments changed.
    """
    commands = sandbox.commands
    events = as_stream(
        commands._rpc.start(
            process_pb.StartRequest(
                process=process_pb.ProcessConfig(
                    cmd="/bin/bash", args=["--noprofile", "--norc", "-c", command], cwd=cwd
                ),
                stdin=True,
            ),
            headers={
                **authentication_header(commands._envd_version, _USER),
                KEEPALIVE_PING_HEADER: str(KEEPALIVE_PING_INTERVAL_SEC),
            },
            timeout_ms=0,
        )
    )
    return await commands._create_handle(events, "start process", None, on_stdout, on_stderr)


async def _run_plain(sandbox: AsyncSandbox, command: str, timeout: float) -> ExecResult:
    """Run *command* through ``_start_plain_bash`` to its end, output dropped,
    from ``/``: E2B starts a process in its user's home by default, which code
    in the sandbox may have removed, and a process whose directory is gone
    does not start."""
    handle = await _start_plain_bash(
        sandbox, command, on_stdout=lambda _: None, on_stderr=lambda _: None, cwd="/"
    )
    try:
        async with asyncio.timeout(timeout):
            result = await handle.wait()
    except CommandExitException as exc:
        return ExecResult(
            stdout="", stderr=exc.stderr, exit_code=_exit_status(exc.exit_code, exc.error)
        )
    finally:
        with contextlib.suppress(Exception):
            await handle.disconnect()
    return ExecResult(stdout="", stderr="", exit_code=result.exit_code)


async def open_e2b_stream(runtime: E2BRuntime, command: str) -> StreamProcess:
    sandbox = await runtime.connected_sandbox()
    stream: E2BStream | None = None

    # The handle calls these from a task of its own, which cannot run before
    # this coroutine next awaits, and by then ``stream`` is set. The handle
    # also keeps every chunk it hands them, for its result, which would hold
    # all the process ever wrote, so each drops what the handle kept.
    def on_stdout(text: str) -> None:
        assert stream is not None
        stream._handle._stdout_chunks.clear()
        stream._out(text.encode())

    def on_stderr(text: str) -> None:
        assert stream is not None
        stream._handle._stderr_chunks.clear()
        stream._err(text.encode())

    pid_file = stream_pid_file()
    handle = await _start_plain_bash(
        sandbox, pid_marked(command, pid_file), on_stdout=on_stdout, on_stderr=on_stderr
    )
    stream = E2BStream(runtime, pid_file, sandbox, handle)
    return stream


class E2BStream(FedStream):
    """E2B's deadline counts no use it cannot see, and a stream can sit idle
    for as long as a tool runs, so the stream keeps the sandbox up until it
    ends. A dropped event stream fails the stream and kills the process:
    envd replays nothing, so a reattach would lose what the process wrote in
    between, and a lost message leaves both ends waiting on each other. A
    signal goes through the same plain bash the stream starts in, since
    ``exec``'s login shell would run a planted profile first, which could
    hold the kill up or skip it.
    """

    def __init__(
        self, runtime: E2BRuntime, pid_file: str, sandbox: AsyncSandbox, handle: AsyncCommandHandle
    ) -> None:
        super().__init__(runtime, pid_file)
        self._sandbox = sandbox
        self._handle = handle
        self._spawn(self._follow())
        self._spawn(self._keep_up())

    async def _follow(self) -> None:
        try:
            result = await self._handle.wait()
        except CommandExitException as exc:
            self._end(_exit_status(exc.exit_code, exc.error))
        except Exception as exc:  # noqa: BLE001 - a drop fails the stream
            self._fail(f"its event stream dropped: {str(exc) or type(exc).__name__}")
        else:
            self._end(result.exit_code)

    async def _keep_up(self) -> None:
        while True:
            try:
                async with asyncio.timeout(min(_KEEP_ALIVE_S, self._runtime._window_s / 4)):
                    await self._ended.wait()
                return
            except TimeoutError:
                pass
            try:
                await self._runtime.note_activity()
            except Exception as exc:  # noqa: BLE001 - the next tick tries again
                logger.warning(
                    "Could not keep E2B sandbox up for its stream",
                    sandbox_id=self._runtime.id,
                    error=str(exc) or type(exc).__name__,
                )

    async def _send(self, data: bytes) -> None:
        await self._sandbox.commands.send_stdin(self._handle.pid, data)

    async def _send_eof(self) -> bool:
        if not _closes_stdin(self._sandbox):
            return False
        await self._sandbox.commands.close_stdin(self._handle.pid)
        return True

    async def _run_kill(self, command: str) -> ExecResult:
        return await _run_plain(self._sandbox, command, timeout=10)

    async def _release(self) -> None:
        with contextlib.suppress(Exception):
            await self._handle.disconnect()
