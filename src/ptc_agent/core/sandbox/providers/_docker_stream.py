"""A process on an attached Docker exec, streamed both ways.

Docker can neither signal an exec nor end its stdin through the API, so a
signal goes to the process group by pid through a second exec, and EOF is a
half-close of the socket, which leaves its reading side open for the rest of
the output.
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
from typing import TYPE_CHECKING, Any

from ptc_agent.core.sandbox.providers._streams import FedStream, pid_marked, stream_pid_file
from ptc_agent.core.sandbox.runtime import ExecResult, StreamProcess

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.providers.docker import DockerRuntime

# How long the exit status may take to land after the socket closes.
_STATUS_WAIT_S = 5.0


def _socket(attach: Any) -> Any:
    """The attach's raw transport, None once lost. aiodocker 0.26 (pinned in
    pyproject) exposes it only as ``Stream._resp.connection.transport``."""
    response = attach._resp
    return response.connection.transport if response and response.connection else None


async def open_docker_stream(runtime: DockerRuntime, command: str) -> StreamProcess:
    pid_file = stream_pid_file()
    exec_obj = await runtime.raw.exec(
        cmd=["bash", "-c", pid_marked(command, pid_file)],
        stdin=True,
        stdout=True,
        stderr=True,
        workdir=runtime.working_dir,
    )
    async with contextlib.AsyncExitStack() as stack:
        attach = await stack.enter_async_context(exec_obj.start(detach=False))
        stream = DockerStream(runtime, pid_file, exec_obj, attach, stack.pop_all())
    return stream


class DockerStream(FedStream):
    def __init__(
        self,
        runtime: DockerRuntime,
        pid_file: str,
        exec_obj: Any,
        attach: Any,
        exits: contextlib.AsyncExitStack,
    ) -> None:
        super().__init__(runtime, pid_file)
        self._exec = exec_obj
        self._attach = attach
        self._exits = exits
        self._spawn(self._read())

    async def _read(self) -> None:
        """Docker hands the bytes over as written, so they are decoded here,
        as E2B and Daytona decode theirs: bytes that are not UTF-8 read as
        U+FFFD, a character cut between chunks waiting for its other bytes."""
        stdout = codecs.getincrementaldecoder("utf-8")(errors="replace")
        stderr = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            while (message := await self._attach.read_out()) is not None:
                if message.stream == 1:
                    self._out(stdout.decode(message.data).encode())
                else:
                    self._err(stderr.decode(message.data).encode())
        except Exception as exc:  # noqa: BLE001 - a lost socket fails the stream
            self._fail(f"the exec socket was lost: {str(exc) or type(exc).__name__}")
            return
        self._out(stdout.decode(b"", final=True).encode())
        self._err(stderr.decode(b"", final=True).encode())
        status = await self._exit_status()
        if status is None:
            self._fail("its exec socket closed and its exit could not be confirmed")
        else:
            self._end(status)

    async def _exit_status(self) -> int | None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _STATUS_WAIT_S
        while True:
            try:
                async with asyncio.timeout_at(deadline):
                    info = await self._exec.inspect()
            except Exception:  # noqa: BLE001 - an unread status is asked again
                info = None
            if info is not None and not info.get("Running"):
                return info.get("ExitCode")
            if loop.time() >= deadline:
                return None
            await asyncio.sleep(0.1)

    async def _send(self, data: bytes) -> None:
        await self._attach.write_in(data)

    async def _send_eof(self) -> bool:
        transport = _socket(self._attach)
        if transport is None:
            raise ConnectionError("The exec socket was lost")
        if not transport.can_write_eof():
            return False
        transport.write_eof()
        return True

    async def _run_kill(self, command: str) -> ExecResult:
        """From ``/``, which Docker can always enter: an exec in a working
        directory that is gone does not start."""
        return await self._runtime._exec(command, 10, workdir="/")

    async def _release(self) -> None:
        await self._exits.aclose()
