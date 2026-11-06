"""A command in a Daytona session of its own, input sent to it, output read off its log socket."""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import shlex
import time
import uuid
from typing import TYPE_CHECKING

import structlog
from daytona import DaytonaNotFoundError, SessionExecuteRequest

from ptc_agent.core.sandbox.providers import _streams
from ptc_agent.core.sandbox.providers._streams import FedStream, pid_marked, stream_pid_file
from ptc_agent.core.sandbox.runtime import ExecResult, StreamProcess

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

logger = structlog.get_logger(__name__)

# A stream's log socket that drops under a running command is reattached:
# first after REATTACH_FIRST_DELAY_S, the delay doubling to
# REATTACH_MAX_DELAY_S while sockets keep dropping, and back to the first once
# one held for REATTACH_RESET_AFTER_S, which makes it a drop rather than a
# socket that will not stay open. STATUS_READ_MISSES failed status reads in a
# row fail the stream.
_REATTACH_FIRST_DELAY_S = 0.5
_REATTACH_MAX_DELAY_S = 30.0
_REATTACH_RESET_AFTER_S = 60.0
_STATUS_READ_MISSES = 5
# How long one status read may take: a read that hangs is a miss, so a
# signal's exit is still seen within the stream's own windows.
_STATUS_READ_S = 5.0
# How often, at most, an open stream counts as activity; a short auto-stop
# interval is touched more often, at a quarter of it.
_KEEP_ALIVE_S = 60


class LineHoldback:
    """Bytes to whole lines of text, for a session command's input.

    Daytona takes input only as text and ends each input it is sent with a
    newline, so a write that ends mid-line keeps its last part back until a
    later write ends the line. Each write is decoded as it comes, a character
    cut between writes waiting for its other bytes, so the write that holds
    bytes that are not UTF-8 is the one refused, as is the write that takes
    the unfinished line past the readers' line limit.
    """

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._partial = bytearray()

    def lines(self, data: bytes) -> str:
        """The lines this write completes; ValueError, with none of it kept,
        on bytes that are not UTF-8 or on a line longer than the limit."""
        cut = data.rfind(b"\n") + 1
        held = len(data) - cut if cut else len(self._partial) + len(data)
        if held > _streams._LINE_LIMIT:
            raise ValueError(f"An input line is longer than {_streams._LINE_LIMIT} bytes")
        state = self._decoder.getstate()
        try:
            self._decoder.decode(data)
        except UnicodeDecodeError:
            self._decoder.setstate(state)
            raise
        self._partial += data
        if not cut:
            return ""
        done = self._partial[: len(self._partial) - held]
        del self._partial[: len(done)]
        return done.decode()


class LineReplay:
    """One output stream off a log socket that may drop and replay.

    The socket replays a command's output from its start, so a reattach skips
    what was already delivered. Only whole lines are delivered: a socket that
    drops flushes what it held, a character cut in two as U+FFFD or the first
    bytes of a stream marker, and that tail sits after the last newline, so
    dropping the unfinished line on reattach drops it too, and the replay
    sends that line again whole. It counts decoded characters, which holds
    because Daytona decodes the same bytes to the same text on every replay.
    """

    def __init__(self) -> None:
        self._delivered = 0
        self._skip = 0
        self._partial = bytearray()

    @property
    def held(self) -> int:
        """UTF-8 bytes of the unfinished line, held until it ends."""
        return len(self._partial)

    def take(self, text: str) -> str:
        """The lines *text* completes."""
        if self._skip:
            dropped = min(self._skip, len(text))
            self._skip -= dropped
            text = text[dropped:]
        end = text.rfind("\n") + 1
        if not end:
            self._partial += text.encode()
            return ""
        lines = (self._partial + text[:end].encode()).decode()
        self._partial = bytearray(text[end:].encode())
        self._delivered += len(lines)
        return lines

    def reattach(self) -> None:
        self._skip = self._delivered
        self._partial = bytearray()

    def rest(self) -> str:
        """The unfinished last line of a log read to its end."""
        rest = self._partial.decode()
        self._partial = bytearray()
        return rest


async def open_daytona_stream(runtime: DaytonaRuntime, command: str) -> StreamProcess:
    process = runtime.raw.process
    session_id = f"stream-{uuid.uuid4().hex[:12]}"
    await process.create_session(session_id)
    pid_file = stream_pid_file()
    request = SessionExecuteRequest(
        command=f"bash -c {shlex.quote(pid_marked(command, pid_file))}",
        run_async=True,
        suppress_input_echo=True,
    )
    try:
        response = await process.execute_session_command(session_id, request)
    except BaseException:
        with contextlib.suppress(Exception):
            await process.delete_session(session_id)
        raise
    return DaytonaStream(runtime, pid_file, session_id, response.cmd_id)


class DaytonaStream(FedStream):
    """Input goes as whole lines (``LineHoldback``), so a write that never ends
    its line never reaches the process. Daytona cannot close a command's
    stdin, so ``write_eof`` says so and the caller terminates instead. A log
    socket can drop (close code 1006) and is reattached, its output delivered
    in whole lines (``LineReplay``); one that dropped as the command ended is
    attached once more for the output it had not delivered, and the stream
    fails if that attach drops too. Output that ends without a newline gains
    one in Daytona's log. The command runs in a session, whose shell (zsh where
    the image has it, else bash) starts without a login or a terminal, so of
    the startup files only zsh's ``/etc/zshenv`` and ``~/.zshenv`` run before
    it. Input waits for the command's pid line: Daytona makes a command's
    input pipe as the command starts, after the execute returns, and refuses
    input sent before it. Daytona counts a running command as no activity, so
    the stream counts itself as activity until it ends.
    """

    def __init__(
        self, runtime: DaytonaRuntime, pid_file: str, session_id: str, command_id: str
    ) -> None:
        super().__init__(runtime, pid_file)
        self._process = runtime.raw.process
        self._session_id = session_id
        self._command_id = command_id
        self._input = LineHoldback()
        self._stdout_lines = LineReplay()
        self._stderr_lines = LineReplay()
        self._signalled = asyncio.Event()
        self._spawn(self._read())
        self._spawn(self._keep_up())

    async def _on_stdout(self, text: str) -> None:
        if self._ended.is_set():
            return
        self._out(self._stdout_lines.take(text).encode())
        self._check_buffered()

    async def _on_stderr(self, text: str) -> None:
        if self._ended.is_set():
            return
        self._err(self._stderr_lines.take(text).encode())
        self._check_buffered()

    async def _read(self) -> None:
        delay = _REATTACH_FIRST_DELAY_S
        misses = 0
        while not self._ended.is_set():
            attached_at = time.monotonic()
            held = await self._follow_logs()
            try:
                async with asyncio.timeout(_STATUS_READ_S):
                    command = await self._process.get_session_command(
                        self._session_id, self._command_id
                    )
            except DaytonaNotFoundError:
                self._fail("its session is gone")
                return
            except Exception:  # noqa: BLE001 - one miss is not a lost command
                misses += 1
                if misses >= _STATUS_READ_MISSES:
                    self._fail("its status could not be read")
                    return
            else:
                misses = 0
                if command.exit_code is not None:
                    if not held:
                        self._reattach()
                        if not await self._follow_logs():
                            self._fail("the end of its output was lost")
                            return
                    self._out(self._stdout_lines.rest().encode())
                    self._err(self._stderr_lines.rest().encode())
                    self._end(command.exit_code)
                    return
            if time.monotonic() - attached_at > _REATTACH_RESET_AFTER_S:
                delay = _REATTACH_FIRST_DELAY_S
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(delay):
                    await self._signalled.wait()
            self._signalled.clear()
            delay = min(delay * 2, _REATTACH_MAX_DELAY_S)
            self._reattach()

    async def _keep_up(self) -> None:
        while True:
            minutes = self._runtime.raw.auto_stop_interval or 0
            try:
                async with asyncio.timeout(min(_KEEP_ALIVE_S, minutes * 15 or _KEEP_ALIVE_S)):
                    await self._ended.wait()
                return
            except TimeoutError:
                pass
            try:
                await self._runtime.raw.refresh_activity()
            except Exception as exc:  # noqa: BLE001 - the next tick tries again
                logger.warning(
                    "Could not keep Daytona sandbox up for its stream",
                    sandbox_id=self._runtime.id,
                    error=str(exc) or type(exc).__name__,
                )

    async def _follow_logs(self) -> bool:
        """Deliver the log until its socket closes; False when it dropped."""
        try:
            await self._process.get_session_command_logs_async(
                self._session_id, self._command_id, self._on_stdout, self._on_stderr
            )
        except Exception as exc:  # noqa: BLE001 - the status read says whether it ran on
            logger.warning(
                "Daytona stream's log socket dropped",
                sandbox_id=self._runtime.id,
                error=str(exc) or type(exc).__name__,
            )
            return False
        return True

    def _held(self) -> int:
        return self._stdout_lines.held + self._stderr_lines.held

    async def _signal(self, name: str) -> None:
        """A signal ends the wait before a reattach, so the reader sees the
        process end now rather than after the backoff."""
        await super()._signal(name)
        self._signalled.set()

    def _reattach(self) -> None:
        self._stdout_lines.reattach()
        self._stderr_lines.reattach()

    async def _send(self, data: bytes) -> None:
        text = self._input.lines(data)
        if text:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(_streams._PID_WAIT_S):
                    await self._pid_known.wait()
            await self._process.send_session_command_input(
                self._session_id, self._command_id, text
            )

    async def _run_kill(self, command: str) -> ExecResult:
        """From ``/``: Daytona starts an exec in the sandbox's working
        directory, which code in the sandbox may have removed, and an exec
        whose directory is gone does not start."""
        result = await self._process.exec(command, cwd="/", timeout=10)
        code = result.exit_code
        return ExecResult(stdout=result.result or "", stderr="", exit_code=-1 if code is None else code)

    async def _release(self) -> None:
        try:
            await self._process.delete_session(self._session_id)
        except DaytonaNotFoundError:
            pass
