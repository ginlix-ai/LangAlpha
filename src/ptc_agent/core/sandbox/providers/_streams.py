"""What every provider's ``StreamProcess`` shares.

A provider hands a stream's output over in pieces, through callbacks or a
socket it reads, so each stream feeds two readers itself. No provider gives a
handle that signals a whole process tree, so the command runs as a process
group of its own whose leader first names its pid on stderr, which the
stream takes off before stderr reaches the reader, and a signal goes to that
group through an exec. The group's id is also kept in a file while the
command runs, for a stream that failed before its pid reached it.
"""

from __future__ import annotations

import asyncio
import shlex
import uuid
from abc import abstractmethod
from collections.abc import Coroutine
from typing import Any

import structlog

from ptc_agent.core.sandbox.runtime import ExecResult, SandboxRuntime, StreamProcess

logger = structlog.get_logger(__name__)

PID_MARK = "__langalpha_stream_pid__"

# The readers' line limit. A stream-json line can carry a whole file, so a
# reader's readline must take megabytes.
_LINE_LIMIT = 64 * 1024 * 1024
# What the two readers, and output held back from them, may hold unread
# between them before the stream fails.
# The provider pushes output whether or not it is read, and code in the
# sandbox can write to the process's stdout as fast as the network goes, so
# without a cap a stream could grow the server's memory without bound.
MAX_BUFFERED = 2 * _LINE_LIMIT
# How long a signal waits for the pid line, which a provider delivers like
# any output, a round trip after the start.
_PID_WAIT_S = 5.0
# How long a signal's exec may take: a provider that stops answering must not
# hold the caller.
_KILL_WAIT_S = 20.0
# How long close may take to kill the process and see it end, then to free
# the stream: every caller awaits it, so 14 s at most however the provider
# answers, which leaves the kill room after a full wait for the pid line.
_CLOSE_WAIT_S = 10.0
_RELEASE_WAIT_S = 4.0
# The status of a kill whose pid file names no process group.
_BAD_PID_FILE = 3


def stream_pid_file() -> str:
    """A path of its own for one stream's ``pid_marked`` file."""
    return f"/tmp/.langalpha-stream-{uuid.uuid4().hex}"


def pid_marked(command: str, pid_file: str) -> str:
    """Shell that runs ``bash -c command`` as a process group of its own, whose
    leader names its pid on stderr before anything else, and exits with the
    command's status. The group's id is in *pid_file*, readable by its owner
    alone, until the command ends.

    Signalling the group reaches everything the command started (a pipeline,
    a child it forked), which signalling one pid would leave running. Job
    control gives the job its group without ``setsid``, which forks when its
    caller already leads a group and so would name the wrong pid. The job is
    forked while job control is on, which keeps the wrapper's stdin for it,
    and job control is off again before ``wait``, so the wrapper reports no
    job status on stderr.
    """
    leader = f'echo "{PID_MARK} $$" >&2; exec bash -c "$1"'
    file = shlex.quote(pid_file)
    return (
        f"set -m; bash -c {shlex.quote(leader)} stream {shlex.quote(command)} & "
        f'set +m; job=$!; (umask 077; set -C; echo "$job" > {file}); '
        f'wait "$job"; status=$?; rm -f {file}; exit "$status"'
    )


def _cancelling() -> bool:
    """Whether the running task is being cancelled, not just handed a
    CancelledError from a send that teardown cancelled."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _buffered(reader: asyncio.StreamReader) -> int:
    """Bytes *reader* holds unread. CPython's StreamReader keeps them in
    ``_buffer``, as it has since 3.4 and still does in 3.13, the version this
    runs on, and says so nowhere public."""
    return len(reader._buffer)


class PidLine:
    """Takes the pid mark off the front of stderr, however it is split."""

    def __init__(self) -> None:
        self._head = bytearray()
        self._done = False
        self.pid: int | None = None

    @property
    def done(self) -> bool:
        return self._done

    def feed(self, data: bytes) -> bytes:
        """The part of *data* that is the process's own stderr."""
        if self._done:
            return data
        self._head += data
        line, newline, rest = self._head.partition(b"\n")
        if not newline:
            return b""
        self._done = True
        mark, _, pid = line.decode(errors="replace").partition(" ")
        if mark == PID_MARK and pid.isdigit():
            self.pid = int(pid)
            return bytes(rest)
        # Not the mark (bash could not start), so all of it is the process's own.
        return bytes(self._head)

    def flush(self) -> bytes:
        """What was held back waiting for a newline that never came."""
        if self._done:
            return b""
        self._done = True
        return bytes(self._head)


class FedStream(StreamProcess):
    """A stream whose provider pushes its output in, started through
    ``pid_marked``, for a subclass that sends.

    Writes go one at a time, in call order, because a provider's stdin is a
    request per write and two in flight can land out of order. A subclass
    runs its readers through ``_spawn``, feeds output through ``_out`` and
    ``_err``, and calls ``_end`` once the process ended, or ``_fail`` once it can
    no longer tell what the process is doing. A signal goes to the process
    group by a kill exec, and one that cannot be sent fails the stream, so
    nothing waits on a process no one can end. A send that fails, other than
    on input that is not UTF-8, fails the stream too: it may or may not have
    arrived, and a line protocol can neither send it again nor go on without
    it. Teardown runs once, as a task every ``close`` awaits, so a close that
    is cancelled or times out still frees what the stream holds.
    """

    def __init__(self, runtime: SandboxRuntime, pid_file: str) -> None:
        self.stdout = asyncio.StreamReader(limit=_LINE_LIMIT)
        self.stderr = asyncio.StreamReader(limit=_LINE_LIMIT)
        self._runtime = runtime
        self._pid_file = pid_file
        self._pid_line = PidLine()
        self._pid_known = asyncio.Event()
        self._ended = asyncio.Event()
        self._returncode: int | None = None
        self._failure: str | None = None
        self._write_lock = asyncio.Lock()
        self._stdin_closed = False
        self._tasks: set[asyncio.Task[Any]] = set()
        self._teardown: asyncio.Task[None] | None = None

    @property
    def returncode(self) -> int | None:
        return self._returncode

    @property
    def failure(self) -> str | None:
        return self._failure

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _out(self, data: bytes) -> None:
        self._feed(self.stdout, data)

    def _err(self, data: bytes) -> None:
        if not self._pid_line.done:
            data = self._pid_line.feed(data)
            if self._pid_line.done:
                self._pid_known.set()
        self._feed(self.stderr, data)

    def _feed(self, reader: asyncio.StreamReader, data: bytes) -> None:
        if not data or self._ended.is_set():
            return
        reader.feed_data(data)
        self._check_buffered()

    def _check_buffered(self) -> None:
        if _buffered(self.stdout) + _buffered(self.stderr) + self._held() > MAX_BUFFERED:
            self._fail(f"more than {MAX_BUFFERED} bytes of output went unread")

    def _held(self) -> int:
        """Output the provider handed over that waits for more before it can
        be fed to a reader."""
        return 0

    def _end(self, returncode: int | None) -> None:
        if self._ended.is_set():
            return
        self._feed(self.stderr, self._pid_line.flush())
        if self._ended.is_set():
            return
        self._returncode = -1 if returncode is None else returncode
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        self._ended.set()
        self._pid_known.set()

    def _fail(self, reason: str) -> None:
        """End the stream as lost and kill the process, which may still run."""
        if self._ended.is_set():
            return
        self._failure = reason
        logger.warning("Sandbox stream failed", sandbox_id=self._runtime.id, reason=reason)
        self._end(None)
        self._spawn(self._signal("KILL"))

    async def wait(self) -> int:
        await self._ended.wait()
        assert self._returncode is not None
        return self._returncode

    async def write(self, data: bytes) -> None:
        async with self._write_lock:
            if self._ended.is_set() or self._teardown is not None:
                raise ConnectionError("The process has ended")
            if self._stdin_closed:
                raise ConnectionError("Its stdin is closed")
            try:
                await self._spawn(self._send(data))
            except ValueError:
                raise
            except (Exception, asyncio.CancelledError) as exc:
                if self._teardown is None:
                    self._fail(f"a write to it was lost: {str(exc) or type(exc).__name__}")
                elif not _cancelling():
                    raise ConnectionError("The stream was closed") from exc
                if isinstance(exc, (ConnectionError, asyncio.CancelledError)):
                    raise
                raise ConnectionError(f"Could not write to the process: {exc}") from exc

    async def write_eof(self) -> bool:
        async with self._write_lock:
            if self._ended.is_set() or self._teardown is not None or self._stdin_closed:
                return True
            try:
                self._stdin_closed = await self._spawn(self._send_eof())
            except (Exception, asyncio.CancelledError) as exc:
                if self._teardown is None:
                    self._fail(f"closing its stdin was lost: {str(exc) or type(exc).__name__}")
                elif not _cancelling():
                    raise ConnectionError("The stream was closed") from exc
                raise
            return self._stdin_closed

    async def terminate(self) -> None:
        if not self._ended.is_set():
            await self._signal("TERM")

    async def kill(self) -> None:
        if not self._ended.is_set():
            await self._signal("KILL")

    async def _signal(self, name: str) -> None:
        """Send signal *name* (TERM, KILL) to the process group, ended or not:
        a failed stream's process may still run."""
        try:
            async with asyncio.timeout(_PID_WAIT_S):
                await self._pid_known.wait()
        except TimeoutError:
            pass
        pid = self._pid_line.pid
        if pid is None:
            # Its pid line never came, so the group's id is read from the file,
            # which the shell writes a moment after the command starts. One
            # still missing after that is a command that already ended.
            file = shlex.quote(self._pid_file)
            kill = (
                f"for _ in 1 2 3 4 5 6 7 8 9 10; do pgid=$(cat {file} 2>/dev/null); "
                '[ -n "$pgid" ] && break; sleep 0.1; done; [ -n "$pgid" ] || exit 1; '
                f'case "$pgid" in *[!0-9]*) exit {_BAD_PID_FILE};; esac; '
                f'[ "$pgid" -gt 1 ] 2>/dev/null || exit {_BAD_PID_FILE}; '
                f'kill -{name} -- -"$pgid"'
            )
        else:
            kill = f"kill -{name} -- -{pid}"
        try:
            async with asyncio.timeout(_KILL_WAIT_S):
                result = await self._run_kill(kill)
        except Exception as exc:  # noqa: BLE001 - a process no one can signal fails the stream
            self._not_killed(str(exc) or type(exc).__name__)
            return
        # A negative status is an exec that did not run; kill's own 1 is a
        # group already gone (its file too), which the stream's reader sees end.
        if result.exit_code < 0:
            self._not_killed(result.stderr or "the kill exec did not run")
        elif pid is None and result.exit_code == _BAD_PID_FILE:
            self._not_killed("its pid file names no process group")

    def _not_killed(self, why: str) -> None:
        """A signal that could not be sent fails the stream, and on a stream
        that already failed it joins the reason, since its process may still
        run."""
        reason = f"it could not be killed: {why}"
        if not self._ended.is_set():
            self._fail(reason)
        elif self._failure is not None:
            logger.warning("Sandbox stream failed", sandbox_id=self._runtime.id, reason=reason)
            self._failure = f"{self._failure}; {reason}"

    async def _run_kill(self, command: str) -> ExecResult:
        """Run a signal's command in the sandbox."""
        return await self._runtime.exec(command, timeout=10)

    async def close(self) -> None:
        if self._teardown is None:
            self._teardown = asyncio.create_task(self._tear_down())
        await asyncio.shield(self._teardown)

    async def _tear_down(self) -> None:
        signalled = False
        try:
            async with asyncio.timeout(_CLOSE_WAIT_S):
                if self._failure is not None or not self._ended.is_set():
                    await self._signal("KILL")
                    signalled = True
                    await self._ended.wait()
        except TimeoutError:
            if not signalled:
                self._not_killed("TimeoutError")
            else:
                logger.warning("Sandbox stream did not end on close", sandbox_id=self._runtime.id)
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            async with asyncio.timeout(_RELEASE_WAIT_S):
                await self._release()
        except Exception as exc:  # noqa: BLE001 - nothing more to free
            logger.warning(
                "Could not free a sandbox stream",
                sandbox_id=self._runtime.id,
                error=str(exc) or type(exc).__name__,
            )
        self._end(None)

    @abstractmethod
    async def _send(self, data: bytes) -> None: ...

    async def _send_eof(self) -> bool:
        return False

    async def _release(self) -> None:
        """Free what the provider holds for the stream, once its tasks are gone."""
