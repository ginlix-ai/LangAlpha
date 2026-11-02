"""One script in a bash of its own on a computer's machine, read while it runs.

An SSH channel and a local subprocess differ only in how a script starts,
says it has exited, and is given up. Waiting for its output, and ending a
framed command's process group on a timeout or a cancel, are the same over
both, so each transport keeps a small subclass of ``ScriptRun``.
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

import structlog

from ptc_agent.core.sandbox.providers._ssh_shell import (
    FRAME_PGID,
    FramedResult,
    kill_group_script,
)

logger = structlog.get_logger(__name__)

READ_CHUNK = 64 * 1024
STDERR_CAP = 1 << 20
# How often a running script is checked for having exited, and how long its
# output may then go quiet before the call returns without the rest.
EXIT_POLL_S = 0.1
AFTER_EXIT_QUIET_S = 1.0
# How long a command ended as it began may take to report its group, which
# can already be running when the end comes, and how long an ended group's
# script may take to end after it.
GROUP_WAIT_S = 2.0

# Held until they end: work that outlives its caller (a cancelled command's
# kill) must not be collected half done. Execution context; nothing reads it.
_background: set[asyncio.Task[Any]] = set()


def spawn(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    task = asyncio.ensure_future(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)
    return task


@dataclass
class ScriptResult:
    """What a script run on the machine produced."""

    exit_code: int | None  # None when it ended without one (a signal, a lost channel)
    stdout: bytes
    stderr: bytes
    stdout_bytes: int = 0  # all of it, including what OUTPUT_CAP dropped


class Drain:
    """Reads one stream to its end, keeping up to *cap* bytes of it."""

    def __init__(self, stream: Any, cap: int) -> None:
        self.buf = bytearray()
        self.total = 0
        self.task = asyncio.ensure_future(self._read(stream, cap))

    async def _read(self, stream: Any, cap: int) -> None:
        while chunk := await stream.read(READ_CHUNK):
            self.total += len(chunk)
            room = cap - len(self.buf)
            if room > 0:
                self.buf += chunk[:room]


class ScriptRun(ABC):
    """A started script, its stdout kept up to *cap* bytes."""

    def __init__(self, stdout: Any, stderr: Any, cap: int) -> None:
        self._out = Drain(stdout, cap)
        self._err = Drain(stderr, STDERR_CAP)

    @property
    def stdout(self) -> bytes:
        return bytes(self._out.buf)

    @property
    def stderr(self) -> bytes:
        return bytes(self._err.buf)

    @property
    def stdout_bytes(self) -> int:
        return self._out.total

    @property
    def stderr_ended(self) -> bool:
        return self._err.task.done()

    @property
    @abstractmethod
    def exit_code(self) -> int | None:
        """None when a signal ended it, or it has not ended."""

    @abstractmethod
    def _exited(self) -> bool: ...

    @abstractmethod
    async def _closed(self) -> None:
        """Wait for the script's end once its output has."""

    @abstractmethod
    def _close(self) -> None:
        """Give up the script's streams, whatever still holds them."""

    @abstractmethod
    async def _kill_group(self, script: str, *, root: bool) -> bool:
        """Run *script*, which ends a process group, beside this one; False
        when there is no way to run it."""

    def kill(self) -> None:
        """End the script's own shell, where the transport can apart from
        closing its streams."""

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Run *coro* in the background, held until it ends."""
        return spawn(coro)

    def _log_fields(self) -> dict[str, Any]:
        return {}

    async def _settle(self) -> None:
        """Give an ended group's script a moment to end with it."""
        deadline = time.monotonic() + GROUP_WAIT_S
        while not self._exited() and time.monotonic() < deadline:
            await asyncio.sleep(EXIT_POLL_S)

    async def finish(self) -> ScriptResult:
        """Wait for the script, and for its output once it has exited.

        Whatever it started in the background may hold its output open until
        that ends (``cmd &``), so once the script has exited, output is
        awaited only while it keeps coming.

        Cut short, it leaves the output read until the caller abandons the
        run: the group's id may still be on its way, and the last of what the
        group wrote before it was ended.
        """
        tasks = (self._out.task, self._err.task)
        while pending := {t for t in tasks if not t.done()}:
            if not self._exited():
                await asyncio.wait(pending, timeout=EXIT_POLL_S)
                continue
            seen = self._out.total + self._err.total
            await asyncio.wait(pending, timeout=AFTER_EXIT_QUIET_S)
            if (
                any(not t.done() for t in tasks)
                and self._out.total + self._err.total == seen
            ):
                self.abandon()
                break
        else:
            for task in tasks:
                task.result()
            await self._closed()
        return ScriptResult(
            exit_code=self.exit_code,
            stdout=self.stdout,
            stderr=self.stderr,
            stdout_bytes=self._out.total,
        )

    def abandon(self) -> None:
        for drain in (self._out, self._err):
            if drain.task.done():
                if not drain.task.cancelled():
                    drain.task.exception()  # read, so it is never logged as lost
            else:
                drain.task.cancel()
        self._close()

    async def end_group(self, computer_dir: str, *, root: bool = False) -> None:
        """End a framed command's group and whatever it started, then its shell."""
        pgid: int | None = None
        try:
            pgid = await reported_group(self)
            if pgid is not None and await self._kill_group(
                kill_group_script(computer_dir, pgid, root=root), root=root
            ):
                # The group is gone, so its shell ends on its own; give the
                # last of its output a moment to arrive.
                await self._settle()
        except Exception as e:
            logger.warning(
                "Could not end a command's process group",
                pgid=pgid,
                error=str(e) or type(e).__name__,
                **self._log_fields(),
            )
        finally:
            self.kill()
            self.abandon()


async def reported_group(run: Any) -> int | None:
    """The group a framed script reported, waited for a moment if it has not yet.

    A command ended just as it began can be running while its group's id is
    still on the way. Only a whole line counts: a cut-off id would name some
    other group.
    """
    deadline = time.monotonic() + GROUP_WAIT_S
    while True:
        stderr = run.stderr
        marker = stderr.find(FRAME_PGID)
        if marker >= 0 and stderr.find(b"\n", marker) >= 0:
            return FramedResult.parse(b"", stderr, exit_code=None).pgid
        if run.stderr_ended or time.monotonic() >= deadline:
            return None
        await asyncio.sleep(EXIT_POLL_S)


async def finish_framed(
    run: ScriptRun,
    *,
    computer_dir: str,
    timeout: float,
    root: bool = False,
    lost: tuple[type[BaseException], ...] = (),
) -> FramedResult:
    """Wait for a started framed script; on timeout or cancel, end its group.

    An error in *lost* says the transport went away under the script, which
    may still be running and cannot be reached to end it: the run is given
    up and the error raised as it is.
    """
    try:
        async with asyncio.timeout(timeout):
            result = await run.finish()
    except TimeoutError:
        # Shielded: a cancel that comes during the kill must not stop it.
        await asyncio.shield(run.spawn(run.end_group(computer_dir, root=root)))
        return FramedResult.parse(
            run.stdout,
            run.stderr,
            exit_code=None,
            timed_out=True,
            stdout_bytes=run.stdout_bytes,
        )
    except lost:
        run.abandon()
        raise
    except BaseException:
        # The caller gave up, or reading failed; what it started must not
        # keep running unowned.
        run.spawn(run.end_group(computer_dir, root=root))
        raise
    return FramedResult.parse(
        result.stdout,
        result.stderr,
        exit_code=result.exit_code,
        stdout_bytes=result.stdout_bytes,
    )
