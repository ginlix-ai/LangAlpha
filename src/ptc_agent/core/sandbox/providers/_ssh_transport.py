"""SSH channels for computers on machines their owners bring.

One connection per machine per process (``_ssh_pool``), reused by every
runtime that reaches the machine, and one more for Claude Code CLIs past its
slots: OpenSSH 9.8 and later penalise a source that keeps opening
connections and dropping them, and a fresh handshake per command costs more
than the command. The pool is execution context only. Whether a computer runs
is read from the machine on each call, so every worker agrees.

Every command reaches bash as a script on stdin (``_ssh_shell.BOOT``). Secrets
in its env stay out of argv, there is no argv size limit, and a fish or csh
login shell never parses it. The script runs the command in its own process
group and records the group, so a timeout, a cancel or a stop can end
everything the command started.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import posixpath
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Generic, Literal, NamedTuple, TypeVar

import asyncssh
import structlog

from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget
from ptc_agent.core.sandbox.providers._ssh_shell import (
    BOOT,
    FRAME_BEGIN,
    FRAME_PGID,
    FRAME_STOPPED,
    HELLO,
    TERM_GRACE_S,
    kill_group_script,
)
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError

logger = structlog.get_logger(__name__)

# sshd's MaxSessions defaults to 10 per connection. Commands take 6 and the
# SFTP channel one. Our own short scripts (state reads, starts, stops) keep one
# and kills another, so a connection full of long commands can still read a
# computer's state, move its files, stop it and end a command. The tenth stays
# free: sshd holds a session until its process exits, which can come after we
# closed the channel.
_COMMAND_SLOTS = 6
# A Claude Code turn's CLI holds a command slot for the whole turn, so no more
# than this many run on a connection, leaving two for the commands, installs
# and checks those turns and their computers still need. CLIs past them run on
# one more connection to the login (``Link.spill``), which carries only
# those CLIs and their kills.
CLI_SLOTS = _COMMAND_SLOTS - 2
# Kept from a command's output, so one runaway command cannot take the
# server's memory. Past it the rest is read and dropped, so the command is
# never blocked writing.
OUTPUT_CAP = 32 * 1024 * 1024
_READ_CHUNK = 64 * 1024
# What an upload reads of its local file at a time, off the event loop; the
# SFTP channel writes each in parallel requests.
_UPLOAD_CHUNK = 4 * 1024 * 1024
# How often a running script is checked for having exited, and how long its
# output may then go quiet before the call returns without the rest.
_EXIT_POLL_S = 0.1
_AFTER_EXIT_QUIET_S = 1.0
# How long a command ended as it began may take to report its group, which
# can already be running when the end comes.
_GROUP_WAIT_S = 2.0
# Opening the SFTP channel, so a machine whose sftp-server hangs (a home on
# a stalled NFS mount, say) fails file calls rather than holding them.
_SFTP_OPEN_S = 30

TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (
    asyncssh.DisconnectError,
    asyncssh.ChannelOpenError,
    asyncssh.SFTPConnectionLost,
    asyncssh.SFTPNoConnection,
    ConnectionError,
    BrokenPipeError,
    EOFError,
)


@dataclass
class ScriptResult:
    """What a script run on the machine produced."""

    exit_code: int | None  # None when the channel closed without one
    stdout: bytes
    stderr: bytes
    stdout_bytes: int = 0  # all of it, including what OUTPUT_CAP dropped


class _Drain:
    """Reads one stream to its end, keeping up to *cap* bytes of it."""

    def __init__(self, stream: Any, cap: int) -> None:
        self.buf = bytearray()
        self.total = 0
        self.task = asyncio.ensure_future(self._read(stream, cap))

    async def _read(self, stream: Any, cap: int) -> None:
        while True:
            chunk = await stream.read(_READ_CHUNK)
            if not chunk:
                return
            self.total += len(chunk)
            room = cap - len(self.buf)
            if room > 0:
                self.buf += chunk[:room]


class _Run:
    """One script on one channel, readable while it runs."""

    def __init__(self, process: Any, cap: int) -> None:
        self.process = process
        self._out = _Drain(process.stdout, cap)
        self._err = _Drain(process.stderr, 1 << 20)

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

    def _exited(self) -> bool:
        return (
            self.process.exit_status is not None or self.process.exit_signal is not None
        )

    async def finish(self) -> ScriptResult:
        """Wait for the script, and for its output once it has exited.

        sshd holds a channel open while anything still has its output open,
        so ``cmd &`` would hold the call until the background job ends. Once
        the script has exited, output is awaited only while it keeps coming.

        Cut short, it leaves the output read until the caller abandons the
        run: the group's id may still be on its way, and the last of what
        the group wrote before it was ended.
        """
        tasks = (self._out.task, self._err.task)
        while pending := {t for t in tasks if not t.done()}:
            if not self._exited():
                await asyncio.wait(pending, timeout=_EXIT_POLL_S)
                continue
            seen = self._out.total + self._err.total
            await asyncio.wait(pending, timeout=_AFTER_EXIT_QUIET_S)
            if (
                any(not t.done() for t in tasks)
                and self._out.total + self._err.total == seen
            ):
                self.abandon()
                break
        else:
            for task in tasks:
                task.result()
            await self.process.wait_closed()
        return ScriptResult(
            exit_code=self.process.exit_status
            if self.process.exit_signal is None
            else None,
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
        with contextlib.suppress(Exception):
            self.process.close()


@dataclass
class HostFacts:
    """What one look at the machine says about it, read once per connection."""

    uid: int
    user: str
    home: str
    os: str
    arch: str
    has_sudo: bool | None = None  # read the first time a command needs root
    # sudo refused only for want of a terminal (sudoers' requiretty).
    sudo_needs_tty: bool = False
    # Where the machine sees this connection come from; empty if it did not say.
    client_address: str = ""
    # The login's $XDG_CONFIG_HOME, which places its Claude Code token; empty when unset.
    config_home: str = ""


async def _release_when_closed(process: Any, slot: asyncio.Semaphore) -> None:
    try:
        await process.wait_closed()
    except Exception:
        pass
    finally:
        slot.release()


async def _after_open(
    opening: asyncio.Future[Any], then: Callable[[Any], Awaitable[None]]
) -> None:
    """Hand an open nobody waits for any more to *then*: the channel, or None
    when it did not open."""
    await asyncio.wait((opening,))
    failed = opening.cancelled() or opening.exception() is not None
    await then(None if failed else opening.result())


async def _give_back(slot: asyncio.Semaphore, process: Any) -> None:
    if process is None:
        slot.release()
        return
    process.close()
    await _release_when_closed(process, slot)


async def _end_upload(slot: asyncio.Semaphore, sftp: Any, *, partial: str | None) -> None:
    """Remove a failed upload's *partial* file, close its channel, and give
    its slot back once the channel is closed."""
    try:
        if sftp is None:
            return
        if partial is not None:
            with contextlib.suppress(Exception):
                await sftp.remove(partial)
        sftp.exit()
        with contextlib.suppress(Exception):
            await sftp.wait_closed()
    finally:
        slot.release()


async def _exit_sftp(sftp: Any) -> None:
    if sftp is not None:
        sftp.exit()


class SlotWaitTimeout(TimeoutError):
    """No slot of the kind ``which`` freed in the time a wait was given; for
    a CLI, ``n`` is how many were running where it waited."""

    def __init__(self, which: Literal["cli", "command"], n: int = 0) -> None:
        super().__init__(f"no {which} slot freed in time")
        self.which = which
        self.n = n


class CliHold(NamedTuple):
    """The connection a CLI runs on, where it is signalled, and what starts
    it there (``Link.cli_hold``)."""

    link: Any
    start: Callable[[str], Awaitable[Any]]


class Link:
    """One connection and what rides on it, pooled or a one-off's."""

    def __init__(self, target: SSHTarget, conn: asyncssh.SSHClientConnection) -> None:
        self.target = target
        self.conn = conn
        self.facts: HostFacts | None = None
        self.slots = asyncio.Semaphore(_COMMAND_SLOTS)
        self.cli_slots = asyncio.Semaphore(CLI_SLOTS)
        self.control_slot = asyncio.Semaphore(1)
        self.kill_slot = asyncio.Semaphore(1)
        self.forwards: dict[int, Any] = {}
        self.last_used = time.monotonic()
        self.active = 0
        self.retired = False
        # Out of the pool, closed once the calls on it are done.
        self.draining = False
        # Channel opens refused in a row, which says sshd holds sessions of
        # this connection we no longer count.
        self.refusals = 0
        self._sftp: asyncssh.SFTPClient | None = None
        self._sftp_lock = asyncio.Lock()
        self._forward_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[Any]] = set()
        # The login's other connection, for CLIs past this one's slots,
        # dialed once needed; None on a one-off's, whose CLIs wait for this
        # one's.
        self.spill: Callable[[], Awaitable[Link]] | None = None
        # Set as a CLI slot frees on any of the login's connections, which
        # share it once pooled, so a CLI waits for whichever frees first.
        self.cli_freed = asyncio.Event()

    @property
    def usable(self) -> bool:
        return not self.retired and not self.conn.is_closed()

    @property
    def host(self) -> str:
        return self.target.host

    @contextlib.asynccontextmanager
    async def in_use(self) -> AsyncIterator[None]:
        self.active += 1
        self.last_used = time.monotonic()
        try:
            yield
        finally:
            self.active -= 1
            self.last_used = time.monotonic()
            if self.draining and self.active == 0:
                self.close()

    def drain(self) -> None:
        """Take no new calls on top of the ones running, and close once
        they are done."""
        self.draining = True
        if self.active == 0:
            self.close()

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Run *coro* in the background, held here until it ends, so work
        that outlives its caller (a channel's closer, a cancelled command's
        kill) is never collected half done."""
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _open(
        self,
        opening: Coroutine[Any, Any, Any],
        then: Callable[[Any], Awaitable[None]],
    ) -> Any:
        """Await a channel's open; if the wait is cancelled, *then* gets the
        channel once the open ends.

        asyncssh leaves a channel whose open was cancelled open on the
        machine, where it holds one of the connection's sessions for good.
        """
        task = asyncio.ensure_future(opening)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            self.spawn(_after_open(task, then))
            raise

    async def open_process(
        self, command: str, *, slot: asyncio.Semaphore | None = None, held: bool = False
    ) -> Any:
        """Open a channel running *command*, with bytes on its streams.

        The slot taken, a command slot unless *slot* names another, is given
        back when the channel closes, however that comes. With *held* the
        caller already took it, waiting for it under a budget of its own,
        and it is given back the same way.
        """
        slot = self.slots if slot is None else slot
        if not held:
            await slot.acquire()
        try:
            process = await self._open(
                self.conn.create_process(command, encoding=None),
                functools.partial(_give_back, slot),
            )
        except asyncio.CancelledError:
            raise  # _give_back holds the slot until the channel is closed
        except BaseException as e:
            if isinstance(e, asyncssh.ChannelOpenError):
                self.refusals += 1
            slot.release()
            raise
        self.refusals = 0
        self.spawn(_release_when_closed(process, slot))
        return process

    async def start(
        self,
        script: str,
        *,
        root: bool = False,
        cap: int = OUTPUT_CAP,
        slot: asyncio.Semaphore | None = None,
    ) -> _Run:
        """Open a channel and send *script*; the caller reads it and finishes it.

        A transport error raised here came before anything was sent, so the
        caller can tell a command that never ran from one that may have.
        """
        command = f"exec sudo -n {BOOT.removeprefix('exec ')}" if root else BOOT
        process = await self.open_process(command, slot=slot)
        run = _Run(process, cap)
        try:
            process.stdin.write(script.encode())
            process.stdin.write_eof()
        except BaseException:
            # The machine closed the channel before the script's end reached
            # it, so nothing ran; closing our side frees the slot.
            run.abandon()
            raise
        return run

    async def run(
        self,
        script: str,
        *,
        timeout: float,
        root: bool = False,
        kill: bool = False,
        long: bool = False,
        slot: asyncio.Semaphore | None = None,
    ) -> ScriptResult:
        """Run a script and wait for it.

        A short management script takes the control slot, or the kill slot
        with *kill*, never a command slot, so long commands cannot hold it up.
        A long one (*long*, or a *slot* of its own) takes a command slot so it
        never holds the control slot.

        Raises:
            TimeoutError: It ran past *timeout*; its channel is closed.
        """
        if slot is None:
            slot = self.slots if long else self.kill_slot if kill else self.control_slot
        async with self.in_use(), asyncio.timeout(timeout):
            run = await self.start(script, root=root, slot=slot)
            try:
                return await run.finish()
            except BaseException:
                run.abandon()
                raise

    async def hello(self) -> HostFacts:
        result = await self.run(HELLO, timeout=self.target.connect_timeout + 10)
        lines = result.stdout.decode(errors="replace").splitlines()
        if result.exit_code != 0 or len(lines) < 5 or not lines[0].isdigit():
            detail = result.stderr.decode(errors="replace").strip()
            raise SSHConnectError(
                "unsupported",
                f"{self.target.label} could not run bash"
                + (f" ({detail})." if detail else "."),
            )
        self.facts = HostFacts(
            uid=int(lines[0]),
            user=lines[1],
            home=lines[2],
            os=lines[3],
            arch=lines[4],
            client_address=lines[5] if len(lines) > 5 else "",
            config_home=lines[6] if len(lines) > 6 else "",
        )
        return self.facts

    async def upload(self, local: str, remote: str, *, timeout: float) -> None:
        """Send a large file on an SFTP channel of its own, in a command slot,
        so the kept channel file calls use never waits behind it. A send that
        fails leaves no partial file behind. The local file is read in a
        thread, as ``put`` would read it on the event loop.

        Raises:
            TimeoutError: It ran past *timeout*.
        """
        async with self.in_use(), asyncio.timeout(timeout):
            await self.slots.acquire()
            try:
                sftp = await self._open(
                    self.conn.start_sftp_client(),
                    functools.partial(_end_upload, self.slots, partial=None),
                )
            except asyncio.CancelledError:
                raise  # _end_upload gives the slot back once the channel is closed
            except BaseException:
                self.slots.release()
                raise
            sent = False
            try:
                src = await asyncio.to_thread(open, local, "rb")
                try:
                    async with sftp.open(remote, "wb") as dst:
                        while chunk := await asyncio.to_thread(src.read, _UPLOAD_CHUNK):
                            await dst.write(chunk)
                finally:
                    await asyncio.to_thread(src.close)
                sent = True
            finally:
                self.spawn(
                    _end_upload(self.slots, sftp, partial=None if sent else remote)
                )

    async def write_files(self, files: list[tuple[str, bytes]]) -> None:
        """Write each file whole and private (``write_atomic``) on the kept SFTP channel."""
        async with self.in_use():
            sftp = await self.sftp()
            try:
                for path, data in files:
                    await write_atomic(sftp, path, data, mode=0o600)
            except TRANSPORT_ERRORS:
                self.drop_sftp(sftp)
                raise

    @contextlib.asynccontextmanager
    async def cli_hold(self, wait_s: float) -> AsyncIterator[CliHold]:
        """Hold what a long-lived CLI needs: a CLI slot on whichever of the
        login's connections has one first, this one or, once its slots are
        taken, the other (``spill``), and the command slot its channel will
        take there, all within ``wait_s``, so commands filling every command
        slot are a wait like any other. It yields the connection the CLI
        runs on and what starts it there: its channel takes the command slot
        over and gives it back as it closes; one never started gives it back
        here.

        Raises:
            SlotWaitTimeout: A slot did not free in time, naming which.
        """
        async with self.in_use():
            deadline = asyncio.get_running_loop().time() + wait_s
            link = await self._cli_slot(deadline)
            try:
                async with link.in_use(), link._cli_start(deadline) as start:
                    yield CliHold(link, start)
            finally:
                link.cli_slots.release()
                link.cli_freed.set()

    async def _cli_slot(self, deadline: float) -> Link:
        """A CLI slot on one of the login's connections, taken by
        ``deadline``: this one's while it has one free, else the other's,
        dialed once needed, else the first either frees (``cli_freed``). A
        slot is taken as it is seen free, with no wait between. A second
        connection that cannot be dialed leaves the CLI to this one's until
        it next wakes, when it looks for one again, so one another CLI dialed
        meanwhile serves it too. This
        one's slot is taken even once it is lost, so the CLI fails starting
        as a lost connection rather than waiting out its time as a full one."""
        loop = asyncio.get_running_loop()
        other = None
        while True:
            if self.cli_slots.locked() and self.spill is not None:
                try:
                    async with asyncio.timeout_at(deadline):
                        other = await self.spill()
                except Exception as e:
                    if isinstance(e, TimeoutError) and loop.time() >= deadline:
                        raise SlotWaitTimeout("cli", CLI_SLOTS if other is None else 2 * CLI_SLOTS) from None
                    logger.warning(
                        "SSH connection for more CLIs failed, waiting for one of the first's",
                        target=self.target.label, error=str(e) or type(e).__name__,
                    )
                    other = None
            for link in (self, other):
                if link is not None and (link is self or link.usable) and not link.cli_slots.locked():
                    await link.cli_slots.acquire()
                    return link
            self.cli_freed.clear()
            try:
                async with asyncio.timeout_at(deadline):
                    await self.cli_freed.wait()
            except TimeoutError:
                raise SlotWaitTimeout("cli", CLI_SLOTS if other is None else 2 * CLI_SLOTS) from None

    @contextlib.asynccontextmanager
    async def _cli_start(self, deadline: float) -> AsyncIterator[Callable[[str], Awaitable[Any]]]:
        """The command slot a CLI's channel on this connection will take, by
        ``deadline``, and what starts the CLI."""
        try:
            async with asyncio.timeout_at(deadline):
                await self.slots.acquire()
        except TimeoutError:
            raise SlotWaitTimeout("command") from None
        started = False

        async def start(command: str) -> Any:
            nonlocal started
            started = True
            return await self.open_process(command, held=True)

        try:
            yield start
        finally:
            if not started:
                self.slots.release()

    async def sftp(self) -> asyncssh.SFTPClient:
        """The connection's SFTP channel, opened once in the session kept for
        it, so file calls never wait behind commands.

        Raises:
            TimeoutError: It did not open within ``_SFTP_OPEN_S``.
        """
        sftp = self._sftp
        if sftp is not None:
            return sftp
        async with asyncio.timeout(_SFTP_OPEN_S), self._sftp_lock:
            if self._sftp is None:
                try:
                    self._sftp = await self._open(
                        self.conn.start_sftp_client(), _exit_sftp
                    )
                except asyncssh.ChannelOpenError:
                    self.refusals += 1
                    raise
                self.refusals = 0
                self.spawn(self._forget_when_closed(self._sftp))
            return self._sftp

    async def _forget_when_closed(self, sftp: asyncssh.SFTPClient) -> None:
        # One that ends between calls, its sftp-server gone, is forgotten as
        # it ends rather than at the next call's failure, so idle() never
        # discounts it from a forward's live socket.
        with contextlib.suppress(Exception):
            await sftp.wait_closed()
        if self._sftp is sftp:
            self._sftp = None

    def drop_sftp(self, dead: asyncssh.SFTPClient) -> None:
        """Forget an SFTP channel that died, so the next call opens another.

        Only *dead* is forgotten: a call that failed on it after another had
        already opened the next must not take that one down too.
        """
        if self._sftp is dead:
            self._sftp = None
        with contextlib.suppress(Exception):
            dead.exit()

    async def forward(self, port: int) -> int:
        """A loopback port on this host that reaches *port* on the machine's loopback."""
        listener = self.forwards.get(port)
        if listener is not None:
            return listener.get_port()
        async with self._forward_lock:
            listener = self.forwards.get(port)
            if listener is None:
                listener = await self.conn.forward_local_port(
                    "127.0.0.1", 0, "127.0.0.1", port
                )
                self.forwards[port] = listener
            return listener.get_port()

    def idle(self) -> bool:
        """Nothing in flight: no call, and no channel but our SFTP one, so a
        request through a forward, or a socket it holds open, is use."""
        return self.active == 0 and self.open_channels() == 0

    def open_channels(self) -> int:
        """Channels open on the connection other than our own SFTP one."""
        channels = getattr(self.conn, "_channels", None)
        if channels is None:
            return 1  # cannot tell, so never idle
        return len(channels) - (1 if self._sftp is not None else 0)

    def close(self) -> None:
        self.retired = True
        self.conn.abort()


L = TypeVar("L")


@dataclass(frozen=True)
class Machine(Generic[L]):
    """A machine work runs its own channels on (a Claude Code turn, its
    token): ``link`` returns what reaches it, the pooled connection of a
    computer of the user's own (``Link``), dialed again once one is lost, or
    a cloud computer's sandbox; ``root`` is the computer root there."""

    link: Callable[[], Awaitable[L]]
    root: Callable[[], str]


# -- Framed commands ----------------------------------------------------------


@dataclass
class FramedResult:
    """A framed command's outcome as the runtime reports it."""

    stdout: bytes
    stderr: bytes  # the command's own, when not merged
    exit_code: int | None
    pgid: int | None  # the command's group, reported before it ran
    stopped: bool  # refused: the computer is marked stopped
    timed_out: bool = False
    framing_error: str = ""  # why the command did not start, as far as known
    stdout_bytes: int = 0

    @property
    def started(self) -> bool:
        """The group began, so the command ran."""
        return self.pgid is not None

    @classmethod
    def parse(
        cls,
        stdout: bytes,
        stderr: bytes,
        *,
        exit_code: int | None,
        timed_out: bool = False,
        stdout_bytes: int = 0,
    ) -> FramedResult:
        """Read what a framed script wrote, its markers taken out.

        Anything before a marker came from the login shell's startup files, not
        from the command, and is dropped from the command's output.
        """
        begin = stdout.find(FRAME_BEGIN, 0, 1 << 16)
        if begin >= 0:
            stdout = stdout[begin + len(FRAME_BEGIN) :]
        pgid: int | None = None
        stopped = False
        marker = stderr.find(FRAME_PGID)
        if marker >= 0:
            end = stderr.find(b"\n", marker)
            end = len(stderr) if end < 0 else end
            value = stderr[marker + len(FRAME_PGID) : end]
            pgid = int(value) if value.isdigit() else None
            noise, stderr = stderr[:marker], stderr[end + 1 :]
        else:
            stopped = FRAME_STOPPED in stderr
            noise, stderr = stderr, b""
        return cls(
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            pgid=pgid,
            stopped=stopped,
            timed_out=timed_out,
            framing_error=noise.decode(errors="replace").strip(),
            stdout_bytes=stdout_bytes,
        )


class ScriptLost(ConnectionError):
    """The connection dropped while a command may have been running."""


async def run_framed(
    link: Link,
    script: str,
    *,
    computer_dir: str,
    timeout: float,
    root: bool = False,
) -> FramedResult:
    """Run a framed script; on timeout or cancel, end its process group.

    *timeout* bounds the run from when the script is sent. The wait for a
    command slot is bounded by it apart, so a command that never got one
    reads as not started rather than as one that ran too long. A transport
    error before the script was sent propagates as it is, so the caller may
    dial again and retry; one after it may have run is wrapped as
    ``ScriptLost``.
    """
    async with link.in_use():
        try:
            async with asyncio.timeout(timeout):
                run = await link.start(script, root=root)
        except TimeoutError:
            return FramedResult(
                stdout=b"",
                stderr=b"",
                exit_code=None,
                pgid=None,
                stopped=False,
                framing_error=(
                    "the command did not start: other commands held every "
                    f"channel to {link.target.host} for {timeout:g}s"
                ),
            )
        try:
            async with asyncio.timeout(timeout):
                result = await run.finish()
        except TimeoutError:
            # Shielded: a cancel that comes during the kill must not stop it.
            await asyncio.shield(
                link.spawn(_end_group(link, run, computer_dir, root=root))
            )
            return FramedResult.parse(
                run.stdout,
                run.stderr,
                exit_code=None,
                timed_out=True,
                stdout_bytes=run.stdout_bytes,
            )
        except TRANSPORT_ERRORS as e:
            run.abandon()
            raise ScriptLost(str(e) or type(e).__name__) from e
        except BaseException:
            # The caller gave up, or reading failed; what it started on the
            # machine must not keep running unowned.
            link.spawn(_end_group(link, run, computer_dir, root=root))
            raise
    return FramedResult.parse(
        result.stdout,
        result.stderr,
        exit_code=result.exit_code,
        stdout_bytes=result.stdout_bytes,
    )


async def _end_group(link: Link, run: _Run, computer_dir: str, *, root: bool) -> None:
    """Kill a framed command's group over another channel, then close its own."""
    pgid: int | None = None
    try:
        pgid = await _reported_group(run)
        if pgid is not None and link.usable:
            await link.run(
                kill_group_script(computer_dir, pgid, root=root),
                timeout=TERM_GRACE_S + 15,
                root=root,
                kill=True,
            )
            # The group is gone, so its channel closes on its own; give the
            # last of its output a moment to arrive.
            with contextlib.suppress(TimeoutError, *TRANSPORT_ERRORS):
                async with asyncio.timeout(2):
                    await asyncio.shield(run.process.wait_closed())
    except Exception as e:
        logger.warning(
            "Could not end a command's process group",
            target=link.target.label,
            pgid=pgid,
            error=str(e) or type(e).__name__,
        )
    finally:
        run.abandon()


async def _reported_group(run: _Run) -> int | None:
    """The group the script reported, waited for a moment if it has not yet.

    A command ended just as it began can be running on the machine while its
    group's id is still on the way. Only a whole line counts: a cut-off id
    would name some other group.
    """
    deadline = time.monotonic() + _GROUP_WAIT_S
    while True:
        stderr = run.stderr
        marker = stderr.find(FRAME_PGID)
        if marker >= 0 and stderr.find(b"\n", marker) >= 0:
            return FramedResult.parse(b"", stderr, exit_code=None).pgid
        if run.stderr_ended or time.monotonic() >= deadline:
            return None
        await asyncio.sleep(_EXIT_POLL_S)


# -- Files ----------------------------------------------------------------------


def temp_sibling(path: str) -> str:
    """A hidden name beside *path*, so a rename onto it stays on one filesystem."""
    parent, name = posixpath.split(path)
    return posixpath.join(parent, f".{name}.la-{uuid.uuid4().hex[:8]}")


async def write_atomic(
    sftp: asyncssh.SFTPClient, path: str, data: bytes, *, mode: int | None = None
) -> None:
    """Write *data* to *path* whole: to a temp file first, then renamed in.

    Parents are created on demand, so the common case costs no extra trip.
    """
    tmp = temp_sibling(path)
    attrs = (
        asyncssh.SFTPAttrs(permissions=mode)
        if mode is not None
        else asyncssh.SFTPAttrs()
    )
    try:
        handle = await sftp.open(tmp, "wb", attrs, encoding=None)
    except asyncssh.SFTPNoSuchFile:
        await sftp.makedirs(posixpath.dirname(path), exist_ok=True)
        handle = await sftp.open(tmp, "wb", attrs, encoding=None)
    try:
        async with handle:
            await handle.write(data)
        try:
            await sftp.posix_rename(tmp, path)
        except asyncssh.SFTPOpUnsupported:
            with contextlib.suppress(asyncssh.SFTPNoSuchFile):
                await sftp.remove(path)
            await sftp.rename(tmp, path)
    except BaseException:
        with contextlib.suppress(Exception):
            await sftp.remove(tmp)
        raise
