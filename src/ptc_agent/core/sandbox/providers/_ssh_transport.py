"""SSH channels for computers on machines their owners bring.

One connection per machine per process, reused by every runtime that reaches
the machine: OpenSSH 9.8 and later penalise a source that keeps opening
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
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any

import asyncssh
import structlog

from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget, connect
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
# Kept from a command's output, so one runaway command cannot take the
# server's memory. Past it the rest is read and dropped, so the command is
# never blocked writing.
OUTPUT_CAP = 32 * 1024 * 1024
_READ_CHUNK = 64 * 1024
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
# A connection nothing has used for this long is closed, unless a channel on
# it is still open (a preview socket, say).
_IDLE_CLOSE_S = 600
_SWEEP_S = 60

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


async def _exit_sftp(sftp: Any) -> None:
    if sftp is not None:
        sftp.exit()


class Link:
    """One connection and what rides on it, pooled or a one-off's."""

    def __init__(self, target: SSHTarget, conn: asyncssh.SSHClientConnection) -> None:
        self.target = target
        self.conn = conn
        self.facts: HostFacts | None = None
        self.slots = asyncio.Semaphore(_COMMAND_SLOTS)
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

    @property
    def usable(self) -> bool:
        return not self.retired and not self.conn.is_closed()

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
        caller can tell a command that never ran from one that may have. The
        slot taken, a command slot unless *slot* names another, is given back
        when the channel closes, however that comes.
        """
        slot = self.slots if slot is None else slot
        await slot.acquire()
        command = f"exec sudo -n {BOOT.removeprefix('exec ')}" if root else BOOT
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
        self, script: str, *, timeout: float, root: bool = False, kill: bool = False
    ) -> ScriptResult:
        """Run a short management script and wait for it.

        It takes the control slot, or the kill slot with *kill*, never a
        command slot, so long commands cannot hold it up.

        Raises:
            TimeoutError: It ran past *timeout*; its channel is closed.
        """
        slot = self.kill_slot if kill else self.control_slot
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
        )
        return self.facts

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


class SSHPool:
    """Pooled connections of one event loop, keyed by target identity."""

    def __init__(self) -> None:
        self._links: dict[tuple[Any, ...], Link] = {}
        self._dialing: dict[tuple[Any, ...], asyncio.Future[Link]] = {}
        self._sweeper: asyncio.Task[None] | None = None

    async def link(
        self, target: SSHTarget, load_key: Callable[[], Awaitable[str]]
    ) -> Link:
        """The target's live connection, dialing it once however many ask.

        Raises:
            SSHConnectError: The machine could not be reached or refused us.
        """
        key = target.pool_key
        link = self._links.get(key)
        if link is not None and link.usable:
            return link
        pending = self._dialing.get(key)
        if pending is None:
            pending = asyncio.ensure_future(self._dial(target, load_key))
            self._dialing[key] = pending
            pending.add_done_callback(lambda _: self._dialing.pop(key, None))
        return await asyncio.shield(pending)

    async def _dial(
        self, target: SSHTarget, load_key: Callable[[], Awaitable[str]]
    ) -> Link:
        conn = await connect(target, await load_key())
        link = Link(target=target, conn=conn)
        try:
            await link.hello()
        except BaseException:
            link.close()
            raise
        old = self._links.get(target.pool_key)
        if old is not None:
            old.close()
        self._links[target.pool_key] = link
        self._start_sweeper()
        logger.debug("SSH connection opened", target=target.label)
        return link

    def evict(self, link: Link) -> None:
        """Drop *link* after a transport error, so the next call dials again."""
        if self._links.get(link.target.pool_key) is link:
            del self._links[link.target.pool_key]
        link.close()

    def shed(self, link: Link) -> None:
        """Dial anew for the next call, closing *link* once its calls are done.

        For a connection that works but will not open channels: closing it
        at once would cut off the commands still running on it.
        """
        if self._links.get(link.target.pool_key) is link:
            del self._links[link.target.pool_key]
        link.drain()

    def peek(self, target: SSHTarget) -> Link | None:
        """The target's connection if one is open, without dialing."""
        link = self._links.get(target.pool_key)
        return link if link is not None and link.usable else None

    def retire(self, target: SSHTarget, *, only_if_idle: bool = False) -> None:
        """Close the target's connection, as a stop does.

        Other computers on the same login share it, so ``only_if_idle`` leaves
        one that is in use, or holds a forward, to the idle sweep.
        """
        link = self._links.get(target.pool_key)
        # A forward is a preview another computer on the login may be showing
        # between requests; the sweep closes it once nothing has used it.
        if link is None or (only_if_idle and (not link.idle() or link.forwards)):
            return
        del self._links[target.pool_key]
        link.close()

    def _start_sweeper(self) -> None:
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.ensure_future(self._sweep())

    async def _sweep(self) -> None:
        while self._links:
            await asyncio.sleep(_SWEEP_S)
            cutoff = time.monotonic() - _IDLE_CLOSE_S
            for key, link in list(self._links.items()):
                if not link.usable:
                    del self._links[key]
                elif link.idle() and link.last_used <= cutoff:
                    del self._links[key]
                    link.close()
                    logger.debug("Closed idle SSH connection", target=link.target.label)


_pools: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, SSHPool] = (
    weakref.WeakKeyDictionary()
)


def pool() -> SSHPool:
    """The running loop's pool. Connections never cross event loops."""
    loop = asyncio.get_running_loop()
    found = _pools.get(loop)
    if found is None:
        found = _pools[loop] = SSHPool()
    return found


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
