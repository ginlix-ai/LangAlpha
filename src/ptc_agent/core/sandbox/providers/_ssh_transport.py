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

from ptc_agent.core.sandbox.providers._script_run import (
    GROUP_WAIT_S,
    ScriptResult,
    ScriptRun,
    finish_framed,
)
from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget
from ptc_agent.core.sandbox.providers._ssh_shell import (
    BOOT,
    HELLO,
    OUTPUT_CAP,
    TERM_GRACE_S,
    FramedResult,
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
# and checks those turns and their computers still need. A turn running a
# command holds a second slot as it waits for it, so CLIs and commands past a
# connection's room run on more connections to the login (``Link.spill``).
CLI_SLOTS = _COMMAND_SLOTS - 2
# A connection LangAlpha's helper answers has no MaxSessions: each session is
# one process on the machine. So it carries every CLI a computer may run
# (``CLIS_MAX``) and a command for each of them, with room for the rest.
_HELPER_CLI_SLOTS = 16
_HELPER_COMMAND_SLOTS = 3 * _HELPER_CLI_SLOTS
# Channel opens refused in a row after which a connection past a login's
# first is passed over and replaced: sshd holds sessions of it that we no
# longer count.
REFUSALS_REPLACED = 2
# What an upload reads of its local file at a time, off the event loop; the
# SFTP channel writes each in parallel requests.
_UPLOAD_CHUNK = 4 * 1024 * 1024
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


class _Run(ScriptRun):
    """One script on one channel, and the connection it runs on (``link``),
    which may be another of the login's than the one it was started from."""

    def __init__(self, process: Any, cap: int, link: Link) -> None:
        super().__init__(process.stdout, process.stderr, cap)
        self.process = process
        self.link = link

    @property
    def exit_code(self) -> int | None:
        return self.process.exit_status if self.process.exit_signal is None else None

    def _exited(self) -> bool:
        # sshd holds a channel open while anything still has its output open,
        # so the exit status says the script ended, not the channel's close.
        return (
            self.process.exit_status is not None or self.process.exit_signal is not None
        )

    async def _closed(self) -> None:
        await self.process.wait_closed()

    def _close(self) -> None:
        with contextlib.suppress(Exception):
            self.process.close()

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        return self.link.spawn(coro)

    def _log_fields(self) -> dict[str, Any]:
        return {"target": self.link.target.label}

    async def _kill_group(self, script: str, *, root: bool) -> bool:
        """Over another channel, in the kill slot kept for it."""
        if not self.link.usable:
            return False
        await self.link.run(script, timeout=TERM_GRACE_S + 15, root=root, kill=True)
        return True

    async def _settle(self) -> None:
        with contextlib.suppress(TimeoutError, *TRANSPORT_ERRORS):
            async with asyncio.timeout(GROUP_WAIT_S):
                await asyncio.shield(self.process.wait_closed())


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
    a CLI, ``n`` is how many were running where it waited. A wait for one of
    a computer's own CLIs (``cli_slots``) also names that computer and
    whether its owner sets that count, so the refusal can offer to raise it."""

    def __init__(
        self, which: Literal["cli", "command"], n: int = 0, *, computer_id: str | None = None, settable: bool = False,
    ) -> None:
        super().__init__(f"no {which} slot freed in time")
        self.which = which
        self.n = n
        self.computer_id = computer_id
        self.settable = settable


class _Slots(asyncio.Semaphore):
    """A connection's command or CLI slots, setting its ``freed`` as one is
    given back, however that comes."""

    def __init__(self, value: int, link: Link) -> None:
        super().__init__(value)
        self._link = link

    def release(self) -> None:
        super().release()
        self._link.freed.set()


class CliHold(NamedTuple):
    """The connection a CLI runs on, where it is signalled, and what starts
    it there (``Link.cli_hold``)."""

    link: Any
    start: Callable[[str], Awaitable[Any]]


class Link:
    """One connection and what rides on it, pooled or a one-off's."""

    def __init__(
        self, target: SSHTarget, conn: asyncssh.SSHClientConnection, *, helper: bool = False
    ) -> None:
        self.target = target
        self.conn = conn
        self.facts: HostFacts | None = None
        #: How many CLIs this connection carries, by what answered it (``helper``).
        self.cli_room = _HELPER_CLI_SLOTS if helper else CLI_SLOTS
        self.slots = _Slots(_HELPER_COMMAND_SLOTS if helper else _COMMAND_SLOTS, self)
        self.cli_slots = _Slots(self.cli_room, self)
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
        # A dial for more room, by what it is for, which the calls waiting
        # share and a slot freed meanwhile overtakes (``_dial_or_freed``).
        self._dials: dict[str, asyncio.Future[Link | None]] = {}
        # Another of the login's connections with room for a CLI or a
        # command, dialed once none has it; None where the login already
        # runs as many as it may, and on a one-off's, whose CLIs and commands
        # wait for this one's. ``peers`` lists the ones dialed so far.
        self.spill: Callable[[Literal["cli", "command"]], Awaitable[Link | None]] | None = None
        self.peers: Callable[[], list[Link]] = list
        # Set as a command or CLI slot frees on any of the login's
        # connections, which share it once pooled, so a command or a CLI
        # waits for whichever frees first.
        self.freed = asyncio.Event()

    @property
    def usable(self) -> bool:
        return not self.retired and not self.conn.is_closed()

    @property
    def accepting(self) -> bool:
        """Open to new calls as another connection's overflow: up, not
        draining, and not refusing the channels sshd says it has room for."""
        return self.usable and not self.draining and self.refusals < REFUSALS_REPLACED

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

        A command that finds this connection's command slots all taken runs
        on another of the login's connections with one free (``spill``), or
        on whichever frees one first (``freed``), so the turns' long commands
        never leave the rest of them waiting while another has room. One
        that other connections fail to start twice waits for one of this
        one's.

        A transport error raised here came before anything was sent, so the
        caller can tell a command that never ran from one that may have.
        """
        if slot is not None and slot is not self.slots:
            return await self._start_here(script, root=root, cap=cap, slot=slot)
        failed = 0
        while self.slots.locked() and failed < REFUSALS_REPLACED:
            other = await self._room_for_command()
            if other is None:
                self.freed.clear()
                if self.slots.locked() and self._peer_with_room() is None:
                    await self.freed.wait()
                continue
            try:
                async with other.in_use():
                    run = await other._start_here(script, root=root, cap=cap, slot=None)
                    other._in_use_until_closed(run.process)
                return run
            except TRANSPORT_ERRORS as e:
                # Lost or refusing, it is passed over from here on and
                # replaced at the next dial (``spill``).
                failed += 1
                logger.warning(
                    "SSH connection for more commands failed a command, trying again",
                    target=self.target.label, error=str(e) or type(e).__name__,
                )
        return await self._start_here(script, root=root, cap=cap, slot=None)

    def _peer_with_room(self) -> Link | None:
        for link in self.peers():
            if link is not self and link.accepting and not link.slots.locked():
                return link
        return None

    async def _room_for_command(self) -> Link | None:
        """Another of the login's connections with a command slot free,
        dialed where none has one; None where the login has no more room,
        the dial failed, or a slot freed before the dial ended, so the
        command looks again."""
        if (link := self._peer_with_room()) is not None:
            return link
        self.freed.clear()
        other = await self._dial_or_freed("command", None)
        if other is None or other is self or not other.accepting or other.slots.locked():
            return None
        return other

    async def _dial_or_freed(self, want: Literal["cli", "command"], deadline: float | None) -> Link | None:
        """A connection of the login with room for ``want`` (``spill``), or
        None once a slot frees first (``freed``, which the caller clears
        before it last looked), the login is at its most, or the dial
        fails. The calls waiting share one dial, which still ends once a
        freed slot overtakes it, for the calls after.

        Raises:
            TimeoutError: ``deadline`` passed first.
        """
        if self.spill is None:
            return None
        dial = self._dials.get(want)
        if dial is None or dial.done():
            dial = self._dials[want] = asyncio.ensure_future(self.spill(want))
            dial.add_done_callback(self._dial_ended)
        freed = asyncio.ensure_future(self.freed.wait())
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.wait((dial, freed), return_when=asyncio.FIRST_COMPLETED)
        finally:
            freed.cancel()
        if not dial.done() or dial.cancelled() or dial.exception() is not None:
            return None
        return dial.result()

    def _dial_ended(self, dial: asyncio.Future[Link | None]) -> None:
        if not dial.cancelled() and (e := dial.exception()) is not None:
            logger.warning(
                "SSH connection for more CLIs and commands failed, waiting on the ones up",
                target=self.target.label, error=str(e) or type(e).__name__,
            )

    def _in_use_until_closed(self, process: Any) -> None:
        """Count *process*'s channel as a call on this connection until it
        closes, so a drain or the idle sweep never closes the connection
        under a command another connection's caller is reading."""
        self.active += 1

        async def closed() -> None:
            with contextlib.suppress(Exception):
                await process.wait_closed()
            self.active -= 1
            self.last_used = time.monotonic()
            if self.draining and self.active == 0:
                self.close()

        self.spawn(closed())

    async def _start_here(
        self, script: str, *, root: bool, cap: int, slot: asyncio.Semaphore | None
    ) -> _Run:
        command = f"exec sudo -n {BOOT.removeprefix('exec ')}" if root else BOOT
        process = await self.open_process(command, slot=slot)
        run = _Run(process, cap, self)
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
        taken, another (``spill``), and the command slot its channel will
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

    async def _cli_slot(self, deadline: float) -> Link:
        """A CLI slot on one of the login's connections, taken by
        ``deadline``: this one's while it has one free, else another's,
        dialed once none has one, else the first any frees (``freed``).
        A slot is taken as it is seen free, with no wait between, and only
        on a connection with a command slot free too, which the CLI's start
        takes. A connection that cannot be dialed, or a login at its most,
        leaves the CLI to the ones up until it next wakes, when it looks
        again, so one another CLI dialed meanwhile serves it too. A wait
        that runs out with CLI slots free but no command slot is the
        commands' (``which`` "command"). This one's slot is taken
        even once it is lost, so the CLI fails starting as a lost connection
        rather than waiting out its time as a full one."""

        def room() -> int:
            return self.cli_room + sum(link.cli_room for link in self.peers() if link is not self and link.accepting)

        def ran_out() -> SlotWaitTimeout:
            return SlotWaitTimeout("command") if open_to_clis() else SlotWaitTimeout("cli", room())

        def open_to_clis() -> list[Link]:
            return [
                link for link in (self, *self.peers())
                if (link is self or link.accepting) and not link.cli_slots.locked()
            ]

        while True:
            # Only one with a command slot free too: on one whose commands
            # hold every slot, the CLI would start only once one of those
            # ended, minutes later for a waited-on command, while another
            # connection may free one first.
            for link in open_to_clis():
                if not link.slots.locked():
                    await link.cli_slots.acquire()
                    return link
            # Cleared before the dial, so a slot freed during it is seen.
            self.freed.clear()
            try:
                other = await self._dial_or_freed("cli", deadline)
            except TimeoutError:
                raise ran_out() from None
            if (
                other is not None and other.accepting
                and not other.cli_slots.locked() and not other.slots.locked()
            ):
                await other.cli_slots.acquire()
                return other
            # By what is free, not by ``freed``: a command woken by the same
            # release may already have cleared it.
            if any(not link.slots.locked() for link in open_to_clis()):
                continue
            try:
                async with asyncio.timeout_at(deadline):
                    await self.freed.wait()
            except TimeoutError:
                raise ran_out() from None

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
        # Its room is gone, which a call waiting at a login's most
        # connections waits for, to dial another in its place.
        self.freed.set()


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


class ScriptLost(ConnectionError):
    """The connection dropped while a command may have been running; ``link``
    is the one it ran on."""

    def __init__(self, message: str, link: Link) -> None:
        super().__init__(message)
        self.link = link


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
            return await finish_framed(
                run,
                computer_dir=computer_dir,
                timeout=timeout,
                root=root,
                lost=TRANSPORT_ERRORS,
            )
        except TRANSPORT_ERRORS as e:
            raise ScriptLost(str(e) or type(e).__name__, run.link) from e


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
