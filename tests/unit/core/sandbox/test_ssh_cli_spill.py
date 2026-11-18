"""CLIs and commands past one connection's slots ride more connections to the login.

sshd's default MaxSessions holds one connection at ``CLI_SLOTS`` CLIs and its
spare slots, and a turn running a command holds a slot of its own as it waits
for it. So the CLIs and commands past a connection's room take another
connection to the login, up to ``_MAX_CONNECTIONS``, each dialed again once
lost or once it keeps refusing channels. A CLI waiting for a slot takes
whichever connection frees one first, and one that cannot be dialed leaves it
to those up. A connection the helper answers has no MaxSessions, so it holds a
computer's most CLIs alone. The transport holds only connection capacity; a
computer's own count is held before it (``cli_slots.computer_cli_slot``).
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace

import asyncssh
import pytest

from ptc_agent.core.claude_code_clis import CLIS_DEFAULT, CLIS_MAX
from ptc_agent.core.sandbox.providers import _ssh_pool
from ptc_agent.core.sandbox.providers._ssh_connect import HELPER_ID_PREFIX
from ptc_agent.core.sandbox.providers._ssh_pool import _MAX_CONNECTIONS, SSHPool
from ptc_agent.core.sandbox.providers._ssh_shell import FRAME_PGID
from ptc_agent.core.sandbox.providers._ssh_transport import (
    _COMMAND_SLOTS,
    _HELPER_CLI_SLOTS,
    CLI_SLOTS,
    Link,
    ScriptLost,
    SlotWaitTimeout,
    run_framed,
)
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError

MOST = _MAX_CONNECTIONS * CLI_SLOTS


class _Process:
    """A command that runs until ``end``."""

    def __init__(self) -> None:
        self.stdin = SimpleNamespace(write=lambda data: None, write_eof=lambda: None)
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.exit_status = None
        self.exit_signal = None
        self._closed = asyncio.Event()

    def end(self) -> None:
        self.exit_status = 0
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        self._closed.set()

    def close(self) -> None:
        self.end()

    async def wait_closed(self) -> None:
        await self._closed.wait()


class _Conn:
    def __init__(self, helper: bool = False) -> None:
        self.closed = False
        self._channels: dict = {}
        self.server_version = f"{HELPER_ID_PREFIX}1.0" if helper else "SSH-2.0-OpenSSH_9.6"
        self.processes: list[_Process] = []

    def get_extra_info(self, name):
        return self.server_version if name == "server_version" else None

    def is_closed(self) -> bool:
        return self.closed

    def abort(self) -> None:
        self.closed = True

    async def create_process(self, command, encoding=None):
        if self.closed:
            raise asyncssh.ConnectionLost("Connection lost")
        process = _Process()
        self.processes.append(process)
        return process


def _target() -> SimpleNamespace:
    return SimpleNamespace(label="u@m", host="m", pool_key=("m", 22, "u"))


async def _key() -> str:
    return "key"


@pytest.fixture
def pool(monkeypatch) -> SSHPool:
    """A pool whose dials make stand-in connections to an sshd, or the
    helper with ``helper`` set; ``dial``, once set, runs first in each dial
    and may stall or fail it."""
    found = SSHPool()
    found.dialed = []
    found.dial = None
    found.helper = False

    async def connect(target, key):
        if found.dial is not None:
            await found.dial()
        return _Conn(found.helper)

    async def hello(self):
        found.dialed.append(self)

    monkeypatch.setattr(_ssh_pool, "connect", connect)
    monkeypatch.setattr(Link, "hello", hello)
    monkeypatch.setattr(SSHPool, "_start_sweeper", lambda self: None)
    return found


async def _hold(stack: contextlib.AsyncExitStack, link: Link, wait_s: float = 0.05):
    return await stack.enter_async_context(link.cli_hold(wait_s))


async def _held(link: Link, n: int, wait_s: float = 0.05) -> list[contextlib.AsyncExitStack]:
    """``n`` CLIs held through ``link``, each ended by closing its stack."""
    stacks = []
    for _ in range(n):
        stack = contextlib.AsyncExitStack()
        stack.hold = await _hold(stack, link, wait_s)
        stacks.append(stack)
    return stacks


def test_a_logins_connections_hold_a_computers_most_clis_and_a_helpers_one_alone():
    assert CLI_SLOTS == CLIS_DEFAULT and MOST >= CLIS_MAX and _HELPER_CLI_SLOTS >= CLIS_MAX


@pytest.mark.asyncio
async def test_clis_past_the_first_connections_slots_take_more_connections_up_to_the_most(pool):
    target = _target()
    first = await pool.link(target, _key)
    held = await _held(first, MOST)
    more = pool._more[target.pool_key]
    assert [s.hold.link for s in held] == [link for link in [first, *more] for _ in range(CLI_SLOTS)]
    assert pool.dialed == [first, *more] and len(more) == _MAX_CONNECTIONS - 1
    async with contextlib.AsyncExitStack() as stack:
        with pytest.raises(SlotWaitTimeout) as refused:
            await _hold(stack, first)
    assert (refused.value.which, refused.value.n) == ("cli", MOST)
    assert len(pool.dialed) == _MAX_CONNECTIONS
    for s in held:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_connection_the_helper_answers_holds_the_most_clis_alone(pool):
    pool.helper = True
    first = await pool.link(_target(), _key)
    held = await _held(first, CLIS_MAX)
    assert {s.hold.link for s in held} == {first} and pool.dialed == [first]
    for s in held:
        await s.aclose()


@pytest.mark.asyncio
async def test_commands_past_the_first_connections_slots_run_on_another(pool):
    """A turn's long commands, each waited on over a slot of its own, never
    leave the rest waiting while the login has room for more connections."""
    target = _target()
    first = await pool.link(target, _key)
    runs = [await first.start("sleep 60") for _ in range(_COMMAND_SLOTS)]
    assert len(first.conn.processes) == _COMMAND_SLOTS and pool.dialed == [first]
    runs.append(await asyncio.wait_for(first.start("sleep 60"), 1))
    second = pool._more[target.pool_key][0]
    assert len(second.conn.processes) == 1 and second.active == 1
    second.conn.processes[0].end()
    await asyncio.sleep(0.01)
    assert second.active == 0
    for process in first.conn.processes:
        process.end()


@pytest.mark.asyncio
async def test_a_command_waiting_with_every_connection_full_takes_whichever_frees_first(pool, monkeypatch):
    monkeypatch.setattr(_ssh_pool, "_MAX_CONNECTIONS", 2)
    target = _target()
    first = await pool.link(target, _key)
    for _ in range(2 * _COMMAND_SLOTS):
        await asyncio.wait_for(first.start("sleep 60"), 1)
    second = pool._more[target.pool_key][0]
    waiting = asyncio.ensure_future(first.start("sleep 60"))
    await asyncio.sleep(0.05)
    assert not waiting.done()
    second.conn.processes[0].end()
    run = await asyncio.wait_for(waiting, 1)
    assert run.link is second and len(second.conn.processes) == _COMMAND_SLOTS + 1
    for link in (first, second):
        for process in link.conn.processes:
            process.end()


@pytest.mark.asyncio
async def test_a_command_another_connection_will_not_start_waits_for_the_first_and_that_one_is_passed_over(pool):
    target = _target()
    first = await pool.link(target, _key)
    for _ in range(_COMMAND_SLOTS):
        await first.start("sleep 60")
    refusing = await pool._spill(target, _key, "command")
    refused = []

    async def refuse(command, encoding=None):
        refused.append(command)
        raise asyncssh.ChannelOpenError(asyncssh.OPEN_RESOURCE_SHORTAGE, "no more sessions")

    refusing.conn.create_process = refuse
    waiting = asyncio.ensure_future(first.start("sleep 60"))
    await asyncio.sleep(0.05)
    assert not waiting.done() and len(refused) == 2 and not refusing.accepting
    first.conn.processes[0].end()
    assert (await asyncio.wait_for(waiting, 1)).link is first
    async with contextlib.AsyncExitStack() as stack:
        hold = await _hold(stack, first)
        assert hold.link not in (first, refusing) and refusing.draining
    for process in first.conn.processes:
        process.end()


@pytest.mark.asyncio
async def test_a_command_lost_with_another_connection_names_that_one(pool):
    """So the caller drops the connection that was lost, not the first and
    every CLI on it."""
    target = _target()
    first = await pool.link(target, _key)
    for _ in range(_COMMAND_SLOTS):
        await first.start("sleep 60")
    running = asyncio.ensure_future(run_framed(first, "sleep 60", computer_dir="/c", timeout=5))
    await asyncio.sleep(0.05)
    second = pool._more[target.pool_key][0]
    for stream in (second.conn.processes[0].stdout, second.conn.processes[0].stderr):
        stream.set_exception(asyncssh.ConnectionLost("Connection lost"))
    with pytest.raises(ScriptLost) as lost:
        await asyncio.wait_for(running, 1)
    assert lost.value.link is second and first.usable
    for process in first.conn.processes:
        process.end()


@pytest.mark.asyncio
async def test_a_command_run_on_another_connection_is_ended_there_once_the_first_is_lost(pool):
    """Its group is killed over the connection it runs on, so a first lost
    meanwhile leaves nothing of it running unowned."""
    target = _target()
    first = await pool.link(target, _key)
    for _ in range(_COMMAND_SLOTS):
        await first.start("sleep 60")
    running = asyncio.ensure_future(run_framed(first, "sleep 60", computer_dir="/c", timeout=0.2))
    await asyncio.sleep(0.05)
    second = pool._more[target.pool_key][0]
    command = second.conn.processes[0]
    command.stderr.feed_data(FRAME_PGID + b"4242\n")
    first.conn.abort()
    async with asyncio.timeout(1):
        while len(second.conn.processes) < 2:
            await asyncio.sleep(0.01)
    second.conn.processes[1].end()
    command.end()
    assert (await asyncio.wait_for(running, 2)).timed_out
    for process in first.conn.processes:
        process.end()


@pytest.mark.asyncio
async def test_a_command_takes_a_slot_freed_while_another_connection_is_dialed(pool):
    """A slow dial never holds a command whose own connection frees a slot;
    the dial still ends, for the commands after it."""
    target = _target()
    first = await pool.link(target, _key)
    for _ in range(_COMMAND_SLOTS):
        await first.start("sleep 60")
    dialing = asyncio.Event()

    async def slow():
        dialing.set()
        await asyncio.sleep(0.3)

    pool.dial = slow
    waiting = asyncio.ensure_future(first.start("sleep 60"))
    await dialing.wait()
    first.conn.processes[0].end()
    assert (await asyncio.wait_for(waiting, 0.2)).link is first
    await asyncio.wait_for(first._dials["command"], 1)
    assert len(pool._more[target.pool_key]) == 1
    for process in first.conn.processes:
        process.end()


@pytest.mark.asyncio
@pytest.mark.parametrize("dial", ["slow", "failing"])
async def test_a_cli_takes_a_slot_freed_while_another_connection_is_dialed(pool, dial):
    """Freed during a dial that is slow or then fails, the slot is the CLI's
    at once, not at the next one freed."""
    target = _target()
    first = await pool.link(target, _key)
    held = await _held(first, CLI_SLOTS)
    dialing = asyncio.Event()
    finish = asyncio.Event()

    async def dialed():
        dialing.set()
        await finish.wait()
        if dial == "failing":
            raise SSHConnectError("unreachable", "The machine could not be reached")
        await asyncio.sleep(5)

    pool.dial = dialed
    waiting = asyncio.ensure_future(first.cli_hold(5).__aenter__())
    await dialing.wait()
    await held[0].aclose()
    if dial == "failing":
        finish.set()
    assert (await asyncio.wait_for(waiting, 0.5)).link is first
    finish.set()
    first._dials["cli"].cancel()
    for s in held[1:]:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_cli_takes_a_freed_slot_a_command_woken_with_it_cleared_the_wake_of(pool):
    """A command and a CLI wait through one dial; the release that wakes both
    leaves a slot of each kind, and the CLI takes its own though the command
    cleared the shared wake first."""
    target = _target()
    first = await pool.link(target, _key)
    held = await _held(first, CLI_SLOTS)
    for _ in range(_COMMAND_SLOTS - CLI_SLOTS):
        await first.start("sleep 60")
    finish = asyncio.Event()

    async def blocked():
        await finish.wait()

    pool.dial = blocked
    command = asyncio.ensure_future(first.start("sleep 60"))
    await asyncio.sleep(0.02)
    cli = asyncio.ensure_future(first.cli_hold(5).__aenter__())
    await asyncio.sleep(0.02)
    first.conn.processes[0].end()
    await held[0].aclose()
    assert (await asyncio.wait_for(command, 0.5)).link is first
    assert (await asyncio.wait_for(cli, 0.5)).link is first
    finish.set()
    for s in held[1:]:
        await s.aclose()
    for process in first.conn.processes:
        process.end()


@pytest.mark.asyncio
async def test_a_first_connection_shed_counts_toward_the_most_until_it_closes(pool, monkeypatch):
    monkeypatch.setattr(_ssh_pool, "_MAX_CONNECTIONS", 2)
    target = _target()
    shed = await pool.link(target, _key)
    shed.active = 1
    pool.shed(shed)
    first = await pool.link(target, _key)
    assert first is not shed and pool._more[target.pool_key] == [shed]
    assert await pool._spill(target, _key, "cli") is None and len(pool.dialed) == 2
    shed.active = 0
    shed.close()
    assert await pool._spill(target, _key, "cli") is not None


@pytest.mark.asyncio
async def test_a_connection_let_go_still_counts_toward_the_most_until_it_closes(pool, monkeypatch):
    monkeypatch.setattr(_ssh_pool, "_MAX_CONNECTIONS", 2)
    target = _target()
    await pool.link(target, _key)
    refusing = await pool._spill(target, _key, "cli")
    refusing.refusals = 2
    refusing.active = 1
    assert await pool._spill(target, _key, "cli") is None
    assert refusing.draining and refusing.usable and len(pool.dialed) == 2
    refusing.active = 0
    refusing.close()
    again = await pool._spill(target, _key, "cli")
    assert again is not None and pool._more[target.pool_key] == [again]


@pytest.mark.asyncio
async def test_a_cli_passes_over_a_connection_whose_commands_hold_every_command_slot(pool):
    """Its CLI slot is free, but the CLI would start only once one of those
    commands ended, so it takes another connection."""
    target = _target()
    first = await pool.link(target, _key)
    for _ in range(_COMMAND_SLOTS):
        await first.start("sleep 60")
    async with contextlib.AsyncExitStack() as stack:
        hold = await _hold(stack, first)
        assert hold.link is pool._more[target.pool_key][0] and first.cli_slots._value == CLI_SLOTS
    for process in first.conn.processes:
        process.end()


@pytest.mark.asyncio
async def test_a_cli_at_the_most_connections_takes_whichever_frees_a_command_slot_first(pool, monkeypatch):
    """With CLI slots free but every command slot held, it reserves none and
    takes the first connection a command frees; one that frees none in time
    is a wait on the commands."""
    monkeypatch.setattr(_ssh_pool, "_MAX_CONNECTIONS", 2)
    target = _target()
    first = await pool.link(target, _key)
    for _ in range(2 * _COMMAND_SLOTS):
        await asyncio.wait_for(first.start("sleep 60"), 1)
    second = pool._more[target.pool_key][0]
    async with contextlib.AsyncExitStack() as stack:
        with pytest.raises(SlotWaitTimeout) as refused:
            await _hold(stack, first)
    assert refused.value.which == "command"
    async with contextlib.AsyncExitStack() as stack:
        waiting = asyncio.ensure_future(_hold(stack, first, 5))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        assert (first.cli_slots._value, second.cli_slots._value) == (CLI_SLOTS, CLI_SLOTS)
        second.conn.processes[0].end()
        assert (await asyncio.wait_for(waiting, 1)).link is second
    for link in (first, second):
        for process in link.conn.processes:
            process.end()


@pytest.mark.asyncio
@pytest.mark.parametrize("frees", ["first", "last"])
async def test_a_cli_waiting_with_every_connection_full_takes_whichever_frees_first(pool, frees):
    target = _target()
    first = await pool.link(target, _key)
    held = await _held(first, MOST)
    last = pool._more[target.pool_key][-1]
    waiting = asyncio.ensure_future(first.cli_hold(5).__aenter__())
    await asyncio.sleep(0.05)
    assert not waiting.done()
    ending = held[0] if frees == "first" else held[-1]
    await ending.aclose()
    hold = await asyncio.wait_for(waiting, 1)
    assert hold.link is (first if frees == "first" else last)
    for s in held[1:] if frees == "first" else held[:-1]:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_further_connection_lost_or_refusing_channels_is_replaced(pool):
    target = _target()
    await pool.link(target, _key)
    lost = await pool._spill(target, _key, "cli")
    lost.close()
    refusing = await pool._spill(target, _key, "cli")
    assert refusing is not lost
    refusing.refusals = 2
    refusing.active = 1
    again = await pool._spill(target, _key, "cli")
    assert again is not refusing and pool._more[target.pool_key] == [refusing, again]
    assert refusing.draining and refusing.usable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fails",
    [
        pytest.param(SSHConnectError("unreachable", "The machine could not be reached"), id="refused"),
        pytest.param(TimeoutError(), id="hello-too-slow"),
        pytest.param(RuntimeError("no key"), id="key-not-loading"),
    ],
)
async def test_a_connection_that_cannot_be_dialed_leaves_the_cli_waiting_on_the_first(pool, fails):
    """Only the CLI's own wait running out refuses it; a dial that fails
    sooner, however it fails, leaves it waiting for a slot on the first."""
    target = _target()
    first = await pool.link(target, _key)

    async def failing():
        raise fails

    pool.dial = failing
    held = await _held(first, CLI_SLOTS)
    waiting = asyncio.ensure_future(first.cli_hold(5).__aenter__())
    await asyncio.sleep(0.05)
    assert not waiting.done()
    await held[0].aclose()
    assert (await asyncio.wait_for(waiting, 1)).link is first
    assert pool.dialed == [first]
    for s in held[1:]:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_cli_waiting_on_a_first_connection_that_is_lost_fails_starting_on_it_at_once(pool):
    """A lost connection reads as one, not as a computer running all it may."""
    target = _target()
    first = await pool.link(target, _key)

    async def failing():
        raise SSHConnectError("unreachable", "The machine could not be reached")

    pool.dial = failing
    held = await _held(first, CLI_SLOTS)
    waiting = asyncio.ensure_future(first.cli_hold(5).__aenter__())
    await asyncio.sleep(0.05)
    first.conn.abort()
    await held[0].aclose()
    hold = await asyncio.wait_for(waiting, 1)
    assert hold.link is first
    with pytest.raises(asyncssh.ConnectionLost):
        await hold.start("claude")
    for s in held[1:]:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_cli_whose_dial_failed_takes_a_slot_on_a_connection_another_cli_dialed(pool):
    target = _target()
    first = await pool.link(target, _key)

    async def failing():
        raise SSHConnectError("unreachable", "The machine could not be reached")

    pool.dial = failing
    held = await _held(first, CLI_SLOTS)
    waiting = asyncio.ensure_future(first.cli_hold(5).__aenter__())
    await asyncio.sleep(0.05)
    assert not waiting.done()
    pool.dial = None
    async with contextlib.AsyncExitStack() as stack:
        dialer = await _hold(stack, first, 1)
        second = pool._more[target.pool_key][0]
        assert dialer.link is second
        assert (await asyncio.wait_for(waiting, 1)).link is second
    for s in held:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_connection_slower_than_the_wait_is_a_cli_wait_that_ran_out(pool):
    target = _target()
    first = await pool.link(target, _key)
    pool.dial = lambda: asyncio.sleep(5)
    held = await _held(first, CLI_SLOTS)
    async with contextlib.AsyncExitStack() as stack:
        with pytest.raises(SlotWaitTimeout) as refused:
            await _hold(stack, first)
    assert (refused.value.which, refused.value.n) == ("cli", CLI_SLOTS)
    for s in held:
        await s.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("ends", ["stop", "sweep"])
async def test_what_a_logins_connections_share_goes_with_the_last_and_stays_while_one_is_in_use(pool, monkeypatch, ends):
    """A CLI waiting holds its connection in use, so the wake it waits on is
    never dropped from under it."""
    target = _target()
    key = target.pool_key
    first = await pool.link(target, _key)
    await pool._spill(target, _key, "cli")
    monkeypatch.setattr(_ssh_pool, "_SWEEP_S", 0)
    monkeypatch.setattr(_ssh_pool, "_IDLE_CLOSE_S", -1)

    async def end():
        if ends == "stop":
            pool.retire(target, only_if_idle=True)
        else:
            sweeping = asyncio.ensure_future(pool._sweep())
            await asyncio.sleep(0.05)
            sweeping.cancel()

    async with first.in_use():
        await end()
        assert key in pool._links and key in pool._freed and key in pool._spill_locks
    await end()
    assert (key in pool._links, key in pool._freed, key in pool._spill_locks) == (False, False, False)


@pytest.mark.asyncio
async def test_a_stop_closes_the_further_connections_and_one_only_if_idle_keeps_a_busy_one(pool):
    target = _target()
    await pool.link(target, _key)
    busy = await pool._spill(target, _key, "cli")
    busy.active = 1
    pool.retire(target, only_if_idle=True)
    assert busy.usable and pool._more[target.pool_key] == [busy]
    pool.retire(target)
    assert not busy.usable and target.pool_key not in pool._more
