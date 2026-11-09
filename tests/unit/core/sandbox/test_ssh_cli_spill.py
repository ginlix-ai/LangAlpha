"""CLIs past one connection's slots ride a second connection to the login.

sshd's default MaxSessions holds one connection at ``CLI_SLOTS`` CLIs and its
spare slots, so the CLIs past them take a second connection, one per login,
dialed again once lost or once it keeps refusing channels. A CLI waiting for
a slot takes whichever of the two frees one first, and a second connection
that cannot be dialed leaves it to the first's. The transport holds only
connection capacity; a computer's own count is held before it
(``cli_slots.computer_cli_slot``).
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace

import asyncssh
import pytest

from ptc_agent.core.claude_code_clis import CLIS_DEFAULT, CLIS_MAX
from ptc_agent.core.sandbox.providers import _ssh_pool
from ptc_agent.core.sandbox.providers._ssh_pool import SSHPool
from ptc_agent.core.sandbox.providers._ssh_transport import CLI_SLOTS, Link, SlotWaitTimeout
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError


class _Conn:
    def __init__(self) -> None:
        self.closed = False
        self._channels: dict = {}

    def is_closed(self) -> bool:
        return self.closed

    def abort(self) -> None:
        self.closed = True

    async def create_process(self, command, encoding=None):
        raise asyncssh.ConnectionLost("Connection lost")


def _target() -> SimpleNamespace:
    return SimpleNamespace(label="u@m", host="m", pool_key=("m", 22, "u"))


async def _key() -> str:
    return "key"


@pytest.fixture
def pool(monkeypatch) -> SSHPool:
    """A pool whose dials make stand-in connections; ``dial``, once set,
    runs first in each dial and may stall or fail it."""
    found = SSHPool()
    found.dialed = []
    found.dial = None

    async def connect(target, key):
        if found.dial is not None:
            await found.dial()
        return _Conn()

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


def test_a_computer_runs_one_connections_clis_unless_set_and_at_most_two_connections():
    assert (CLIS_DEFAULT, CLIS_MAX) == (CLI_SLOTS, 2 * CLI_SLOTS)


@pytest.mark.asyncio
async def test_clis_past_the_first_connections_slots_take_one_second_connection_and_no_more(pool):
    target = _target()
    first = await pool.link(target, _key)
    held = await _held(first, 2 * CLI_SLOTS)
    second = pool._spilled[target.pool_key]
    assert [s.hold.link for s in held] == [first] * CLI_SLOTS + [second] * CLI_SLOTS
    assert pool.dialed == [first, second]
    async with contextlib.AsyncExitStack() as stack:
        with pytest.raises(SlotWaitTimeout) as refused:
            await _hold(stack, first)
    assert (refused.value.which, refused.value.n) == ("cli", 2 * CLI_SLOTS)
    assert pool.dialed == [first, second]
    for s in held:
        await s.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("frees", ["first", "second"])
async def test_a_cli_waiting_with_both_connections_full_takes_whichever_frees_first(pool, frees):
    target = _target()
    first = await pool.link(target, _key)
    held = await _held(first, 2 * CLI_SLOTS)
    second = pool._spilled[target.pool_key]
    waiting = asyncio.ensure_future(first.cli_hold(5).__aenter__())
    await asyncio.sleep(0.05)
    assert not waiting.done()
    ending = held[0] if frees == "first" else held[-1]
    await ending.aclose()
    hold = await asyncio.wait_for(waiting, 1)
    assert hold.link is (first if frees == "first" else second)
    for s in held[1:] if frees == "first" else held[:-1]:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_second_connection_lost_or_refusing_channels_is_replaced(pool):
    target = _target()
    await pool.link(target, _key)
    lost = await pool._spill(target, _key)
    lost.close()
    refusing = await pool._spill(target, _key)
    assert refusing is not lost
    refusing.refusals = 2
    refusing.active = 1
    again = await pool._spill(target, _key)
    assert again is not refusing and pool._spilled[target.pool_key] is again
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
async def test_a_second_connection_that_cannot_be_dialed_leaves_the_cli_waiting_on_the_first(pool, fails):
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
async def test_a_cli_whose_dial_failed_takes_a_slot_on_a_second_connection_another_cli_dialed(pool):
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
        second = pool._spilled[target.pool_key]
        assert dialer.link is second
        assert (await asyncio.wait_for(waiting, 1)).link is second
    for s in held:
        await s.aclose()


@pytest.mark.asyncio
async def test_a_second_connection_slower_than_the_wait_is_a_cli_wait_that_ran_out(pool):
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
    await pool._spill(target, _key)
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
        assert key in pool._links and key in pool._cli_freed and key in pool._spill_locks
    await end()
    assert (key in pool._links, key in pool._cli_freed, key in pool._spill_locks) == (False, False, False)


@pytest.mark.asyncio
async def test_a_stop_closes_the_second_connection_and_one_only_if_idle_keeps_a_busy_one(pool):
    target = _target()
    await pool.link(target, _key)
    busy = await pool._spill(target, _key)
    busy.active = 1
    pool.retire(target, only_if_idle=True)
    assert busy.usable
    pool.retire(target)
    assert not busy.usable and target.pool_key not in pool._spilled
