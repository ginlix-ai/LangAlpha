"""A connection's command slots are given back, and never stand in the way.

A connection runs a fixed number of commands at once. A command whose script
could not be sent never ran, and if its slot stayed taken, a machine that
dropped a few channels would leave every later command on that connection
waiting for good. Our own short scripts and kills have slots of their own, so
long commands filling the rest cannot hold up a state read or a stop, and a
command that waited out its timeout for a slot is not reported as one that
ran past it.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ptc_agent.core.sandbox.providers._ssh_shell import FRAME_PGID
from ptc_agent.core.sandbox.providers._ssh_transport import (
    _COMMAND_SLOTS,
    Link,
    _reported_group,
    run_framed,
)


class _Stream:
    def __init__(self, closed: asyncio.Event) -> None:
        self._closed = closed

    async def read(self, n: int) -> bytes:
        await self._closed.wait()
        return b""


class _Process:
    """A channel that refuses its stdin, and closes only when closed."""

    def __init__(self) -> None:
        self._closed = asyncio.Event()
        self.stdout = _Stream(self._closed)
        self.stderr = _Stream(self._closed)
        self.stdin = MagicMock()
        self.stdin.write.side_effect = BrokenPipeError("channel closed")
        self.exit_status = None
        self.exit_signal = None

    def close(self) -> None:
        self._closed.set()

    async def wait_closed(self) -> None:
        await self._closed.wait()


class _Conn:
    def __init__(self) -> None:
        self.processes: list[_Process] = []

    async def create_process(self, command: str, encoding: None = None) -> _Process:
        process = _Process()
        self.processes.append(process)
        return process


@pytest.mark.asyncio
async def test_a_refused_script_frees_its_slot_and_its_channel():
    conn = _Conn()
    link = Link(SimpleNamespace(label="u@m"), conn)

    async with asyncio.timeout(5):
        for _ in range(2 * _COMMAND_SLOTS):
            with pytest.raises(BrokenPipeError):
                await link.start("true")
        await asyncio.gather(*link._tasks)

    assert all(p._closed.is_set() for p in conn.processes)
    for _ in range(_COMMAND_SLOTS):
        await asyncio.wait_for(link.slots.acquire(), timeout=1)


class _Answering(_Process):
    """A channel whose script runs at once and prints *out*."""

    def __init__(self, out: bytes = b"", err: bytes = b"") -> None:
        super().__init__()
        self.stdin = MagicMock()
        self.stdout = _Chunks(out)
        self.stderr = _Chunks(err)
        self.exit_status = 0
        self._closed.set()


class _Chunks:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def read(self, n: int) -> bytes:
        data, self._data = self._data, b""
        return data


class _Answers(_Conn):
    async def create_process(self, command: str, encoding: None = None) -> _Process:
        process = _Answering(b"1\n")
        self.processes.append(process)
        return process


async def _full(link: Link) -> None:
    for _ in range(_COMMAND_SLOTS):
        await link.slots.acquire()


@pytest.mark.asyncio
async def test_a_management_script_runs_while_long_commands_hold_every_slot():
    link = Link(SimpleNamespace(label="u@m", host="m"), _Answers())
    await _full(link)

    result = await asyncio.wait_for(link.run("state", timeout=5), timeout=1)

    assert result.stdout == b"1\n"


@pytest.mark.asyncio
async def test_a_command_that_never_got_a_slot_reads_as_not_started():
    link = Link(SimpleNamespace(label="u@m", host="m"), _Answers())
    await _full(link)

    result = await run_framed(link, "true", computer_dir="/d", timeout=0.05)

    assert not result.started
    assert not result.timed_out
    assert "did not start" in result.framing_error


class _SlowOpen(_Conn):
    def __init__(self) -> None:
        super().__init__()
        self.opened = asyncio.Event()

    async def create_process(self, command: str, encoding: None = None) -> _Process:
        await self.opened.wait()
        return await super().create_process(command)


@pytest.mark.asyncio
async def test_a_channel_whose_open_was_cancelled_is_closed_once_it_opens():
    conn = _SlowOpen()
    link = Link(SimpleNamespace(label="u@m"), conn)

    starting = asyncio.ensure_future(link.start("true"))
    await asyncio.sleep(0.01)
    starting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starting
    conn.opened.set()
    async with asyncio.timeout(5):
        while link._tasks:
            await asyncio.sleep(0)

    assert conn.processes[0]._closed.is_set()
    for _ in range(_COMMAND_SLOTS):
        await asyncio.wait_for(link.slots.acquire(), timeout=1)


@pytest.mark.asyncio
async def test_a_group_id_still_on_its_way_is_waited_for():
    run = SimpleNamespace(stderr=b"", stderr_ended=False)

    async def arrives() -> None:
        await asyncio.sleep(0.2)
        run.stderr = FRAME_PGID + b"4242"
        await asyncio.sleep(0.2)
        run.stderr += b"\n"

    asyncio.ensure_future(arrives())

    assert await _reported_group(run) == 4242
