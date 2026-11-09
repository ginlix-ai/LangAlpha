"""A computer's Claude Code CLIs held to the number its owner set.

Read as each CLI starts, the count holds among the CLIs this worker runs
there, within the one wait the CLI is given, and each computer on a login is
held to its own.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

from ptc_agent.core.claude_code_clis import CLIS_DEFAULT
from ptc_agent.core.sandbox.providers._ssh_transport import SlotWaitTimeout
from src.server.services.claude_code import cli_slots

CID = "c-1"


@pytest.fixture
def count(monkeypatch):
    counts = {"n": 2, "read_s": 0.0}

    async def computer_clis(computer_id):
        await asyncio.sleep(counts["read_s"])
        return counts.get(computer_id, counts["n"])

    monkeypatch.setattr(cli_slots, "computer_clis", computer_clis)
    return counts


@pytest.mark.asyncio
async def test_a_worker_holds_its_clis_to_the_computers_count(count):
    async with cli_slots.computer_cli_slot(CID, 1), cli_slots.computer_cli_slot(CID, 1):
        with pytest.raises(SlotWaitTimeout) as refused:
            async with cli_slots.computer_cli_slot(CID, 0.2):
                pass
        assert (refused.value.which, refused.value.n) == ("cli", 2)
    async with cli_slots.computer_cli_slot(CID, 0.2):
        pass


@pytest.mark.asyncio
async def test_each_computer_on_a_login_is_held_to_its_own_count(count):
    """Two computers on one machine's login share its connections but not
    their counts: one set to 8 running 2 leaves one set to 2 its own two."""
    count.update({"x": 8, CID: 2})
    async with cli_slots.computer_cli_slot("x", 1), cli_slots.computer_cli_slot("x", 1):
        async with cli_slots.computer_cli_slot(CID, 0.2), cli_slots.computer_cli_slot(CID, 0.2):
            pass


@pytest.mark.asyncio
async def test_a_cli_waiting_in_its_worker_starts_once_one_there_frees(count):
    freed = asyncio.Event()

    async def holding():
        async with cli_slots.computer_cli_slot(CID, 1), cli_slots.computer_cli_slot(CID, 1):
            await freed.wait()

    held = asyncio.ensure_future(holding())
    await asyncio.sleep(0.05)
    waiting = asyncio.ensure_future(cli_slots.computer_cli_slot(CID, 5).__aenter__())
    await asyncio.sleep(0.2)
    assert not waiting.done()
    freed.set()
    await held
    await asyncio.wait_for(waiting, 2)


@pytest.mark.asyncio
async def test_one_wait_covers_reading_the_count_and_the_hold(count):
    count.update(n=1, read_s=0.3)
    async with cli_slots.computer_cli_slot(CID, 1):
        started = time.monotonic()
        with pytest.raises(SlotWaitTimeout):
            async with cli_slots.computer_cli_slot(CID, 0.5):
                pass
        assert time.monotonic() - started < 0.7


@pytest.mark.asyncio
async def test_a_count_that_cannot_be_read_within_the_wait_is_a_wait_that_ran_out(count):
    count["read_s"] = 5
    started = time.monotonic()
    with pytest.raises(SlotWaitTimeout) as refused:
        await asyncio.wait_for(cli_slots.computer_cli_slot(CID, 0.3).__aenter__(), 2)
    assert (refused.value.which, refused.value.n) == ("cli", CLIS_DEFAULT)
    assert time.monotonic() - started < 0.6


@pytest.mark.asyncio
async def test_a_count_lowered_while_a_cli_waits_in_its_worker_holds_it_too(count):
    async with contextlib.AsyncExitStack() as first:
        await first.enter_async_context(cli_slots.computer_cli_slot(CID, 1))
        second = contextlib.AsyncExitStack()
        await second.enter_async_context(cli_slots.computer_cli_slot(CID, 1))
        waiting = asyncio.ensure_future(cli_slots.computer_cli_slot(CID, 5).__aenter__())
        await asyncio.sleep(0.05)
        count["n"] = 1
        await second.aclose()
        await asyncio.sleep(0.2)
        assert not waiting.done()
    await asyncio.wait_for(waiting, 2)


@pytest.mark.asyncio
async def test_a_count_raised_while_a_cli_waits_in_its_worker_lets_it_in_with_none_ending(count, monkeypatch):
    monkeypatch.setattr(cli_slots, "POLL_S", 0.05)
    count["n"] = 1
    async with cli_slots.computer_cli_slot(CID, 1):
        waiting = asyncio.ensure_future(cli_slots.computer_cli_slot(CID, 5).__aenter__())
        await asyncio.sleep(0.1)
        assert not waiting.done()
        count["n"] = 2
        await asyncio.wait_for(waiting, 1)
