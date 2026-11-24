"""The gate a workspace's calls on its computer wait at while its synced files
are brought up to date off the turn's path."""

from __future__ import annotations

import asyncio

import pytest

from ptc_agent.core.sandbox.asset_gate import AssetGate

pytestmark = pytest.mark.asyncio


async def _yield(times: int = 5) -> None:
    for _ in range(times):
        await asyncio.sleep(0)


async def test_a_workspace_nothing_holds_is_settled_and_waits_for_no_other():
    gate = AssetGate()
    gate.hold("ws-b")
    ran: list[int] = []
    asyncio.get_running_loop().call_soon(ran.append, 1)

    assert gate.settled("ws-a")
    assert await gate.wait("ws-a") == 0.0
    assert ran == []
    assert not gate.settled("ws-b")


async def test_a_wait_ends_only_once_every_hold_is_released_including_one_taken_while_it_waited():
    gate = AssetGate()
    first = gate.hold("ws-a")
    waiter = asyncio.create_task(gate.wait("ws-a", timeout=5))
    await _yield()
    second = gate.hold("ws-a")

    first()
    await _yield()
    assert not waiter.done()

    second()
    waited = await asyncio.wait_for(waiter, 1)
    assert waited > 0.0
    assert gate.settled("ws-a")


async def test_a_release_is_idempotent_and_one_of_two_leaves_the_workspace_held():
    gate = AssetGate()
    first = gate.hold("ws-a")
    second = gate.hold("ws-a")

    first()
    first()
    assert not gate.settled("ws-a")

    second()
    assert gate.settled("ws-a")
    first()
    second()
    assert gate.settled("ws-a")
    assert await gate.wait("ws-a") == 0.0


async def test_a_hold_that_outlives_the_timeout_ends_the_wait_without_raising():
    gate = AssetGate()
    gate.hold("ws-a")

    waited = await gate.wait("ws-a", timeout=0.01)

    assert waited >= 0.01
    assert not gate.settled("ws-a")


async def test_a_wait_with_no_timeout_ends_only_by_the_release():
    gate = AssetGate()
    release = gate.hold("ws-a")

    waiting = asyncio.create_task(gate.wait("ws-a", timeout=None))
    await asyncio.sleep(0.02)
    assert not waiting.done()

    release()
    # Above zero only: the wait's own clock starts once its task first runs.
    assert await asyncio.wait_for(waiting, 1) > 0


async def test_a_hold_on_the_computer_holds_every_workspace_until_its_release():
    gate = AssetGate()
    release = gate.hold_computer()
    assert not gate.settled("ws-a")
    assert not gate.settled("ws-b")
    assert not gate.settled(None)

    waiting = asyncio.ensure_future(gate.wait("ws-a", timeout=None))
    await _yield()
    assert not waiting.done()

    release()
    await asyncio.wait_for(waiting, 1)
    assert gate.settled("ws-a")


async def test_a_workspace_held_with_its_computer_waits_for_both():
    gate = AssetGate()
    release_own = gate.hold("ws-a")
    release_computer = gate.hold_computer()
    waiting = asyncio.ensure_future(gate.wait("ws-a", timeout=None))

    release_own()
    await _yield()
    assert not waiting.done()
    assert gate.settled("ws-b") is False

    release_computer()
    await asyncio.wait_for(waiting, 1)
    assert gate.settled("ws-b")



async def test_a_caller_at_the_root_waits_for_every_hold_including_one_taken_meanwhile():
    gate = AssetGate()
    assert await gate.wait_all() == 0.0
    release_a = gate.hold("ws-a")
    waiting = asyncio.ensure_future(gate.wait_all(timeout=None))
    await _yield()

    release_b = gate.hold("ws-b")
    release_a()
    await _yield()
    assert not waiting.done()

    release_b()
    assert await asyncio.wait_for(waiting, 1) > 0
