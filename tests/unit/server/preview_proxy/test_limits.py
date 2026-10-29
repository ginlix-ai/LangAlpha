"""One sandbox's previews cannot take what every other user's share."""

from __future__ import annotations

import asyncio

import pytest

from src.server.preview_proxy.limits import SandboxSlots


@pytest.mark.asyncio
async def test_a_full_sandbox_is_refused_while_another_still_gets_through():
    slots = SandboxSlots(2)

    assert await slots.acquire("a")
    assert await slots.acquire("a")
    assert not await slots.acquire("a")
    assert not await slots.acquire("a", wait=0.01)
    assert await slots.acquire("b")

    for key in ("a", "a", "b"):
        slots.release(key)
    assert slots._slots == {} and slots._users == {}


@pytest.mark.asyncio
async def test_a_waiting_request_takes_the_slot_that_frees():
    slots = SandboxSlots(1)
    assert await slots.acquire("a")

    waiting = asyncio.create_task(slots.acquire("a", wait=5))
    await asyncio.sleep(0)
    slots.release("a")

    assert await waiting
    slots.release("a")
    assert slots._slots == {}
