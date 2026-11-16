import asyncio

import pytest

from src.server.services import turn_awake


class _Runtime:
    def __init__(self, answers):
        self.answers = list(answers)
        self.holds: list[int] = []

    async def hold_awake(self, seconds: int) -> bool:
        self.holds.append(seconds)
        answer = self.answers.pop(0) if self.answers else True
        if isinstance(answer, Exception):
            raise answer
        return answer


async def _stream(events, gate: asyncio.Event):
    for event in events:
        yield event
    await gate.wait()


async def _drive(runtime, *, renewals: int) -> list:
    """Run a turn until ``runtime`` has been asked ``renewals`` times (or the
    hold gave up), then end it and wait a few renew periods more."""
    gate = asyncio.Event()
    seen = []

    async def consume():
        async for event in turn_awake.held_awake(lambda: runtime, _stream([1, 2], gate)):
            seen.append(event)

    task = asyncio.create_task(consume())
    for _ in range(200):
        if len(runtime.holds) >= renewals:
            break
        await asyncio.sleep(0.005)
    gate.set()
    await task
    return seen


@pytest.fixture(autouse=True)
def _fast_renewals(monkeypatch):
    monkeypatch.setattr(turn_awake, "RENEW_S", 0.01)


@pytest.mark.asyncio
async def test_a_mac_is_held_awake_until_the_turn_ends():
    runtime = _Runtime([])
    assert await _drive(runtime, renewals=3) == [1, 2]
    assert runtime.holds[:3] == [turn_awake.HOLD_S] * 3
    asked = len(runtime.holds)
    await asyncio.sleep(0.05)
    assert len(runtime.holds) == asked


@pytest.mark.asyncio
async def test_a_machine_that_never_sleeps_is_asked_once():
    runtime = _Runtime([False])
    await _drive(runtime, renewals=3)
    assert runtime.holds == [turn_awake.HOLD_S]


@pytest.mark.asyncio
async def test_a_failed_hold_is_tried_again_at_the_next_renewal():
    runtime = _Runtime([ConnectionError("link lost")])
    await _drive(runtime, renewals=2)
    assert len(runtime.holds) >= 2
