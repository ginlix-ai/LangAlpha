"""The lease that keeps a computer to one asset sync at a time across workers."""

from __future__ import annotations

import asyncio

import pytest

from src.server.services.computer_manager import _sync_turn
from src.server.services.computer_manager._sync_turn import sync_turn, wait_for_sync

pytestmark = pytest.mark.asyncio

COMPUTER = "computer-1"
KEY = "asset-sync:computer-1"


class _Redis:
    """The four calls the turn makes, over one dict; ``away`` answers as a
    cache with no Redis behind it does."""

    def __init__(self, *, away: bool = False) -> None:
        self.held: dict[str, str] = {}
        self.away = away
        self.extended: list[str] = []

    async def acquire_lock(self, key: str, token: str, ttl_ms: int) -> bool | None:
        if self.away:
            return None
        if key in self.held:
            return False
        self.held[key] = token
        return True

    async def extend_lock(self, key: str, token: str, ttl_ms: int) -> bool:
        if self.held.get(key) != token:
            return False
        self.extended.append(key)
        return True

    async def release_lock(self, key: str, token: str) -> None:
        if self.held.get(key) == token:
            del self.held[key]

    async def exists(self, key: str) -> bool:
        return not self.away and key in self.held


@pytest.fixture
def redis(monkeypatch):
    fake = _Redis()
    monkeypatch.setattr(_sync_turn, "get_cache_client", lambda: fake)
    monkeypatch.setattr(_sync_turn, "POLL_S", 0.001)
    return fake


async def test_a_sync_holds_the_turn_for_its_length_and_gives_it_back(redis):
    async with sync_turn(COMPUTER):
        assert KEY in redis.held
    assert redis.held == {}


async def test_a_second_sync_waits_for_the_first_to_end(redis):
    order: list[str] = []
    first_in, let_first_go = asyncio.Event(), asyncio.Event()

    async def first():
        async with sync_turn(COMPUTER):
            first_in.set()
            await let_first_go.wait()
            order.append("first ended")

    async def second():
        async with sync_turn(COMPUTER):
            order.append("second ran")

    running = asyncio.ensure_future(first())
    await first_in.wait()
    waiting = asyncio.ensure_future(second())
    await asyncio.sleep(0.01)
    assert not waiting.done()

    let_first_go.set()
    await asyncio.wait_for(asyncio.gather(running, waiting), 1)
    assert order == ["first ended", "second ran"]
    assert redis.held == {}


async def test_a_sync_that_fails_or_is_cancelled_gives_the_turn_back(redis):
    with pytest.raises(RuntimeError):
        async with sync_turn(COMPUTER):
            raise RuntimeError("upload refused")
    assert redis.held == {}

    entered = asyncio.Event()

    async def cut_short():
        async with sync_turn(COMPUTER):
            entered.set()
            await asyncio.sleep(60)

    task = asyncio.ensure_future(cut_short())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert redis.held == {}


async def test_with_redis_away_a_sync_runs_without_a_turn(monkeypatch):
    away = _Redis(away=True)
    monkeypatch.setattr(_sync_turn, "get_cache_client", lambda: away)
    ran = False
    async with sync_turn(COMPUTER):
        ran = True
    assert ran and away.held == {}
    assert await wait_for_sync(COMPUTER) < 0.5


async def test_a_holder_that_outlasts_the_wait_is_synced_beside(redis, monkeypatch):
    """A lease its holder keeps renewing past the wait, or one left by a
    holder that will never release it, does not stop a sync for good."""
    monkeypatch.setattr(_sync_turn, "MAX_WAIT_S", 0.02)
    redis.held[KEY] = "another worker"
    ran = False
    async with sync_turn(COMPUTER):
        ran = True
    assert ran
    assert redis.held == {KEY: "another worker"}


async def test_a_long_sync_keeps_renewing_its_lease(redis, monkeypatch):
    monkeypatch.setattr(_sync_turn, "RENEW_S", 0.005)
    async with sync_turn(COMPUTER):
        await asyncio.sleep(0.03)
    assert len(redis.extended) >= 2


async def test_a_stop_waits_for_a_sync_it_cannot_cancel_until_it_ends(redis):
    redis.held[KEY] = "another worker"
    waiting = asyncio.ensure_future(wait_for_sync(COMPUTER))
    await asyncio.sleep(0.01)
    assert not waiting.done()

    del redis.held[KEY]
    assert await asyncio.wait_for(waiting, 1) > 0


async def test_a_stop_goes_ahead_once_it_has_waited_its_bound(redis, monkeypatch):
    monkeypatch.setattr(_sync_turn, "STOP_WAIT_S", 0.02)
    redis.held[KEY] = "another worker"
    waited = await asyncio.wait_for(wait_for_sync(COMPUTER), 1)
    assert waited >= 0.02
    assert redis.held == {KEY: "another worker"}
