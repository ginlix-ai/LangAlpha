"""A computer's Claude Code CLIs held to the number its owner set.

Read as each CLI starts, the count holds first among the CLIs this worker
runs there, then across every worker through Redis slots, all within the one
wait the CLI is given. A dead worker's slots are reaped once they stop being
renewed, a live one's renewal counts it again after Redis lost it, and with
Redis gone each worker still holds its own CLIs to the count.
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


class _Pipeline:
    def __init__(self, redis):
        self._redis, self._ops = redis, []

    def zremrangebyscore(self, key, lo, hi):
        self._ops.append(("rem", key, hi))
        return self

    def zadd(self, key, mapping, xx=False):
        self._ops.append(("add", key, mapping, xx))
        return self

    def zcard(self, key):
        self._ops.append(("card", key))
        return self

    def expire(self, key, ttl):
        self._ops.append(("expire",))
        return self

    async def execute(self):
        out = []
        for op in self._ops:
            members = self._redis.zsets.setdefault(op[1], {}) if len(op) > 1 else None
            if op[0] == "rem":
                for m in [m for m, s in members.items() if s <= op[2]]:
                    del members[m]
                out.append(0)
            elif op[0] == "add":
                mapping = {m: s for m, s in op[2].items() if not op[3] or m in members}
                members.update(mapping)
                out.append(len(mapping))
            elif op[0] == "card":
                out.append(len(members))
            else:
                out.append(True)
        return out


class _Redis:
    def __init__(self, down=False):
        self.zsets: dict[str, dict[str, float]] = {}
        self.down = down

    def pipeline(self):
        if self.down:
            raise ConnectionError("redis is down")
        return _Pipeline(self)

    async def zrem(self, key, member):
        self.zsets.get(key, {}).pop(member, None)


class _Cache:
    def __init__(self, client, enabled=True):
        self.client, self.enabled = client, enabled


@pytest.fixture
def count(monkeypatch):
    counts = {"n": 2, "read_s": 0.0}

    async def computer_clis(computer_id):
        await asyncio.sleep(counts["read_s"])
        return counts.get(computer_id, counts["n"])

    monkeypatch.setattr(cli_slots, "computer_clis", computer_clis)
    return counts


@pytest.fixture
def redis(monkeypatch, count):
    client = _Redis()
    monkeypatch.setattr(cli_slots, "get_cache_client", lambda: _Cache(client))
    monkeypatch.setattr(cli_slots, "POLL_S", 0.05)
    return client


def _held(redis):
    return redis.zsets.get(cli_slots.slots_key(CID), {})


@pytest.mark.asyncio
@pytest.mark.parametrize("cache", ["up", "down", "disabled"])
async def test_a_worker_holds_its_own_clis_to_the_computers_count_with_or_without_redis(monkeypatch, count, cache):
    client = _Redis(down=cache == "down")
    monkeypatch.setattr(cli_slots, "get_cache_client", lambda: _Cache(client, enabled=cache != "disabled"))
    async with cli_slots.computer_cli_slot(CID, 1), cli_slots.computer_cli_slot(CID, 1):
        with pytest.raises(SlotWaitTimeout) as refused:
            async with cli_slots.computer_cli_slot(CID, 0.2):
                pass
        assert (refused.value.which, refused.value.n) == ("cli", 2)
    async with cli_slots.computer_cli_slot(CID, 0.2):
        pass


@pytest.mark.asyncio
async def test_each_computer_on_a_login_is_held_to_its_own_count(redis, count):
    """Two computers on one machine's login share its connections but not
    their counts: one set to 8 running 2 leaves one set to 2 its own two."""
    count.update({"x": 8, CID: 2})
    async with cli_slots.computer_cli_slot("x", 1), cli_slots.computer_cli_slot("x", 1):
        async with cli_slots.computer_cli_slot(CID, 0.2), cli_slots.computer_cli_slot(CID, 0.2):
            pass


@pytest.mark.asyncio
async def test_a_cli_waiting_in_its_worker_starts_once_one_there_frees(redis):
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
    assert len(_held(redis)) == 1


@pytest.mark.asyncio
async def test_clis_in_other_workers_count_against_the_computers_one_count(redis):
    redis.zsets[cli_slots.slots_key(CID)] = {"other-worker": time.time()}
    async with cli_slots.computer_cli_slot(CID, 1):
        assert len(_held(redis)) == 2
        with pytest.raises(SlotWaitTimeout) as refused:
            async with cli_slots.computer_cli_slot(CID, 0.2):
                pass
        assert refused.value.n == 2
    assert list(_held(redis)) == ["other-worker"]


@pytest.mark.asyncio
async def test_one_wait_covers_reading_the_count_and_both_holds(redis, count):
    count.update(n=1, read_s=0.3)
    async with cli_slots.computer_cli_slot(CID, 1):
        started = time.monotonic()
        with pytest.raises(SlotWaitTimeout):
            async with cli_slots.computer_cli_slot(CID, 0.5):
                pass
        assert time.monotonic() - started < 0.7


@pytest.mark.asyncio
async def test_a_count_that_cannot_be_read_within_the_wait_is_a_wait_that_ran_out(redis, count):
    count["read_s"] = 5
    started = time.monotonic()
    with pytest.raises(SlotWaitTimeout) as refused:
        await asyncio.wait_for(cli_slots.computer_cli_slot(CID, 0.3).__aenter__(), 2)
    assert (refused.value.which, refused.value.n) == ("cli", CLIS_DEFAULT)
    assert time.monotonic() - started < 0.6


@pytest.mark.asyncio
async def test_a_count_lowered_while_a_cli_waits_in_its_worker_holds_it_too(redis, count):
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
async def test_a_count_raised_while_a_cli_waits_in_its_worker_lets_it_in_with_none_ending(redis, count):
    count["n"] = 1
    async with cli_slots.computer_cli_slot(CID, 1):
        waiting = asyncio.ensure_future(cli_slots.computer_cli_slot(CID, 5).__aenter__())
        await asyncio.sleep(0.1)
        assert not waiting.done()
        count["n"] = 2
        await asyncio.wait_for(waiting, 1)


@pytest.mark.asyncio
async def test_redis_failing_after_it_found_the_slots_full_keeps_the_cli_waiting(redis, count, monkeypatch):
    count["n"] = 1
    redis.zsets[cli_slots.slots_key(CID)] = {"other-worker": time.time()}
    admit = cli_slots.acquire_slot_member
    tries = []

    async def blinking(*args, **kwargs):
        tries.append(1)
        if len(tries) == 2:
            raise ConnectionError("redis blinked")
        return await admit(*args, **kwargs)

    monkeypatch.setattr(cli_slots, "acquire_slot_member", blinking)
    with pytest.raises(SlotWaitTimeout) as refused:
        async with cli_slots.computer_cli_slot(CID, 0.3):
            pass
    assert refused.value.n == 1 and len(tries) > 2
    assert list(_held(redis)) == ["other-worker"]


@pytest.mark.asyncio
async def test_a_count_lowered_while_a_cli_waits_on_other_workers_holds_it_too(redis, count):
    redis.zsets[cli_slots.slots_key(CID)] = {"other-worker": time.time()}
    async with cli_slots.computer_cli_slot(CID, 1):
        waiting = asyncio.ensure_future(cli_slots.computer_cli_slot(CID, 5).__aenter__())
        await asyncio.sleep(0.1)
        assert not waiting.done()
        count["n"] = 1
        del _held(redis)["other-worker"]
        await asyncio.sleep(0.3)
        assert not waiting.done()
    await asyncio.wait_for(waiting, 2)


@pytest.mark.asyncio
async def test_a_running_cli_keeps_its_slot_and_a_dead_workers_is_reaped(redis, count, monkeypatch):
    count["n"] = 1
    monkeypatch.setattr(cli_slots, "RENEW_S", 0.05)
    monkeypatch.setattr(cli_slots, "STALE_S", 0.3)
    redis.zsets[cli_slots.slots_key(CID)] = {"dead-worker": 0.0}
    async with cli_slots.computer_cli_slot(CID, 1):
        await asyncio.sleep(0.6)
        assert "dead-worker" not in _held(redis) and len(_held(redis)) == 1


@pytest.mark.asyncio
async def test_a_renewal_counts_again_a_cli_redis_lost(redis, monkeypatch):
    monkeypatch.setattr(cli_slots, "RENEW_S", 0.05)
    async with cli_slots.computer_cli_slot(CID, 1):
        mine = list(_held(redis))
        del redis.zsets[cli_slots.slots_key(CID)]
        await asyncio.sleep(0.15)
        assert list(_held(redis)) == mine
    assert _held(redis) == {}


@pytest.mark.asyncio
async def test_a_wait_cancelled_while_redis_admits_it_frees_the_slot_once_it_lands(redis, monkeypatch):
    landing = asyncio.Event()
    admit = cli_slots.acquire_slot_member

    async def slow(*args, **kwargs):
        await landing.wait()
        return await admit(*args, **kwargs)

    monkeypatch.setattr(cli_slots, "acquire_slot_member", slow)
    entering = asyncio.ensure_future(cli_slots.computer_cli_slot(CID, 5).__aenter__())
    await asyncio.sleep(0.05)
    entering.cancel()
    await asyncio.sleep(0.05)
    assert entering.done()
    landing.set()
    await asyncio.sleep(0.05)
    with pytest.raises(asyncio.CancelledError):
        await entering
    assert _held(redis) == {}
    async with cli_slots.computer_cli_slot(CID, 0.2), cli_slots.computer_cli_slot(CID, 0.2):
        pass



@pytest.mark.asyncio
async def test_redis_answering_no_admission_within_the_wait_holds_only_this_workers_clis(redis, monkeypatch):
    """A Redis that does not answer is one that is down, however long its
    client would wait: the CLI starts on its worker's count within its wait."""

    async def slow(*args, **kwargs):
        await asyncio.sleep(3)

    monkeypatch.setattr(cli_slots, "acquire_slot_member", slow)
    started = time.monotonic()

    async def entered():
        async with cli_slots.computer_cli_slot(CID, 0.3):
            return time.monotonic() - started

    assert await asyncio.wait_for(entered(), 2) < 0.6
    assert _held(redis) == {}


@pytest.mark.asyncio
async def test_a_renewal_under_way_as_the_cli_ends_lands_before_its_slot_is_freed(redis, monkeypatch):
    """A renewal already sent lands whether or not its answer is awaited, so
    freeing the slot first would leave it held again until it went stale."""
    monkeypatch.setattr(cli_slots, "RENEW_S", 0.01)
    sent = asyncio.Event()

    async def renew(client, key, slot_id, *, stale_after):
        sent.set()
        try:
            await asyncio.sleep(0.05)
        finally:
            client.zsets.setdefault(key, {})[slot_id] = time.time()

    monkeypatch.setattr(cli_slots, "renew_slot_member", renew)
    async with cli_slots.computer_cli_slot(CID, 1):
        await sent.wait()
    await asyncio.sleep(0.1)
    assert _held(redis) == {}
