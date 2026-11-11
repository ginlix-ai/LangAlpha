"""ginlix-data's liquidity tables: Redis shared across workers, a memory copy
of it, a refresh every six hours, five minutes' memory of a failure, and no
use of a table built long ago."""

import asyncio
from datetime import datetime, timezone

import pytest
from market_protocol import from_instrument_key

from src.server.services.cache import liquidity_tables as lq

NOW = datetime(2026, 11, 10, 15, tzinfo=timezone.utc).timestamp()


class _Redis:
    """The shared cache: what one worker writes, every other reads."""

    def __init__(self):
        self.data, self.reads = {}, 0

    async def get(self, key):
        self.reads += 1
        return self.data.get(key)

    async def set(self, key, value, ttl=None):
        self.data[key] = value


class _Clock:
    def __init__(self, now=NOW):
        self.now = now

    def __call__(self):
        return self.now


def _body(as_of="2026-11-09", rows=(("NVDA.XNAS", 3.1e10),), currency="USD"):
    return {
        "market": "us", "currency": currency, "as_of": as_of, "sessions": 20,
        "rows": [{"instrument_key": k, "symbol": k.split(".")[0], "adv": v, "rank": i + 1} for i, (k, v) in enumerate(rows)],
    }


class _Route:
    """``get_liquidity_v2``: answers *bodies* in turn (an exception raises, None is a 404)."""

    def __init__(self, *bodies, delay=0.0):
        self.bodies, self.delay, self.calls = list(bodies), delay, 0

    async def __call__(self, market):
        self.calls += 1
        await asyncio.sleep(self.delay)
        body = self.bodies.pop(0) if len(self.bodies) > 1 else self.bodies[0]
        if isinstance(body, Exception):
            raise body
        return body


def _tables(route, redis, clock):
    return lq.LiquidityTables(route, cache=lambda: redis, clock=clock)


@pytest.mark.asyncio
async def test_a_cold_worker_fetches_once_and_shares_it_through_redis():
    redis, clock, route = _Redis(), _Clock(), _Route(_body())
    first = _tables(route, redis, clock)
    a, b = await asyncio.gather(first.table("us"), first.table("us"))  # one fetch for both
    assert route.calls == 1 and a is b and a.value_of(from_instrument_key("NVDA.XNAS")) == 3.1e10

    other = await _tables(route, redis, clock).table("us")  # another worker: Redis, no fetch
    assert route.calls == 1 and other.as_of == "2026-11-09"


@pytest.mark.asyncio
async def test_memory_answers_without_redis_until_the_recheck():
    redis, clock = _Redis(), _Clock()
    tables = _tables(_Route(_body()), redis, clock)
    await tables.table("us")
    reads = redis.reads
    clock.now += lq.RECHECK_S - 1
    await tables.table("us")
    assert redis.reads == reads


@pytest.mark.asyncio
async def test_a_table_another_worker_fetched_is_taken_up_at_the_recheck():
    redis, clock = _Redis(), _Clock()
    mine = _tables(_Route(_body()), redis, clock)
    await mine.table("us")
    clock.now += 60
    await _tables(_Route(_body(as_of="2026-11-10")), redis, clock)._load("us")  # the other worker's refresh
    clock.now += lq.RECHECK_S
    assert (await mine.table("us")).as_of == "2026-11-10"


@pytest.mark.asyncio
async def test_a_due_table_answers_while_its_refresh_runs():
    redis, clock = _Redis(), _Clock()
    route = _Route(_body(), _body(as_of="2026-11-10"), delay=0.05)
    tables = _tables(route, redis, clock)
    await tables.table("us")
    clock.now += lq.REFRESH_S
    assert (await tables.table("us")).as_of == "2026-11-09"  # no wait on the refresh
    await asyncio.sleep(0.1)
    assert route.calls == 2
    assert (await tables.table("us")).as_of == "2026-11-10"


@pytest.mark.asyncio
async def test_a_failure_is_remembered_for_five_minutes_across_workers():
    redis, clock = _Redis(), _Clock()
    route = _Route(None)  # 404: no table built
    tables = _tables(route, redis, clock)
    assert await tables.table("us") is None
    assert await tables.table("us") is None
    assert await _tables(route, redis, clock).table("us") is None  # the other worker sees the marker
    assert route.calls == 1

    clock.now += lq.FAIL_TTL_S + 1
    redis.data.pop(lq._key("us", "failed"))  # Redis expired it
    route.bodies = [_body(as_of="2026-11-10")]
    assert (await tables.table("us")).as_of == "2026-11-10"


@pytest.mark.asyncio
async def test_a_failed_refresh_keeps_the_held_table():
    redis, clock = _Redis(), _Clock()
    route = _Route(_body(), RuntimeError("down"))
    tables = _tables(route, redis, clock)
    await tables.table("us")
    clock.now += lq.REFRESH_S
    assert (await tables.table("us")).as_of == "2026-11-09"
    await asyncio.sleep(0.01)  # the refresh fails
    assert (await tables.table("us")).as_of == "2026-11-09"
    assert route.calls == 2  # and is not retried at once
    assert redis.data[lq._key("us", "failed")] is True


@pytest.mark.asyncio
async def test_a_table_built_long_ago_is_not_used():
    tables = _tables(_Route(_body(as_of="2026-10-20")), _Redis(), _Clock())
    assert await tables.table("us") is None


@pytest.mark.asyncio
async def test_an_uncovered_market_is_never_asked():
    route = _Route(_body())
    assert await _tables(route, _Redis(), _Clock()).table("hk") is None
    assert route.calls == 0


def test_a_key_on_another_venue_finds_its_code():
    table = lq.LiquidityTable.from_wire("us", _body(rows=(("SPY.ARCX", 4e10), ("ABC.ARCX", 1.0), ("ABC.BATS", 2.0))), NOW)
    assert table.value_of(from_instrument_key("SPY.XNYS")) == 4e10
    assert table.value_of(from_instrument_key("ABC.XNYS")) is None  # two listings share the code: no guess


def test_a_table_read_back_from_redis_still_finds_codes():
    table = lq.LiquidityTable.from_wire("us", _body(rows=(("SPY.ARCX", 4e10),)), NOW)
    assert lq.LiquidityTable.from_stored(table.stored()).value_of(from_instrument_key("SPY.XNYS")) == 4e10
