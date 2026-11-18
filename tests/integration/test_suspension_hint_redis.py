"""The suspension hint's Redis writes, against a real Redis.

The hint is written in the fenced Lua (``set_many_fenced(counter=, delete=)``)
and in the epoch bump's MULTI (``bump(also_set=, also_delete=)``), whose slot
arithmetic and transaction a fake cache only imitates. Requires ``REDIS_URL``.
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def cache():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not redis_url.startswith("redis://"):
        pytest.skip("REDIS_URL not set to a real Redis instance")
    from src.utils.cache.redis_cache import RedisCacheClient

    client = RedisCacheClient(url=redis_url, max_connections=4)
    try:
        await client.connect()
    except Exception as exc:
        pytest.skip(f"Redis is not reachable at REDIS_URL: {exc}")
    if not client.enabled or not client.client:
        pytest.skip("Redis client did not initialize")
    prefix = f"test-hint:{uuid.uuid4().hex}"
    yield client, prefix
    keys = [k async for k in client.client.scan_iter(match=f"{prefix}*")]
    if keys:
        await client.client.delete(*keys)
    await client.disconnect()


async def _ttl(client, key: str) -> int:
    return await client.client.ttl(key)


async def test_a_write_under_its_counter_sets_each_value_and_deletes(cache):
    client, p = cache
    counter, a, b, gone = f"{p}:epoch", f"{p}:a", f"{p}:b", f"{p}:gone"
    assert await client.bump(counter, 100) == 1
    await client.set(gone, True, ttl=50)

    assert await client.set_many_fenced(
        [(a, {"x": 1}, 30), (b, True, 40)],
        counter=(counter, 1, 200),
        delete=(gone,),
    )

    assert await client.mget([a, b, gone]) == [{"x": 1}, True, None]
    # Each value keeps its own TTL, and the counter's is renewed to outlive them.
    assert 25 < await _ttl(client, a) <= 30
    assert 35 < await _ttl(client, b) <= 40
    assert 190 < await _ttl(client, counter) <= 200


async def test_a_write_from_before_a_bump_writes_and_deletes_nothing(cache):
    client, p = cache
    counter, a, kept = f"{p}:epoch", f"{p}:a", f"{p}:kept"
    await client.bump(counter, 100)
    await client.set(kept, True, ttl=50)
    await client.bump(counter, 100)

    assert not await client.set_many_fenced(
        [(a, 1, 30)], counter=(counter, 1, 200), delete=(kept,)
    )

    assert await client.mget([a, kept]) == [None, True]


async def test_a_counter_never_bumped_reads_as_zero(cache):
    client, p = cache
    counter, a, gone = f"{p}:epoch", f"{p}:a", f"{p}:gone"
    await client.set(gone, True, ttl=50)

    assert await client.set_many_fenced(
        [(a, 1, 30)], counter=(counter, 0, 200), delete=(gone,)
    )
    assert await client.mget([a, gone, counter]) == [1, None, None]
    # A delete alone, with nothing to set.
    await client.set(gone, True, ttl=50)
    assert await client.set_many_fenced([], counter=(counter, 0, 200), delete=(gone,))
    assert await client.get(gone) is None


async def test_a_bump_sets_and_deletes_in_its_own_transaction(cache):
    client, p = cache
    counter, hint, other = f"{p}:epoch", f"{p}:hint", f"{p}:other"
    await client.set(other, True, ttl=50)

    assert await client.bump(counter, 100, also_set=(hint, True, 60), also_delete=(other,)) == 1
    assert await client.mget([hint, other]) == [True, None]
    assert 55 < await _ttl(client, hint) <= 60
    assert 95 < await _ttl(client, counter) <= 100

    assert await client.bump(counter, 100, also_delete=(hint,)) == 2
    assert await client.get(hint) is None


async def test_the_hint_end_to_end_on_a_real_redis(cache, monkeypatch):
    """A notice suspends, the auth service is down: the hint refuses. An
    answer it cannot read keeps the hint; one that reads active clears it."""
    from src.server.services import membership
    from src.server.services import account
    from tests.unit.server.services.test_account_contract import _Auth, _deployment

    client, p = cache
    user = f"{p}-u"

    class _Down(_Auth):
        async def post(self, url, json=None, headers=None):
            import httpx

            raise httpx.ConnectError("down")

    member = {
        "valid": True, "access_tier": 2, "plan_display_name": "Pro",
        "org": {"id": "o-1", "name": "Acme"}, "features": {"teams": True},
    }
    hint_key = membership._membership_suspended_key(user)
    try:
        with _deployment(client, _Auth(member), door=True):
            assert not (await account.read_standing(user)).suspended
            await membership.forget_membership(user, suspended=True)
        assert await client.get(hint_key) is True
        with _deployment(client, _Down(), door=True):
            assert (await account.read_standing(user)).suspended
        with _deployment(client, _Auth({**member, "org": {"id": "o-1"}}), door=True):
            await membership.forget_membership(user)
            assert (await account.read_standing(user)).suspended
        assert await client.get(hint_key) is True
        with _deployment(client, _Auth(member), door=True):
            await membership.forget_membership(user)
            assert not (await account.read_standing(user)).suspended
        assert await client.get(hint_key) is None
    finally:
        keys = [k async for k in client.client.scan_iter(match=f"*{user}*")]
        if keys:
            await client.client.delete(*keys)
