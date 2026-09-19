"""The Redis lease behind sandbox path writes: taken when it can be, never a wall."""

import asyncio

import pytest

from src.server.services import path_lock_coordination as coord


class _FakeCache:
    def __init__(self, answers):
        self.answers = list(answers)
        self.acquired: list[tuple[str, str, int]] = []
        self.released: list[tuple[str, str]] = []

    async def acquire_lock(self, key, token, ttl_ms):
        self.acquired.append((key, token, ttl_ms))
        return self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]

    async def release_lock(self, key, token):
        self.released.append((key, token))


class _StopClient:
    def __init__(self):
        self.set_calls = []
        self.eval_calls = []
        self.exists_result = 1
        self.renewed = asyncio.Event()

    async def set(self, *args, **kwargs):
        self.set_calls.append((args, kwargs))
        return True

    async def eval(self, *args):
        self.eval_calls.append(args)
        if args[0] == coord._RENEW_STOP_HEARTBEAT:
            self.renewed.set()
        return 1

    async def exists(self, key):
        return self.exists_result


class _StopCache:
    def __init__(self):
        self.client = _StopClient()


@pytest.mark.asyncio
async def test_a_held_lease_is_released_with_its_own_token():
    cache = _FakeCache([True])
    async with coord.redis_path_lease("sb", "/a", cache=cache):
        pass
    key, token, ttl = cache.acquired[0]
    assert key == coord.lease_key("sb", "/a")
    assert ttl == coord.LEASE_TTL_MS
    assert cache.released == [(key, token)]


@pytest.mark.asyncio
async def test_redis_down_proceeds_without_a_lease():
    cache = _FakeCache([None])
    async with coord.redis_path_lease("sb", "/a", cache=cache):
        pass
    assert len(cache.acquired) == 1
    assert cache.released == []


@pytest.mark.asyncio
async def test_a_contender_waits_for_the_holder():
    cache = _FakeCache([False, False, True])
    async with coord.redis_path_lease("sb", "/a", cache=cache, poll_interval_s=0):
        pass
    assert [a[0] for a in cache.acquired] == [coord.lease_key("sb", "/a")] * 3
    assert len(cache.released) == 1


@pytest.mark.asyncio
async def test_the_wait_is_bounded_and_fails_open():
    cache = _FakeCache([False])
    async with coord.redis_path_lease(
        "sb", "/a", cache=cache, wait_timeout_s=0.02, poll_interval_s=0.005
    ):
        pass
    assert cache.released == []


@pytest.mark.asyncio
async def test_release_survives_cancellation_of_the_holder():
    cache = _FakeCache([True])
    started = asyncio.Event()

    async def holder():
        async with coord.redis_path_lease("sb", "/a", cache=cache):
            started.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(holder())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(cache.released) == 1


def test_the_key_hides_the_path_and_separates_scopes():
    a = coord.lease_key("sb-1", "/home/workspace/x.md")
    b = coord.lease_key("sb-2", "/home/workspace/x.md")
    assert a != b
    assert "/home" not in a


@pytest.mark.asyncio
async def test_stop_heartbeat_is_refreshed_and_released_with_its_token(monkeypatch):
    cache = _StopCache()
    block = asyncio.Event()
    sleeps = 0

    async def controlled_sleep(delay):
        nonlocal sleeps
        assert delay == coord.STOP_HEARTBEAT_REFRESH_S
        sleeps += 1
        if sleeps == 1:
            return
        await block.wait()

    monkeypatch.setattr(coord.asyncio, "sleep", controlled_sleep)
    async with coord.computer_stop_heartbeat("comp-1", cache=cache):
        await cache.client.renewed.wait()

    assert cache.client.renewed.is_set()
    (set_args, set_kwargs) = cache.client.set_calls[0]
    assert set_args[0] == "computer:stop:comp-1"
    assert set_kwargs == {"ex": coord.STOP_HEARTBEAT_TTL_S}
    renew, release = cache.client.eval_calls[0], cache.client.eval_calls[-1]
    assert renew[0] == coord._RENEW_STOP_HEARTBEAT
    assert release[0] == coord._RELEASE_STOP_HEARTBEAT
    assert renew[3] == release[3]


@pytest.mark.asyncio
async def test_stop_heartbeat_presence_reads_the_machine_key():
    cache = _StopCache()
    assert await coord.computer_stop_heartbeat_present("comp-1", cache=cache)
    cache.client.exists_result = 0
    assert not await coord.computer_stop_heartbeat_present("comp-1", cache=cache)
