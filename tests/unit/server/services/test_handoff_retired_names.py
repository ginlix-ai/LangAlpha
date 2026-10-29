"""The startup rename of report-back state left under the Flash agent's names.

Every worker runs it at once, so the fake Redis yields between each call: two
runs interleave the way two processes would, and a key one of them moves is
gone when the other reaches it.
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ResponseError

from src.server.services.report_back.handoff import keys, retired_names

CACHE = "src.utils.cache.redis_cache.get_cache_client"


class _Redis:
    """Strings, sets and lists by key, each with an optional TTL."""

    def __init__(self):
        self.data: dict[str, object] = {}
        self.ttl: dict[str, int] = {}
        self.renames = 0
        self.lost = 0

    def put(self, key, value, ttl=None):
        self.data[key] = value
        if ttl is not None:
            self.ttl[key] = ttl

    async def scan_iter(self, match=None, count=None):
        for key in [k for k in list(self.data) if fnmatch.fnmatchcase(k, match)]:
            await asyncio.sleep(0)
            yield key.encode()

    async def rename(self, src, dst):
        await asyncio.sleep(0)
        if src not in self.data:
            self.lost += 1
            raise ResponseError("no such key")
        self.data[dst] = self.data.pop(src)
        self.ttl.pop(dst, None)
        if src in self.ttl:
            self.ttl[dst] = self.ttl.pop(src)
        self.renames += 1

    async def get(self, key):
        await asyncio.sleep(0)
        return self.data.get(key)

    async def eval(self, script, numkeys, key, expected, new):
        assert script == retired_names._REWRITE_LUA and numkeys == 1
        await asyncio.sleep(0)
        if self.data.get(key) != expected:
            return 0
        self.data[key] = new.encode()  # SET ... KEEPTTL leaves self.ttl alone
        return 1


def _old_origin(**extra) -> bytes:
    return json.dumps(
        {
            "origin": "flash",
            "flash_thread_id": "home-1",
            "flash_workspace_id": None,
            "ptc_thread_id": "ptc-a",
            "user_id": "u-1",
            "dispatch_gen": "g-1",
            "requested_from": [{"asked": "the brief"}],
            **extra,
        }
    ).encode()


async def _run(redis):
    with patch(CACHE, return_value=SimpleNamespace(enabled=True, client=redis)):
        await retired_names.rename_retired_names()


@pytest.mark.asyncio
async def test_each_old_prefix_moves_to_its_new_name_with_its_ttl():
    redis = _Redis()
    redis.put("flash_watch:home-1", {"ptc-a"}, ttl=600)
    redis.put("flash_rb_run:home-1:ptc-a", b"run-1", ttl=500)
    redis.put("flash_rb_done:home-1", [b"run-0"], ttl=400)
    redis.put("flash_user_pending:u-1", {"ptc-a"}, ttl=300)

    await _run(redis)

    assert redis.data == {
        keys.watch_key("home-1"): {"ptc-a"},
        keys.rb_run_key("home-1", "ptc-a"): b"run-1",
        keys.rb_done_key("home-1"): [b"run-0"],
        keys.user_pending_key("u-1"): {"ptc-a"},
    }
    assert redis.ttl == {
        "handoff_watch:home-1": 600,
        "handoff_rb_run:home-1:ptc-a": 500,
        "handoff_rb_done:home-1": 400,
        "handoff_user_pending:u-1": 300,
    }


@pytest.mark.asyncio
async def test_keys_it_does_not_name_are_left_alone():
    redis = _Redis()
    redis.put("flash_rb_queue:home-1", [b"ptc-a"], ttl=60)
    redis.put("handoff_watch:home-2", {"ptc-b"})
    redis.put("thread:wake:home-1", b"x")

    await _run(redis)

    assert set(redis.data) == {"flash_rb_queue:home-1", "handoff_watch:home-2", "thread:wake:home-1"}
    assert redis.renames == 0


@pytest.mark.asyncio
async def test_an_origin_record_takes_the_new_names_and_keeps_the_rest_and_its_ttl():
    redis = _Redis()
    redis.put("ptc_origin:ptc-a", _old_origin(), ttl=900)

    await _run(redis)

    assert json.loads(redis.data["ptc_origin:ptc-a"]) == {
        "origin": "home",
        "home_thread_id": "home-1",
        "home_workspace_id": None,
        "ptc_thread_id": "ptc-a",
        "user_id": "u-1",
        "dispatch_gen": "g-1",
        "requested_from": [{"asked": "the brief"}],
    }
    assert redis.ttl == {"ptc_origin:ptc-a": 900}


@pytest.mark.parametrize(
    "value",
    [
        json.dumps({"origin": "home", "home_thread_id": "home-1"}).encode(),
        b"not json",
        json.dumps(["a", "list"]).encode(),
    ],
)
@pytest.mark.asyncio
async def test_a_record_with_nothing_to_rename_is_not_written(value):
    redis = _Redis()
    redis.put("ptc_origin:ptc-a", value)
    redis.eval = AsyncMock()

    await _run(redis)

    assert redis.data["ptc_origin:ptc-a"] == value
    redis.eval.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_record_naming_both_keeps_the_new_name():
    redis = _Redis()
    redis.put("ptc_origin:ptc-a", _old_origin(home_thread_id="home-2"))

    await _run(redis)

    record = json.loads(redis.data["ptc_origin:ptc-a"])
    assert record["home_thread_id"] == "home-2"
    assert "flash_thread_id" not in record


@pytest.mark.asyncio
async def test_two_workers_at_once_end_where_one_would_and_neither_fails():
    redis = _Redis()
    for i in range(5):
        redis.put(f"flash_watch:home-{i}", {f"ptc-{i}"}, ttl=600 + i)
        redis.put(f"flash_rb_run:home-{i}:ptc-{i}", f"run-{i}".encode(), ttl=500 + i)
        redis.put(f"ptc_origin:ptc-{i}", _old_origin(ptc_thread_id=f"ptc-{i}"), ttl=900 + i)

    # One patch around both: two patches of one name, unwound out of order,
    # would leave the fake installed for every later test.
    with patch(CACHE, return_value=SimpleNamespace(enabled=True, client=redis)):
        await asyncio.gather(
            retired_names.rename_retired_names(), retired_names.rename_retired_names()
        )

    assert redis.renames == 10  # each key moved once
    assert redis.lost > 0  # and the runs did race: the loser found no key
    assert sorted(k for k in redis.data if k.startswith("flash_")) == []
    for i in range(5):
        assert redis.ttl[f"handoff_watch:home-{i}"] == 600 + i
        assert redis.ttl[f"handoff_rb_run:home-{i}:ptc-{i}"] == 500 + i
        assert redis.ttl[f"ptc_origin:ptc-{i}"] == 900 + i
        record = json.loads(redis.data[f"ptc_origin:ptc-{i}"])
        assert (record["origin"], record["home_thread_id"]) == ("home", "home-1")


@pytest.mark.asyncio
async def test_a_record_rewritten_since_it_was_read_is_never_overwritten():
    """A rival, or a worker already serving, wrote the record between this
    worker's read and its write: the compare-and-set misses, and the reread
    finds nothing left to rename."""
    redis = _Redis()
    redis.put("ptc_origin:ptc-a", _old_origin(), ttl=900)
    theirs = json.dumps({"origin": "home", "home_thread_id": "home-9"}).encode()
    real_eval = redis.eval

    async def _rival_first(script, numkeys, key, expected, new):
        redis.data[key] = theirs
        redis.eval = real_eval
        return await real_eval(script, numkeys, key, expected, new)

    redis.eval = _rival_first

    await _run(redis)

    assert redis.data["ptc_origin:ptc-a"] == theirs


@pytest.mark.asyncio
async def test_without_redis_it_does_nothing():
    redis = _Redis()
    redis.put("flash_watch:home-1", {"ptc-a"})
    with patch(CACHE, return_value=SimpleNamespace(enabled=False, client=redis)):
        await retired_names.rename_retired_names()

    assert "flash_watch:home-1" in redis.data


class _Flaky(_Redis):
    def __init__(self, failures):
        super().__init__()
        self.failures = failures

    async def scan_iter(self, match=None, count=None):
        if self.failures:
            self.failures -= 1
            raise ConnectionError("redis went away")
        async for key in super().scan_iter(match=match, count=count):
            yield key


@pytest.mark.asyncio
async def test_a_redis_error_is_retried_until_the_rename_finishes():
    redis = _Flaky(failures=2)
    redis.put("flash_watch:home-1", {"ptc-a"})

    with patch.object(retired_names.asyncio, "sleep", AsyncMock()):
        await _run(redis)

    assert keys.watch_key("home-1") in redis.data


@pytest.mark.asyncio
async def test_a_rename_that_cannot_finish_stops_the_worker_from_starting():
    redis = _Flaky(failures=retired_names._ATTEMPTS)
    redis.put("flash_watch:home-1", {"ptc-a"})

    with patch.object(retired_names.asyncio, "sleep", AsyncMock()), pytest.raises(ConnectionError):
        await _run(redis)

    assert "flash_watch:home-1" in redis.data
