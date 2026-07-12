"""The report-back non-overlap invariant: mark_completed(N) happens-before
mark_active(N+1).

A flash thread's concurrent PTC completions must render back as ordered,
non-overlapping turns. The load-bearing guarantee is that the single in-process
consumer cannot start report-back N+1 while N is still a live watch member — it
POSTs N (mark_active N), then parks in the membership-first terminal wait until
``clear_flash_report_back`` removes N's membership (mark_completed N), and only
then advances to POST N+1.

This proves that invariant directly at the consumer level: with N's terminal
withheld until the test explicitly triggers it, N+1's POST must NOT fire before
N's terminal clear, and MUST fire after. Fake-cache patterns mirror
``test_flash_report_back.py``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from src.server.handlers.chat import report_back


# ---------------------------------------------------------------------------
# Stateful fake Redis — SET / LIST / KV ops the consumer path actually uses.
# ---------------------------------------------------------------------------


class _FakePipeline:
    """Queues client ops and replays them against the same fake client on execute."""

    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self._ops: list = []

    def delete(self, key) -> "_FakePipeline":
        self._ops.append(("delete", key))
        return self

    def srem(self, key, member) -> "_FakePipeline":
        self._ops.append(("srem", key, member))
        return self

    def sadd(self, key, member) -> "_FakePipeline":
        self._ops.append(("sadd", key, member))
        return self

    def lrem(self, key, count, value) -> "_FakePipeline":
        self._ops.append(("lrem", key, count, value))
        return self

    def lpush(self, key, value) -> "_FakePipeline":
        self._ops.append(("lpush", key, value))
        return self

    def ltrim(self, key, start, stop) -> "_FakePipeline":
        self._ops.append(("ltrim", key, start, stop))
        return self

    def expire(self, key, ttl) -> "_FakePipeline":
        self._ops.append(("expire", key, ttl))
        return self

    async def execute(self) -> list:
        results = []
        for name, *args in self._ops:
            results.append(await getattr(self._client, name)(*args))
        self._ops.clear()
        return results


class _FakeClient:
    def __init__(self) -> None:
        self.sets: dict[str, set] = {}
        self.lists: dict[str, list] = {}
        self.kv: dict[str, object] = {}
        self.published: list[tuple[str, str]] = []

    async def sismember(self, key, member) -> bool:
        return member in self.sets.get(key, set())

    async def sadd(self, key, member) -> int:
        s = self.sets.setdefault(key, set())
        if member in s:
            return 0
        s.add(member)
        return 1

    async def srem(self, key, member) -> None:
        self.sets.get(key, set()).discard(member)

    async def scard(self, key) -> int:
        return len(self.sets.get(key, set()))

    async def smembers(self, key) -> set:
        return set(self.sets.get(key, set()))

    async def rpush(self, key, value) -> None:
        self.lists.setdefault(key, []).append(value)

    async def lindex(self, key, index):
        lst = self.lists.get(key, [])
        return lst[index] if -len(lst) <= index < len(lst) else None

    async def lrem(self, key, count, value) -> None:
        lst = self.lists.get(key, [])
        if count == 0:
            self.lists[key] = [x for x in lst if x != value]
            return
        removed = 0
        out = []
        for x in lst:
            if x == value and removed < count:
                removed += 1
                continue
            out.append(x)
        self.lists[key] = out

    async def llen(self, key) -> int:
        return len(self.lists.get(key, []))

    async def lpush(self, key, value) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, stop) -> None:
        lst = self.lists.get(key, [])
        self.lists[key] = lst[start : stop + 1]

    async def lrange(self, key, start, stop) -> list:
        return list(self.lists.get(key, [])[start : stop + 1])

    async def expire(self, key, ttl) -> None:
        pass

    async def delete(self, key) -> None:
        self.sets.pop(key, None)
        self.lists.pop(key, None)
        self.kv.pop(key, None)

    async def publish(self, channel, message) -> None:
        self.published.append((channel, message))

    async def eval(self, script, numkeys, *keys_and_args):
        """Emulate the report-back enqueue EVAL (_ENQUEUE_REPORT_BACK_LUA)."""
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        watch_key, queued_key, queue_key = keys
        member = args[0]
        if member not in self.sets.get(watch_key, set()):
            return 0
        if await self.sadd(queued_key, member) == 0:
            return 0
        await self.rpush(queue_key, member)
        return 1

    def pipeline(self, transaction: bool = True) -> _FakePipeline:
        return _FakePipeline(self)


class _FakeCache:
    def __init__(self) -> None:
        self.enabled = True
        self.client = _FakeClient()
        self.kv: dict[str, object] = self.client.kv

    async def get(self, key):
        return self.kv.get(key)

    async def get_strict(self, key):
        # Fake transport never fails, so strict == plain get here.
        return self.kv.get(key)

    async def set(self, key, value, ttl=None) -> None:
        self.kv[key] = value

    async def delete(self, key) -> None:
        self.kv.pop(key, None)


def _origin(ptc: str, flash: str = "flash-1", user: str = "u-1") -> dict:
    return {
        "origin": "flash",
        "report_back": True,
        "flash_thread_id": flash,
        "flash_workspace_id": "fws-1",
        "ptc_thread_id": ptc,
        "ptc_workspace_id": f"ws-{ptc}",
        "user_id": user,
    }


def _seed_dispatched(cache: _FakeCache, flash: str, ptcs: list[str], user: str = "u-1") -> None:
    """Mirror what reservation + origin recording leave behind for each dispatch."""
    for ptc in ptcs:
        cache.client.sets.setdefault(report_back.flash_watch_key(flash), set()).add(ptc)
        cache.client.sets.setdefault(f"flash_user_pending:{user}", set()).add(ptc)
        cache.kv[f"ptc_origin:{ptc}"] = _origin(ptc, flash, user)


async def _drain(flash: str, *, ticks: int = 200) -> None:
    """Yield to the event loop until the flash consumer task finishes."""
    for _ in range(ticks):
        await asyncio.sleep(0)
        task = report_back._rb_consumers.get(flash)
        if task is not None and task.done():
            return
    task = report_back._rb_consumers.get(flash)
    if task is not None:
        await asyncio.wait_for(task, timeout=2.0)


@pytest.fixture(autouse=True)
def _reset_consumer_state():
    """Module-global consumer registries must not leak across tests."""
    report_back._rb_consumers.clear()
    report_back._rb_terminal_events.clear()
    yield
    for task in list(report_back._rb_consumers.values()):
        task.cancel()
    report_back._rb_consumers.clear()
    report_back._rb_terminal_events.clear()


# ---------------------------------------------------------------------------
# The invariant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_report_back_n_plus_1_never_starts_before_n_terminal():
    """mark_completed(N) happens-before mark_active(N+1).

    Two report-backs are queued in completion order. The consumer POSTs N
    (mark_active N) but N's terminal is withheld, so it parks in the membership
    wait. The invariant: N+1 is NOT POSTed while N is still a live watch member,
    and IS POSTed only after N's terminal clear (mark_completed N) is triggered.
    """
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-2"])
    posts: list[str] = []

    async def fake_post(c, f, ptc, origin):
        posts.append(ptc)
        # Return dispatched but fire NO terminal — the consumer must block on N
        # until the test explicitly clears it.
        return "dispatched", f"run-{ptc}"

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch.object(report_back, "_post_report_back", side_effect=fake_post):
        # Enqueue both completions in order and let the consumer POST N (ptc-1).
        await report_back._flash_report_back("ptc-1")
        await report_back._flash_report_back("ptc-2")
        for _ in range(30):
            await asyncio.sleep(0)

        # INVARIANT (before): N is a live member and N+1 has NOT started.
        assert posts == ["ptc-1"]
        assert "ptc-1" in cache.client.sets[report_back.flash_watch_key(flash)]
        assert "ptc-2" not in posts

        # mark_completed(N): the terminal for ptc-1 fires ONLY now.
        await report_back.clear_flash_report_back(cache, "ptc-1", flash)
        for _ in range(30):
            await asyncio.sleep(0)

        # INVARIANT (after): N is gone, so N+1 was allowed to start — never before.
        assert "ptc-1" not in cache.client.sets.get(report_back.flash_watch_key(flash), set())
        assert posts == ["ptc-1", "ptc-2"]

        # Drain the second so the consumer terminates cleanly.
        await report_back.clear_flash_report_back(cache, "ptc-2", flash)
        await _drain(flash)

    assert not cache.client.lists.get(report_back.flash_rb_queue_key(flash))
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
