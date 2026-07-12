"""Shared stateful fake Redis for report-back tests.

Models the SET / LIST / KV / pipeline / EVAL ops the report-back path uses so
the consumer's keep-until-terminal queue draining is exercised for real.
"""

from __future__ import annotations

import asyncio
import json

from src.server.handlers.chat import report_back


class FakePipeline:
    """Queues client ops and replays them against the fake client on execute.

    Mirrors redis-py's async pipeline shape: command methods are synchronous
    (queue + return self), ``execute`` is awaited and runs them in order.
    """

    def __init__(self, client: "FakeClient") -> None:
        self._client = client
        self._ops: list = []

    def __getattr__(self, name):
        def _queue(*args) -> "FakePipeline":
            self._ops.append((name, *args))
            return self

        return _queue

    async def execute(self) -> list:
        results = []
        for name, *args in self._ops:
            results.append(await getattr(self._client, name)(*args))
        self._ops.clear()
        return results


class FakeClient:
    def __init__(self) -> None:
        self.sets: dict[str, set] = {}
        self.lists: dict[str, list] = {}
        # Shared with FakeCache.kv so raw-client DELETE (pipeline) and the
        # wrapper's get/set/delete address one keyspace, as real Redis does.
        self.kv: dict[str, object] = {}
        self.published: list[tuple[str, str]] = []
        # Last TTL set per key, so tests can assert EXPIRE was issued.
        self.ttls: dict[str, int] = {}

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

    async def lrem(self, key, count, value) -> int:
        lst = self.lists.get(key, [])
        if count == 0:
            kept = [x for x in lst if x != value]
            self.lists[key] = kept
            return len(lst) - len(kept)
        removed = 0
        out = []
        for x in lst:
            if x == value and removed < count:
                removed += 1
                continue
            out.append(x)
        self.lists[key] = out
        return removed

    async def llen(self, key) -> int:
        return len(self.lists.get(key, []))

    async def lpush(self, key, value) -> None:
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, stop) -> None:
        lst = self.lists.get(key, [])
        self.lists[key] = lst[start : None if stop == -1 else stop + 1]

    async def lrange(self, key, start, stop) -> list:
        lst = self.lists.get(key, [])
        return list(lst[start : None if stop == -1 else stop + 1])

    async def mget(self, keys) -> list:
        # The raw client hands back serialized values; the wrapper's kv holds
        # parsed ones, so re-serialize dicts the way prod Redis would.
        return [
            json.dumps(v) if isinstance(v, dict) else v
            for v in (self.kv.get(k) for k in keys)
        ]

    async def expire(self, key, ttl) -> None:
        self.ttls[key] = ttl

    async def delete(self, key) -> None:
        self.sets.pop(key, None)
        self.lists.pop(key, None)
        self.kv.pop(key, None)

    async def publish(self, channel, message) -> None:
        self.published.append((channel, message))

    async def eval(self, script, numkeys, *keys_and_args):
        """Emulate the report-back enqueue EVAL (_ENQUEUE_REPORT_BACK_LUA):
        membership gate -> dedup SADD -> RPUSH. Returns 1 when newly enqueued."""
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

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        return FakePipeline(self)


class FakeCache:
    def __init__(self) -> None:
        self.enabled = True
        self.client = FakeClient()
        # One keyspace: the wrapper's string KV is the client's kv dict.
        self.kv: dict[str, object] = self.client.kv

    async def get(self, key):
        return self.kv.get(key)

    async def get_strict(self, key):
        # Fake transport never fails, so strict == plain get here.
        return self.kv.get(key)

    async def set(self, key, value, ttl=None) -> bool:
        # Mirror RedisCache.set's True-on-success: reserve()'s fail-closed
        # origin write treats a falsy return as a dispatch failure.
        self.kv[key] = value
        return True

    async def delete(self, key) -> None:
        self.kv.pop(key, None)


def origin(ptc: str, flash: str = "flash-1", user: str = "u-1") -> dict:
    return {
        "origin": "flash",
        "report_back": True,
        "flash_thread_id": flash,
        "flash_workspace_id": "fws-1",
        "ptc_thread_id": ptc,
        "ptc_workspace_id": f"ws-{ptc}",
        "user_id": user,
    }


def seed_dispatched(cache: FakeCache, flash: str, ptcs: list[str], user: str = "u-1") -> None:
    """Mirror what reservation + origin recording leave behind for each dispatch."""
    for ptc in ptcs:
        cache.client.sets.setdefault(report_back.flash_watch_key(flash), set()).add(ptc)
        cache.client.sets.setdefault(report_back.flash_user_pending_key(user), set()).add(ptc)
        cache.kv[report_back.ptc_origin_key(ptc)] = origin(ptc, flash, user)


async def drain(flash: str, *, ticks: int = 200) -> None:
    """Yield to the event loop until the flash consumer task finishes."""
    for _ in range(ticks):
        await asyncio.sleep(0)
        task = report_back._rb_consumers.get(flash)
        if task is not None and task.done():
            return
    task = report_back._rb_consumers.get(flash)
    if task is not None:
        await asyncio.wait_for(task, timeout=2.0)
