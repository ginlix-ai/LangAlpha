"""Cross-worker concurrency admission over a Redis ZSET of holder ids.

One member per in-flight holder, scored by admission time. Release is ZREM —
idempotent, so a retried release can never free a slot it does not hold (the
pathology of an INCR/DECR counter). A holder that dies without releasing is
reaped by score on the next admission, so a leak self-heals after
``stale_after`` even on a key a busy caller keeps alive.

Callers own their Redis handle and their fail-open policy; this module only
issues the commands.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import uuid4


@dataclass(frozen=True)
class SlotAdmission:
    """Outcome of one admission. ``current`` is the holder count after it."""

    allowed: bool
    current: int
    limit: int
    slot_id: str | None = None


async def acquire_slot_member(
    redis, key: str, *, limit: int, stale_after: int
) -> SlotAdmission:
    """Take a slot in ``key``, rolling our own member back when over ``limit``.

    ``stale_after`` is both the reap horizon and the key TTL, so it must exceed
    the longest legitimate hold — a shorter window reaps live holders and hands
    out slots past the limit.
    """
    slot_id = str(uuid4())
    now = time.time()
    pipe = redis.pipeline()
    pipe.zremrangebyscore(key, "-inf", now - stale_after)
    pipe.zadd(key, {slot_id: now})
    pipe.zcard(key)
    pipe.expire(key, stale_after)
    current = int((await pipe.execute())[2])

    if current > limit:
        await redis.zrem(key, slot_id)
        return SlotAdmission(allowed=False, current=current - 1, limit=limit)
    return SlotAdmission(
        allowed=True, current=current, limit=limit, slot_id=slot_id
    )


async def release_slot_member(redis, key: str, slot_id: str) -> None:
    await redis.zrem(key, slot_id)
