"""The cross-process lease behind sandbox path write locks.

A request, its subagents and a sibling thread's turn may run on different
uvicorn workers, so the in-process registry in ``path_locks`` covers only a
slice of the writers to one file. This holds a short Redis lease per path for
the rest. A Postgres advisory lock would do the same, but it pins a pooled
connection across every sandbox write round trip, and the pool is the scarcer
resource under fan-out.

Fail-open by design: with Redis unavailable, or after the bounded wait, the
write proceeds under process-local exclusion alone. A lost update is
recoverable from the sandbox mirror; a write that never happens is not.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from ptc_agent.core.sandbox import path_locks

logger = logging.getLogger(__name__)

# Longer than any single sandbox write, so a lease never lapses under its
# holder; short enough that a worker killed mid-write frees the path promptly.
LEASE_TTL_MS = 60_000
# How long a contender waits before proceeding without the lease.
WAIT_TIMEOUT_S = 30.0
POLL_INTERVAL_S = 0.05
STOP_HEARTBEAT_TTL_S = 20
STOP_HEARTBEAT_REFRESH_S = 5

_RENEW_STOP_HEARTBEAT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""
_RELEASE_STOP_HEARTBEAT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


def computer_stop_heartbeat_key(computer_id: str) -> str:
    return f"computer:stop:{computer_id}"


async def computer_stop_heartbeat_present(
    computer_id: str, *, cache: Any = None
) -> bool:
    if cache is None:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
    try:
        return bool(await cache.client.exists(computer_stop_heartbeat_key(computer_id)))
    except Exception:
        logger.warning(
            "Could not read stop heartbeat for computer %s; treating ownership as live",
            computer_id,
            exc_info=True,
        )
        return True


@asynccontextmanager
async def computer_stop_heartbeat(
    computer_id: str, *, cache: Any = None
) -> AsyncIterator[None]:
    if cache is None:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()

    key = computer_stop_heartbeat_key(computer_id)
    token = uuid.uuid4().hex
    acquired = False
    refresh_task: asyncio.Task | None = None
    try:
        acquired = bool(await cache.client.set(key, token, ex=STOP_HEARTBEAT_TTL_S))
        if not acquired:
            logger.warning(
                "Computer %s stop heartbeat is already held; relying on the "
                "Postgres stop fence",
                computer_id,
            )
    except Exception:
        logger.warning(
            "Could not create stop heartbeat for computer %s; relying on the "
            "Postgres stop fence",
            computer_id,
            exc_info=True,
        )

    async def refresh() -> None:
        while True:
            await asyncio.sleep(STOP_HEARTBEAT_REFRESH_S)
            try:
                renewed = await cache.client.eval(
                    _RENEW_STOP_HEARTBEAT,
                    1,
                    key,
                    token,
                    STOP_HEARTBEAT_TTL_S,
                )
                if not renewed:
                    logger.warning(
                        "Lost stop heartbeat ownership for computer %s",
                        computer_id,
                    )
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Could not refresh stop heartbeat for computer %s",
                    computer_id,
                    exc_info=True,
                )

    if acquired:
        refresh_task = asyncio.create_task(refresh())
    try:
        yield
    finally:
        if refresh_task is not None:
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)
        if acquired:

            async def release() -> None:
                try:
                    await cache.client.eval(_RELEASE_STOP_HEARTBEAT, 1, key, token)
                except Exception:
                    logger.warning(
                        "Could not release stop heartbeat for computer %s",
                        computer_id,
                        exc_info=True,
                    )

            await asyncio.shield(release())


def lease_key(scope: str, normalized_path: str) -> str:
    digest = hashlib.sha1(normalized_path.encode("utf-8")).hexdigest()
    return f"pathlock:{scope}:{digest}"


@asynccontextmanager
async def redis_path_lease(
    scope: str,
    normalized_path: str,
    *,
    cache: Any = None,
    ttl_ms: int = LEASE_TTL_MS,
    wait_timeout_s: float = WAIT_TIMEOUT_S,
    poll_interval_s: float = POLL_INTERVAL_S,
) -> AsyncIterator[None]:
    if cache is None:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()

    key = lease_key(scope, normalized_path)
    token = uuid.uuid4().hex
    held = False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_timeout_s
    while True:
        acquired = await cache.acquire_lock(key, token, ttl_ms)
        if acquired is None:
            # Redis is down: process-local exclusion is all there is.
            break
        if acquired:
            held = True
            break
        if loop.time() >= deadline:
            logger.warning(
                f"Path lease {key} still held after {wait_timeout_s:.0f}s; "
                "writing under process-local exclusion only"
            )
            break
        await asyncio.sleep(poll_interval_s)

    try:
        yield
    finally:
        if held:
            # Release must survive the turn being cancelled while it writes;
            # otherwise the path stays locked for the whole TTL.
            await asyncio.shield(cache.release_lock(key, token))


def install() -> None:
    """Wire the Redis lease into ``path_locks`` for this worker."""
    path_locks.install_cross_process_lock(redis_path_lease)
