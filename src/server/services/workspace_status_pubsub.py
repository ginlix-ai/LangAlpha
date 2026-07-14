"""Cross-worker pub/sub for workspace lifecycle status changes.

Replaces the loser-side DB poll loop with a push notification so a
stopped→running transition wakes waiting workers in milliseconds rather
than 0.5–2 s polling cycles. Also feeds the ``/workspaces/{id}/events``
SSE channel so the frontend can drop interval-polling.

Degrades silently when Redis is unavailable: ``subscribe_to_status``
yields ``None`` and ``publish_status_change`` is a no-op, so callers
must keep their DB-poll path as a safety net.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable, Optional

import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

from src.config.settings import get_redis_max_connections
from src.utils.cache.redis_cache import RedisCacheClient, get_cache_client

logger = logging.getLogger(__name__)

# Dedicated connection pool for long-lived status subscriptions. Each /events
# SSE stream (up to 600s) and each cross-worker start wait (up to 300s) holds a
# connection for the subscription's lifetime. Isolating them in their own pool
# keeps a burst of warming workspaces from exhausting the shared cache pool and
# degrading unrelated cache/SSE-buffer ops. Publishes stay on the shared cache
# client (they're sub-millisecond and don't hold a connection).
_pubsub_client: Optional[redis.Redis] = None
_pubsub_pool: Optional[ConnectionPool] = None
_pubsub_init_lock = asyncio.Lock()
# Sticky flag set when dedicated-pool construction fails (deterministic — a bad
# URL fails every time). Lets later calls return the shared client via the
# lock-free fast path instead of re-acquiring the lock and retrying a build that
# can't succeed. NOT stored in _pubsub_client: that would make
# close_status_pubsub_pool aclose() the shared cache client. Reset on shutdown.
_pubsub_fallback = False

# Backoff after a broken-connection get_message so error paths don't busy-spin.
_BROKEN_CONN_BACKOFF_S = 1.0


async def _get_pubsub_client(cache: RedisCacheClient) -> redis.Redis:
    """Return the dedicated pubsub client, lazily built from the cache's URL.

    Falls back to the shared cache client if the dedicated pool can't be
    created — correctness is unaffected, only the pool isolation is lost.
    """
    global _pubsub_client, _pubsub_pool, _pubsub_fallback
    if _pubsub_client is not None:
        return _pubsub_client
    if _pubsub_fallback:
        return cache.client
    async with _pubsub_init_lock:
        if _pubsub_client is not None:
            return _pubsub_client
        if _pubsub_fallback:
            return cache.client
        try:
            _pubsub_pool = ConnectionPool.from_url(
                cache.url,
                max_connections=get_redis_max_connections(),
                decode_responses=False,
                health_check_interval=30,
            )
            _pubsub_client = redis.Redis(connection_pool=_pubsub_pool)
        except Exception as exc:
            logger.warning(
                "Failed to init dedicated pubsub pool, using shared cache pool: %s",
                exc,
            )
            _pubsub_fallback = True
            return cache.client
    return _pubsub_client


async def get_shared_pubsub_client() -> Optional[redis.Redis]:
    """Public accessor for the dedicated pubsub client so other long-lived
    subscribers (e.g. the turn-cancel nudge listener) share the isolated
    pool instead of holding connections from the general cache pool.
    Returns None when Redis is disabled.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None
    return await _get_pubsub_client(cache)


async def close_status_pubsub_pool() -> None:
    """Tear down the dedicated pubsub pool on shutdown. Best-effort."""
    global _pubsub_client, _pubsub_pool, _pubsub_fallback
    client, _pubsub_client = _pubsub_client, None
    pool, _pubsub_pool = _pubsub_pool, None
    # Clear the sticky fallback so a restart can re-attempt the dedicated pool.
    _pubsub_fallback = False
    if client is not None:
        try:
            await client.aclose()
        except Exception:
            pass
    if pool is not None:
        try:
            await pool.disconnect()
        except Exception:
            pass


# Single source of truth for the channel name format.
def status_channel(workspace_id: str) -> str:
    return f"ws:status:{workspace_id}"


# Type of the `wait()` coroutine yielded by subscribe_to_status.
WaitFn = Callable[[Optional[float]], Awaitable[Optional[dict]]]


async def publish_status_change(
    workspace_id: str,
    status: str,
    *,
    extra: Optional[dict] = None,
) -> None:
    """Best-effort cross-worker notification of a status transition.

    Never raises — failures are debug-logged and swallowed so callers
    can wire this into critical paths (DB writes) without risking the
    main mutation.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return
    payload: dict = {"workspace_id": workspace_id, "status": status}
    if extra:
        payload.update(extra)
    try:
        await cache.client.publish(status_channel(workspace_id), json.dumps(payload))
    except Exception as exc:
        logger.debug(
            "Failed to publish status change for %s: %s", workspace_id, exc
        )


@asynccontextmanager
async def subscribe_to_status(
    workspace_id: str,
) -> AsyncIterator[Optional[WaitFn]]:
    """Subscribe to a workspace's status channel and yield a ``wait()`` coroutine.

    Yields ``None`` when Redis is disabled so callers fall back to DB
    polling. When the channel is live, yields an async ``wait(timeout)``
    that returns the next decoded payload dict (or ``None`` on timeout /
    decode error). Subscribers MUST re-read the authoritative DB state
    after subscribing — the channel may have published before our
    SUBSCRIBE completed.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        yield None
        return

    client = await _get_pubsub_client(cache)
    pubsub = client.pubsub()
    try:
        await pubsub.subscribe(status_channel(workspace_id))
    except Exception as exc:
        logger.debug(
            "Failed to subscribe to status channel for %s: %s",
            workspace_id,
            exc,
        )
        try:
            await pubsub.aclose()
        except Exception:
            pass
        yield None
        return

    async def _wait(timeout: Optional[float] = None) -> Optional[dict]:
        try:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=timeout,
            )
        except Exception as exc:
            logger.debug("Pubsub get_message error for %s: %s", workspace_id, exc)
            # On a broken connection get_message raises immediately instead of
            # blocking for `timeout`; pace the error path so looping callers
            # (the start-wait loop, the /events SSE handler) don't busy-spin
            # DB reads until their deadline. Cap at the requested timeout so we
            # never sleep longer than the caller asked to wait.
            backoff = _BROKEN_CONN_BACKOFF_S if timeout is None else min(timeout, _BROKEN_CONN_BACKOFF_S)
            await asyncio.sleep(backoff)
            return None
        if not msg or msg.get("type") != "message":
            return None
        data = msg.get("data")
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                return None
        try:
            payload = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return None
        return payload if isinstance(payload, dict) else None

    try:
        yield _wait
    finally:
        # Cancellation-safe teardown — unsubscribe then close, swallowing
        # everything so a cancelled SSE generator doesn't leak warnings.
        try:
            await pubsub.unsubscribe(status_channel(workspace_id))
        except Exception:
            pass
        try:
            await pubsub.aclose()
        except Exception:
            pass


async def wait_for_status_change(
    workspace_id: str,
    *,
    timeout: float,
) -> Optional[dict]:
    """Subscribe once and wait for a single status-change payload.

    Returns the payload, or ``None`` if Redis is disabled or the
    timeout elapses without a message. Convenience wrapper used by
    callers that don't need a long-lived subscription.
    """
    async with subscribe_to_status(workspace_id) as wait:
        if wait is None:
            return None
        return await wait(timeout)
