"""Cross-worker pub/sub: the shared subscribe primitive, plus workspace status.

Replaces the loser-side DB poll loop with a push notification so a
stopped→running transition wakes waiting workers in milliseconds rather
than 0.5–2 s polling cycles. Also feeds the ``/computers/{id}/events`` and
``/workspaces/{id}/events`` SSE channels so the frontend can drop
interval-polling.

``subscribe_to_channel`` owns the one long-lived subscription contract (the
dedicated pool, the tri-state wait, cancellation-safe teardown); the per-domain
``subscribe_to_*`` functions — here and in ``thread_lifecycle_feed`` — are
channel wrappers over it. Degrades silently when Redis is unavailable: the
subscribe yields ``None`` and ``publish_status_change`` is a no-op, so callers
must keep their DB-poll path as a safety net.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable, Optional, Tuple

import anyio
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

from src.config.settings import (
    get_redis_pubsub_max_connections,
    get_redis_socket_connect_timeout,
)
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
# Monotonic deadline before which a failed pool build is not re-attempted, so a
# broken URL doesn't cost a rebuild on every subscribe.
_pubsub_retry_after = 0.0
_POOL_RETRY_COOLDOWN_S = 30.0


async def _get_pubsub_client(cache: RedisCacheClient) -> Optional[redis.Redis]:
    """Return the dedicated pubsub client, or None when its pool can't be built.

    Deliberately does NOT fall back to the shared cache client. ``from_url``
    only parses the URL — it never connects — so the only way construction
    fails is a URL the shared client could not have used either. Falling back
    would silently move every long-lived subscription (600s ``/events`` streams
    and every cross-worker start wait) onto the pool this isolation exists to
    protect, for the life of the process and with no signal that it happened.
    """
    global _pubsub_client, _pubsub_pool, _pubsub_retry_after
    if _pubsub_client is not None:
        return _pubsub_client
    loop = asyncio.get_running_loop()
    if loop.time() < _pubsub_retry_after:
        return None
    async with _pubsub_init_lock:
        if _pubsub_client is not None:
            return _pubsub_client
        if loop.time() < _pubsub_retry_after:
            return None
        try:
            # socket_connect_timeout only: connect is bounded by nature, and
            # redis-py wraps both the AUTH handshake read and pool disconnect in
            # it — unset, a Redis that accepts TCP but never answers parks a
            # subscriber on the OS SYN timeout (~75-130s). socket_timeout stays
            # unset because subscribers park on blocking reads; every reader
            # here passes its own explicit get_message(timeout=...).
            _pubsub_pool = ConnectionPool.from_url(
                cache.url,
                max_connections=get_redis_pubsub_max_connections(),
                socket_connect_timeout=get_redis_socket_connect_timeout(),
                decode_responses=False,
                health_check_interval=30,
            )
            _pubsub_client = redis.Redis(connection_pool=_pubsub_pool)
        except Exception as exc:
            logger.warning(
                "Failed to init dedicated pubsub pool; status pub/sub is "
                "unavailable for the next %.0fs (callers fall back to polling): %s",
                _POOL_RETRY_COOLDOWN_S,
                exc,
            )
            _pubsub_retry_after = loop.time() + _POOL_RETRY_COOLDOWN_S
            return None
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


def peek_status_pubsub_pool() -> Optional[ConnectionPool]:
    """The pubsub pool if one exists, without building it (metrics callbacks)."""
    return _pubsub_pool


async def close_status_pubsub_pool() -> None:
    """Tear down the dedicated pubsub pool on shutdown. Best-effort."""
    global _pubsub_client, _pubsub_pool, _pubsub_retry_after
    client, _pubsub_client = _pubsub_client, None
    pool, _pubsub_pool = _pubsub_pool, None
    # Clear the cooldown so a restart can re-attempt the dedicated pool.
    _pubsub_retry_after = 0.0
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
def status_channel(computer_id: str) -> str:
    """The channel a machine's status transitions are published on.

    Keyed by computer, not workspace: the sandbox belongs to the machine, so
    several projects can be waiting on the same transition and one publish has
    to reach all of them. Every subscriber resolves workspace to computer first.
    """
    return f"computer:status:{computer_id}"


def workspace_status_channel(workspace_id: str) -> str:
    """The fallback channel for a workspace that has no machine of its own.

    A flash workspace never gets a computer, and a workspace the 046 backfill
    skipped has not been given one yet; both still have an /events subscriber
    and a start waiter, so they keep a channel rather than losing the publish.
    """
    return f"ws:status:{workspace_id}"


def _channel_for(workspace_id: str, computer_id: Optional[str]) -> str:
    """Computer-keyed when the workspace has a machine, workspace-keyed otherwise."""
    return (
        status_channel(computer_id)
        if computer_id
        else workspace_status_channel(workspace_id)
    )


# ('message', payload) | ('timeout', None) | ('error', None)
ChannelWaitFn = Callable[
    [Optional[float]], Awaitable[Tuple[str, Optional[dict]]]
]

# Historical alias — subscribe_to_status yields the same tri-state wait fn.
WaitFn = ChannelWaitFn


async def _publish(channel: str, payload: dict, extra: Optional[dict]) -> None:
    """Best-effort publish. Never raises. See ``publish_status_change``."""
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return
    if extra:
        payload.update(extra)
    try:
        await cache.client.publish(channel, json.dumps(payload))
    except Exception as exc:
        logger.debug("Failed to publish status change on %s: %s", channel, exc)


async def publish_status_change(
    workspace_id: str,
    status: str,
    *,
    computer_id: Optional[str] = None,
    extra: Optional[dict] = None,
) -> None:
    """Best-effort cross-worker notification of a status transition.

    Never raises — failures are debug-logged and swallowed so callers
    can wire this into critical paths (DB writes) without risking the
    main mutation.

    Pass ``computer_id`` whenever the writer's row carries one: it selects the
    machine's channel, and the payload names both ids so a consumer can tell
    which project moved and which machine it moved on.
    """
    computer_id = str(computer_id) if computer_id else None
    payload: dict = {
        "workspace_id": workspace_id,
        "computer_id": computer_id,
        "status": status,
    }
    if computer_id is None:
        del payload["computer_id"]
    await _publish(_channel_for(workspace_id, computer_id), payload, extra)


async def publish_computer_status_change(
    computer_id: str,
    status: str,
    *,
    extra: Optional[dict] = None,
) -> None:
    """Wake every subscriber of one machine, with no project in the payload.

    The computer-entry counterpart of ``publish_status_change``: the writer
    moved the machine, so naming one of its projects would be arbitrary.
    """
    await _publish(
        status_channel(str(computer_id)),
        {"computer_id": str(computer_id), "status": status},
        extra,
    )


@asynccontextmanager
async def subscribe_to_channel(
    channel: str,
) -> AsyncIterator[Optional[ChannelWaitFn]]:
    """Subscribe to *channel* on the dedicated pub/sub pool.

    Yields ``None`` when Redis is disabled or the subscribe fails, so callers
    fall back to their DB path. Otherwise yields ``wait(timeout)`` returning a
    tri-state: ``('message', payload)``, ``('timeout', None)`` (quiet
    interval — keepalive/reconcile tick), or ``('error', None)`` (broken
    connection). ⚠️ ``('error', None)`` returns IMMEDIATELY, so a caller that
    treats it as a timeout busy-spins: abandon the subscription and pace the
    fallback. Subscribers MUST re-read the authoritative state after
    subscribing — the channel may have published before SUBSCRIBE completed.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        yield None
        return

    client = await _get_pubsub_client(cache)
    if client is None:
        # No isolated pool to subscribe on, and the shared one is off limits
        # for a 600s hold. Callers keep their DB-poll path for exactly this.
        yield None
        return

    pubsub = client.pubsub()
    try:
        await pubsub.subscribe(channel)
    except Exception as exc:
        # warning, not debug: this is where pool exhaustion surfaces, and it
        # silently downgrades five subsystems to their DB-poll fallbacks.
        logger.warning("Failed to subscribe to %s: %s", channel, exc)
        try:
            await pubsub.aclose()
        except Exception:
            pass
        yield None
        return

    async def _wait(
        timeout: Optional[float] = None,
    ) -> Tuple[str, Optional[dict]]:
        try:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=timeout,
            )
        except Exception as exc:
            logger.debug("Pubsub get_message error on %s: %s", channel, exc)
            return ("error", None)
        if not msg or msg.get("type") != "message":
            return ("timeout", None)
        data = msg.get("data")
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                return ("timeout", None)
        try:
            payload = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return ("timeout", None)
        if not isinstance(payload, dict):
            return ("timeout", None)
        return ("message", payload)

    try:
        yield _wait
    finally:
        # Shielded teardown: this generator dies by CANCELLATION (client
        # disconnect aborts the SSE task), and under an already-tripped anyio
        # cancel scope every bare await re-raises CancelledError — which
        # `except Exception` does NOT catch. aclose() is the only path that
        # returns the connection to the pubsub pool, so an unshielded close
        # leaks one pool slot per disconnect until MaxConnectionsError kills
        # the pool for the process lifetime.
        with anyio.CancelScope(shield=True):
            try:
                await pubsub.unsubscribe(channel)
            except Exception:
                pass
            try:
                await pubsub.aclose()
            except Exception:
                pass


@asynccontextmanager
async def subscribe_to_status(
    workspace_id: str,
    *,
    computer_id: Optional[str] = None,
) -> AsyncIterator[Optional[ChannelWaitFn]]:
    """Subscribe to the channel a workspace's transitions land on.

    ``computer_id`` must be resolved from the workspace row before subscribing,
    the same way every publisher reads it, or the two pick different channels.
    """
    async with subscribe_to_channel(
        _channel_for(workspace_id, str(computer_id) if computer_id else None)
    ) as wait:
        yield wait


@asynccontextmanager
async def subscribe_to_computer_status(
    computer_id: str,
) -> AsyncIterator[Optional[ChannelWaitFn]]:
    """Subscribe to a machine's status channel (see subscribe_to_channel)."""
    async with subscribe_to_channel(status_channel(str(computer_id))) as wait:
        yield wait


async def wait_for_status_change(
    workspace_id: str,
    *,
    timeout: float,
    computer_id: Optional[str] = None,
) -> Optional[dict]:
    """Subscribe once and wait for a single status-change payload.

    Returns the payload, or ``None`` if Redis is disabled, the subscription
    breaks, or the timeout elapses without a message. Convenience wrapper
    used by callers that don't need a long-lived subscription.
    """
    async with subscribe_to_status(workspace_id, computer_id=computer_id) as wait:
        if wait is None:
            return None
        _kind, payload = await wait(timeout)
        return payload
