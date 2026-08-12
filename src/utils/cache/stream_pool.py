"""Dedicated Redis pool for blocking stream reads.

Every SSE consumer parks in ``XREAD BLOCK`` and re-issues immediately, so it
holds a connection for essentially the whole life of the stream. Sharing the
cache pool with them means an ordinary hour of open tabs starves every short
cache op in the process — which is exactly how the shared pool was exhausted.
Split out, a reader that cannot get a slot stalls its own stream and nothing
else.

The pool BLOCKS on acquire rather than erroring: for a reader, waiting a moment
is always better than dropping the stream, and the wait is bounded well inside
the caller's own XREAD deadline.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import redis.asyncio as redis
from redis.asyncio.connection import AbstractConnection, BlockingConnectionPool
from redis.exceptions import ConnectionError as RedisConnectionError

from src.config.settings import (
    get_redis_socket_timeout,
    get_redis_stream_max_connections,
)

logger = logging.getLogger(__name__)

# Acquire and connect budgets, derived rather than configured. A reader BLOCKs
# for ``socket_timeout - 1s`` (4s by default) inside an outer ``wait_for`` of
# ``block + 2s``, so the queue wait, the connect, and one response round trip
# have to share the ~2s that is left.
#
# redis-py bounds the pieces, never the whole, so each budget below has to be
# imposed at the right seam: ``timeout=`` covers the acquire queue only
# (``ensure_connection`` runs outside it), ``socket_connect_timeout`` covers the
# TCP dial only, and the AUTH/CLIENT-SETINFO/SELECT handshake after the dial has
# no enclosing deadline at all — each of its round trips gets a fresh
# ``socket_timeout``, so they sum. ``_bounded_handshake`` supplies that missing
# cap; the caller's ``wait_for`` is a backstop, and reaching it cancels
# mid-handshake into the self-feeding redial storm this pool split prevents.
_ACQUIRE_TIMEOUT_S = 0.5
_CONNECT_TIMEOUT_S = 1.0


def connect_path_budget_s() -> float:
    """Worst case to hand a caller a usable connection, before any command.

    Exported so the readers can size their outer deadline off the real
    budgets instead of a hand-tuned constant: a deadline below this sum
    cancels mid-connect, and that cancel is what starts the redial storm.
    The dial and the handshake are capped separately, both at
    ``_CONNECT_TIMEOUT_S``, so each is counted.
    """
    return _ACQUIRE_TIMEOUT_S + (2 * _CONNECT_TIMEOUT_S)


async def _bounded_handshake(conn: AbstractConnection) -> None:
    """Cap the post-dial handshake that redis-py leaves unbounded.

    Installed as ``redis_connect_func``, which redis-py substitutes for the
    default ``on_connect`` wholesale. It has to fail as a ``RedisError``: that
    is what makes ``connect_check_health`` drop the half-open socket, and it
    keeps the SSE readers on their backoff branch instead of the immediate-retry
    branch they take for a bare ``asyncio.TimeoutError``.
    """
    try:
        async with asyncio.timeout(_CONNECT_TIMEOUT_S):
            await conn.on_connect_check_health(check_health=True)
    except asyncio.TimeoutError as exc:
        raise RedisConnectionError(
            f"Timed out after {_CONNECT_TIMEOUT_S}s during Redis handshake"
        ) from exc


_reader_client: Optional[redis.Redis] = None
_reader_pool: Optional[BlockingConnectionPool] = None
_init_lock = asyncio.Lock()
_retry_after = 0.0
_RETRY_COOLDOWN_S = 30.0


async def get_stream_reader_client(cache) -> Optional[redis.Redis]:
    """The shared reader client, or None when its pool cannot be built.

    Takes the caller's cache client rather than resolving one: every call site
    already holds it, and borrowing it keeps this module free of a back-import
    into ``redis_cache``. Only the URL and the enabled flag are read — the
    connection itself is never shared, which is the entire point.

    Never falls back to the cache client: that fallback is what this module
    exists to remove, and ``from_url`` only parses the URL, so a build failure
    means a URL nothing else could have used either.
    """
    global _reader_client, _reader_pool, _retry_after
    if _reader_client is not None:
        return _reader_client

    if not getattr(cache, "enabled", False):
        return None

    loop = asyncio.get_running_loop()
    if loop.time() < _retry_after:
        return None
    async with _init_lock:
        if _reader_client is not None:
            return _reader_client
        if loop.time() < _retry_after:
            return None
        try:
            _reader_pool = BlockingConnectionPool.from_url(
                cache.url,
                max_connections=get_redis_stream_max_connections(),
                timeout=_ACQUIRE_TIMEOUT_S,
                socket_timeout=get_redis_socket_timeout(),
                socket_connect_timeout=_CONNECT_TIMEOUT_S,
                redis_connect_func=_bounded_handshake,
                decode_responses=False,
                health_check_interval=30,
            )
            _reader_client = redis.Redis(connection_pool=_reader_pool)
            logger.info(
                "Redis stream-reader pool ready (max=%d, acquire_timeout=%.1fs)",
                get_redis_stream_max_connections(),
                _ACQUIRE_TIMEOUT_S,
            )
        except Exception as exc:
            logger.warning(
                "Failed to init Redis stream-reader pool; SSE reads are "
                "unavailable for the next %.0fs: %s",
                _RETRY_COOLDOWN_S,
                exc,
            )
            _retry_after = loop.time() + _RETRY_COOLDOWN_S
            return None
    return _reader_client


def peek_stream_reader_pool() -> Optional[BlockingConnectionPool]:
    """The reader pool if one exists, without building it (metrics callbacks)."""
    return _reader_pool


async def close_stream_reader_pool() -> None:
    """Tear down the reader pool on shutdown. Best-effort."""
    global _reader_client, _reader_pool, _retry_after
    client, _reader_client = _reader_client, None
    pool, _reader_pool = _reader_pool, None
    _retry_after = 0.0
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
