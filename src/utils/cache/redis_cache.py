"""
Redis cache client.

Provides connection pooling, health checking, and basic cache operations
with TTL support and pattern-based deletion.
"""

import json
import logging
import os
from datetime import datetime, date
from typing import Any, Optional
from contextlib import asynccontextmanager
from uuid import UUID

import redis.asyncio as redis
import redis.exceptions as redis_exceptions
from redis.asyncio.connection import BlockingConnectionPool, ConnectionPool

from src.config.settings import (
    is_redis_cache_enabled,
    get_nested_config,
    get_redis_max_connections,
    get_redis_pool_timeout,
    get_redis_socket_timeout,
    get_redis_socket_connect_timeout,
)

logger = logging.getLogger(__name__)

# set() without a TTL used to write an immortal key (`if ttl:`), turning
# every missed cleanup into a permanent Redis leak. Immortality is now
# opt-in via ttl=PERSIST; an omitted TTL falls back to a generous safety
# net that outlives any legitimate transient state.
PERSIST = -1
SAFETY_TTL = 7 * 24 * 3600


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime and UUID objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


class EventBufferUnavailableError(RuntimeError):
    """Redis event transport is disabled or not connected.

    Distinct from a transient wire failure on purpose: there is nothing to
    retry, so the caller must fail the run immediately instead of burning its
    retry budget.
    """


# How often an active stream re-asserts "no TTL". Every write used to PERSIST,
# which cost one command per event to insure against a rare stale stamp; a
# periodic heal buys the same protection for ~0.2% of the traffic. Private:
# both extras are derived from ``event_id`` below, so no caller needs to know
# the cadence to ask for the right one.
_HEAL_INTERVAL = 512


def is_pool_exhaustion(err: BaseException) -> bool:
    """True when ``err`` means "no pool slot", whatever pool class raised it.

    Three spellings must map to one concept: ``MaxConnectionsError`` (non-blocking
    pool, redis-py ≥5), the bare-parent ``"Too many connections"`` string from
    older/forked variants, and ``ConnectionError("No connection available.")``
    — what ``BlockingConnectionPool`` raises when its acquire timeout expires.
    Missing the third would make the exhaustion detector go silent exactly
    where we are moving the hot pools.
    """
    if isinstance(err, getattr(redis_exceptions, "MaxConnectionsError", ())):
        return True
    text = str(err)
    return "Too many connections" in text or "No connection available" in text


# Compare-and-delete: only release a lock we still own, so a request whose lock
# already expired can't delete the lock a later holder acquired.
_RELEASE_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class RedisCacheClient:
    """
    Async Redis cache client with connection pooling.

    Features:
    - Connection pool management
    - Health checking
    - JSON serialization
    - TTL support
    - Pattern-based deletion
    - Namespace support
    """

    def __init__(
        self,
        url: Optional[str] = None,
        max_connections: int = 50,
        socket_timeout: int | None = None,
        socket_connect_timeout: int | None = None,
        decode_responses: bool = False,  # We handle JSON encoding manually
    ):
        """
        Initialize Redis cache client.

        Args:
            url: Redis connection URL (redis://host:port/db)
            max_connections: Maximum connections in pool
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Socket connect timeout in seconds
            decode_responses: Whether to decode responses to strings
        """
        # Load URL from config or environment variables
        self.url = (
            url
            or get_nested_config('redis.url')
            or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        )
        self.max_connections = max_connections

        self.socket_timeout = socket_timeout if socket_timeout is not None else get_redis_socket_timeout()
        self.socket_connect_timeout = socket_connect_timeout if socket_connect_timeout is not None else get_redis_socket_connect_timeout()

        # Check if caching is enabled (config.yaml and env var)
        cache_enabled_env = os.getenv("REDIS_CACHE_ENABLED", "true").lower() in ["true", "1", "yes"]
        self.enabled = is_redis_cache_enabled() and cache_enabled_env

        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[redis.Redis] = None

        # Cache statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "errors": 0,
        }

        if not self.enabled:
            logger.warning("Redis cache is disabled in configuration")

    async def connect(self) -> None:
        """Initialize Redis connection pool."""
        if not self.enabled:
            logger.info("Redis cache disabled, skipping connection")
            return

        try:
            # Blocking, now that nothing long-lived shares this pool: stream
            # readers and pub/sub subscribers have their own. Every remaining
            # caller is a short op, so a momentary shortage should cost a few
            # ms of queueing — not an instant error that kills a live turn.
            #
            # health_check_interval: redis-py sends PING on idle connections before
            # handing them out, so poisoned connections (dropped by the server or
            # network) get detected and discarded instead of lingering in the pool.
            pool_timeout = get_redis_pool_timeout()
            self.pool = BlockingConnectionPool.from_url(
                self.url,
                max_connections=self.max_connections,
                timeout=pool_timeout,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                decode_responses=False,
                health_check_interval=30,
            )

            self.client = redis.Redis(connection_pool=self.pool)

            # Test connection
            await self.client.ping()
            logger.info(
                f"Redis cache connected: {self.url} "
                f"(pool max={self.max_connections}, acquire_timeout={pool_timeout}s, "
                f"health_check=30s)"
            )

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.enabled = False
            raise

    async def disconnect(self) -> None:
        """Close Redis connection pool."""
        if self.client:
            await self.client.aclose()
            logger.info("Redis cache disconnected")

        if self.pool:
            await self.pool.disconnect()

    def _log_error(self, context: str, err: Exception) -> None:
        """Log a Redis error, with pool stats if the error is pool exhaustion."""
        if is_pool_exhaustion(err):
            logger.error(
                f"{context} — {type(err).__name__}: {err} "
                f"(pool: {self.pool_snapshot()})"
            )
        else:
            logger.error(f"{context}: {err}")

    async def health_check(self) -> bool:
        """
        Check Redis connection health.

        Returns:
            True if healthy, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            await self.client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value (JSON deserialized) or None if not found
        """
        if not self.enabled or not self.client:
            return None

        try:
            value = await self.client.get(key)

            if value is None:
                self.stats["misses"] += 1
                logger.debug(f"Cache MISS: {key}")
                return None

            self.stats["hits"] += 1
            logger.debug(f"Cache HIT: {key}")

            # Deserialize JSON
            return json.loads(value)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize cache value for {key}: {e}")
            self.stats["errors"] += 1
            return None
        except Exception as e:
            self._log_error(f"Cache get error for {key}", e)
            self.stats["errors"] += 1
            return None

    async def get_strict(self, key: str) -> Optional[Any]:
        """Get with fail-loud semantics: None ONLY for a confirmed-absent key.

        For callers whose retry path is an exception (outbox executors) —
        the swallowing ``get`` would turn a Redis blip into "no data" and
        let a required effect ack as a no-op.
        """
        if not self.enabled or not self.client:
            raise RuntimeError("Redis cache disabled — strict read unavailable")
        value = await self.client.get(key)
        if value is None:
            return None
        return json.loads(value)

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set value in cache.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time-to-live in seconds. Omitted → SAFETY_TTL (7 days), so a
                missed cleanup can never leak a key forever; pass PERSIST for
                a deliberately immortal key.

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            # Serialize to JSON with datetime support
            serialized = json.dumps(value, ensure_ascii=False, cls=DateTimeEncoder)

            if ttl == PERSIST:
                await self.client.set(key, serialized)
            else:
                await self.client.setex(key, ttl if ttl and ttl > 0 else SAFETY_TTL, serialized)

            self.stats["sets"] += 1
            logger.debug(f"Cache SET: {key} (TTL: {ttl}s)")
            return True

        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize value for {key}: {e}")
            self.stats["errors"] += 1
            return False
        except Exception as e:
            self._log_error(f"Cache set error for {key}", e)
            self.stats["errors"] += 1
            return False

    async def set_many(self, items: list[tuple[str, Any, Optional[int]]]) -> bool:
        """Write many ``(key, value, ttl)`` triples over ONE pooled connection.

        The gather-of-``set()`` alternative takes a connection per key, so a
        single wide fill can exhaust the pool by itself — which is how a
        250-symbol quote fill DoS'd a 150-slot pool from one request.
        """
        if not items:
            return True
        if not self.enabled or not self.client:
            return False

        try:
            async with self.client.pipeline(transaction=False) as pipe:
                for key, value, ttl in items:
                    serialized = json.dumps(
                        value, ensure_ascii=False, cls=DateTimeEncoder
                    )
                    if ttl == PERSIST:
                        pipe.set(key, serialized)
                    else:
                        pipe.setex(
                            key, ttl if ttl and ttl > 0 else SAFETY_TTL, serialized
                        )
                await pipe.execute()
            self.stats["sets"] += len(items)
            return True
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize values for set_many: {e}")
            self.stats["errors"] += 1
            return False
        except Exception as e:
            self._log_error(f"Cache set_many error ({len(items)} keys)", e)
            self.stats["errors"] += 1
            return False

    async def mget(self, keys: list) -> list:
        """Get many keys in one round trip; result aligns with *keys* (None = miss)."""
        if not keys:
            return []
        if not self.enabled or not self.client:
            return [None] * len(keys)

        try:
            values = await self.client.mget(keys)
        except Exception as e:
            self._log_error("Cache mget error", e)
            self.stats["errors"] += 1
            return [None] * len(keys)

        out = []
        for key, value in zip(keys, values):
            if value is None:
                self.stats["misses"] += 1
                out.append(None)
                continue
            try:
                out.append(json.loads(value))
                self.stats["hits"] += 1
            except json.JSONDecodeError as e:
                logger.error(f"Failed to deserialize cache value for {key}: {e}")
                self.stats["errors"] += 1
                out.append(None)
        return out

    async def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        Args:
            key: Cache key

        Returns:
            True if key was deleted, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            deleted = await self.client.delete(key)
            self.stats["deletes"] += 1

            if deleted:
                logger.debug(f"Cache DELETE: {key}")
                return True
            return False

        except Exception as e:
            logger.error(f"Cache delete error for {key}: {e}")
            self.stats["errors"] += 1
            return False

    async def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern.

        Uses SCAN for safe iteration over large keysets.

        Args:
            pattern: Key pattern (e.g., "cache:results:*")

        Returns:
            Number of keys deleted
        """
        if not self.enabled or not self.client:
            return 0

        try:
            deleted_count = 0

            # Use SCAN to iterate safely
            async for key in self.client.scan_iter(match=pattern, count=100):
                await self.client.delete(key)
                deleted_count += 1

            self.stats["deletes"] += deleted_count
            logger.debug(f"Cache DELETE pattern '{pattern}': {deleted_count} keys")
            return deleted_count

        except Exception as e:
            logger.error(f"Cache delete pattern error for {pattern}: {e}")
            self.stats["errors"] += 1
            return 0

    async def scan_keys(self, pattern: str) -> list[str]:
        """Return all keys matching *pattern* using non-blocking SCAN.

        Uses SCAN (via ``scan_iter``) rather than KEYS so a large keyspace can't
        block the Redis event loop on a shared instance.
        """
        if not self.enabled or not self.client:
            return []

        try:
            keys: list[str] = []
            async for key in self.client.scan_iter(match=pattern, count=100):
                keys.append(key.decode("utf-8") if isinstance(key, bytes) else key)
            return keys
        except Exception as e:
            self._log_error(f"Cache scan error for {pattern}", e)
            self.stats["errors"] += 1
            return []

    async def acquire_lock(self, key: str, token: str, ttl_ms: int) -> bool | None:
        """Try to acquire a short-lived distributed lock via ``SET key NX PX``.

        Returns True if acquired (caller is the leader), False if another holder
        owns it, or None if Redis is unavailable — in which case the caller
        should proceed without coordination rather than block.
        """
        if not self.enabled or not self.client:
            return None

        try:
            ok = await self.client.set(key, token, nx=True, px=ttl_ms)
            return bool(ok)
        except Exception as e:
            self._log_error(f"Lock acquire error for {key}", e)
            self.stats["errors"] += 1
            return None

    async def release_lock(self, key: str, token: str) -> None:
        """Release a lock only if this caller still owns it (compare-and-delete)."""
        if not self.enabled or not self.client:
            return

        try:
            await self.client.eval(_RELEASE_LOCK_LUA, 1, key, token)
        except Exception as e:
            self._log_error(f"Lock release error for {key}", e)
            self.stats["errors"] += 1

    async def exists(self, key: str) -> bool:
        """
        Check if key exists in cache.

        Args:
            key: Cache key

        Returns:
            True if key exists, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            return bool(await self.client.exists(key))
        except Exception as e:
            logger.error(f"Cache exists error for {key}: {e}")
            return False

    async def ttl(self, key: str) -> int:
        """
        Get remaining TTL for key.

        Args:
            key: Cache key

        Returns:
            TTL in seconds, -1 if no expiry, -2 if key doesn't exist
        """
        if not self.enabled or not self.client:
            return -2

        try:
            return await self.client.ttl(key)
        except Exception as e:
            logger.error(f"Cache TTL error for {key}: {e}")
            return -2

    def pool_snapshot(self) -> str:
        """Human-readable pool occupancy for error logs, never raises.

        Pool-internal attributes are underscore-prefixed in redis-py; a future
        rename degrades to zeros rather than masking the original error.
        """
        in_use = len(getattr(self.pool, "_in_use_connections", []) or [])
        avail = len(getattr(self.pool, "_available_connections", []) or [])
        return f"in_use={in_use}, available={avail}, max={self.max_connections}"

    async def pipelined_event_buffer(
        self,
        stream_key: str,
        *,
        event_id: int,
        max_size: int,
        stream_event: str | bytes,
        stream_record: str | bytes | None = None,
        ttl: Optional[int] = None,
        bare: bool = False,
    ) -> None:
        """Append one SSE frame to a run's event stream. Raises on failure.

        Steady state is a single bare ``XADD``. The explicit ``{event_id}-0``
        id is both the frontend's integer cursor and the write's idempotency
        fence — a duplicate id is rejected by Redis, which is what lets the
        caller retry an ambiguous write safely.

        Two extras ride along, both derived from ``event_id`` because both are
        properties of where the frame sits in the run: the first frame DELs
        whatever a crashed predecessor left under the key, and every 512th
        re-PERSISTs so a TTL stamped by a failed cleanup or a stale collector
        can't expire a live stream. ``bare`` suppresses both, which is what a
        retry needs — replaying a DEL could erase a frame this process never
        wrote, and replaying a PERSIST could re-immortalize a stream already
        stamped terminal.

        When ``stream_record`` is given the entry carries a second
        ``b"record"`` field, which the post-turn collector reads back with
        XRANGE instead of keeping a parallel List.

        Never swallows the cause: the caller classifies by exception type, and
        a bare ``False`` would erase the difference between "pool exhausted,
        nothing was sent" and "reply lost, the write may have landed".
        """
        if not self.enabled or not self.client:
            raise EventBufferUnavailableError(
                f"Redis event transport unavailable for {stream_key}"
            )

        payload = (
            stream_event.encode("utf-8")
            if isinstance(stream_event, str)
            else stream_event
        )
        fields: dict[bytes, bytes] = {b"event": payload}
        if stream_record is not None:
            fields[b"record"] = (
                stream_record.encode("utf-8")
                if isinstance(stream_record, str)
                else stream_record
            )
        append = dict(
            id=f"{int(event_id)}-0", maxlen=max_size, approximate=True
        )
        reset_epoch = not bare and event_id == 1
        heal_retention = not bare and event_id % _HEAL_INTERVAL == 0

        try:
            if not (reset_epoch or heal_retention or ttl is not None):
                await self.client.xadd(stream_key, fields, **append)
            else:
                async with self.client.pipeline(transaction=True) as pipe:
                    if reset_epoch:
                        pipe.delete(stream_key)
                    pipe.xadd(stream_key, fields, **append)
                    if ttl is not None:
                        pipe.expire(stream_key, ttl)
                    elif heal_retention:
                        pipe.persist(stream_key)
                    await pipe.execute()
        except Exception:
            self.stats["errors"] += 1
            raise
        self.stats["sets"] += 1

    async def stream_tail(
        self, stream_key: str
    ) -> Optional[tuple[int, Optional[bytes], Optional[bytes]]]:
        """Newest entry's integer id, ``event`` and ``record``; None if empty.

        The disambiguator for an ambiguous write: MAXLEN trims from the head,
        never the tail, so the newest entry is a stable witness of what landed.
        Auto-id frames (the run_end/error path) carry a millisecond timestamp
        and therefore compare far above any event id — which is exactly the
        signal that something other than this writer appended.

        Both fields ride along because the id alone cannot prove authorship:
        a run's stream is reset at event 1, so a crashed predecessor's frame
        can sit under the very id the caller is about to write. ``record`` is
        returned as well as ``event`` because it is the only field carrying
        the writer's ownership ids — the rendered SSE frame does not. One
        XREVRANGE returns all three, so proving the bytes costs no round trip.
        """
        if not self.enabled or not self.client:
            return None
        entries = await self.client.xrevrange(stream_key, count=1)
        if not entries:
            return None
        raw, fields = entries[0][0], entries[0][1]
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        fields = fields or {}

        def _as_bytes(value: Any) -> Optional[bytes]:
            return value.encode("utf-8") if isinstance(value, str) else value

        return (
            int(str(raw).split("-", 1)[0]),
            _as_bytes(fields.get(b"event")),
            _as_bytes(fields.get(b"record")),
        )

    async def clear_all(self) -> bool:
        """
        Clear all market-data cache entries.

        Uses targeted pattern deletes instead of FLUSHDB to avoid
        wiping unrelated Redis data (e.g. event buffers, sessions).

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            total = 0
            for pattern in ("ohlcv:*", "snapshot:*", "fmp:*", "market:*"):
                total += await self.delete_pattern(pattern)
            logger.warning("Cache cleared: %d market-data keys removed", total)
            return True
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
            return False

    def get_stats(self) -> dict:
        """
        Get cache statistics.

        Returns:
            Dict with hits, misses, sets, deletes, errors, hit_rate
        """
        total_requests = self.stats["hits"] + self.stats["misses"]
        hit_rate = (
            (self.stats["hits"] / total_requests * 100)
            if total_requests > 0
            else 0.0
        )

        return {
            **self.stats,
            "total_requests": total_requests,
            "hit_rate": round(hit_rate, 2),
            "enabled": self.enabled,
        }

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        self.stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "errors": 0,
        }
        logger.info("Cache statistics reset")


# Global cache client instance
_cache_client: Optional[RedisCacheClient] = None


def get_cache_client() -> RedisCacheClient:
    """
    Get global cache client instance.

    Returns:
        RedisCacheClient instance
    """
    global _cache_client

    if _cache_client is None:
        _cache_client = RedisCacheClient(max_connections=get_redis_max_connections())

    return _cache_client


def peek_cache_pool() -> Optional[ConnectionPool]:
    """The cache pool if one exists, without building it (metrics callbacks)."""
    return _cache_client.pool if _cache_client is not None else None


async def init_cache() -> None:
    """Initialize global cache client."""
    client = get_cache_client()
    await client.connect()


async def close_cache() -> None:
    """Close global cache client."""
    global _cache_client

    if _cache_client:
        await _cache_client.disconnect()
        _cache_client = None


@asynccontextmanager
async def cache_context():
    """
    Async context manager for cache lifecycle.

    Usage:
        async with cache_context():
            cache = get_cache_client()
            await cache.set("key", "value", ttl=60)
    """
    await init_cache()
    try:
        yield get_cache_client()
    finally:
        await close_cache()
