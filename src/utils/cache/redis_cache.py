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
from redis.asyncio.connection import ConnectionPool

from src.config.settings import (
    is_redis_cache_enabled,
    get_nested_config,
    get_redis_max_connections,
    get_redis_socket_timeout,
    get_redis_socket_connect_timeout,
)

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime and UUID objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


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
            # health_check_interval: redis-py sends PING on idle connections before
            # handing them out, so poisoned connections (dropped by the server or
            # network) get detected and discarded instead of lingering in the pool.
            self.pool = ConnectionPool.from_url(
                self.url,
                max_connections=self.max_connections,
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
                f"(pool max={self.max_connections}, health_check=30s)"
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
        """Log a Redis error, with pool stats if the error is pool exhaustion.

        Tested against redis-py 5.x where `MaxConnectionsError` is a subclass
        of `ConnectionError`. The string fallback (`"Too many connections"`)
        catches older/forked variants that raise the bare parent type.
        Pool-internal attributes are underscore-prefixed in redis-py; we guard
        with getattr so a future rename degrades gracefully to a plain log line.
        """
        is_pool_exhaustion = (
            isinstance(err, getattr(redis.exceptions, "MaxConnectionsError", ()))
            or "Too many connections" in str(err)
        )
        if is_pool_exhaustion:
            in_use = len(getattr(self.pool, "_in_use_connections", []) or [])
            avail = len(getattr(self.pool, "_available_connections", []) or [])
            logger.error(
                f"{context} — {type(err).__name__}: {err} "
                f"(pool: in_use={in_use}, available={avail}, "
                f"max={self.max_connections})"
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
            ttl: Time-to-live in seconds (optional)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            # Serialize to JSON with datetime support
            serialized = json.dumps(value, ensure_ascii=False, cls=DateTimeEncoder)

            # Set with optional TTL
            if ttl:
                await self.client.setex(key, ttl, serialized)
            else:
                await self.client.set(key, serialized)

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

    # ==================== List Operations ====================

    async def list_append(
        self,
        key: str,
        value: Any,
        max_size: Optional[int] = None,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Append value to Redis list and optionally trim to max size.

        Args:
            key: Redis key
            value: Value to append (will be JSON serialized)
            max_size: Optional max list size (LTRIM to enforce FIFO)
            ttl: Optional TTL for the entire list

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            # Serialize value (handle both strings and objects)
            if isinstance(value, str):
                serialized = value
            else:
                serialized = json.dumps(value, ensure_ascii=False, cls=DateTimeEncoder)

            # Atomic RPUSH + LTRIM + EXPIRE via pipeline
            async with self.client.pipeline(transaction=True) as pipe:
                pipe.rpush(key, serialized)
                if max_size:
                    pipe.ltrim(key, -max_size, -1)
                if ttl:
                    pipe.expire(key, ttl)
                await pipe.execute()

            self.stats["sets"] += 1
            logger.debug(f"List APPEND: {key} (max_size: {max_size})")
            return True

        except Exception as e:
            self._log_error(f"List append error for {key}", e)
            self.stats["errors"] += 1
            return False

    async def list_range(
        self,
        key: str,
        start: int = 0,
        end: int = -1,
    ) -> list:
        """
        Get range of elements from Redis list.

        Args:
            key: Redis key
            start: Start index (0-based)
            end: End index (-1 means end of list)

        Returns:
            List of values (strings, not deserialized)
        """
        if not self.enabled or not self.client:
            return []

        try:
            values = await self.client.lrange(key, start, end)

            if not values:
                self.stats["misses"] += 1
                return []

            self.stats["hits"] += 1

            # Decode bytes to strings
            result = []
            for value in values:
                if isinstance(value, bytes):
                    result.append(value.decode('utf-8'))
                else:
                    result.append(value)

            return result

        except Exception as e:
            logger.error(f"List range error for {key}: {e}")
            self.stats["errors"] += 1
            return []

    async def list_length(self, key: str) -> int:
        """
        Get length of Redis list.

        Args:
            key: Redis key

        Returns:
            List length, or 0 if not found/error
        """
        if not self.enabled or not self.client:
            return 0

        try:
            return await self.client.llen(key)
        except Exception as e:
            logger.error(f"List length error for {key}: {e}")
            self.stats["errors"] += 1
            return 0

    # ==================== Hash Operations ====================

    async def hash_set(
        self,
        key: str,
        field: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set hash field value.

        When `ttl` is set, HSET and EXPIRE are pipelined in a single MULTI/EXEC
        so the write takes one pool checkout instead of two.

        Args:
            key: Redis key
            field: Hash field name
            value: Value to set (will be JSON serialized)
            ttl: Optional TTL for the entire hash

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            # Serialize value
            serialized = json.dumps(value, ensure_ascii=False, cls=DateTimeEncoder)

            # Pipeline HSET + EXPIRE so we make one pool checkout instead of two.
            # Under high event rate the doubled checkouts per hash write were a
            # contributor to pool exhaustion.
            if ttl:
                async with self.client.pipeline(transaction=True) as pipe:
                    pipe.hset(key, field, serialized)
                    pipe.expire(key, ttl)
                    await pipe.execute()
            else:
                await self.client.hset(key, field, serialized)

            self.stats["sets"] += 1
            logger.debug(f"Hash SET: {key}:{field} (TTL: {ttl}s)")
            return True

        except Exception as e:
            self._log_error(f"Hash set error for {key}:{field}", e)
            self.stats["errors"] += 1
            return False

    async def hash_get(self, key: str, field: str) -> Optional[Any]:
        """
        Get hash field value.

        Args:
            key: Redis key
            field: Hash field name

        Returns:
            Deserialized value or None
        """
        if not self.enabled or not self.client:
            return None

        try:
            value = await self.client.hget(key, field)

            if value is None:
                self.stats["misses"] += 1
                return None

            self.stats["hits"] += 1

            # Decode if bytes
            if isinstance(value, bytes):
                value = value.decode('utf-8')

            # Deserialize JSON
            return json.loads(value)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize hash field {key}:{field}: {e}")
            self.stats["errors"] += 1
            return None
        except Exception as e:
            logger.error(f"Hash get error for {key}:{field}: {e}")
            self.stats["errors"] += 1
            return None

    async def hash_get_all(self, key: str) -> dict:
        """
        Get all hash fields and values.

        Args:
            key: Redis key

        Returns:
            Dictionary of field -> value (deserialized)
        """
        if not self.enabled or not self.client:
            return {}

        try:
            data = await self.client.hgetall(key)

            if not data:
                self.stats["misses"] += 1
                return {}

            self.stats["hits"] += 1

            # Deserialize all values
            result = {}
            for field, value in data.items():
                try:
                    # Decode bytes keys and values
                    field_str = field.decode('utf-8') if isinstance(field, bytes) else field
                    value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                    result[field_str] = json.loads(value_str)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"Failed to deserialize hash field: {e}")

            return result

        except Exception as e:
            self._log_error(f"Hash get all error for {key}", e)
            self.stats["errors"] += 1
            return {}

    async def pipelined_event_buffer(
        self,
        events_key: str,
        meta_key: str,
        event: str,
        max_size: int,
        ttl: int,
        last_event_id: Optional[int] = None,
        stream_key: Optional[str] = None,
    ) -> tuple[bool, int]:
        """Atomic pipeline for the SSE event-buffer hot path.

        Collapses ~9 sequential Redis commands (list append, trim, meta hash,
        optional stream append) into one MULTI/EXEC so the whole spill takes
        one pool checkout.

        When ``stream_key`` and ``last_event_id`` are both provided, the same
        event is dual-written to a Redis Stream at ``stream_key`` with explicit
        ID ``f"{last_event_id}-0"``. This is the dual-write phase of the
        Streams refactor — readers continue reading the List during this phase
        while the Stream gets populated for parity verification.

        Returns (success, seq). On success `seq` is the new event count (1+);
        on failure it's 0.
        """
        if not self.enabled or not self.client:
            return False, 0

        try:
            now_iso_json = json.dumps(datetime.now().isoformat())
            async with self.client.pipeline(transaction=True) as pipe:
                # Dirty-resume guard runs BEFORE the new writes so RPUSH and
                # HINCRBY land on fresh state. When last_event_id == 1 we're
                # at the start of a fresh handler instance (counter resets to
                # 1 per turn). If a prior turn left state behind (process
                # crash before the post-persist ``_on_pair_persisted``
                # callback ran ``clear_event_buffer``), three things can be
                # stale:
                # - ``stream_key``: XADD with id=1-0 would fail because the
                #   last id is N-0 from that prior turn — Redis enforces
                #   strict id monotonicity.
                # - ``events_key``: a stale RPUSH tail from the prior turn
                #   would persist alongside the fresh entries, breaking
                #   ``LLEN == XLEN`` parity until both are DEL'd together.
                # - ``meta_key`` ``seq``: HINCRBY against a stale counter
                #   would return N+1 instead of 1, drifting the returned
                #   ``seq`` away from the SSE wire id.
                # DEL'ing inside the same MULTI/EXEC keeps the reset atomic.
                # ``created_at`` on the meta hash is preserved (HDEL only
                # the ``seq`` field) since it documents thread-first-write
                # for diagnostics, not per-turn state.
                guard_cmds = 0
                if last_event_id == 1:
                    if stream_key is not None:
                        pipe.delete(stream_key)
                        guard_cmds += 1
                    pipe.delete(events_key)
                    pipe.hdel(meta_key, "seq")
                    guard_cmds += 2

                pipe.rpush(events_key, event)
                pipe.ltrim(events_key, -max_size, -1)
                pipe.expire(events_key, ttl)
                pipe.hincrby(meta_key, "seq", 1)
                pipe.hsetnx(meta_key, "created_at", now_iso_json)
                pipe.hset(meta_key, "updated_at", now_iso_json)
                if last_event_id is not None:
                    pipe.hset(meta_key, "last_event_id", json.dumps(last_event_id))
                pipe.expire(meta_key, ttl)
                # Dual-write to Redis Stream when both key and id are provided.
                # Explicit ID `<seq>-0` keeps the cursor integer-friendly for
                # the frontend's parseInt-based last_event_id parsing while
                # preserving Redis Streams' lexicographic ordering.
                if stream_key is not None and last_event_id is not None:
                    payload = event.encode("utf-8") if isinstance(event, str) else event
                    pipe.xadd(
                        stream_key,
                        {b"event": payload},
                        id=f"{last_event_id}-0",
                        maxlen=max_size,
                        approximate=True,
                    )
                    pipe.expire(stream_key, ttl)
                results = await pipe.execute()
            self.stats["sets"] += 1
            # HINCRBY's return value sits at index ``guard_cmds + 3``: 3
            # commands of new-write prefix (RPUSH, LTRIM, EXPIRE) precede it,
            # plus any dirty-resume guard commands that ran first.
            hincrby_index = guard_cmds + 3
            seq = (
                int(results[hincrby_index])
                if len(results) > hincrby_index
                else 0
            )
            return True, seq
        except Exception as e:
            self._log_error(f"Pipelined event buffer failed for {events_key}", e)
            self.stats["errors"] += 1
            return False, 0

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
