"""
Redis cache client.

Provides connection pooling, health checking, and basic cache operations
with TTL support and pattern-based deletion.
"""

import json
import logging
import os
from datetime import datetime, date
from typing import Any, Optional, Tuple, Union
from contextlib import asynccontextmanager
from uuid import UUID

import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

from src.config.settings import is_redis_cache_enabled, get_nested_config

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
        max_connections: int = 10,
        socket_timeout: int = 5,
        socket_connect_timeout: int = 5,
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
            self.pool = ConnectionPool.from_url(
                self.url,
                max_connections=self.max_connections,
                decode_responses=False,  # We handle JSON encoding
            )

            self.client = redis.Redis(connection_pool=self.pool)

            # Test connection
            await self.client.ping()
            logger.info(f"Redis cache connected: {self.url}")

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
            logger.error(f"Cache get error for {key}: {e}")
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
            logger.error(f"Cache set error for {key}: {e}")
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

    # ==================== Stale-While-Revalidate Operations ====================

    async def get_with_swr(
        self,
        key: str,
        original_ttl: int,
        soft_ttl_ratio: float = 0.6,
    ) -> Tuple[Optional[Any], bool]:
        """
        Get value with stale-while-revalidate info.

        Returns cached value and whether a background refresh is recommended.
        The soft TTL threshold determines when data is considered "stale but usable".

        Args:
            key: Cache key
            original_ttl: Original TTL when the key was set (needed for ratio calculation)
            soft_ttl_ratio: Ratio of TTL at which data becomes stale (default 0.6 = 60%)
                           When remaining TTL < original_ttl * (1 - soft_ttl_ratio),
                           the data is stale and needs_refresh=True

        Returns:
            Tuple of (value, needs_refresh):
            - value: Cached value or None if not found
            - needs_refresh: True if cache miss OR remaining TTL below soft threshold
        """
        if not self.enabled or not self.client:
            return None, True

        try:
            # Get value
            value = await self.client.get(key)

            if value is None:
                self.stats["misses"] += 1
                logger.debug(f"Cache MISS (SWR): {key}")
                return None, True

            self.stats["hits"] += 1

            # Deserialize JSON
            try:
                deserialized = json.loads(value)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to deserialize cache value for {key}: {e}")
                self.stats["errors"] += 1
                return None, True

            # Check remaining TTL to determine if refresh is needed
            remaining_ttl = await self.client.ttl(key)

            # TTL of -1 means no expiry, -2 means key doesn't exist
            soft_threshold = original_ttl * soft_ttl_ratio
            if remaining_ttl < 0:
                # No expiry set or key vanished - consider fresh if we have data
                needs_refresh = remaining_ttl == -2
            else:
                # Calculate soft threshold: refresh when we've consumed (1 - soft_ttl_ratio) of TTL
                # Example: soft_ttl_ratio=0.6 means refresh when 40% of TTL has passed
                # i.e., when remaining_ttl < 60% of original_ttl
                needs_refresh = remaining_ttl < soft_threshold

            if needs_refresh:
                logger.debug(f"Cache HIT (SWR stale): {key} - remaining TTL {remaining_ttl}s < threshold {soft_threshold}s")
            else:
                logger.debug(f"Cache HIT (SWR fresh): {key} - remaining TTL {remaining_ttl}s")

            return deserialized, needs_refresh

        except Exception as e:
            logger.error(f"Cache get_with_swr error for {key}: {e}")
            self.stats["errors"] += 1
            return None, True

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

            # Append to list
            await self.client.rpush(key, serialized)

            # Trim to max size (FIFO: keep newest elements)
            if max_size:
                await self.client.ltrim(key, -max_size, -1)

            # Set TTL if provided
            if ttl:
                await self.client.expire(key, ttl)

            self.stats["sets"] += 1
            logger.debug(f"List APPEND: {key} (max_size: {max_size})")
            return True

        except Exception as e:
            logger.error(f"List append error for {key}: {e}")
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

            # Set hash field
            await self.client.hset(key, field, serialized)

            # Set TTL if provided
            if ttl:
                await self.client.expire(key, ttl)

            self.stats["sets"] += 1
            logger.debug(f"Hash SET: {key}:{field} (TTL: {ttl}s)")
            return True

        except Exception as e:
            logger.error(f"Hash set error for {key}:{field}: {e}")
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
            logger.error(f"Hash get all error for {key}: {e}")
            self.stats["errors"] += 1
            return {}

    async def clear_all(self) -> bool:
        """
        Clear all cache entries.

        WARNING: This flushes the entire Redis database.

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            await self.client.flushdb()
            logger.warning("Cache cleared (FLUSHDB)")
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
        _cache_client = RedisCacheClient()

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
