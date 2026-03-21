"""Utility helpers for background subagent middleware."""

from collections.abc import Awaitable, Callable

MessageChecker = Callable[[], Awaitable[bool]]


async def build_message_checker(thread_id: str | None) -> MessageChecker | None:
    """Return an async closure that peeks at the Redis key for pending steering messages.

    Uses ``LLEN`` (O(1)) — never consumes messages. Returns ``None`` when
    Redis is unavailable or *thread_id* is falsy, so callers can skip the check.
    """
    if not thread_id:
        return None

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None

    key = f"workflow:steering:{thread_id}"

    async def checker() -> bool:
        return (await cache.client.llen(key)) > 0

    return checker
