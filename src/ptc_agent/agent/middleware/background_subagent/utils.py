"""Utility helpers for background subagent middleware."""

import json
from collections.abc import Awaitable, Callable

MessageChecker = Callable[[], Awaitable[bool]]


def config_own_run_id(config: dict | None) -> str | None:
    """The run stamped into this invocation's config metadata (v4 2.4c) —
    the same identity SteeringMiddleware filters deliveries by."""
    return ((config or {}).get("metadata") or {}).get("run_id")


async def build_message_checker(
    thread_id: str | None, own_run_id: str | None = None
) -> MessageChecker | None:
    """Return an async closure that peeks at the Redis key for pending steering messages.

    Peeks without consuming. A payload stamped with a FOREIGN run_id is not
    counted (v4 2.4c): SteeringMiddleware will never deliver it to this run,
    so waking a wait or re-invoking the agent for it would chase a message
    that cannot arrive. Returns ``None`` when Redis is unavailable or
    *thread_id* is falsy, so callers can skip the check.
    """
    if not thread_id:
        return None

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None

    key = f"workflow:steering:{thread_id}"

    async def checker() -> bool:
        raws = await cache.client.lrange(key, 0, -1)
        for raw in raws:
            try:
                data = json.loads(
                    raw.decode("utf-8") if isinstance(raw, bytes) else raw
                )
            except (ValueError, UnicodeDecodeError):
                continue
            if (
                isinstance(data, dict)
                and own_run_id
                and data.get("run_id") not in (None, own_run_id)
            ):
                continue
            return True
        return False

    return checker
