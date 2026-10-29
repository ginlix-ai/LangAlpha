"""The Redis the mount's services coordinate through, and their keys' tag."""

from __future__ import annotations

from typing import Any

from src.utils.cache.redis_cache import get_cache_client


def client() -> Any:
    """The Redis client, or None when Redis is off."""
    cache = get_cache_client()
    return cache.client if cache.enabled and cache.client else None


def text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def tag(computer_id: str) -> str:
    """The prefix of every key one computer's mount keeps. The braces put
    them in one cluster slot, so one script may take several."""
    return f"livefs:{{{computer_id}}}"
