"""A per-key request limit every worker counts against together."""

import logging

logger = logging.getLogger(__name__)


async def throttle(key: str, limit: int, window_s: int) -> bool:
    """Whether one more call under ``key`` fits: at most ``limit`` in the
    ``window_s`` seconds from the first.

    Fails open, without Redis or on any error, as every limit here is a guard
    against abuse rather than a quota a user pays for. The window is set with
    the first count in one transaction, so a worker dying between the two
    cannot leave a count that never expires.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        return True
    try:
        async with cache.client.pipeline(transaction=True) as pipe:
            pipe.set(key, 0, ex=window_s, nx=True)
            pipe.incr(key)
            _, count = await pipe.execute()
    except Exception:
        logger.debug("[throttle] count failed for %s; failing open", key, exc_info=True)
        return True
    return int(count) <= limit
