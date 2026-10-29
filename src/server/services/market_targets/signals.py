"""Cross-worker nudge that a thread's targets changed.

The price monitor in every worker reloads armed targets on a timer; this
lets a just-armed or just-cancelled target take effect without waiting for
it. The message is the thread's id, so a worker re-reads that thread's
targets alone. Best effort: a missed message only delays a change until the
next reload, which stays the source of truth.
"""

import logging

from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

TARGETS_CHANGED_CHANNEL = "market_targets:changed"


async def publish_targets_changed(thread_id: str) -> None:
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return
    try:
        await cache.client.publish(TARGETS_CHANGED_CHANNEL, str(thread_id))
    except Exception:
        logger.debug("[MarketTargets] changed nudge not published", exc_info=True)
