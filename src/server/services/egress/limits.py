"""Cross-worker rate + concurrency limits for the egress relay (Redis).

Limits are protective plumbing, not the security boundary (that's the JWT +
grant checks) — so an unreachable Redis fails OPEN with a warning rather than
taking every connector down with it.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from src.server.services.egress import RelayError, RelayRejection
from src.server.utils.slot_guard import acquire_slot_member, release_slot_member

logger = logging.getLogger(__name__)

# Per-grant budgets, deliberately generous — a single agent turn fans out at
# most a handful of concurrent tool calls.
RATE_LIMIT_RPM = 120
CONCURRENCY_LIMIT = 4

# The rate bucket is a fixed-minute counter; its TTL only has to outlive the
# minute it counts. The concurrency window is a reap horizon for slots whose
# holder died, so it must exceed the relay's 55s wall clock.
_RATE_KEY_TTL = 120
_CONC_STALE_AFTER = 120


class RelayLimited(RelayRejection):
    """A budget rejection: 429 with the vendor-agnostic backoff hint."""

    def __init__(self, kind: str):
        self.kind = kind  # "rate" | "concurrency"
        super().__init__(
            429,
            RelayError(f"limited_{kind}"),
            f"relay limit: {kind}",
            retry_after=5,
        )


@asynccontextmanager
async def acquire_slot(grant_id: str):
    """Hold one concurrency slot for the duration of a relayed request."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        logger.warning("[egress_limits] Redis unavailable; limits fail open")
        yield
        return
    redis = cache.client

    minute = int(time.time() // 60)
    rate_key = f"egress:rate:{grant_id}:{minute}"
    conc_key = f"egress:conc:{grant_id}"

    try:
        async with redis.pipeline(transaction=True) as pipe:
            pipe.incr(rate_key)
            pipe.expire(rate_key, _RATE_KEY_TTL)
            count, _ = await pipe.execute()
    except Exception:
        logger.warning("[egress_limits] rate check failed; failing open", exc_info=True)
        yield
        return
    if int(count) > RATE_LIMIT_RPM:
        raise RelayLimited("rate")

    try:
        admitted = await acquire_slot_member(
            redis, conc_key, limit=CONCURRENCY_LIMIT, stale_after=_CONC_STALE_AFTER
        )
    except Exception:
        logger.warning(
            "[egress_limits] concurrency check failed; failing open", exc_info=True
        )
        yield
        return

    if not admitted.allowed:
        raise RelayLimited("concurrency")
    try:
        yield
    finally:
        try:
            await release_slot_member(redis, conc_key, admitted.slot_id)
        except Exception:
            logger.warning(
                "[egress_limits] slot release failed for %s", grant_id
            )
