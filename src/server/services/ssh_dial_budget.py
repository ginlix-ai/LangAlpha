"""A per-user budget of SSH dials the server makes to addresses the caller names.

Every plan may add a machine, and a probe, a pairing report or a create makes
this server connect to an address the caller chose, so without a cap the
deployment's address is a scanner for anyone with an account. Starting or
opening a computer that exists dials its stored address and is not counted.

The budget is abuse protection, not the security boundary, so an unreachable
Redis fails open. The counter lives in Redis because a request may land on
any worker.
"""

from __future__ import annotations

import logging
import math
import time

from ptc_agent.config.core import SSHConfig

logger = logging.getLogger(__name__)

_WINDOW_S = 3600


class SSHDialBudgetExceeded(Exception):
    """The caller has spent the hour's dials; ``retry_after`` is the seconds left in it."""

    code = "rate_limited"

    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        minutes = max(1, math.ceil(retry_after / 60))
        unit = "minute" if minutes == 1 else "minutes"
        super().__init__(
            "This account has tried too many SSH connections in the last hour. "
            f"Try again in {minutes} {unit}."
        )


async def spend_dials(user_id: str, attempts: int, *, settings: SSHConfig) -> None:
    """Spend ``attempts`` dials before making any of them, or refuse them all.

    Counted per host and port tried, so a report naming many addresses costs
    what it makes the server do. A refused request is given back, so a later
    one that still fits is not shut out by it.

    Raises:
        SSHDialBudgetExceeded: The hour's budget cannot cover ``attempts``.
    """
    limit = settings.dials_per_hour
    if limit <= 0 or attempts <= 0:
        return

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        logger.warning("[ssh_dial_budget] Redis unavailable; budget fails open")
        return
    redis = cache.client

    now = time.time()
    window = int(now // _WINDOW_S)
    key = f"ssh:dials:{user_id}:{window}"
    try:
        async with redis.pipeline(transaction=True) as pipe:
            pipe.incrby(key, attempts)
            pipe.expire(key, _WINDOW_S)
            used, _ = await pipe.execute()
    except Exception:
        logger.warning("[ssh_dial_budget] count failed; failing open", exc_info=True)
        return
    if int(used) <= limit:
        return
    try:
        await redis.decrby(key, attempts)
    except Exception:
        logger.warning("[ssh_dial_budget] refund failed", exc_info=True)
    raise SSHDialBudgetExceeded(math.ceil((window + 1) * _WINDOW_S - now))
