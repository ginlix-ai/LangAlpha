"""Per-computer code admission shared across workers.

Threads and subagents share tier vCPUs; Redis ZSET slots avoid holding a
Postgres pool connection for each minutes-long run. Fail open on admission
errors so Redis loss costs the concurrency bound, not the turn.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import time
from collections.abc import AsyncIterator
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# One slot per tier vCPU.
CODE_SLOTS_BY_TIER: dict[str, int] = {
    "standard": 1,
    "performance": 2,
    "max": 4,
}

# Unknown deployment tiers get the safest bound: overserialization beats oversubscription.
DEFAULT_CODE_SLOTS = 1

# Jitter prevents workers from repeatedly colliding after a release.
_QUEUE_BASE_DELAY_S = 0.25
_QUEUE_MAX_DELAY_S = 2.0

# Cover all three PTCSandbox.execute attempts plus transport slack.
_EXECUTE_ATTEMPTS = 3
_EXECUTE_TRANSPORT_SLACK_S = 30
_DEFAULT_MAX_EXECUTION_TIME_S = 300
_REAP_MARGIN_S = 60

# Process-local state gates logging only, never admission truth.
_fail_open_logged = False


def code_run_key(computer_id: str) -> str:
    return f"sandbox:code_run:{computer_id}"


def code_slots_for_tier(tier: str | None) -> int:
    return CODE_SLOTS_BY_TIER.get((tier or "").strip().lower(), DEFAULT_CODE_SLOTS)


def reap_horizon_seconds(max_execution_time: int | None = None) -> int:
    """Exceed the longest live hold or reaping admits runs beyond the limit.

    CODE_RUN_SLOT_TTL overrides the horizon for tests.
    """
    override = os.getenv("CODE_RUN_SLOT_TTL")
    if override:
        return int(override)
    per_attempt = (
        max_execution_time or _DEFAULT_MAX_EXECUTION_TIME_S
    ) + _EXECUTE_TRANSPORT_SLACK_S
    return _EXECUTE_ATTEMPTS * per_attempt + _REAP_MARGIN_S


def queue_timeout_seconds(max_execution_time: int | None = None) -> int:
    """Wait through one holder attempt but not a leaked slot; CODE_RUN_QUEUE_TIMEOUT is for tests."""
    override = os.getenv("CODE_RUN_QUEUE_TIMEOUT")
    if override:
        return int(override)
    return (
        max_execution_time or _DEFAULT_MAX_EXECUTION_TIME_S
    ) + _EXECUTE_TRANSPORT_SLACK_S


def max_execution_time_of(backend: Any) -> int | None:
    security = getattr(
        getattr(getattr(backend, "sandbox", None), "config", None), "security", None
    )
    value = getattr(security, "max_execution_time", None)
    return value if isinstance(value, int) else None


def _log_fail_open_once(reason: str, **fields: Any) -> None:
    global _fail_open_logged
    if _fail_open_logged:
        return
    _fail_open_logged = True
    logger.warning(
        "Per-computer code-run admission is running unbounded", reason=reason, **fields
    )


def _redis_client() -> Any | None:
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
    except Exception as exc:  # cache layer absent (CLI / OSS embedding)
        _log_fail_open_once(f"cache client unavailable: {exc}")
        return None
    if not cache.enabled or not cache.client:
        _log_fail_open_once("redis cache disabled or not connected")
        return None
    return cache.client


async def _queue_for_slot(
    client: Any, key: str, *, limit: int, stale_after: int, queue_timeout: int
) -> str | None:
    from src.server.utils.slot_guard import acquire_slot_member

    deadline = time.monotonic() + queue_timeout
    delay = _QUEUE_BASE_DELAY_S
    queued_at: float | None = None

    while True:
        try:
            admitted = await acquire_slot_member(
                client, key, limit=limit, stale_after=stale_after
            )
        except Exception as exc:
            _log_fail_open_once(f"redis error: {exc}", key=key)
            return None

        if admitted.allowed:
            if queued_at is not None:
                logger.info(
                    "Code run admitted after queueing",
                    key=key,
                    limit=limit,
                    waited_s=round(time.monotonic() - queued_at, 2),
                )
            return admitted.slot_id

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning(
                "Code-run slot wait expired, running unbounded",
                key=key,
                limit=limit,
                holders=admitted.current,
            )
            return None

        if queued_at is None:
            queued_at = time.monotonic()
            logger.info(
                "Code run queued for a slot",
                key=key,
                limit=limit,
                holders=admitted.current,
            )
        await asyncio.sleep(min(delay + random.uniform(0.0, delay / 2), remaining))
        delay = min(delay * 2, _QUEUE_MAX_DELAY_S)


@contextlib.asynccontextmanager
async def code_run_slot(
    computer_id: str | None,
    *,
    tier: str | None = None,
    max_execution_time: int | None = None,
) -> AsyncIterator[str | None]:
    if not computer_id:
        yield None
        return

    client = _redis_client()
    if client is None:
        yield None
        return

    key = code_run_key(computer_id)
    slot_id = await _queue_for_slot(
        client,
        key,
        limit=code_slots_for_tier(tier),
        stale_after=reap_horizon_seconds(max_execution_time),
        queue_timeout=queue_timeout_seconds(max_execution_time),
    )
    try:
        yield slot_id
    finally:
        if slot_id:
            from src.server.utils.slot_guard import release_slot_member

            try:
                await release_slot_member(client, key, slot_id)
            except Exception as exc:
                # Reaping covers leaks; release failure must not replace a successful code result.
                logger.warning(
                    "Code-run slot release failed", key=key, error=str(exc)
                )
