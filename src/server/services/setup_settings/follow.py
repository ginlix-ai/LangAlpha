"""Keeps every worker on the saved settings.

The nudge is only a shortcut: each worker also re-reads the table on a timer,
whether or not Redis is up, so a nudge missed in a reconnect, or one a worker
could not receive at all, only delays a change by a tick.
"""

from __future__ import annotations

import asyncio
import logging
import time

from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

CHANGED_CHANNEL = "setup_settings:changed"

_TICK_SECONDS = 60.0
_POLL_SECONDS = 5.0
_RETRY_SECONDS = 10.0

_task: asyncio.Task | None = None


async def publish_changed() -> None:
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return
    try:
        await cache.client.publish(CHANGED_CHANNEL, "1")
    except Exception:  # noqa: BLE001 - the other workers catch up on their tick
        logger.debug("[SETUP] changed nudge not published", exc_info=True)


async def _refresh_quietly() -> None:
    from src.server.services.setup_settings.apply import refresh

    try:
        await refresh()
    except Exception as exc:  # noqa: BLE001 - the next tick tries again
        logger.warning("[SETUP] re-reading saved settings failed: %s", type(exc).__name__)


async def _listen(client) -> None:
    pubsub = client.pubsub()
    try:
        await pubsub.subscribe(CHANGED_CHANNEL)
        # Whatever was published before the subscribe landed is gone.
        await _refresh_quietly()
        read_at = time.monotonic()
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=_POLL_SECONDS
            )
            if message or time.monotonic() - read_at >= _TICK_SECONDS:
                await _refresh_quietly()
                read_at = time.monotonic()
    finally:
        try:
            await pubsub.aclose()
        except Exception:  # noqa: BLE001
            pass


async def _follow() -> None:
    from src.server.services.workspace_status_pubsub import get_shared_pubsub_client

    while True:
        try:
            client = await get_shared_pubsub_client()
            if client is None:
                await asyncio.sleep(_TICK_SECONDS)
                await _refresh_quietly()
                continue
            await _listen(client)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never retire while Redis is away
            logger.warning("[SETUP] settings listener error, retrying: %s", type(exc).__name__)
            await _refresh_quietly()
        await asyncio.sleep(_RETRY_SECONDS)


def start_following() -> None:
    global _task
    if _task is None or _task.done():
        _task = asyncio.create_task(_follow(), name="setup_settings_follow")


async def stop_following() -> None:
    global _task
    task, _task = _task, None
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
