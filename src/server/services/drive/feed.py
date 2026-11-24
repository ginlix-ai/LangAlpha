"""Wake-ups for callers waiting on a space's next change.

Postgres is the truth: a waiter reads the space's change number before it
sleeps and again when woken. Redis only carries the hint that a space
changed, on one channel every worker listens to through one subscription,
so a thousand waiting mounts cost one connection per worker rather than one
each. A lost hint costs the waiter its timeout, never a change.
"""

from __future__ import annotations

import asyncio
import json
import logging

from src.server.services.workspace_status_pubsub import subscribe_to_channel
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

CHANNEL = "drive:changed"

# Execution context only: which of this worker's requests wait on which
# space. Nothing reads it as the state of a space.
_waiters: dict[str, set[asyncio.Event]] = {}
_listener: asyncio.Task | None = None


async def announce(space_id: str) -> None:
    """Tell the waiters on every worker that the space changed; best effort."""
    _wake(space_id)
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return
    try:
        await cache.client.publish(CHANNEL, json.dumps({"space": space_id}))
    except Exception as exc:
        logger.debug("drive change publish failed: %s", exc)


def _wake(space_id: str | None = None) -> None:
    groups = [_waiters.get(space_id, ())] if space_id else list(_waiters.values())
    for group in groups:
        for event in group:
            event.set()


async def _listen() -> None:
    while True:
        try:
            async with subscribe_to_channel(CHANNEL) as wait:
                if wait is None:
                    await asyncio.sleep(5)
                    continue
                # Anything published before the subscription took is
                # unseen, so every waiter reads again.
                _wake()
                while True:
                    kind, payload = await wait(30)
                    if kind == "error":
                        break
                    if kind == "message" and isinstance(payload, dict):
                        space = payload.get("space")
                        if isinstance(space, str):
                            _wake(space)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("drive change listener failed", exc_info=True)
        await asyncio.sleep(1)


def _ensure_listener() -> None:
    global _listener
    if _listener is None or _listener.done():
        _listener = asyncio.get_running_loop().create_task(_listen())


class Waiter:
    """A registration taken before the caller's first read, so a change
    committed between that read and the sleep still wakes it."""

    def __init__(self, space_id: str) -> None:
        self.space_id = space_id
        self.event = asyncio.Event()

    def __enter__(self) -> Waiter:
        _ensure_listener()
        _waiters.setdefault(self.space_id, set()).add(self.event)
        return self

    def __exit__(self, *_: object) -> None:
        group = _waiters.get(self.space_id)
        if group is not None:
            group.discard(self.event)
            if not group:
                _waiters.pop(self.space_id, None)

    async def sleep(self, timeout: float) -> None:
        """Until a hint for the space comes or ``timeout`` passes."""
        try:
            await asyncio.wait_for(self.event.wait(), timeout)
        except TimeoutError:
            pass
        self.event.clear()
