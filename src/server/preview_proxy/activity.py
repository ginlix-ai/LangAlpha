"""Preview traffic as computer activity.

E2B counts only the server's own calls toward a sandbox's deadline, so a
person using an app through the proxy would see it pause under them. Each
proxied request, and each chunk or message relayed either way on a stream or
socket, notes the capability's sandbox; at most once a minute across
all workers, the computer's deadline is pushed out, never resuming a stopped
one. A capability that lapsed counts for nothing, which bounds how long an
abandoned tab can hold a computer awake to its lifetime plus one idle window.
"""

from __future__ import annotations

import asyncio
import logging
import time

from src.server.services.preview_capabilities import Capability
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# Shorter than the briefest idle window E2B allows (60 s) would be needless;
# this keeps any window of two minutes or more alive under steady traffic.
_DEBOUNCE_S = 60
_DEBOUNCE_MS = _DEBOUNCE_S * 1000
_RECENT_CAP = 4096


class PreviewActivity:
    """Debounces preview traffic into keep-alive calls, per sandbox."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[None]] = set()
        # When this worker last asked Redis about a sandbox. Only saves the
        # lock round trip on every request: the debounce itself is the Redis
        # lock, which every worker shares.
        self._recent: dict[str, float] = {}

    def note(self, capability: Capability) -> None:
        if time.time() >= capability.expires_at:
            return
        now = time.monotonic()
        last = self._recent.get(capability.sandbox_id)
        if last is not None and now - last < _DEBOUNCE_S:
            return
        if len(self._recent) >= _RECENT_CAP:
            self._recent.clear()
        self._recent[capability.sandbox_id] = now
        task = asyncio.create_task(self._renew(capability))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _renew(self, capability: Capability) -> None:
        try:
            acquired = await get_cache_client().acquire_lock(
                f"preview:activity:{capability.sandbox_id}", "1", _DEBOUNCE_MS
            )
            if not acquired:
                return
            from src.server.services.workspace_manager import WorkspaceManager

            await WorkspaceManager.get_instance().keep_sandbox_alive(
                capability.workspace_id, capability.sandbox_id
            )
        except Exception as e:
            logger.debug(
                "Preview activity did not reach sandbox %s: %s",
                capability.sandbox_id,
                e,
            )

    async def aclose(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
