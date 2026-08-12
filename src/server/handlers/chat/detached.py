"""Fire-and-forget task scheduling for chat handlers.

Holds strong references to detached background tasks so the event loop can't GC
them mid-flight; task exceptions are logged and swallowed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine

logger = logging.getLogger("src.server.handlers.chat_handler")

# Strong references to fire-and-forget tasks so the event loop doesn't GC them.
_background_tasks: set[asyncio.Task] = set()


def fire_and_forget(coro: Coroutine, *, name: str = "") -> None:
    """Schedule a coroutine as a fire-and-forget background task.

    Exceptions are logged and suppressed, so they never surface
    as 'Task exception was never retrieved'.
    """
    async def _safe():
        try:
            await coro
        except Exception:
            logger.warning(f"[CHAT] Fire-and-forget task failed: {name}", exc_info=True)
        finally:
            _background_tasks.discard(t)
    t = asyncio.create_task(_safe(), name=name or None)
    _background_tasks.add(t)
