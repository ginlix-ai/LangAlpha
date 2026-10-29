"""Background starts answered with a 202, tracked until they settle.

asyncio holds only weak references to tasks, so a fire-and-forget
``create_task`` whose handle goes out of scope can be garbage-collected
mid-flight, after the row was already claimed 'starting'. The registry here
holds the strong reference, refuses a second start for a key whose first is
still running, and is drained at shutdown so a cancelled start gets to revert
its claim instead of being torn down with the loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)

# Keyed by the caller's own identity for the thing being started (a project or
# a machine); the key is the dedup unit, not something this module derives.
_start_tasks: dict[str, asyncio.Task] = {}


def _forget(key: str, task: asyncio.Task) -> None:
    # Only the task that owns the slot may clear it: a start scheduled after
    # this one finished must not be evicted by a late callback.
    if _start_tasks.get(key) is task:
        del _start_tasks[key]


def _log_failure(key: str, task: asyncio.Task) -> None:
    """A create_task failure has no awaiter, so it is logged here or nowhere."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.exception("Background start %s failed: %s", key, exc, exc_info=exc)


def schedule_start(key: str, start: Callable[[], Awaitable[object]]) -> asyncio.Task:
    """Run ``start()`` in the background, once per key while it is in flight.

    Takes a factory rather than a coroutine so a deduped call never leaves an
    un-awaited coroutine behind. Returns the task that owns the key, which is
    the earlier one when the call was deduped.
    """
    live = _start_tasks.get(key)
    if live is not None and not live.done():
        logger.info("Background start %s already in flight; not scheduling twice", key)
        return live

    task = asyncio.create_task(start(), name=f"start-{key}")
    _start_tasks[key] = task
    task.add_done_callback(lambda t, k=key: _forget(k, t))
    task.add_done_callback(lambda t, k=key: _log_failure(k, t))
    return task


async def drain_start_tasks() -> None:
    """Cancel and await every tracked start; call before the runtime shuts down.

    A start cancelled mid-flight runs its own CancelledError revert and hands
    the 'starting' claim back. Torn down abruptly with the loop, that revert
    may never land and the row stays wedged until the next process reaps it.
    """
    if not _start_tasks:
        return
    tasks = list(_start_tasks.values())
    logger.info("Draining %d in-flight background start task(s)", len(tasks))
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
