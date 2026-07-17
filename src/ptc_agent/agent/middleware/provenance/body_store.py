"""Shared background writer for content-addressed provenance result bodies.

Both ``ProvenanceMiddleware`` and ``MarketWatchMiddleware`` persist result bodies
to the global content-addressed store OFF their critical path. This module owns
the ``src.server.database.provenance_bodies`` import, the bounded-concurrency task
lifecycle, and exception consumption so neither middleware reimplements it. The
task set is caller-owned (passed in), so each middleware instance keeps its own —
concurrent subagents on a shared instance can't mix data.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable

logger = logging.getLogger(__name__)

# Max concurrent in-flight background body writes per task set. Backpressure
# valve: once this many are outstanding, schedule_body_write waits for one to
# finish before starting the next, so a burst of source-bearing calls can't spawn
# unbounded tasks or exhaust the DB pool. Keep below the pool's comfortable
# concurrency.
BODY_WRITE_TASK_LIMIT = 16

# Ceiling on any inline wait for in-flight writes. Pool-acquire failures raise
# (and are swallowed), but a wedged connection can hang a write far longer than
# any exception path — the store is best-effort, so neither the tool/model path
# (schedule) nor turn-end (drain) may block behind a stall.
BODY_WRITE_WAIT_TIMEOUT = 10.0


async def store_bodies(items: list[tuple]) -> None:
    """Batch-write ``(sha, body, byte_len, content_type)`` rows (never raises).

    ``store_result_bodies`` dedupes by sha and upserts in one connection; a lost
    body must not break the turn.
    """
    if not items:
        return
    try:
        from src.server.database.provenance_bodies import store_result_bodies

        await store_result_bodies(items)
    except Exception as e:
        logger.warning(
            "[PROVENANCE] store_result_bodies failed (%d items): %s", len(items), e
        )


async def store_body(
    sha256: str, body: str, byte_len: int, content_type: str
) -> None:
    """Write one body row to the content-addressed store (never raises)."""
    try:
        from src.server.database.provenance_bodies import store_result_body

        await store_result_body(sha256, body, byte_len, content_type)
    except Exception as e:
        logger.warning("[PROVENANCE] store_result_body failed: %s", e)


def _consume(task: asyncio.Task[None]) -> None:
    """Retrieve a finished write's result so its exception is never orphaned."""
    try:
        task.result()
    except asyncio.CancelledError:
        logger.debug("[PROVENANCE] background body write cancelled")
    except Exception:
        # store_bodies/store_body already swallow; belt-and-suspenders so a
        # background task can't surface as "exception never retrieved".
        logger.warning("[PROVENANCE] background body write failed", exc_info=True)


def _reap(tasks: set[asyncio.Task[None]]) -> None:
    for task in tuple(tasks):
        if task.done():
            tasks.discard(task)
            _consume(task)


def _on_done(tasks: set[asyncio.Task[None]], task: asyncio.Task[None]) -> None:
    tasks.discard(task)
    _consume(task)


async def schedule_body_write(
    tasks: set[asyncio.Task[None]],
    coro: Awaitable[None],
    *,
    max_tasks: int = BODY_WRITE_TASK_LIMIT,
    name: str = "provenance_body_flush",
) -> None:
    """Run ``coro`` as a tracked background write on the caller-owned ``tasks`` set.

    Scheduled OFF the caller's critical path so the tool result / model call
    returns without waiting on the DB write. Bounded by ``max_tasks``: when
    saturated, waits for one in-flight write to drain before scheduling — the only
    inline wait on the common path.
    """
    _reap(tasks)
    while len(tasks) >= max_tasks:
        done, _ = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
            timeout=BODY_WRITE_WAIT_TIMEOUT,
        )
        if not done:
            # Every in-flight write is stalled. Piling more onto a stuck pool
            # is worse than losing one best-effort body — drop this write.
            logger.warning(
                "[PROVENANCE] body-write backpressure stalled for %.0fs; "
                "dropping one body write",
                BODY_WRITE_WAIT_TIMEOUT,
            )
            if hasattr(coro, "close"):
                coro.close()  # never-scheduled coroutine would warn at GC
            return
        for task in done:
            tasks.discard(task)
            _consume(task)
    task = asyncio.create_task(coro, name=name)
    tasks.add(task)
    task.add_done_callback(lambda t: _on_done(tasks, t))


async def drain_body_writes(tasks: set[asyncio.Task[None]]) -> None:
    """Await writes already scheduled at entry (idempotent best-effort).

    Snapshots the tracked tasks and awaits them; ``shield`` keeps an in-flight DB
    write from being cancelled if this drain is itself cancelled (e.g. a hard turn
    stop) mid-transaction. Writes scheduled after entry stay tracked for the next
    drain.
    """
    _reap(tasks)
    pending = tuple(tasks)
    if not pending:
        return
    try:
        await asyncio.wait_for(
            asyncio.gather(
                *(asyncio.shield(task) for task in pending), return_exceptions=True
            ),
            timeout=BODY_WRITE_WAIT_TIMEOUT,
        )
    except TimeoutError:
        # Turn-end must not hang the SSE close behind a wedged write; the
        # shielded tasks stay tracked and get consumed by their done callbacks.
        logger.warning(
            "[PROVENANCE] body-write drain timed out after %.0fs (%d pending)",
            BODY_WRITE_WAIT_TIMEOUT,
            len(pending),
        )
    finally:
        _reap(tasks)
