"""The sweep that keeps conversation search's index whole.

A turn's indexing at settle is fire-and-forget in the process that ran it,
so a worker lost or a deploy loses it. Every ``interval_seconds`` a pass looks
at the turns settled in the last ``RECENT`` for any left stale (no row, or
one read from another response) and indexes them and the turn before each (a
turn's answered interrupts reach its text at the next turn's resume). That
window is all a pass reads once nothing is missing.

An answer whose read failed is retried from its row (``due_retries``), the
row holding its failures and when it is due, however long ago its turn
settled, until the database layer's cap gives up on it. Each pass's retries
and the walk's slice share one budget of work.

The deployment also walks every thread, newest created first: the backfill
for turns that settled before search existed, and after it, every
``REWALK_S``, the slow safety net for anything the turn end and the window
missed. The walk's place is a row in the database (``thread_search_walk``),
so the workers walk once between them. It goes a slice at a time: retries
and pages until ``WALK_SLICE_S`` of work, then the slice records where it
stopped and that no slice may run before it has rested as long as it worked
(``pause_seconds`` at least, ``MAX_REST_S`` at most), which every worker
honours, so a backfill never holds the database, the checkpoint reader or
the lock for long.

Every worker runs the loop; each pass runs under a session advisory lock on
a connection of its own, so one worker sweeps at a time and the sweep never
holds a pool slot through its reads.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Optional

from src.utils.concurrency import cancel_and_join

if TYPE_CHECKING:
    from src.config.models import ThreadSearchConfig
    from src.server.database.search.index import Walk

logger = logging.getLogger(__name__)

LOCK_KEY = "thread_search_sweep"
STOP_GRACE = 30.0
# How far back a pass looks for turns settled and left stale, at least: the
# sweep's interval several times over, so a pass that failed or a worker
# that held the lock and died is covered by the next.
RECENT = timedelta(hours=1)
# The work one slice (retries and walk pages) does before it lets the lock go.
WALK_SLICE_S = 10.0
# The longest rest between slices, however long the slice took.
MAX_REST_S = 60.0
# How long after a walk reaches its end the next one starts.
REWALK_S = 24 * 3600.0
# A thread whose indexing raised is passed over by the window's pass for
# the interval, doubled at each failure, up to this. In this process only:
# the walk reaches it again anyway.
MAX_RAISED_BACKOFF_S = 3600.0


@dataclass
class _Raised:
    failures: int
    until: float


@dataclass
class _Tally:
    written: int = 0
    threads: int = 0


class ThreadSearchSweeper:
    _instance: Optional["ThreadSearchSweeper"] = None

    def __init__(self) -> None:
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        # Execution context only, this process's alone: when its window pass
        # last ran, and the threads whose indexing raised.
        self._recent_at: float | None = None
        self._raised: dict[str, _Raised] = {}

    @classmethod
    def get_instance(cls) -> "ThreadSearchSweeper":
        if cls._instance is None:
            cls._instance = ThreadSearchSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    def start(self) -> None:
        from src.config.settings import get_thread_search_config

        config = get_thread_search_config()
        if not config.enabled:
            logger.info("[ThreadSearchSweeper] not started: thread_search.enabled is off")
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop(), name="thread-search-sweeper")
        logger.info(
            f"[ThreadSearchSweeper] started (interval={config.interval_seconds}s, "
            f"batch={config.batch_threads} threads)"
        )

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning(
                "[ThreadSearchSweeper] loop ended with an error at shutdown", exc_info=True
            )
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        from src.config.settings import get_thread_search_config

        while not self._stop_event.is_set():
            config = get_thread_search_config()
            interval = config.interval_seconds * (0.8 + 0.4 * random.random())
            rest = interval
            try:
                walk_wait = await self._sweep(config)
                if walk_wait is not None:
                    # Wake for the walk's next slice, if sooner, a little
                    # after it so the workers do not all ask at once.
                    walk_wait = max(config.pause_seconds, walk_wait)
                    rest = min(interval, walk_wait * (1 + 0.2 * random.random()))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[ThreadSearchSweeper] sweep failed", exc_info=True)
            if await self._wait(rest):
                return

    async def _wait(self, seconds: float) -> bool:
        """Sleep ``seconds``, or less if stopped; True when stopped."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
            return True
        except TimeoutError:
            return False

    async def sweep_once(self) -> int:
        """One pass, if no other worker is sweeping; returns the turns
        written."""
        from src.config.settings import get_thread_search_config

        tally = _Tally()
        await self._sweep(get_thread_search_config(), tally)
        return tally.written

    async def _sweep(
        self, config: ThreadSearchConfig, tally: _Tally | None = None
    ) -> float | None:
        """A pass, if no other worker holds the lock. Returns the seconds
        until the walk's next slice may run."""
        from src.server.database.search import index as db
        from src.server.database.session_lock import try_own_session_lock

        async with try_own_session_lock(LOCK_KEY) as held:
            if held:
                return await self._pass(config, tally or _Tally())
        # Another worker is sweeping; a due slice is likely its own, so look
        # again after one slice's length.
        walk = await db.read_walk()
        return walk.wait_s or WALK_SLICE_S * (0.8 + 0.4 * random.random())

    async def _pass(self, config: ThreadSearchConfig, tally: _Tally) -> float:
        from src.server.database.search import index as db

        now = time.monotonic()
        self._raised = {
            tid: r for tid, r in self._raised.items() if r.until > now - MAX_RAISED_BACKOFF_S
        }
        # The window's pass on its own interval, though slices come sooner.
        if self._recent_at is None or now - self._recent_at >= config.interval_seconds:
            self._recent_at = now
            await self._recent(config, tally)
        # The slice: due retries, then the walk's pages if its rest is over,
        # within one budget.
        started = time.monotonic()
        deadline = started + WALK_SLICE_S
        await self._retry(config, tally, deadline)
        walk = await db.read_walk()
        if walk.wait_s > 0 or self._stop_event.is_set():
            self._log(tally)
            return walk.wait_s
        after, finished = await self._walk(walk, config, tally, deadline)
        if finished:
            rest = REWALK_S
            logger.info("[ThreadSearchSweeper] walked every thread")
        else:
            spent = time.monotonic() - started
            rest = min(max(config.pause_seconds, spent), MAX_REST_S)
        await db.save_walk(after, rest_s=rest, finished=finished)
        self._log(tally)
        return rest

    def _log(self, tally: _Tally) -> None:
        if tally.threads:
            logger.info(
                f"[ThreadSearchSweeper] indexed {tally.written} turn(s) in "
                f"{tally.threads} thread(s)"
            )
            tally.threads = 0

    async def _recent(self, config: ThreadSearchConfig, tally: _Tally) -> None:
        """Index the stale turns among those settled in the last ``RECENT``."""
        from src.server.database.search import index as db

        window = max(RECENT, timedelta(seconds=3 * config.interval_seconds))
        since = datetime.now(UTC) - window
        after = None
        while not self._stop_event.is_set():
            batch = await db.stale_since(since, limit=config.batch_threads, after=after)
            for thread in batch:
                if self._stop_event.is_set():
                    return
                await self._index(thread.thread_id, thread.turns, tally)
                after = thread.thread_id
            if len(batch) < config.batch_threads or await self._wait(config.pause_seconds):
                return

    async def _retry(
        self, config: ThreadSearchConfig, tally: _Tally, deadline: float
    ) -> None:
        """Read again the unread answers whose retry is due, until
        ``deadline``. Each read moves its rows' retry on or clears it."""
        from src.server.database.search import index as db
        from src.server.services.search.index import index_turns

        tried: set[str] = set()
        while not self._stop_event.is_set() and time.monotonic() < deadline:
            due = [
                t for t in await db.due_retries(limit=config.batch_threads)
                if t.thread_id not in tried
            ]
            if not due:
                return
            for thread in due:
                if self._stop_event.is_set() or time.monotonic() >= deadline:
                    return
                tried.add(thread.thread_id)
                try:
                    indexed = await index_turns(
                        thread.thread_id, turn_indexes=thread.turns
                    )
                except Exception as e:
                    logger.warning(
                        "[ThreadSearchSweeper] retry of thread %s failed: %s: %s",
                        thread.thread_id,
                        type(e).__name__,
                        e,
                    )
                    await db.defer_retries(thread.thread_id, thread.turns)
                    continue
                tally.written += indexed.written
                tally.threads += 1

    async def _walk(
        self,
        walk: Walk,
        config: ThreadSearchConfig,
        tally: _Tally,
        deadline: float,
    ) -> tuple[tuple[datetime, str] | None, bool]:
        """Walk on from where the last slice stopped, indexing the stale
        turns found, until ``deadline`` (a page at least). Returns where it
        stopped and whether that is the end."""
        from src.server.database.search import index as db

        after = walk.after
        while not self._stop_event.is_set():
            page = await db.walk_threads(limit=config.batch_threads, after=after)
            for thread in page:
                if self._stop_event.is_set():
                    return after, False
                if thread.turns:
                    await self._index(thread.thread_id, thread.turns, tally)
                after = thread.key
            if len(page) < config.batch_threads:
                return None, True
            if time.monotonic() >= deadline:
                break
        return after, False

    async def _index(self, thread_id: str, turns: list[int], tally: _Tally) -> None:
        """Index a thread's stale turns and the turn before each."""
        from src.config.settings import get_thread_search_config
        from src.server.services.search.index import index_turns

        raised = self._raised.get(thread_id)
        if raised is not None and time.monotonic() < raised.until:
            return
        wanted = set(turns) | {t - 1 for t in turns if t > 0}
        try:
            indexed = await index_turns(thread_id, turn_indexes=wanted)
        except Exception as e:
            failures = raised.failures + 1 if raised else 1
            interval = get_thread_search_config().interval_seconds
            delay = min(interval * 2 ** (failures - 1), MAX_RAISED_BACKOFF_S)
            self._raised[thread_id] = _Raised(failures, time.monotonic() + delay)
            logger.warning(
                "[ThreadSearchSweeper] thread %s not indexed (failure %d, passed "
                "over for %.0fs): %s: %s",
                thread_id,
                failures,
                delay,
                type(e).__name__,
                e,
                # The traceback once; a repeat says only that it still fails.
                exc_info=failures == 1,
            )
            return
        self._raised.pop(thread_id, None)
        tally.written += indexed.written
        tally.threads += 1
