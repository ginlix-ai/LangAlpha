"""A per-worker loop that runs one sweep pass at a jittered interval.

Every worker runs its own; a pass claims its rows in Postgres, so running it
everywhere at once is safe, and the jitter keeps workers started by one
deploy from all claiming at the same moment.
"""

from __future__ import annotations

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar, Self

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

STOP_GRACE = 30.0


class JitteredSweeper(ABC):
    """Subclasses name themselves, set ``interval`` and write ``sweep_once``;
    ``enabled`` keeps the loop from starting where it has nothing to do."""

    name: ClassVar[str]
    interval: ClassVar[float]
    _instance: ClassVar[JitteredSweeper | None] = None

    def __init__(self) -> None:
        self._loop_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> Self:
        if cls.__dict__.get("_instance") is None:
            cls._instance = cls()
        return cls._instance  # type: ignore[return-value]

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    @abstractmethod
    async def sweep_once(self) -> None: ...

    def enabled(self) -> bool:
        return True

    def start(self) -> None:
        if not self.enabled():
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop(), name=self.name)
        logger.info(f"[{self.name}] started (interval={self.interval:.0f}s)")

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning(f"[{self.name}] loop ended with an error at shutdown", exc_info=True)
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            # Jitter first, so workers started by one deploy do not all claim
            # at once.
            jitter = self.interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(f"[{self.name}] sweep failed", exc_info=True)


class KeysetCursor:
    """Where a sweep's next pass resumes in a list ordered by ``computer_id``,
    so rows that fail on every pass cannot fill each page and keep the ones
    sorted after them waiting. Per worker: what keeps workers off one row is
    the sweep's own lock, not this."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.after: Any = None
        self._start: Any = None

    async def page(self, fetch: Callable[..., Awaitable[list[dict[str, Any]]]]) -> list[dict[str, Any]]:
        """The next page *fetch* returns, called with ``limit`` and ``after``.
        Past the end it wraps at once rather than lose a pass."""
        start = self.after
        rows = await fetch(limit=self.limit, after=start)
        if not rows and start is not None:
            start = None
            rows = await fetch(limit=self.limit, after=None)
        self._start = start
        self.after = rows[-1]["computer_id"] if len(rows) == self.limit else None
        return rows

    def stopped_at(self, rows: list[dict[str, Any]], index: int) -> None:
        """The pass ended before handling ``rows[index]`` of the last page, so
        the next one starts there rather than past the rows it never reached."""
        self.after = rows[index - 1]["computer_id"] if index else self._start

    def reset(self) -> None:
        self.after = None
