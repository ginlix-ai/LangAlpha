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
from typing import ClassVar, Self

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
