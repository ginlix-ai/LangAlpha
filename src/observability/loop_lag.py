"""Event-loop lag heartbeat.

A blocked event loop and a sick Redis are indistinguishable from the client
side: both surface as ``Timeout reading from redis`` because redis-py's socket
reader never gets scheduled to drain the reply. This sampler makes the
difference observable — if lag spikes alongside the timeout, the fault is
in-process (CPU-bound work on the loop), not on the wire.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

# Sampling cadence. Short enough to catch the sub-second stalls that precede a
# 5s socket timeout, cheap enough to leave running in production.
_INTERVAL_S = 0.5

# A stall this long is already enough to trip short Redis deadlines (the
# subagent spill uses 0.5s), so it is worth a line in the log on its own.
_WARN_THRESHOLD_MS = 1000.0

# Re-log suppression: a sustained stall would otherwise emit a line per sample.
_WARN_COOLDOWN_S = 10.0


class EventLoopLagMonitor:
    """Per-worker singleton sampling how late a fixed-interval sleep returns."""

    _instance: Optional["EventLoopLagMonitor"] = None

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        self._last_warn = 0.0
        self.max_lag_ms = 0.0

    @classmethod
    def get_instance(cls) -> "EventLoopLagMonitor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="event-loop-lag")

    async def stop(self) -> None:
        self._stopping.set()
        task, self._task = self._task, None
        if task is not None:
            await cancel_and_join(task)

    async def _run(self) -> None:
        from src.observability.metrics import event_loop_lag_ms

        while not self._stopping.is_set():
            before = time.monotonic()
            try:
                await asyncio.sleep(_INTERVAL_S)
            except asyncio.CancelledError:
                return
            lag_ms = (time.monotonic() - before - _INTERVAL_S) * 1000.0
            if lag_ms < 0:
                lag_ms = 0.0
            self.max_lag_ms = max(self.max_lag_ms, lag_ms)
            try:
                event_loop_lag_ms.record(lag_ms)
            except Exception:
                pass
            if lag_ms >= _WARN_THRESHOLD_MS:
                now = time.monotonic()
                if now - self._last_warn >= _WARN_COOLDOWN_S:
                    self._last_warn = now
                    logger.warning(
                        "[loop-lag] event loop stalled %.0f ms "
                        "(sample interval %.0f ms) — Redis/SSE timeouts in this "
                        "window are in-process, not upstream",
                        lag_ms,
                        _INTERVAL_S * 1000.0,
                    )
