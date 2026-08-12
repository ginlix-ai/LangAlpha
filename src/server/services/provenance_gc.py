"""Periodic GC for the content-addressed provenance body store.

Runs a daily mark-and-sweep that deletes ``provenance_result_bodies`` rows whose
``result_sha256`` is no longer referenced by any ``provenance_records`` row and
that are past the grace window, plus their spilled objects. The DELETE + object
cleanup lives in ``sweep_orphan_bodies``; this service just schedules it on an
interval and survives per-cycle failures.
"""

from __future__ import annotations

import asyncio
import logging

from src.server.database.provenance_bodies import GC_GRACE_DAYS, sweep_orphan_bodies

logger = logging.getLogger(__name__)

# Once per day. The sweep is cheap (indexed NOT EXISTS) and bodies churn slowly,
# so a tighter cadence buys nothing.
_DEFAULT_INTERVAL_SECONDS = 86400
# Grace window before an unreferenced body is reaped, so a body written mid-turn
# isn't swept before its provenance row commits. Sourced from provenance_bodies
# so the sweep grace and the reuse-touch window stay coupled (see GC_GRACE_DAYS).
_DEFAULT_GRACE_DAYS = GC_GRACE_DAYS


class ProvenanceGCService:
    """Singleton background sweeper for orphaned provenance result bodies."""

    _instance: ProvenanceGCService | None = None

    @classmethod
    def get_instance(cls) -> ProvenanceGCService:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(
        self,
        interval_seconds: int = _DEFAULT_INTERVAL_SECONDS,
        grace_days: int = _DEFAULT_GRACE_DAYS,
    ) -> None:
        self._interval = interval_seconds
        self._grace_days = grace_days
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Launch the sweep loop (no-op if already running)."""
        if self._task and not self._task.done():
            return
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self._loop(), name="provenance_gc_sweep")
        logger.info(
            "[ProvenanceGC] started — sweep every %ds, grace=%dd",
            self._interval,
            self._grace_days,
        )

    async def stop(self) -> None:
        """Signal shutdown and cancel the sweep loop."""
        self._shutdown_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[ProvenanceGC] stopped")

    async def _loop(self) -> None:
        """Sweep on start, then every interval — one failed cycle never kills the loop.

        Sweeping first (rather than after a full interval) means a process that
        restarts more often than ``_interval`` — frequent deploys, ``--reload`` —
        still runs a sweep each start instead of never reaching the first one. The
        grace window in ``sweep_orphan_bodies`` keeps a startup sweep from reaping
        bodies written just before this boot.
        """
        while not self._shutdown_event.is_set():
            try:
                deleted = await sweep_orphan_bodies(grace_days=self._grace_days)
                if deleted:
                    logger.info("[ProvenanceGC] swept %d orphan body row(s)", deleted)
            except Exception:
                logger.error("[ProvenanceGC] sweep cycle failed", exc_info=True)

            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self._interval
                )
                return  # shutdown requested during the sleep
            except asyncio.TimeoutError:
                pass
