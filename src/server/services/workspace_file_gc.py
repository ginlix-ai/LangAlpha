"""Periodic garbage collection for workspace file blobs.

Schedules :func:`sweep_blob_garbage` on an interval and survives per-cycle
failures. The protocol that makes deletion safe lives with the registry in
:mod:`src.server.database.workspace_file_blobs`; this service only paces it.
"""

from __future__ import annotations

import asyncio
import logging

from src.server.database.workspace_file_blobs import sweep_blob_garbage
from src.utils.storage import is_storage_enabled

logger = logging.getLogger(__name__)

# Four cycles a day at GC_REAP_BATCH objects each clears far more than any
# plausible churn; a tighter cadence only adds store calls.
_DEFAULT_INTERVAL_SECONDS = 6 * 3600


class WorkspaceFileGCService:
    """Singleton background sweeper for orphaned workspace file blobs."""

    _instance: WorkspaceFileGCService | None = None

    @classmethod
    def get_instance(cls) -> WorkspaceFileGCService:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self, interval_seconds: int = _DEFAULT_INTERVAL_SECONDS) -> None:
        self._interval = interval_seconds
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Launch the sweep loop; inert when no object store is configured."""
        if self._task and not self._task.done():
            return
        if not is_storage_enabled():
            logger.info("[WorkspaceFileGC] no object store configured; not started")
            return
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self._loop(), name="workspace_file_gc_sweep")
        logger.info("[WorkspaceFileGC] started — sweep every %ds", self._interval)

    async def stop(self) -> None:
        """Cancel the sweep and wait, untimed, for it to unwind.

        A reap in flight holds a row lock across an object delete it cannot
        interrupt, and keeps the lock until the delete settles. Bounding this
        await would deliver a second cancellation into that wait, rolling the
        transaction back with the delete still running, which is the race the
        shield in ``_reap_one`` exists to close.
        """
        self._shutdown_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[WorkspaceFileGC] stopped")

    async def _loop(self) -> None:
        """Sweep on start, then every interval; one failed cycle never kills the loop.

        Sweeping first means a process that restarts more often than the
        interval still sweeps; the grace periods keep a startup sweep from
        touching anything recent.
        """
        while not self._shutdown_event.is_set():
            try:
                stats = await sweep_blob_garbage()
                if stats is None:
                    logger.debug("[WorkspaceFileGC] sweep skipped, lock held elsewhere")
                elif any(stats.values()):
                    logger.info(
                        "[WorkspaceFileGC] condemned %d, deleted %d, failed %d",
                        stats["condemned"],
                        stats["deleted"],
                        stats["failed"],
                    )
            except Exception:
                logger.error("[WorkspaceFileGC] sweep cycle failed", exc_info=True)

            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=self._interval)
                return
            except asyncio.TimeoutError:
                pass
