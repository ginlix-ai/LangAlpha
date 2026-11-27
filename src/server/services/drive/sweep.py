"""Keeps each Drive space within its rules (``drive.maintain``: the trash's
days, undo copies' days, released copies' grace, the trash's bound), and ends
uploads in parts that were left.

Every worker runs the loop; a pass runs where it takes the session lock, so
several workers make one pass between them. Each space is kept up in its own
transaction under its row lock, as a person's change is.
"""

from __future__ import annotations

import asyncio
import logging

from src.config.core import get_infrastructure_config
from src.server.database.drive import reads
from src.server.database.session_lock import try_session_lock
from src.server.services import drive
from src.server.services.drive import uploads

logger = logging.getLogger(__name__)

_LOCK_KEY = "drive:trash_sweep"
_INTERVAL_SECONDS = 3600
#: Spaces one pass purges; a larger backlog waits for the next pass.
_SPACES_PER_PASS = 500


async def sweep_once() -> int | None:
    """One pass; the spaces kept up, or None when another process holds the pass."""
    async with try_session_lock(_LOCK_KEY) as held:
        if not held:
            return None
        days = get_infrastructure_config().drive.trash_days
        spaces = await reads.spaces_to_maintain(
            days, drive.history_rule().grace_seconds, _SPACES_PER_PASS
        )
        for space_id in spaces:
            try:
                await drive.maintain(space_id)
            except Exception:
                logger.warning("[DriveTrash] upkeep of space %s failed", space_id, exc_info=True)
        try:
            await uploads.sweep_once()
        except Exception:
            logger.warning("[DriveTrash] upload sweep failed", exc_info=True)
        return len(spaces)


class DriveTrashSweep:
    _instance: DriveTrashSweep | None = None

    @classmethod
    def get_instance(cls) -> DriveTrashSweep:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self, interval_seconds: int = _INTERVAL_SECONDS) -> None:
        self._interval = interval_seconds
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._shutdown.clear()
        self._task = asyncio.create_task(self._loop(), name="drive_trash_sweep")

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                kept = await sweep_once()
                if kept:
                    logger.info("[DriveTrash] kept up %d spaces", kept)
            except Exception:
                logger.error("[DriveTrash] sweep pass failed", exc_info=True)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self._interval)
                return
            except asyncio.TimeoutError:
                pass
