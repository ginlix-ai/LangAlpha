"""Drive rounds nobody is driving: queued, or running with a lapsed lease.

Round progress is saved after each slot, so resuming is just claiming the row
again and walking from its cursor. A round already being driven cannot be
claimed twice (the guarded UPDATE), which is what keeps two workers from
posting the same slot.
"""

from __future__ import annotations

import asyncio
import logging

from src.server.database import rooms as room_db

logger = logging.getLogger(__name__)

SCAN_INTERVAL_SECONDS = 15.0


class RoomRoundSweeper:
    """The per-worker loop that resumes rounds no worker is driving."""

    _instance: "RoomRoundSweeper | None" = None

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stopping = False

    @classmethod
    def get_instance(cls) -> "RoomRoundSweeper":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="room-round-sweeper")
        logger.info("RoomRoundSweeper started")

    async def stop(self) -> None:
        self._stopping = True
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                if self._stopping:
                    return
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[ROOM] sweeper pass failed: %s", exc)

    async def sweep_once(self) -> int:
        from src.server.services.rooms.coordinator import drive_round

        rows = await room_db.due_rounds()
        for row in rows:
            try:
                await drive_round(row["round_id"])
            except Exception as exc:
                logger.warning("[ROOM] resume of %s failed: %s", row["round_id"], exc)
        return len(rows)
