"""The clock side of wakeups: fire what has come due, in every worker.

Each pass consumes the timer slots that are due, claims the lapse reminders
of price wakeups, closes the price and order wakeups whose window passed,
and fires any order wakeup whose order filled or ended through a writer
other than the reconciler. Every one of those is a guarded UPDATE that
enqueues its notice in the same transaction, so every worker sweeps at once
and none doubles another's fire; nothing is kept between passes, and a
sweeper that is down only makes a fire late (a late pass consumes the slots
it missed, the middle ones as skips).
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Optional

logger = logging.getLogger(__name__)

_LOG = "[WakeupSweeper]"

INTERVAL = 10.0
# Spread so the workers' passes do not land on the database together.
JITTER = 2.0
_LAPSE_BATCH = 50
STOP_GRACE = 10.0


def _nudge_hook_drainer() -> None:
    """Post-commit hint that a notice was enqueued; the drainer's poll in
    every worker is what delivers it, so a failed nudge is not an error."""
    try:
        from src.server.services.hook_outbox import HookOutboxDrainer

        HookOutboxDrainer.get_instance().nudge()
    except Exception:
        logger.debug(f"{_LOG} hook drainer nudge failed", exc_info=True)


class WakeupSweeper:
    _instance: Optional["WakeupSweeper"] = None

    @classmethod
    def get_instance(cls) -> "WakeupSweeper":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._loop(), name="wakeup-sweeper")

    async def stop(self) -> None:
        self._stopping.set()
        task, self._task = self._task, None
        if task is None:
            return
        try:
            await asyncio.wait_for(task, timeout=STOP_GRACE)
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning(f"{_LOG} exceeded stop grace")
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning(f"{_LOG} raised during stop", exc_info=True)

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=INTERVAL + random.uniform(0, JITTER)
                )
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(f"{_LOG} pass failed", exc_info=True)

    async def sweep_once(self) -> dict[str, int]:
        """One pass; how many slots fired and were skipped, lapse reminders
        claimed, wakeups closed, and order wakeups fired. Each step stands
        alone, so a failure in one leaves the others to run."""
        from src.server.database.wakeups import fire as fire_db

        counts = {"fired": 0, "skipped": 0, "lapses": 0, "expired": 0, "orders": 0}
        try:
            slots = await fire_db.fire_due_slots()
            counts["fired"] = sum(1 for row in slots if row.get("fired"))
            counts["skipped"] = len(slots) - counts["fired"]
        except Exception:
            logger.error(f"{_LOG} timer slots failed", exc_info=True)
        try:
            while True:
                claimed = await fire_db.claim_due_lapses(limit=_LAPSE_BATCH)
                counts["lapses"] += len(claimed)
                if len(claimed) < _LAPSE_BATCH:
                    break
        except Exception:
            logger.error(f"{_LOG} lapse reminders failed", exc_info=True)
        try:
            counts["expired"] = await fire_db.expire_due_wakeups()
        except Exception:
            logger.error(f"{_LOG} closing lapsed wakeups failed", exc_info=True)
        try:
            # The backstop for a fire the reconciler's own write could not
            # make: a wakeup set while its observation committed, or a fire
            # its savepoint lost.
            counts["orders"] = len(await fire_db.fire_order_wakeups())
        except Exception:
            logger.error(f"{_LOG} order wakeups failed", exc_info=True)

        if counts["fired"] or counts["lapses"] or counts["orders"]:
            _nudge_hook_drainer()
        if any(counts.values()):
            logger.info(
                f"{_LOG} fired={counts['fired']} skipped={counts['skipped']} "
                f"lapses={counts['lapses']} expired={counts['expired']} "
                f"orders={counts['orders']}"
            )
        return counts
