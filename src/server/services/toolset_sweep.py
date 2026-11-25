"""Background sweep bringing running computers' tools to the server's table.

The acquire converges a computer when a turn or a start reaches it, but an
always-on computer's warm session rarely acquires again, so without this a
tool update would wait for its next restart. Each pass lists running
computers of the kinds the server converges whose row is not stamped at the
table's version, a page at a time past the last one it saw, and converges them.

A converge ends no process and swaps each tool in atomically, so it waits for
no gap between turns and takes no turn's lock. It holds the computer's decision
lock only to see that nothing (a delete, a move, a scrub) is deciding about the
machine as it begins, never across the converge: a download can take minutes,
and those decisions wait at most half a minute for the lock before giving up.
The sweep and the acquire path take one Redis lease per computer and wait out
a failure on one shared clock (``toolset.RetryClock``); the machine's own lock
would make a second converge a no-op anyway, so the lease only saves the
duplicate work.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Final

from src.server.services import toolset
from src.server.services.sweep_loop import JitteredSweeper, KeysetCursor

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S: Final = 300.0
SWEEP_BATCH_LIMIT: Final = 25


class ToolsetSweeper(JitteredSweeper):
    name = "ToolsetSweeper"
    interval = SWEEP_INTERVAL_S

    def __init__(self) -> None:
        super().__init__()
        # Per worker, like the platform-secret sweep's: the lease keeps
        # workers off one machine.
        self._cursor = KeysetCursor(SWEEP_BATCH_LIMIT)

    async def sweep_once(self) -> None:
        from src.server.database.computer import (
            list_computers_behind_toolset,
            list_kinds_behind_toolset,
        )

        clock = toolset.RetryClock()
        if not clock.available:
            # No lease to keep workers off one machine; the acquire still
            # converges a computer a turn reaches.
            logger.debug(f"[{self.name}] Redis is off; skipping")
            return
        want = toolset.target()
        kinds = [
            kind
            for kind in await list_kinds_behind_toolset(want)
            if await asyncio.to_thread(toolset.kind_served, kind)
        ]
        if not kinds:
            self._cursor.reset()
            return
        rows = await self._cursor.page(functools.partial(list_computers_behind_toolset, want, kinds))
        for index, row in enumerate(rows):
            if self._stop_event.is_set():
                self._cursor.stopped_at(rows, index)
                break
            if not await self._sweep_one(clock, row):
                logger.warning(f"[{self.name}] Redis did not answer the lease; ending the pass")
                self._cursor.stopped_at(rows, index)
                break

    async def _sweep_one(self, clock: toolset.RetryClock, row: dict[str, Any]) -> bool:
        """Converge one computer and stamp it, when its wait is up and no one
        else is converging it. False when Redis did not answer, which ends the
        pass rather than converge a machine another worker may be converging
        too."""
        from src.server.services.data_token_sweep import keeps_running

        computer_id = str(row["computer_id"])
        # The reaper's own reading, so a computer it is about to stop for being
        # idle is not kept awake at the provider by a download.
        last_activity = row.get("last_activity_at") or row.get("updated_at")
        if not await keeps_running({**row, "last_activity": last_activity}):
            return True
        async with clock.claim(computer_id) as claim:
            if claim.verdict is toolset.Verdict.NO_REDIS:
                return False
            # Otherwise not due yet, or another holder is converging it.
            if claim.verdict is toolset.Verdict.GO:
                await self._converge_claimed(claim, row)
        return True

    async def _converge_claimed(self, claim: toolset.Claim, row: dict[str, Any]) -> None:
        from src.server.database.computer import computer_advisory_key
        from src.server.database.session_lock import try_session_lock
        from src.server.services.data_token_sweep import running_machine

        computer_id = claim.computer_id
        async with try_session_lock(computer_advisory_key(computer_id)) as free:
            if not free:
                return  # something is deciding about the machine
        try:
            async with running_machine(row) as machine:
                if machine is None:
                    # The row lags a stop; the stop path or the next pass
                    # settles it. Not started for this.
                    return
                runtime, internal_src = machine
                outcome = await toolset.converge_and_stamp(
                    runtime, internal_src, computer_id, str(row["provider_ref"])
                )
        except Exception as exc:
            wait = await claim.settle(None) or 0.0
            logger.warning(
                f"[{self.name}] converge failed for computer {computer_id} "
                f"(error_type={type(exc).__name__}); retrying in {wait / 60:.0f} min"
            )
            return
        wait = await claim.settle(outcome) or 0.0
        if outcome is toolset.Outcome.BEHIND:
            logger.warning(f"[{self.name}] computer {computer_id} retrying in {wait / 60:.0f} min")
        elif outcome is toolset.Outcome.CURRENT:
            logger.info(f"[{self.name}] converged computer {computer_id} to {toolset.target()}")
