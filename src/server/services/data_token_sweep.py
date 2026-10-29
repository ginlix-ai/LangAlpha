"""Background sweep keeping running sandboxes' market-data tokens fresh.

A turn's start writes a fresh token when the current one runs low, but a
sandbox can go on working with no turn starting: a long turn, a background
task, an always-on computer. This sweep writes one into each running computer
whose token runs low and that the idle reaper would keep running: always-on,
used lately, or with work in flight. An idle one is left alone, since writing
into a sandbox counts as activity at its provider (Daytona's auto-stop, E2B's
pause deadline) and would hold off that stop for good; its next turn's start
writes the token before any of that turn's code runs. Every worker runs the
sweep; claiming a computer in Postgres has one of them write each.

A pass first sends again each revocation a stop could not deliver, the
platform being unreachable then, until the platform confirms it or every
token it covers has run out.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

# Comfortably inside the window a token has left when it falls due
# (``data_tokens.REFRESH_BELOW``), jitter and a failed pass included.
SWEEP_INTERVAL_S = 300.0
SWEEP_BATCH_LIMIT = 25
# A pass claims pages until none is due, up to this many: a backlog drains in
# one pass, and a pass still ends.
SWEEP_MAX_PAGES = 10
# A computer taken up is not taken again for this long, by any worker, so one
# whose sandbox keeps failing costs a try per interval, not one per worker.
RETRY_AFTER = timedelta(seconds=SWEEP_INTERVAL_S)
# A revocation sent is not sent again for this long by any worker. Shorter than
# the interval, so the next pass on any worker after it sends it.
REVOKE_RETRY_AFTER = timedelta(seconds=60)
# How long a rewrite after a revocation waits out a sibling's write in progress.
REWRITE_WAIT_S = 10.0
_LOCK_POLL_S = 0.25
STOP_GRACE = 30.0


def _lock_key(computer_id: str) -> int:
    from src.server.database.sql_fences import advisory_key

    return advisory_key("computer_data_token", computer_id)


class DataTokenSweeper:
    _instance: Optional["DataTokenSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "DataTokenSweeper":
        if cls._instance is None:
            cls._instance = DataTokenSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        """Start the sweep loop; inert where no market-data tokens are minted."""
        from src.server.services import data_tokens

        if not data_tokens.configured():
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(
            self._loop(), name="data-token-sweeper"
        )
        logger.info(f"[DataTokenSweeper] started (interval={self._interval:.0f}s)")

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
                "[DataTokenSweeper] loop ended with an error at shutdown",
                exc_info=True,
            )
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        # A pass at once: after a deploy, running sandboxes may hold a token
        # no row records, and only a write gives them one this server keeps.
        while not self._stop_event.is_set():
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[DataTokenSweeper] sweep failed", exc_info=True)
            # Jitter desynchronizes sibling workers so their advisory-lock
            # probes don't land in lockstep every cycle.
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass

    # ---------------------------------------------------------------- sweep

    async def sweep_once(self) -> int:
        """One bounded pass; returns how many sandboxes took a fresh token."""
        from src.server.database import computer_data_tokens as db
        from src.server.services import data_tokens

        await self._settle_revocations()
        written = 0
        for _ in range(SWEEP_MAX_PAGES):
            rows = await db.claim_due(
                refresh_below=data_tokens.REFRESH_BELOW,
                retry_after=RETRY_AFTER,
                limit=SWEEP_BATCH_LIMIT,
            )
            for row in rows:
                if self._stop_event.is_set():
                    return written
                try:
                    if await self._sweep_one(row):
                        written += 1
                except Exception as e:
                    logger.warning(
                        "[DataTokenSweeper] computer %s not swept (%s)",
                        row["computer_id"],
                        type(e).__name__,
                    )
            if len(rows) < SWEEP_BATCH_LIMIT:
                break
        return written

    async def _settle_revocations(self) -> None:
        """Send again the revocations still owed. The first that finds the
        platform unreachable ends this part of the pass: the rest would wait on
        it too, and the next pass takes them first. One the platform refuses
        stays owed, and the rest go on."""
        from src.server.database import computer_data_tokens as db
        from src.server.services import data_tokens

        for _ in range(SWEEP_MAX_PAGES):
            owed = await db.claim_revocations(
                retry_after=REVOKE_RETRY_AFTER, limit=SWEEP_BATCH_LIMIT
            )
            for revocation in owed:
                if self._stop_event.is_set():
                    return
                answer = await data_tokens.settle(revocation)
                if answer == "unreachable":
                    return
                if answer != "revoked":
                    continue
                logger.info(
                    "[DataTokenSweeper] revoked the market-data tokens of "
                    "computer %s, owed since its stop",
                    revocation.computer_id,
                )
            if len(owed) < SWEEP_BATCH_LIMIT:
                return

    async def rewrite(self, computer_id: str) -> bool:
        """Write a fresh token into the computer's sandbox now, the platform
        having confirmed a revocation that may have ended the one it holds.

        Waits out a sibling's write in progress rather than leave the sandbox
        to the next pass: that write read the row before the revocation was
        recorded, so its token may be one the platform refuses, and its save
        or hold misses the row. A computer still starting is in use, whatever
        its last activity says.
        """
        from src.server.database import computer_data_tokens as db

        row = await db.load_in_service(computer_id)
        if row is None:
            return False
        if row.get("status") != "starting" and not await self._keeps_running(row):
            return False
        return await self._write_when_free(computer_id, row, wait_s=REWRITE_WAIT_S)

    @staticmethod
    async def _keeps_running(row: dict[str, Any]) -> bool:
        """Whether the idle reaper would leave the computer running: always-on,
        used within the idle limit of its own provider, or with work in
        flight, failing toward active as the reaper does. Settings no
        provider can be built from rule out a write as well."""
        if row.get("is_always_on"):
            return True
        from src.server.services.computer_manager import ComputerManager
        from src.server.services.runs.executor import LocalRunExecutor

        try:
            # The reaper's own limit, so a computer it is about to stop for
            # being idle is not the one thing keeping it awake at the provider.
            idle_timeout = ComputerManager.get_instance()._idle_timeout_for(row)
        except ValueError:
            return False
        last_activity = row.get("last_activity")
        if last_activity is not None:
            if last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=UTC)
            idle_seconds = (datetime.now(UTC) - last_activity).total_seconds()
            if idle_seconds <= idle_timeout:
                return True

        return await LocalRunExecutor.get_instance().has_active_tasks_for_computer(
            str(row["computer_id"])
        )

    async def _sweep_one(self, row: dict[str, Any]) -> bool:
        if not await self._keeps_running(row):
            return False
        return await self._write_when_free(str(row["computer_id"]), row)

    async def _write_when_free(
        self, computer_id: str, row: dict[str, Any], *, wait_s: float = 0.0
    ) -> bool:
        from src.server.database.session_lock import try_session_lock

        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        while True:
            # Session-level, so the server releases it if this worker dies
            # mid-write.
            async with try_session_lock(_lock_key(computer_id)) as held:
                if held:
                    return await self._write_locked(computer_id, row)
            if loop.time() >= deadline:
                return False  # a sibling worker is writing this one
            await asyncio.sleep(_LOCK_POLL_S)

    async def _write_locked(self, computer_id: str, row: dict[str, Any]) -> bool:
        from ptc_agent.core.paths import SandboxLayout
        from ptc_agent.core.sandbox._shared import (
            REPLACE_UNLESS_NEWER_SCRIPT,
            publish_private_file,
        )
        from ptc_agent.core.sandbox.runtime import RuntimeState
        from src.server.database import computer_data_tokens as db
        from src.server.services import data_tokens
        from src.server.services.computer_manager import ComputerManager

        sandbox_id = str(row["provider_ref"])
        # Read again under the lock: a turn's start or a sibling's pass since
        # the listing may have written one already.
        current = await db.load(computer_id)
        if not data_tokens.due(current, sandbox_id):
            return False

        manager = ComputerManager.get_instance()
        async with manager._detached_runtime(
            sandbox_id, binding=manager._binding_from_computer("", row)
        ) as runtime:
            # The row can say running while the sandbox is not, after an
            # auto-stop no stop path saw; a token minted for it goes unused.
            if await runtime.get_state() != RuntimeState.RUNNING:
                return False
            path = SandboxLayout(await runtime.fetch_working_dir()).mcp_tokens

            async def write(payload: dict[str, Any]) -> bool:
                # Again after the mint: E2B resumes a sandbox it attaches to,
                # so a stop that paused it meanwhile must not be undone here.
                if await runtime.refresh_state() != RuntimeState.RUNNING:
                    return False
                return await publish_private_file(
                    runtime,
                    path,
                    payload,
                    what="sandbox token file",
                    script=REPLACE_UNLESS_NEWER_SCRIPT,
                    args=(str(payload["iat_ms"]),),
                )

            return await data_tokens.push(
                computer_id,
                str(row["user_id"]),
                sandbox_id,
                write,
                generation=current.generation,
            )
