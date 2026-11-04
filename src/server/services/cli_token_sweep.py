"""Background sweep keeping running sandboxes' CLI tokens fresh.

The acquire that runs a turn writes the token, but a command can outlive the
turn: a background job, a long turn, an always-on computer. Each pass writes
a fresh one into every running computer whose token runs low and that the
idle reaper would keep running, by the market-data sweep's rule, since a
write counts as activity at the provider and would hold off its stop.
Unlike that sweep it runs on every deployment, the token needing no
platform. Every worker sweeps; a claim on each row in Postgres, and the
mint's compare on it, have one of them write each.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import timedelta
from typing import Any, Optional

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

# Comfortably inside ``cli_tokens.REFRESH_BELOW``, jitter and a failed pass
# included.
SWEEP_INTERVAL_S = 300.0
SWEEP_BATCH_LIMIT = 25
SWEEP_MAX_PAGES = 10
RETRY_AFTER = timedelta(seconds=SWEEP_INTERVAL_S)
# Outlasts a turn's mint and write into a slow sandbox.
MINT_SETTLES = timedelta(minutes=2)
STOP_GRACE = 30.0
CALLS_KEPT = timedelta(days=90)
PRUNE_BATCH_LIMIT = 5000


class CliTokenSweeper:
    _instance: Optional["CliTokenSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "CliTokenSweeper":
        if cls._instance is None:
            cls._instance = CliTokenSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop(), name="cli-token-sweeper")
        logger.info(f"[CliTokenSweeper] started (interval={self._interval:.0f}s)")

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning("[CliTokenSweeper] loop ended with an error at shutdown", exc_info=True)
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[CliTokenSweeper] sweep failed", exc_info=True)
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass

    async def sweep_once(self) -> int:
        """One bounded pass; returns how many sandboxes took a fresh token."""
        from src.server.database import computer_cli_tokens as db
        from src.server.services import cli_tokens

        await self._prune_calls()
        written = 0
        for _ in range(SWEEP_MAX_PAGES):
            rows = await db.claim_due(
                refresh_below=cli_tokens.REFRESH_BELOW,
                retry_after=RETRY_AFTER,
                settling=MINT_SETTLES,
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
                        "[CliTokenSweeper] computer %s not swept (%s)",
                        row["computer_id"],
                        type(e).__name__,
                    )
            if len(rows) < SWEEP_BATCH_LIMIT:
                break
        return written

    async def _prune_calls(self) -> None:
        """The record of what computers' agents did through ``la`` is kept
        ``CALLS_KEPT``; past that it goes, a bounded batch a pass."""
        from datetime import datetime, timezone

        from src.server.database import computer_cli_calls

        try:
            await computer_cli_calls.prune(
                datetime.now(timezone.utc) - CALLS_KEPT, PRUNE_BATCH_LIMIT
            )
        except Exception:
            logger.warning("[CliTokenSweeper] CLI call record not pruned", exc_info=True)

    async def _sweep_one(self, row: dict[str, Any]) -> bool:
        from ptc_agent.core.paths import SandboxLayout
        from src.server.services import cli_tokens
        from src.server.services.data_token_sweep import (
            keeps_running,
            running_sandbox,
            token_writer,
        )

        if not await keeps_running(row):
            return False
        async with running_sandbox(row) as runtime:
            if runtime is None:
                return False
            path = SandboxLayout(await runtime.fetch_working_dir()).cli_token
            return await cli_tokens.keep(
                row["computer_id"],
                str(row["user_id"]),
                str(row["provider_ref"]),
                # The sandbox's provider, which an acquire reads off the sandbox.
                str(row["kind"]),
                token_writer(runtime, path),
            )
