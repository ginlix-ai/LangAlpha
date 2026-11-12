"""Background sweep deleting sandboxes a create gave up on.

A Daytona create answers only once its sandbox runs, so a create the server
stopped waiting for still comes up with no computer naming it, and Daytona
refuses to delete it while it is mid-transition. The provider records each
such sandbox (``ptc_agent.core.sandbox.abandoned``); each pass here deletes
what Daytona now lets go and drops a row once nothing answers to its name.

A name stays until it has been absent for ``ABSENT_FOR``: a create the API
took late can land after the provider's own lookup found nothing. Every worker
sweeps; a claim on each row in Postgres has one of them try it.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import timedelta
from typing import Any, Optional

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 300.0
SWEEP_BATCH_LIMIT = 25
RETRY_AFTER = timedelta(seconds=SWEEP_INTERVAL_S)
ABSENT_FOR = timedelta(minutes=30)
# What one row may take, so a pass ends well inside RETRY_AFTER and no other
# worker claims its rows while it still holds them.
ROW_BUDGET_S = 10.0
# About an hour of passes between warnings about one sandbox, from its first.
WARN_EVERY = 12
STOP_GRACE = 30.0


def install_abandoned_sink() -> None:
    """Have this worker's providers record what they abandon.

    Every worker runs this from the lifespan, since a create can run on any.
    """
    from ptc_agent.core.sandbox.abandoned import set_abandoned_sink
    from src.server.database import abandoned_sandboxes

    set_abandoned_sink(abandoned_sandboxes.record)


class AbandonedSandboxSweeper:
    _instance: Optional["AbandonedSandboxSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "AbandonedSandboxSweeper":
        if cls._instance is None:
            cls._instance = AbandonedSandboxSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(
            self._loop(), name="abandoned-sandbox-sweeper"
        )
        logger.info(
            f"[AbandonedSandboxSweeper] started (interval={self._interval:.0f}s)"
        )

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
                "[AbandonedSandboxSweeper] loop ended with an error at shutdown",
                exc_info=True,
            )
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[AbandonedSandboxSweeper] sweep failed", exc_info=True)
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass

    async def sweep_once(self) -> int:
        """One bounded pass; returns how many rows it settled for good."""
        from src.server.database import abandoned_sandboxes as db

        rows = await db.claim_due(retry_after=RETRY_AFTER, limit=SWEEP_BATCH_LIMIT)
        settled = 0
        providers: dict[str, Any] = {}
        try:
            for row in rows:
                if self._stop_event.is_set():
                    break
                if await self._sweep_one(row, providers):
                    settled += 1
        finally:
            for provider in providers.values():
                await provider.close()
        return settled

    async def _sweep_one(self, row: dict[str, Any], providers: dict[str, Any]) -> bool:
        from src.server.database import abandoned_sandboxes as db

        kind, ref = row["kind"], row["ref"]
        try:
            if kind not in providers:
                providers[kind] = _deployment_provider(kind)
            async with asyncio.timeout(ROW_BUDGET_S):
                gone = await providers[kind].remove_abandoned(ref)
            error = None if gone else "the provider refused to delete it yet"
        except Exception as e:
            gone, error = False, f"{type(e).__name__}: {e}"[:500]
        if gone and row["age"] >= ABSENT_FOR:
            await db.drop(kind, ref)
            logger.info(
                "[AbandonedSandboxSweeper] %s sandbox %s is gone (%s)",
                kind,
                ref,
                row["reason"],
            )
            return True
        await db.note_attempt(kind, ref, error)
        if error and row["attempts"] % WARN_EVERY == 1:
            logger.warning(
                "[AbandonedSandboxSweeper] %s sandbox %s not removed after "
                "%d attempts: %s (%s)",
                kind,
                ref,
                row["attempts"],
                error,
                row["reason"],
            )
        return False


def _deployment_provider(kind: str) -> Any:
    """A client for *kind* under the deployment's own settings.

    A computer's ``provider_config`` can override them, but a create that
    failed named no computer; a sandbox made under an override reads as
    absent here and leaves with its row.
    """
    from ptc_agent.core.sandbox.providers import build_provider
    from src.server.services.computer_manager import ComputerManager

    manager = ComputerManager.get_instance()
    core_config = manager.config.to_core_config()
    return build_provider(
        kind,
        core_config.sandbox.settings_for(kind),
        working_dir=manager.config.filesystem.working_directory,
        platform_secrets=core_config.sandbox.platform_secrets,
    )
