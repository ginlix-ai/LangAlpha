"""Background refresh sweep for MCP OAuth connections.

Proactively refreshes connections approaching expiry so relay requests almost
never pay the refresh on the hot path. Cross-worker dedup rides the same
per-connection advisory try-lock ``ensure_fresh_access_token`` takes — this
loop is just a pacemaker. Schema snapshots are refreshed only on connect and
via the Plugins refresh action, never here.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Optional

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 300.0
SWEEP_BATCH_LIMIT = 20
# Refresh anything expiring inside this window (comfortably wider than the
# hot path's 10-minute margin so the sweeper usually gets there first).
DUE_MARGIN_SECONDS = 900
STOP_GRACE = 30.0


class McpOAuthRefreshSweeper:
    _instance: Optional["McpOAuthRefreshSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "McpOAuthRefreshSweeper":
        if cls._instance is None:
            cls._instance = McpOAuthRefreshSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(
            self._loop(), name="mcp-oauth-refresh-sweeper"
        )
        logger.info(
            f"[McpOAuthRefreshSweeper] started (interval={self._interval:.0f}s)"
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
                "[McpOAuthRefreshSweeper] loop ended with an error at shutdown",
                exc_info=True,
            )
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            # Jitter desynchronizes sibling workers so their advisory-lock
            # probes don't land in lockstep every cycle.
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(
                    "[McpOAuthRefreshSweeper] sweep failed", exc_info=True
                )

    # ---------------------------------------------------------------- sweep

    async def sweep_once(self) -> int:
        """One bounded pass; returns how many connections were refreshed."""
        from src.server.database.mcp_oauth import list_due_refresh
        from src.server.services.mcp_oauth.lifecycle import (
            TokenUnavailable,
            ensure_fresh_access_token,
        )

        rows = await list_due_refresh(
            margin_seconds=DUE_MARGIN_SECONDS, limit=SWEEP_BATCH_LIMIT
        )
        refreshed = 0
        for row in rows:
            if self._stop_event.is_set():
                break
            connection_id = str(row["connection_id"])
            try:
                await ensure_fresh_access_token(connection_id)
                refreshed += 1
            except TokenUnavailable as e:
                # Terminal states are the lifecycle's outcome, not an error.
                logger.info(
                    "[McpOAuthRefreshSweeper] connection %s not refreshable: %s",
                    connection_id, e.reason,
                )
            except Exception:
                logger.warning(
                    "[McpOAuthRefreshSweeper] refresh errored for %s",
                    connection_id, exc_info=True,
                )
        if len(rows) == SWEEP_BATCH_LIMIT:
            logger.info(
                "[McpOAuthRefreshSweeper] batch limit reached; more due "
                "connections pending next pass"
            )
        return refreshed
