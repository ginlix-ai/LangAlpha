"""Background sweep converging behind sandboxes onto the platform-secret rollout.

The request path only ever hot-remounts placeholder bindings (never
destructive), so the scrub-restart that purges legacy plaintext from live
processes needs an owner that is NOT a turn. This sweeper enumerates running
computers behind the fleet generation (always-on sandboxes never re-init, so
no bringup would ever converge them), takes the machine's advisory lock (one
worker migrates each), skips any machine with an active turn on any of
its projects (the sweep owns no turn, so the predicate never self-counts),
and converges in the idle gap: a scrub-restart for never-certified sandboxes,
a hot remount for certified-but-behind ones. Always-on machines are
guaranteed to converge because the sweep retries every cycle until it
catches an inter-turn gap. A turn beginning between the idle check and the force-stop
remains possible (the check is an observation, not admission-coupled); that
window is a few provider calls wide and a hit degrades to the existing
sandbox-transient retry paths.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Optional

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 60.0
SWEEP_BATCH_LIMIT = 25

# A busy machine defers its scrub to the next cycle; warn periodically so a
# never-idle always-on machine surfaces in logs instead of silently lagging.
BUSY_WARN_EVERY = 30

# Grace for a scrub in flight at shutdown before it is cancelled.
STOP_GRACE = 30.0


class PlatformSecretSweeper:
    _instance: Optional["PlatformSecretSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._config: Any = None
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._busy_skips: dict[str, int] = {}

    @classmethod
    def get_instance(cls) -> "PlatformSecretSweeper":
        if cls._instance is None:
            cls._instance = PlatformSecretSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    # ------------------------------------------------------------ lifecycle

    def start(self, config: Any) -> None:
        """Start the periodic sweep loop; inert unless managed secrets are active."""
        from ptc_agent.core.sandbox.platform_secrets import platform_secrets_active

        if not platform_secrets_active(config):
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._config = config
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(
            self._loop(), name="platform-secret-sweeper"
        )
        logger.info(
            f"[PlatformSecretSweeper] started (interval={self._interval:.0f}s)"
        )

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            logger.warning(
                "[PlatformSecretSweeper] sweep exceeded stop grace; cancelling"
            )
            self._loop_task.cancel()
            try:
                await self._loop_task
            except (asyncio.CancelledError, Exception):
                pass
        except Exception:
            logger.warning(
                "[PlatformSecretSweeper] sweep loop ended with an error at "
                "shutdown",
                exc_info=True,
            )
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        # Immediate startup pass — this IS the fleet migration after a deploy
        # or an identity bump; the periodic loop is the retry engine.
        while not self._stop_event.is_set():
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[PlatformSecretSweeper] sweep failed", exc_info=True)
            # Jitter desynchronizes sibling workers so the advisory-lock
            # probes don't land in lockstep every cycle.
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass

    # ---------------------------------------------------------------- sweep

    async def sweep_once(self) -> int:
        """One bounded pass; returns how many computers converged."""
        from ptc_agent.core.sandbox.providers import create_provider
        from src.server.services.platform_secret_rollout import (
            get_platform_secret_rollouts,
            list_workspaces_behind_platform_secret,
        )

        try:
            rollout_set = await get_platform_secret_rollouts()
        except Exception:
            # Rollout identity not registered (boot reconcile softened to a
            # missing prior rollout) — nothing to converge onto yet.
            logger.debug(
                "[PlatformSecretSweeper] no active rollout; skipping pass"
            )
            return 0

        rows = await list_workspaces_behind_platform_secret(
            rollout_set, limit=SWEEP_BATCH_LIMIT
        )
        if not rows:
            return 0

        provider = create_provider(self._config)
        converged = 0
        try:
            for row in rows:
                if self._stop_event.is_set():
                    break
                if await self._sweep_one(row, rollout_set, provider):
                    converged += 1
        finally:
            await provider.close()
        if len(rows) == SWEEP_BATCH_LIMIT:
            logger.info(
                "[PlatformSecretSweeper] batch limit reached; more behind "
                "computers pending next pass"
            )
        return converged

    async def _sweep_one(
        self, row: dict[str, Any], rollout_set: Any, provider: Any
    ) -> bool:
        from src.server.database.computer import computer_advisory_key
        from src.server.database.pool import get_db_connection

        computer_id = str(row["computer_id"])
        sandbox_id = str(row["sandbox_id"])
        # The lock is the machine's, because what converges is the sandbox: two
        # projects on one computer holding a key each would scrub the same
        # sandbox concurrently.
        lock_key = computer_advisory_key(computer_id)

        # A session-level advisory lock held for this machine's convergence
        # only — one pool connection, sequential, released in finally (and by
        # the server on connection loss), so a crash cannot wedge the fleet.
        async with get_db_connection() as conn:
            cur = await conn.execute(
                "SELECT pg_try_advisory_lock(%s)", (lock_key,)
            )
            acquired = (await cur.fetchone())[0]
            if not acquired:
                return False  # a sibling worker owns this machine's migration
            try:
                return await self._converge_locked(
                    computer_id, sandbox_id, row, rollout_set, provider
                )
            finally:
                try:
                    await conn.execute(
                        "SELECT pg_advisory_unlock(%s)", (lock_key,)
                    )
                except Exception:
                    pass

    async def _converge_locked(
        self,
        computer_id: str,
        sandbox_id: str,
        row: dict[str, Any],
        rollout_set: Any,
        provider: Any,
    ) -> bool:
        from ptc_agent.core.sandbox.platform_secrets import (
            converge_sandbox_platform_secrets,
            remount_platform_secret_bindings,
            verify_runtime_platform_secrets,
        )
        from src.server.services.platform_secret_rollout import (
            stamp_platform_secret_version,
        )
        from src.server.services.runs.executor import LocalRunExecutor

        # The row's project is a representative (the machine's oldest live
        # one, or none at all): it feeds the in-process busy pre-check and the
        # logs, never the stamp or the lock.
        workspace_id = (
            str(row["workspace_id"]) if row.get("workspace_id") else None
        )
        version = int(row.get("platform_secret_version") or 0)
        needs_scrub = version == 0

        if needs_scrub:
            # The scrub force-stops the sandbox; never under an active turn.
            # The sandbox is the machine's, so one idle project does not
            # settle it. The sweep is not a turn, so this cannot self-count.
            executor = LocalRunExecutor.get_instance()
            busy = await executor.has_active_tasks_for_computer(
                computer_id, workspace_id=workspace_id
            )
            if busy:
                self._note_busy_skip(computer_id)
                return False
        self._busy_skips.pop(computer_id, None)

        try:
            runtime = await provider.get(sandbox_id)
            if needs_scrub:
                await converge_sandbox_platform_secrets(
                    runtime,
                    expected=rollout_set.placeholders,
                    bindings=rollout_set.bindings,
                )
                # Auto-stop persists across restarts; re-assert defensively so
                # a scrubbed always-on sandbox provably stays always-on.
                if row.get("is_always_on") and "autostop" in runtime.capabilities:
                    try:
                        await runtime.set_autostop_interval(0)
                    except Exception:
                        logger.warning(
                            "[PlatformSecretSweeper] always-on re-assert "
                            f"failed for computer {computer_id}"
                        )
            else:
                # Certified placeholders, behind an identity bump: hot swap.
                await remount_platform_secret_bindings(
                    runtime,
                    expected=rollout_set.placeholders,
                    bindings=rollout_set.bindings,
                )
                await verify_runtime_platform_secrets(
                    runtime, expected=rollout_set.placeholders
                )
            await stamp_platform_secret_version(
                computer_id=computer_id,
                expected_sandbox_id=sandbox_id,
                rollout_set=rollout_set,
            )
        except Exception as exc:
            logger.warning(
                "[PlatformSecretSweeper] convergence failed for computer "
                f"{computer_id} (error_type={type(exc).__name__}); will retry"
            )
            return False

        if needs_scrub:
            await self._drop_local_session(computer_id)
        represented = f", represented by workspace {workspace_id}" if workspace_id else ""
        logger.info(
            f"[PlatformSecretSweeper] converged computer {computer_id} "
            f"(generation={rollout_set.generation}, scrubbed={needs_scrub}"
            f"{represented})"
        )
        return True

    async def _drop_local_session(self, computer_id: str) -> None:
        """Best-effort retirement of this worker's cached session after a scrub.

        The restart killed the session's exec processes; retiring makes the
        next acquisition re-attach instead of tripping a transient. Other
        workers' caches recover through the existing sandbox-transient paths,
        the same as any out-of-band sandbox restart.

        Retirement, not destruction: the sandbox was just scrubbed and remains
        the machine's durable identity, so destroying it here would leave
        Postgres pointing at a sandbox that no longer exists.
        """
        try:
            from src.server.services.computer_manager import ComputerManager

            manager = ComputerManager.current()
            if manager is not None:
                await manager.retire_computer_session_if_present(
                    computer_id, reason="platform-secret scrub restart"
                )
        except Exception:
            logger.warning(
                "[PlatformSecretSweeper] local session retirement failed for "
                f"computer {computer_id}",
                exc_info=True,
            )

    def _note_busy_skip(self, computer_id: str) -> None:
        count = self._busy_skips.get(computer_id, 0) + 1
        self._busy_skips[computer_id] = count
        if count % BUSY_WARN_EVERY == 0:
            logger.warning(
                f"[PlatformSecretSweeper] computer {computer_id} has "
                f"deferred its plaintext scrub {count} cycles (always busy); "
                "it converges at the next inter-turn gap"
            )
        else:
            logger.debug(
                f"[PlatformSecretSweeper] computer {computer_id} busy; "
                "deferring scrub to next cycle"
            )
