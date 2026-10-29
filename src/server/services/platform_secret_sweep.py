"""Background sweep converging behind sandboxes onto their kind's platform-secret rollout.

The request path only ever hot-remounts placeholder bindings (never
destructive), so the scrub-restart that purges legacy plaintext from live
processes needs an owner that is NOT a turn. This sweeper enumerates running
computers behind the generation of their own provider kind's rollout
(always-on sandboxes never re-init, so no bringup would ever converge them),
takes the machine's advisory lock (one worker migrates each), skips any
machine with an active turn on any of its projects (the sweep owns no turn, so
the predicate never self-counts), and converges in the idle gap: a
scrub-restart for never-certified sandboxes, a hot remount for
certified-but-behind ones, and a rebuild from backup for a sandbox whose env is
fixed at create (E2B) and holds a catalog credential, which no restart clears.
Always-on machines are guaranteed to converge because the sweep retries every
cycle until it catches an inter-turn gap. A turn beginning between the idle
check and the force-stop remains possible (the check is an observation, not
admission-coupled); that window is a few provider calls wide and a hit
degrades to the existing sandbox-transient retry paths.

It is also the retry path for a served kind with no rollout rows, which boot
leaves unready rather than failing on: each pass registers such a kind, one
worker at a time under that kind's advisory lock.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 60.0
SWEEP_BATCH_LIMIT = 25

# A busy machine defers its scrub to the next cycle; warn periodically so a
# never-idle always-on machine surfaces in logs instead of silently lagging.
BUSY_WARN_EVERY = 30

# Grace for a scrub in flight at shutdown before it is cancelled.
STOP_GRACE = 30.0

# A rebuild that failed (most often a backup that could not complete) is not
# retried every cycle: each attempt starts with a full strict backup.
REBUILD_RETRY_S = 900.0

# A kind whose registration failed is retried at most this often per worker,
# so a provider that is down is not called every pass.
REGISTER_RETRY_S = 300.0


def registration_lock_key(kind: str) -> str:
    """Lets one worker at a time register a provider kind's Secrets."""
    return f"platform-secret-registration:{kind}"


class PlatformSecretSweeper:
    _instance: Optional["PlatformSecretSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._config: Any = None
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._busy_skips: dict[str, int] = {}
        # Where the next pass resumes in the behind set, so machines that never
        # converge cannot hold every batch. Per worker, like ``_busy_skips``:
        # the advisory lock, not this, keeps workers off one machine.
        self._cursor: Any = None
        self._rebuild_retry_at: dict[str, float] = {}
        self._register_retry_at: dict[str, float] = {}

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
        """Start the periodic sweep loop; inert without a platform-secret catalog.

        Gated on the catalog alone, not on the deployment's own provider: a
        deployment on a provider without platform Secrets can still hold
        computers on one that has them, and those must converge.
        """
        if not config.sandbox.platform_secrets:
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
        from ptc_agent.core.sandbox.platform_secrets import (
            provider_credentials_configured,
        )
        from src.server.services.computer_manager import ComputerManager
        from src.server.services.platform_secret_rollout import (
            get_all_platform_secret_rollouts,
            list_workspaces_behind_platform_secret,
        )

        # Fresh every pass: another worker may have registered a kind or a
        # new identity since the last one.
        rollout_sets = await get_all_platform_secret_rollouts()
        if await self._register_missing_kinds(rollout_sets):
            rollout_sets = await get_all_platform_secret_rollouts()
        # A kind without credentials cannot be reached, so it is not listed.
        rollout_sets = {
            kind: rollout_set
            for kind, rollout_set in rollout_sets.items()
            if provider_credentials_configured(self._config, kind)
        }
        if not rollout_sets:
            logger.debug(
                "[PlatformSecretSweeper] no active rollout; skipping pass"
            )
            return 0

        rows = await list_workspaces_behind_platform_secret(
            rollout_sets, limit=SWEEP_BATCH_LIMIT, after=self._cursor
        )
        if not rows and self._cursor is not None:
            # Past the end: wrap now rather than lose a pass.
            rows = await list_workspaces_behind_platform_secret(
                rollout_sets, limit=SWEEP_BATCH_LIMIT
            )
        self._cursor = (
            rows[-1]["computer_id"] if len(rows) == SWEEP_BATCH_LIMIT else None
        )
        if not rows:
            return 0

        manager = ComputerManager.get_instance()
        converged = 0
        for row in rows:
            if self._stop_event.is_set():
                break
            provider = None
            try:
                binding = manager._binding_from_computer("", row)
                provider = manager._provider_for(binding)
                if await self._sweep_one(
                    row, rollout_sets[row["kind"]], provider
                ):
                    converged += 1
            except Exception:
                logger.warning(
                    "[PlatformSecretSweeper] provider failed for computer %s",
                    row["computer_id"], exc_info=True,
                )
            finally:
                if provider is not None:
                    await provider.close()
        if len(rows) == SWEEP_BATCH_LIMIT:
            logger.info(
                "[PlatformSecretSweeper] batch limit reached; more behind "
                "computers pending next pass"
            )
        return converged

    # --------------------------------------------------------- registration

    async def _register_missing_kinds(self, registered: dict[str, Any]) -> bool:
        """Register each served kind that has credentials but no rollout rows.

        Covers a kind whose boot registration met an outage with nothing
        registered before, rows removed by hand, and a computer adopted onto
        a kind no boot registered. True when any kind was registered.
        """
        from ptc_agent.core.sandbox.platform_secrets import (
            provider_credentials_configured,
        )
        from src.server.services.platform_secret_rollout import (
            served_platform_secret_kinds,
        )

        try:
            served = await served_platform_secret_kinds(self._config)
        except Exception:
            logger.warning(
                "[PlatformSecretSweeper] could not read the served kinds",
                exc_info=True,
            )
            return False
        now = time.monotonic()
        registered_any = False
        for kind in sorted(served):
            if kind in registered:
                continue
            if not provider_credentials_configured(self._config, kind):
                continue
            if self._register_retry_at.get(kind, 0.0) > now:
                continue
            if await self._register_kind(kind):
                registered_any = True
        return registered_any

    async def _register_kind(self, kind: str) -> bool:
        from src.server.database.session_lock import try_session_lock
        from src.server.services.platform_secret_rollout import (
            PlatformSecretReadinessError,
            get_platform_secret_rollouts,
            reconcile_and_register_kind,
        )

        async with try_session_lock(registration_lock_key(kind)) as held:
            if not held:
                return False  # a sibling worker is registering this kind
            try:
                try:
                    # A sibling may have registered it since this pass read.
                    await get_platform_secret_rollouts(kind, fresh=True)
                    return False
                except PlatformSecretReadinessError:
                    pass
                rollout_set = await reconcile_and_register_kind(self._config, kind)
            except Exception as exc:
                # Nothing here may be fatal: the server is already serving.
                self._register_retry_at[kind] = time.monotonic() + REGISTER_RETRY_S
                logger.error(
                    f"[PlatformSecretSweeper] registering platform Secrets for "
                    f"{kind} failed (error_type={type(exc).__name__}); its "
                    f"computers stay unready, retrying in "
                    f"{REGISTER_RETRY_S / 60:.0f} min",
                    exc_info=True,
                )
                return False
        self._register_retry_at.pop(kind, None)
        logger.info(
            f"[PlatformSecretSweeper] registered platform Secrets for {kind} "
            f"(generation={rollout_set.generation})"
        )
        return True

    async def _sweep_one(
        self, row: dict[str, Any], rollout_set: Any, provider: Any
    ) -> bool:
        from src.server.database.computer import computer_advisory_key
        from src.server.database.session_lock import try_session_lock

        computer_id = str(row["computer_id"])
        sandbox_id = str(row["sandbox_id"])
        # The lock is the machine's, because what converges is the sandbox: two
        # projects on one computer holding a key each would scrub the same
        # sandbox concurrently. Held for this machine's convergence only, on
        # one pool connection, and released by the server on connection loss,
        # so a crash cannot wedge the fleet.
        async with try_session_lock(computer_advisory_key(computer_id)) as held:
            if not held:
                return False  # a sibling worker owns this machine's migration
            return await self._converge_locked(
                computer_id, sandbox_id, row, rollout_set, provider
            )

    async def _converge_locked(
        self,
        computer_id: str,
        sandbox_id: str,
        row: dict[str, Any],
        rollout_set: Any,
        provider: Any,
    ) -> bool:
        from ptc_agent.core.sandbox.platform_secrets import (
            PlatformSecretReplacementRequired,
            converge_sandbox_platform_secrets,
            remount_platform_secret_bindings,
            verify_runtime_platform_secrets,
        )
        from ptc_agent.core.sandbox.providers import provider_class
        from src.server.services.livefs.mount import restarted as livefs_restarted
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
        # A provider that cannot scrub (E2B) is verified like a certified one:
        # what its env holds now it holds for good.
        needs_scrub = (
            version == 0 and provider_class(row["kind"]).scrubs_platform_secrets
        )

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

        rebuilt = False
        try:
            runtime = await provider.get(sandbox_id)
            if needs_scrub:
                try:
                    await converge_sandbox_platform_secrets(
                        runtime,
                        expected=rollout_set.placeholders,
                        bindings=rollout_set.bindings,
                    )
                finally:
                    # The force-stop left no file mount. Unrecorded, the
                    # scrub is not stamped either, so the next cycle runs it
                    # and records it again.
                    await livefs_restarted(computer_id, sandbox_id)
                # Auto-stop persists across restarts; re-assert defensively so
                # a scrubbed always-on sandbox provably stays always-on.
                if row.get("is_always_on") and runtime.autostops:
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
                try:
                    await verify_runtime_platform_secrets(
                        runtime, expected=rollout_set.placeholders
                    )
                except PlatformSecretReplacementRequired:
                    # The new sandbox is certified and stamped as it binds.
                    if not await self._rebuild(computer_id, sandbox_id):
                        return False
                    rebuilt = True
            if not rebuilt:
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
            f"(generation={rollout_set.generation}, scrubbed={needs_scrub}, "
            f"rebuilt={rebuilt}{represented})"
        )
        return True

    async def _rebuild(self, computer_id: str, sandbox_id: str) -> bool:
        """Replace a sandbox no restart can clean; never under a turn.

        True once the machine runs on a new sandbox, or, holding no workspace,
        has stopped without one for its next start to build. A turn on any of its
        projects defers it like a busy scrub, and any other failure waits
        ``REBUILD_RETRY_S`` on this worker, the sandbox kept as it was.
        """
        from src.server.services.computer_errors import MachineBusyError
        from src.server.services.computer_manager import ComputerManager

        now = time.monotonic()
        if self._rebuild_retry_at.get(computer_id, 0.0) > now:
            return False
        try:
            rebuilt = await ComputerManager.get_instance().replace_sandbox_for_platform_secrets(
                computer_id, sandbox_id
            )
        except MachineBusyError:
            self._note_busy_skip(computer_id)
            return False
        except Exception as exc:
            self._rebuild_retry_at[computer_id] = now + REBUILD_RETRY_S
            logger.warning(
                f"[PlatformSecretSweeper] rebuild failed for computer {computer_id} "
                f"(error_type={type(exc).__name__}); keeping its sandbox, retrying "
                f"in {REBUILD_RETRY_S / 60:.0f} min",
                exc_info=True,
            )
            return False
        self._rebuild_retry_at.pop(computer_id, None)
        self._busy_skips.pop(computer_id, None)
        return rebuilt

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
