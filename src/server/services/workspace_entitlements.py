"""Workspace entitlement controls: spec tiers, always-on, duplicate, and the
idle-reaper entitlement reconciliation. Mixin for WorkspaceManager."""

import asyncio
import logging
from dataclasses import replace
from typing import Any, Dict

from ptc_agent.core.session import Session, SessionManager

from src.server.database.computer import (
    get_computer,
    set_computer_always_on as db_set_computer_always_on,
    set_computer_resource_tier as db_set_computer_resource_tier,
    try_claim_computer_for_start,
    update_computer_status,
)
from src.server.database.workspace import (
    delete_workspace as db_delete_workspace,
    get_live_workspace_ids_for_computer,
    get_workspace as db_get_workspace,
)
from src.server.models.computer import ComputerStatus
from src.server.services.computer_manager._types import ComputerBinding
from src.server.database.workspace_file import (
    copy_workspace_files,
    get_workspace_total_size,
)

logger = logging.getLogger(__name__)

# Disk reserved for the OS, Python venv, and MCP wrapper packages baked into
# every snapshot. Subtracted from a tier's disk to estimate space usable for
# restored user files when guarding a downgrade. ~2 GiB matches the standard
# tier's 3 GiB disk leaving ~1 GiB for files (the existing soft per-workspace cap).
_DISK_SYSTEM_RESERVE_BYTES = 2 * 1024**3
_GIB = 1024**3


class WorkspaceEntitlementsMixin:
    """Spec-tier, always-on, duplicate, and entitlement-reconciliation methods for WorkspaceManager."""

    async def _entitled_tier(self, binding: ComputerBinding, user_id: str | None) -> str:
        """Resolve the tier to provision, lazily reclaiming a lapsed elevated tier.

        The tier is the machine's, so it is read and reclaimed on the computer
        row: reading a project's shadow is how one lagging row provisions a
        sandbox at a size the machine has left. Keeps the elevated size when the
        check is inconclusive (fail-safe / OSS) or the backed-up files would not
        fit the standard disk (data safety over enforcement).
        """
        from src.server.dependencies.usage_limits import spec_entitlement_lost

        tier = binding.resource_tier or "standard"
        if tier == "standard" or not user_id:
            return tier
        if not await spec_entitlement_lost(user_id, tier):
            return tier
        computer_id = binding.computer_id
        standard = self.config.sandbox.daytona.resource_tiers.get("standard")
        if standard is not None:
            try:
                await self._assert_machine_disk_fits(computer_id, standard.disk)
            except RuntimeError as e:
                logger.warning(
                    f"Spec entitlement lost for computer {computer_id} "
                    f"(user {user_id}, tier {tier!r}) but files exceed the "
                    f"standard disk; keeping size: {e}"
                )
                return tier
        logger.info(
            f"Spec entitlement lost for computer {computer_id} "
            f"(user {user_id}); reclaiming tier {tier!r} -> 'standard'"
        )
        await db_set_computer_resource_tier(computer_id, "standard")
        return "standard"

    async def _entitled_always_on(
        self, binding: ComputerBinding, user_id: str | None
    ) -> bool:
        """Resolve whether to (re)provision always-on, lazily reclaiming a lapse.

        Mirrors :meth:`_entitled_tier`, on the same authority. The idle reaper
        only reconciles running rows, so a machine whose plan lapsed while
        stopped would otherwise restart always-on; this closes that gap at
        (re)provision time. Fail-safe: keeps always-on when the check is
        inconclusive (OSS/unreachable).
        """
        from src.server.dependencies.usage_limits import always_on_entitlement_lost

        if not binding.is_always_on or not user_id:
            return binding.is_always_on
        if not await always_on_entitlement_lost(user_id):
            return True
        logger.info(
            f"Always-on entitlement lost for computer {binding.computer_id} "
            f"(user {user_id}); reclaiming on recover"
        )
        await db_set_computer_always_on(binding.computer_id, False)
        return False

    async def _maybe_reclaim_lazy_tier(
        self, binding: ComputerBinding, user_id: str, session: Session
    ) -> Session | None:
        """Phase-2 arm of lazy spec reclaim: when the owner's elevated-tier
        entitlement lapsed, destroy the reconnected sandbox and recover at the
        reclaimed tier, returning the recovered session; None when the restart
        may proceed on the existing sandbox.

        Runs outside the per-workspace lock — cross-worker exclusion comes from
        the 'starting' claim row, same-worker coalescing from the Phase-2 event.
        """
        computer_id = binding.computer_id
        machine = self._machine(computer_id)
        machine.pending_tier_recheck = False
        # Re-read the machine: the binding was frozen at turn start and the tier
        # is what this check is about.
        computer = await get_computer(computer_id)
        if computer is None:
            return None
        # The folder is the project's, not a machine column, and a bound
        # project's folder never moves, so it rides across the refresh.
        fresh = replace(
            self._binding_from_computer(binding.workspace_id, computer),
            dir_name=binding.dir_name,
        )
        tier = fresh.resource_tier or "standard"
        if tier == "standard":
            return None
        if await self._entitled_tier(fresh, user_id) == tier:
            return None
        sandbox_id = fresh.provider_ref
        if sandbox_id:
            try:
                await self._destroy_sandbox(sandbox_id, binding=fresh)
            except Exception as e:
                # Best-effort: an orphaned sandbox is reclaimed by Daytona's
                # dormancy timers; don't block the rebuild.
                logger.warning(
                    f"Failed to destroy outsized sandbox {sandbox_id} "
                    f"for computer {computer_id}: {e}"
                )
        if self._cached_session(computer_id) is session:
            await self._clear_session(computer_id, evict_session=session)
        # _clear_session clears pending_lazy_sync; re-arm it so a recovery
        # failure hits Phase 2's revert-to-'stopped' + re-raise handler instead
        # of being tolerated as a warm re-sync hiccup.
        machine.pending_lazy_sync = True
        recovered = await self._recover_sandbox(
            fresh, user_id, self._core_config_for(fresh)
        )
        machine.pending_lazy_sync = False
        return recovered

    async def _destroy_sandbox(self, sandbox_id: str, *, binding: Any = None) -> None:
        """Delete a sandbox by id via the provider, reaching it like archive_workspace.

        Used to retire a stopped workspace's sandbox so its next start recreates
        it from the (possibly new) tier snapshot. Always closes the provider.
        ``binding`` points it at the machine's own backend when the caller knows
        which machine this was.
        """
        provider = self._provider_for(binding)
        try:
            runtime = await provider.get(sandbox_id)
            await runtime.delete()
        finally:
            await provider.close()

    async def _assert_disk_fits(self, workspace_id: str, target_disk_gib: int) -> None:
        """Reject a downgrade whose backed-up files won't fit the target disk.

        File restore is per-file best-effort, so a sandbox recreated on a smaller
        disk silently drops whatever overflows. The summed ``file_size`` is a
        lower bound (tracked workspace files only, excluding caches/venv), so this
        catches gross overflow rather than every byte. Call *before* teardown.

        Raises:
            RuntimeError: Backed-up files exceed the target disk's usable space.
        """
        usable = max(0, target_disk_gib * _GIB - _DISK_SYSTEM_RESERVE_BYTES)
        total = await get_workspace_total_size(workspace_id)
        if total > usable:
            raise RuntimeError(
                f"Cannot downgrade: workspace files ({total / _GIB:.1f} GiB) "
                f"exceed the {target_disk_gib} GiB tier's usable space "
                f"(~{usable / _GIB:.1f} GiB). Free up space first."
            )

    async def _assert_machine_disk_fits(
        self, computer_id: str, target_disk_gib: int
    ) -> None:
        """One disk holds every project on the machine, so the sum is the guard.

        Checking only the project that asked is how a two-project machine passes
        a downgrade its combined files cannot fit."""
        usable = max(0, target_disk_gib * _GIB - _DISK_SYSTEM_RESERVE_BYTES)
        total = 0
        for workspace_id in await get_live_workspace_ids_for_computer(computer_id):
            total += await get_workspace_total_size(workspace_id)
        if total > usable:
            raise RuntimeError(
                f"Cannot downgrade: files on this computer "
                f"({total / _GIB:.1f} GiB) exceed the {target_disk_gib} GiB "
                f"tier's usable space (~{usable / _GIB:.1f} GiB). "
                "Free up space first."
            )

    async def set_workspace_spec(
        self,
        workspace_id: str,
        tier: str,
        *,
        user_id: str | None = None,
    ) -> Dict[str, Any]:
        """Project-addressed entry for the deprecated workspace spec route."""
        binding = await self.resolve_binding(workspace_id)
        await self.set_computer_spec(
            binding.computer_id, tier, user_id=user_id, workspace_id=workspace_id
        )
        return await db_get_workspace(workspace_id) or {}

    async def set_computer_spec(
        self,
        computer_id: str,
        tier: str,
        *,
        user_id: str | None = None,
        workspace_id: str | None = None,
    ) -> Dict[str, Any]:
        """Change a computer's resource tier by recreating its sandbox.

        Hosted Daytona can't resize a snapshot sandbox or override its resources,
        so sizing lives in per-tier snapshots and a spec change means recreate,
        not resize. The sandbox is the machine's, so this recreates it for every
        project on it:

        - **running**: back every project's files up to the DB, tear the live
          sandbox down, and recreate from the target tier's snapshot
          (``_recover_sandbox`` restores the files and applies always-on);
        - **stopped**: destroy the sandbox so the next start recreates it at the
          new tier (files were backed up to the DB on stop);
        - **never-started**: just persist the tier (still under the machine
          lock, so a spec change racing the initial create serializes behind it
          instead of persisting a tier the sandbox was not built at).

        The persisted tier is reverted if the recreate fails. A downgrade whose
        backed-up files won't fit the smaller disk is rejected before teardown.

        Raises:
            ValueError: Computer not found or ``tier`` is unknown.
            RuntimeError: Downgrade rejected, files exceed the target disk.
        """
        tiers = self.config.sandbox.daytona.resource_tiers
        if tier not in tiers:
            raise ValueError(f"Unknown resource tier: {tier}")

        computer = await get_computer(computer_id)
        if not computer:
            raise ValueError(f"Computer {computer_id} not found")

        current_tier_name = computer.get("resource_tier") or "standard"

        if tier == current_tier_name and computer.get("provider_ref"):
            # Already at this tier with a live sandbox, nothing to do.
            return computer

        # A disk shrink risks silently dropping files that don't fit the smaller
        # sandbox (restore is best-effort), so the guard travels as the disk the
        # backup has to fit, and None where the move cannot shrink it. Treat an
        # unknown current tier as a possible downgrade (data-safe).
        current_tier = tiers.get(current_tier_name)
        target_disk = tiers[tier].disk
        may_shrink = current_tier is None or target_disk < current_tier.disk
        disk_guard = target_disk if may_shrink else None

        try:
            async with self._observed_lock(computer_id, "computer.spec"):
                await self._apply_spec_change(
                    computer,
                    tier=tier,
                    disk_guard=disk_guard,
                    user_id=user_id,
                    workspace_id=workspace_id,
                )
        except (Exception, asyncio.CancelledError):
            # Same reason the inner handler names CancelledError: it is a
            # BaseException, so a plain `except Exception` would let a
            # cancellation land with the row already stamped at the new tier
            # the recreate never reached.
            await db_set_computer_resource_tier(computer_id, current_tier_name)
            raise

        return await get_computer(computer_id) or computer

    async def _apply_spec_change(
        self,
        computer: Dict[str, Any],
        *,
        tier: str,
        disk_guard: int | None,
        user_id: str | None,
        workspace_id: str | None,
    ) -> None:
        """Persist the tier and make the machine's sandbox match it.

        The critical section of ``set_computer_spec``: everything here runs
        under the machine lock, and the caller reverts the tier if it raises."""
        computer_id = str(computer["computer_id"])
        # Re-read under the lock: status/provider_ref may have moved since the
        # pre-lock read (a concurrent start/stop/cleanup). Branching on the
        # locked status keeps the teardown from racing a lifecycle op that owns
        # the sandbox.
        locked = await get_computer(computer_id) or computer
        locked_status = locked.get("status")
        locked_sandbox_id = locked.get("provider_ref")

        # Persist the new tier inside the lock (not before it) so the
        # create/recover paths read the right size, the new tier is not
        # globally visible until we own the critical section, and the
        # compensating revert window is bounded to it. Reverted by the caller
        # if the recreate fails so DB and platform billing never claim an
        # unreached upgrade.
        await db_set_computer_resource_tier(computer_id, tier)
        # The binding is what the recreate sizes from, so it carries the tier
        # just persisted: built from the row read above it would rebuild the
        # sandbox at the size the change is moving away from.
        binding = replace(
            self._binding_from_computer(workspace_id or "", locked),
            resource_tier=tier,
        )

        if not locked_sandbox_id:
            # Never started, or the sandbox went away while we waited: the tier
            # is persisted, so the next create/start builds it at the new size.
            logger.info(
                f"Computer {computer_id} has no sandbox; "
                f"persisted tier {tier!r} only"
            )
        elif locked_status == ComputerStatus.RUNNING:
            await self._replace_running_sandbox(
                binding,
                locked_sandbox_id,
                tier=tier,
                disk_guard=disk_guard,
                user_id=user_id,
                workspace_id=workspace_id,
            )
        elif locked_status == ComputerStatus.STOPPED:
            # Destroy the stopped sandbox so the next start recreates it from
            # the tier snapshot (files were backed up to the DB on stop).
            if disk_guard is not None:
                await self._assert_machine_disk_fits(computer_id, disk_guard)
            logger.info(
                f"Destroying stopped sandbox for computer {computer_id}; "
                f"will recreate at tier {tier!r} on next start"
            )
            await self._destroy_sandbox(locked_sandbox_id, binding=binding)
        else:
            # Transient state (creating/starting/stopping/error) may have an
            # in-flight op holding the sandbox, so refuse rather than tear it
            # down underneath that op (maps to 400 at the route).
            raise RuntimeError(
                f"Cannot change spec while this computer is "
                f"{locked_status!r}; wait for the current operation "
                "to finish"
            )

    async def _project_addressed(self, binding: ComputerBinding) -> ComputerBinding:
        """The same machine binding, addressed by one of its own projects.

        Nothing about a tier belongs to one project, but the recreate is
        project-addressed: binding a project is the write that publishes the
        machine's new sandbox ref, and its folder is where the new sandbox is
        laid out. The oldest live project stands in for a caller that named
        none, the one a consolidated machine's root already belongs to.
        Unchanged when the machine has no live project left.
        """
        workspace_id = binding.workspace_id or ""
        if not workspace_id:
            live = await get_live_workspace_ids_for_computer(binding.computer_id)
            if not live:
                return binding
            workspace_id = live[0]
        return replace(
            binding,
            workspace_id=workspace_id,
            dir_name=await self._workspace_folder(workspace_id),
        )

    async def _assert_machine_is_replaceable(
        self, computer_id: str, workspace_id: str | None
    ) -> None:
        """Refuse a recreate that would destroy work or files instead."""
        # Don't yank the sandbox out from under a live agent turn: execute_code
        # keeps running in the sandbox after get_session_for_workspace returns
        # without holding the machine lock, so recreating here would abort it
        # with SandboxGoneError. Mirror the idle reaper's guard and refuse (the
        # caller reverts the persisted tier; maps to 400).
        if await self._machine_has_active_tasks(
            computer_id, workspace_id=workspace_id
        ):
            raise RuntimeError(
                "Cannot change spec while an agent turn is running; "
                "wait for the current turn to finish"
            )
        # The backup and teardown act through this process's attached session;
        # without one (backend just restarted, or another replica owns it) the
        # backup would silently no-op and the teardown would orphan the live
        # sandbox, so refuse instead of destroying files we never snapshotted.
        session = self._cached_session(computer_id)
        if not session or not getattr(session, "sandbox", None):
            raise RuntimeError(
                "This computer is running but its session is not "
                "attached on this server; stop it first, then "
                "change spec"
            )

    async def _replace_running_sandbox(
        self,
        binding: ComputerBinding,
        locked_sandbox_id: str,
        *,
        tier: str,
        disk_guard: int | None,
        user_id: str | None,
        workspace_id: str | None,
    ) -> None:
        """Swap a live machine's sandbox for one built at the persisted tier."""
        computer_id = binding.computer_id
        # Resolved here and not with the machine binding above: the other two
        # branches need no project, and a folder read that fails must not stop a
        # machine that has no sandbox to lay out from changing tier.
        binding = await self._project_addressed(binding)
        if not binding.workspace_id:
            # Backup, bind and restore are all project-addressed, and a machine
            # with no live project has nothing to rebuild for. Refuse before any
            # teardown: the running sandbox stays up and the caller reverts the
            # tier, where recreating would delete it and bind nothing back.
            raise RuntimeError(
                "This computer has no live project to rebuild its sandbox for"
            )
        await self._assert_machine_is_replaceable(computer_id, workspace_id)

        # Recreate in place, mirroring the sandbox-migration path.
        logger.info(f"Recreating computer {computer_id} at tier {tier!r}")
        # Strict: the sandbox is about to be destroyed, so a missed backup here
        # is data loss, not a degraded sync. Pinned to the locked sandbox id:
        # a session bound to an already-superseded sandbox would snapshot the
        # wrong filesystem over the DB copy.
        await self._backup_machine_files_to_db(
            computer_id,
            workspace_id=workspace_id,
            strict=True,
            expected_sandbox_id=locked_sandbox_id,
        )
        if disk_guard is not None:
            # Checked post-backup (fresh DB sizes), pre-claim: the live sandbox
            # is untouched and the row still advertises running/<old id>, so a
            # rejection aborts cleanly instead of stranding the machine
            # mid-replacement.
            await self._assert_machine_disk_fits(computer_id, disk_guard)
        # Durably fence the replacement BEFORE teardown. The machine lock is a
        # per-process asyncio.Lock, so without this the row keeps advertising
        # running/<old id> while that sandbox is deleted, and a request on
        # another worker would attach to it, fail, and race us into
        # provisioning a duplicate.
        if not await try_claim_computer_for_start(
            computer_id,
            from_status="running",
            expected_provider_ref=locked_sandbox_id,
            require_provider_ref=True,
        ):
            raise RuntimeError(
                "This computer changed while preparing the spec change; retry"
            )
        # Everything past the claim runs under a compensator. The row now reads
        # 'starting' and only this task can release it; leaving through any
        # other exit strands the workspace permanently unstartable, because the
        # start path claims from 'stopped', never from 'starting'. Scoping this
        # to the _recover_sandbox call alone left the teardown steps,
        # themselves failure-prone, outside the net.
        try:
            # Tear the old sandbox down via the canonical teardown, then
            # force-evict the SessionManager entry: cleanup_session skips its
            # own pop when session.cleanup() raised, so the _recover_sandbox
            # below must not return the stale broken session.
            await self._clear_session(computer_id)
            SessionManager.remove_session(computer_id)
            # Belt-and-braces: if cleanup raised before deleting the sandbox,
            # destroy it by id (mirrors the stopped path) so a half-torn-down
            # sandbox can't keep running.
            try:
                await self._destroy_sandbox(locked_sandbox_id, binding=binding)
            except Exception as e:
                logger.debug(f"Old sandbox {locked_sandbox_id} already gone: {e}")
            await self._recover_sandbox(
                binding, user_id, self._core_config_for(binding)
            )
        except (Exception, asyncio.CancelledError):
            # Files are safely backed up, so mark the row 'stopped' and the next
            # start self-heals (claim -> restart -> SandboxGone -> recover).
            # 'error' would be terminal: the start path refuses it outright. The
            # caller still reverts the tier.
            #
            # CancelledError is caught explicitly because it is a
            # BaseException: a client disconnect or a shutdown landing
            # mid-replacement would otherwise skip this and strand the row in
            # 'starting', which no start can claim.
            await update_computer_status(computer_id, ComputerStatus.STOPPED)
            raise

    async def _apply_autostop_for_always_on(
        self,
        sandbox_id: str,
        *,
        enabled: bool,
        runtime: Any = None,
        binding: ComputerBinding | None = None,
    ) -> None:
        """Sync a live sandbox's auto-stop interval to the always-on flag.

        Interval 0 (never auto-stop) when enabled, else the configured default.
        Reuses ``runtime`` when the caller already holds a connected one (the
        reconnect path) to avoid a throwaway provider and an extra round trip.
        No-ops if the runtime lacks the ``autostop`` capability.
        """
        minutes = (
            0 if enabled else self.config.sandbox.daytona.auto_stop_interval // 60
        )

        if runtime is not None:
            if "autostop" in runtime.capabilities:
                await runtime.set_autostop_interval(minutes)
            return

        # The binding selects the machine's own backend. Building a
        # deployment-global provider instead dials the default backend at a
        # sandbox that may not live there, which is the whole reason a computer
        # carries provider_kind and provider_config.
        async with self._detached_runtime(sandbox_id, binding=binding) as detached:
            if "autostop" in detached.capabilities:
                await detached.set_autostop_interval(minutes)

    async def set_workspace_always_on(
        self,
        workspace_id: str,
        enabled: bool,
    ) -> Dict[str, Any]:
        """Project-addressed entry for the deprecated workspace always-on route."""
        binding = await self.resolve_binding(workspace_id)
        await self.set_computer_always_on(binding.computer_id, enabled)
        return await db_get_workspace(workspace_id) or {}

    async def set_computer_always_on(
        self,
        computer_id: str,
        enabled: bool,
    ) -> Dict[str, Any]:
        """Toggle a computer's always-on flag, syncing the live auto-stop interval.

        Auto-stop is a persisted Daytona property of the machine's sandbox that a
        plain reconnect does not re-assert, so toggling either direction on a
        machine that is not running is re-applied on its next restart (see
        ``_restart_workspace``).

        Raises:
            ValueError: Computer not found.
        """
        computer = await get_computer(computer_id)
        if not computer:
            raise ValueError(f"Computer {computer_id} not found")

        await db_set_computer_always_on(computer_id, enabled)

        sandbox_id = computer.get("provider_ref")
        if computer["status"] == ComputerStatus.RUNNING and sandbox_id:
            # Best-effort: the flag is already persisted and _restart_workspace
            # re-asserts auto-stop on the next start, so a transient sandbox
            # hiccup (it stopped between the read and here) must not 500 the
            # toggle.
            try:
                await self._apply_autostop_for_always_on(
                    sandbox_id,
                    enabled=enabled,
                    binding=self._binding_from_computer("", computer),
                )
            except Exception as e:
                logger.warning(
                    f"Failed to apply always-on auto-stop for computer "
                    f"{computer_id}: {e}"
                )

        return await get_computer(computer_id) or computer

    async def duplicate_workspace(
        self,
        source_id: str,
        user_id: str,
    ) -> Dict[str, Any]:
        """Copy a workspace's files into a fresh "<name> (copy)" project.

        The copy is a project on the same machine, so it takes that machine's
        tier and always-on rather than carrying the source's: those belong to
        the computer now, and a copy cannot mint a second one. Files are
        persisted to the DB first when the source is running and copied to the
        new row; the sandbox side is the machine's first start, which restores
        them, so this returns as fast as an ordinary create.

        Raises:
            ValueError: Source missing, not owned by ``user_id``, or a flash
                workspace.
        """
        source = await db_get_workspace(source_id)
        if not source or source.get("user_id") != user_id:
            raise ValueError(f"Workspace {source_id} not found")
        if source["status"] == "flash":
            raise ValueError("Cannot duplicate a flash workspace")

        # Files only persist to the DB on stop/delete, so flush a running source
        # before the copy or the new workspace would miss in-sandbox changes.
        # We already hold the row, so hand the durable id over rather than
        # making the backup re-read it.
        if source["status"] == "running":
            source_binding = await self.resolve_binding(source_id, workspace=source)
            await self.backup_project_files(
                source_id,
                computer_id=source_binding.computer_id,
                expected_sandbox_id=source_binding.provider_ref,
            )

        # Carry over the source config minus the sandbox-identity stamps — those
        # belong to the source's sandbox and are re-stamped when the machine is
        # next provisioned.
        source_config = dict(source.get("config") or {})
        for stamp_key in (
            "sandbox_config_hash",
            "sandbox_provider",
            "sandbox_working_dir",
        ):
            source_config.pop(stamp_key, None)

        copy_name = f"{source['name']} (copy)"
        new_workspace = await self.create_workspace(
            user_id=user_id,
            name=copy_name,
            description=source.get("description"),
            config=source_config or None,
        )
        new_id = str(new_workspace["workspace_id"])

        logger.info(
            f"Duplicating workspace {source_id} -> {new_id} for user {user_id}"
        )
        try:
            await copy_workspace_files(source_id, new_id)
        except Exception as e:
            # Tombstone the fresh row rather than stamping it 'error': status is
            # the machine's and mirrors across it, so a failed copy on the new
            # project would move the source machine and every sibling on it into
            # a state whose only exit is delete and recreate.
            logger.error(f"Failed to seed duplicated workspace {new_id}: {e}")
            await db_delete_workspace(new_id)
            raise

        return new_workspace

    async def _reconcile_always_on_entitlements(
        self, running_computers: list[dict]
    ) -> set[str]:
        """Disable always-on for computers whose owner lost the entitlement.

        This is the only periodic loop already walking always-on rows, so it
        doubles as the entitlement reconciler. Returns the set of computer_ids
        that remain EXEMPT from idle reaping this cycle: machines still entitled
        (or the platform can't confirm otherwise, fail-safe), plus any whose
        disable failed (left flagged so a transient error doesn't yank them). A
        machine whose entitlement is gone is disabled and NOT exempt, so it
        falls through to idle reaping (reaped now if idle; stops on a later tick
        if in use, no mid-use yank). The entitlement check runs once per
        distinct owner (bounded-concurrent) so several always-on computers for
        one user trigger a single platform validate.
        """
        from src.server.dependencies.usage_limits import always_on_entitlement_lost

        exempt: set[str] = set()

        always_on_rows = [c for c in running_computers if c.get("is_always_on")]
        # One platform validate per distinct owner, bounded-concurrent so a
        # large always-on fleet doesn't serialize the reaper cycle on RTTs.
        semaphore = asyncio.Semaphore(5)

        async def _probe(uid: str) -> tuple[str, bool]:
            async with semaphore:
                return uid, await always_on_entitlement_lost(uid)

        distinct_users = {str(c["user_id"]) for c in always_on_rows}
        entitlement_lost: dict[str, bool] = dict(
            await asyncio.gather(*(_probe(uid) for uid in distinct_users))
        )

        for computer in always_on_rows:
            user_id = str(computer["user_id"])
            computer_id = str(computer["computer_id"])
            if not entitlement_lost[user_id]:
                exempt.add(computer_id)
                continue
            # Entitlement gone (e.g. plan downgraded): clear the flag — which
            # also retires the live Daytona auto-stop.
            logger.info(
                f"Always-on entitlement lost for computer {computer_id} "
                f"(user {user_id}); disabling always-on"
            )
            try:
                await self.set_computer_always_on(computer_id, False)
            except Exception as e:
                logger.error(f"Error disabling always-on for {computer_id}: {e}")
                # Disable failed — keep it exempt this tick rather than reap a
                # still-flagged machine.
                exempt.add(computer_id)

        return exempt
