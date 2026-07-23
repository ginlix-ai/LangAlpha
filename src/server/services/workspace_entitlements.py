"""Workspace entitlement controls: spec tiers, always-on, duplicate, and the
idle-reaper entitlement reconciliation. Mixin for WorkspaceManager."""

import asyncio
import logging
from typing import Any, Dict

from ptc_agent.core.session import Session, SessionManager

from src.observability import (
    safe_add,
    workspace_created,
)
from src.observability.tracing import hash_id as _obs_hash_id

from src.server.services.runs.executor import LocalRunExecutor

from src.server.database.workspace import (
    create_workspace as db_create_workspace,
    get_workspace as db_get_workspace,
    set_workspace_always_on as db_set_workspace_always_on,
    set_workspace_resource_tier as db_set_workspace_resource_tier,
    update_workspace_status,
)
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

    async def _entitled_tier(
        self, workspace: Dict[str, Any], user_id: str | None
    ) -> str:
        """Resolve the tier to provision, lazily reclaiming a lapsed elevated tier.

        Returns the persisted ``resource_tier`` unless the platform confirms the
        owner no longer holds that tier's entitlement, in which case the tier is
        persisted back to ``standard`` and returned. Keeps the elevated size when
        the check is inconclusive (fail-safe / OSS) or the backed-up files would
        not fit the standard disk (data safety over enforcement).
        """
        from src.server.dependencies.usage_limits import spec_entitlement_lost

        tier = workspace.get("resource_tier") or "standard"
        if tier == "standard" or not user_id:
            return tier
        if not await spec_entitlement_lost(user_id, tier):
            return tier
        workspace_id = str(workspace["workspace_id"])
        standard = self.config.sandbox.daytona.resource_tiers.get("standard")
        if standard is not None:
            try:
                await self._assert_disk_fits(workspace_id, standard.disk)
            except RuntimeError as e:
                logger.warning(
                    f"Spec entitlement lost for workspace {workspace_id} "
                    f"(user {user_id}, tier {tier!r}) but files exceed the "
                    f"standard disk; keeping size: {e}"
                )
                return tier
        logger.info(
            f"Spec entitlement lost for workspace {workspace_id} "
            f"(user {user_id}); reclaiming tier {tier!r} -> 'standard'"
        )
        await db_set_workspace_resource_tier(workspace_id, "standard")
        return "standard"

    async def _entitled_always_on(
        self, workspace: Dict[str, Any], user_id: str | None
    ) -> bool:
        """Resolve whether to (re)provision always-on, lazily reclaiming a lapse.

        Mirrors :meth:`_entitled_tier`. Returns the persisted ``is_always_on``
        flag unless the platform confirms the owner no longer holds the always-on
        entitlement, in which case the flag is cleared and False returned so the
        sandbox comes back auto-stop-enabled. The idle reaper only reconciles
        running rows, so a workspace whose plan lapsed while stopped would
        otherwise restart always-on; this closes that gap at (re)provision time.
        Fail-safe: keeps always-on when the check is inconclusive (OSS/unreachable).
        """
        from src.server.dependencies.usage_limits import always_on_entitlement_lost

        always_on = bool(workspace.get("is_always_on"))
        if not always_on or not user_id:
            return always_on
        if not await always_on_entitlement_lost(user_id):
            return True
        workspace_id = str(workspace["workspace_id"])
        logger.info(
            f"Always-on entitlement lost for workspace {workspace_id} "
            f"(user {user_id}); reclaiming on recover"
        )
        await db_set_workspace_always_on(workspace_id, False)
        return False

    async def _maybe_reclaim_lazy_tier(
        self, workspace_id: str, user_id: str, session: Session
    ) -> Session | None:
        """Phase-2 arm of lazy spec reclaim: when the owner's elevated-tier
        entitlement lapsed, destroy the reconnected sandbox and recover at the
        reclaimed tier, returning the recovered session; None when the restart
        may proceed on the existing sandbox.

        Runs outside the per-workspace lock — cross-worker exclusion comes from
        the 'starting' claim row, same-worker coalescing from the Phase-2 event.
        """
        self._pending_tier_recheck.discard(workspace_id)
        workspace = await db_get_workspace(workspace_id)
        tier = (workspace or {}).get("resource_tier") or "standard"
        if workspace is None or tier == "standard":
            return None
        if await self._entitled_tier(workspace, user_id) == tier:
            return None
        sandbox_id = workspace.get("sandbox_id")
        if sandbox_id:
            try:
                await self._destroy_sandbox(sandbox_id)
            except Exception as e:
                # Best-effort: an orphaned sandbox is reclaimed by Daytona's
                # dormancy timers; don't block the rebuild.
                logger.warning(
                    f"Failed to destroy outsized sandbox {sandbox_id} "
                    f"for workspace {workspace_id}: {e}"
                )
        if self._sessions.get(workspace_id) is session:
            await self._clear_session(workspace_id, evict_session=session)
        # _clear_session discards _pending_lazy_sync; re-arm it so a recovery
        # failure hits Phase 2's revert-to-'stopped' + re-raise handler instead
        # of being tolerated as a warm re-sync hiccup.
        self._pending_lazy_sync.add(workspace_id)
        recovered = await self._recover_sandbox(
            workspace_id, user_id, self.config.to_core_config()
        )
        self._pending_lazy_sync.discard(workspace_id)
        return recovered

    async def _destroy_sandbox(self, sandbox_id: str) -> None:
        """Delete a sandbox by id via the provider, reaching it like archive_workspace.

        Used to retire a stopped workspace's sandbox so its next start recreates
        it from the (possibly new) tier snapshot. Always closes the provider.
        """
        from ptc_agent.core.sandbox.providers import create_provider

        provider = create_provider(self.config.to_core_config())
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

    async def set_workspace_spec(
        self,
        workspace_id: str,
        tier: str,
        *,
        user_id: str | None = None,
    ) -> Dict[str, Any]:
        """Change a workspace's resource tier by recreating its sandbox.

        Hosted Daytona can't resize a snapshot sandbox or override its resources,
        so sizing lives in per-tier snapshots and a spec change means recreate,
        not resize:

        - **running** — back files up to the DB, tear the live sandbox down, and
          recreate from the target tier's snapshot (``_recover_sandbox`` restores
          the files and applies always-on);
        - **stopped** — destroy the sandbox so the next start recreates it at the
          new tier (files were backed up to the DB on stop);
        - **never-started** — just persist the tier (still under the workspace
          lock, so a spec change racing the initial create serializes behind
          ``workspace.create`` instead of persisting a tier the sandbox was
          not built at).

        The persisted tier is reverted if the recreate fails. A downgrade whose
        backed-up files won't fit the smaller disk is rejected before teardown.

        Raises:
            ValueError: Workspace not found or ``tier`` is unknown.
            RuntimeError: Downgrade rejected — files exceed the target disk.
        """
        tiers = self.config.sandbox.daytona.resource_tiers
        if tier not in tiers:
            raise ValueError(f"Unknown resource tier: {tier}")

        workspace = await db_get_workspace(workspace_id)
        if not workspace:
            raise ValueError(f"Workspace {workspace_id} not found")

        current_tier_name = workspace.get("resource_tier") or "standard"
        sandbox_id = workspace.get("sandbox_id")

        if tier == current_tier_name and sandbox_id:
            # Already at this tier with a live sandbox — nothing to do.
            return workspace

        # A disk shrink risks silently dropping files that don't fit the smaller
        # sandbox (restore is best-effort). Guard those before any teardown.
        # Treat an unknown current tier as a possible downgrade (data-safe).
        target_disk = tiers[tier].disk
        current_tier = tiers.get(current_tier_name)
        is_downgrade = current_tier is None or target_disk < current_tier.disk

        try:
            async with self._observed_lock(workspace_id, "workspace.spec"):
                # Re-read under the lock — status/sandbox_id may have moved since
                # the pre-lock read (a concurrent start/stop/cleanup). Branching
                # on the locked status keeps the teardown from racing a
                # lifecycle op that owns the sandbox.
                locked = await db_get_workspace(workspace_id) or workspace
                locked_status = locked.get("status")
                locked_sandbox_id = locked.get("sandbox_id")

                # Persist the new tier inside the lock (not before it) so the
                # create/recover paths read the right size, the new tier is not
                # globally visible until we own the critical section, and the
                # compensating revert window is bounded to it. Reverted by the
                # outer except if the recreate fails so DB and platform billing
                # never claim an unreached upgrade.
                await db_set_workspace_resource_tier(workspace_id, tier)

                if not locked_sandbox_id:
                    # Never started, or the sandbox went away while we waited —
                    # the tier is persisted, so the next create/start builds it
                    # at the new size.
                    logger.info(
                        f"Workspace {workspace_id} has no sandbox; "
                        f"persisted tier {tier!r} only"
                    )
                elif locked_status == "running":
                    # Don't yank the sandbox out from under a live agent turn:
                    # execute_code keeps running in the sandbox after
                    # get_session_for_workspace returns without holding this
                    # lock, so recreating here would abort it with
                    # SandboxGoneError. Mirror the idle reaper's guard and refuse
                    # (the outer except reverts the persisted tier; maps to 400).
                    if await LocalRunExecutor.get_instance().has_active_tasks_for_workspace(
                        workspace_id
                    ):
                        raise RuntimeError(
                            "Cannot change spec while an agent turn is running; "
                            "wait for the current turn to finish"
                        )
                    # The backup and teardown below act through this process's
                    # attached session; without one (backend just restarted, or
                    # another replica owns it) the backup would silently no-op
                    # and the teardown would orphan the live sandbox — refuse
                    # instead of destroying files we never snapshotted.
                    session = self._sessions.get(workspace_id)
                    if not session or not getattr(session, "sandbox", None):
                        raise RuntimeError(
                            "Workspace is running but its session is not attached "
                            "on this server; stop the workspace first, then "
                            "change spec"
                        )
                    # Recreate in place — mirrors the sandbox-migration path.
                    logger.info(f"Recreating workspace {workspace_id} at tier {tier!r}")
                    # Strict: the sandbox is about to be destroyed, so a missed
                    # backup here is data loss, not a degraded sync.
                    await self._backup_files_to_db(workspace_id, strict=True)
                    if is_downgrade:
                        # Checked post-backup (fresh DB sizes), pre-teardown — the
                        # live sandbox is untouched, so a failure aborts cleanly.
                        await self._assert_disk_fits(workspace_id, target_disk)
                    # Tear the old sandbox down via the canonical teardown, then
                    # force-evict the SessionManager entry: cleanup_session skips
                    # its own pop when session.cleanup() raised, so the immediate
                    # _recover_sandbox get_session below must not return the stale
                    # broken session.
                    await self._clear_session(workspace_id)
                    SessionManager.remove_session(workspace_id)
                    # Belt-and-braces: if cleanup raised before deleting the
                    # sandbox, destroy it by id (mirrors the stopped path) so a
                    # half-torn-down sandbox can't keep running and billing.
                    try:
                        await self._destroy_sandbox(locked_sandbox_id)
                    except Exception as e:
                        logger.debug(
                            f"Old sandbox {locked_sandbox_id} already gone: {e}"
                        )
                    try:
                        await self._recover_sandbox(
                            workspace_id, user_id, self.config.to_core_config()
                        )
                    except Exception:
                        # Old sandbox is already torn down but the files are
                        # safely backed up — mark the row 'stopped' so the next
                        # start self-heals (claim → restart → SandboxGone →
                        # recover). 'error' would be terminal: the start path
                        # refuses it outright. The outer except still reverts
                        # the tier.
                        await update_workspace_status(
                            workspace_id=workspace_id, status="stopped"
                        )
                        raise
                elif locked_status == "stopped":
                    # Destroy the stopped sandbox so the next start recreates it
                    # from the tier snapshot (files were backed up to the DB on
                    # stop).
                    if is_downgrade:
                        await self._assert_disk_fits(workspace_id, target_disk)
                    logger.info(
                        f"Destroying stopped sandbox for {workspace_id}; "
                        f"will recreate at tier {tier!r} on next start"
                    )
                    await self._destroy_sandbox(locked_sandbox_id)
                else:
                    # Transient state (creating/starting/stopping/error) may have
                    # an in-flight op holding the sandbox — refuse rather than tear
                    # it down underneath that op (maps to 400 at the route).
                    raise RuntimeError(
                        f"Cannot change spec while workspace is {locked_status!r}; "
                        "wait for the current operation to finish"
                    )
        except Exception:
            await db_set_workspace_resource_tier(workspace_id, current_tier_name)
            raise

        return await db_get_workspace(workspace_id) or workspace

    async def _apply_autostop_for_always_on(
        self, sandbox_id: str, *, enabled: bool, runtime: Any = None
    ) -> None:
        """Sync a live sandbox's auto-stop interval to the always-on flag.

        Interval 0 (never auto-stop) when enabled, else the configured default.
        Reuses ``runtime`` when the caller already holds a connected one (the
        reconnect path) to avoid a throwaway provider + extra round-trip; else
        reaches the sandbox via a throwaway provider. No-ops if the runtime lacks
        the ``autostop`` capability.
        """
        minutes = (
            0 if enabled else self.config.sandbox.daytona.auto_stop_interval // 60
        )

        if runtime is not None:
            if "autostop" in runtime.capabilities:
                await runtime.set_autostop_interval(minutes)
            return

        from ptc_agent.core.sandbox.providers import create_provider

        provider = create_provider(self.config.to_core_config())
        try:
            runtime = await provider.get(sandbox_id)
            if "autostop" in runtime.capabilities:
                await runtime.set_autostop_interval(minutes)
        finally:
            await provider.close()

    async def set_workspace_always_on(
        self,
        workspace_id: str,
        enabled: bool,
        *,
        user_id: str | None = None,
    ) -> Dict[str, Any]:
        """Toggle a workspace's always-on flag, syncing the live auto-stop interval.

        Persists the flag, then — if the sandbox is running and the provider
        supports it — disables auto-stop when enabling (interval 0) or restores
        the configured auto-stop interval when disabling. Auto-stop is a
        persisted Daytona property that a plain reconnect does not re-assert, so
        toggling either direction on a non-running workspace is re-applied on
        its next restart (see ``_restart_workspace``).

        Raises:
            ValueError: Workspace not found.
        """
        workspace = await db_get_workspace(workspace_id)
        if not workspace:
            raise ValueError(f"Workspace {workspace_id} not found")

        await db_set_workspace_always_on(workspace_id, enabled)

        sandbox_id = workspace.get("sandbox_id")
        if workspace["status"] == "running" and sandbox_id:
            # Best-effort: the flag is already persisted and _restart_workspace
            # re-asserts auto-stop on the next start, so a transient sandbox
            # hiccup (e.g. it stopped between the read and here) must not 500
            # the toggle.
            try:
                await self._apply_autostop_for_always_on(
                    sandbox_id, enabled=enabled
                )
            except Exception as e:
                logger.warning(
                    f"Failed to apply always-on auto-stop for {workspace_id}: {e}"
                )

        return await db_get_workspace(workspace_id) or workspace

    async def duplicate_workspace(
        self,
        source_id: str,
        user_id: str,
    ) -> Dict[str, Any]:
        """Copy a workspace (files + tier) into a fresh "<name> (copy)" workspace.

        Files are persisted to the DB first when the source is running, copied to
        the new row, then a fresh sandbox is created eagerly and the files
        restored. The resource tier is carried over; always-on is forced off so
        the user re-checks the entitlement before re-enabling it.

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
        if source["status"] == "running":
            await self._backup_files_to_db(source_id)

        source_tier = source.get("resource_tier") or "standard"

        # A duplicate is a NEW allocation, so re-check the spec entitlement
        # (scope + per-tier count) for the carried tier. Without this a user
        # could clone a performance/max workspace past their quota and get a
        # free elevated sandbox. If they aren't entitled, copy at standard
        # instead of failing. No-op in OSS mode (the gate fails open).
        if source_tier != "standard":
            from src.server.dependencies.usage_limits import spec_grantable

            if not await spec_grantable(
                user_id, source_tier, current_tier="standard"
            ):
                logger.info(
                    "Duplicate of %s: user %s not entitled to tier %r; "
                    "creating copy at standard instead",
                    source_id,
                    user_id,
                    source_tier,
                )
                source_tier = "standard"

        # Carry over the source config minus the sandbox-identity stamps — those
        # belong to the source's sandbox and are re-stamped after the new one is
        # created (see create_workspace).
        source_config = dict(source.get("config") or {})
        for stamp_key in (
            "sandbox_config_hash",
            "sandbox_provider",
            "sandbox_working_dir",
        ):
            source_config.pop(stamp_key, None)

        # 1. Create the new DB row (status='creating') so the file copy has a
        #    destination, then carry over the tier (db_create_workspace does not
        #    take resource_tier — it defaults to 'standard').
        new_workspace = await db_create_workspace(
            user_id=user_id,
            name=f"{source['name']} (copy)",
            description=source.get("description"),
            config=source_config or None,
        )
        new_id = str(new_workspace["workspace_id"])
        try:
            if source_tier != "standard":
                await db_set_workspace_resource_tier(new_id, source_tier)

            # 2. Copy files (dest row now exists).
            await copy_workspace_files(source_id, new_id)
        except Exception as e:
            # Mark the fresh row error so a failed seed doesn't strand it in
            # 'creating' forever (it would count against the workspace quota
            # with no sandbox and no start path).
            logger.error(f"Failed to seed duplicated workspace {new_id}: {e}")
            await update_workspace_status(workspace_id=new_id, status="error")
            raise

        logger.info(
            f"Duplicating workspace {source_id} -> {new_id} for user {user_id}"
        )

        async with self._observed_lock(
            new_id, "workspace.duplicate", user_id=_obs_hash_id(user_id)
        ):
            try:
                # 3. Create the sandbox eagerly (mirror create_workspace). Eager
                #    is mandatory — a NULL sandbox_id would make _restart_workspace
                #    raise on the next start. The copy is built at the carried
                #    tier directly — hosted Daytona can't resize after the fact —
                #    and is not always-on (new rows default to auto-stop enabled),
                #    so no auto_stop override. Restore the copied files as the
                #    post-init step (overwrites the seeded agent.md — intended; the
                #    copy should reflect the source's content).
                async def _post_init(session: Session) -> None:
                    if session.sandbox:
                        await self._restore_files(new_id, session.sandbox)

                _session, new_workspace = await self._provision_sandbox_session(
                    new_id,
                    user_id,
                    tier=source_tier,
                    ws_version=0,
                    kick_discovery=False,
                    post_init=_post_init,
                )

                await self._update_workspace_config_fields(
                    new_id, self._sandbox_config_stamp()
                )

                logger.info(
                    f"Workspace {new_id} duplicated with sandbox "
                    f"{new_workspace.get('sandbox_id') if new_workspace else None}"
                )
                # A duplicate is a new allocation, so count it like create_workspace.
                safe_add(workspace_created, 1)
            except Exception as e:
                # The provisioning spine already tore down any half-built sandbox;
                # mark the new row error so it stops claiming 'creating'/'running'.
                logger.error(
                    f"Failed to create sandbox for duplicated workspace {new_id}: {e}"
                )
                await update_workspace_status(workspace_id=new_id, status="error")
                raise

        # The carried tier was baked into the sandbox at creation (above), so no
        # post-create resize is needed.
        return new_workspace

    async def _reconcile_always_on_entitlements(
        self, running_workspaces: list[dict]
    ) -> set[str]:
        """Disable always-on for workspaces whose owner lost the entitlement.

        This is the only periodic loop already walking always-on rows, so it
        doubles as the entitlement reconciler. Returns the set of workspace_ids
        that remain EXEMPT from idle reaping this cycle: rows still entitled (or
        the platform can't confirm otherwise, fail-safe), plus any whose disable
        failed (left flagged so a transient error doesn't yank them). A row whose
        entitlement is gone is disabled and NOT exempt, so it falls through to
        idle reaping (reaped now if idle; stops on a later tick if in use — no
        mid-use yank). The entitlement check runs once per distinct owner
        (bounded-concurrent) so several always-on workspaces for one user
        trigger a single platform validate.
        """
        from src.server.dependencies.usage_limits import always_on_entitlement_lost

        exempt: set[str] = set()

        always_on_rows = [w for w in running_workspaces if w.get("is_always_on")]
        # One platform validate per distinct owner, bounded-concurrent so a
        # large always-on fleet doesn't serialize the reaper cycle on RTTs.
        semaphore = asyncio.Semaphore(5)

        async def _probe(uid: str) -> tuple[str, bool]:
            async with semaphore:
                return uid, await always_on_entitlement_lost(uid)

        distinct_users = {str(w["user_id"]) for w in always_on_rows}
        entitlement_lost: dict[str, bool] = dict(
            await asyncio.gather(*(_probe(uid) for uid in distinct_users))
        )

        for workspace in always_on_rows:
            user_id = str(workspace["user_id"])
            workspace_id = str(workspace["workspace_id"])
            if not entitlement_lost[user_id]:
                exempt.add(workspace_id)
                continue
            # Entitlement gone (e.g. plan downgraded): clear the flag — which
            # also retires the live Daytona auto-stop.
            logger.info(
                f"Always-on entitlement lost for workspace {workspace_id} "
                f"(user {user_id}); disabling always-on"
            )
            try:
                await self.set_workspace_always_on(workspace_id, False)
            except Exception as e:
                logger.error(f"Error disabling always-on for {workspace_id}: {e}")
                # Disable failed — keep it exempt this tick rather than reap a
                # still-flagged workspace.
                exempt.add(workspace_id)

        return exempt
