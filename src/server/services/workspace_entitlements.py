"""Workspace entitlement controls: lazy tier reclaim, always-on, duplicate, and
the idle-reaper entitlement reconciliation. Mixin for WorkspaceManager.

Changing a spec on request lives in ``computer_manager._spec``."""

import asyncio
import logging
from contextlib import AsyncExitStack
from dataclasses import replace
from typing import Any, Dict

from ptc_agent.core.session import Session

from src.server.database.computer import (
    computer_capacity_lock,
    get_computer,
    set_computer_always_on as db_set_computer_always_on,
    set_computer_resource_tier as db_set_computer_resource_tier,
)
from src.server.database.home_workspace import is_flash_row
from src.server.database.workspace import (
    SANDBOX_STAMP_KEYS,
    duplicate_workspace_on_computer,
    get_workspace as db_get_workspace,
    get_workspace_name_keys,
)
from src.server.database.workspace_names import WorkspaceNameTaken, copy_name
from src.server.models.computer import ComputerStatus
from src.server.services.computer_manager._types import ComputerBinding

logger = logging.getLogger(__name__)

_COPY_NAME_ATTEMPTS = 3


class WorkspaceEntitlementsMixin:
    """Tier reclaim, always-on, duplicate, and entitlement-reconciliation methods for WorkspaceManager."""

    async def _entitled_tier(
        self,
        binding: ComputerBinding,
        user_id: str | None,
        *,
        session: Session | None = None,
        mirror_only: bool = False,
    ) -> str:
        """Resolve the tier to provision, lazily reclaiming a lapsed elevated tier.

        The tier is the machine's, so it is read and reclaimed on the computer
        row: reading a project's shadow is how one lagging row provisions a
        sandbox at a size the machine has left. Keeps the elevated size when the
        check is inconclusive (fail-safe / OSS) or the backed-up files would not
        fit the standard disk (data safety over enforcement). ``mirror_only``
        is for a recovery, which has no sandbox left to back up from.
        """
        from src.server.dependencies.usage_limits import spec_entitlement_lost

        tier = binding.resource_tier or "standard"
        if tier == "standard" or not user_id:
            return tier
        if not await spec_entitlement_lost(user_id, tier):
            return tier
        computer_id = binding.computer_id
        settings = self._provider_settings(
            binding.kind or self.config.sandbox.provider, binding.provider_config
        )
        standard = settings.resource_tiers.get("standard")
        if standard is None:
            logger.warning("No standard tier for computer %s; keeping %s", computer_id, tier)
            return tier
        try:
            # The mirror is measured before the strict backup that refreshes
            # it: one that already overflows standard keeps the size without a
            # full backup on every start, and space freed since counts from the
            # next mirror.
            await self._assert_machine_disk_fits(computer_id, standard.disk)
            if binding.provider_ref and not mirror_only:
                await self._backup_machine_files_to_db(
                    computer_id,
                    strict=True,
                    expected_sandbox_id=binding.provider_ref,
                    session=session,
                )
                await self._assert_machine_disk_fits(computer_id, standard.disk)
        except RuntimeError as e:
            logger.warning(
                f"Spec entitlement lost for computer {computer_id} "
                f"(user {user_id}, tier {tier!r}) but a complete backup "
                f"fitting the standard disk is unavailable; keeping size: {e}"
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
        # The folder is the workspace's, not a machine column; it moves only in
        # an acquisition's settle, before this binding was resolved, so it
        # rides across the refresh.
        fresh = replace(
            self._binding_from_computer(binding.workspace_id, computer),
            dir_name=binding.dir_name,
        )
        tier = fresh.resource_tier or "standard"
        if tier == "standard":
            return None
        entitled_tier = await self._entitled_tier(fresh, user_id, session=session)
        if entitled_tier == tier:
            return None
        fresh = replace(fresh, resource_tier=entitled_tier)
        sandbox_id = fresh.provider_ref
        if sandbox_id:
            try:
                await self._destroy_sandbox(sandbox_id, binding=fresh)
            except Exception as e:
                logger.warning(
                    f"Failed to destroy outsized sandbox {sandbox_id} "
                    f"for computer {computer_id}; replacement aborted: {e}"
                )
                raise
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
        settings = (
            self._provider_settings(binding.kind, binding.provider_config)
            if binding is not None and binding.kind
            else self.config.sandbox.daytona
        )
        interval = getattr(
            settings, "auto_stop_interval", self.config.sandbox.daytona.auto_stop_interval
        )
        minutes = 0 if enabled else interval // 60

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
        *,
        user_id: str | None = None,
    ) -> Dict[str, Any]:
        """Project-addressed entry for the deprecated workspace always-on route."""
        binding = await self.resolve_binding(workspace_id)
        await self.set_computer_always_on(
            binding.computer_id, enabled, user_id=user_id
        )
        return await db_get_workspace(workspace_id) or {}

    async def set_computer_always_on(
        self,
        computer_id: str,
        enabled: bool,
        *,
        user_id: str | None = None,
    ) -> Dict[str, Any]:
        """Toggle a computer's always-on flag, syncing the live auto-stop interval.

        Auto-stop is a persisted Daytona property of the machine's sandbox that a
        plain reconnect does not re-assert, so toggling either direction on a
        machine that is not running is re-applied on its next restart (see
        ``_restart_workspace``).

        Raises:
            ValueError: Computer not found.
        """
        async with self._observed_lock(computer_id, "computer.always_on"):
            async with AsyncExitStack() as stack:
                computer = await get_computer(computer_id)
                if not computer:
                    raise ValueError(f"Computer {computer_id} not found")
                owner_id = str(computer["user_id"])
                from src.server.dependencies.usage_limits import (
                    assert_always_on_allowed,
                    platform_gating_active,
                )

                if enabled and platform_gating_active():
                    await stack.enter_async_context(computer_capacity_lock(owner_id))
                    computer = await get_computer(computer_id)
                    if not computer:
                        raise ValueError(f"Computer {computer_id} not found")
                    if not computer.get("is_always_on"):
                        await assert_always_on_allowed(owner_id)
                acquired = await stack.enter_async_context(
                    self._machine_decision_lock(computer_id)
                )
                if not acquired:
                    raise RuntimeError("Computer is busy; retry the always-on change")
                return await self._set_computer_always_on_locked(computer_id, enabled)

    async def _set_computer_always_on_locked(
        self,
        computer_id: str,
        enabled: bool,
    ) -> Dict[str, Any]:
        """Persist and apply one always-on decision under the machine lock."""
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
        """Copy a workspace's files into a fresh workspace named "<name> (copy)", or
        "<name> (copy 2)" and on when that name is taken.

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
        if is_flash_row(source):
            raise ValueError("Cannot duplicate a flash workspace")

        # Files only persist to the DB on stop/delete, so flush a running source
        # before the copy or the new workspace would miss in-sandbox changes.
        # We already hold the row, so hand the durable id over rather than
        # making the backup re-read it.
        source_binding = await self.resolve_binding(source_id, workspace=source)
        if source["status"] == "running":
            # The request may land on a worker that has never served this
            # computer. Reconnect before taking the strict mirror so a copy
            # cannot silently fall back to stale persisted bytes.
            await self.get_session_for_computer(
                source_binding.computer_id,
                user_id=user_id,
            )
            await self.backup_project_files(
                source_id,
                computer_id=source_binding.computer_id,
                expected_sandbox_id=source_binding.provider_ref,
                strict=True,
            )

        # Carry over the source config minus the sandbox-identity stamps — those
        # belong to the source's sandbox and are re-stamped when the machine is
        # next provisioned.
        source_config = dict(source.get("config") or {})
        for stamp_key in SANDBOX_STAMP_KEYS:
            source_config.pop(stamp_key, None)

        for attempt in range(_COPY_NAME_ATTEMPTS):
            try:
                new_workspace = await duplicate_workspace_on_computer(
                    source_id,
                    user_id,
                    copy_name(source["name"], await get_workspace_name_keys(user_id)),
                    source_binding.computer_id,
                    description=source.get("description"),
                    config=source_config or None,
                )
                break
            except WorkspaceNameTaken:
                # Another duplicate took the same copy name between the read
                # and the insert; the next read sees it.
                if attempt == _COPY_NAME_ATTEMPTS - 1:
                    raise
        if new_workspace is None:
            raise RuntimeError("Computer was removed before the duplicate was created")
        await self.resolve_binding(str(new_workspace["workspace_id"]), workspace=new_workspace)
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
