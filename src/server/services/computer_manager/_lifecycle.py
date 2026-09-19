"""Seam: the workspace-facing start, stop and restart state machine.

One file of the ComputerManager split; see the package __init__."""

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any, Dict, Optional

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from ptc_agent.core.session import Session

from src.server.services.runs.executor import LocalRunExecutor

from src.observability import (
    safe_add,
    safe_record,
    session_acquire_phase_duration_ms,
    session_acquire_total_ms,
    session_path_counter,
    workspace_cold_start_duration_ms,
)

from src.server.database.computer import (
    get_computer,
    try_bind_computer_provider_ref,
    try_claim_computer_for_start,
    update_computer_activity,
)
from src.server.database.workspace import (
    adopt_computer_sandbox_into_workspaces,
    get_workspace as db_get_workspace,
    SandboxIdentityLostError,
    update_workspace_activity,
    update_workspace_status,
)
from src.server.models.computer import CLAIMABLE_FOR_START, ComputerStatus
from src.server.services.computer_manager._types import ComputerBinding
from src.server.services.path_lock_coordination import (
    computer_stop_heartbeat,
    computer_stop_heartbeat_present,
)
from src.server.services.workspace_status_pubsub import (
    publish_status_change,
    subscribe_to_status,
)


logger = logging.getLogger(__name__)


class SessionLifecycleMixin:
    async def _machine_has_active_tasks(
        self, computer_id: str, *, workspace_id: str | None = None
    ) -> bool:
        """An idle project cannot authorize taking away a shared sandbox."""
        executor = LocalRunExecutor.get_instance()
        return await executor.has_active_tasks_for_computer(
            computer_id, workspace_id=workspace_id
        )

    async def _bind_machine_identity(
        self,
        binding: ComputerBinding,
        *,
        sandbox_id: str,
        expected_previous_sandbox_id: str | None,
        platform_secret_version: int,
    ) -> Optional[Dict[str, Any]]:
        """The computers.provider_ref CAS elects one provisioner.

        Workspace shadows share that statement, so the project row this returns
        is the one the CAS winner wrote. A None result requires the loser to
        delete its sandbox."""
        workspace_id = binding.workspace_id
        computer_id = binding.computer_id
        computer = await try_bind_computer_provider_ref(
            computer_id,
            provider_ref=sandbox_id,
            expected_previous_provider_ref=expected_previous_sandbox_id,
            platform_secret_version=platform_secret_version,
        )
        if computer is None:
            return None
        self._warn_unshadowed(workspace_id, computer, "bind")
        bound = await db_get_workspace(workspace_id)
        if bound is None:
            logger.warning(
                f"Computer {computer_id} bound {sandbox_id} but workspace "
                f"{workspace_id} is gone; unwinding the sandbox"
            )
        return bound

    async def _claim_machine_for_start(
        self, binding: ComputerBinding, *, from_status: str
    ) -> Optional[Dict[str, Any]]:
        """Claim on the machine so sibling projects cannot start it twice.

        ``from_status`` is the state the caller saw, so a shadow that lagged
        loses its CAS instead of starting a machine that has moved on."""
        workspace_id = binding.workspace_id
        computer = await try_claim_computer_for_start(
            binding.computer_id, from_status=from_status
        )
        if computer is None:
            return None
        self._warn_unshadowed(workspace_id, computer, "start claim")
        claimed = await db_get_workspace(workspace_id)
        if claimed is None:
            logger.warning(
                f"Computer {binding.computer_id} claimed for start but workspace "
                f"{workspace_id} is gone; leaving it to the reaper"
            )
        return claimed

    async def _acquire_session(
        self,
        workspace_id: str,
        user_id: str | None = None,
        on_state_observed: Callable[[str], None] | None = None,
        skills_signature: str | None = None,
        _attempt: int = 0,
    ) -> Session:
        """Sizing and always-on belong to create/restart, not post-start checks.

        Observe reconnect state for archived-restore SSE without an extra SDK probe.
        A changed skills_signature bypasses cooldown so warm sandboxes converge;
        None skips only that comparison, while transitions still sync."""
        _t0 = time.time()
        _session_phases: dict[str, float] = {}

        def _mark(name: str) -> None:
            nonlocal _t0
            now = time.time()
            _session_phases[name] = (now - _t0) * 1000
            _t0 = now

        binding = await self.resolve_binding(workspace_id)
        computer_id = binding.computer_id
        machine = self._machine(computer_id)
        _was_cached = machine.session is not None

        session: Session | None = None
        needs_sync = False
        needs_deferred_sync = False
        pending_start_wait = False
        pending_stop_wait = False
        workspace_user_id = user_id
        # Versions piggyback the slow-path row read; early warm returns leave None.
        ws_mcp_version: int | None = None
        ws_platform_secret_version: int | None = None

        async with self._observed_lock(
            computer_id, "workspace.session.acquire", cached_on_entry=_was_cached
        ):
            session = await self._take_valid_cached_session(binding, _mark)
            if session is not None:
                if not session.sandbox.is_ready():
                    if session.sandbox.has_failed():
                        init_err = session.sandbox.init_error
                        logger.warning(
                            f"Lazy init failed for workspace {workspace_id}: "
                            f"{init_err}. Clearing session for recovery."
                        )
                        await self._clear_session(computer_id)

                        if isinstance(init_err, SandboxGoneError):
                            recovered = await self._recover_sandbox(
                                binding,
                                workspace_user_id,
                                self._core_config_for(binding),
                            )
                            return recovered
                        session = None
                    else:
                        # No recreation on this warm path, so no entitlement recheck is needed.
                        logger.info(
                            f"Sandbox still initializing for {workspace_id}, "
                            f"skipping sync"
                        )
                        safe_add(session_path_counter, 1, {"path": "warm_initializing"})
                        return session
                else:
                    needs_deferred_sync = machine.pending_lazy_sync
                    # Changed skills are non-redundant work and must bypass cooldown.
                    skills_stale = (
                        skills_signature is not None
                        and session.skills_signature != skills_signature
                    )
                    superseded_resolve = workspace_id in machine.resolve_superseded
                    machine.resolve_superseded.discard(workspace_id)
                    workspace_view = self._workspace_tool_view(
                        computer_id, workspace_id, session
                    )
                    if workspace_view is None:
                        workspace_view = self._freeze_tool_view(
                            computer_id, workspace_id, session
                        )
                    needs_sync = (
                        workspace_view is None
                        or not self._sync_cooldown_ok(computer_id, workspace_id)
                        or needs_deferred_sync
                        or skills_stale
                        or superseded_resolve
                    )
                    if not needs_sync:
                        # JWT age is an in-memory check unless near expiry, about once per 1.5h.
                        # No recreation means no tier recheck.
                        await self._apply_session_mcp(
                            binding,
                            workspace_user_id,
                            session,
                            ws_version=workspace_view.mcp_config_version,
                        )
                        self._freeze_tool_view(computer_id, workspace_id, session)
                        safe_add(session_path_counter, 1, {"path": "warm_cooldown"})
                        return session

            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            status = workspace["status"]
            sandbox_id_from_db = workspace.get("sandbox_id")
            workspace_user_id = workspace.get("user_id") or user_id
            ws_mcp_version = (
                int(workspace.get("mcp_config_version") or 0)
                if workspace.get("mcp_config_version") is not None
                else 0
            )
            ws_platform_secret_version = int(
                workspace.get("platform_secret_version") or 0
            )
            logger.debug(
                f"Workspace {workspace_id} from DB: status={status}, sandbox_id={sandbox_id_from_db}, user_id={workspace_user_id}"
            )

            if status == "deleted":
                raise RuntimeError(f"Workspace {workspace_id} has been deleted")
            if status == "error":
                raise RuntimeError(
                    f"Workspace {workspace_id} is in error state. "
                    "Please delete and recreate."
                )

            if session is None:
                if status in CLAIMABLE_FOR_START or status == ComputerStatus.STARTING:
                    # Claim under the lock, but wait outside it: a 60-300s archived restore
                    # would block all workspace operations beyond the 60s lock-acquire ceiling.
                    # The claimable set is the machine's, not this row's: a
                    # project whose machine sits at 'creating' is startable for
                    # the same reason the computer route finds it startable, and
                    # refusing it here is what left a crashed create unusable
                    # from the only page that addresses it.
                    if status != ComputerStatus.STARTING:
                        session = await self._claim_and_restart(
                            binding,
                            workspace_user_id,
                            on_state_observed,
                            from_status=status,
                        )
                    if session is not None:
                        needs_sync = True
                        needs_deferred_sync = True
                    else:
                        pending_start_wait = True

                elif status == "running":
                    session, did_init = await self._attach_running_session(
                        binding,
                        workspace,
                        workspace_user_id,
                        on_state_observed,
                        _mark,
                    )
                    if not did_init:
                        needs_sync = True

                elif status == "stopping":
                    # Settling takes seconds and may need a provider probe, so
                    # it waits outside the machine lock: holding it here blocks
                    # every sibling project on the machine meanwhile.
                    pending_stop_wait = True

                elif status == "flash":
                    raise ValueError(
                        f"Workspace {workspace_id} is a flash workspace (no sandbox). "
                        "Use agent_mode='flash' instead, or create a new workspace for PTC mode."
                    )

                else:
                    raise RuntimeError(f"Unknown workspace status: {status}")

            # Install the Phase 2 gate under lock so same-worker callers cannot both own it.
            phase2_owner = False
            phase2_event: Optional[asyncio.Event] = None
            if needs_sync and session is not None and session.sandbox is not None:
                existing_event = machine.phase2_event
                if existing_event is not None and not existing_event.is_set():
                    phase2_event = existing_event
                else:
                    phase2_event = asyncio.Event()
                    machine.phase2_event = phase2_event
                    phase2_owner = True

        if pending_stop_wait:
            return await self._await_stop_and_retry(
                binding,
                user_id=user_id,
                workspace_user_id=workspace_user_id,
                on_state_observed=on_state_observed,
                attempt=_attempt,
            )

        # Wait outside the lock so a 60-300s restore cannot block other operations.
        if pending_start_wait:
            # Attach/retry applies sizing and always-on at create/restart; no extra check.
            attached = await self._await_in_flight_start(
                binding,
                user_id=user_id,
                workspace_user_id=workspace_user_id,
                on_state_observed=on_state_observed,
                mark=_mark,
                attempt=_attempt,
            )
            return attached

        # Expensive sync is idempotent or self-guarded, so it runs outside the lock.
        _mark("lock_and_init")
        session = await self._complete_phase2_sync(
            binding,
            session,
            workspace_user_id=workspace_user_id,
            needs_sync=needs_sync,
            needs_deferred_sync=needs_deferred_sync,
            phase2_owner=phase2_owner,
            phase2_event=phase2_event,
            mark=_mark,
            ws_mcp_version=ws_mcp_version,
            ws_platform_secret_version=ws_platform_secret_version,
            skills_signature=skills_signature,
        )

        if _session_phases:
            total = sum(_session_phases.values())
            phases = " ".join(f"{k}={v:.0f}ms" for k, v in _session_phases.items())
            logger.info(
                f"[SESSION_TIMING] workspace_id={workspace_id} total={total:.0f}ms ({phases})"
            )
            if needs_deferred_sync:
                session_path = "cold_resume"
            elif _was_cached:
                session_path = "warm_sync"
            else:
                session_path = "cold_create"
            safe_add(session_path_counter, 1, {"path": session_path})
            safe_record(session_acquire_total_ms, total, {"session_path": session_path})
            for _phase, _ms in _session_phases.items():
                safe_record(
                    session_acquire_phase_duration_ms,
                    _ms,
                    {"phase": _phase, "session_path": session_path},
                )

        return session

    async def _await_stop_and_retry(
        self,
        binding: ComputerBinding,
        *,
        user_id: str | None,
        workspace_user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
        attempt: int,
    ) -> Session:
        """Let a stop finish, then acquire again from whatever state it left.

        Runs with no lock held, so the retry is a plain re-entry into
        acquisition instead of the hand-built session the in-lock version had
        to assemble for a corrected row."""
        workspace_id = binding.workspace_id
        logger.info(f"Workspace {workspace_id} is stopping, waiting for it to finish")
        settled = False
        for _ in range(20):
            await asyncio.sleep(0.5)
            computer = await get_computer(binding.computer_id)
            if computer is None:
                raise RuntimeError(
                    f"Computer {binding.computer_id} was deleted while "
                    f"workspace {workspace_id} waited for its stop"
                )
            if computer["status"] != ComputerStatus.STOPPING:
                settled = True
                break

        if not settled:
            await self._correct_stuck_stopping(binding)

        if attempt > 0:
            # One retry only: a machine that is still stopping after a
            # correction is a state to report, not to spin on.
            raise RuntimeError(
                f"Workspace {workspace_id} is still stopping after a recovery "
                "attempt. Please wait and try again."
            )
        return await self._acquire_session(
            workspace_id,
            user_id=user_id or workspace_user_id,
            on_state_observed=on_state_observed,
            _attempt=attempt + 1,
        )

    async def _correct_stuck_stopping(self, binding: ComputerBinding) -> None:
        """A process can die mid-stop, so ask the provider before trusting the row.

        The correction is the same CAS from 'stopping' that a real stop settles
        with, which is what keeps it from stomping a peer that has meanwhile
        moved the machine itself."""
        computer_id = binding.computer_id
        if await computer_stop_heartbeat_present(computer_id):
            logger.info(
                f"Computer {computer_id} is still owned by its stopper; "
                "leaving the stopping row in place"
            )
            return
        sandbox_id = binding.provider_ref
        if not sandbox_id:
            # Nothing to probe: the stop had no sandbox to take down, so the
            # row is settled to 'stopped' and a start rebuilds from scratch.
            await self._settle_machine_stop(computer_id, ComputerStatus.STOPPED)
            return
        try:
            async with self._detached_runtime(sandbox_id, binding=binding) as runtime:
                actual_state = await runtime.get_state()
        except SandboxGoneError as e:
            logger.warning(
                f"Sandbox {sandbox_id} is gone for computer {computer_id} during "
                f"stopping-state recovery ({e}); settling the row to 'stopped'"
            )
            await self._settle_machine_stop(computer_id, ComputerStatus.STOPPED)
            return

        state = actual_state.value
        logger.warning(
            f"Computer {computer_id} is stuck in 'stopping' but sandbox "
            f"{sandbox_id} is actually {state!r}; correcting the row"
        )
        # Only a settled provider state authorizes a correction; a transition in
        # flight is left to finish and be re-read on the next request.
        if state in ("stopped", "archived"):
            await self._settle_machine_stop(computer_id, ComputerStatus.STOPPED)
        elif state == "running":
            await self._settle_machine_stop(computer_id, ComputerStatus.RUNNING)
            # The idle sweep reads this stamp, and a just-corrected machine must
            # not look like it has been idle for the whole stuck window.
            await update_computer_activity(computer_id)
        else:
            raise RuntimeError(
                f"Workspace {binding.workspace_id} sandbox is in transient "
                f"state {state!r}. Please wait and try again."
            )

    async def _await_in_flight_start(
        self,
        binding: ComputerBinding,
        *,
        user_id: str | None,
        workspace_user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
        mark: Callable[[str], None],
        attempt: int,
    ) -> Session:
        """Wait outside the machine lock so archived restores cannot block other ops."""
        workspace_id = binding.workspace_id
        ws_done = await self._wait_for_start_completion(workspace_id)
        wait_status = ws_done["status"]
        if wait_status == "running":
            async with self._observed_lock(
                binding.computer_id, "workspace.session.attach"
            ):
                session, _ = await self._attach_running_session(
                    binding, ws_done, workspace_user_id, on_state_observed, mark
                )
            return session
        if wait_status == "stopped" and attempt == 0:
            # Retry full acquisition once to reuse claim, sync and promotion after owner failure.
            logger.info(
                f"Workspace {workspace_id} reverted to 'stopped' "
                "(prior owner failed); retrying start"
            )
            return await self._acquire_session(
                workspace_id,
                user_id=user_id,
                on_state_observed=on_state_observed,
                _attempt=attempt + 1,
            )
        raise RuntimeError(
            f"Workspace {workspace_id} ended start in unexpected "
            f"status '{wait_status}' after waiting"
        )

    async def _complete_phase2_sync(
        self,
        binding: ComputerBinding,
        session: Session | None,
        *,
        workspace_user_id: str | None,
        needs_sync: bool,
        needs_deferred_sync: bool,
        phase2_owner: bool,
        phase2_event: Optional[asyncio.Event],
        mark: Callable[[str], None],
        ws_mcp_version: int | None = None,
        ws_platform_secret_version: int | None = None,
        skills_signature: str | None = None,
    ) -> Session | None:
        """Sync outside the machine lock; operations are idempotent or self-guarded.

        Same-worker waiters coalesce on phase2_event but trust the DB after waking.
        Promote lazy starts only when fully ready; revert failures to stopped so
        callers never receive a half-ready session."""
        workspace_id = binding.workspace_id
        computer_id = binding.computer_id
        machine = self._machine(computer_id)
        if needs_sync and session and session.sandbox:
            if not phase2_owner:
                if phase2_event is None:
                    # A non-owner without a gate has nothing to wait on, which
                    # only the caller's own bookkeeping can produce.
                    raise RuntimeError(
                        f"Phase 2 for workspace {workspace_id} has no gate to "
                        "wait on and does not own one"
                    )
                try:
                    await asyncio.wait_for(
                        phase2_event.wait(),
                        timeout=self.start_wait_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Phase 2 wait timed out for workspace %s after %.0fs",
                        workspace_id,
                        self.start_wait_timeout,
                    )
                mark("phase2_wait")
                # Trust the owner's DB status; its cache may remain stale or half-initialized
                # after failure.
                ws_after = await db_get_workspace(workspace_id)
                if ws_after is not None and ws_after["status"] == "running":
                    cached = self._cached_session(computer_id)
                    active = cached if cached is not None else session
                    version = int(ws_after.get("mcp_config_version") or 0)
                    resolved_mcp = await self._apply_session_mcp(
                        binding,
                        workspace_user_id,
                        active,
                        ws_version=version,
                    )
                    self._freeze_tool_view(computer_id, workspace_id, active)
                    if resolved_mcp is not None:
                        await self._sync_sandbox_assets(
                            binding,
                            workspace_user_id,
                            active.sandbox,
                            reusing_sandbox=True,
                        )
                        view = self.tool_view(active, workspace_id)
                        self._kick_mcp_discovery(
                            binding,
                            workspace_user_id,
                            active,
                            self._servers_needing_discovery(
                                active,
                                resolved_mcp,
                                workspace_id=workspace_id,
                            ),
                            view.mcp_config_version or 0,
                        )
                    self._record_sync(computer_id, workspace_id)
                    return active
                raise RuntimeError(
                    f"Workspace {workspace_id} did not reach 'running' after "
                    f"Phase 2 (status={ws_after['status'] if ws_after else 'deleted'})"
                )

            try:
                # Reclaim outside the Phase 1 lock to avoid blocking other operations during
                # reprovision. Entitlement checks overlap background start; Phase 2 gates
                # return so callers never receive the outsized sandbox.
                if (
                    needs_deferred_sync
                    and workspace_user_id
                    and machine.pending_tier_recheck
                ):
                    reclaimed = await self._maybe_reclaim_lazy_tier(
                        binding, workspace_user_id, session
                    )
                    if reclaimed is not None:
                        mark("tier_reclaim")
                        return reclaimed

                await session.sandbox.ensure_sandbox_ready()
                mark("sandbox_ready")

                # Resync before MCP/assets so warm and always-on sandboxes receive secret
                # rollouts within one cooldown; the session stamp makes unchanged sync free.
                await self._apply_session_platform_secret(
                    binding, session, ws_version=ws_platform_secret_version
                )
                mark("platform_secret")

                # Install before asset sync so codegen sees the effective MCP registry.
                # Piggyback ws_mcp_version to avoid extra reads for unchanged configs.
                _t_resolve = time.time()
                resolved_mcp = await self._apply_session_mcp(
                    binding,
                    workspace_user_id,
                    session,
                    ws_version=ws_mcp_version,
                )
                self._freeze_tool_view(computer_id, workspace_id, session)
                mcp_changed = resolved_mcp is not None
                if mcp_changed:
                    view = self.tool_view(session, workspace_id)
                    logger.info(
                        "[ASSET_SYNC] workspace_id=%s mcp_resolve=%.0fms version=%s",
                        workspace_id,
                        (time.time() - _t_resolve) * 1000,
                        view.mcp_config_version,
                    )
                    mark("mcp_resolve")

                if needs_deferred_sync:
                    logger.debug(
                        f"Completing deferred sync for lazy-init workspace {workspace_id}"
                    )
                    await self._sync_sandbox_assets(
                        binding,
                        workspace_user_id,
                        session.sandbox,
                        reusing_sandbox=True,
                    )
                    mark("asset_sync")
                    await self._maybe_restore_files(binding, session.sandbox)
                    mark("file_restore")
                    await self._reconcile_skills(
                        binding,
                        workspace_user_id,
                        session.sandbox,
                        source="lazy_phase2",
                    )
                    mark("skill_reconcile")
                    # Defer interval writes until ready to avoid racing archived startup.
                    # An auto-stop hiccup must not revert an otherwise healthy start.
                    # Re-read the machine rather than the binding frozen at turn
                    # start, and the machine rather than the project: always-on
                    # and the sandbox id are the computer's, and the workspace
                    # columns are shadows of them.
                    fresh = await get_computer(binding.computer_id)
                    if fresh and fresh.get("provider_ref"):
                        try:
                            await self._apply_autostop_for_always_on(
                                fresh["provider_ref"],
                                enabled=bool(fresh.get("is_always_on")),
                                runtime=(
                                    getattr(session.sandbox, "runtime", None)
                                    if session.sandbox
                                    else None
                                ),
                                binding=binding,
                            )
                        except Exception as e:
                            logger.warning(
                                f"Failed to re-assert auto-stop interval for "
                                f"{workspace_id} after lazy start: {e}"
                            )
                    # Publish running only after readiness, assets and files are complete, so
                    # SSE close means usable. Non-lazy starts already promoted and are not pending.
                    if machine.pending_lazy_sync:
                        await update_workspace_status(
                            workspace_id=workspace_id,
                            status="running",
                        )
                        await update_workspace_activity(workspace_id)
                        machine.pending_lazy_sync = False
                elif mcp_changed or (
                    skills_signature is not None
                    and session.skills_signature != skills_signature
                ):
                    # Warm sessions still need changed MCP wrappers or skills uploaded; manifest
                    # diffs bound that work without repeating restore or promotion.
                    await self._sync_sandbox_assets(
                        binding,
                        workspace_user_id,
                        session.sandbox,
                        reusing_sandbox=True,
                    )
                    mark("mcp_asset_sync")

                # Stamp only after successful sync so failures retain the retry signal.
                if skills_signature is not None:
                    session.skills_signature = skills_signature

                # Keep up-to-30s discovery off the turn and lock; completion syncs new wrappers.
                if resolved_mcp is not None:
                    view = self.tool_view(session, workspace_id)
                    needing = self._servers_needing_discovery(
                        session, resolved_mcp, workspace_id=workspace_id
                    )
                    self._kick_mcp_discovery(
                        binding,
                        workspace_user_id,
                        session,
                        needing,
                        view.mcp_config_version or 0,
                    )

                self._record_sync(computer_id, workspace_id)
            except SandboxGoneError as e:
                logger.warning(
                    f"Sandbox gone for workspace {workspace_id} during "
                    f"Phase 2: {e}. Recovering."
                )
                # Phase 2 can race replacement. Check identity here and across cleanup
                # awaits via evict_session, or a healthy replacement could be destroyed.
                if self._cached_session(computer_id) is session:
                    await self._clear_session(computer_id, evict_session=session)

                async with self._acquire_machine_lock(computer_id):
                    # Another request may have recovered while this one waited for the lock.
                    existing = self._cached_session(computer_id)
                    if existing and existing.sandbox and existing.sandbox.is_ready():
                        return existing
                    return await self._recover_sandbox(
                        binding,
                        workspace_user_id,
                        self._core_config_for(binding),
                    )
            except SandboxTransientError as e:
                # Failed initialization needs eviction; post-init transients leave a healthy
                # sandbox and can retry later.
                if session.sandbox.has_failed():
                    logger.warning(
                        f"Phase 2 init exhausted retries for {workspace_id}: "
                        f"{e}. Clearing session for fresh recovery."
                    )
                    # Revert before cleanup drops pending_lazy_sync, or reversion becomes a
                    # no-op. Guard identity here and across cleanup awaits so a concurrent
                    # replacement survives.
                    await self._revert_unpromoted_lazy_start(binding)
                    if self._cached_session(computer_id) is session:
                        await self._clear_session(computer_id, evict_session=session)
                    raise
                logger.warning(
                    f"Phase 2 sync transient for workspace {workspace_id} "
                    f"(will retry next request): {e}"
                )
                # Capture before reverting, the revert clears pending_lazy_sync.
                was_unpromoted_lazy = machine.pending_lazy_sync
                await self._revert_unpromoted_lazy_start(binding)
                if was_unpromoted_lazy:
                    # Returning a healthy sandbox after reverting its row to stopped lets
                    # another worker claim and spawn a duplicate; force clean reacquisition.
                    raise
                # An already-running sandbox remains usable; retry periodic sync next request.
            except asyncio.CancelledError:
                # CancelledError bypasses Exception handlers. Shield reversion so cancellation
                # cannot strand starting; reap_stuck_starting_workspaces is the backstop if
                # the event loop dies before the DB write.
                revert = asyncio.create_task(
                    self._revert_unpromoted_lazy_start(binding)
                )
                try:
                    await asyncio.shield(revert)
                except asyncio.CancelledError:
                    pass
                raise
            except Exception as e:
                logger.warning(f"Phase 2 sync failed for workspace {workspace_id}: {e}")
                # Capture before reverting, the revert clears pending_lazy_sync.
                was_unpromoted_lazy = machine.pending_lazy_sync
                await self._revert_unpromoted_lazy_start(binding)
                if was_unpromoted_lazy:
                    # Never return a half-synced sandbox whose row was reverted to stopped;
                    # raise so the caller reclaims cleanly.
                    raise
                # An already-running sandbox remains usable; retry periodic sync next request.
            finally:
                # Wake waiters without evicting a newer caller's event from the registry.
                self._release_phase2_gate(computer_id, phase2_event)
        elif phase2_owner and phase2_event is not None:
            # Concurrent stop may remove the sandbox after gate installation. Release
            # the orphaned gate or waiters exhaust start_wait_timeout with no owner.
            self._release_phase2_gate(computer_id, phase2_event)

        return session

    def _release_phase2_gate(
        self, computer_id: str, phase2_event: Optional[asyncio.Event]
    ) -> None:
        """Wake waiters without evicting a newer caller's event from the registry."""
        if phase2_event is None:
            return
        phase2_event.set()
        machine = self._machine_if_known(computer_id)
        if machine is not None and machine.phase2_event is phase2_event:
            machine.phase2_event = None

    async def _revert_unpromoted_lazy_start(self, binding: ComputerBinding) -> None:
        """Release failed lazy-start claims so other workers can retry immediately.

        Waiters otherwise spend start_wait_timeout on a stranded starting row.
        pending_lazy_sync restricts reversion to unpromoted lazy starts."""
        workspace_id = binding.workspace_id
        machine = self._machine_if_known(binding.computer_id)
        if machine is None or not machine.pending_lazy_sync:
            return
        machine.pending_lazy_sync = False
        try:
            await update_workspace_status(workspace_id=workspace_id, status="stopped")
        except Exception:
            # Do not mask the sync failure; a failed revert leaves waiters to time out.
            logger.exception(
                "Failed to revert workspace %s to 'stopped' after Phase 2 failure",
                workspace_id,
            )

    async def _attach_running_session(
        self,
        binding: ComputerBinding,
        workspace: Dict[str, Any],
        workspace_user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
        mark: Callable[[str], None],
    ) -> tuple[Session, bool]:
        """The caller must hold the machine's _observed_lock.

        A false did_init requires Phase 2 sync of the already-initialized session."""
        workspace_id = binding.workspace_id
        computer_id = binding.computer_id
        core_config = self._core_config_for(binding)
        session = self._session_handle(binding, core_config)
        did_init = False

        # A joining project with no ref adopts its running machine's ref; rebuilding
        # would disrupt siblings already using that sandbox.
        sandbox_id = workspace.get("sandbox_id")
        if sandbox_id is None and binding.provider_ref is not None:
            await adopt_computer_sandbox_into_workspaces(computer_id)
            sandbox_id = binding.provider_ref
            workspace = {**workspace, "sandbox_id": sandbox_id}

        # A computer CAS can win while its shadow fails and the loser deletes its
        # sandbox. Conflicting refs require recreation, not a guessed reconnect.
        if binding.provider_ref is not None and binding.provider_ref != sandbox_id:
            logger.warning(
                f"Computer/workspace binding is split for {workspace_id} "
                f"(computer={binding.provider_ref}, workspace={sandbox_id}); "
                "recreating the sandbox",
                extra={"workspace_id": workspace_id},
            )
            await self._clear_session(computer_id)
            recovered = await self._recover_sandbox(
                binding, workspace_user_id, core_config
            )
            return recovered, True

        # Stop preserves SessionManager for restart. Validate its initialized handle
        # before caching it again, or a replaced sandbox bypasses reinitialization.
        if session._initialized:
            stale_reason = self._identity_is_stale(binding, session, dict(workspace))
            if stale_reason is not None:
                logger.warning(
                    f"Discarding stale SessionManager session for "
                    f"{workspace_id} on attach ({stale_reason})",
                    extra={"workspace_id": workspace_id, "reason": stale_reason},
                )
                await self._retire_session(computer_id, session, reason=stale_reason)
                safe_add(session_path_counter, 1, {"path": "stale_reattach"})
                session = self._session_handle(binding, core_config)

        if not session._initialized:
            try:
                await session.initialize(
                    sandbox_id=sandbox_id,
                    on_state_observed=on_state_observed,
                )
            except SandboxGoneError as e:
                await self._clear_session(computer_id)
                logger.warning(
                    f"Sandbox {sandbox_id} unavailable for workspace "
                    f"{workspace_id} ({e}). Creating fresh sandbox."
                )
                recovered = await self._recover_sandbox(
                    binding, workspace_user_id, core_config
                )
                return recovered, True
            mark("session_initialize")

            # Apply the secret rollout before any further work in the reattached sandbox.
            await self._apply_session_platform_secret(
                binding,
                session,
                ws_version=int(workspace.get("platform_secret_version") or 0),
            )
            mark("platform_secret")

            # initialize builds when no ref exists. Bind that sandbox or every next
            # request sees NULL as stale, retires it and leaks another billed sandbox.
            live_sandbox_id = self._session_sandbox_id(session)
            if live_sandbox_id and live_sandbox_id != sandbox_id:
                from src.server.services.platform_secret_rollout import (
                    certify_platform_secrets,
                )

                # Resync above enables certification; certify before any row names this sandbox.
                secret_version = await certify_platform_secrets(
                    core_config, runtime=session.sandbox.runtime
                )
                bound = await self._bind_machine_identity(
                    binding,
                    sandbox_id=live_sandbox_id,
                    expected_previous_sandbox_id=sandbox_id,
                    platform_secret_version=secret_version,
                )
                if bound is None:
                    # A losing provision is billed and unreferenced; destroy it before retrying attach.
                    logger.warning(
                        f"Lost the sandbox-identity race for {workspace_id} on "
                        f"attach; discarding our sandbox {live_sandbox_id}",
                        extra={
                            "workspace_id": workspace_id,
                            "sandbox_id": live_sandbox_id,
                        },
                    )
                    await self._clear_session(computer_id, evict_session=session)
                    raise SandboxIdentityLostError(workspace_id, live_sandbox_id)
                workspace = bound
                mark("bind_attached_sandbox")

            # Install before asset sync so codegen includes user-server wrappers.
            ws_version = (
                int(workspace.get("mcp_config_version") or 0)
                if workspace.get("mcp_config_version") is not None
                else 0
            )
            resolved_mcp = await self._apply_session_mcp(
                binding, workspace_user_id, session, ws_version=ws_version
            )
            self._freeze_tool_view(computer_id, workspace_id, session)

            await self._sync_sandbox_assets(
                binding,
                workspace_user_id,
                session.sandbox,
                reusing_sandbox=sandbox_id is not None,
            )
            mark("cold_asset_sync")

            await self._reconcile_skills(
                binding,
                workspace_user_id,
                session.sandbox,
                source="attach_running",
            )
            mark("skill_reconcile")

            # Cache before discovery or its liveness gate exits permanently. Unwind
            # this publication if migration later fails.
            self._put_session(computer_id, session, workspace_id=workspace_id)

            try:
                if resolved_mcp is not None:
                    view = self.tool_view(session, workspace_id)
                    self._kick_mcp_discovery(
                        binding,
                        workspace_user_id,
                        session,
                        self._servers_needing_discovery(
                            session,
                            resolved_mcp,
                            workspace_id=workspace_id,
                        ),
                        view.mcp_config_version or 0,
                    )

                migrated = await self._maybe_migrate_sandbox(
                    binding,
                    workspace_user_id,
                    session,
                    workspace,
                )
                if migrated is not None:
                    session = migrated
            except Exception:
                self._cancel_mcp_discovery(computer_id)
                if self._cached_session(computer_id) is session:
                    self._drop_session(computer_id)
                raise
            did_init = True

        self._put_session(computer_id, session, workspace_id=workspace_id)
        return session, did_init

    async def _claim_and_restart(
        self,
        binding: ComputerBinding,
        workspace_user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
        *,
        from_status: str,
    ) -> Optional[Session]:
        """Revert failed start claims so waiters need not exhaust the 300s timeout."""
        workspace_id = binding.workspace_id
        claimed = await self._claim_machine_for_start(binding, from_status=from_status)
        if claimed is None:
            return None
        claimed_computer_id = binding.computer_id

        def _observe_and_broadcast(state: str) -> None:
            if on_state_observed is not None:
                on_state_observed(state)
            # Only the claim winner observes archived state; publish it so losing
            # workers and /events consumers can show the slow-restore spinner.
            if state == "archived":
                task = asyncio.create_task(
                    publish_status_change(
                        workspace_id,
                        "starting",
                        computer_id=claimed_computer_id,
                        extra={"sandbox_state": state},
                    )
                )
                self._status_publish_tasks.add(task)
                task.add_done_callback(self._status_publish_tasks.discard)

        try:
            logger.info(f"Restarting workspace {workspace_id} (claimed for start)")
            return await self._restart_workspace(
                binding,
                claimed,
                user_id=workspace_user_id,
                lazy_init=True,
                on_state_observed=_observe_and_broadcast,
            )
        except (Exception, asyncio.CancelledError):
            # Do not mask the start error if revert fails; waiters still have the 300s timeout.
            # CancelledError included: a dropped SSE client mid-provision must
            # not leave the claim standing for every sibling on the machine.
            try:
                await asyncio.shield(
                    update_workspace_status(
                        workspace_id=workspace_id,
                        status="stopped",
                    )
                )
            except Exception:
                logger.exception(
                    f"Failed to revert workspace {workspace_id} status after start error"
                )
            raise

    async def _wait_for_start_completion(
        self,
        workspace_id: str,
        max_wait_s: float | None = None,
        poll_interval_s: float | None = None,
    ) -> Dict[str, Any]:
        """Subscribe before rereading the DB to close the missed-publish race.

        Read the machine id first to select its channel; rebinding or dropped
        messages degrade to 30s DB rereads. Redis failure falls back to DB polling
        with exponential backoff from 0.5s to a 2s cap."""
        timeout = self.start_wait_timeout if max_wait_s is None else max_wait_s
        base_interval = (
            self.start_wait_poll_interval
            if poll_interval_s is None
            else poll_interval_s
        )
        max_interval = max(base_interval, 2.0)
        deadline = time.monotonic() + timeout

        # Publishers key on workspaces.computer_id. A computers join can hide that
        # row behind a fence and incorrectly select a different channel.
        prior = await db_get_workspace(workspace_id)
        computer_id = prior.get("computer_id") if prior else None

        async with subscribe_to_status(
            workspace_id, computer_id=computer_id
        ) as wait_for_notify:
            # Subscribe before rereading so a racing publish cannot be missed.
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(
                    f"Workspace {workspace_id} not found while waiting for start"
                )
            status = workspace["status"]
            if status == "running":
                return workspace
            if status == "error":
                raise RuntimeError(f"Workspace {workspace_id} failed to start")
            if status != "starting":
                return workspace

            interval = base_interval
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break

                if wait_for_notify is not None:
                    # Bound missed-publish latency with a 30s DB reread.
                    kind, _payload = await wait_for_notify(min(remaining, 30.0))
                    if kind == "error":
                        # Broken pub/sub returns immediately; fall back to polling to avoid busy-spin reads.
                        wait_for_notify = None
                else:
                    await asyncio.sleep(min(interval, remaining))
                    interval = min(interval * 2, max_interval)

                workspace = await db_get_workspace(workspace_id)
                if not workspace:
                    raise ValueError(
                        f"Workspace {workspace_id} not found while waiting for start"
                    )
                status = workspace["status"]
                if status == "running":
                    return workspace
                if status == "error":
                    raise RuntimeError(f"Workspace {workspace_id} failed to start")
                if status != "starting":
                    return workspace

        raise RuntimeError(
            f"Workspace {workspace_id} stuck in 'starting' after {timeout:.0f}s; "
            "another worker may have died mid-start"
        )

    async def _restart_workspace(
        self,
        binding: ComputerBinding,
        workspace: Dict[str, Any],
        user_id: str | None = None,
        lazy_init: bool = False,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> Session:
        workspace_id = binding.workspace_id
        computer_id = binding.computer_id
        sandbox_id = binding.provider_ref

        if not sandbox_id:
            # Instant project creation defers its first sandbox build until start.
            logger.info(
                f"Workspace {workspace_id} has no sandbox yet; provisioning one"
            )
            return await self._provision_first_sandbox(workspace, user_id)

        # Reconnect uses persisted size; Phase 2 must recheck elevated entitlements
        # outside the workspace lock. Mark here while the row is available.
        if (binding.resource_tier or "standard") != "standard":
            self._machine(computer_id).pending_tier_recheck = True

        # Block on config changes so migration catches stale paths before agent
        # execution.
        expected_hash = self._compute_sandbox_config_hash(self.config, binding)
        ws_config = workspace.get("config") or {}
        stored_hash = ws_config.get("sandbox_config_hash")
        if stored_hash != expected_hash and lazy_init:
            logger.info(
                f"Forcing non-lazy init for {workspace_id}: "
                f"sandbox_config_hash={stored_hash!r}, expected={expected_hash!r}"
            )
            lazy_init = False

        logger.debug(
            f"Reconnecting to sandbox {sandbox_id} for workspace {workspace_id}",
            extra={"lazy_init": lazy_init},
        )

        _cold_start_t0 = time.monotonic()
        try:
            core_config = self._core_config_for(binding)
            session = self._session_handle(binding, core_config)

            sandbox_gone = False

            try:
                if lazy_init:
                    await session.initialize_lazy(
                        sandbox_id=sandbox_id,
                        on_state_observed=on_state_observed,
                    )
                    self._machine(computer_id).pending_lazy_sync = True
                    logger.debug(
                        f"Session lazy-initialized for workspace {workspace_id}"
                    )
                else:
                    await session.initialize(
                        sandbox_id=sandbox_id,
                        on_state_observed=on_state_observed,
                    )
                    logger.debug(f"Session initialized for workspace {workspace_id}")
            except SandboxGoneError as e:
                sandbox_gone = True
                await self._clear_session(computer_id)
                logger.warning(
                    f"Sandbox {sandbox_id} unavailable for workspace "
                    f"{workspace_id} ({e}). Creating fresh sandbox."
                )

            # Recovery reapplies always-on at creation.
            if sandbox_gone:
                return await self._recover_sandbox(binding, user_id, core_config)

            # Apply secrets before further sandbox work; lazy starts wait for Phase 2 readiness.
            if not lazy_init:
                await self._apply_session_platform_secret(
                    binding,
                    session,
                    ws_version=int(workspace.get("platform_secret_version") or 0),
                )

            # Reconnect preserves Daytona's interval even if always-on changed while
            # stopped. Reseed in both directions, best-effort so failures do not block
            # start. Lazy starts defer to Phase 2 because writes race archived startup.
            if sandbox_id and not lazy_init:
                # Reuse the runtime to avoid an extra provider and metadata round trip at cold start.
                live_runtime = (
                    getattr(session.sandbox, "runtime", None)
                    if session.sandbox
                    else None
                )
                try:
                    await self._apply_autostop_for_always_on(
                        sandbox_id,
                        enabled=binding.is_always_on,
                        runtime=live_runtime,
                        binding=binding,
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to re-assert auto-stop interval for "
                        f"{workspace_id}: {e}"
                    )

            if not lazy_init:
                await self._sync_sandbox_assets(
                    binding, user_id, session.sandbox, reusing_sandbox=True
                )
                if session.sandbox:
                    await self._maybe_restore_files(binding, session.sandbox)
                    await self._reconcile_skills(
                        binding, user_id, session.sandbox, source="restart"
                    )
                self._record_sync(computer_id, workspace_id)

                migrated = await self._maybe_migrate_sandbox(
                    binding,
                    user_id,
                    session,
                    workspace,
                    expected_hash=expected_hash,
                    # Restart already owns the starting transition.
                    transition_already_owned=True,
                )
                if migrated is not None:
                    return migrated

            # Keep lazy starts at starting so file/public routes use safe fallbacks
            # until Phase 2 is usable. Non-lazy promotion must also stamp activity.
            if lazy_init:
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="starting",
                )
                self._put_session(computer_id, session, workspace_id=workspace_id)
                # The idle sweep only visits running rows, so starting needs no activity stamp.
                logger.info(f"Workspace {workspace_id} restart initiated (lazy)")
            else:
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="running",
                )
                self._put_session(computer_id, session, workspace_id=workspace_id)
                # Refresh activity so idle cleanup cannot act on a stale timestamp.
                await update_workspace_activity(workspace_id)
                logger.info(f"Workspace {workspace_id} restarted successfully")
            # Lazy timing covers initiation, not background readiness; still record it
            # to keep lazy starts represented in the latency histogram.
            safe_record(
                workspace_cold_start_duration_ms,
                (time.monotonic() - _cold_start_t0) * 1000.0,
            )
            return session

        except Exception as e:
            logger.error(
                f"Error restarting workspace {workspace_id}: {type(e).__name__}: {e}"
            )
            raise

    async def _stop_machine(
        self, computer_id: str, *, workspace_id: str | None = None
    ) -> Dict[str, Any]:
        """Stop the machine, which takes every project on it down with it.

        The machine's own row is the authority and the CAS is the mutex, so a
        project shadow that lags cannot refuse the stop forever. workspace_id
        only names the project that asked, for the mirror and the logs."""
        async with self._observed_lock(computer_id, "computer.stop"):
            # Stopping one project removes every sibling's sandbox; gate across
            # the machine.
            if await self._machine_has_active_tasks(
                computer_id, workspace_id=workspace_id
            ):
                raise RuntimeError("Cannot stop a computer that still has work running")

            # The claim is the whole gate on state: it reads the row, refuses a
            # state no stop can leave, and answers None for one already settled
            # or moved by a peer. A second read and a second raise here could
            # only disagree with it, and did: the pre-check raised on 'stopping'
            # where the claim answers "nothing to stop", so the idle reaper hit
            # its 3-strike backoff on a machine that needed no work.
            claimed = await self._claim_machine_for_stop(computer_id)
            if claimed is None:
                return await self._stop_result(computer_id, workspace_id)
            binding = self._binding_from_computer(workspace_id or "", claimed)
            durable_sandbox_id = binding.provider_ref
            logger.info(f"Stopping computer {computer_id}")

            # Cancel discovery before teardown to prevent dead-sandbox probes and
            # orphan schemas.
            self._cancel_mcp_discovery(computer_id)

            async with computer_stop_heartbeat(computer_id):
                return await self._finish_claimed_stop(
                    binding,
                    workspace_id=workspace_id,
                    durable_sandbox_id=durable_sandbox_id,
                )

    async def _finish_claimed_stop(
        self,
        binding: ComputerBinding,
        *,
        workspace_id: str | None,
        durable_sandbox_id: str | None,
    ) -> Dict[str, Any]:
        computer_id = binding.computer_id
        try:
            backup_started = time.monotonic()
            folder_count = await self._backup_machine_files_to_db(
                computer_id,
                workspace_id=workspace_id,
                expected_sandbox_id=durable_sandbox_id,
            )
            logger.info(
                "Computer %s stop backup completed in %.0fms for %d folder(s)",
                computer_id,
                (time.monotonic() - backup_started) * 1000,
                int(folder_count or 0),
            )

            session = self._cached_session(computer_id)
            attached_sandbox_id = self._session_sandbox_id(session)
            if session is not None and attached_sandbox_id != durable_sandbox_id:
                await self._retire_session(
                    computer_id,
                    session,
                    reason=(
                        "stop saw a session bound to "
                        f"{attached_sandbox_id}, durable is {durable_sandbox_id}"
                    ),
                )
                session = None

            if await self._machine_has_active_tasks(
                computer_id, workspace_id=workspace_id
            ):
                logger.warning(
                    "Computer %s gained active work during stop backup; aborting stop",
                    computer_id,
                )
                await self._settle_machine_stop(computer_id, ComputerStatus.RUNNING)
                return await self._stop_result(computer_id, workspace_id)

            if not await self._retain_machine_stop_claim(computer_id):
                logger.warning(
                    "Computer %s no longer owns the stopping row; aborting provider stop",
                    computer_id,
                )
                return await self._stop_result(computer_id, workspace_id)

            if session is not None:
                await session.stop()
                self._drop_session(computer_id)
            elif durable_sandbox_id:
                try:
                    await self._detached_sandbox_teardown(
                        durable_sandbox_id,
                        delete=False,
                        binding=binding,
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not stop detached sandbox {durable_sandbox_id} "
                        f"for computer {computer_id}: {e}; marking stopped and "
                        "leaving it to auto-stop"
                    )

            machine = self._machine_if_known(computer_id)
            if machine is not None:
                machine.pending_lazy_sync = False
                machine.last_sync_at = None

            await self._settle_machine_stop(computer_id, ComputerStatus.STOPPED)
            logger.info(f"Computer {computer_id} stopped successfully")
            return await self._stop_result(computer_id, workspace_id)

        except Exception as e:
            logger.error(f"Error stopping computer {computer_id}: {e}")
            await self._settle_machine_stop(computer_id, ComputerStatus.ERROR)
            raise

    async def _stop_result(
        self, computer_id: str, workspace_id: str | None
    ) -> Dict[str, Any]:
        """The project row when one asked, the machine's row when none did."""
        if workspace_id is not None:
            row = await db_get_workspace(workspace_id)
            if row is not None:
                return row
        row = await get_computer(computer_id)
        if row is None:
            raise ValueError(f"Computer {computer_id} not found")
        return row

    async def _archive_machine(
        self, computer_id: str, *, workspace_id: str | None = None
    ) -> Dict[str, Any]:
        """Archive moves the machine's disk to storage; its row is the only gate."""
        async with self._observed_lock(computer_id, "computer.archive"):
            # Gate across the machine so archive cannot freeze a sibling's live run.
            if await self._machine_has_active_tasks(
                computer_id, workspace_id=workspace_id
            ):
                raise RuntimeError(
                    "Cannot archive a computer that still has work running"
                )

            computer = await self._machine_ready_to_archive(computer_id)
            binding = self._binding_from_computer(workspace_id or "", computer)
            sandbox_id = computer["provider_ref"]

            async with self._detached_runtime(sandbox_id, binding=binding) as runtime:
                if "archive" not in runtime.capabilities:
                    raise RuntimeError(
                        f"Provider does not support archiving "
                        f"(capabilities: {runtime.capabilities})"
                    )
                await runtime.archive()

            logger.info(f"Computer {computer_id} archived successfully")
            return await self._stop_result(computer_id, workspace_id)
