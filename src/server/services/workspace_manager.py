"""Workspace Manager: the project-facing façade over ``ComputerManager``.

A workspace is a project; the machine it runs on is a computer. Everything
keyed by machine -- the session cache, the locks, the sandbox lifecycle --
lives in ``ComputerManager``; what is left here is the project-shaped API the
routes and the agent call, plus the two operations that are genuinely about a
project rather than a machine: creating one and deleting one.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ptc_agent.core.session import Session

from src.observability import safe_add, workspace_created
from src.server.database.computer import (
    get_computers_by_status,
    update_computer_status,
)
from src.server.database.workspace import (
    create_workspace_on_computer,
    delete_workspace as db_delete_workspace,
)
from src.server.services.computer_manager import ComputerManager

logger = logging.getLogger(__name__)

# How many consecutive idle-stop failures on one machine before the sweep stops
# trying it, and for how long. Without this a machine that cannot be stopped
# repeats its error every cycle for as long as the process lives.
_IDLE_REAP_FAILURE_LIMIT = 3
_IDLE_REAP_BACKOFF_SECONDS = 3600.0


class WorkspaceManager(ComputerManager):
    """Project-shaped entry points over the machine lifecycle."""

    async def create_workspace(
        self,
        user_id: str,
        name: str,
        description: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        resource_tier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a project on the user's computer and return it straight away.

        No sandbox is built here. The project is inserted already bound to the
        machine, with the folder it owns on it, and the machine is brought up by
        the first turn (or an explicit start) exactly as it is for a project
        that has been sitting stopped. That is what makes creation instant
        rather than a minute of provisioning the user watches.

        Args:
            user_id: Owner user ID
            name: Workspace name
            description: Optional description
            config: Optional configuration
            resource_tier: Tier for a user who has no computer yet; ignored
                once they have one, since the tier belongs to the machine.

        Returns:
            Created workspace record, bound and ready to be started.
        """
        computer = await self.ensure_primary_computer(
            user_id, name=name, resource_tier=resource_tier
        )
        computer_id = str(computer["computer_id"])

        workspace = await create_workspace_on_computer(
            user_id,
            name,
            computer_id,
            description=description,
            config=config,
        )
        if workspace is None:
            raise RuntimeError(
                f"Computer {computer_id} was gone before workspace {name!r} "
                "could be created on it"
            )

        workspace_id = str(workspace["workspace_id"])
        await self.resolve_binding(workspace_id, workspace=workspace)
        logger.info(
            f"Created workspace {workspace_id} for user {user_id} on computer "
            f"{computer_id} (folder {workspace.get('dir_name')})"
        )
        safe_add(workspace_created, 1)
        return workspace

    async def get_session_for_workspace(
        self,
        workspace_id: str,
        user_id: str | None = None,
        on_state_observed: Callable[[str], None] | None = None,
        skills_signature: str | None = None,
        _attempt: int = 0,
    ) -> Session:
        """Get or restart the session for the machine a workspace runs on.

        Resolves the project to its computer and hands off: the session, the
        sandbox and every lock are the machine's, so two projects on one
        computer converge on one handle instead of racing to build two.

        Sandbox sizing and always-on are applied at create/restart time, a
        recreated sandbox is built from the workspace's tier snapshot with
        auto-stop set in ``_recover_sandbox``, a reconnected always-on sandbox
        has its auto-stop re-asserted in ``_restart_workspace``, and a plain
        reconnect keeps its existing spec, so no post-start re-ensure is needed.

        Args:
            workspace_id: Workspace UUID
            user_id: Optional user ID for syncing user data to sandbox
            on_state_observed: Optional sync callback invoked with the
                initial sandbox state ("archived", "running", ...) as
                soon as the reconnect path observes it. Used by the chat
                SSE generator to emit a refined "restoring from storage"
                copy on the archived cold-start path without a separate
                SDK probe. Ignored on the warm path and when creating a
                fresh sandbox (no pre-existing state to observe).
            skills_signature: The turn's skills delivery signature
                (``skills_delivery_signature`` over the already-loaded
                bundle). When it differs from the session's stamp, the warm
                fast path is bypassed and Phase 2 re-runs the asset sync, so
                a warm sandbox converges on skill changes. None (file routes,
                ``/start``) keeps today's behavior: transitions still sync
                unconditionally, only the warm compare is skipped.

        Returns:
            Initialized Session instance.

        Raises:
            ValueError: If workspace not found
            RuntimeError: If workspace is in error/deleted state
        """
        session = await self._acquire_session(
            workspace_id,
            user_id=user_id,
            on_state_observed=on_state_observed,
            skills_signature=skills_signature,
            _attempt=_attempt,
        )
        # Per project, not per machine: the acquire above may have handed back
        # the session a sibling started, which prepared only that sibling's
        # folder and only that sibling's tool overlay.
        binding = await self.resolve_binding(workspace_id)
        await self._ensure_project_attached(binding, session, user_id=user_id)
        return session

    async def stop_workspace(self, workspace_id: str) -> Dict[str, Any]:
        """Stop the machine this project runs on (preserves data)."""
        binding = await self.resolve_binding(workspace_id)
        return await self._stop_machine(binding.computer_id, workspace_id=workspace_id)

    async def archive_workspace(self, workspace_id: str) -> Dict[str, Any]:
        """Archive a stopped machine (moves its sandbox to object storage)."""
        binding = await self.resolve_binding(workspace_id)
        return await self._archive_machine(
            binding.computer_id, workspace_id=workspace_id
        )

    async def delete_workspace(
        self,
        workspace_id: str,
    ) -> bool:
        """
        Delete a workspace: the project only, never its machine by itself.

        The sandbox belongs to the computer, and siblings may be mid-turn on
        it, so a delete mirrors this project's folder one last time and unlinks
        it. The machine is torn down only where it is genuinely finished: no
        live project left on it, and not the user's primary, which instant
        create hands the next project.

        Args:
            workspace_id: Workspace UUID

        Returns:
            True if deleted successfully
        """
        binding = await self.resolve_binding(workspace_id)
        computer_id = binding.computer_id
        async with self._observed_lock(computer_id, "workspace.delete"):
            workspace = await self.workspace_row(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            logger.info(f"Deleting workspace {workspace_id}")

            retired = False

            try:
                # The project's last mirror sync, before anything on the
                # machine's disk moves. Best effort, as every other backup.
                await self.backup_project_files(
                    workspace_id,
                    computer_id=computer_id,
                    expected_sandbox_id=binding.provider_ref,
                )

                # Soft delete in DB. Before the emptiness decision, so two
                # deletes racing a machine's last two projects cannot both read
                # the other as a live sibling and both leave it running.
                await db_delete_workspace(workspace_id)

                retired = await self._retire_machine_if_empty(
                    computer_id, workspace_id, workspace
                )

                logger.info(f"Workspace {workspace_id} deleted successfully")

            except Exception as e:
                logger.error(f"Error deleting workspace {workspace_id}: {e}")
                raise

        # The record, its lock included, outlives the project and goes only
        # with the machine: dropping it while a sibling holds the lock would let
        # the next sibling build a second one and run alongside the holder.
        if retired:
            self._forget_machine(computer_id)
        self._forget_project(workspace_id)

        return True

    async def cleanup_idle_workspaces(self) -> int:
        """Stop computers that have gone quiet, and reconcile always-on.

        The unit is the machine, not the project: the sandbox is the computer's,
        so one idle project on a busy machine is not a reason to take it away,
        and a machine with five idle projects is stopped once rather than five
        times. Activity is the computer's own stamp, which any project's turn
        refreshes.

        Returns:
            Number of computers stopped.
        """
        now = datetime.now(timezone.utc)
        stopped_count = 0

        running_computers = await get_computers_by_status("running", limit=1000)

        # First pass: reconcile always-on entitlements. The returned set stays
        # exempt from the idle-reaping loop below (still-entitled machines, plus
        # any whose disable failed) so that loop stays single-purpose.
        exempt_ids = await self._reconcile_always_on_entitlements(running_computers)

        for computer in running_computers:
            computer_id = str(computer["computer_id"])
            if computer_id in exempt_ids:
                continue
            machine = self._machine_if_known(computer_id)
            if machine is not None and (
                machine.idle_reap_backoff_until > time.monotonic()
            ):
                continue

            last_activity = computer.get("last_activity_at")
            if not last_activity:
                # Never used, skip
                continue

            # Handle timezone-aware comparison
            if last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=timezone.utc)

            idle_seconds = (now - last_activity).total_seconds()
            if idle_seconds <= self.idle_timeout:
                continue

            # Skip machines that still have an active agent workflow. The gate
            # spans the whole computer: stopping it takes every project on it
            # down, so one idle project is not enough to decide.
            if await self._machine_has_active_tasks(computer_id):
                logger.info(
                    f"Computer {computer_id} idle for {idle_seconds:.0f}s "
                    "but has active workflow, skipping"
                )
                continue

            logger.info(
                f"Computer {computer_id} idle for {idle_seconds:.0f}s, stopping"
            )

            try:
                await self._stop_machine(computer_id)
                stopped_count += 1
                if machine is not None:
                    machine.idle_reap_failures = 0
                    machine.idle_reap_backoff_until = 0.0
            except Exception as e:
                self._note_idle_reap_failure(computer_id, e)

        if stopped_count > 0:
            logger.info(f"Stopped {stopped_count} idle computers")

        return stopped_count

    def _note_idle_reap_failure(self, computer_id: str, error: Exception) -> None:
        """Park a machine the sweep cannot stop, so one warning replaces a loop.

        The counter is this worker's own and is deliberately not durable: a
        restart is allowed to try again, and no other reader may treat it as the
        machine's state.
        """
        machine = self._machine(computer_id)
        failures = machine.idle_reap_failures + 1
        machine.idle_reap_failures = failures
        if failures < _IDLE_REAP_FAILURE_LIMIT:
            logger.error(f"Error stopping idle computer {computer_id}: {error}")
            return

        logger.warning(
            f"Idle stop of computer {computer_id} has failed {failures} times in "
            f"a row ({error}); not trying it again for "
            f"{_IDLE_REAP_BACKOFF_SECONDS / 60:.0f} minutes"
        )
        machine.idle_reap_backoff_until = time.monotonic() + _IDLE_REAP_BACKOFF_SECONDS

    async def reap_stuck_starting_workspaces(self) -> int:
        """Revert computers wedged in 'starting' back to 'stopped'.

        Backstop for the cross-worker start mutex: if a claim winner's Phase 2
        dies without reverting (worker crash, event-loop teardown that beats the
        CancelledError revert, or a publish that never lands), the row stays
        'starting' with no other recovery path — every later caller waits out
        start_wait_timeout then raises, and /start rejects non-'stopped'.

        Never reaps a start THIS process is still running: an in-flight lazy
        owner holds the record's ``pending_lazy_sync`` flag and will promote (on
        success) or revert (on failure) the row itself. Reaping it would discard
        that membership and silently no-op the owner's promotion, stranding a
        ready session behind a 'stopped' row and triggering a duplicate restart.
        That guard makes the in-process case correct regardless of how slow the
        restore is. The ``reap_stuck_after`` threshold (2x start_wait_timeout by
        default) then only governs the cross-process backstop — rows wedged by a
        crashed/recycled worker, which carry no local membership.

        The revert enters through the computer, so every project on the machine
        follows it back to 'stopped' in the same statement.

        Returns:
            Number of computers reverted.
        """
        now = datetime.now(timezone.utc)
        reverted = 0

        starting_computers = await get_computers_by_status("starting", limit=1000)
        if len(starting_computers) == 1000:
            logger.warning(
                "reap_stuck_starting hit the 1000-row scan cap; more stuck "
                "rows may remain and will be reaped on the next cycle"
            )
        for computer in starting_computers:
            updated_at = computer.get("updated_at")
            if not updated_at:
                continue
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            if (now - updated_at).total_seconds() <= self.reap_stuck_after:
                continue

            computer_id = str(computer["computer_id"])
            if self._pending_lazy_start(computer_id):
                # A lazy-start owner on THIS worker is still mid-flight. It
                # owns the row's transition (promote on success, revert on
                # failure); reaping here would discard its membership and
                # no-op that promotion, leaving a ready session behind a
                # 'stopped' row. Cross-process stuck rows have no local
                # membership and fall through to the reap below.
                continue
            logger.warning(
                f"Reaping computer {computer_id} stuck in 'starting' for "
                f"{(now - updated_at).total_seconds():.0f}s "
                f"(threshold {self.reap_stuck_after:.0f}s); reverting to 'stopped'"
            )
            try:
                await update_computer_status(
                    computer_id, "stopped", expected="starting"
                )
                machine = self._machine_if_known(computer_id)
                if machine is not None:
                    machine.pending_lazy_sync = False
                reverted += 1
            except Exception as e:
                logger.error(
                    f"Error reaping stuck-starting computer {computer_id}: {e}"
                )

        if reverted > 0:
            logger.info(f"Reaped {reverted} computers stuck in 'starting'")

        return reverted

    async def start_cleanup_task(self) -> None:
        """Start background cleanup task."""
        if self._cleanup_task is not None:
            return

        self._shutdown = False

        async def cleanup_loop():
            while not self._shutdown:
                try:
                    await asyncio.sleep(self.cleanup_interval)
                    if not self._shutdown:
                        await self.cleanup_idle_workspaces()
                        await self.reap_stuck_starting_workspaces()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in workspace cleanup loop: {e}")

        self._cleanup_task = asyncio.create_task(cleanup_loop())
        logger.info("Workspace cleanup task started")
