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
from collections.abc import Callable, Collection
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, NamedTuple, Optional

from ptc_agent.core.sandbox.runtime import RuntimeState, SandboxTransientError
from ptc_agent.core.session import Session

from src.observability import safe_add, workspace_created
from src.server.database.computer import (
    DEFAULT_ROOT_DIR,
    get_computer,
    get_computer_for_workspace,
    get_computers_by_status,
    update_computer_status,
)
from src.server.database.conversation import forget_workspace_exists
from src.server.database.home_workspace import is_home_row
from src.server.database.workspace import (
    ACTIVE_WORK_DELETE_REFUSAL,
    MOVING_DELETE_REFUSAL,
    SERVER_OWNED_CONFIG_KEYS,
    WorkspaceBusyError,
    WorkspaceMovingError,
    complete_workspace_folder_cleanup,
    create_workspace_on_computer,
    delete_workspace as db_delete_workspace,
    files_restore_offered,
    get_live_workspace_ids_for_computer,
    get_pending_workspace_folder_cleanups,
    workspace_has_active_work,
    workspace_is_moving,
)
from src.server.database.workspace_folders import (
    FOLDER_LAYOUT_VERSION,
    WorkspaceFolderMoving,
    is_top_level,
    workspace_folder_in_use,
)
from src.server.database.workspace_names import checked_workspace_name
from src.server.models.computer import ComputerStatus
from src.server.services.path_lock_coordination import STOP_HEARTBEAT_UNAVAILABLE_RECOVERY_S
from src.server.services.computer_errors import (
    ComputerNotFoundError,
    ComputerPreparingError,
    ComputerRemovalRefused,
    Retirement,
)
from src.server.services.computer_manager import ComputerManager
from src.server.services.computer_manager._types import (
    MachineTarget,
    WorkspaceNotFound,
    WorkspaceNotOnComputer,
)
from src.server.services.persistence.file import FilePersistenceService

logger = logging.getLogger(__name__)


class IdleWindow(NamedTuple):
    """When the idle sweep stops a running computer if nothing more counts as
    activity, and the idle timeout that time is its last activity plus."""

    stop_at: datetime
    timeout_s: int


# How many consecutive idle-stop failures on one machine before the sweep stops
# trying it, and for how long. Without this a machine that cannot be stopped
# repeats its error every cycle for as long as the process lives.
_IDLE_REAP_FAILURE_LIMIT = 3
_IDLE_REAP_BACKOFF_SECONDS = 3600.0
# Past a provider stop's own bound, for one reconcile of a stuck stop.
STUCK_STOP_RECONCILE_TIMEOUT_S = 150.0


class WorkspaceManager(ComputerManager):
    """Project-shaped entry points over the machine lifecycle."""

    async def create_workspace(
        self,
        user_id: str,
        name: str,
        description: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        resource_tier: Optional[str] = None,
        computer_id: Optional[str] = None,
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
            computer_id: One of the user's computers to create the project on
                instead of their primary.

        Returns:
            Created workspace record, bound and ready to be started.

        Raises:
            ComputerNotFoundError: ``computer_id`` names no live computer of
                the user's.
            ComputerPreparingError: the computer is building its first sandbox.
        """
        # Before a first computer is minted for a name that cannot be stored.
        name = checked_workspace_name(name)
        chosen = computer_id is not None
        if chosen:
            computer = await self._chosen_computer(user_id, computer_id)
        else:
            computer = await self.ensure_primary_computer(
                user_id, default_name=name, resource_tier=resource_tier
            )
        # Never had a sandbox and one is being built. A workspace placed on it
        # now would make its first request wait out that build, so it waits here.
        if computer.get("status") in (
            ComputerStatus.CREATING,
            ComputerStatus.STARTING,
        ) and not computer.get("provider_ref"):
            raise ComputerPreparingError("Computer is still being prepared")
        computer_id = str(computer["computer_id"])

        workspace = await create_workspace_on_computer(
            user_id,
            name,
            computer_id,
            description=description,
            config={
                key: value
                for key, value in (config or {}).items()
                if key not in SERVER_OWNED_CONFIG_KEYS
            },
        )
        if workspace is None:
            if chosen:
                raise ComputerNotFoundError("Computer not found")
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

    @staticmethod
    async def _chosen_computer(user_id: str, computer_id: str) -> Dict[str, Any]:
        """The user's live computer by id, for a project they place on it.

        A machine waiting to be retired once empty counts as gone: its teardown
        would tombstone the new project with it.
        """
        computer = await get_computer(computer_id)
        if (
            computer is None
            or str(computer.get("user_id")) != str(user_id)
            or (computer.get("config") or {}).get("retire_when_empty")
        ):
            raise ComputerNotFoundError("Computer not found")
        return computer

    async def get_session_for_workspace(
        self,
        workspace_id: str,
        user_id: str | None = None,
        on_state_observed: Callable[[str], None] | None = None,
        skills_signature: str | None = None,
        run_id: str | None = None,
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
            WorkspaceMovingError: The workspace is moving to another computer.
        """
        # Before a start too: a stopped computer starts by restoring its
        # starter's folder, which a move has set aside.
        if await workspace_is_moving(workspace_id):
            raise WorkspaceMovingError(workspace_id)
        session = await self._acquire_session(
            workspace_id,
            user_id=user_id,
            on_state_observed=on_state_observed,
            skills_signature=skills_signature,
            _attempt=_attempt,
        )
        runtime = getattr(getattr(session, "sandbox", None), "runtime", None)
        if runtime is not None:
            binding = await self.resolve_binding(workspace_id)
            # Every acquire, warm ones included: no sync runs on most of them,
            # and an idle computer's token is left to run out, so this is
            # what has one in place before the turn's code runs. A read of
            # the row when the token has time left.
            await self._keep_data_token(binding.computer_id, user_id, session.sandbox)
            await self._settle_folders(
                binding.computer_id, runtime, root=binding.root_dir, own_run_id=run_id
            )
        try:
            # A settle on any worker moves only a folder it can hold, and without
            # a run (/start, a file route) this workspace is not busy to it: the
            # restore and overlay below write only to the folder read under this.
            async with workspace_folder_in_use(workspace_id):
                binding = await self.resolve_binding(workspace_id)
                if binding.dir_name and not is_top_level(binding.dir_name):
                    # A move a settle could not finish; the agent cannot work in _internal.
                    raise WorkspaceFolderMoving(workspace_id)
                # Again under the hold a move sets the folder aside under, for
                # a move claimed while this acquired.
                if await workspace_is_moving(workspace_id):
                    raise WorkspaceMovingError(workspace_id)
                # Per project, not per machine: the acquire above may have handed
                # back the session a sibling started, which prepared only that
                # sibling's folder and only that sibling's tool overlay.
                await self._ensure_project_attached(binding, session, user_id=user_id)
        except WorkspaceFolderMoving as e:
            raise SandboxTransientError(
                "This workspace's folder is being moved. Try again in a moment."
            ) from e
        return session

    async def reach_computer(self, computer_id: str, *, user_id: str) -> Session:
        """This worker's session on a computer Home's agent works on from its
        root, started if it is down. The caller checks the user owns it.

        A workspace's acquire also keeps the machine's data token and moves
        the folders owed a rename; this does both, and restores the folders
        still owed their files, since the agent reads every folder there
        without opening each workspace.
        """
        session = await self.wake_computer(computer_id, user_id=user_id)
        sandbox = session.sandbox
        computer = await get_computer(computer_id)
        if sandbox is None or sandbox.runtime is None or computer is None:
            raise RuntimeError(f"Computer {computer_id} has no sandbox to run on")
        runtime = sandbox.runtime
        binding = self._binding_from_computer("", computer)
        await self._keep_data_token(computer_id, user_id, sandbox)
        await self._settle_folders(computer_id, runtime, root=binding.root_dir)
        await self.restore_owed_folders(session, binding)
        if (computer.get("layout_version") or 0) >= FOLDER_LAYOUT_VERSION:
            # Below it the root is still one workspace's own folder.
            await self._prune_root_thread_dirs(runtime, binding.root_dir, user_id)
        return session

    @staticmethod
    async def _prune_root_thread_dirs(runtime: Any, root: str | None, user_id: str) -> None:
        """Remove the dirs a deleted Home thread left at a computer's root.

        Home's commands on a computer it reaches keep their logs and scripts
        in thread dirs at the root, where no workspace's bring-up looks, so
        they are judged here against Home's threads. Never fails the reach.
        """
        from ptc_agent.core.paths import SandboxLayout
        from src.server.database.home_workspace import get_home_workspace_id
        from src.server.services.computer_manager import _thread_dirs

        try:
            layout = SandboxLayout.for_root(root or DEFAULT_ROOT_DIR).for_workspace(None)
            await _thread_dirs.prune_dead_thread_dirs(
                runtime, layout, get_home_workspace_id(user_id)
            )
        except Exception as e:  # noqa: BLE001 - the next reach prunes again
            logger.warning(f"Could not prune Home's thread dirs at {root}: {e}")

    async def get_session_if_running(
        self, workspace_id: str, user_id: str | None = None
    ) -> Session | None:
        """The workspace's session, or None while its computer is not running.

        For reads that must never start a computer: an open preview card polls
        every two minutes, so a read that starts one undoes every stop within
        two. Postgres says whether the computer was stopped. Without a session
        on this worker the provider is asked too, because a sandbox can stop on
        its own idle timer before the server notices, and attaching to a
        paused E2B sandbox resumes it.
        """
        computer = await get_computer_for_workspace(workspace_id)
        if (
            computer is None
            or computer.get("status") != "running"
            or not computer.get("provider_ref")
        ):
            return None
        provider_ref = str(computer["provider_ref"])
        session = self.get_session_if_ready(
            workspace_id,
            expected_sandbox_id=provider_ref,
            target=MachineTarget.of_computer(computer),
        )
        if session is not None:
            return session
        binding = self._binding_from_computer(workspace_id, computer)
        async with self._detached_runtime(provider_ref, binding=binding) as runtime:
            # Just looked up, so the state it holds is current.
            state = await runtime.get_state()
        if state is not RuntimeState.RUNNING:
            return None
        return await self.get_session_for_workspace(workspace_id, user_id=user_id)

    async def settle_files_restore_offer(
        self, workspace_id: str, *, user_id: str, restore: bool
    ) -> bool:
        """Restore the folder an offer stands for, or keep it as it is; False if none stands.

        Either way on the machine: a restore lays the files there, and keeping
        the folder writes the markers that say it is current before the offer
        ends. Settling makes the files a new arrival, so every worker attaches
        the folder again: this one now, which lays the restore now owed or
        queues the bring-up the offer held back, and any other at its next
        open, which lays a restore cut short here.
        """
        if not await files_restore_offered(workspace_id):
            return False
        session = await self.get_session_for_workspace(workspace_id, user_id=user_id)
        async with workspace_folder_in_use(workspace_id):
            binding = await self.resolve_binding(workspace_id)
            async with self._held_workspace_folder(workspace_id) as dir_name:
                settled = await FilePersistenceService.settle_restore_offer(
                    workspace_id,
                    session.sandbox,
                    layout=await self._project_layout(
                        workspace_id,
                        binding.computer_id,
                        dir_name=dir_name or "",
                        root=binding.root_dir,
                    ),
                    restore=restore,
                )
            if not settled:
                return False
            # Read again, for the arrival the settle recorded.
            binding = await self.resolve_binding(workspace_id)
            await self._ensure_project_attached(binding, session, user_id=user_id)
            return True

    async def archive_workspace(self, workspace_id: str) -> Dict[str, Any]:
        """Archive a stopped machine (moves its sandbox to object storage)."""
        binding = await self.resolve_binding(workspace_id)
        return await self._archive_machine(
            binding.computer_id, workspace_id=workspace_id
        )

    async def delete_workspace(
        self,
        workspace_id: str,
        *,
        delete_files: bool = False,
        on_computer: Optional[str] = None,
    ) -> bool:
        """
        Delete a workspace: the project only, never its machine by itself.

        The sandbox belongs to the computer, and siblings may be mid-turn on
        it, so a delete mirrors this project's folder one last time and unlinks
        it. The machine is torn down only where it is genuinely finished: no
        live project left on it, and not the user's primary, which instant
        create hands the next project.

        On a machine its owner brought, the folder holds their files, which
        may be a clone or work kept nowhere else: it stays where it is unless
        ``delete_files`` says the owner asked for it to go. Every other
        caller, an agent's tool included, leaves it.

        Args:
            workspace_id: Workspace UUID
            delete_files: The owner asked for the folder on their machine to go too.
            on_computer: Only from this computer; a workspace a move has
                taken elsewhere is left alone.

        Returns:
            True if deleted successfully

        Raises:
            WorkspaceNotFound: It is deleted already, or not on ``on_computer``.
            WorkspaceBusyError: A move or a turn holds it.
            ValueError: It is Home, or has no computer.
        """
        binding = await self.resolve_binding(workspace_id)
        computer_id = binding.computer_id
        if on_computer is not None and str(computer_id) != str(on_computer):
            raise WorkspaceNotFound(workspace_id, computer_id=on_computer)
        try:
            owners_machine = not binding.provider.recreatable
        except ValueError:
            # A kind this build cannot reach, where nothing is removed anyway.
            owners_machine = True
        keep_folder = owners_machine and not delete_files
        async with self._observed_lock(computer_id, "workspace.delete"):
            workspace = await self.workspace_row(workspace_id)
            if not workspace:
                raise WorkspaceNotFound(workspace_id)
            if is_home_row(workspace):
                # Here rather than only at the route: an agent's tool deletes
                # through this method too.
                raise ValueError("Cannot delete Home")
            # Before the last backup holds the folder, which would fail the
            # move instead; the delete's own transaction checks again.
            if await workspace_is_moving(workspace_id):
                raise WorkspaceBusyError(MOVING_DELETE_REFUSAL)
            # Before its commands are ended below, which a delete refused for
            # a live run would otherwise have stopped for nothing.
            if await workspace_has_active_work(workspace_id):
                raise WorkspaceBusyError(ACTIVE_WORK_DELETE_REFUSAL)

            logger.info(f"Deleting workspace {workspace_id}")

            retired = False

            # Ended here, before the last backup reads the folder: a command
            # left running would keep writing into a folder that is going,
            # and report into a project that is gone.
            from src.server.services.commands.control import end_workspace_commands

            await end_workspace_commands(workspace_id)

            try:
                # The project's last mirror sync, before anything on the
                # machine's disk moves. Best effort, as every other backup,
                # and none where the machine's owner turned backups off.
                if binding.files_backup:
                    await self.backup_project_files(
                        workspace_id,
                        computer_id=computer_id,
                        expected_sandbox_id=binding.provider_ref,
                        routine=True,
                    )

                # Soft delete in DB. Before the emptiness decision, so two
                # deletes racing a machine's last two projects cannot both read
                # the other as a live sibling and both leave it running.
                await db_delete_workspace(
                    workspace_id, bound_to=computer_id, keep_folder=keep_folder
                )

                retired = await self._retire_machine_if_empty(
                    computer_id, workspace_id, workspace, keep_folder=keep_folder
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

        from src.server.services.wakeups import cancel as wakeups_cancel

        await wakeups_cancel.cancel_on_workspace_delete(workspace_id)
        # Every delete, whoever asked for it (the route, the agent's tool, a
        # removed computer's workspaces), so the next thread write reads the row.
        await forget_workspace_exists(workspace_id)

        if not retired:
            # Its proxied previews stop now rather than at expiry; a retired
            # machine's teardown already ended them. Revocation is per sandbox,
            # so a sibling's preview mints a fresh link on next open.
            from src.server.services.preview_capabilities import revoke

            await revoke(binding.provider_ref)

        return True

    async def remove_computer_with_workspaces(
        self,
        computer_id: str,
        named: Collection[str] = (),
        *,
        delete_files: bool = False,
    ) -> list[str]:
        """Remove a computer its owner no longer wants, deleting the workspaces
        on it first, and answer the ones deleted.

        ``named`` are the workspaces its owner was shown, and every one on it
        must be among them, so an empty computer needs none. What would refuse
        the remove anyway is asked first (``removal_held``), so a refusal
        deletes nothing: the primary, a transition, a workspace not named, a
        workspace a move holds or a run is working in, and Home, which no
        delete takes. A named one no longer on it is left alone, and is not
        among those answered, so the client forgets only what went. Each goes
        as its own delete would take it, so a machine the user brought keeps
        its folders unless ``delete_files``. On a provider that builds machines
        the last delete retires the computer, which is this removal done.

        Raises:
            ComputerRemovalRefused: nothing was deleted, or something landed
                on it meanwhile, and ``deleted`` names the workspaces already
                gone; ``code`` says why.
            ValueError: there is no such computer, or it is already gone.
        """
        computer = await get_computer(computer_id)
        if computer is None:
            raise ValueError(f"Computer {computer_id} not found")
        held = self.removal_held(computer)
        workspace_ids = await get_live_workspace_ids_for_computer(computer_id)
        shown = {str(i) for i in named}
        for workspace_id in workspace_ids if held is None else ():
            row = await self.workspace_row(workspace_id)
            if str(workspace_id) not in shown or (row and is_home_row(row)):
                held = Retirement.HAS_WORKSPACES
            elif await workspace_is_moving(workspace_id) or await workspace_has_active_work(
                workspace_id
            ):
                held = Retirement.BUSY
            if held is not None:
                break
        if held is not None:
            raise ComputerRemovalRefused(held)
        deleted: list[str] = []
        for workspace_id in workspace_ids:
            try:
                await self.delete_workspace(
                    workspace_id, delete_files=delete_files, on_computer=computer_id
                )
            except WorkspaceBusyError:
                # A move or a run that started since the check above.
                raise ComputerRemovalRefused(Retirement.BUSY, deleted=deleted) from None
            except (WorkspaceNotFound, WorkspaceNotOnComputer):
                # Deleted by another request since the list was read, or
                # moved off this computer.
                continue
            deleted.append(str(workspace_id))
        if workspace_ids and await get_computer(computer_id) is None:
            return deleted
        try:
            await self.remove_computer(computer_id)
        except ComputerRemovalRefused as e:
            # A workspace landed on it meanwhile; the ones deleted stay so.
            raise ComputerRemovalRefused(e.code, deleted=deleted) from None
        return deleted

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

        running_computers = []
        page_size = 1000
        while True:
            page = await get_computers_by_status(
                "running", limit=page_size, offset=len(running_computers)
            )
            running_computers.extend(page)
            if len(page) < page_size:
                break

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

            try:
                window = self._idle_window(computer)
            except ValueError as e:
                # Settings the stop could not build a provider from either.
                self._note_idle_reap_failure(computer_id, e)
                continue
            if window is None:
                continue
            idle_seconds = window.timeout_s + (now - window.stop_at).total_seconds()
            if now <= window.stop_at:
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
                await self._stop_machine(
                    computer_id,
                    unchanged_since=computer.get("updated_at"),
                    reason="idle",
                )
                stopped_count += 1
                if machine is not None:
                    machine.idle_reap_failures = 0
                    machine.idle_reap_backoff_until = 0.0
            except Exception as e:
                self._note_idle_reap_failure(computer_id, e)

        if stopped_count > 0:
            logger.info(f"Stopped {stopped_count} idle computers")

        return stopped_count

    async def reap_pending_workspace_folder_cleanups(self) -> int:
        """Retry folder removals recorded by the workspace tombstone."""
        completed = 0
        for workspace in await get_pending_workspace_folder_cleanups(limit=1000):
            computer_id = str(workspace["computer_id"])
            computer = await get_computer(computer_id)
            if computer is None or computer.get("status") == "deleted":
                if await complete_workspace_folder_cleanup(
                    str(workspace["workspace_id"]),
                    computer_id=computer_id,
                    dir_name=str(workspace["dir_name"]),
                ):
                    completed += 1
                continue
            if await self._remove_workspace_folder(
                str(workspace["workspace_id"]), workspace, computer
            ):
                completed += 1
        return completed

    def _idle_window(self, computer: Dict[str, Any]) -> Optional[IdleWindow]:
        """When the sweep stops this running computer if nothing more counts
        as activity, and the idle timeout that time comes from; None for a
        row with no stamp at all.

        A machine nothing has stamped, such as one a migration carried over,
        has been idle since its row last changed. Skipping it left the row
        'running' for good once the provider stopped the sandbox: short of
        the user stopping it, nothing else moves a running row.

        Raises:
            ValueError: The computer's provider settings are unreadable.
        """
        last_activity = computer.get("last_activity_at") or computer.get("updated_at")
        if not last_activity:
            return None
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        idle_timeout = self._idle_timeout_for(computer)
        return IdleWindow(last_activity + timedelta(seconds=idle_timeout), idle_timeout)

    def idle_stop_window(self, computer: Dict[str, Any]) -> Optional[IdleWindow]:
        """``_idle_window`` for a reader outside the sweep, such as the
        background command watcher keeping a computer up: None where the sweep
        never stops the machine.

        An always-on flag exempts it. The sweep exempts exactly the flagged
        machines once it has reconciled them, since it clears the flag of one
        whose owner lost the entitlement before it reaps; a machine whose
        settings no provider can be built from is never stopped either.
        """
        if computer.get("is_always_on"):
            return None
        try:
            return self._idle_window(computer)
        except ValueError:
            return None

    def _idle_timeout_for(self, computer: Dict[str, Any]) -> int:
        """Seconds idle before the sweep stops this computer.

        Ten minutes ahead of its own provider's auto-stop, overrides included,
        so the server cleans up before the provider stops the sandbox; never
        under five minutes.

        Raises:
            ValueError: The computer's provider settings are unreadable.
        """
        settings = self._provider_settings(
            computer["kind"], computer.get("provider_config") or None
        )
        return max(settings.auto_stop_interval - 600, 300)

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
        """Revert computers wedged in 'starting' or 'creating' back to 'stopped'.

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
        follows it back to 'stopped' in the same statement. A 'creating' row is
        claimed by its own create request within moments, so one that has sat
        past the threshold lost that request and would hold a running slot.

        Returns:
            Number of computers reverted.
        """
        now = datetime.now(timezone.utc)
        reverted = 0

        starting_computers: list[Dict[str, Any]] = []
        for stuck_status in ("starting", "creating"):
            rows = await get_computers_by_status(stuck_status, limit=1000)
            if len(rows) == 1000:
                logger.warning(
                    f"reap_stuck_starting hit the 1000-row scan cap on "
                    f"{stuck_status!r}; more stuck rows may remain and will be "
                    "reaped on the next cycle"
                )
            starting_computers.extend(rows)
        for computer in starting_computers:
            stuck_status = computer["status"]
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
                f"Reaping computer {computer_id} stuck in {stuck_status!r} for "
                f"{(now - updated_at).total_seconds():.0f}s "
                f"(threshold {self.reap_stuck_after:.0f}s); reverting to 'stopped'"
            )
            try:
                # Spec replacement owns this durable state while holding the
                # cross-worker machine key. A timeout here means it is still
                # live work, not an abandoned start. A crashed owner releases
                # its session lock and is recovered by the next sweep.
                async with self._machine_decision_lock(computer_id) as locked:
                    if not locked:
                        continue
                    current = await get_computer(computer_id)
                    if current is None or current.get("status") != stuck_status:
                        continue
                    changed = await update_computer_status(
                        computer_id,
                        "stopped",
                        expected=stuck_status,
                        updated_before=now - timedelta(seconds=self.reap_stuck_after),
                    )
                if changed is None:
                    # The start owner renewed after this sweep read its row.
                    continue
                machine = self._machine_if_known(computer_id)
                if machine is not None:
                    machine.pending_lazy_sync = False
                reverted += 1
            except Exception as e:
                logger.error(
                    f"Error reaping stuck-starting computer {computer_id}: {e}"
                )

        if reverted > 0:
            logger.info(
                f"Reaped {reverted} computers stuck in 'starting' or 'creating'"
            )

        return reverted

    async def reap_stuck_stopping_computers(self) -> int:
        """Settle computers a stop left in 'stopping' with no stopper alive.

        A worker that dies mid-stop (a deploy, a crash) leaves the row
        'stopping', and only a start or a workspace's bring-up used to ask the
        provider about it: a machine with neither stayed 'stopping', which
        refuses both its start and its removal. Each row past a provider
        stop's bound goes through the reconcile a start runs, which leaves a
        stopper still renewing its heartbeat be and settles the rest from the
        provider's state.

        Returns:
            Number of computers no longer 'stopping'.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(
            seconds=STOP_HEARTBEAT_UNAVAILABLE_RECOVERY_S
        )
        settled = 0
        for computer in await get_computers_by_status("stopping", limit=1000):
            updated_at = computer.get("updated_at")
            if not updated_at:
                continue
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            if updated_at > cutoff:
                continue
            computer_id = str(computer["computer_id"])
            try:
                # A dark machine answers within its dial timeout; this bounds
                # one that never does, so the rest are still reached.
                async with asyncio.timeout(STUCK_STOP_RECONCILE_TIMEOUT_S):
                    after = await self.reconcile_stopping_computer(computer_id)
            except Exception as e:
                logger.warning(f"Could not settle stuck stop of computer {computer_id}: {e!r}")
                continue
            if after is not None and after.get("status") != ComputerStatus.STOPPING:
                logger.info(f"Settled computer {computer_id}, stuck in 'stopping', as {after.get('status')!r}")
                settled += 1
        return settled

    async def start_cleanup_task(self) -> None:
        """Start the background cleanup task, and the left-folder, usage and
        stuck-stop sweeps beside it.

        Both sweeps dial machines or providers, any of which may be dark or
        slow, so each runs in a loop of its own rather than ahead of idle
        reaping.
        """
        if self._cleanup_task is not None:
            return

        self._shutdown = False

        async def cleanup_loop():
            while not self._shutdown:
                try:
                    await asyncio.sleep(self.cleanup_interval)
                    if not self._shutdown:
                        # Remove tombstoned project folders, and those moves
                        # left, before idle cleanup can stop their computer and
                        # make the runtime unavailable.
                        await self.reap_pending_workspace_folder_cleanups()
                        await self.reap_workspace_moves()
                        await self.cleanup_idle_workspaces()
                        await self.reap_stuck_starting_workspaces()
                        await self.reap_retirement_pending_computers()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in workspace cleanup loop: {e}")

        async def left_folders_loop():
            while not self._shutdown:
                try:
                    await asyncio.sleep(self.cleanup_interval)
                    if not self._shutdown:
                        await self.set_aside_left_folders()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in left-folder sweep loop: {e}")

        async def stuck_stopping_loop():
            while not self._shutdown:
                try:
                    await asyncio.sleep(self.cleanup_interval)
                    if not self._shutdown:
                        await self.reap_stuck_stopping_computers()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in stuck-stop sweep loop: {e}")

        async def usage_loop():
            # First at once, so a deploy opens runs for the computers already
            # running rather than a cycle later.
            while not self._shutdown:
                try:
                    await self.sweep_usage()
                    await asyncio.sleep(self.cleanup_interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in usage sweep loop: {e}")
                    await asyncio.sleep(self.cleanup_interval)

        self._cleanup_task = asyncio.create_task(cleanup_loop())
        self._left_folders_task = asyncio.create_task(left_folders_loop())
        self._usage_task = asyncio.create_task(usage_loop())
        self._stuck_stopping_task = asyncio.create_task(stuck_stopping_loop())
        logger.info("Workspace cleanup task started")


def prune_thread_dirs_soon(workspace_id: str) -> None:
    """``WorkspaceManager.prune_thread_dirs_soon`` on this process's manager.
    A process without one leaves the dirs to the workspace's next bring-up."""
    manager = WorkspaceManager.current()
    if manager is not None:
        manager.prune_thread_dirs_soon(workspace_id)


def prune_if_archived_soon(thread_id: str) -> None:
    """``WorkspaceManager.prune_if_archived_soon`` on this process's manager."""
    manager = WorkspaceManager.current()
    if manager is not None:
        manager.prune_if_archived_soon(thread_id)

