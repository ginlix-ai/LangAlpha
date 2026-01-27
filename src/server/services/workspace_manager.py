"""
Workspace Manager Service.

Manages workspace lifecycle with database persistence and sandbox integration:
- Creates workspaces with dedicated Daytona sandboxes (1:1 mapping)
- Stops sandboxes when idle (preserves data for quick restart)
- Handles sandbox reconnection for stopped workspaces
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ptc_agent.config import AgentConfig
from ptc_agent.core.session import Session, SessionManager

from src.server.database.workspace_db import (
    create_workspace as db_create_workspace,
    delete_workspace as db_delete_workspace,
    get_workspace as db_get_workspace,
    get_workspaces_by_status,
    get_workspaces_for_user,
    update_workspace_activity,
    update_workspace_status,
)
from src.server.services.sync_user_data import sync_user_data_to_sandbox

logger = logging.getLogger(__name__)


class WorkspaceManager:
    """
    Manages workspace lifecycle with database persistence.

    Each workspace has a dedicated Daytona sandbox (1:1 mapping).
    Workspaces are stopped (not deleted) when idle to preserve data.
    """

    _instance: Optional["WorkspaceManager"] = None
    _lock = asyncio.Lock()

    def __init__(
        self,
        config: AgentConfig,
        idle_timeout: int = 1800,  # 30 minutes default
        cleanup_interval: int = 300,  # 5 minutes
    ):
        """
        Initialize Workspace Manager.

        Args:
            config: AgentConfig for creating sessions
            idle_timeout: Seconds before idle workspaces are stopped
            cleanup_interval: Seconds between cleanup runs
        """
        self.config = config
        self.idle_timeout = idle_timeout
        self.cleanup_interval = cleanup_interval

        # In-memory session cache (workspace_id -> Session)
        self._sessions: Dict[str, Session] = {}

        # Track which sessions have had user data synced (to avoid syncing every request)
        self._user_data_synced: set[str] = set()

        # Cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown = False

        logger.info(
            "WorkspaceManager initialized",
            extra={
                "idle_timeout": idle_timeout,
                "cleanup_interval": cleanup_interval,
            },
        )

    @classmethod
    def get_instance(
        cls,
        config: Optional[AgentConfig] = None,
        **kwargs,
    ) -> "WorkspaceManager":
        """
        Get or create singleton instance.

        Args:
            config: AgentConfig (required on first call)
            **kwargs: Additional arguments for __init__

        Returns:
            WorkspaceManager instance
        """
        if cls._instance is None:
            if config is None:
                raise ValueError("config is required on first call to get_instance")
            cls._instance = cls(config, **kwargs)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (for testing)."""
        cls._instance = None

    async def _sync_user_data_if_needed(
        self,
        workspace_id: str,
        user_id: str | None,
        sandbox: Any,
        force: bool = False,
    ) -> None:
        """
        Sync user data to sandbox if not already synced for this workspace.

        Args:
            workspace_id: Workspace ID
            user_id: User ID (sync skipped if None)
            sandbox: Sandbox instance (sync skipped if None)
            force: If True, sync even if already synced (for create/restart)
        """
        if not user_id or not sandbox:
            return
        if not force and workspace_id in self._user_data_synced:
            return
        try:
            await sync_user_data_to_sandbox(sandbox, user_id)
            self._user_data_synced.add(workspace_id)
            logger.debug(f"User data synced for workspace {workspace_id}")
        except Exception as e:
            logger.warning(f"User data sync failed for workspace {workspace_id}: {e}")

    async def _sync_sandbox_assets(
        self,
        workspace_id: str,
        user_id: str | None,
        sandbox: Any,
        reusing_sandbox: bool = False,
    ) -> None:
        """
        Sync skills and user data to sandbox in parallel.

        Args:
            workspace_id: Workspace ID
            user_id: User ID (user data sync skipped if None)
            sandbox: Sandbox instance (all syncs skipped if None)
            reusing_sandbox: If True, sandbox already has skills (skip unchanged)
        """
        if not sandbox:
            return

        tasks = []

        # Skills sync task
        if self.config.skills.enabled:
            skill_dirs = self.config.skills.local_skill_dirs_with_sandbox()
            tasks.append(sandbox.sync_skills(skill_dirs, reusing_sandbox=reusing_sandbox))

        # User data sync task
        if user_id:
            tasks.append(sync_user_data_to_sandbox(sandbox, user_id))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.warning(f"Asset sync failed for {workspace_id}: {result}")

            # Track user data sync completion
            if user_id:
                self._user_data_synced.add(workspace_id)

    async def create_workspace(
        self,
        user_id: str,
        name: str,
        description: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new workspace with dedicated sandbox.

        Args:
            user_id: Owner user ID
            name: Workspace name
            description: Optional description
            config: Optional configuration

        Returns:
            Created workspace record
        """
        async with self._lock:
            # 1. Create DB record (status='creating')
            workspace = await db_create_workspace(
                user_id=user_id,
                name=name,
                description=description,
                config=config,
            )
            workspace_id = str(workspace["workspace_id"])

            logger.info(f"Creating workspace {workspace_id} for user {user_id}")

            try:
                # 2. Initialize sandbox via ptc-agent Session
                core_config = self.config.to_core_config()
                session = SessionManager.get_session(workspace_id, core_config)
                await session.initialize()

                # Sync skills and user data to sandbox in parallel
                await self._sync_sandbox_assets(
                    workspace_id, user_id, session.sandbox, reusing_sandbox=False
                )

                # Store session in cache
                self._sessions[workspace_id] = session

                # Get sandbox ID
                sandbox_id = None
                if session.sandbox:
                    sandbox_id = getattr(session.sandbox, "sandbox_id", None)

                # 3. Update DB with sandbox_id (status='running')
                workspace = await update_workspace_status(
                    workspace_id=workspace_id,
                    status="running",
                    sandbox_id=sandbox_id,
                )

                logger.info(
                    f"Workspace {workspace_id} created with sandbox {sandbox_id}"
                )
                return workspace

            except Exception as e:
                # Mark as error if sandbox creation fails
                logger.error(f"Failed to create sandbox for workspace {workspace_id}: {e}")
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="error",
                )
                raise

    async def get_session_for_workspace(
        self,
        workspace_id: str,
        user_id: str | None = None,
    ) -> Session:
        """
        Get or restart session for workspace.

        Args:
            workspace_id: Workspace UUID
            user_id: Optional user ID for syncing user data to sandbox

        Returns:
            Initialized Session instance

        Raises:
            ValueError: If workspace not found
            RuntimeError: If workspace is in error/deleted state
        """
        async with self._lock:
            logger.info(
                f"get_session_for_workspace called: workspace_id={workspace_id}, user_id={user_id}, "
                f"in_cache={workspace_id in self._sessions}, already_synced={workspace_id in self._user_data_synced}"
            )
            # Get workspace from DB
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            status = workspace["status"]
            sandbox_id_from_db = workspace.get("sandbox_id")
            # Use workspace owner's user_id for syncing (don't rely on endpoint passing it)
            workspace_user_id = workspace.get("user_id") or user_id
            logger.info(f"Workspace {workspace_id} from DB: status={status}, sandbox_id={sandbox_id_from_db}, user_id={workspace_user_id}")

            # Check for invalid states
            if status == "deleted":
                raise RuntimeError(f"Workspace {workspace_id} has been deleted")
            if status == "error":
                raise RuntimeError(
                    f"Workspace {workspace_id} is in error state. "
                    "Please delete and recreate."
                )

            # Check cache first
            if workspace_id in self._sessions:
                session = self._sessions[workspace_id]
                logger.info(f"Found cached session for {workspace_id}, initialized={session._initialized}, has_sandbox={session.sandbox is not None}")
                if session._initialized:
                    # Sync user data if not already synced
                    await self._sync_user_data_if_needed(
                        workspace_id, workspace_user_id, session.sandbox
                    )
                    # Update activity timestamp
                    await update_workspace_activity(workspace_id)
                    return session

            # Handle based on status
            if status == "stopped":
                # Restart stopped workspace
                logger.info(f"Restarting stopped workspace {workspace_id}")
                return await self._restart_workspace(workspace, user_id=workspace_user_id)

            elif status == "running":
                # Re-fetch session from SessionManager
                logger.debug(f"Workspace {workspace_id} status is running, getting session from SessionManager")
                core_config = self.config.to_core_config()
                session = SessionManager.get_session(workspace_id, core_config)
                logger.debug(f"Session for {workspace_id}: initialized={session._initialized}, has_sandbox={session.sandbox is not None}")

                if not session._initialized:
                    # Session was dropped, reinitialize with existing sandbox
                    sandbox_id = workspace.get("sandbox_id")
                    await session.initialize(sandbox_id=sandbox_id)

                    # Sync skills and user data in parallel
                    await self._sync_sandbox_assets(
                        workspace_id,
                        workspace_user_id,
                        session.sandbox,
                        reusing_sandbox=sandbox_id is not None,
                    )
                else:
                    # Session was already initialized - sync user data if not already synced
                    await self._sync_user_data_if_needed(
                        workspace_id, workspace_user_id, session.sandbox
                    )

                self._sessions[workspace_id] = session
                await update_workspace_activity(workspace_id)
                return session

            elif status == "creating":
                # Wait for creation to complete (shouldn't happen normally)
                raise RuntimeError(
                    f"Workspace {workspace_id} is still being created. "
                    "Please wait and try again."
                )

            elif status == "stopping":
                # Wait for stop to complete
                raise RuntimeError(
                    f"Workspace {workspace_id} is being stopped. "
                    "Please wait and try again."
                )

            else:
                raise RuntimeError(f"Unknown workspace status: {status}")

    async def _restart_workspace(
        self,
        workspace: Dict[str, Any],
        user_id: str | None = None,
    ) -> Session:
        """
        Restart a stopped workspace.

        Args:
            workspace: Workspace record from DB

        Returns:
            Initialized Session instance
        """
        workspace_id = str(workspace["workspace_id"])
        sandbox_id = workspace.get("sandbox_id")

        if not sandbox_id:
            raise RuntimeError(
                f"Workspace {workspace_id} has no sandbox_id. Cannot restart."
            )

        logger.info(f"Reconnecting to sandbox {sandbox_id} for workspace {workspace_id}")

        try:
            # Get session from SessionManager
            core_config = self.config.to_core_config()
            session = SessionManager.get_session(workspace_id, core_config)

            # Initialize with existing sandbox_id (fast reconnect path)
            await session.initialize(sandbox_id=sandbox_id)
            logger.info(f"Session initialized for workspace {workspace_id}")

            # Sync skills and user data in parallel
            await self._sync_sandbox_assets(
                workspace_id, user_id, session.sandbox, reusing_sandbox=True
            )

            # Update status to running
            await update_workspace_status(
                workspace_id=workspace_id,
                status="running",
            )
            logger.info(f"Status updated to running for workspace {workspace_id}")

            # Cache session
            self._sessions[workspace_id] = session

            logger.info(f"Workspace {workspace_id} restarted successfully")
            return session

        except Exception as e:
            logger.error(f"Error restarting workspace {workspace_id}: {type(e).__name__}: {e}")
            raise

    async def stop_workspace(
        self,
        workspace_id: str,
    ) -> Dict[str, Any]:
        """
        Stop a workspace sandbox (preserves data).

        Args:
            workspace_id: Workspace UUID

        Returns:
            Updated workspace record
        """
        async with self._lock:
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            if workspace["status"] != "running":
                raise RuntimeError(
                    f"Cannot stop workspace in '{workspace['status']}' state. "
                    "Only running workspaces can be stopped."
                )

            logger.info(f"Stopping workspace {workspace_id}")

            # Update status to stopping
            await update_workspace_status(
                workspace_id=workspace_id,
                status="stopping",
            )

            try:
                # Stop the session (stops sandbox, preserves data)
                session = self._sessions.get(workspace_id)
                if session:
                    await session.stop()
                    # Remove from cache (will be recreated on restart)
                    del self._sessions[workspace_id]

                # Clear user data sync tracking (will re-sync on restart)
                self._user_data_synced.discard(workspace_id)

                # NOTE: Don't call SessionManager.cleanup_session() here!
                # That would delete the sandbox. The session stays in SessionManager's
                # cache and will be reused when the workspace is restarted.

                # Update status to stopped
                workspace = await update_workspace_status(
                    workspace_id=workspace_id,
                    status="stopped",
                )

                logger.info(f"Workspace {workspace_id} stopped successfully")
                return workspace

            except Exception as e:
                logger.error(f"Error stopping workspace {workspace_id}: {e}")
                # Mark as error
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="error",
                )
                raise

    async def delete_workspace(
        self,
        workspace_id: str,
    ) -> bool:
        """
        Delete a workspace and its sandbox.

        Args:
            workspace_id: Workspace UUID

        Returns:
            True if deleted successfully
        """
        async with self._lock:
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            logger.info(f"Deleting workspace {workspace_id}")

            try:
                # Stop and cleanup session if running
                session = self._sessions.get(workspace_id)
                if session:
                    try:
                        await session.cleanup()
                    except Exception as e:
                        logger.warning(f"Error cleaning up session: {e}")
                    del self._sessions[workspace_id]

                # Clear user data sync tracking
                self._user_data_synced.discard(workspace_id)

                # Also cleanup from SessionManager
                try:
                    await SessionManager.cleanup_session(workspace_id)
                except Exception as e:
                    logger.warning(f"Error cleaning up from SessionManager: {e}")

                # Soft delete in DB
                await db_delete_workspace(workspace_id)

                logger.info(f"Workspace {workspace_id} deleted successfully")
                return True

            except Exception as e:
                logger.error(f"Error deleting workspace {workspace_id}: {e}")
                raise

    async def cleanup_idle_workspaces(self) -> int:
        """
        Stop workspaces that have been idle for too long.

        Returns:
            Number of workspaces stopped
        """
        now = datetime.now(timezone.utc)
        stopped_count = 0

        # Get running workspaces
        running_workspaces, _ = await get_workspaces_for_user(
            user_id="",  # This won't work - need to get all running workspaces
            limit=1000,
        )

        # Actually, let's use get_workspaces_by_status
        running_workspaces = await get_workspaces_by_status("running", limit=1000)

        for workspace in running_workspaces:
            last_activity = workspace.get("last_activity_at")
            if not last_activity:
                # Never used, skip
                continue

            # Handle timezone-aware comparison
            if last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=timezone.utc)

            idle_seconds = (now - last_activity).total_seconds()

            if idle_seconds > self.idle_timeout:
                workspace_id = str(workspace["workspace_id"])
                logger.info(
                    f"Workspace {workspace_id} idle for {idle_seconds:.0f}s, stopping"
                )

                try:
                    await self.stop_workspace(workspace_id)
                    stopped_count += 1
                except Exception as e:
                    logger.error(f"Error stopping idle workspace {workspace_id}: {e}")

        if stopped_count > 0:
            logger.info(f"Stopped {stopped_count} idle workspaces")

        return stopped_count

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
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in workspace cleanup loop: {e}")

        self._cleanup_task = asyncio.create_task(cleanup_loop())
        logger.info("Workspace cleanup task started")

    async def shutdown(self) -> None:
        """Shutdown service and cleanup resources."""
        logger.info("Shutting down WorkspaceManager...")

        self._shutdown = True

        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

        # Clear session cache (don't stop workspaces on shutdown)
        self._sessions.clear()
        self._user_data_synced.clear()

        logger.info("WorkspaceManager shutdown complete")

    def get_stats(self) -> Dict[str, Any]:
        """Get service statistics."""
        return {
            "cached_sessions": len(self._sessions),
            "idle_timeout": self.idle_timeout,
            "cleanup_interval": self.cleanup_interval,
            "cached_workspace_ids": list(self._sessions.keys()),
        }
