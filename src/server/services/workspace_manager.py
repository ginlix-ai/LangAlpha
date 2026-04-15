"""
Workspace Manager Service.

Manages workspace lifecycle with database persistence and sandbox integration:
- Creates workspaces with dedicated Daytona sandboxes (1:1 mapping)
- Stops sandboxes when idle (preserves data for quick restart)
- Handles sandbox reconnection for stopped workspaces
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx

from ptc_agent.config import AgentConfig
from ptc_agent.core.sandbox.runtime import SandboxGoneError
from ptc_agent.core.session import Session, SessionManager

from src.server.services.background_task_manager import BackgroundTaskManager

from src.server.database.workspace import (
    create_workspace as db_create_workspace,
    delete_workspace as db_delete_workspace,
    get_workspace as db_get_workspace,
    get_workspaces_by_status,
    update_workspace_activity,
    update_workspace_status,
)
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.sync_user_data import sync_user_data_to_sandbox

logger = logging.getLogger(__name__)


class WorkspaceManager:
    """
    Manages workspace lifecycle with database persistence.

    Each workspace has a dedicated Daytona sandbox (1:1 mapping).
    Workspaces are stopped (not deleted) when idle to preserve data.
    """

    _instance: Optional["WorkspaceManager"] = None

    # Sync cooldown: skip ensure_sandbox_ready + sync_sandbox_assets if synced recently
    _SYNC_COOLDOWN_SECONDS = 30

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
        # Bidirectional maps: workspace_id ↔ user_id (for mark_user_data_stale)
        self._workspace_to_user: dict[str, str] = {}
        self._user_to_workspaces: dict[str, set[str]] = {}

        # Track workspaces that used lazy init and still need skills/assets synced
        # Once sandbox is ready and sync completes, workspace is removed from this set
        self._pending_lazy_sync: set[str] = set()

        # Per-workspace locks (replaces global _lock to avoid cross-workspace blocking)
        self._lock_registry_mu = asyncio.Lock()  # protects _workspace_locks dict only
        self._workspace_locks: Dict[str, asyncio.Lock] = {}

        # Track last sync time per workspace for cooldown
        self._last_sync_at: Dict[str, float] = {}

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

    @classmethod
    def mark_user_data_stale(cls, user_id: str) -> None:
        """Clear user data sync flag for all workspaces owned by a user.

        Safe to call before the singleton is initialized (no-ops).
        Next message to each affected workspace will re-sync user data.
        """
        inst = cls._instance
        if inst is None:
            return
        for ws_id in inst._user_to_workspaces.get(user_id, ()):
            inst._user_data_synced.discard(ws_id)

    async def _get_workspace_lock(self, workspace_id: str) -> asyncio.Lock:
        """Get or create a per-workspace lock."""
        async with self._lock_registry_mu:
            if workspace_id not in self._workspace_locks:
                self._workspace_locks[workspace_id] = asyncio.Lock()
            return self._workspace_locks[workspace_id]

    @asynccontextmanager
    async def _acquire_workspace_lock(self, workspace_id: str, timeout: float = 60.0):
        """Acquire per-workspace lock with timeout."""
        lock = await self._get_workspace_lock(workspace_id)
        try:
            await asyncio.wait_for(lock.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Timeout acquiring lock for workspace {workspace_id} after {timeout}s"
            )
        try:
            yield
        finally:
            lock.release()

    def _sync_cooldown_ok(self, workspace_id: str) -> bool:
        """Return True if sync was done recently enough to skip."""
        last = self._last_sync_at.get(workspace_id)
        if last is None:
            return False
        return (time.monotonic() - last) < self._SYNC_COOLDOWN_SECONDS

    def _record_sync(self, workspace_id: str) -> None:
        """Record that a sync was performed for this workspace."""
        self._last_sync_at[workspace_id] = time.monotonic()

    async def push_vault_secrets(
        self, workspace_id: str, sandbox: "PTCSandbox | None" = None,
    ) -> None:
        """Push vault secrets to the running sandbox.

        Called by the vault API on mutation and by ``_sync_sandbox_assets``
        during workspace startup/restart.

        Args:
            workspace_id: Workspace UUID.
            sandbox: Optional sandbox to push to directly.  When omitted the
                sandbox is looked up from the session cache — this fails during
                initial startup (session not cached yet), so callers that
                already hold a sandbox reference should pass it explicitly.
        """
        if sandbox is None:
            session = self._sessions.get(workspace_id)
            if not session or not session.sandbox:
                return
            sandbox = session.sandbox

        from src.server.database.vault_secrets import get_workspace_secrets_decrypted

        secrets = await get_workspace_secrets_decrypted(workspace_id)
        await sandbox.upload_vault_secrets(secrets)
        logger.debug(
            f"[vault] Pushed {len(secrets)} secret(s) to sandbox",
            extra={"workspace_id": workspace_id},
        )

    @staticmethod
    async def _mint_sandbox_tokens(user_id: str, workspace_id: str) -> dict:
        """Mint scoped OAuth2 tokens for sandbox ginlix-data access.

        Returns token dict on success, empty dict on failure (graceful degradation).
        When empty, the sandbox runs in FMP-only mode.
        """
        auth_url = os.getenv("AUTH_SERVICE_URL", "")
        service_token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
        ginlix_data_url = os.getenv("GINLIX_DATA_URL", "")

        # Skip entire token chain if ginlix-data is not configured
        if not ginlix_data_url or not auth_url or not service_token:
            return {}

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{auth_url}/api/auth/data-tokens",
                    json={"user_id": user_id, "workspace_id": workspace_id},
                    headers={"X-Service-Token": service_token},
                    timeout=10,
                )
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            logger.warning(
                f"Failed to mint sandbox tokens — ginlix-data features disabled: {e}",
                extra={"workspace_id": workspace_id},
            )
            return {}

    @staticmethod
    async def _prepare_user_data_files(user_id: str) -> dict[str, str] | None:
        """Fetch user data from DB and format as markdown files.

        Returns a dict of {filename: markdown_content} for the manifest-based
        sync, or None if the fetch fails. The actual upload is handled by
        PTCSandbox._upload_user_data_files when the manifest hash differs.
        """
        try:
            from src.server.services.sync_user_data import (
                PORTFOLIO_FILE,
                PREFERENCE_FILE,
                WATCHLIST_FILE,
                fetch_all_user_data,
                format_portfolio_md,
                format_preferences_md,
                format_watchlist_md,
            )

            data = await fetch_all_user_data(user_id)
            return {
                PREFERENCE_FILE: format_preferences_md(data),
                WATCHLIST_FILE: format_watchlist_md(data),
                PORTFOLIO_FILE: format_portfolio_md(data),
            }
        except Exception as e:
            logger.warning(f"Failed to prepare user data files: {e}")
            return None

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
            self._workspace_to_user[workspace_id] = user_id
            self._user_to_workspaces.setdefault(user_id, set()).add(workspace_id)
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
        """Sync all sandbox assets (tools, skills, data client, tokens) and user data.

        Uses the unified manifest for tools/skills/data_client/tokens, and
        syncs user data in parallel.

        Args:
            workspace_id: Workspace ID
            user_id: User ID (user data sync skipped if None)
            sandbox: Sandbox instance (all syncs skipped if None)
            reusing_sandbox: If True, sandbox already has assets (skip unchanged)
        """
        if not sandbox:
            return

        # Unified asset sync (skills + tools + data_client + tokens)
        skill_dirs = (
            self.config.skills.local_skill_dirs_with_sandbox()
            if self.config.skills.enabled
            else None
        )

        # All sync tasks run in parallel. Token minting and user data fetching
        # are bundled with the manifest sync so their results feed into the
        # unified hash comparison — only upload if content actually changed.
        _sync_t0 = time.time()
        _sync_times: dict[str, float] = {}

        async def _timed(name: str, coro: Any) -> Any:
            t0 = time.time()
            try:
                return await coro
            finally:
                _sync_times[name] = (time.time() - t0) * 1000

        _ud_files_ok = False  # set inside closure when user data was prepared

        async def _mint_and_sync_assets() -> Any:
            nonlocal _ud_files_ok
            tokens = {}
            if reusing_sandbox and user_id:
                tokens = await self._mint_sandbox_tokens(user_id, workspace_id)

            # Fetch + format user data for manifest hash comparison.
            # The actual upload only happens if the hash differs from the
            # sandbox manifest (same pattern as tokens, skills, etc.).
            user_data_files = None
            if user_id:
                user_data_files = await self._prepare_user_data_files(user_id)
                _ud_files_ok = user_data_files is not None

            return await sandbox.sync_sandbox_assets(
                skill_dirs=skill_dirs,
                reusing_sandbox=reusing_sandbox,
                tokens=tokens or None,
                user_id=user_id,
                workspace_id=workspace_id,
                user_data_files=user_data_files,
            )

        tasks: list[Any] = [_timed("mint+manifest", _mint_and_sync_assets())]

        # Vault secrets — piggyback on existing parallel gather so
        # secrets are available after stop/start and sandbox recovery.
        # Pass sandbox directly: session may not be in self._sessions yet.
        tasks.append(_timed("vault", self.push_vault_secrets(workspace_id, sandbox=sandbox)))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Asset sync failed for {workspace_id}: {result}")

        # Always maintain workspace↔user mapping (needed for mark_user_data_stale).
        # Only mark user data as synced if the files were actually prepared —
        # a transient DB failure in _prepare_user_data_files should retry on
        # the next message, not be permanently suppressed.
        if user_id and results and not isinstance(results[0], Exception):
            self._workspace_to_user[workspace_id] = user_id
            self._user_to_workspaces.setdefault(user_id, set()).add(workspace_id)
            if _ud_files_ok:
                self._user_data_synced.add(workspace_id)

        total = (time.time() - _sync_t0) * 1000
        parts = " ".join(f"{k}={v:.0f}ms" for k, v in _sync_times.items())
        logger.info(
            f"[SYNC_DETAIL] workspace_id={workspace_id} total={total:.0f}ms ({parts})"
        )

    @staticmethod
    async def _seed_agent_md(
        sandbox: Any,
        name: str,
        description: Optional[str] = None,
    ) -> None:
        """Write a default agent.md with workspace metadata and update instructions.

        Uses YAML front matter so the agent (and future tooling) can parse
        workspace identity from the file. Includes inline instructions so
        the agent knows how to maintain this file without detection logic.
        """
        if not sandbox:
            return

        desc = (
            description
            or "Brief 1-2 sentence description — update based on the first conversation."
        )
        lines = [
            "---",
            f"workspace_name: {name}",
            f"description: {desc}",
            "---",
            "",
            f"# {name}",
            "",
        ]
        lines += [
            "<!--",
            "This is a starter template. Replace these comments with real content",
            "as you work. The system prompt has full guidelines on what to maintain.",
            "-->",
            "",
            "## Thread Index",
            "",
            "## Key Findings",
            "",
            "## File Index",
            "",
        ]

        content = "\n".join(lines)
        try:
            # Pass relative path — awrite_file_text calls normalize_path internally
            written = await sandbox.awrite_file_text("agent.md", content)
            if written:
                logger.info(f"Seeded agent.md for workspace '{name}'")
            else:
                logger.warning(f"Failed to seed agent.md for workspace '{name}'")
        except Exception as e:
            logger.warning(f"Failed to seed agent.md: {e}")

    async def _recover_sandbox(
        self,
        workspace_id: str,
        user_id: str | None,
        core_config: Any,
    ) -> Session:
        """Create a fresh sandbox after the old one was deleted, restore files from DB.

        Returns the new session (already cached and DB-updated).
        """
        sandbox_tokens = await self._mint_sandbox_tokens(user_id or "", workspace_id)
        session = SessionManager.get_session(workspace_id, core_config)
        await session.initialize(
            sandbox_tokens=sandbox_tokens,
            user_id=user_id,
            workspace_id=workspace_id,
        )
        new_sandbox_id = getattr(session.sandbox, "sandbox_id", None)

        await self._sync_sandbox_assets(
            workspace_id, user_id, session.sandbox, reusing_sandbox=False
        )

        if session.sandbox:
            await self._restore_files(workspace_id, session.sandbox)

        await update_workspace_status(
            workspace_id=workspace_id,
            status="running",
            sandbox_id=new_sandbox_id,
        )
        self._sessions[workspace_id] = session
        self._record_sync(workspace_id)
        await update_workspace_activity(workspace_id)
        return session

    async def _backup_files_to_db(self, workspace_id: str) -> None:
        """Backup workspace files from sandbox to DB. Non-blocking on failure."""
        session = self._sessions.get(workspace_id)
        if not session or not getattr(session, "sandbox", None):
            return
        try:
            result = await FilePersistenceService.sync_to_db(
                workspace_id, session.sandbox
            )
            logger.debug(f"File backup completed for {workspace_id}: {result}")
        except Exception as e:
            logger.warning(f"File backup failed for {workspace_id}: {e}")

    async def _restore_files(self, workspace_id: str, sandbox: Any) -> None:
        """Restore backed-up files from DB to sandbox. Non-blocking on failure."""
        try:
            result = await FilePersistenceService.restore_to_sandbox(
                workspace_id, sandbox
            )
            logger.info(
                f"Restored {result['restored']} files to sandbox for {workspace_id}"
            )
        except Exception as e:
            logger.warning(f"File restore failed for {workspace_id}: {e}")

    async def _maybe_restore_files(self, workspace_id: str, sandbox: Any) -> None:
        """Restore files if sync marker is missing. Non-blocking on failure."""
        try:
            await FilePersistenceService.maybe_restore(workspace_id, sandbox)
        except Exception as e:
            logger.warning(f"File restore check failed for {workspace_id}: {e}")

    # ── Sandbox config migration ─────────────────────────────────────

    @staticmethod
    def _compute_sandbox_config_hash(config: AgentConfig) -> str:
        """Hash of sandbox config fields that require sandbox recreation on change.

        Adding a new field to the dict automatically invalidates old hashes,
        triggering transparent migration for existing workspaces.
        """
        data = {
            "provider": config.sandbox.provider,
            "working_dir": config.filesystem.working_directory,
        }
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()[:8]

    def _sandbox_config_stamp(self) -> Dict[str, Any]:
        """Build the sandbox config fields to persist in workspace config JSONB.

        Stores both the hash (for fast mismatch detection) and the actual
        values (for observability / debugging).
        """
        return {
            "sandbox_config_hash": self._compute_sandbox_config_hash(self.config),
            "sandbox_provider": self.config.sandbox.provider,
            "sandbox_working_dir": self.config.filesystem.working_directory,
        }

    @staticmethod
    async def _update_workspace_config_fields(
        workspace_id: str, fields: Dict[str, Any], *, raise_on_error: bool = False
    ) -> None:
        """Merge keys into the workspace config JSONB column (atomic, non-destructive).

        Args:
            raise_on_error: If True, re-raise exceptions after logging so the
                caller can retry or handle the failure.  Default False keeps
                the original fire-and-forget behaviour for non-critical stamps.
        """
        from psycopg.types.json import Json

        from src.server.database.conversation import get_db_connection

        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE workspaces
                        SET config = COALESCE(config, '{}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE workspace_id = %s
                        """,
                        (Json(fields), workspace_id),
                    )
        except Exception as e:
            logger.warning(
                f"Failed to update config for workspace {workspace_id}: {e}"
            )
            if raise_on_error:
                raise

    async def _maybe_migrate_sandbox(
        self,
        workspace_id: str,
        user_id: str | None,
        session: Session,
        workspace: Dict[str, Any],
        *,
        expected_hash: str | None = None,
    ) -> Session | None:
        """Check if sandbox working directory matches config; migrate if not.

        Returns a new Session if migration happened, None if no migration needed.
        Migration = backup files from old sandbox → destroy → create fresh → restore.

        Args:
            expected_hash: Pre-computed config hash (avoids recomputation when
                the caller already checked it, e.g. in ``_restart_workspace``).
        """
        if expected_hash is None:
            expected_hash = self._compute_sandbox_config_hash(self.config)

        # Fast path: DB config says already on target version
        ws_config = workspace.get("config") or {}
        stored_hash = ws_config.get("sandbox_config_hash")
        if stored_hash == expected_hash:
            return None

        # Check actual sandbox working dir (set by fetch_working_dir during reconnect)
        if not session.sandbox:
            return None
        actual_wd = session.sandbox.working_dir
        expected_wd = self.config.filesystem.working_directory
        if actual_wd == expected_wd:
            # Already correct (sandbox was recreated for other reasons). Just stamp DB.
            await self._update_workspace_config_fields(
                workspace_id, self._sandbox_config_stamp()
            )
            return None

        # --- Full migration needed ---
        logger.info(
            f"Migrating workspace {workspace_id} sandbox: "
            f"{actual_wd} -> {expected_wd}"
        )

        # 1. Backup files to DB (must succeed or we abort — data loss prevention)
        try:
            result = await FilePersistenceService.sync_to_db(
                workspace_id, session.sandbox
            )
            logger.info(f"Pre-migration backup for {workspace_id}: {result}")
        except Exception:
            logger.error(
                f"Migration aborted for {workspace_id}: file backup failed",
                exc_info=True,
            )
            return None

        # 2. Tear down old sandbox (delete, not just stop — we're replacing it)
        self._sessions.pop(workspace_id, None)
        try:
            await SessionManager.cleanup_session(workspace_id)
        except Exception as e:
            # cleanup_session may fail after cleanup() but before del _sessions,
            # leaving a stale entry.  Evict unconditionally so _recover_sandbox
            # creates a fresh session.
            SessionManager.remove_session(workspace_id)
            logger.warning(f"Old sandbox cleanup failed for {workspace_id}: {e}")

        # 3. Create fresh sandbox + restore files from DB
        core_config = self.config.to_core_config()
        new_session = await self._recover_sandbox(
            workspace_id, user_id, core_config
        )

        # 4. Stamp DB so future reconnects skip migration.
        # Retry once on failure — an unstamped workspace would re-migrate every
        # reconnect, wasting resources and risking data loss.
        stamp = self._sandbox_config_stamp()
        for attempt in range(2):
            try:
                await self._update_workspace_config_fields(
                    workspace_id, stamp, raise_on_error=True
                )
                break
            except Exception:
                if attempt == 0:
                    logger.warning(
                        f"Retrying config stamp for {workspace_id}"
                    )
                else:
                    logger.error(
                        f"Failed to stamp sandbox config for {workspace_id} "
                        f"after 2 attempts. Workspace may re-migrate on next reconnect.",
                        exc_info=True,
                    )

        logger.info(f"Migration complete for workspace {workspace_id}")
        return new_session

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
        # 1. Create DB record (no lock needed — DB generates unique ID)
        workspace = await db_create_workspace(
            user_id=user_id,
            name=name,
            description=description,
            config=config,
        )
        workspace_id = str(workspace["workspace_id"])

        logger.info(f"Creating workspace {workspace_id} for user {user_id}")

        async with self._acquire_workspace_lock(workspace_id):
            try:
                # 2. Mint scoped tokens for sandbox ginlix-data access
                sandbox_tokens = await self._mint_sandbox_tokens(user_id, workspace_id)

                # 3. Initialize sandbox via ptc-agent Session
                core_config = self.config.to_core_config()
                session = SessionManager.get_session(workspace_id, core_config)
                await session.initialize(
                    sandbox_tokens=sandbox_tokens,
                    user_id=user_id,
                    workspace_id=workspace_id,
                )

                # Sync skills and user data to sandbox in parallel
                await self._sync_sandbox_assets(
                    workspace_id, user_id, session.sandbox, reusing_sandbox=False
                )

                # Seed default agent.md with workspace metadata
                await self._seed_agent_md(session.sandbox, name, description)

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

                self._record_sync(workspace_id)

                # Stamp sandbox config (provider, working dir, hash) for migration detection
                await self._update_workspace_config_fields(
                    workspace_id, self._sandbox_config_stamp()
                )

                logger.info(
                    f"Workspace {workspace_id} created with sandbox {sandbox_id}"
                )
                return workspace

            except Exception as e:
                # Mark as error if sandbox creation fails
                logger.error(
                    f"Failed to create sandbox for workspace {workspace_id}: {e}"
                )
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="error",
                )
                raise

    def has_ready_session(self, workspace_id: str) -> bool:
        """Check if a ready session exists in cache (no I/O).

        Used by callers that need a quick pre-check before committing
        to the full get_session_for_workspace() path.
        """
        session = self._sessions.get(workspace_id)
        if session is None or not session._initialized or not session.sandbox:
            return False
        return session.sandbox.is_ready()

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
        _t0 = time.time()
        _session_phases: dict[str, float] = {}

        def _mark(name: str) -> None:
            nonlocal _t0
            now = time.time()
            _session_phases[name] = (now - _t0) * 1000
            _t0 = now

        # ── Phase 1: Read/mutate session cache under per-workspace lock ──
        session: Session | None = None
        needs_sync = False
        needs_deferred_sync = False
        workspace_user_id = user_id

        async with self._acquire_workspace_lock(workspace_id):
            # ── Fast path: check session cache before any DB call ──
            if workspace_id in self._sessions:
                session = self._sessions[workspace_id]
                logger.debug(
                    f"Found cached session for {workspace_id}, "
                    f"initialized={session._initialized}, has_sandbox={session.sandbox is not None}"
                )

                if not session._initialized or not session.sandbox:
                    # Session exists but not usable, fall through to status-based handling
                    session = None
                elif not session.sandbox.is_ready():
                    if session.sandbox.has_failed():
                        # Lazy init completed with error — clear broken session
                        init_err = session.sandbox.init_error
                        logger.warning(
                            f"Lazy init failed for workspace {workspace_id}: "
                            f"{init_err}. Clearing session for recovery."
                        )
                        SessionManager.remove_session(workspace_id)
                        del self._sessions[workspace_id]
                        self._pending_lazy_sync.discard(workspace_id)

                        if isinstance(init_err, SandboxGoneError):
                            core_config = self.config.to_core_config()
                            return await self._recover_sandbox(
                                workspace_id, workspace_user_id, core_config
                            )
                        # Non-sandbox-gone error: fall through to status-based handling
                        session = None
                    else:
                        # Sandbox still initializing (lazy init in progress)
                        logger.info(
                            f"Sandbox still initializing for {workspace_id}, "
                            f"skipping sync"
                        )
                        return session
                else:
                    # Sandbox ready — check if sync is needed
                    needs_deferred_sync = workspace_id in self._pending_lazy_sync
                    needs_sync = (
                        not self._sync_cooldown_ok(workspace_id) or needs_deferred_sync
                    )
                    if not needs_sync:
                        # Cooldown active, skip expensive Daytona calls
                        return session

            # ── Slow path: need DB to determine what to do ──
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            status = workspace["status"]
            sandbox_id_from_db = workspace.get("sandbox_id")
            workspace_user_id = workspace.get("user_id") or user_id
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

            # No usable cached session — handle based on status
            if session is None:
                if status == "stopped":
                    logger.info(f"Restarting stopped workspace {workspace_id}")
                    session = await self._restart_workspace(
                        workspace, user_id=workspace_user_id, lazy_init=True
                    )
                    # Don't return immediately for lazy init — fall through
                    # to Phase 2 which waits for init and handles SandboxGoneError.
                    needs_sync = True
                    needs_deferred_sync = True

                elif status == "running":
                    core_config = self.config.to_core_config()
                    session = SessionManager.get_session(workspace_id, core_config)

                    if not session._initialized:
                        sandbox_id = workspace.get("sandbox_id")
                        try:
                            await session.initialize(sandbox_id=sandbox_id)
                        except SandboxGoneError as e:
                            SessionManager.remove_session(workspace_id)
                            logger.warning(
                                f"Sandbox {sandbox_id} unavailable for workspace "
                                f"{workspace_id} ({e}). Creating fresh sandbox."
                            )
                            return await self._recover_sandbox(
                                workspace_id, workspace_user_id, core_config
                            )

                        await self._sync_sandbox_assets(
                            workspace_id,
                            workspace_user_id,
                            session.sandbox,
                            reusing_sandbox=sandbox_id is not None,
                        )

                        # Check if sandbox needs config migration
                        migrated = await self._maybe_migrate_sandbox(
                            workspace_id, workspace_user_id, session, workspace
                        )
                        if migrated is not None:
                            session = migrated
                    else:
                        needs_sync = True

                    self._sessions[workspace_id] = session

                elif status == "creating":
                    raise RuntimeError(
                        f"Workspace {workspace_id} is still being created. "
                        "Please wait and try again."
                    )

                elif status == "stopping":
                    logger.info(
                        f"Workspace {workspace_id} is stopping, waiting for it to finish..."
                    )
                    for _ in range(20):  # Max ~10 seconds
                        await asyncio.sleep(0.5)
                        workspace = await db_get_workspace(workspace_id)
                        status = workspace.get("status", "unknown")
                        if status == "stopped":
                            logger.info(
                                f"Workspace {workspace_id} finished stopping, restarting"
                            )
                            session = await self._restart_workspace(
                                workspace,
                                user_id=workspace_user_id,
                                lazy_init=True,
                            )
                            needs_sync = True
                            needs_deferred_sync = True
                            break
                    else:
                        # Still "stopping" after 10s — check actual sandbox state
                        # from the provider. If the sandbox is actually running or
                        # stopped, the DB status is stale (e.g. process crashed
                        # mid-stop). Recover by correcting the DB.
                        sandbox_id = workspace.get("sandbox_id")
                        if sandbox_id:
                            try:
                                from ptc_agent.core.sandbox.providers import create_provider

                                provider = create_provider(self.config.to_core_config())
                                try:
                                    runtime = await provider.get(sandbox_id)
                                    actual_state = await runtime.get_state()
                                finally:
                                    await provider.close()

                                logger.warning(
                                    "Workspace %s stuck in 'stopping' but sandbox "
                                    "is actually '%s', recovering",
                                    workspace_id,
                                    actual_state.value,
                                )
                                # Correct the DB status based on actual sandbox state
                                # Only treat definitively stopped/archived as "stopped";
                                # transient states (starting, stopping, archiving) should
                                # not trigger a restart — let them finish naturally.
                                stopped_states = {"stopped", "archived"}
                                if actual_state.value in stopped_states:
                                    corrected = "stopped"
                                elif actual_state.value == "running":
                                    corrected = "running"
                                else:
                                    logger.info(
                                        "Workspace %s sandbox in transient state '%s', "
                                        "not correcting — will retry on next request",
                                        workspace_id,
                                        actual_state.value,
                                    )
                                    raise RuntimeError(
                                        f"Workspace {workspace_id} sandbox is in transient "
                                        f"state '{actual_state.value}'. Please wait and try again."
                                    )
                                workspace = await update_workspace_status(
                                    workspace_id=workspace_id,
                                    status=corrected,
                                )
                                if corrected == "stopped":
                                    session = await self._restart_workspace(
                                        workspace,
                                        user_id=workspace_user_id,
                                        lazy_init=True,
                                    )
                                    needs_sync = True
                                    needs_deferred_sync = True
                                else:
                                    # Sandbox is running — create session inline
                                    # (cannot recurse into get_session_for_workspace
                                    # because the per-workspace asyncio.Lock is held
                                    # and is not reentrant)
                                    core_config = self.config.to_core_config()
                                    session = SessionManager.get_session(workspace_id, core_config)
                                    if not session._initialized:
                                        await session.initialize(sandbox_id=sandbox_id)
                                        await self._sync_sandbox_assets(
                                            workspace_id,
                                            workspace_user_id,
                                            session.sandbox,
                                            reusing_sandbox=True,
                                        )
                                    self._sessions[workspace_id] = session
                            except SandboxGoneError as e:
                                logger.warning(
                                    "Sandbox gone for workspace %s during "
                                    "stopping-state recovery (%s). Recovering.",
                                    workspace_id,
                                    e,
                                )
                                core_config = self.config.to_core_config()
                                SessionManager.remove_session(workspace_id)
                                return await self._recover_sandbox(
                                    workspace_id, workspace_user_id, core_config
                                )
                            except Exception as e:
                                logger.error(
                                    "Failed to check actual sandbox state for %s: %s",
                                    workspace_id,
                                    e,
                                )

                        if session is None:
                            raise RuntimeError(
                                f"Workspace {workspace_id} is still stopping after timeout. "
                                "Please wait and try again."
                            )

                elif status == "flash":
                    raise ValueError(
                        f"Workspace {workspace_id} is a flash workspace (no sandbox). "
                        "Use agent_mode='flash' instead, or create a new workspace for PTC mode."
                    )

                else:
                    raise RuntimeError(f"Unknown workspace status: {status}")

        # ── Phase 2: Expensive sync operations OUTSIDE the lock ──
        # These are safe to call concurrently (idempotent or have their own internal guards).
        # Wrapped in try/except because a concurrent stop_workspace could invalidate
        # the session while we're syncing. The session is already cached and usable;
        # sync is best-effort — next request will retry if it failed.
        _mark("lock_and_init")
        if needs_sync and session and session.sandbox:
            try:
                await session.sandbox.ensure_sandbox_ready()
                _mark("sandbox_ready")

                if needs_deferred_sync:
                    logger.debug(
                        f"Completing deferred sync for lazy-init workspace {workspace_id}"
                    )
                    await self._sync_sandbox_assets(
                        workspace_id,
                        workspace_user_id,
                        session.sandbox,
                        reusing_sandbox=True,
                    )
                    _mark("asset_sync")
                    await self._maybe_restore_files(workspace_id, session.sandbox)
                    _mark("file_restore")
                    self._pending_lazy_sync.discard(workspace_id)

                # User data is now handled by the unified manifest inside
                # _sync_sandbox_assets (hash comparison + upload if changed).
                # _sync_sandbox_assets already manages _user_data_synced with
                # proper gating (_ud_files_ok), so we only maintain the
                # workspace↔user mappings here as a safety net.
                if needs_deferred_sync and workspace_user_id:
                    self._workspace_to_user[workspace_id] = workspace_user_id
                    self._user_to_workspaces.setdefault(workspace_user_id, set()).add(workspace_id)

                if not needs_deferred_sync:
                    await self._sync_user_data_if_needed(
                        workspace_id, workspace_user_id, session.sandbox
                    )
                _mark("user_data_sync")
                self._record_sync(workspace_id)
            except SandboxGoneError as e:
                logger.warning(
                    f"Sandbox gone for workspace {workspace_id} during "
                    f"Phase 2: {e}. Recovering."
                )
                SessionManager.remove_session(workspace_id)
                self._sessions.pop(workspace_id, None)
                self._pending_lazy_sync.discard(workspace_id)

                async with self._acquire_workspace_lock(workspace_id):
                    # Guard: another request may have recovered while we
                    # waited for the lock
                    existing = self._sessions.get(workspace_id)
                    if existing and existing.sandbox and existing.sandbox.is_ready():
                        return existing
                    core_config = self.config.to_core_config()
                    return await self._recover_sandbox(
                        workspace_id, workspace_user_id, core_config
                    )
            except Exception as e:
                logger.warning(
                    f"Phase 2 sync failed for workspace {workspace_id} "
                    f"(will retry next request): {e}"
                )

        if _session_phases:
            total = sum(_session_phases.values())
            phases = " ".join(f"{k}={v:.0f}ms" for k, v in _session_phases.items())
            logger.info(
                f"[SESSION_TIMING] workspace_id={workspace_id} total={total:.0f}ms ({phases})"
            )

        return session

    async def _restart_workspace(
        self,
        workspace: Dict[str, Any],
        user_id: str | None = None,
        lazy_init: bool = False,
    ) -> Session:
        """
        Restart a stopped workspace.

        Args:
            workspace: Workspace record from DB
            user_id: Optional user ID for syncing user data to sandbox
            lazy_init: If True, start sandbox in background for faster response

        Returns:
            Initialized Session instance
        """
        workspace_id = str(workspace["workspace_id"])
        sandbox_id = workspace.get("sandbox_id")

        if not sandbox_id:
            raise RuntimeError(
                f"Workspace {workspace_id} has no sandbox_id. Cannot restart."
            )

        # Force non-lazy init if sandbox config may have changed (e.g., working
        # directory migration).  Without blocking init we cannot detect the
        # mismatch before the agent starts executing with stale paths.
        expected_hash = self._compute_sandbox_config_hash(self.config)
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

        try:
            # Get session from SessionManager
            core_config = self.config.to_core_config()
            session = SessionManager.get_session(workspace_id, core_config)

            sandbox_gone = False

            # Try to reconnect to existing sandbox
            try:
                if lazy_init:
                    await session.initialize_lazy(sandbox_id=sandbox_id)
                    self._pending_lazy_sync.add(workspace_id)
                    logger.debug(
                        f"Session lazy-initialized for workspace {workspace_id}"
                    )
                else:
                    await session.initialize(sandbox_id=sandbox_id)
                    logger.debug(f"Session initialized for workspace {workspace_id}")
            except SandboxGoneError as e:
                sandbox_gone = True
                SessionManager.remove_session(workspace_id)
                logger.warning(
                    f"Sandbox {sandbox_id} unavailable for workspace "
                    f"{workspace_id} ({e}). Creating fresh sandbox."
                )

            # Sandbox was deleted — recover with fresh one
            if sandbox_gone:
                return await self._recover_sandbox(workspace_id, user_id, core_config)

            # Existing sandbox reconnected successfully — sync assets
            if not lazy_init:
                await self._sync_sandbox_assets(
                    workspace_id, user_id, session.sandbox, reusing_sandbox=True
                )
                if session.sandbox:
                    await self._maybe_restore_files(workspace_id, session.sandbox)
                self._record_sync(workspace_id)

                # Check if sandbox needs config migration (e.g., working dir change)
                migrated = await self._maybe_migrate_sandbox(
                    workspace_id, user_id, session, workspace,
                    expected_hash=expected_hash,
                )
                if migrated is not None:
                    return migrated

            # Update status to running
            await update_workspace_status(
                workspace_id=workspace_id,
                status="running",
            )

            # Cache session
            self._sessions[workspace_id] = session

            logger.info(f"Workspace {workspace_id} restarted successfully")
            return session

        except Exception as e:
            logger.error(
                f"Error restarting workspace {workspace_id}: {type(e).__name__}: {e}"
            )
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
        async with self._acquire_workspace_lock(workspace_id):
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
                # Backup files to DB before stopping sandbox
                await self._backup_files_to_db(workspace_id)

                # Stop the session (stops sandbox, preserves data)
                session = self._sessions.get(workspace_id)
                if session:
                    await session.stop()
                    # Remove from cache (will be recreated on restart)
                    del self._sessions[workspace_id]

                # Clear user data sync tracking (will re-sync on restart)
                self._user_data_synced.discard(workspace_id)
                uid = self._workspace_to_user.pop(workspace_id, None)
                if uid:
                    self._user_to_workspaces.get(uid, set()).discard(workspace_id)
                self._pending_lazy_sync.discard(workspace_id)
                self._last_sync_at.pop(workspace_id, None)

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

    async def archive_workspace(self, workspace_id: str) -> Dict[str, Any]:
        """Archive a stopped workspace (moves sandbox to object storage)."""
        async with self._acquire_workspace_lock(workspace_id):
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            if workspace["status"] != "stopped":
                raise RuntimeError(
                    f"Cannot archive workspace in '{workspace['status']}' state. "
                    "Only stopped workspaces can be archived."
                )

            sandbox_id = workspace.get("sandbox_id")
            if not sandbox_id:
                raise RuntimeError("No sandbox associated with this workspace")

            from ptc_agent.core.sandbox.providers import create_provider

            provider = create_provider(self.config.to_core_config())
            try:
                runtime = await provider.get(sandbox_id)
                if "archive" not in runtime.capabilities:
                    raise RuntimeError(
                        f"Provider does not support archiving "
                        f"(capabilities: {runtime.capabilities})"
                    )
                await runtime.archive()
            finally:
                await provider.close()

            logger.info(f"Workspace {workspace_id} archived successfully")
            return workspace

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
        async with self._acquire_workspace_lock(workspace_id):
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            logger.info(f"Deleting workspace {workspace_id}")

            try:
                # Backup files to DB before deleting (if sandbox is accessible)
                await self._backup_files_to_db(workspace_id)

                # Remove from local cache (SessionManager.cleanup_session handles actual cleanup)
                self._sessions.pop(workspace_id, None)

                # Clear user data sync tracking
                self._user_data_synced.discard(workspace_id)
                uid = self._workspace_to_user.pop(workspace_id, None)
                if uid:
                    self._user_to_workspaces.get(uid, set()).discard(workspace_id)
                self._pending_lazy_sync.discard(workspace_id)
                self._last_sync_at.pop(workspace_id, None)

                # Cleanup session (single path — avoids double cleanup)
                try:
                    await SessionManager.cleanup_session(workspace_id)
                except Exception as e:
                    logger.warning(f"Error cleaning up from SessionManager: {e}")

                # Soft delete in DB
                await db_delete_workspace(workspace_id)

                logger.info(f"Workspace {workspace_id} deleted successfully")

            except Exception as e:
                logger.error(f"Error deleting workspace {workspace_id}: {e}")
                raise

        # Clean up the per-workspace lock itself (after releasing it)
        async with self._lock_registry_mu:
            self._workspace_locks.pop(workspace_id, None)

        return True

    async def cleanup_idle_workspaces(self) -> int:
        """
        Stop workspaces that have been idle for too long.

        Returns:
            Number of workspaces stopped
        """
        now = datetime.now(timezone.utc)
        stopped_count = 0

        # Get running workspaces
        running_workspaces = await get_workspaces_by_status("running", limit=1000)

        task_mgr = BackgroundTaskManager.get_instance()

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

                # Skip workspaces that still have an active agent workflow
                if await task_mgr.has_active_tasks_for_workspace(workspace_id):
                    logger.info(
                        f"Workspace {workspace_id} idle for {idle_seconds:.0f}s "
                        "but has active workflow, skipping"
                    )
                    continue

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
        self._workspace_to_user.clear()
        self._user_to_workspaces.clear()
        self._pending_lazy_sync.clear()
        self._last_sync_at.clear()
        self._workspace_locks.clear()

        logger.info("WorkspaceManager shutdown complete")

    def get_stats(self) -> Dict[str, Any]:
        """Get service statistics."""
        return {
            "cached_sessions": len(self._sessions),
            "idle_timeout": self.idle_timeout,
            "cleanup_interval": self.cleanup_interval,
            "cached_workspace_ids": list(self._sessions.keys()),
        }
