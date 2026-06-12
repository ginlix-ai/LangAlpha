"""Workspace Manager — 1:1 workspace↔sandbox lifecycle with DB persistence and idle-stop."""

import asyncio
import hashlib
import json
import logging
import os
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional

import httpx

from ptc_agent.config import AgentConfig
from ptc_agent.core.mcp_sanitize import is_user_server
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from ptc_agent.core.session import Session, SessionManager

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

from src.observability import (
    safe_add,
    safe_record,
    session_acquire_phase_duration_ms,
    session_acquire_total_ms,
    session_path_counter,
    workspace_cold_start_duration_ms,
    workspace_created,
)
from src.observability.tracing import hash_id as _obs_hash_id
from src.observability.tracing import safe_aspan

from src.server.services.background_task_manager import BackgroundTaskManager

from src.server.database.workspace import (
    create_workspace as db_create_workspace,
    delete_workspace as db_delete_workspace,
    get_workspace as db_get_workspace,
    get_workspaces_by_status,
    try_claim_workspace_for_start,
    update_workspace_activity,
    update_workspace_status,
)
from src.server.services.persistence.file import FilePersistenceService
from src.server.services.workspace_status_pubsub import (
    publish_status_change,
    subscribe_to_status,
)

logger = logging.getLogger(__name__)


class WorkspaceManager:
    """Singleton that owns in-process session cache and workspace lifecycle (DB + sandbox)."""

    _instance: Optional["WorkspaceManager"] = None

    # Sync cooldown: skip ensure_sandbox_ready + sync_sandbox_assets if synced recently
    _SYNC_COOLDOWN_SECONDS = 30

    def __init__(
        self,
        config: AgentConfig,
        idle_timeout: int = 1800,  # 30 minutes default
        cleanup_interval: int = 300,  # 5 minutes
        start_wait_timeout: float = 300.0,
        start_wait_poll_interval: float = 0.5,
        reap_stuck_after: float | None = None,
    ):
        self.config = config
        self.idle_timeout = idle_timeout
        self.cleanup_interval = cleanup_interval
        # Cross-worker start-mutex polling — see _wait_for_start_completion.
        # 300s covers the worst-case archived-sandbox cold restore and the
        # ceiling for how long a loser waits on the claim winner.
        self.start_wait_timeout = start_wait_timeout
        self.start_wait_poll_interval = start_wait_poll_interval
        # Reaper age threshold — MUST be strictly greater than the worst-case
        # legit start (start_wait_timeout), or the reaper races an in-flight
        # archived restore: it would flip the row to 'stopped' AND discard its
        # _pending_lazy_sync membership, silently no-op'ing the owner's later
        # promotion (ready session, 'stopped' DB row) and triggering a
        # duplicate restart. 2x gives headroom past the 60-300s worst case;
        # only a genuinely-wedged start exceeds it.
        self.reap_stuck_after = (
            reap_stuck_after
            if reap_stuck_after is not None
            else start_wait_timeout * 2
        )

        # In-memory session cache (workspace_id -> Session)
        self._sessions: Dict[str, Session] = {}

        # Track workspaces that used lazy init and still need skills/assets synced
        # Once sandbox is ready and sync completes, workspace is removed from this set
        self._pending_lazy_sync: set[str] = set()

        # Per-workspace locks (replaces global _lock to avoid cross-workspace blocking)
        self._lock_registry_mu = asyncio.Lock()  # protects _workspace_locks dict only
        self._workspace_locks: Dict[str, asyncio.Lock] = {}

        # In-worker Phase 2 dedupe — when the warm endpoint + a chat
        # message race, both arrive in ``get_session_for_workspace`` for
        # the same workspace within the warm window. The per-workspace
        # lock serializes the cache check but Phase 2 (ensure_sandbox_ready
        # + sync_sandbox_assets) runs OUTSIDE the lock, so the second
        # caller would otherwise duplicate the work. The first caller
        # installs an event here; the second awaits it and returns the
        # cached session the first caller hydrated.
        self._phase2_events: Dict[str, asyncio.Event] = {}

        # Strong refs to fire-and-forget sandbox-state broadcast publishes
        # spawned from the (sync) on_state_observed callback. asyncio holds
        # only weak refs to tasks, so without this they could be GC'd before
        # the PUBLISH lands. Discarded in each task's done callback.
        self._status_publish_tasks: set[asyncio.Task] = set()

        # Track last sync time per workspace for cooldown
        self._last_sync_at: Dict[str, float] = {}

        # Strong refs to fire-and-forget background MCP discovery+re-sync tasks
        # (asyncio holds only weak refs to tasks). Discarded in each task's done
        # callback. These run OUTSIDE the per-workspace lock — discovery's stdio
        # cold-start (up to 30s) must never sit on the response path or the lock.
        # Tracked per-workspace so stop/delete can cancel a workspace's in-flight
        # discovery (else it runs against a torn-down sandbox and writes orphaned
        # schema rows); shutdown() drains all of them.
        self._mcp_discovery_tasks: set[asyncio.Task] = set()
        self._mcp_discovery_tasks_by_ws: Dict[str, set[asyncio.Task]] = {}

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
        """Return or create the singleton. ``config`` required on the first call."""
        if cls._instance is None:
            if config is None:
                raise ValueError("config is required on first call to get_instance")
            cls._instance = cls(config, **kwargs)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (for testing)."""
        cls._instance = None

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

    @asynccontextmanager
    async def _observed_lock(self, workspace_id: str, span_name: str, **extra_attrs):
        """``safe_aspan(span_name) + _acquire_workspace_lock`` chain in one helper.

        ``workspace_id`` is hashed for the span attribute. Extra attributes are
        passed through to the span as-is."""
        attrs = {"workspace_id": _obs_hash_id(workspace_id), **extra_attrs}
        async with safe_aspan(span_name, attrs):
            async with self._acquire_workspace_lock(workspace_id):
                yield

    def _sync_cooldown_ok(self, workspace_id: str) -> bool:
        """Return True if sync was done recently enough to skip."""
        last = self._last_sync_at.get(workspace_id)
        if last is None:
            return False
        return (time.monotonic() - last) < self._SYNC_COOLDOWN_SECONDS

    def _record_sync(self, workspace_id: str) -> None:
        """Record that a sync was performed for this workspace."""
        self._last_sync_at[workspace_id] = time.monotonic()

    async def _clear_session(
        self,
        workspace_id: str,
        *,
        evict_session: "Session | None" = None,
    ) -> None:
        """Remove all traces of a broken session and proactively release its
        resources (MCP connections + provider aiohttp client) instead of
        waiting for GC.

        ``cleanup_session`` awaits ``session.cleanup()``, so a concurrent
        request can install a replacement in ``self._sessions[workspace_id]``
        while we're yielded. When the caller passes the session object it
        intended to evict, we identity-check before popping — so the
        replacement survives. Callers inside the workspace lock can omit
        ``evict_session`` (the lock already prevents the race).

        Safe to call when the workspace is not present — idempotent.
        """
        # Cancel in-flight discovery before tearing down the session, mirroring
        # stop_workspace/delete_workspace — it must not run against the torn-down
        # sandbox or write orphaned schema rows for an evicted session.
        self._cancel_mcp_discovery(workspace_id)
        try:
            await SessionManager.cleanup_session(workspace_id)
        except Exception as e:
            logger.warning(
                "Error during session cleanup (continuing)",
                extra={"workspace_id": workspace_id, "error": str(e)},
            )
        if evict_session is None or self._sessions.get(workspace_id) is evict_session:
            self._sessions.pop(workspace_id, None)
        self._pending_lazy_sync.discard(workspace_id)

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

    # ── Per-workspace MCP resolution + composite caching ────────────────
    #
    # Resolved once per session (under the slow path, never in the cooldown
    # window), cached on the Session, and re-used per turn so create_agent never
    # re-resolves or queries the DB. The version check piggybacks the existing
    # post-cooldown ``db_get_workspace`` read: when the workspace's
    # ``mcp_config_version`` differs from the session's applied version we
    # re-resolve + rebuild the composite, then re-run the existing sync path so
    # wrappers update. Discovery for new/stale user servers is BACKGROUNDED — it
    # never runs inline on the turn and never under the per-workspace lock.

    async def _apply_session_mcp(
        self,
        workspace_id: str,
        user_id: str | None,
        session: Session,
        *,
        ws_version: int | None,
    ) -> Any | None:
        """Resolve the effective MCP set and stash composite+summary on ``session``.

        ``ws_version`` is the ``mcp_config_version`` already read from the
        workspaces row (piggyback — no extra read). Returns the ``ResolvedMCP``
        when the composite was (re)built, ``None`` when the session was already
        current (callers then skip the discovery kick). Cheap work only —
        resolve (DB reads) + in-memory composite build; discovery is separate.
        """
        sandbox = session.sandbox
        if sandbox is None:
            return None

        # Already current: same version AND a composite is installed. Skip the
        # resolve entirely so an unchanged-config slow-path sync adds ZERO reads.
        if (
            session.mcp_config_version is not None
            and ws_version is not None
            and session.mcp_config_version == ws_version
            and session.mcp_tool_summary is not None
        ):
            return None

        from src.server.handlers.chat.mcp_config import resolve_mcp_config

        try:
            resolved = await resolve_mcp_config(
                self.config, user_id or "", workspace_id
            )
        except Exception as e:
            logger.warning(
                "[ASSET_SYNC] MCP resolve failed for %s: %s — keeping prior set",
                workspace_id,
                e,
            )
            return None

        await self._install_session_composite(session, resolved)
        return resolved

    async def _install_session_composite(
        self, session: Session, resolved: Any
    ) -> None:
        """Build the composite registry + tool summary from ``resolved`` and stash.

        The session's CoreConfig is already a per-workspace deep copy, so we make
        its ``config.mcp.servers`` the EFFECTIVE set (built-ins + user servers)
        — this is what the sandbox reads at every per-site audited point. The
        composite registry (built-ins verbatim + ok-status user schemas) is set
        on both ``session.mcp_registry`` and ``session.sandbox.mcp_registry`` so
        codegen + the per-turn prompt read the same object. Zero user servers ⇒
        the composite IS the built-in registry (identity), byte-identical.
        """
        from ptc_agent.core.mcp_registry import build_composite_registry
        from ptc_agent.agent.prompts.formatter import (
            build_tool_summary_from_registry,
        )

        sandbox = session.sandbox

        # Effective server list onto the per-workspace CoreConfig copy.
        if sandbox is not None and getattr(sandbox, "config", None) is not None:
            sandbox.config.mcp.servers = list(resolved.servers)
        session.config.mcp.servers = list(resolved.servers)

        # User servers (source='workspace') + their ok-status cached schemas.
        user_servers = [s for s in resolved.servers if is_user_server(s)]
        tool_schemas: dict[str, list[dict]] = {}
        if user_servers:
            from src.server.database.mcp_servers import get_tool_schemas
            from src.server.services.mcp_discovery import mcp_discovery_fingerprint

            # Load a cached snapshot only when it's for the server's CURRENT
            # config (hash match). A toggled/unrelated mutation leaves a server's
            # fingerprint unchanged, so its tools load from cache — no re-verify;
            # a server whose own config changed misses the cache and is picked up
            # by background discovery.
            fp_by_name = {s.name: mcp_discovery_fingerprint(s) for s in user_servers}
            rows = await get_tool_schemas(session.conversation_id)
            for row in rows:
                name = row["server_name"]
                if row.get("status") == "ok" and row.get("config_hash") == fp_by_name.get(name):
                    tool_schemas[name] = row.get("tools") or []

        # Always build from the BUILTIN registry, never a prior composite —
        # session.mcp_registry may already be a composite from an earlier resolve.
        builtin_registry = session._builtin_mcp_registry or session.mcp_registry
        composite = build_composite_registry(
            builtin_registry,
            user_servers,
            tool_schemas,
            getattr(resolved, "disabled_builtin_names", frozenset()),
        )

        session.mcp_registry = composite
        if sandbox is not None:
            sandbox.mcp_registry = composite

        try:
            tool_exposure = self.config.mcp.tool_exposure_mode
        except Exception:
            tool_exposure = "summary"
        session.mcp_tool_summary = build_tool_summary_from_registry(
            composite, mode=tool_exposure
        )
        session.mcp_config_version = resolved.version

    def _servers_needing_discovery(
        self, session: Session, resolved: Any
    ) -> list[Any]:
        """User servers in ``resolved`` lacking an ok-status schema in the composite.

        Used to decide whether to kick background discovery. A server with cached
        tools already appears in the composite; one without (pending/error/new)
        contributes config but zero tools until discovery completes.
        """
        registry = session.mcp_registry
        get_all = getattr(registry, "get_all_tools", None)
        present_with_tools: set[str] = set()
        if callable(get_all):
            for name, tools in get_all().items():
                if tools:
                    present_with_tools.add(name)
        return [
            s for s in resolved.servers
            if is_user_server(s) and s.name not in present_with_tools
        ]

    def _kick_mcp_discovery(
        self,
        workspace_id: str,
        user_id: str | None,
        session: Session,
        servers: list[Any],
        version: int,
    ) -> None:
        """Fire-and-forget discovery + composite rebuild for ``servers`` (background).

        Never awaited on the turn and never under the per-workspace lock: stdio
        cold-start is up to 30s. On completion the session's composite+summary
        are rebuilt in this same task (a mid-turn swap is safe — create_agent
        reads the registry+summary at turn start, so the worst case is the new
        tools appear one turn later).
        """
        if not servers:
            return

        def _session_live() -> bool:
            # Bail if the workspace was stopped/deleted (or replaced) while
            # discovery ran: don't touch a torn-down sandbox or write orphaned
            # schema rows for a session that's no longer the live one.
            if self._sessions.get(workspace_id) is not session:
                return False
            sandbox = session.sandbox
            if sandbox is None:
                return False
            is_ready = getattr(sandbox, "is_ready", None)
            return is_ready() if callable(is_ready) else True

        async def _run() -> None:
            try:
                if not _session_live():
                    return
                from src.server.services.mcp_discovery import discover_and_cache

                _t_disc = time.time()
                await discover_and_cache(workspace_id, session.sandbox, servers)
                logger.info(
                    "[ASSET_SYNC] workspace_id=%s mcp_discovery=%.0fms servers=%d",
                    workspace_id,
                    (time.time() - _t_disc) * 1000,
                    len(servers),
                )
                # Rebuild from the freshly-cached ok rows so this session sees the
                # new tools without waiting for the next post-cooldown acquire.
                # Only swap if the session's config version is still ``version``
                # (no newer mutation landed) AND this is still the live session.
                if (
                    session.mcp_config_version == version
                    and _session_live()
                ):
                    from src.server.handlers.chat.mcp_config import (
                        resolve_mcp_config,
                    )

                    resolved = await resolve_mcp_config(
                        self.config, user_id or "", workspace_id
                    )
                    if resolved.version == version and _session_live():
                        await self._install_session_composite(session, resolved)
                        # Re-run sync so the new wrappers land in the sandbox.
                        await self._sync_sandbox_assets(
                            workspace_id,
                            user_id,
                            session.sandbox,
                            reusing_sandbox=True,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "[ASSET_SYNC] background MCP discovery failed for %s: %s",
                    workspace_id,
                    e,
                )

        task = asyncio.create_task(_run())
        self._mcp_discovery_tasks.add(task)
        self._mcp_discovery_tasks_by_ws.setdefault(workspace_id, set()).add(task)

        def _on_done(t: asyncio.Task) -> None:
            self._mcp_discovery_tasks.discard(t)
            ws_tasks = self._mcp_discovery_tasks_by_ws.get(workspace_id)
            if ws_tasks is not None:
                ws_tasks.discard(t)
                if not ws_tasks:
                    self._mcp_discovery_tasks_by_ws.pop(workspace_id, None)

        task.add_done_callback(_on_done)

    def _cancel_mcp_discovery(self, workspace_id: str) -> None:
        """Cancel a workspace's in-flight background discovery tasks.

        Called on stop/delete so discovery can't run against a torn-down sandbox
        or write orphaned schema rows. The done callbacks prune both the global
        set and the per-workspace map.
        """
        for task in list(self._mcp_discovery_tasks_by_ws.get(workspace_id, ())):
            task.cancel()
        self._mcp_discovery_tasks_by_ws.pop(workspace_id, None)

    async def _sync_sandbox_assets(
        self,
        workspace_id: str,
        user_id: str | None,
        sandbox: Any,
        reusing_sandbox: bool = False,
    ) -> None:
        """Sync all sandbox assets (unified manifest + vault secrets) in parallel.

        ``reusing_sandbox=True`` passes the flag through to ``sync_sandbox_assets`` so
        unchanged modules are skipped. No-op when ``sandbox`` is None.
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

        async def _mint_and_sync_assets() -> Any:
            tokens = {}
            if reusing_sandbox and user_id:
                tokens = await self._mint_sandbox_tokens(user_id, workspace_id)

            return await sandbox.sync_sandbox_assets(
                skill_dirs=skill_dirs,
                reusing_sandbox=reusing_sandbox,
                tokens=tokens or None,
                user_id=user_id,
                workspace_id=workspace_id,
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

        # Install the per-workspace composite before asset sync so user-server
        # wrappers are regenerated for the fresh sandbox. ws_version=None forces
        # a resolve (the session is brand new). Discovery kicked in background.
        resolved_mcp = await self._apply_session_mcp(
            workspace_id, user_id, session, ws_version=None
        )

        await self._sync_sandbox_assets(
            workspace_id, user_id, session.sandbox, reusing_sandbox=False
        )

        # Cache the session BEFORE kicking discovery: the background task's
        # liveness gate (``self._sessions.get(workspace_id) is session``) would
        # otherwise see no cached session and exit permanently. Any later step
        # that raises must NOT leave this broken session cached — the old code
        # only cached after every step succeeded — so unwind on failure.
        self._sessions[workspace_id] = session

        try:
            if resolved_mcp is not None:
                self._kick_mcp_discovery(
                    workspace_id,
                    user_id,
                    session,
                    self._servers_needing_discovery(session, resolved_mcp),
                    session.mcp_config_version or 0,
                )

            if session.sandbox:
                await self._restore_files(workspace_id, session.sandbox)

            await update_workspace_status(
                workspace_id=workspace_id,
                status="running",
                sandbox_id=new_sandbox_id,
            )
        except Exception:
            self._cancel_mcp_discovery(workspace_id)
            if self._sessions.get(workspace_id) is session:
                self._sessions.pop(workspace_id, None)
            raise

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

        async with self._observed_lock(
            workspace_id, "workspace.create", user_id=_obs_hash_id(user_id)
        ):
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

                # Install the per-workspace MCP composite (a brand-new workspace
                # has zero MCP rows → builtins-only identity, byte-identical) so
                # session.mcp_registry + summary are cached from creation.
                await self._apply_session_mcp(
                    workspace_id, user_id, session, ws_version=0
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
                safe_add(workspace_created, 1)
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

    def get_applied_mcp_config_version(self, workspace_id: str) -> int | None:
        """The MCP config version the warm session has applied (no I/O, no lock).

        Returns None when no ready session exists — the config isn't loaded
        anywhere live yet. The effective-list endpoint surfaces this so the UI
        shows a version-accurate "applied / still applying" state instead of a
        best-effort timer.
        """
        if not self.has_ready_session(workspace_id):
            return None
        session = self._sessions.get(workspace_id)
        return session.mcp_config_version if session is not None else None

    async def proactively_apply_mcp_config(
        self, workspace_id: str, user_id: str | None = None
    ) -> None:
        """Front-load verifying + applying a just-mutated MCP config — warming
        the sandbox if it isn't running yet.

        Mutations only bump ``mcp_config_version`` in the DB; the live agent
        normally picks the change up on its next acquire (the next message).
        This runs that acquire/re-sync NOW, in the background, so a server is
        discovered and loaded before the user's next turn — no surprise.

        We always drive ``get_session_for_workspace``: when a session is warm we
        first clear the 30s sync cooldown so the acquire actually re-resolves +
        re-syncs instead of short-circuiting; when none is warm we still acquire,
        which warms (or cold-starts) the sandbox. A user who just configured an
        MCP server in the workspace panel expects it to come up and verify
        regardless of whether the sandbox happened to be running — entering the
        workspace warms it anyway, so a config change does the same.

        Strictly additive and best-effort: any failure (cold-start error,
        workspace mid-create / in error) is swallowed here and the change falls
        back to today's next-message apply.
        """
        self._last_sync_at.pop(workspace_id, None)
        try:
            await self.get_session_for_workspace(workspace_id, user_id=user_id)
        except Exception as e:
            logger.warning(
                "[ASSET_SYNC] proactive MCP apply failed for %s: %s — "
                "falling back to next-message apply",
                workspace_id,
                e,
            )

    async def refresh_session_mcp(
        self, workspace_id: str, user_id: str | None = None
    ) -> None:
        """Rebuild the live session's MCP composite WITHOUT a version bump.

        For out-of-band schema-cache updates (the manual ``/discover`` probe)
        where ``mcp_config_version`` is unchanged so ``_apply_session_mcp``
        would short-circuit. Busting the session's cached version forces the
        next apply to re-resolve, reload the fresh snapshots, and re-sync
        wrappers — then the standard proactive-apply path does the work.
        No-op (beyond a warm acquire) when no session is live.
        """
        # Deliberately lock-free: racing a concurrent _apply_session_mcp (which
        # reads this field under the workspace lock) costs at most one redundant
        # re-resolve — never a missed one, since the proactive apply below
        # re-enters the locked path. Don't add a lock here; it would put this
        # background refresh in contention with the hot chat path.
        session = self._sessions.get(workspace_id)
        if session is not None:
            session.mcp_config_version = None
        await self.proactively_apply_mcp_config(workspace_id, user_id)

    async def get_session_for_workspace(
        self,
        workspace_id: str,
        user_id: str | None = None,
        on_state_observed: Callable[[str], None] | None = None,
        _attempt: int = 0,
    ) -> Session:
        """
        Get or restart session for workspace.

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

        _was_cached = workspace_id in self._sessions

        # ── Phase 1: Read/mutate session cache under per-workspace lock ──
        session: Session | None = None
        needs_sync = False
        needs_deferred_sync = False
        pending_start_wait = False
        workspace_user_id = user_id
        # mcp_config_version from the post-cooldown workspaces read (piggyback —
        # no extra query). None when we never reach the slow-path DB read (cooldown
        # warm hit / still-initializing — both early-return before this point).
        ws_mcp_version: int | None = None

        async with self._observed_lock(
            workspace_id, "workspace.session.acquire", cached_on_entry=_was_cached
        ):
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
                        await self._clear_session(workspace_id)

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
                        safe_add(session_path_counter, 1, {"path": "warm_initializing"})
                        return session
                else:
                    # Sandbox ready — check if sync is needed
                    needs_deferred_sync = workspace_id in self._pending_lazy_sync
                    needs_sync = (
                        not self._sync_cooldown_ok(workspace_id) or needs_deferred_sync
                    )
                    if not needs_sync:
                        # Cooldown active, skip expensive Daytona calls
                        safe_add(session_path_counter, 1, {"path": "warm_cooldown"})
                        return session

            # ── Slow path: need DB to determine what to do ──
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            status = workspace["status"]
            sandbox_id_from_db = workspace.get("sandbox_id")
            workspace_user_id = workspace.get("user_id") or user_id
            # Piggyback the MCP config version off this existing read.
            ws_mcp_version = (
                int(workspace.get("mcp_config_version") or 0)
                if workspace.get("mcp_config_version") is not None
                else 0
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

            # No usable cached session — handle based on status
            if session is None:
                if status in ("stopped", "starting"):
                    # Cross-worker mutex: only one worker may transition
                    # stopped → starting at a time. Try the claim HERE (a fast
                    # atomic UPDATE) but NEVER wait under the lock — a 60-300s
                    # archived cold-start would head-of-line block every other
                    # op on this workspace (stop/delete/concurrent get) until
                    # the 60s lock-acquire ceiling. The winner restarts and
                    # owes Phase 2; losers (and arrivals already at 'starting')
                    # set pending_start_wait and wait OUTSIDE the lock below.
                    if status == "stopped":
                        session = await self._claim_and_restart(
                            workspace_id,
                            workspace_user_id,
                            on_state_observed,
                        )
                    if session is not None:
                        # Winner: lazy-init Phase 2 needs to sync + promote.
                        needs_sync = True
                        needs_deferred_sync = True
                    else:
                        # Lost the claim, or status was already 'starting'.
                        pending_start_wait = True

                elif status == "running":
                    session, did_init = await self._attach_running_session(
                        workspace,
                        workspace_user_id,
                        on_state_observed,
                        _mark,
                    )
                    if not did_init:
                        # Session was already initialized — refresh via Phase 2 sync.
                        needs_sync = True

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
                                on_state_observed=on_state_observed,
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
                                # Fresh last_activity_at so the idle sweep does
                                # not immediately stop a just-corrected workspace
                                # on a stale timestamp. Mirrors _recover_sandbox
                                # and _restart_workspace.
                                await update_workspace_activity(workspace_id)
                                if corrected == "stopped":
                                    session = await self._restart_workspace(
                                        workspace,
                                        user_id=workspace_user_id,
                                        lazy_init=True,
                                        on_state_observed=on_state_observed,
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
                                        await session.initialize(
                                            sandbox_id=sandbox_id,
                                            on_state_observed=on_state_observed,
                                        )
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
                                await self._clear_session(workspace_id)
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

            # In-worker Phase 2 dedupe gate. Set up while still inside the
            # per-workspace lock so two same-worker callers can't both
            # install events for the same workspace.
            phase2_owner = False
            phase2_event: Optional[asyncio.Event] = None
            if needs_sync and session is not None and session.sandbox is not None:
                existing_event = self._phase2_events.get(workspace_id)
                if existing_event is not None and not existing_event.is_set():
                    phase2_event = existing_event
                else:
                    phase2_event = asyncio.Event()
                    self._phase2_events[workspace_id] = phase2_event
                    phase2_owner = True

        # ── Phase 1.5: cross-worker start wait, OUTSIDE the per-workspace lock ──
        # A caller that lost the claim (or arrived at status 'starting') waits
        # for the owning worker to finish, then attaches the now-running session
        # (or retries the claim once if the owner failed). Outside the lock so a
        # slow archived cold-start (60-300s) doesn't head-of-line block other
        # ops on this workspace behind the 60s lock-acquire ceiling.
        if pending_start_wait:
            return await self._await_in_flight_start(
                workspace_id,
                user_id=user_id,
                workspace_user_id=workspace_user_id,
                on_state_observed=on_state_observed,
                mark=_mark,
                attempt=_attempt,
            )

        # ── Phase 2: expensive sync OUTSIDE the lock (idempotent / self-guarded).
        # Coalesces same-worker callers on the dedupe gate, promotes a lazy start
        # to 'running' only once the sandbox is fully ready, and reverts to
        # 'stopped' on any failure. See _complete_phase2_sync.
        _mark("lock_and_init")
        session = await self._complete_phase2_sync(
            workspace_id,
            session,
            workspace_user_id=workspace_user_id,
            needs_sync=needs_sync,
            needs_deferred_sync=needs_deferred_sync,
            phase2_owner=phase2_owner,
            phase2_event=phase2_event,
            mark=_mark,
            ws_mcp_version=ws_mcp_version,
        )

        if _session_phases:
            total = sum(_session_phases.values())
            phases = " ".join(f"{k}={v:.0f}ms" for k, v in _session_phases.items())
            logger.info(
                f"[SESSION_TIMING] workspace_id={workspace_id} total={total:.0f}ms ({phases})"
            )
            # Classify path: cold_resume = lazy-restart path (needs_deferred_sync),
            # warm_sync = cached session that needed a sync refresh, cold_create =
            # first session for this workspace (not previously cached).
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

    async def _await_in_flight_start(
        self,
        workspace_id: str,
        *,
        user_id: str | None,
        workspace_user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
        mark: Callable[[str], None],
        attempt: int,
    ) -> Session:
        """Wait for another worker's in-flight start, then attach (or retry once).

        Entered when this caller lost the stopped→starting claim or arrived
        while status was already 'starting'. Runs OUTSIDE the per-workspace
        lock; re-acquires it only briefly to attach the now-running session.

        Raises:
            RuntimeError: the start ended in an unexpected status.
        """
        ws_done = await self._wait_for_start_completion(workspace_id)
        wait_status = ws_done["status"]
        if wait_status == "running":
            async with self._observed_lock(workspace_id, "workspace.session.attach"):
                session, _ = await self._attach_running_session(
                    ws_done, workspace_user_id, on_state_observed, mark
                )
            return session
        if wait_status == "stopped" and attempt == 0:
            # Owner failed and reverted to 'stopped'. Retry the whole start
            # once: the recursive call re-enters Phase 1 and re-claims,
            # becoming the owner with a full Phase 2 — so we don't have to
            # duplicate the claim+sync+promote logic here. Bounded to one
            # retry by the attempt guard.
            logger.info(
                f"Workspace {workspace_id} reverted to 'stopped' "
                "(prior owner failed); retrying start"
            )
            return await self.get_session_for_workspace(
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
        workspace_id: str,
        session: Session | None,
        *,
        workspace_user_id: str | None,
        needs_sync: bool,
        needs_deferred_sync: bool,
        phase2_owner: bool,
        phase2_event: Optional[asyncio.Event],
        mark: Callable[[str], None],
        ws_mcp_version: int | None = None,
    ) -> Session | None:
        """Run the post-lock sync/promote step and return the usable session.

        Expensive operations (ensure_sandbox_ready, asset sync, file restore)
        run OUTSIDE the per-workspace lock — they're idempotent or self-guarded.
        Same-worker callers coalesce on ``phase2_event``: the owner runs the
        work, waiters await the gate then trust the authoritative DB row. A lazy
        start is promoted to 'running' only after the sandbox is fully ready;
        any failure reverts the row to 'stopped' so it never lingers half-ready.
        Returns the session to hand back (possibly one freshly recovered from a
        SandboxGoneError).
        """
        if needs_sync and session and session.sandbox:
            if not phase2_owner:
                # Another caller on this worker is already running Phase 2.
                # Wait for them and return the session they hydrated.
                assert phase2_event is not None
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
                # The Phase 2 owner is the authoritative status writer:
                # 'running' on success, 'stopped'/'error' on failure. Trust the
                # DB, not the cached session — on owner failure the cache may
                # hold a stale or half-initialized session, and returning it
                # would silently hand back a broken sandbox.
                ws_after = await db_get_workspace(workspace_id)
                if ws_after is not None and ws_after["status"] == "running":
                    return self._sessions.get(workspace_id, session)
                raise RuntimeError(
                    f"Workspace {workspace_id} did not reach 'running' after "
                    f"Phase 2 (status={ws_after['status'] if ws_after else 'deleted'})"
                )

            try:
                await session.sandbox.ensure_sandbox_ready()
                mark("sandbox_ready")

                # Resolve + apply the per-workspace MCP composite BEFORE asset
                # sync so codegen (which reads session.sandbox.mcp_registry) sees
                # the effective set. Cheap (resolve + in-memory build); the slow
                # discovery is kicked in the background below. The version check
                # rides ws_mcp_version (piggybacked from the post-cooldown read),
                # so an unchanged config adds zero extra DB reads.
                _t_resolve = time.time()
                resolved_mcp = await self._apply_session_mcp(
                    workspace_id,
                    workspace_user_id,
                    session,
                    ws_version=ws_mcp_version,
                )
                mcp_changed = resolved_mcp is not None
                if mcp_changed:
                    logger.info(
                        "[ASSET_SYNC] workspace_id=%s mcp_resolve=%.0fms "
                        "version=%s",
                        workspace_id,
                        (time.time() - _t_resolve) * 1000,
                        session.mcp_config_version,
                    )
                    mark("mcp_resolve")

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
                    mark("asset_sync")
                    await self._maybe_restore_files(workspace_id, session.sandbox)
                    mark("file_restore")
                    # Promote to 'running' ONLY after the sandbox is ready AND
                    # assets + files are synced — so 'running' (and its pub/sub
                    # notification + SSE close) truthfully means "usable". Any
                    # failure above is caught below and reverts the row to
                    # 'stopped', never leaving a half-ready 'running'. A forced
                    # non-lazy restart already promoted inside _restart_workspace
                    # (not in _pending_lazy_sync) — no-op here.
                    if workspace_id in self._pending_lazy_sync:
                        await update_workspace_status(
                            workspace_id=workspace_id,
                            status="running",
                        )
                        await update_workspace_activity(workspace_id)
                        self._pending_lazy_sync.discard(workspace_id)
                elif mcp_changed:
                    # Warm re-sync path normally skips asset sync; a config-version
                    # delta means wrappers changed, so push them now (off the lock,
                    # bounded to changed modules by the manifest diff). No file
                    # restore / promotion — the running session already owns those.
                    await self._sync_sandbox_assets(
                        workspace_id,
                        workspace_user_id,
                        session.sandbox,
                        reusing_sandbox=True,
                    )
                    mark("mcp_asset_sync")

                # Kick background discovery for user servers still lacking ok
                # schemas (new/pending/error). Never awaited here and never under
                # the lock — stdio cold-start is up to 30s. On completion it
                # rebuilds this session's composite + re-syncs the new wrappers.
                if resolved_mcp is not None:
                    needing = self._servers_needing_discovery(session, resolved_mcp)
                    self._kick_mcp_discovery(
                        workspace_id,
                        workspace_user_id,
                        session,
                        needing,
                        session.mcp_config_version or 0,
                    )

                self._record_sync(workspace_id)
            except SandboxGoneError as e:
                logger.warning(
                    f"Sandbox gone for workspace {workspace_id} during "
                    f"Phase 2: {e}. Recovering."
                )
                # Identity check: a concurrent request may have already
                # installed a replacement session while we were running
                # Phase 2 outside the lock. Clearing that healthy session
                # would tear down its MCP+provider and double-spawn Daytona.
                # Pass evict_session so the pop inside _clear_session is
                # also identity-guarded across its own await boundary.
                if self._sessions.get(workspace_id) is session:
                    await self._clear_session(workspace_id, evict_session=session)

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
            except SandboxTransientError as e:
                # Narrow: if lazy init exhausted retries the session is
                # marked failed — clearing it removes the zombie so the
                # next request starts fresh. Post-init transient (asset
                # sync etc.) leaves sandbox healthy; best-effort retry.
                if session.sandbox.has_failed():
                    logger.warning(
                        f"Phase 2 init exhausted retries for {workspace_id}: "
                        f"{e}. Clearing session for fresh recovery."
                    )
                    # Identity check: a concurrent request may have already
                    # observed has_failed() in its own Phase 1, cleared this
                    # session, and installed a replacement. Clearing again
                    # would tear down the healthy replacement's MCP+provider.
                    # Pass evict_session so the pop inside _clear_session is
                    # also identity-guarded across its own await boundary.
                    # Revert BEFORE clearing the session — _clear_session
                    # discards _pending_lazy_sync, which would make the revert
                    # a no-op (it keys off pending membership).
                    await self._revert_unpromoted_lazy_start(workspace_id)
                    if self._sessions.get(workspace_id) is session:
                        await self._clear_session(workspace_id, evict_session=session)
                    raise
                logger.warning(
                    f"Phase 2 sync transient for workspace {workspace_id} "
                    f"(will retry next request): {e}"
                )
                # Capture before reverting — the revert clears _pending_lazy_sync.
                was_unpromoted_lazy = workspace_id in self._pending_lazy_sync
                await self._revert_unpromoted_lazy_start(workspace_id)
                if was_unpromoted_lazy:
                    # We just reverted this lazy start's row to 'stopped'. The
                    # sandbox is healthy (has_failed() was False), but returning
                    # the session now hands the caller a sandbox the DB says is
                    # 'stopped' — other workers would claim and spawn a second
                    # one (split-brain). Surface the transient so the caller
                    # re-claims cleanly, mirroring the generic Exception branch.
                    raise
                # Already-'running' re-sync hit a transient; the sandbox was
                # usable before this periodic sync, so keep the cached session
                # and let the next request retry.
            except asyncio.CancelledError:
                # A client disconnect or server shutdown mid-Phase-2 cancels
                # this coroutine. CancelledError is a BaseException, so without
                # this clause it bypasses every revert handler and leaves the
                # row wedged in 'starting' forever (no reaper would promote it,
                # and /start rejects non-'stopped'). Revert on a shielded task
                # so the DB write survives the cancellation; if the event loop
                # itself is tearing down, reap_stuck_starting_workspaces() is
                # the backstop on the next process. Re-raise to preserve
                # cancellation semantics.
                revert = asyncio.create_task(
                    self._revert_unpromoted_lazy_start(workspace_id)
                )
                try:
                    await asyncio.shield(revert)
                except asyncio.CancelledError:
                    pass
                raise
            except Exception as e:
                logger.warning(
                    f"Phase 2 sync failed for workspace {workspace_id}: {e}"
                )
                # Capture before reverting — the revert clears _pending_lazy_sync.
                was_unpromoted_lazy = workspace_id in self._pending_lazy_sync
                await self._revert_unpromoted_lazy_start(workspace_id)
                if was_unpromoted_lazy:
                    # Lazy start failed before promotion: we just reverted the
                    # row to 'stopped' and the sandbox never finished asset/file
                    # sync. Returning the session would hand the agent a
                    # half-initialized sandbox while the DB says 'stopped'.
                    # Surface the failure so the caller re-claims cleanly.
                    raise
                # Already-'running' re-sync hit a transient error; the sandbox
                # was usable before this periodic sync, so keep the cached
                # session and let the next request retry the sync.
            finally:
                # Release the dedupe gate so any waiter on this worker can
                # proceed. Identity-check the registry slot so a fresh event
                # installed by the NEXT caller (already past our finally)
                # isn't accidentally evicted.
                if phase2_event is not None:
                    phase2_event.set()
                    if self._phase2_events.get(workspace_id) is phase2_event:
                        self._phase2_events.pop(workspace_id, None)
        elif phase2_owner and phase2_event is not None:
            # We installed a dedupe event under the lock, but the Phase 2
            # precondition (session.sandbox) no longer holds — e.g. a
            # concurrent stop_workspace nulled the sandbox between lock
            # release and here. Release the gate so waiters (and any future
            # caller that would attach to this event) don't hang out the
            # full start timeout on an event nobody will ever set.
            phase2_event.set()
            if self._phase2_events.get(workspace_id) is phase2_event:
                self._phase2_events.pop(workspace_id, None)

        return session

    async def _revert_unpromoted_lazy_start(self, workspace_id: str) -> None:
        """Revert a lazy-start owner's row to 'stopped' when Phase 2 fails
        before promotion.

        The cross-worker mutex parks losers in ``_wait_for_start_completion``
        until the row leaves 'starting'. If the claim winner fails in Phase 2
        (ensure_sandbox_ready / asset sync / file restore) the row would
        otherwise stay 'starting' with no reaper, so every loser waits out the
        full ``start_wait_timeout``. Reverting to 'stopped' (and publishing via
        ``update_workspace_status``) lets losers retry the claim immediately.

        No-op once the row has been promoted to 'running' (the owner discards
        it from ``_pending_lazy_sync`` on promotion), and for non-lazy restarts
        (never added to ``_pending_lazy_sync``).
        """
        if workspace_id not in self._pending_lazy_sync:
            return
        self._pending_lazy_sync.discard(workspace_id)
        try:
            await update_workspace_status(workspace_id=workspace_id, status="stopped")
        except Exception:
            # Best-effort — if the revert itself fails, the start_wait_timeout
            # still recovers losers, just with worse latency.
            logger.exception(
                "Failed to revert workspace %s to 'stopped' after Phase 2 failure",
                workspace_id,
            )

    async def _attach_running_session(
        self,
        workspace: Dict[str, Any],
        workspace_user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
        mark: Callable[[str], None],
    ) -> tuple[Session, bool]:
        """Acquire/initialize a session for a workspace whose DB status is 'running'.

        Shared between the running-status branch and the cross-worker wait
        paths (where another worker just promoted status to 'running').
        Caller must hold the per-workspace `_observed_lock`.

        Returns:
            (session, did_init) — ``did_init`` is True when this call performed
            the cold initialization, False when the cached session was already
            initialized (caller should run Phase 2 sync).
        """
        workspace_id = str(workspace["workspace_id"])
        core_config = self.config.to_core_config()
        session = SessionManager.get_session(workspace_id, core_config)
        did_init = False

        if not session._initialized:
            sandbox_id = workspace.get("sandbox_id")
            try:
                await session.initialize(
                    sandbox_id=sandbox_id,
                    on_state_observed=on_state_observed,
                )
            except SandboxGoneError as e:
                await self._clear_session(workspace_id)
                logger.warning(
                    f"Sandbox {sandbox_id} unavailable for workspace "
                    f"{workspace_id} ({e}). Creating fresh sandbox."
                )
                recovered = await self._recover_sandbox(
                    workspace_id, workspace_user_id, core_config
                )
                return recovered, True
            mark("session_initialize")

            # Resolve + install the per-workspace composite before asset sync so
            # codegen uploads user-server wrappers. Cheap; discovery kicked in
            # the background (fire-and-forget — doesn't hold the lock).
            ws_version = (
                int(workspace.get("mcp_config_version") or 0)
                if workspace.get("mcp_config_version") is not None
                else 0
            )
            resolved_mcp = await self._apply_session_mcp(
                workspace_id, workspace_user_id, session, ws_version=ws_version
            )

            await self._sync_sandbox_assets(
                workspace_id,
                workspace_user_id,
                session.sandbox,
                reusing_sandbox=sandbox_id is not None,
            )
            mark("cold_asset_sync")

            # Cache the session BEFORE kicking discovery: the background task's
            # liveness gate (``self._sessions.get(workspace_id) is session``)
            # would otherwise see no cached session and exit permanently. If a
            # later step raises, don't leave this broken session cached — the
            # old code only cached after migration succeeded — so unwind.
            self._sessions[workspace_id] = session

            try:
                if resolved_mcp is not None:
                    self._kick_mcp_discovery(
                        workspace_id,
                        workspace_user_id,
                        session,
                        self._servers_needing_discovery(session, resolved_mcp),
                        session.mcp_config_version or 0,
                    )

                migrated = await self._maybe_migrate_sandbox(
                    workspace_id, workspace_user_id, session, workspace
                )
                if migrated is not None:
                    session = migrated
            except Exception:
                self._cancel_mcp_discovery(workspace_id)
                if self._sessions.get(workspace_id) is session:
                    self._sessions.pop(workspace_id, None)
                raise
            did_init = True

        self._sessions[workspace_id] = session
        return session, did_init

    async def _claim_and_restart(
        self,
        workspace_id: str,
        workspace_user_id: str | None,
        on_state_observed: Callable[[str], None] | None,
    ) -> Optional[Session]:
        """Try to win the stopped→starting claim and restart the workspace.

        On exception during restart, revert the row back to 'stopped' so other
        callers can retry immediately instead of waiting out the 300s timeout.

        Returns:
            Session if we won the claim and restarted, None if another worker
            had already moved the row out of 'stopped'.
        """
        claimed = await try_claim_workspace_for_start(workspace_id)
        if claimed is None:
            return None

        def _observe_and_broadcast(state: str) -> None:
            # Forward to the caller's own observer (the chat path uses it to
            # drive its in-conversation spinner).
            if on_state_observed is not None:
                on_state_observed(state)
            # Broadcast a slow-restore hint cross-worker. The pre-start sandbox
            # state is observed only by whoever wins the claim; without this,
            # a worker that lost the claim (or the /events SSE the frontend
            # opened on entry) never learns the restore is the slow 'archived'
            # kind. Publishing it on the status channel lets every consumer
            # show the right spinner regardless of who owns the start.
            if state == "archived":
                task = asyncio.create_task(
                    publish_status_change(
                        workspace_id, "starting", extra={"sandbox_state": state}
                    )
                )
                self._status_publish_tasks.add(task)
                task.add_done_callback(self._status_publish_tasks.discard)

        try:
            logger.info(
                f"Restarting workspace {workspace_id} (claimed for start)"
            )
            return await self._restart_workspace(
                claimed,
                user_id=workspace_user_id,
                lazy_init=True,
                on_state_observed=_observe_and_broadcast,
            )
        except Exception:
            # Best-effort revert — if it fails, the 300s timeout still recovers,
            # just with worse UX. Don't shadow the original exception.
            try:
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="stopped",
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
        """Wait for an in-flight start to resolve, using pub/sub + DB safety net.

        Used by the cross-worker mutex path: when ``try_claim_workspace_for_start``
        returns None, another worker (or process) is mid-start. We subscribe
        to the Redis status channel BEFORE re-reading the DB to close the
        race where the publish landed during the lock window, then await
        notifications with a 30 s ceiling so a missed message still re-reads
        the DB instead of waiting out the full timeout.

        When Redis is unavailable, falls back to the original exponential-
        backoff DB poll (0.5 s → 2 s cap).

        Returns the updated workspace dict.

        Raises:
            ValueError: Workspace deleted while waiting.
            RuntimeError: Status went to 'error', or timeout exceeded.
        """
        timeout = self.start_wait_timeout if max_wait_s is None else max_wait_s
        base_interval = (
            self.start_wait_poll_interval if poll_interval_s is None else poll_interval_s
        )
        max_interval = max(base_interval, 2.0)
        deadline = time.monotonic() + timeout

        async with subscribe_to_status(workspace_id) as wait_for_notify:
            # Read DB AFTER subscribing — closes the race where the
            # publish landed before our SUBSCRIBE completed.
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
                    # Pub/sub fast path. Cap at 30 s so a dropped publish
                    # still triggers a periodic DB re-read.
                    await wait_for_notify(min(remaining, 30.0))
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
        workspace: Dict[str, Any],
        user_id: str | None = None,
        lazy_init: bool = False,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> Session:
        """
        Restart a stopped workspace.

        Args:
            workspace: Workspace record from DB
            user_id: Optional user ID for syncing user data to sandbox
            lazy_init: If True, start sandbox in background for faster response
            on_state_observed: Optional callback forwarded to Session.initialize
                /initialize_lazy; invoked with the initial sandbox state so
                callers can distinguish ``archived`` from ``stopped`` restarts.

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

        _cold_start_t0 = time.monotonic()
        try:
            # Get session from SessionManager
            core_config = self.config.to_core_config()
            session = SessionManager.get_session(workspace_id, core_config)

            sandbox_gone = False

            # Try to reconnect to existing sandbox
            try:
                if lazy_init:
                    await session.initialize_lazy(
                        sandbox_id=sandbox_id,
                        on_state_observed=on_state_observed,
                    )
                    self._pending_lazy_sync.add(workspace_id)
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
                await self._clear_session(workspace_id)
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

            # Update DB status. Lazy path stops at "starting" so downstream
            # read-side callers (workspace_files.py, public.py) use DB/safe
            # fallbacks while Phase 2 resolves; Phase 2 promotes to "running"
            # and stamps activity once the sandbox is actually ready.
            # Non-lazy path completes synchronously here — keep the
            # stopped → running transition plus activity stamp (PR #152).
            if lazy_init:
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="starting",
                )
                # Cache session
                self._sessions[workspace_id] = session
                # No activity stamp: cleanup_idle_workspaces only sweeps
                # status="running", so "starting" rows are immune.
                logger.info(f"Workspace {workspace_id} restart initiated (lazy)")
            else:
                await update_workspace_status(
                    workspace_id=workspace_id,
                    status="running",
                )
                # Cache session
                self._sessions[workspace_id] = session
                # Stamp last_activity_at so the idle sweep cannot pick this
                # workspace up using a stale timestamp. Mirrors _recover_sandbox.
                await update_workspace_activity(workspace_id)
                logger.info(f"Workspace {workspace_id} restarted successfully")
            # Non-lazy: cold-start finished here. Lazy: only initiation finished;
            # the second-stage init runs in the background. Record both to keep
            # the histogram non-empty on the lazy path — frontend latency is
            # dominated by the non-lazy phase regardless.
            safe_record(workspace_cold_start_duration_ms, (time.monotonic() - _cold_start_t0) * 1000.0)
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
        async with self._observed_lock(workspace_id, "workspace.stop"):
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

            # Cancel in-flight background discovery before tearing down the
            # sandbox so it can't run against a dead sandbox / write orphan rows.
            self._cancel_mcp_discovery(workspace_id)

            try:
                # Backup files to DB before stopping sandbox
                await self._backup_files_to_db(workspace_id)

                # Stop the session (stops sandbox, preserves data)
                session = self._sessions.get(workspace_id)
                if session:
                    await session.stop()
                    # Remove from cache (will be recreated on restart)
                    del self._sessions[workspace_id]

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
        async with self._observed_lock(workspace_id, "workspace.archive"):
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
        async with self._observed_lock(workspace_id, "workspace.delete"):
            workspace = await db_get_workspace(workspace_id)
            if not workspace:
                raise ValueError(f"Workspace {workspace_id} not found")

            logger.info(f"Deleting workspace {workspace_id}")

            # Cancel in-flight background discovery before tearing down the
            # sandbox so it can't run against a dead sandbox / write orphan rows.
            self._cancel_mcp_discovery(workspace_id)

            try:
                # Backup files to DB before deleting (if sandbox is accessible)
                await self._backup_files_to_db(workspace_id)

                # Remove from local cache (SessionManager.cleanup_session handles actual cleanup)
                self._sessions.pop(workspace_id, None)

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

    async def reap_stuck_starting_workspaces(self) -> int:
        """Revert workspaces wedged in 'starting' back to 'stopped'.

        Backstop for the cross-worker start mutex: if a claim winner's Phase 2
        dies without reverting (worker crash, event-loop teardown that beats the
        CancelledError revert, or a publish that never lands), the row stays
        'starting' with no other recovery path — every later caller waits out
        start_wait_timeout then raises, and /start rejects non-'stopped'.

        Never reaps a start THIS process is still running: an in-flight lazy
        owner holds ``_pending_lazy_sync`` membership and will promote (on
        success) or revert (on failure) the row itself. Reaping it would discard
        that membership and silently no-op the owner's promotion, stranding a
        ready session behind a 'stopped' row and triggering a duplicate restart.
        That guard makes the in-process case correct regardless of how slow the
        restore is. The ``reap_stuck_after`` threshold (2x start_wait_timeout by
        default) then only governs the cross-process backstop — rows wedged by a
        crashed/recycled worker, which carry no local membership.

        Returns:
            Number of workspaces reverted.
        """
        now = datetime.now(timezone.utc)
        reverted = 0

        starting_workspaces = await get_workspaces_by_status("starting", limit=1000)
        if len(starting_workspaces) == 1000:
            logger.warning(
                "reap_stuck_starting hit the 1000-row scan cap; more stuck "
                "rows may remain and will be reaped on the next cycle"
            )
        for workspace in starting_workspaces:
            updated_at = workspace.get("updated_at")
            if not updated_at:
                continue
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            if (now - updated_at).total_seconds() <= self.reap_stuck_after:
                continue

            workspace_id = str(workspace["workspace_id"])
            if workspace_id in self._pending_lazy_sync:
                # A lazy-start owner on THIS worker is still mid-flight. It
                # owns the row's transition (promote on success, revert on
                # failure); reaping here would discard its membership and
                # no-op that promotion, leaving a ready session behind a
                # 'stopped' row. Cross-process stuck rows have no local
                # membership and fall through to the reap below.
                continue
            logger.warning(
                f"Reaping workspace {workspace_id} stuck in 'starting' for "
                f"{(now - updated_at).total_seconds():.0f}s "
                f"(threshold {self.reap_stuck_after:.0f}s); reverting to 'stopped'"
            )
            try:
                await update_workspace_status(
                    workspace_id=workspace_id, status="stopped"
                )
                self._pending_lazy_sync.discard(workspace_id)
                reverted += 1
            except Exception as e:
                logger.error(
                    f"Error reaping stuck-starting workspace {workspace_id}: {e}"
                )

        if reverted > 0:
            logger.info(f"Reaped {reverted} workspaces stuck in 'starting'")

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

        # Cancel any in-flight background MCP discovery tasks.
        for task in list(self._mcp_discovery_tasks):
            task.cancel()
        self._mcp_discovery_tasks.clear()
        self._mcp_discovery_tasks_by_ws.clear()

        # Clear session cache (don't stop workspaces on shutdown)
        self._sessions.clear()
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
