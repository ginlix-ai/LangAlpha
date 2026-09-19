"""Session Management - Handle conversation lifecycle and sandbox persistence."""

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import structlog

from ptc_agent.config.core import CoreConfig

from .mcp_registry import MCPRegistry, get_global_registry
from .sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class EgressBinding:
    """What this process last pushed to a session's sandbox credential file.

    ``jwt_exp`` (from the mint, never recomputed) drives the cheap remint
    check; ``user_id`` lets the remint run on paths with no request user in
    hand. Not liveness truth — the grant rows are.
    """

    grants: Mapping[str, str]
    jwt_exp: float
    user_id: str


class Session:
    """Represents a conversation session with a persistent sandbox."""

    def __init__(
        self,
        session_label: str,
        config: CoreConfig,
        *,
        computer_id: str | None = None,
        resource_tier: str | None = None,
    ) -> None:
        """Initialize session.

        Args:
            session_label: What to call this session in logs. Not an identity:
                the cache is keyed per machine and several workspaces share one
                session, so nothing may read a workspace out of this.
            config: Application configuration
            computer_id: The machine this session's sandbox belongs to, when the
                caller has one. None for a caller with no notion of a computer,
                which is every use of this library outside the server.
            resource_tier: That machine's tier, which sizes how much runs on it
                at once. Travels with ``computer_id`` and is refreshed the same
                way, so a resize converges on the next acquisition.
        """
        self.session_label = session_label
        self.computer_id = computer_id
        self.resource_tier = resource_tier
        self.config = config
        # Pristine server list snapshotted before the WorkspaceManager mutates
        # ``config.mcp.servers`` to the resolved composite. Restored on stop() so
        # a restart re-resolves from built-ins, not the prior resolution.
        self._pristine_mcp_servers = list(config.mcp.servers)
        self.sandbox: PTCSandbox | None = None
        self.mcp_registry: MCPRegistry | None = None
        # The built-in registry this session connected/borrowed. ``mcp_registry``
        # above may be SWAPPED to a per-workspace composite that wraps this one;
        # cleanup/stop must disconnect the BUILTIN (when owned), never the
        # composite (which has no live subprocesses to tear down).
        self._builtin_mcp_registry: MCPRegistry | None = None
        # False means we're borrowing the process-global frozen registry;
        # stop/cleanup must NOT call disconnect_all on a borrowed instance.
        self._owns_mcp_registry: bool = False
        self._initialized = False

        # Per-workspace MCP resolution, cached once per session (resolved by the
        # WorkspaceManager under its lock, then re-used per turn so create_agent
        # never re-resolves or re-queries the DB). ``mcp_registry`` may be
        # swapped to a composite (built-ins + user servers) by the manager;
        # ``mcp_tool_summary`` is the precomputed prompt summary string;
        # ``mcp_config_version`` tags which workspace config version produced
        # the current composite + summary.
        self.mcp_tool_summary: str | None = None
        self.mcp_config_version: int | None = None
        # Which workspace that version belongs to. Sibling workspaces share one
        # session on a computer and their config versions are independent
        # counters, so the number alone cannot say whose composite is loaded.
        self.mcp_config_workspace_id: str | None = None
        # Untrusted servers with a current-fingerprint ok discovery snapshot,
        # recorded at composite install. Settlement lives here rather than in
        # the composite's tool lists: a server that legitimately advertises
        # zero tools is settled too, and must not re-probe every acquire.
        self.mcp_settled_servers: set[str] = set()
        # Tools bound to the model directly (server name -> the server's
        # ``DirectServerTools``), taken out of the composite at install so the
        # sandbox never gets a wrapper for them. Opaque here: the value is the
        # server layer's, which this library does not import. Execution
        # context, like the composite.
        self.direct_mcp_tools: dict[str, Any] = {}

        # Egress-relay binding for OAuth-connected servers: what THIS process
        # last pushed to the sandbox. Execution context only — grant truth is
        # the sandbox_egress_grants table (see services/egress/session_binding).
        self.egress_binding: EgressBinding | None = None

        # The platform-secret fleet generation whose bindings were last applied
        # to this session's sandbox (the ``mcp_config_version`` analog) — lets
        # the per-acquisition resync short-circuit without touching the provider.
        self.platform_secret_version: int | None = None

        # The skills delivery signature (user-tier view dir + disabled names)
        # this session's sandbox last synced under. Third stamp in the family
        # above, but content-derived: the acquire path compares it against the
        # value computed from the turn's already-loaded skill bundle and
        # re-runs the asset sync on mismatch, so a warm sandbox converges on
        # skill changes with no writer-side bump to forget. None means this
        # process can't vouch for the sandbox — the next carrying acquire
        # verifies once against the sandbox manifest and stamps.
        self.skills_signature: str | None = None

        # Last-writer sidecar for agent.md, taken once by the runtime-context
        # baseline to attribute the next change it observes. Best-effort by
        # design: it is process-local, so a write on another worker leaves it
        # empty and the provenance falls back to the file path alone. Never
        # truth, only colour; the content itself is always read fresh.
        self._agent_md_writer: dict[str, Any] | None = None

        logger.debug("Created session", session=session_label)

    def note_agent_md_write(self, stamp: dict[str, Any] | None = None) -> None:
        """Record who wrote agent.md, for the next change the baseline observes.

        The stamp is decoration on a change the content hash finds anyway, so
        a malformed one is dropped rather than raised.
        """
        if isinstance(stamp, dict) and stamp:
            self._agent_md_writer = dict(stamp)

    def take_agent_md_writer(self) -> dict[str, Any] | None:
        """The last agent.md write this process saw, cleared on read.

        Cleared so one local write labels one observed change: left in place,
        the stamp would attribute every later change, including one made from
        another worker, to a writer it never had.
        """
        writer = self._agent_md_writer
        self._agent_md_writer = None
        return dict(writer) if writer else None

    async def initialize(
        self,
        sandbox_id: str | None = None,
        sandbox_tokens: dict | None = None,
        user_id: str | None = None,
        workspace_id: str | None = None,
        on_state_observed: Callable[[str], None] | None = None,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        dir_name: str | None = None,
    ) -> None:
        """Initialize the session (connect MCP servers and setup sandbox).

        Args:
            sandbox_id: Optional existing sandbox ID to reconnect to instead of creating new
            sandbox_tokens: Optional scoped OAuth2 tokens for sandbox ginlix-data access
            user_id: User ID for token tracking in manifest.
            workspace_id: Workspace ID for token tracking in manifest.
            on_state_observed: Optional sync callback invoked with the initial
                sandbox state string when reconnecting (ignored on new-sandbox
                path since state doesn't exist yet). See PTCSandbox.reconnect.
            tier: Resource tier to size a newly-created sandbox at (ignored on the
                reconnect path — an existing sandbox keeps its size).
            auto_stop_minutes: Auto-stop interval override in minutes (0 for
                always-on) applied to a newly-created sandbox.
            dir_name: Folder this workspace owns on the computer. Session
                acquisition runs outside a turn, so the folder cannot come
                from the project context and travels as an argument.
        """
        if self._initialized:
            logger.warning(
                "Session already initialized", session=self.session_label
            )
            return

        logger.debug(
            "Initializing session",
            session=self.session_label,
            reconnecting=sandbox_id is not None,
        )

        # Borrow the global frozen registry when available; otherwise own one.
        global_registry = get_global_registry()
        if global_registry is not None:
            self.mcp_registry = global_registry
            self._owns_mcp_registry = False
        else:
            self.mcp_registry = MCPRegistry(self.config)
            self._owns_mcp_registry = True
        self._builtin_mcp_registry = self.mcp_registry

        if sandbox_id:
            # RECONNECT MODE: Run MCP connections and sandbox start in parallel
            self.sandbox = PTCSandbox(self.config, None)

            try:
                await asyncio.gather(
                    self.mcp_registry.connect_all(),
                    self.sandbox.reconnect(
                        sandbox_id, on_state_observed=on_state_observed
                    ),
                )
            except Exception:
                if self.mcp_registry and self._owns_mcp_registry:
                    try:
                        await self.mcp_registry.disconnect_all()
                    except Exception:
                        pass
                self.mcp_registry = None
                self._owns_mcp_registry = False
                self.sandbox = None
                raise

            self.sandbox.mcp_registry = self.mcp_registry

            logger.debug(
                "Reconnected to existing sandbox",
                session=self.session_label,
                sandbox_id=sandbox_id,
            )
        else:
            # NEW SANDBOX MODE: Run workspace setup and MCP connect concurrently
            self.sandbox = PTCSandbox(self.config, None)

            try:
                snapshot_name, _ = await asyncio.gather(
                    self.sandbox.setup_sandbox_workspace(
                        tier=tier,
                        auto_stop_minutes=auto_stop_minutes,
                        dir_name=dir_name,
                    ),
                    self.mcp_registry.connect_all(),
                )
            except Exception:
                if self.mcp_registry and self._owns_mcp_registry:
                    try:
                        await self.mcp_registry.disconnect_all()
                    except Exception:
                        pass
                self.mcp_registry = None
                self._owns_mcp_registry = False
                self.sandbox = None
                raise

            self.sandbox.mcp_registry = self.mcp_registry

            await self.sandbox.setup_tools_and_mcp(
                snapshot_name,
                tokens=sandbox_tokens,
                user_id=user_id,
                workspace_id=workspace_id,
            )

        self._initialized = True

        logger.debug("Session initialized", session=self.session_label)

    async def initialize_lazy(
        self,
        sandbox_id: str,
        on_state_observed: Callable[[str], None] | None = None,
    ) -> None:
        """Initialize session with lazy sandbox startup.

        MCP registry connects immediately, sandbox starts in background.
        Use for stopped workspaces to reduce latency.

        Args:
            sandbox_id: Existing sandbox ID to reconnect to
            on_state_observed: Optional sync callback invoked with the
                initial sandbox state once the background reconnect task
                observes it. Called asynchronously (after this method
                returns) when the background reconnect task reads state.
        """
        if self._initialized:
            logger.warning(
                "Session already initialized", session=self.session_label
            )
            return

        logger.debug(
            "Lazy initializing session",
            session=self.session_label,
            sandbox_id=sandbox_id,
        )

        _t0 = time.time()

        # Create sandbox and fire Daytona reconnect in background FIRST —
        # reconnect() is pure Daytona API calls, doesn't need the MCP registry.
        # This lets the sandbox start while MCP subprocesses are connecting.
        self.sandbox = PTCSandbox(self.config, mcp_registry=None)
        self.sandbox.start_lazy_init(sandbox_id, on_state_observed=on_state_observed)

        # Borrow the global frozen registry when available; otherwise own one.
        global_registry = get_global_registry()
        if global_registry is not None:
            self.mcp_registry = global_registry
            self._owns_mcp_registry = False
        else:
            self.mcp_registry = MCPRegistry(self.config)
            self._owns_mcp_registry = True
            await self.mcp_registry.connect_all()
        self._builtin_mcp_registry = self.mcp_registry
        mcp_ms = (time.time() - _t0) * 1000

        # Attach registry to sandbox (needed later for sync_sandbox_assets)
        self.sandbox.mcp_registry = self.mcp_registry

        self._initialized = True

        logger.info(
            f"[LAZY_INIT] sandbox_id={sandbox_id} mcp_connect={mcp_ms:.0f}ms "
            f"borrowed_global={not self._owns_mcp_registry}",
        )

    async def get_sandbox(self) -> PTCSandbox | None:
        """Get the sandbox for this session (initializes if needed).

        Returns:
            PTCSandbox instance
        """
        if not self._initialized:
            await self.initialize()

        return self.sandbox

    async def cleanup(self) -> None:
        """Clean up session resources."""
        logger.info("Cleaning up session", session=self.session_label)

        if self.sandbox:
            await self.sandbox.cleanup()
            self.sandbox = None

        # Disconnect the BUILTIN registry (never the composite — it has no live
        # subprocesses). mcp_registry may have been swapped to a composite.
        if self._builtin_mcp_registry and self._owns_mcp_registry:
            await self._builtin_mcp_registry.disconnect_all()
        self.mcp_registry = None
        self._builtin_mcp_registry = None
        self._owns_mcp_registry = False
        self.mcp_tool_summary = None
        self.mcp_config_version = None
        self.mcp_config_workspace_id = None
        self.direct_mcp_tools = {}
        # The binding records what the (now gone) sandbox held; keeping it
        # would violate that invariant and read as a teardown trigger on the
        # next sync.
        self.egress_binding = None

        self._initialized = False

        logger.info("Session cleaned up", session=self.session_label)

    async def stop(self) -> None:
        """Stop sandbox for session persistence.

        This is used when persist_session is enabled - stops the sandbox
        so it can be restarted quickly on the next session, rather than
        deleting it entirely.

        Important: this should *not* delete the underlying sandbox.
        It should, however, ensure the next start/restart path actually
        reinitializes and reconnects.
        """
        logger.info(
            "Stopping session for persistence", session=self.session_label
        )

        if self.sandbox:
            await self.sandbox.stop_sandbox()
            try:
                await self.sandbox.close()
            except Exception:
                pass

        if self._builtin_mcp_registry and self._owns_mcp_registry:
            await self._builtin_mcp_registry.disconnect_all()

        # Mark as uninitialized so the next restart will reconnect.
        # This preserves the fast early-return path in initialize() when the
        # session is genuinely already initialized.
        self._initialized = False
        self.sandbox = None
        self.mcp_registry = None
        self._builtin_mcp_registry = None
        self._owns_mcp_registry = False
        self.mcp_tool_summary = None
        self.mcp_config_version = None
        self.mcp_config_workspace_id = None
        self.direct_mcp_tools = {}
        self.egress_binding = None
        # Restore the pristine server list so a restart re-enters PTCSandbox with
        # the unresolved built-ins, not the stale per-workspace resolution.
        self.config.mcp.servers = list(self._pristine_mcp_servers)

        logger.info("Session stopped", session=self.session_label)

    async def __aenter__(self) -> "Session":
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.cleanup()


class SessionManager:
    """Process cache of live sessions, one per machine.

    ``session_key`` is whatever the caller uses to name a machine -- the server
    layer keys it by computer, so several projects on one machine share a single
    session and a single sandbox handle. It is deliberately opaque here: this
    library has no notion of a computer.
    """

    _sessions: dict[str, Session] = {}

    @classmethod
    async def stop_session(cls, session_key: str) -> None:
        """Stop (but do not delete) a specific session.

        This is intended for graceful shutdown / persistence: it stops the
        underlying sandbox so it can be reconnected later, but avoids calling
        Session.cleanup() which deletes the sandbox.

        Args:
            session_key: The machine whose session to stop.
        """
        if session_key in cls._sessions:
            session = cls._sessions[session_key]
            await session.stop()
            del cls._sessions[session_key]
            logger.info("Session stopped and removed", session_key=session_key)

    @classmethod
    async def stop_all(cls) -> None:
        """Stop all active sessions without deleting sandboxes."""
        logger.info("Stopping all sessions", count=len(cls._sessions))

        for session_key in list(cls._sessions.keys()):
            try:
                await cls.stop_session(session_key)
            except Exception as e:
                logger.warning(
                    "Error stopping session",
                    session_key=session_key,
                    error=str(e),
                )

        logger.info("All sessions stopped")

    @classmethod
    def get_session(
        cls,
        session_key: str,
        config: CoreConfig,
        *,
        label: str | None = None,
        computer_id: str | None = None,
        resource_tier: str | None = None,
    ) -> Session:
        """Get or create the session cached under *session_key*.

        ``label`` only names the session in logs and defaults to the key.
        ``computer_id`` names the machine and is refreshed on a cached session,
        because a worker can hand out a session it built before it learned the
        binding. ``resource_tier`` is refreshed for the same reason, and
        because the machine can be resized under a session this process is
        still holding. Nothing here carries a workspace: the caller's
        ``ProjectContext`` does, because one session serves several.
        """
        if session_key not in cls._sessions:
            logger.debug("Creating new session", session_key=session_key)
            cls._sessions[session_key] = Session(
                label or session_key,
                config,
                computer_id=computer_id,
                resource_tier=resource_tier,
            )
        else:
            logger.debug("Returning existing session", session_key=session_key)
            if computer_id is not None:
                cls._sessions[session_key].computer_id = computer_id
            if resource_tier is not None:
                cls._sessions[session_key].resource_tier = resource_tier

        return cls._sessions[session_key]

    @classmethod
    def get_cached_session(cls, session_key: str) -> Session | None:
        """Return the cached session without creating one.

        Lets callers identity-check what this process has cached before evicting
        it, so a concurrent replacement isn't thrown away.
        """
        return cls._sessions.get(session_key)

    @classmethod
    def remove_session(cls, session_key: str) -> None:
        """Remove a session from cache without stopping it.

        Used to evict broken sessions so the next request creates a fresh one.

        Args:
            session_key: The machine whose session to forget.
        """
        cls._sessions.pop(session_key, None)

    @classmethod
    async def cleanup_session(cls, session_key: str) -> None:
        """Clean up a specific session.

        Safe under concurrent callers: the final pop is identity-guarded
        so a second caller does not tear down a replacement session that
        another request installed while we were inside ``session.cleanup()``.

        Args:
            session_key: The machine whose session to clean up.
        """
        session = cls._sessions.get(session_key)
        if session is None:
            return

        await session.cleanup()
        # Identity-guarded pop: if a concurrent request replaced the entry
        # with a fresh session while we were awaiting cleanup(), leave the
        # replacement in place instead of evicting it.
        if cls._sessions.get(session_key) is session:
            cls._sessions.pop(session_key, None)
            logger.info("Session removed", session_key=session_key)
        else:
            logger.info(
                "Session cleaned up (replacement retained)",
                session_key=session_key,
            )

    @classmethod
    async def cleanup_all(cls) -> None:
        """Clean up all active sessions."""
        logger.info("Cleaning up all sessions", count=len(cls._sessions))

        for session_key in list(cls._sessions.keys()):
            await cls.cleanup_session(session_key)

        logger.info("All sessions cleaned up")

    @classmethod
    def get_active_sessions(cls) -> list[str]:
        """Get list of active session keys."""
        return list(cls._sessions.keys())

    @classmethod
    def get_session_count(cls) -> int:
        """Get count of active sessions.

        Returns:
            Number of active sessions
        """
        return len(cls._sessions)
