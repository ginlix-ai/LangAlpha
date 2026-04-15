"""Session Management - Handle conversation lifecycle and sandbox persistence."""

import asyncio
import time
from types import TracebackType

import structlog

from ptc_agent.config.core import CoreConfig

from .mcp_registry import MCPRegistry
from .sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


class Session:
    """Represents a conversation session with a persistent sandbox."""

    def __init__(self, conversation_id: str, config: CoreConfig) -> None:
        """Initialize session.

        Args:
            conversation_id: Unique conversation identifier
            config: Application configuration
        """
        self.conversation_id = conversation_id
        self.config = config
        self.sandbox: PTCSandbox | None = None
        self.mcp_registry: MCPRegistry | None = None
        self._initialized = False

        # agent.md cache with dirty flag (force first read)
        self._agent_md_cache: str | None = None
        self._agent_md_dirty: bool = True

        logger.debug("Created session", conversation_id=conversation_id)

    async def get_agent_md(self) -> str | None:
        """Read agent.md from sandbox, with session-level caching.

        Returns cached content unless invalidated by invalidate_agent_md().
        """
        if self._agent_md_dirty:
            if self.sandbox:
                try:
                    self._agent_md_cache = await self.sandbox.aread_file_text(
                        self.sandbox.normalize_path("agent.md")
                    )
                except Exception:
                    self._agent_md_cache = None
            else:
                self._agent_md_cache = None
            self._agent_md_dirty = False
        return self._agent_md_cache

    def invalidate_agent_md(self) -> None:
        """Mark agent.md cache as stale so the next get_agent_md() re-reads."""
        self._agent_md_dirty = True

    async def initialize(
        self,
        sandbox_id: str | None = None,
        sandbox_tokens: dict | None = None,
        user_id: str | None = None,
        workspace_id: str | None = None,
    ) -> None:
        """Initialize the session (connect MCP servers and setup sandbox).

        Args:
            sandbox_id: Optional existing sandbox ID to reconnect to instead of creating new
            sandbox_tokens: Optional scoped OAuth2 tokens for sandbox ginlix-data access
            user_id: User ID for token tracking in manifest.
            workspace_id: Workspace ID for token tracking in manifest.
        """
        if self._initialized:
            logger.warning(
                "Session already initialized", conversation_id=self.conversation_id
            )
            return

        logger.debug(
            "Initializing session",
            conversation_id=self.conversation_id,
            reconnecting=sandbox_id is not None,
        )

        # Initialize MCP registry
        self.mcp_registry = MCPRegistry(self.config)

        if sandbox_id:
            # RECONNECT MODE: Run MCP connections and sandbox start in parallel

            # Create sandbox instance without mcp_registry
            self.sandbox = PTCSandbox(self.config, None)

            # Run both operations in parallel
            try:
                await asyncio.gather(
                    self.mcp_registry.connect_all(),
                    self.sandbox.reconnect(sandbox_id),
                )
            except Exception:
                # Clean up MCP connections to avoid leaks
                if self.mcp_registry:
                    try:
                        await self.mcp_registry.disconnect_all()
                    except Exception:
                        pass
                    self.mcp_registry = None
                self.sandbox = None
                raise

            self.sandbox.mcp_registry = self.mcp_registry

            logger.debug(
                "Reconnected to existing sandbox",
                conversation_id=self.conversation_id,
                sandbox_id=sandbox_id,
            )
        else:
            # NEW SANDBOX MODE: Run workspace setup and MCP connect concurrently
            self.sandbox = PTCSandbox(self.config, None)

            try:
                snapshot_name, _ = await asyncio.gather(
                    self.sandbox.setup_sandbox_workspace(),
                    self.mcp_registry.connect_all(),
                )
            except Exception:
                if self.mcp_registry:
                    try:
                        await self.mcp_registry.disconnect_all()
                    except Exception:
                        pass
                    self.mcp_registry = None
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

        logger.debug("Session initialized", conversation_id=self.conversation_id)

    async def initialize_lazy(self, sandbox_id: str) -> None:
        """Initialize session with lazy sandbox startup.

        MCP registry connects immediately, sandbox starts in background.
        Use for stopped workspaces to reduce latency.

        Args:
            sandbox_id: Existing sandbox ID to reconnect to
        """
        if self._initialized:
            logger.warning(
                "Session already initialized", conversation_id=self.conversation_id
            )
            return

        logger.debug(
            "Lazy initializing session",
            conversation_id=self.conversation_id,
            sandbox_id=sandbox_id,
        )

        _t0 = time.time()

        # Create sandbox and fire Daytona reconnect in background FIRST —
        # reconnect() is pure Daytona API calls, doesn't need the MCP registry.
        # This lets the sandbox start while MCP subprocesses are connecting.
        self.sandbox = PTCSandbox(self.config, mcp_registry=None)
        self.sandbox.start_lazy_init(sandbox_id)

        # Initialize MCP registry (required for system prompt) — runs in
        # parallel with the Daytona sandbox start above.
        self.mcp_registry = MCPRegistry(self.config)
        await self.mcp_registry.connect_all()
        mcp_ms = (time.time() - _t0) * 1000

        # Attach registry to sandbox (needed later for sync_sandbox_assets)
        self.sandbox.mcp_registry = self.mcp_registry

        self._initialized = True

        logger.info(
            f"[LAZY_INIT] sandbox_id={sandbox_id} mcp_connect={mcp_ms:.0f}ms",
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
        logger.info("Cleaning up session", conversation_id=self.conversation_id)

        if self.sandbox:
            await self.sandbox.cleanup()
            self.sandbox = None

        if self.mcp_registry:
            await self.mcp_registry.disconnect_all()
            self.mcp_registry = None

        self._initialized = False
        self._agent_md_dirty = True

        logger.info("Session cleaned up", conversation_id=self.conversation_id)

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
            "Stopping session for persistence", conversation_id=self.conversation_id
        )

        if self.sandbox:
            await self.sandbox.stop_sandbox()
            try:
                await self.sandbox.close()
            except Exception:
                pass

        if self.mcp_registry:
            await self.mcp_registry.disconnect_all()

        # Mark as uninitialized so the next restart will reconnect.
        # This preserves the fast early-return path in initialize() when the
        # session is genuinely already initialized.
        self._initialized = False
        self._agent_md_dirty = True
        self.sandbox = None
        self.mcp_registry = None

        logger.info("Session stopped", conversation_id=self.conversation_id)

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
    """Manages multiple conversation sessions."""

    _sessions: dict[str, Session] = {}

    @classmethod
    async def stop_session(cls, conversation_id: str) -> None:
        """Stop (but do not delete) a specific session.

        This is intended for graceful shutdown / persistence: it stops the
        underlying sandbox so it can be reconnected later, but avoids calling
        Session.cleanup() which deletes the sandbox.

        Args:
            conversation_id: Conversation identifier
        """
        if conversation_id in cls._sessions:
            session = cls._sessions[conversation_id]
            await session.stop()
            del cls._sessions[conversation_id]
            logger.info("Session stopped and removed", conversation_id=conversation_id)

    @classmethod
    async def stop_all(cls) -> None:
        """Stop all active sessions without deleting sandboxes."""
        logger.info("Stopping all sessions", count=len(cls._sessions))

        for conversation_id in list(cls._sessions.keys()):
            try:
                await cls.stop_session(conversation_id)
            except Exception as e:
                logger.warning(
                    "Error stopping session",
                    conversation_id=conversation_id,
                    error=str(e),
                )

        logger.info("All sessions stopped")

    @classmethod
    def get_session(cls, conversation_id: str, config: CoreConfig) -> Session:
        """Get or create a session for a conversation.

        Args:
            conversation_id: Unique conversation identifier
            config: Application configuration

        Returns:
            Session instance
        """
        if conversation_id not in cls._sessions:
            logger.debug("Creating new session", conversation_id=conversation_id)
            cls._sessions[conversation_id] = Session(conversation_id, config)
        else:
            logger.debug("Returning existing session", conversation_id=conversation_id)

        return cls._sessions[conversation_id]

    @classmethod
    def remove_session(cls, conversation_id: str) -> None:
        """Remove a session from cache without stopping it.

        Used to evict broken sessions so the next request creates a fresh one.

        Args:
            conversation_id: Conversation identifier
        """
        cls._sessions.pop(conversation_id, None)

    @classmethod
    async def cleanup_session(cls, conversation_id: str) -> None:
        """Clean up a specific session.

        Args:
            conversation_id: Conversation identifier
        """
        if conversation_id in cls._sessions:
            session = cls._sessions[conversation_id]
            await session.cleanup()
            del cls._sessions[conversation_id]

            logger.info("Session removed", conversation_id=conversation_id)

    @classmethod
    async def cleanup_all(cls) -> None:
        """Clean up all active sessions."""
        logger.info("Cleaning up all sessions", count=len(cls._sessions))

        for conversation_id in list(cls._sessions.keys()):
            await cls.cleanup_session(conversation_id)

        logger.info("All sessions cleaned up")

    @classmethod
    def get_active_sessions(cls) -> list[str]:
        """Get list of active session IDs.

        Returns:
            List of conversation IDs
        """
        return list(cls._sessions.keys())

    @classmethod
    def get_session_count(cls) -> int:
        """Get count of active sessions.

        Returns:
            Number of active sessions
        """
        return len(cls._sessions)
