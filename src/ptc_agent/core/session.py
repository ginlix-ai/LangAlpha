"""Session Management - Handle conversation lifecycle and sandbox persistence."""

import asyncio
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

        logger.info("Created session", conversation_id=conversation_id)

    async def initialize(self, sandbox_id: str | None = None) -> None:
        """Initialize the session (connect MCP servers and setup sandbox).

        Args:
            sandbox_id: Optional existing sandbox ID to reconnect to instead of creating new
        """
        if self._initialized:
            logger.warning("Session already initialized", conversation_id=self.conversation_id)
            return

        logger.info(
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
            await asyncio.gather(
                self.mcp_registry.connect_all(),
                self.sandbox.reconnect(sandbox_id),
            )

            self.sandbox.mcp_registry = self.mcp_registry

            logger.info(
                "Reconnected to existing sandbox",
                conversation_id=self.conversation_id,
                sandbox_id=sandbox_id,
            )
        else:
            # NEW SANDBOX MODE: Run workspace setup and MCP connect concurrently
            self.sandbox = PTCSandbox(self.config, None)

            snapshot_name, _ = await asyncio.gather(
                self.sandbox.setup_sandbox_workspace(),
                self.mcp_registry.connect_all(),
            )

            self.sandbox.mcp_registry = self.mcp_registry

            await self.sandbox.setup_tools_and_mcp(snapshot_name)

        self._initialized = True

        logger.info("Session initialized", conversation_id=self.conversation_id)

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
        logger.info("Stopping session for persistence", conversation_id=self.conversation_id)

        if self.sandbox:
            await self.sandbox.stop_sandbox()

        if self.mcp_registry:
            await self.mcp_registry.disconnect_all()

        # Mark as uninitialized so the next restart will reconnect.
        # This preserves the fast early-return path in initialize() when the
        # session is genuinely already initialized.
        self._initialized = False
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
            logger.info("Creating new session", conversation_id=conversation_id)
            cls._sessions[conversation_id] = Session(conversation_id, config)
        else:
            logger.debug("Returning existing session", conversation_id=conversation_id)

        return cls._sessions[conversation_id]

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
