"""
PTC Graph Factory - Build per-conversation LangGraph graphs.

This module creates LangGraph-compatible graphs for the PTC agent,
with parameterized session handling for flexibility across different
deployment contexts (CLI, server, etc.).

Key Features:
- SessionProvider protocol for dependency injection
- Reusable outside server context
- Support for custom session management strategies
"""

import asyncio
import logging
from typing import Any, Protocol, runtime_checkable

from ptc_agent.agent.agent import PTCAgent
from ptc_agent.config import AgentConfig
from ptc_agent.core.session import Session

logger = logging.getLogger(__name__)


async def get_user_profile_for_prompt(user_id: str) -> dict[str, Any] | None:
    """Fetch user profile data for system prompt injection.

    Args:
        user_id: The user's unique identifier

    Returns:
        Dict with name, timezone, locale, agent_preference if found, None otherwise
    """
    try:
        from src.server.database import user as user_db

        result = await user_db.get_user_with_preferences(user_id)
        if result:
            user = result.get("user", {})
            preferences = result.get("preferences", {}) or {}
            return {
                "name": user.get("name"),
                "timezone": user.get("timezone"),
                "locale": user.get("locale"),
                "agent_preference": preferences.get("agent_preference"),
            }
    except Exception as e:
        logger.warning(f"Failed to fetch user profile for {user_id}: {e}")
    return None


@runtime_checkable
class SessionProvider(Protocol):
    """Protocol for session management.

    Implementations provide get_or_create_session() for different contexts:
    - Server: Uses SessionService for per-conversation sessions
    - CLI: Uses SessionManager for standalone sessions
    - Testing: Uses mock sessions

    Example implementation:
        class MySessionProvider:
            async def get_or_create_session(
                self, conversation_id: str, sandbox_id: str | None = None
            ) -> Session:
                # Return or create a Session instance
                ...
    """

    async def get_or_create_session(
        self, conversation_id: str, sandbox_id: str | None = None
    ) -> Session:
        """Get or create a session for the given conversation.

        Args:
            conversation_id: Unique conversation identifier
            sandbox_id: Optional sandbox ID to reconnect to existing sandbox

        Returns:
            Initialized Session instance with sandbox and MCP registry
        """
        ...


async def build_ptc_graph(
    conversation_id: str,
    config: AgentConfig,
    session_provider: SessionProvider,
    subagent_names: list[str] | None = None,
    sandbox_id: str | None = None,
    operation_callback: Any | None = None,
    checkpointer: Any | None = None,
    background_registry: Any | None = None,
) -> Any:
    """
    Build a compiled LangGraph for a specific conversation.

    This creates a per-conversation sandbox session and wraps the PTCAgent
    in a LangGraph-compatible graph structure.

    Args:
        conversation_id: Unique conversation identifier for session management
        config: AgentConfig with LLM and tool configuration
        session_provider: SessionProvider implementation for session management
        subagent_names: Optional list of subagent names to enable
        sandbox_id: Optional specific sandbox ID to use (for reconnecting to existing sandbox)
        operation_callback: Optional callback for file operation logging
        checkpointer: Optional LangGraph checkpointer for state persistence (e.g., AsyncPostgresSaver)
        background_registry: Optional shared registry for background subagent tasks

    Returns:
        Compiled StateGraph compatible with LangGraph streaming

    Example:
        # Using with a custom session provider
        session_provider = MySessionProvider(config)
        ptc_graph = await build_ptc_graph(
            "conv-123",
            agent_config,
            session_provider,
            checkpointer=checkpointer
        )
        async for event in ptc_graph.astream(input_state, config):
            process_event(event)
    """
    logger.info(f"Building PTC graph for conversation: {conversation_id}")

    # Get session from provider
    session = await session_provider.get_or_create_session(
        conversation_id=conversation_id,
        sandbox_id=sandbox_id,
    )

    if not session.sandbox or not session.mcp_registry:
        raise RuntimeError(
            f"Failed to initialize session for conversation {conversation_id}"
        )

    # Create PTCAgent instance (blocking I/O wrapped in thread)
    ptc_agent = await asyncio.to_thread(PTCAgent, config)

    # Create the inner agent with conversation-specific sandbox.
    # IMPORTANT: pass the server checkpointer into the deepagent so that partial
    # progress (tools, intermediate messages, etc.) is checkpointed frequently.
    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=session.mcp_registry,
        subagent_names=subagent_names or config.subagents_enabled,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
    )

    logger.info(
        f"Created PTC agent for {conversation_id} with "
        f"subagents: {subagent_names or config.subagents_enabled} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    # Return the deepagent/orchestrator directly.
    # It supports .astream/.ainvoke/.aget_state and will persist state via checkpointer.
    return inner_agent


async def build_ptc_graph_with_session(
    session: Session,
    config: AgentConfig,
    subagent_names: list[str] | None = None,
    operation_callback: Any | None = None,
    checkpointer: Any | None = None,
    background_registry: Any | None = None,
    user_id: str | None = None,
) -> Any:
    """
    Build a compiled LangGraph using a provided session.

    This is used for scenarios where the session is managed externally
    (e.g., workspace-based requests where WorkspaceManager handles sessions).

    Args:
        session: Pre-initialized Session with sandbox and MCP registry
        config: AgentConfig with LLM and tool configuration
        subagent_names: Optional list of subagent names to enable
        operation_callback: Optional callback for file operation logging
        checkpointer: Optional LangGraph checkpointer for state persistence
        background_registry: Optional shared registry for background subagent tasks
        user_id: Optional user ID for fetching user profile to inject into system prompt

    Returns:
        Compiled StateGraph compatible with LangGraph streaming

    Example:
        session = await workspace_manager.get_session_for_workspace(workspace_id)
        ptc_graph = await build_ptc_graph_with_session(
            session,
            config,
            checkpointer=checkpointer
        )
        async for event in ptc_graph.astream(input_state, config):
            process_event(event)
    """
    workspace_id = session.conversation_id
    logger.info(f"Building PTC graph with session for workspace: {workspace_id}")

    if not session.sandbox or not session.mcp_registry:
        raise RuntimeError(
            f"Session for workspace {workspace_id} is not properly initialized"
        )

    # Fetch user profile for prompt injection
    user_profile = None
    if user_id:
        user_profile = await get_user_profile_for_prompt(user_id)
        if user_profile:
            logger.debug(f"Loaded user profile for {user_id}: {user_profile}")

    # Create PTCAgent instance (blocking I/O wrapped in thread)
    ptc_agent = await asyncio.to_thread(PTCAgent, config)

    # Create the inner agent with the session's sandbox.
    # IMPORTANT: pass the server checkpointer into the deepagent so that partial
    # progress (tools, intermediate messages, etc.) is checkpointed frequently.
    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=session.mcp_registry,
        subagent_names=subagent_names or config.subagents_enabled,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
        user_profile=user_profile,
    )

    logger.info(
        f"Created PTC agent for workspace {workspace_id} with "
        f"subagents: {subagent_names or config.subagents_enabled} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    # Return the deepagent/orchestrator directly.
    # It supports .astream/.ainvoke/.aget_state and will persist state via checkpointer.
    return inner_agent
