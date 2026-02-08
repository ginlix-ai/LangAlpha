"""Sandbox health monitoring utilities."""

import asyncio
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from ptc_agent.core.session import Session

logger = structlog.get_logger(__name__)


async def check_sandbox_health(session: "Session") -> bool:
    """Check if sandbox is running using Daytona SDK.

    Uses sandbox.refresh_data() to get fresh state from Daytona API,
    then checks if state is 'started'.

    Args:
        session: The Session object containing the sandbox

    Returns:
        True if sandbox is healthy (state='started'), False otherwise
    """
    if session is None or session.sandbox is None:
        return False

    try:
        # Get the underlying Daytona sandbox object
        daytona_sandbox = getattr(session.sandbox, "sandbox", None)
        if daytona_sandbox is None:
            return False

        # Refresh state from Daytona API (no side effects)
        await asyncio.to_thread(daytona_sandbox.refresh_data)

        # Check state - Daytona uses 'started' not 'running'
        state = daytona_sandbox.state
        if hasattr(state, "value"):
            state = state.value

        logger.debug("Sandbox health check", state=state)

    except Exception as e:  # noqa: BLE001
        # Broad exception catch is intentional - any error means unhealthy sandbox
        logger.warning("Sandbox health check failed", error=str(e))
        return False
    else:
        return state == "started"


class EmptyResultTracker:
    """Tracks consecutive empty tool results for sandbox health detection."""

    SENSITIVE_TOOLS = {"ls", "Glob", "Grep"}
    THRESHOLD = 2

    def __init__(self) -> None:
        """Initialize the empty result tracker."""
        self._count = 0

    def record(self, tool_name: str, content: str | None) -> bool:
        """Record a tool result. Returns True if threshold exceeded."""
        if tool_name not in self.SENSITIVE_TOOLS:
            return False

        is_empty = not content or content.strip() in ("", "[]", "{}")
        if is_empty:
            self._count += 1
            return self._count >= self.THRESHOLD
        self._count = 0
        return False

    def reset(self) -> None:
        """Reset the consecutive empty result counter."""
        self._count = 0
