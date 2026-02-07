"""Background subagent execution middleware.

This middleware intercepts 'Task' tool calls and spawns them in the background,
allowing the main agent to continue working without blocking.
"""

import asyncio
import contextvars
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import AgentState
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background.registry import BackgroundTaskRegistry
from ptc_agent.agent.middleware.background.tools import (
    create_task_output_tool,
    create_wait_tool,
)

# This ContextVar propagates task_id to subagent tool calls, used by
# ToolCallCounterMiddleware to track which background task a tool call
# belongs to.
current_background_task_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("current_background_task_id", default=None)

logger = structlog.get_logger(__name__)


def _truncate_description(description: str, max_sentences: int = 2) -> str:
    """Truncate description to first N sentences.

    Args:
        description: Full task description
        max_sentences: Maximum number of sentences to keep

    Returns:
        Truncated description ending at the Nth period
    """
    sentences = []
    remaining = description
    for _ in range(max_sentences):
        period_idx = remaining.find(".")
        if period_idx == -1:
            sentences.append(remaining)
            break
        sentences.append(remaining[: period_idx + 1])
        remaining = remaining[period_idx + 1 :].lstrip()
        if not remaining:
            break
    return " ".join(sentences)


class BackgroundSubagentMiddleware(AgentMiddleware):
    """Middleware that enables background subagent execution.

    This middleware intercepts 'Task' tool calls and:
    1. Spawns the subagent execution in a background asyncio task
    2. Returns an immediate pseudo-result to the main agent
    3. Tracks pending tasks in a registry
    4. Collects results after the agent ends via the waiting room pattern

    The main agent can continue with other work while subagents execute
    in the background. When the agent finishes its current work, the
    BackgroundSubagentOrchestrator will collect pending results and
    re-invoke the agent for synthesis.

    Usage:
        middleware = BackgroundSubagentMiddleware(timeout=60.0)
        agent = create_deep_agent(
            model=...,
            tools=...,
            middleware=[middleware],
        )
        orchestrator = BackgroundSubagentOrchestrator(agent, middleware)
        result = await orchestrator.ainvoke(input_state)
    """

    def __init__(
        self,
        timeout: float = 60.0,
        *,
        enabled: bool = True,
        registry: BackgroundTaskRegistry | None = None,
    ) -> None:
        """Initialize the middleware.

        Args:
            timeout: Maximum time to wait for background tasks (seconds)
            enabled: Whether background execution is enabled
            registry: Optional shared registry for background tasks
        """
        super().__init__()
        self.registry = registry or BackgroundTaskRegistry()
        self.timeout = timeout
        self.enabled = enabled

        # Create native tools for this middleware
        # These allow the main agent to wait for and check on background tasks
        self.tools = [
            create_wait_tool(self),
            create_task_output_tool(self.registry),
        ]

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        """Synchronous wrap_tool_call - delegates to blocking execution.

        For sync execution, we can't spawn background tasks, so we
        fall back to normal blocking execution.
        """
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        """Intercept task tool calls and spawn in background.

        For 'Task' tool calls, this:
        1. Spawns the subagent execution as a background asyncio task
        2. Returns an immediate pseudo-result
        3. Stores the task in the registry for later result collection

        For all other tools, passes through to the handler normally.
        """
        # Get tool name from request
        tool_call = request.tool_call
        tool_name = tool_call.get("name", "")

        # Only intercept 'Task' tool calls when enabled
        if not self.enabled or tool_name != "Task":
            return await handler(request)

        # Extract task details
        tool_call_id = tool_call.get("id", "unknown")
        if not tool_call_id or tool_call_id == "unknown":
            raise RuntimeError("Tool call ID is required for background tasks")
        args = tool_call.get("args", {})
        description = args.get("description", "unknown task")
        subagent_type = args.get("subagent_type", "general-purpose")

        # Register the task first to get the task number
        task = await self.registry.register(
            task_id=tool_call_id,
            description=description,
            subagent_type=subagent_type,
            asyncio_task=None,  # Will be set after task creation
        )
        task_number = task.task_number

        logger.info(
            "Intercepting task tool call for background execution",
            tool_call_id=tool_call_id,
            task_number=task_number,
            display_id=task.display_id,
            subagent_type=subagent_type,
            description=description[:100],
        )

        current_background_task_id.set(tool_call_id)

        # Define the background execution coroutine
        async def execute_in_background() -> dict[str, Any]:
            """Execute the subagent in the background."""
            # Context already has task_id from parent
            #
            # Important: the main agent run may be interrupted/cancelled. Background subagent
            # execution must continue independently, so we shield the handler coroutine.
            async def run_handler() -> ToolMessage | Command:
                return await handler(request)

            handler_task: asyncio.Task[ToolMessage | Command] = asyncio.create_task(run_handler())
            task.handler_task = handler_task
            try:
                result = await asyncio.shield(handler_task)
                logger.debug(
                    "Background subagent completed",
                    tool_call_id=tool_call_id,
                    display_id=task.display_id,
                    result_type=type(result).__name__,
                )
                return {"success": True, "result": result}
            except asyncio.CancelledError:
                # If this background wrapper is cancelled, keep the underlying handler running
                # and await it to completion so the registry can still observe a result.
                logger.info(
                    "Background subagent cancellation requested; continuing",
                    tool_call_id=tool_call_id,
                    display_id=task.display_id,
                )
                try:
                    result = await handler_task
                    return {"success": True, "result": result}
                except Exception as e:
                    logger.error(
                        "Background subagent failed after cancellation",
                        tool_call_id=tool_call_id,
                        display_id=task.display_id,
                        error=str(e),
                    )
                    return {"success": False, "error": str(e), "error_type": type(e).__name__}
            except Exception as e:
                logger.error(
                    "Background subagent failed",
                    tool_call_id=tool_call_id,
                    display_id=task.display_id,
                    error=str(e),
                )
                return {"success": False, "error": str(e), "error_type": type(e).__name__}

        # Spawn background task
        asyncio_task = asyncio.create_task(
            execute_in_background(),
            name=f"background_subagent_{task.display_id}",
        )

        # Update the task with the asyncio task reference
        task.asyncio_task = asyncio_task

        # Return immediate pseudo-result with Task-N format
        short_description = _truncate_description(description, max_sentences=2)
        pseudo_result = (
            f"Background subagent deployed: **{task.display_id}**\n"
            f"- Type: {subagent_type}\n"
            f"- Task: {short_description}\n"
            f"- Status: Running in background\n\n"
            f"You can:\n"
            f"- Continue with other work\n"
            f"- Use `TaskOutput(task_number={task_number})` to get progress or result\n"
            f"- Use `Wait(task_number={task_number})` to block until complete\n"
            f"- Use `Wait()` to wait for all background tasks"
        )

        return ToolMessage(
            content=pseudo_result,
            tool_call_id=tool_call_id,
            name="Task",
        )

    def after_agent(self, state: AgentState, runtime: Any) -> dict[str, Any] | None:
        """Sync after_agent - no-op for sync execution."""
        return None

    async def aafter_agent(self, state: AgentState, runtime: Any) -> dict[str, Any] | None:
        """No-op hook after agent ends.

        The waiting room logic has been moved to the orchestrator.
        This hook returns immediately without blocking.
        """
        return None

    def clear_registry(self) -> None:
        """Clear the task registry.

        Should be called by the orchestrator after handling all tasks.
        """
        self.registry.clear()
        logger.debug("Cleared background task registry")

    async def cancel_all_tasks(self, *, force: bool = False) -> int:
        """Cancel all pending background tasks.

        Args:
            force: Cancel underlying handler tasks as well

        Returns:
            Number of tasks cancelled
        """
        return await self.registry.cancel_all(force=force)

    @property
    def pending_task_count(self) -> int:
        """Get the number of pending background tasks."""
        return self.registry.pending_count
