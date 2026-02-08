"""Tool call counter middleware for tracking subagent tool usage.

This middleware is injected into subagents to count their tool calls and
report metrics back to the BackgroundTaskRegistry.

It also emits a `subagent_identity` custom stream event on the first model
call so the streaming handler can map LangGraph namespace UUIDs to our
stable background task identities.
"""

from collections.abc import Awaitable, Callable

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background.middleware import current_background_task_id
from ptc_agent.agent.middleware.background.registry import BackgroundTaskRegistry

logger = structlog.get_logger(__name__)


class ToolCallCounterMiddleware(AgentMiddleware):
    """Middleware to count tool calls and report to BackgroundTaskRegistry.

    This middleware is designed to be injected into subagents running in the
    background. It tracks how many tool calls each subagent makes and what
    tools are being used.

    It also emits a ``subagent_identity`` custom stream event on the first
    model call.  The streaming handler receives this event *with* the
    LangGraph namespace tuple attached, which lets it register the mapping
    from opaque ``tools:<uuid>`` namespace to our stable ``agent_id``.

    The middleware uses a contextvar (current_background_task_id) to identify
    which background task it belongs to. Contextvars properly propagate across
    await boundaries, ensuring tool calls are tracked even when subagents
    execute in different execution contexts.

    Usage:
        # Create counter middleware with shared registry
        counter = ToolCallCounterMiddleware(registry=background_middleware.registry)

        # Inject into subagent specs
        subagent_spec["middleware"] = [counter]
    """

    def __init__(self, registry: BackgroundTaskRegistry) -> None:
        """Initialize the counter middleware.

        Args:
            registry: The BackgroundTaskRegistry to report metrics to
        """
        super().__init__()
        self.tools = []  # No additional tools
        self.registry = registry
        self._emitted_identity: set[str] = set()  # task_ids that already emitted identity event

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """Emit subagent_identity stream event on first model call.

        This runs BEFORE the LLM generates any output, so the streaming
        handler can register the namespace mapping before any message_chunk
        or tool_call_chunks events arrive.

        The custom event carries ``task_id``; the streaming infrastructure
        automatically attaches the correct ``namespace_tuple`` so the handler
        knows which LangGraph namespace UUID maps to which background task.
        """
        task_id = current_background_task_id.get()

        if task_id and self.registry and task_id not in self._emitted_identity:
            try:
                from langgraph.config import get_stream_writer

                writer = get_stream_writer()
                writer({"type": "subagent_identity", "task_id": task_id})
                self._emitted_identity.add(task_id)
                logger.debug(
                    "Emitted subagent_identity event",
                    task_id=task_id,
                )
            except Exception as e:
                logger.debug(
                    "Failed to emit subagent_identity event",
                    task_id=task_id,
                    error=str(e),
                )

        return await handler(request)

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        """Synchronous wrap_tool_call - no tracking in sync mode."""
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        """Count tool call and report to registry.

        This method:
        1. Extracts the tool name from the request
        2. Gets the task_id from the asyncio task context
        3. Reports the metric to the registry
        4. Executes the tool call
        """
        # Extract tool name
        tool_call = request.tool_call
        tool_name = tool_call.get("name", "unknown")

        # Get task_id from contextvar (set by BackgroundSubagentMiddleware)
        # Contextvars properly propagate across await boundaries
        task_id = current_background_task_id.get()

        # Report metric to registry before execution
        if task_id and self.registry:
            await self.registry.update_metrics(task_id, tool_name)
            logger.debug(
                "Counted tool call for background task",
                task_id=task_id,
                tool_name=tool_name,
            )

        # Execute the tool call
        return await handler(request)
