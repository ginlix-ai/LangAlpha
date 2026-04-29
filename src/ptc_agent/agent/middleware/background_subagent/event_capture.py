"""Subagent event capture middleware.

Injected into subagents running in the background. It:

- Captures LLM output events (message_chunk/tool_calls/tool_call_result) into
  ``BackgroundTask.captured_events_tail`` for post-interrupt persistence and
  per-task SSE replay.
- Emits a ``subagent_identity`` custom stream event on the first model call so
  the streaming handler can map LangGraph namespace UUIDs to stable background
  task identities.
- Reports tool-call metrics (``total_tool_calls``, ``tool_call_counts``,
  ``current_tool``) back to the ``BackgroundTaskRegistry``.
"""

import time
from collections.abc import Awaitable, Callable

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.middleware import current_background_tool_call_id
from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTaskRegistry

logger = structlog.get_logger(__name__)

# Hard cap on per-event captured content. Captured events live in memory on
# BackgroundTask.captured_events_tail until task completion; a single huge
# tool result (e.g., bash dumping a multi-MB file) would otherwise sit in RAM
# for the rest of the task lifetime. The LLM sees the full result via the
# normal LangGraph message flow — this cap only affects the SSE display copy.
_MAX_CAPTURED_CONTENT_BYTES = 256 * 1024


def _truncate_content(content: str) -> str:
    """Cap captured event content to ``_MAX_CAPTURED_CONTENT_BYTES``.

    Measures UTF-8 byte length so the cap is deterministic regardless of
    which non-ASCII characters the subagent emits.
    """
    encoded = content.encode("utf-8")
    if len(encoded) <= _MAX_CAPTURED_CONTENT_BYTES:
        return content
    truncated = encoded[:_MAX_CAPTURED_CONTENT_BYTES].decode("utf-8", errors="ignore")
    return (
        truncated
        + f"\n\n[...truncated, {len(encoded) - _MAX_CAPTURED_CONTENT_BYTES} more bytes]"
    )


def _tool_message_to_event_data(msg: ToolMessage, agent_id: str) -> dict:
    """Build the ``data`` payload for a captured ``tool_call_result`` event.

    Shared by the direct-ToolMessage branch and the Command-wrapped branch in
    ``awrap_tool_call``. Keeps artifact handling and content stringification in
    one place so the two code paths can't drift.
    """
    content = msg.content if isinstance(msg.content, str) else str(msg.content)
    data: dict = {
        "agent": agent_id,
        "id": getattr(msg, "id", ""),
        "role": "assistant",
        "tool_call_id": msg.tool_call_id,
        "content": _truncate_content(content),
        "content_type": "text",
    }
    if getattr(msg, "artifact", None) is not None:
        data["artifact"] = msg.artifact
    return data


class SubagentEventCaptureMiddleware(AgentMiddleware):
    """Middleware to capture subagent events and report metrics to BackgroundTaskRegistry.

    Responsibilities:

    - Capture LLM output events (reasoning, text, tool_calls, tool_call_result)
      into ``BackgroundTask.captured_events_tail`` so they can be replayed to
      SSE clients that connect (or reconnect) to a per-task stream.
    - Emit a ``subagent_identity`` custom stream event on the first model call.
      The streaming handler receives this event *with* the LangGraph namespace
      tuple attached, which lets it register the mapping from opaque
      ``tools:<uuid>`` namespace to our stable ``agent_id``.
    - Report tool-call metrics (count, top tools, current tool) so the main
      agent's ``TaskOutput`` tool can show meaningful progress.

    The middleware uses a contextvar (current_background_tool_call_id) to identify
    which background task it belongs to. Contextvars properly propagate across
    await boundaries, ensuring tool calls are tracked even when subagents
    execute in different execution contexts.

    Usage:
        # Create event capture middleware with shared registry
        event_capture = SubagentEventCaptureMiddleware(registry=background_middleware.registry)

        # Inject into subagent specs
        subagent_spec["middleware"] = [event_capture]
    """

    def __init__(self, registry: BackgroundTaskRegistry) -> None:
        """Initialize the event capture middleware.

        Args:
            registry: The BackgroundTaskRegistry to report metrics to
        """
        super().__init__()
        self.tools = []  # No additional tools
        self.registry = registry
        self._emitted_identity: set[str] = (
            set()
        )  # task_ids that already emitted identity event

    def clear_identity(self, tool_call_id: str) -> None:
        """Remove a tool_call_id from the emitted identity set.

        Called when resuming a completed subagent so that the
        ``subagent_identity`` event is re-emitted on the resumed
        invocation's first model call, allowing the streaming handler
        to register new namespace UUID mappings.

        Args:
            tool_call_id: The tool_call_id to clear
        """
        self._emitted_identity.discard(tool_call_id)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """Emit subagent_identity stream event on first model call.

        This runs BEFORE the LLM generates any output, so the streaming
        handler can register the namespace mapping before any message_chunk
        or tool_call_chunks events arrive.

        The custom event carries ``tool_call_id``; the streaming infrastructure
        automatically attaches the correct ``namespace_tuple`` so the handler
        knows which LangGraph namespace UUID maps to which background task.
        """
        tool_call_id = current_background_tool_call_id.get()

        if tool_call_id and self.registry and tool_call_id not in self._emitted_identity:
            try:
                from langgraph.config import get_stream_writer

                writer = get_stream_writer()
                writer({"type": "subagent_identity", "tool_call_id": tool_call_id})
                self._emitted_identity.add(tool_call_id)
                logger.debug(
                    "Emitted subagent_identity event",
                    tool_call_id=tool_call_id,
                )
            except Exception as e:
                logger.debug(
                    "Failed to emit subagent_identity event",
                    tool_call_id=tool_call_id,
                    error=str(e),
                )

        response = await handler(request)

        # Capture events for post-interrupt persistence
        tool_call_id = current_background_tool_call_id.get()
        if tool_call_id and self.registry:
            try:
                ai_msg = response.result[0] if response.result else None
                if ai_msg:
                    tool_calls = getattr(ai_msg, "tool_calls", None) or []
                    agent_id = self._get_agent_id(tool_call_id)
                    msg_id = getattr(ai_msg, "id", f"msg-{tool_call_id}")

                    # Text/reasoning tokens are forwarded per-token by
                    # ``_SubagentTokenForwarder``; capturing them here too
                    # would duplicate every chunk. Tool calls have no streaming
                    # counterpart so they stay captured here.
                    if tool_calls:
                        await self.registry.append_captured_event(
                            tool_call_id,
                            {
                                "event": "tool_calls",
                                "data": {
                                    "agent": agent_id,
                                    "id": msg_id,
                                    "role": "assistant",
                                    "tool_calls": [
                                        {
                                            "name": tc["name"],
                                            "args": tc.get("args", {}),
                                            "id": tc["id"],
                                            "type": "tool_call",
                                        }
                                        for tc in tool_calls
                                    ],
                                    "finish_reason": "tool_calls",
                                },
                                "ts": time.time(),
                            },
                        )

            except Exception as e:
                # Never break the agent for capture failures, but leave a
                # breadcrumb so capture regressions are debuggable.
                logger.debug(
                    "Failed to capture model output events",
                    tool_call_id=tool_call_id,
                    error=str(e),
                )

        return response

    def _get_agent_id(self, tool_call_id: str) -> str:
        """Resolve agent identifier in task:{task_id} format."""
        task = self.registry._tasks.get(tool_call_id)
        return f"task:{task.task_id}" if task else f"subagent:{tool_call_id}"

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

        # Get tool_call_id from contextvar (set by BackgroundSubagentMiddleware)
        # Contextvars properly propagate across await boundaries
        tool_call_id = current_background_tool_call_id.get()

        # Report metric to registry before execution
        if tool_call_id and self.registry:
            await self.registry.update_metrics(tool_call_id, tool_name)
            logger.debug(
                "Counted tool call for background task",
                tool_call_id=tool_call_id,
                tool_name=tool_name,
            )

        # Execute the tool call
        result = await handler(request)

        # Capture tool_call_result for post-interrupt persistence
        tool_call_id = current_background_tool_call_id.get()
        if tool_call_id and self.registry:
            try:
                agent_id = self._get_agent_id(tool_call_id)
                if isinstance(result, ToolMessage):
                    await self.registry.append_captured_event(
                        tool_call_id,
                        {
                            "event": "tool_call_result",
                            "data": _tool_message_to_event_data(result, agent_id),
                            "ts": time.time(),
                        },
                    )
                elif isinstance(result, Command):
                    msgs = (result.update or {}).get("messages", [])
                    for msg in msgs:
                        if isinstance(msg, ToolMessage):
                            await self.registry.append_captured_event(
                                tool_call_id,
                                {
                                    "event": "tool_call_result",
                                    "data": _tool_message_to_event_data(msg, agent_id),
                                    "ts": time.time(),
                                },
                            )
            except Exception as e:
                # Never break the agent for capture failures, but leave a
                # breadcrumb so capture regressions are debuggable.
                logger.debug(
                    "Failed to capture tool_call_result event",
                    tool_call_id=tool_call_id,
                    error=str(e),
                )

        return result
