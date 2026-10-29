"""Subagent event capture middleware.

Injected into every subagent stack, as two middleware at two depths:

- ``SubagentEventCaptureMiddleware``, innermost, captures the model's tool
  calls and reports tool-call metrics (``total_tool_calls``,
  ``tool_call_counts``, ``current_tool``) to the ``BackgroundTaskRegistry``.
- ``SubagentToolResultCaptureMiddleware``, just outside large-result
  eviction, captures each tool result as the checkpoint keeps it.

Both route records through ``BackgroundTaskRegistry.append_captured_event``,
which spills each to the per-task Redis Stream for SSE replay.
"""

import time
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import ToolMessage
from langchain_core.runnables.config import var_child_runnable_config
from langgraph.constants import TAG_NOSTREAM
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.middleware import current_background_tool_call_id
from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTaskRegistry
from src.server.utils.content_normalizer import normalize_text_content

logger = structlog.get_logger(__name__)

# Hard cap on per-event captured content. Each captured event is XADD'd to
# the per-task Redis Stream, and a multi-MB result would inflate its payload.
# Eviction already swaps an oversized result for its pointer before capture
# sees it, so this only bites on a result eviction leaves whole (a tool it
# excludes, a failed write). It shortens only the SSE display copy.
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


def _visible_text(content: Any) -> str:
    """The text a client should see for a tool result.

    An attachment rides on the tool result as a block list, so stringifying it
    would spend the whole capture budget on base64 where the acknowledgment
    belongs. Read through the same extractor the main SSE producer uses, and
    keep ``str`` for a shape it cannot name.
    """
    if isinstance(content, str):
        return content
    text, _ = normalize_text_content(content)
    return text if text is not None else str(content)


def _tool_message_to_event_data(msg: ToolMessage, agent_id: str) -> dict:
    """Build the ``data`` payload for a captured ``tool_call_result`` event.

    Shared by the direct-ToolMessage branch and the Command-wrapped branch in
    ``awrap_tool_call``. Keeps status/artifact handling and content stringification
    in one place so the two code paths can't drift.
    """
    content = _visible_text(msg.content)
    data: dict = {
        "agent": agent_id,
        "id": getattr(msg, "id", ""),
        "role": "assistant",
        "tool_call_id": msg.tool_call_id,
        "content": _truncate_content(content),
        "content_type": "text",
    }
    # The whole status travels, not just the failing one. An explicit success
    # is what stops a client from reading prose that merely looks like a failure.
    status = getattr(msg, "status", None)
    if status:
        data["status"] = status
    if getattr(msg, "artifact", None) is not None:
        data["artifact"] = msg.artifact
    return data


def _agent_id(registry: BackgroundTaskRegistry, tool_call_id: str) -> str:
    """Resolve agent identifier in task:{task_id} format."""
    task = registry._tasks.get(tool_call_id)
    return f"task:{task.task_id}" if task else f"subagent:{tool_call_id}"


class SubagentEventCaptureMiddleware(AgentMiddleware):
    """Captures a subagent's tool calls and reports its tool-call metrics.

    Innermost in the subagent stack: the tool calls are captured from the
    response this model call returned, and a call is counted only once every
    guard outside has let it through to the tool. Its results are captured
    by ``SubagentToolResultCaptureMiddleware`` further out.

    The middleware uses a contextvar (current_background_tool_call_id) to identify
    which background task it belongs to. Contextvars properly propagate across
    await boundaries, ensuring tool calls are tracked even when subagents
    execute in different execution contexts.
    """

    def __init__(self, registry: BackgroundTaskRegistry) -> None:
        """Initialize the event capture middleware.

        Args:
            registry: The BackgroundTaskRegistry to report metrics to
        """
        super().__init__()
        self.tools = []  # No additional tools
        self.registry = registry

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """Capture the model's tool calls into the per-task event stream."""
        response = await handler(request)
        # A call kept out of the stream (a compaction summary) is not a step
        # of the task: a tool call it returns is never run.
        if TAG_NOSTREAM in ((var_child_runnable_config.get() or {}).get("tags") or ()):
            return response

        # Capture events for post-interrupt persistence
        tool_call_id = current_background_tool_call_id.get()
        if tool_call_id and self.registry:
            try:
                ai_msg = response.result[0] if response.result else None
                if ai_msg:
                    tool_calls = getattr(ai_msg, "tool_calls", None) or []
                    agent_id = _agent_id(self.registry, tool_call_id)
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
        """Count the tool call against its background task, then run it."""
        tool_name = request.tool_call.get("name", "unknown")
        tool_call_id = current_background_tool_call_id.get()
        if tool_call_id and self.registry:
            await self.registry.update_metrics(tool_call_id, tool_name)
            logger.debug(
                "Counted tool call for background task",
                tool_call_id=tool_call_id,
                tool_name=tool_name,
            )
        return await handler(request)


class SubagentToolResultCaptureMiddleware(AgentMiddleware):
    """Captures each subagent tool result as the subagent's checkpoint keeps it.

    Replay projects a subagent's tool results from its checkpoint, so the live
    copy has to be the same message or the two disagree: a large result
    streamed in full would replay as its eviction pointer. Outermost of the
    tool wrappers, it sees the message the checkpoint is written with: the
    pointer, the redacted text, the error a raised tool was turned into, the
    time-up stop's refusal. It acts only on what comes back, so the stop's
    reason to go outside every tool wrapper does not reach it. The main
    agent's stream reads the same post-middleware message, so both lanes show
    a result the same way.
    """

    def __init__(self, registry: BackgroundTaskRegistry) -> None:
        super().__init__()
        self.tools = []
        self.registry = registry

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        """Synchronous wrap_tool_call - no capture in sync mode."""
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        result = await handler(request)

        tool_call_id = current_background_tool_call_id.get()
        if tool_call_id and self.registry:
            try:
                agent_id = _agent_id(self.registry, tool_call_id)
                if isinstance(result, ToolMessage):
                    messages = [result]
                elif isinstance(result, Command):
                    messages = [
                        msg
                        for msg in (result.update or {}).get("messages", [])
                        if isinstance(msg, ToolMessage)
                    ]
                else:
                    messages = []
                for msg in messages:
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
