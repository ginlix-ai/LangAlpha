"""
TodoWrite Middleware for Real-Time SSE Event Emission.

This middleware intercepts TodoWrite tool calls to emit todo_update events
using LangGraph's custom event streaming API.

Architecture:
- Uses get_stream_writer() to emit custom events after tool execution
- Emits single "completed" event with full todo list for frontend display
- Avoids state pollution - events go directly to stream, not agent context

Event Structure (ordered fields):
1. artifact_type - "todo_update"
2. artifact_id - Tool call identifier
3. agent - Agent name (hardcoded to "ptc")
4. timestamp - ISO format timestamp
5. status - "completed" or "failed"
6. payload - Contains todos array and status counts
"""

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from langchain.agents.middleware import AgentMiddleware
from langgraph.config import get_stream_writer

logger = logging.getLogger(__name__)


class TodoWriteMiddleware(AgentMiddleware):
    """
    Middleware that emits todo_update SSE events after TodoWrite tool execution.

    Hooks into tool execution to emit custom events with full todo list data,
    enabling frontend/CLI to display real-time todo updates without polluting
    agent context.
    """

    # Tool to monitor for todo operations
    MONITORED_TOOL = "TodoWrite"

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """
        Intercept tool calls and emit todo_update events after execution.

        Emits a single event per operation with full todo list for frontend display.
        Event field order: artifact_type, artifact_id, agent, timestamp, status, payload.

        Args:
            request: Tool call request with tool_call dict containing name, args, id
            handler: Next handler in chain (actual tool execution)

        Returns:
            Tool execution result
        """
        tool_call = request.tool_call
        tool_name = tool_call.get("name")

        # Only process TodoWrite tool calls
        if tool_name != self.MONITORED_TOOL:
            return await handler(request)

        tool_call_id = tool_call.get("id", "unknown")
        tool_args = tool_call.get("args", {})
        todos = tool_args.get("todos", [])

        logger.debug(f"[TODO_MIDDLEWARE] Intercepting {tool_name} (id: {tool_call_id})")

        # Hardcode agent name for now (PTCAgent is the main agent)
        agent_name = "ptc"

        # Get stream writer for custom event emission
        try:
            writer = get_stream_writer()
        except Exception as e:
            logger.error(f"[TODO_MIDDLEWARE] Failed to get stream writer: {e}")
            # Continue with tool execution even if streaming fails
            return await handler(request)

        # Execute the actual tool
        try:
            result = await handler(request)

            # Restrict payload to the TodoItem schema: strip any legacy
            # or unknown fields the LLM may still send, and lowercase status
            # so the frontend's strict equality (status === 'pending') holds
            # regardless of case variance. Anything non-dict is dropped.
            status_counts = {"pending": 0, "in_progress": 0, "completed": 0}
            normalized_todos = []
            for todo in todos:
                if not isinstance(todo, dict):
                    continue
                status = str(todo.get("status", "")).lower()
                if status in status_counts:
                    status_counts[status] += 1
                normalized_todos.append({
                    "content": todo.get("content", ""),
                    "activeForm": todo.get("activeForm", ""),
                    "status": status,
                })

            # Build completed event with structure expected by streaming_handler
            # Must include artifact_type for handler to recognize and emit as SSE artifact event
            timestamp = datetime.now(timezone.utc).isoformat()

            # Build payload with todo data
            payload: dict[str, Any] = {
                "todos": normalized_todos,
                "total": len(normalized_todos),
                "completed": status_counts["completed"],
                "in_progress": status_counts["in_progress"],
                "pending": status_counts["pending"],
            }

            # Structure matches streaming_handler expectations (artifact_type triggers artifact SSE event)
            completed_event = {
                "artifact_type": "todo_update",  # Required for streaming_handler recognition
                "artifact_id": tool_call_id,
                "agent": agent_name,
                "timestamp": timestamp,
                "status": "completed",
                "payload": payload,
            }

            try:
                writer(completed_event)
                logger.debug(
                    f"[TODO_MIDDLEWARE] ✓ Emitted completed event: {len(todos)} todos "
                    f"(completed: {status_counts['completed']}, "
                    f"in_progress: {status_counts['in_progress']}, "
                    f"pending: {status_counts['pending']})"
                )
            except Exception as e:
                logger.error(f"[TODO_MIDDLEWARE] Failed to emit completed event: {e}")

            return result

        except Exception as e:
            # Emit "failed" event on error
            logger.error(f"[TODO_MIDDLEWARE] Tool execution failed: {e}")

            failed_timestamp = datetime.now(timezone.utc).isoformat()
            failed_event = {
                "artifact_type": "todo_update",  # Required for streaming_handler recognition
                "artifact_id": tool_call_id,
                "agent": agent_name,
                "timestamp": failed_timestamp,
                "status": "failed",
                "payload": {
                    "todos": todos,
                    "total": len(todos),
                    "error": str(e),
                },
            }

            try:
                writer(failed_event)
                logger.debug("[TODO_MIDDLEWARE] ✓ Emitted failed event")
            except Exception as emit_error:
                logger.error(f"[TODO_MIDDLEWARE] Failed to emit failed event: {emit_error}")

            # Re-raise to preserve error handling
            raise
