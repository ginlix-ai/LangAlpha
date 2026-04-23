"""
File Operation Middleware for Real-Time SSE Event Emission.

This middleware intercepts Write and Edit tool calls to emit
file_operation events using LangGraph's custom event streaming API.

Architecture:
- Uses get_stream_writer() to emit custom events after tool execution
- Emits single "completed" event with full file content for frontend display
- Avoids state pollution - events go directly to stream, not agent context
- Works around limitation that tool Command updates don't reach stream_mode="updates"

Event Structure (ordered fields):
1. agent - Agent name
2. operation - "Write" or "Edit"
3. file_path - Full path to the file
4. tool_call_id - Tool call identifier
5. timestamp - ISO format timestamp
6. status - "completed" or "failed"
7. line_count - Number of lines in content
8. content - Full file content (Write only)
9. old_string - Original content (Edit only)
10. new_string - Replacement content (Edit only)
"""

import logging
from datetime import datetime, timezone
from typing import Annotated, Any, Awaitable, Callable

from langchain.agents.middleware import AgentMiddleware, AgentState
from typing_extensions import NotRequired
from langgraph.config import get_stream_writer
from ptc_agent.utils.file_operations import _file_operations_log_reducer

logger = logging.getLogger(__name__)


class FileOperationState(AgentState):
    """State schema for file operation middleware.

    Declares current_agent field so it propagates to request.state
    during tool execution, enabling agent name extraction in middleware.
    """
    current_agent: NotRequired[str] = "unknown"
    file_operations_log: Annotated[NotRequired[list[dict[str, Any]]], _file_operations_log_reducer]
    """Persistent audit trail of all file operations during workflow execution."""


class FileOperationMiddleware(AgentMiddleware):
    """
    Middleware that emits file_operation SSE events after tool execution.

    Hooks into tool execution to emit custom events with full file content
    for write_file and edit_file operations, enabling frontend to display
    file changes without polluting agent context.
    """

    # State schema declaration - makes current_agent accessible in request.state
    state_schema = FileOperationState

    # Tools to monitor for file operations
    # Tool names match @tool("Write") and @tool("Edit") decorators in file_ops.py
    MONITORED_TOOLS = {"Write", "Edit"}

    def __init__(
        self,
        on_agent_md_write: Callable[[], None] | None = None,
        work_dir: str = "/home/workspace",
    ) -> None:
        super().__init__()
        self._on_agent_md_write = on_agent_md_write
        self._work_dir_prefix = work_dir.rstrip("/") + "/"

    @staticmethod
    def _count_lines(text: str) -> int:
        """Count number of lines in text."""
        if not text:
            return 0
        return text.count('\n') + (1 if text and not text.endswith('\n') else 0)

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """
        Intercept tool calls and emit file operation events after execution.

        Emits a single event per operation with full content for frontend display.
        Event field order: agent, operation, file_path, tool_call_id, timestamp,
        status, line_count, content/old_string/new_string.

        Args:
            request: Tool call request with tool_call dict containing name, args, id
            handler: Next handler in chain (actual tool execution)

        Returns:
            Tool execution result
        """
        tool_call = request.tool_call
        tool_name = tool_call.get("name")

        # Only process file operation tools
        if tool_name not in self.MONITORED_TOOLS:
            return await handler(request)

        tool_call_id = tool_call.get("id", "unknown")
        tool_args = tool_call.get("args", {})
        file_path = tool_args.get("file_path", "unknown")

        logger.debug(f"[FILE_OP_MIDDLEWARE] Intercepting {tool_name} (id: {tool_call_id})")

        # Hardcode agent name for now (PTCAgent is the main agent)
        agent_name = "ptc"

        # Get stream writer for custom event emission
        try:
            writer = get_stream_writer()
        except Exception as e:
            logger.error(f"[FILE_OP_MIDDLEWARE] Failed to get stream writer: {e}")
            # Continue with tool execution even if streaming fails
            return await handler(request)

        # Execute the actual tool
        try:
            result = await handler(request)

            # removeprefix (not lstrip) — lstrip eats a leading "." from paths
            # like ".agents/user/memory/memory.md".
            normalized = file_path or ""
            if normalized.startswith(self._work_dir_prefix):
                normalized = normalized[len(self._work_dir_prefix):]
            normalized = normalized.removeprefix("./")

            # Invalidate agent.md cache when agent.md is written or edited
            if self._on_agent_md_write and normalized == "agent.md":
                try:
                    self._on_agent_md_write()
                except Exception:
                    logger.debug("[FILE_OP_MIDDLEWARE] Failed to invalidate agent.md cache")

            # Build completed event with structure expected by streaming_handler
            # Must include artifact_type for handler to recognize and emit as SSE artifact event
            timestamp = datetime.now(timezone.utc).isoformat()

            # Build payload with operation-specific content
            payload: dict[str, Any] = {
                "operation": tool_name,
                "file_path": file_path,
            }

            if tool_name == "Write":
                content = tool_args.get("content", "")
                payload["line_count"] = self._count_lines(content)
                payload["content"] = content

            elif tool_name == "Edit":
                old_string = tool_args.get("old_string", "")
                new_string = tool_args.get("new_string", "")
                payload["line_count"] = self._count_lines(new_string)
                payload["old_string"] = old_string
                payload["new_string"] = new_string

            # Structure matches streaming_handler expectations (artifact_type triggers artifact SSE event)
            completed_event = {
                "artifact_type": "file_operation",  # Required for streaming_handler recognition
                "artifact_id": tool_call_id,
                "agent": agent_name,
                "timestamp": timestamp,
                "status": "completed",
                "payload": payload,
            }

            try:
                writer(completed_event)
                logger.debug(
                    f"[FILE_OP_MIDDLEWARE] ✓ Emitted completed event: {file_path} "
                    f"({payload.get('line_count', 0)} lines)"
                )
            except Exception as e:
                logger.error(f"[FILE_OP_MIDDLEWARE] Failed to emit completed event: {e}")

            return result

        except Exception as e:
            # Emit "failed" event on error
            logger.error(f"[FILE_OP_MIDDLEWARE] Tool execution failed: {e}")

            failed_timestamp = datetime.now(timezone.utc).isoformat()
            failed_event = {
                "artifact_type": "file_operation",  # Required for streaming_handler recognition
                "artifact_id": tool_call_id,
                "agent": agent_name,
                "timestamp": failed_timestamp,
                "status": "failed",
                "payload": {
                    "operation": tool_name,
                    "file_path": file_path,
                    "error": str(e),
                },
            }

            try:
                writer(failed_event)
                logger.debug(f"[FILE_OP_MIDDLEWARE] ✓ Emitted failed event: {file_path}")
            except Exception as emit_error:
                logger.error(f"[FILE_OP_MIDDLEWARE] Failed to emit failed event: {emit_error}")

            # Re-raise to preserve error handling
            raise
