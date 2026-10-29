"""Tool error handling middleware.

Catches tool execution errors and returns simplified error messages.
"""
import logging

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.errors import GraphBubbleUp

logger = logging.getLogger(__name__)


def simplify_tool_error(error: Exception) -> str:
    """Simplify tool error messages by removing verbose input args.

    Args:
        error: The exception raised during tool execution

    Returns:
        Simplified error message string (max 800 chars)

    Example:
        ValidationError with missing field -> "Field 'file_path': field required"
        Generic error -> "Error message..." (truncated if too long)
    """
    # Handle Pydantic ValidationError (missing/invalid fields)
    if hasattr(error, 'errors') and callable(error.errors):
        errors = error.errors()
        if errors:
            # Extract first error details
            first_error = errors[0]
            field = first_error.get('loc', ['unknown'])[-1]  # Get last part of location tuple
            msg = first_error.get('msg', 'validation failed')
            return f"Field '{field}': {msg}"

    # Generic error - truncate if too long
    error_str = str(error)
    if len(error_str) > 800:
        return error_str[:800] + "..."
    return error_str



def format_tool_error(error: Exception, tool_name: str | None = None) -> str:
    """The one shape a failed tool call is reported in.

    Module level because the converter below is not the only place that has to
    produce it: a middleware that must build the error message itself, to keep
    something the converter would strip, still has to say it the same way.
    """
    simplified = simplify_tool_error(error)
    if tool_name:
        return f"Tool '{tool_name}' failed: {simplified}"
    return f"Tool execution failed: {simplified}"


class ToolErrorHandlingMiddleware(AgentMiddleware):
    """Middleware that handles tool execution errors with simplified messages.

    This middleware catches exceptions during tool execution and returns
    simplified error messages to the agent instead of crashing the workflow.
    This allows agents to see errors and potentially adjust their approach.

    Error simplification:
    - Pydantic ValidationErrors: Shows only field name and error message
    - Generic errors: Truncated to 800 characters max
    - Removes verbose input arguments that make errors unreadable
    """

    def wrap_tool_call(self, request, handler):
        """Synchronous tool error handler."""
        try:
            return handler(request)
        except GraphBubbleUp:
            raise  # Let LangGraph control-flow exceptions (interrupt, etc.) propagate
        except Exception as e:
            tool_name = request.tool_call.get("name", "unknown")
            error_message = format_tool_error(e, tool_name)
            return ToolMessage(
                content=error_message,
                tool_call_id=request.tool_call["id"],
                status="error"
            )

    async def awrap_tool_call(self, request, handler):
        """Asynchronous tool error handler."""
        try:
            return await handler(request)
        except GraphBubbleUp:
            raise  # Let LangGraph control-flow exceptions (interrupt, etc.) propagate
        except Exception as e:
            tool_name = request.tool_call.get("name", "unknown")
            error_message = format_tool_error(e, tool_name)
            return ToolMessage(
                content=error_message,
                tool_call_id=request.tool_call["id"],
                status="error"
            )
