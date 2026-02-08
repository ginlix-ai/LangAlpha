"""Middleware for evicting large tool results to filesystem.

This middleware intercepts tool call results and evicts them to the filesystem
when they exceed a token threshold, preventing context window overflow.
"""

from collections.abc import Awaitable, Callable

from langchain.agents.middleware.types import AgentMiddleware
from langchain.tools import ToolRuntime
from langchain.tools.tool_node import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deepagents.backends.protocol import BackendProtocol
from deepagents.backends.utils import (
    format_content_with_line_numbers,
    sanitize_tool_call_id,
)

# Approximate number of characters per token for truncation calculations.
# Using 4 chars per token as a conservative approximation (actual ratio varies by content)
NUM_CHARS_PER_TOKEN = 4

# Tools excluded from eviction (PascalCase versions of the original snake_case tools)
# These either have built-in truncation or are problematic to evict
TOOLS_EXCLUDED_FROM_EVICTION = (
    "Glob",  # Has built-in truncation
    "Grep",  # Has built-in truncation
    "Read",  # Problematic truncation behavior (single long lines)
    "Write",  # Returns minimal confirmation
    "Edit",  # Returns minimal confirmation
)

# Message template for evicted tool results
TOO_LARGE_TOOL_MSG = """Tool result too large, the result of this tool call {tool_call_id} was saved in the filesystem at this path: {file_path}
You can read the result from the filesystem by using the Read tool, but make sure to only read part of the result at a time.
You can do this by specifying an offset and limit in the Read tool call.
For example, to read the first 100 lines, you can use the Read tool with offset=0 and limit=100.

Here is a preview showing the head and tail of the result (lines of the form
... [N lines truncated] ...
indicate omitted lines in the middle of the content):

{content_sample}
"""


def _create_content_preview(
    content_str: str, *, head_lines: int = 5, tail_lines: int = 5
) -> str:
    """Create a preview of content showing head and tail with truncation marker.

    Args:
        content_str: The full content string to preview.
        head_lines: Number of lines to show from the start.
        tail_lines: Number of lines to show from the end.

    Returns:
        Formatted preview string with line numbers.
    """
    lines = content_str.splitlines()

    if len(lines) <= head_lines + tail_lines:
        # If file is small enough, show all lines
        preview_lines = [line[:1000] for line in lines]
        return format_content_with_line_numbers(preview_lines, start_line=1)

    # Show head and tail with truncation marker
    head = [line[:1000] for line in lines[:head_lines]]
    tail = [line[:1000] for line in lines[-tail_lines:]]

    head_sample = format_content_with_line_numbers(head, start_line=1)
    truncation_notice = (
        f"\n... [{len(lines) - head_lines - tail_lines} lines truncated] ...\n"
    )
    tail_sample = format_content_with_line_numbers(
        tail, start_line=len(lines) - tail_lines + 1
    )

    return head_sample + truncation_notice + tail_sample


class LargeResultEvictionMiddleware(AgentMiddleware):
    """Middleware for evicting large tool results to filesystem.

    This middleware intercepts tool call results and evicts them to the filesystem
    when they exceed a token threshold, preventing context window overflow.

    The evicted content is written to /large_tool_results/{tool_call_id} and the
    original message is replaced with a truncated preview plus file reference.

    Args:
        backend: Backend for file storage (must implement BackendProtocol).
        tool_token_limit_before_evict: Token limit before evicting a tool result.
            Default is 20000 tokens (~80000 characters).
    """

    def __init__(
        self,
        *,
        backend: BackendProtocol,
        tool_token_limit_before_evict: int = 20000,
    ) -> None:
        """Initialize the large result eviction middleware.

        Args:
            backend: Backend for file storage.
            tool_token_limit_before_evict: Token limit before evicting results.
        """
        self.backend = backend
        self._tool_token_limit_before_evict = tool_token_limit_before_evict

    def _process_large_message(
        self,
        message: ToolMessage,
    ) -> tuple[ToolMessage, dict | None]:
        """Process a large ToolMessage by evicting its content to filesystem.

        Args:
            message: The ToolMessage with large content to evict.

        Returns:
            A tuple of (processed_message, files_update):
            - processed_message: New ToolMessage with truncated content and file reference
            - files_update: Dict of file updates to apply to state, or None if eviction failed
        """
        # Early exit if eviction not configured
        if not self._tool_token_limit_before_evict:
            return message, None

        # Convert content to string once for both size check and eviction
        # Special case: single text block - extract text directly for readability
        if (
            isinstance(message.content, list)
            and len(message.content) == 1
            and isinstance(message.content[0], dict)
            and message.content[0].get("type") == "text"
            and "text" in message.content[0]
        ):
            content_str = str(message.content[0]["text"])
        elif isinstance(message.content, str):
            content_str = message.content
        else:
            # Multiple blocks or non-text content - stringify entire structure
            content_str = str(message.content)

        # Check if content exceeds eviction threshold
        if (
            len(content_str)
            <= NUM_CHARS_PER_TOKEN * self._tool_token_limit_before_evict
        ):
            return message, None

        # Write content to filesystem
        sanitized_id = sanitize_tool_call_id(message.tool_call_id)
        file_path = f"/large_tool_results/{sanitized_id}"
        result = self.backend.write(file_path, content_str)
        if result.error:
            return message, None

        # Create preview showing head and tail of the result
        content_sample = _create_content_preview(content_str)
        replacement_text = TOO_LARGE_TOOL_MSG.format(
            tool_call_id=message.tool_call_id,
            file_path=file_path,
            content_sample=content_sample,
        )

        # Always return as plain string after eviction
        processed_message = ToolMessage(
            content=replacement_text,
            tool_call_id=message.tool_call_id,
            name=message.name,
        )
        return processed_message, result.files_update

    async def _aprocess_large_message(
        self,
        message: ToolMessage,
    ) -> tuple[ToolMessage, dict | None]:
        """Async version of _process_large_message.

        Uses async backend methods to avoid sync calls in async context.
        """
        # Early exit if eviction not configured
        if not self._tool_token_limit_before_evict:
            return message, None

        # Convert content to string once for both size check and eviction
        if (
            isinstance(message.content, list)
            and len(message.content) == 1
            and isinstance(message.content[0], dict)
            and message.content[0].get("type") == "text"
            and "text" in message.content[0]
        ):
            content_str = str(message.content[0]["text"])
        elif isinstance(message.content, str):
            content_str = message.content
        else:
            content_str = str(message.content)

        if (
            len(content_str)
            <= NUM_CHARS_PER_TOKEN * self._tool_token_limit_before_evict
        ):
            return message, None

        # Write content to filesystem using async method
        sanitized_id = sanitize_tool_call_id(message.tool_call_id)
        file_path = f"/large_tool_results/{sanitized_id}"
        result = await self.backend.awrite(file_path, content_str)
        if result.error:
            return message, None

        # Create preview showing head and tail of the result
        content_sample = _create_content_preview(content_str)
        replacement_text = TOO_LARGE_TOOL_MSG.format(
            tool_call_id=message.tool_call_id,
            file_path=file_path,
            content_sample=content_sample,
        )

        processed_message = ToolMessage(
            content=replacement_text,
            tool_call_id=message.tool_call_id,
            name=message.name,
        )
        return processed_message, result.files_update

    def _intercept_large_tool_result(
        self, tool_result: ToolMessage | Command, runtime: ToolRuntime
    ) -> ToolMessage | Command:
        """Intercept and process large tool results before they're added to state.

        Args:
            tool_result: The tool result to potentially evict (ToolMessage or Command).
            runtime: The tool runtime (unused, kept for API consistency).

        Returns:
            Either the original result (if small enough) or a Command with evicted
            content written to filesystem and truncated message.
        """
        if isinstance(tool_result, ToolMessage):
            processed_message, files_update = self._process_large_message(tool_result)
            return (
                Command(
                    update={
                        "files": files_update,
                        "messages": [processed_message],
                    }
                )
                if files_update is not None
                else processed_message
            )

        if isinstance(tool_result, Command):
            update = tool_result.update
            if update is None:
                return tool_result
            command_messages = update.get("messages", [])
            accumulated_file_updates = dict(update.get("files", {}))
            processed_messages = []
            for message in command_messages:
                if not isinstance(message, ToolMessage):
                    processed_messages.append(message)
                    continue

                processed_message, files_update = self._process_large_message(message)
                processed_messages.append(processed_message)
                if files_update is not None:
                    accumulated_file_updates.update(files_update)
            return Command(
                update={
                    **update,
                    "messages": processed_messages,
                    "files": accumulated_file_updates,
                }
            )
        raise AssertionError(
            f"Unreachable code in _intercept_large_tool_result: tool_result type {type(tool_result)}"
        )

    async def _aintercept_large_tool_result(
        self, tool_result: ToolMessage | Command, runtime: ToolRuntime
    ) -> ToolMessage | Command:
        """Async version of _intercept_large_tool_result."""
        if isinstance(tool_result, ToolMessage):
            processed_message, files_update = await self._aprocess_large_message(
                tool_result
            )
            return (
                Command(
                    update={
                        "files": files_update,
                        "messages": [processed_message],
                    }
                )
                if files_update is not None
                else processed_message
            )

        if isinstance(tool_result, Command):
            update = tool_result.update
            if update is None:
                return tool_result
            command_messages = update.get("messages", [])
            accumulated_file_updates = dict(update.get("files", {}))
            processed_messages = []
            for message in command_messages:
                if not isinstance(message, ToolMessage):
                    processed_messages.append(message)
                    continue

                processed_message, files_update = await self._aprocess_large_message(
                    message
                )
                processed_messages.append(processed_message)
                if files_update is not None:
                    accumulated_file_updates.update(files_update)
            return Command(
                update={
                    **update,
                    "messages": processed_messages,
                    "files": accumulated_file_updates,
                }
            )
        raise AssertionError(
            f"Unreachable code in _aintercept_large_tool_result: tool_result type {type(tool_result)}"
        )

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        """Check the size of the tool call result and evict to filesystem if too large.

        Args:
            request: The tool call request being processed.
            handler: The handler function to call with the request.

        Returns:
            The raw ToolMessage, or a Command with evicted content.
        """
        if (
            self._tool_token_limit_before_evict is None
            or request.tool_call["name"] in TOOLS_EXCLUDED_FROM_EVICTION
        ):
            return handler(request)

        tool_result = handler(request)
        return self._intercept_large_tool_result(tool_result, request.runtime)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        """Async version: check size and evict to filesystem if too large.

        Args:
            request: The tool call request being processed.
            handler: The async handler function to call with the request.

        Returns:
            The raw ToolMessage, or a Command with evicted content.
        """
        if (
            self._tool_token_limit_before_evict is None
            or request.tool_call["name"] in TOOLS_EXCLUDED_FROM_EVICTION
        ):
            return await handler(request)

        tool_result = await handler(request)
        return await self._aintercept_large_tool_result(tool_result, request.runtime)
