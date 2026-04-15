"""Protected path middleware — warns agent when it accesses restricted directories.

Detects tool calls that reference protected paths (from config's denied_directories)
and injects <system_warning> tags so the LLM knows to stop and explain to the user.

Two detection modes:
1. Input scanning: If tool args explicitly reference a protected path, the tool
   call is short-circuited with a system warning (tool never executes).
2. Output scanning: If tool output reveals protected paths (e.g., glob results),
   a system warning is appended to the result.
"""

from __future__ import annotations

import os

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

logger = structlog.get_logger(__name__)

_BLOCKED_RESPONSE = (
    "<system_warning>Access denied. The path you requested is inside a protected "
    "system directory. This directory contains internal system files managed by the "
    "platform. Do not attempt to access, read, modify, or disclose files in this "
    "location. Inform the user that these are protected system resources that "
    "cannot be accessed.</system_warning>"
)

_OUTPUT_WARNING = (
    "\n\n<system_warning>The output above references a protected system directory. "
    "Files in this directory contain internal system files managed by the platform. "
    "Do not attempt to read or access these files. If the user asks about them, "
    "explain that they are protected system resources.</system_warning>"
)


class ProtectedPathMiddleware(AgentMiddleware):
    """Detects and warns when agent attempts to access protected system paths.

    Reads protected paths from config's ``denied_directories`` list and derives
    short fragments for substring matching in tool args and output.

    Runs in the shared middleware stack so it applies to both the main agent
    and all subagents.
    """

    def __init__(self, denied_directories: list[str] | None = None) -> None:
        # Build match fragments from full paths, e.g.:
        #   "/home/workspace/_internal" → ["_internal/", "_internal\\", "_internal"]
        fragments: set[str] = set()
        for path in denied_directories or []:
            basename = os.path.basename(path.rstrip("/"))
            if basename:
                fragments.add(f"{basename}/")
                fragments.add(f"{basename}\\")
        self._fragments = tuple(sorted(fragments, key=len, reverse=True))

        if self._fragments:
            logger.debug(
                "ProtectedPathMiddleware initialized",
                fragments=self._fragments,
            )

    def _references_protected(self, text: str) -> bool:
        return any(frag in text for frag in self._fragments)

    def _args_reference_protected(self, args: dict) -> bool:
        for value in args.values():
            if isinstance(value, str) and self._references_protected(value):
                return True
        return False

    def _make_blocked(self, tool_call_id: str, tool_name: str) -> ToolMessage:
        logger.warning("Protected path access blocked", tool=tool_name)
        return ToolMessage(content=_BLOCKED_RESPONSE, tool_call_id=tool_call_id)

    def _maybe_warn_output(self, result: ToolMessage, tool_name: str) -> ToolMessage:
        if isinstance(result.content, str) and self._references_protected(result.content):
            logger.warning("Protected path in tool output", tool=tool_name)
            result.content += _OUTPUT_WARNING
        return result

    # -- sync --

    def wrap_tool_call(self, request, handler):
        tool_name = request.tool_call.get("name", "")
        args = request.tool_call.get("args", {})

        if self._args_reference_protected(args):
            return self._make_blocked(request.tool_call["id"], tool_name)

        result = handler(request)

        if isinstance(result, ToolMessage):
            result = self._maybe_warn_output(result, tool_name)

        return result

    # -- async --

    async def awrap_tool_call(self, request, handler):
        tool_name = request.tool_call.get("name", "")
        args = request.tool_call.get("args", {})

        if self._args_reference_protected(args):
            return self._make_blocked(request.tool_call["id"], tool_name)

        result = await handler(request)

        if isinstance(result, ToolMessage):
            result = self._maybe_warn_output(result, tool_name)

        return result
