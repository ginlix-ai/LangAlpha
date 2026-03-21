"""Code validation middleware — blocks ExecuteCode access to protected platform paths.

Prevents agent-generated code from reading _internal/, .mcp_tokens, or
.mcp_secrets files in the sandbox.
"""

from __future__ import annotations

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

logger = structlog.get_logger(__name__)

# Patterns that indicate attempts to access protected internal files
_INTERNAL_PATH_PATTERNS = ("_internal/", ".mcp_tokens", ".mcp_secrets", ".vault_secrets")


class CodeValidationMiddleware(AgentMiddleware):
    """Blocks ExecuteCode calls that reference protected platform paths.

    Scans code for _internal/, .mcp_tokens, .mcp_secrets references.
    Returns a system_warning so the LLM can explain the restriction
    rather than hard-failing silently.
    """

    def _check_code(self, code: str) -> str | None:
        """Return a warning message if code references protected paths, else None."""
        for pattern in _INTERNAL_PATH_PATTERNS:
            if pattern in code:
                logger.warning("Internal path reference in code", pattern=pattern)
                return (
                    "<system_warning>Code validation failed: references to internal "
                    "system paths (_internal/, .mcp_tokens, .mcp_secrets, "
                    ".vault_secrets) are not allowed. These are protected platform "
                    "files that must not be accessed directly.</system_warning>"
                )
        return None

    def wrap_tool_call(self, request, handler):
        tool_name = request.tool_call.get("name", "")
        if tool_name == "ExecuteCode":
            args = request.tool_call.get("args", {})
            code = args.get("code", "")
            warning = self._check_code(code)
            if warning:
                return ToolMessage(
                    content=warning, tool_call_id=request.tool_call["id"]
                )
        return handler(request)

    async def awrap_tool_call(self, request, handler):
        tool_name = request.tool_call.get("name", "")
        if tool_name == "ExecuteCode":
            args = request.tool_call.get("args", {})
            code = args.get("code", "")
            warning = self._check_code(code)
            if warning:
                return ToolMessage(
                    content=warning, tool_call_id=request.tool_call["id"]
                )
        return await handler(request)
