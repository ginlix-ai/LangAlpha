"""Execute code tool for running Python code in the PTC sandbox."""

from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

from ptc_agent.agent.backends.sandbox import SandboxBackend

logger = structlog.get_logger(__name__)

# Same guard as bash — sandbox Python cannot reach the store-backed memory.
_MEMORY_PATH_MARKERS: tuple[str, ...] = (
    ".agents/user/memory/",
    ".agents/workspace/memory/",
)

_MEMORY_ROUTE_ERROR = (
    "ERROR: Memory paths (.agents/user/memory/**, .agents/workspace/memory/**) "
    "are managed by the store-backed long-term memory system and are NOT on "
    "the sandbox filesystem. Read them with the Read tool before this call and "
    "pass the content in as a string; write them with Write/Edit. ExecuteCode "
    "cannot persist to memory."
)


def _code_touches_memory(code: str) -> bool:
    return any(marker in code for marker in _MEMORY_PATH_MARKERS)


def create_execute_code_tool(backend: SandboxBackend, mcp_registry: Any, thread_id: str = "") -> BaseTool:
    """Factory function to create execute_code tool with injected dependencies.

    Args:
        backend: SandboxBackend wrapping the sandbox
        mcp_registry: MCPRegistry instance with available MCP tools
        thread_id: Short thread ID (first 8 chars) for thread-scoped code storage

    Returns:
        Configured execute_code tool function
    """

    @tool("ExecuteCode")
    async def execute_code(
        code: str,
        description: str | None = None,
    ) -> str:
        """Execute Python code directly.

        Use for: disposable one-shots — quick MCP calls, small transforms, sanity checks.
        Do not use for iterative or reusable code - write to a file and run via Bash instead.
        Import MCP tools: from tools.{server} import {tool}

        Args:
            code: Python code to execute. Print summary to stdout.
            description: Brief description (5-10 words, active voice)

        Returns:
            SUCCESS with stdout/files, or ERROR with stderr

        Paths: Use RELATIVE paths (results/, data/). Never /results/ or /workspace/.
        """
        if not backend:
            return "ERROR: Sandbox not initialized"

        if _code_touches_memory(code):
            logger.info(
                "Blocked execute_code referencing memory path",
                code_length=len(code),
            )
            return _MEMORY_ROUTE_ERROR

        try:
            logger.info("Executing code in sandbox", code_length=len(code), thread_id=thread_id)

            # Execute code in sandbox (thread_id from closure for thread-scoped storage)
            result = await backend.aexecute_code(code, thread_id=thread_id or None)

            if result.success:
                # Format success response
                parts = ["SUCCESS"]

                if result.stdout:
                    parts.append(result.stdout)

                if result.files_created:
                    # Extract file names from file objects
                    files = [
                        f.name if hasattr(f, "name") else str(f)
                        for f in result.files_created
                    ]
                    if files:
                        parts.append(f"Files created: {', '.join(files)}")

                response = "\n".join(parts)
                logger.info(
                    "Code executed successfully",
                    stdout_length=len(result.stdout),
                )
                return response
            # Format error response
            # Python tracebacks often go to stdout in some environments
            # Show stderr if available, otherwise show stdout
            error_output = result.stderr if result.stderr else result.stdout

            logger.warning(
                "Code execution failed",
                stderr_length=len(result.stderr),
                stdout_length=len(result.stdout),
            )

            return f"ERROR\n{error_output}"

        except Exception as e:
            logger.error("Code execution exception", error=str(e), exc_info=True)
            return f"ERROR: {e!s}"

    return execute_code
