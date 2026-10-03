"""Execute code tool for running Python code in the PTC sandbox."""

from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.tools.code_admission import code_run_slot, max_execution_time_of
from ptc_agent.agent.tools.mount_guard import run_guarded
from ptc_agent.core.paths import (
    MEMO_USER_DIR,
    MEMORY_USER_DIR,
    WorkspaceLayout,
)
from ptc_agent.core.sandbox import livefs_mount

logger = structlog.get_logger(__name__)

# Same guard as bash: without the file mount, sandbox Python cannot reach the
# store-backed memory or user-managed memo store. Memo paths are additionally
# read-only to the agent.
_MEMORY_ROUTE_ERROR = (
    f"ERROR: Store-backed paths ({MEMORY_USER_DIR}/**, "
    f"{WorkspaceLayout.MEMORY_DIR}/**, "
    f"{MEMO_USER_DIR}/**) are managed by the long-term memory/memo system and "
    "are NOT on the sandbox filesystem. Read them with the Read tool before this "
    "call and pass the content in as a string; write memory paths with "
    "Write/Edit. Memo paths are read-only — ask the user to upload via the memo "
    "panel. ExecuteCode cannot persist to memory or memo."
)


# Files the server keeps, which the sandbox sees only while the file mount
# serves them.
_FILES_ROUTE_ERROR = (
    "ERROR: {dir}/** is kept on the server, not on the sandbox filesystem, and no "
    "file mount serves it to ExecuteCode right now. Read a file with the Read tool and "
    "pass the content in if your code needs it; change them with Edit and Write."
)


# A directory only the file tools reach, mounted or not.
_FILE_TOOLS_ONLY_ERROR = (
    "ERROR: {dir}/ is reachable only through the file tools. Read a file with the "
    "Read tool and pass the content in if your code needs it; change them with Edit "
    "and Write."
)


def create_execute_code_tool(
    backend: SandboxBackend,
    mcp_registry: Any,
    thread_id: str = "",
    *,
    session: Any = None,
    call_context: livefs_mount.CallContext | None = None,
    file_tools_only: tuple[str, ...] = (),
) -> BaseTool:
    """Factory function to create execute_code tool with injected dependencies.

    Args:
        backend: SandboxBackend wrapping the sandbox
        mcp_registry: MCPRegistry instance with available MCP tools
        thread_id: Short thread ID (first 8 chars) for thread-scoped code storage
        session: The machine's session, read at call time for its ``computer_id``
            and ``resource_tier`` to size per-computer admission. Omitted by
            callers with no notion of a computer, which skips admission.
        call_context: Who the code runs for, which a save through the file
            mount reads its defaults from
        file_tools_only: Directories under the sandbox root only the file
            tools reach; a program naming one is refused before it runs

    Returns:
        Configured execute_code tool function
    """

    @tool("ExecuteCode", response_format="content_and_artifact")
    async def execute_code(
        code: str,
        description: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Execute Python code directly.

        Use for: disposable one-shots — quick MCP calls, small transforms, sanity checks.
        Do not use for iterative or reusable code - write to a file and run via Bash instead.
        Import MCP tools: from tools.{server} import {tool}

        Args:
            code: Python code to execute. Print a summary to stdout. It runs in
                your workspace folder, so use RELATIVE paths (<task>/,
                data/); a leading slash is the real filesystem root,
                where none of those exist.
            description: Brief description (5-10 words, active voice)

        Returns:
            SUCCESS with stdout, or ERROR with stderr.
        """
        if not backend:
            return "ERROR: Sandbox not initialized", {"mcp_trace": []}

        return await run_guarded(
            backend,
            code,
            call_context,
            lambda call_id: _run(code, call_id),
            memory_error=_MEMORY_ROUTE_ERROR,
            files_error=_FILES_ROUTE_ERROR,
            file_tools_only=file_tools_only,
            file_tools_error=_FILE_TOOLS_ONLY_ERROR,
            blocked_event="Blocked execute_code referencing a store-backed path",
            code_length=len(code),
        )

    async def _run(code: str, call_id: str | None) -> tuple[str, dict[str, Any]]:
        try:
            logger.info("Executing code in sandbox", code_length=len(code), thread_id=thread_id)

            # Execute code in sandbox (thread_id from closure for thread-scoped storage).
            # The machine's tier bounds how many of these run on it at once; the
            # session is read here rather than closed over because a long turn's
            # session can be rebound between agent build and call.
            async with code_run_slot(
                getattr(session, "computer_id", None),
                tier=getattr(session, "resource_tier", None),
                max_execution_time=max_execution_time_of(backend),
            ):
                result = await backend.aexecute_code(
                    code, thread_id=thread_id or None, call_id=call_id
                )

            mcp_trace = list(getattr(result, "mcp_trace", []) or [])
            artifact = {"mcp_trace": mcp_trace}

            if result.success:
                # Format success response
                parts = ["SUCCESS"]

                if result.stdout:
                    parts.append(result.stdout)

                response = "\n".join(parts)
                logger.info(
                    "Code executed successfully",
                    stdout_length=len(result.stdout),
                )
                return response, artifact
            # Format error response
            # Python tracebacks often go to stdout in some environments
            # Show stderr if available, otherwise show stdout
            error_output = result.stderr if result.stderr else result.stdout

            logger.warning(
                "Code execution failed",
                stderr_length=len(result.stderr),
                stdout_length=len(result.stdout),
            )

            return f"ERROR\n{error_output}", artifact

        except Exception as e:
            logger.error("Code execution exception", error=str(e), exc_info=True)
            return f"ERROR: {e!s}", {"mcp_trace": []}

    return execute_code
