"""Execute bash commands in the sandbox."""

import structlog
from langchain_core.tools import BaseTool, tool

from ptc_agent.agent.backends.sandbox import SandboxBackend

logger = structlog.get_logger(__name__)

# Bash runs in the sandbox FS; memory lives in the store. Refuse memory paths
# so the agent routes them through the file tools instead of silently dropping
# writes on the sandbox.
_MEMORY_PATH_MARKERS: tuple[str, ...] = (
    ".agents/user/memory/",
    ".agents/workspace/memory/",
)

_MEMORY_ROUTE_ERROR = (
    "ERROR: Memory paths (.agents/user/memory/**, .agents/workspace/memory/**) "
    "are managed by the store-backed long-term memory system and are NOT on the "
    "workspace filesystem. Use the Write, Edit, Read, Glob, or Grep file tools "
    "for these paths so they route to the memory store. Bash will not see or "
    "persist memory content."
)


def _command_touches_memory(command: str) -> bool:
    return any(marker in command for marker in _MEMORY_PATH_MARKERS)


def create_execute_bash_tool(backend: SandboxBackend, thread_id: str = "") -> BaseTool:
    """Factory function to create Bash tool with injected dependencies.

    Args:
        backend: SandboxBackend wrapping the sandbox
        thread_id: Short thread ID (first 8 chars) for thread-scoped script storage

    Returns:
        Configured Bash tool function
    """

    # Resolve the default working directory from sandbox config at tool creation time
    _default_working_dir = backend.filesystem_config.working_directory

    @tool
    async def Bash(
        command: str,
        description: str | None = None,
        timeout: int | None = 120000,
        run_in_background: bool | None = False,
        working_dir: str | None = _default_working_dir,
    ) -> str:
        """Execute bash commands in a persistent shell session.

        Use for: git, npm, docker, system commands, directory operations,
        and running Python scripts written to files.
        NOT for: reading/writing/editing files - use Read/Write/Edit tools instead

        Args:
            command: The bash command to execute
            description: Brief description (5-10 words, active voice)
            timeout: Milliseconds (default: 120000, max: 600000)
            run_in_background: Run asynchronously (default: False)
            working_dir: Working directory (default: /home/workspace)

        Returns:
            Command output (stdout/stderr), or ERROR message

        Paths: Quote paths with spaces. Use /home/workspace/ for workspace files.
        """
        # Bash cannot reach the store-backed memory tier.
        if _command_touches_memory(command):
            logger.info(
                "Blocked bash command touching memory path",
                command=command[:100],
            )
            return _MEMORY_ROUTE_ERROR

        try:
            logger.debug(
                "Executing bash command",
                command=command[:100],
                working_dir=working_dir,
                timeout=timeout,
                background=run_in_background,
                thread_id=thread_id or None,
            )

            # Convert timeout from milliseconds to seconds for sandbox (int required)
            timeout_seconds = int(timeout / 1000) if timeout else 120

            # Execute bash command in sandbox
            result = await backend.aexecute_bash(
                command,
                working_dir=working_dir,
                timeout=timeout_seconds,
                background=run_in_background,
                thread_id=thread_id or None,
            )

            if result["success"]:
                stdout = result.get("stdout", "")
                stderr = result.get("stderr", "")

                # Combine stdout and stderr for complete output
                output = stdout
                if stderr:
                    output += f"\n{stderr}" if output else stderr

                if output:
                    logger.debug(
                        "Bash command executed successfully",
                        command=command[:50],
                        output_length=len(output),
                    )
                    return output
                # Command succeeded but no output (e.g., mkdir)
                logger.debug(
                    "Bash command executed successfully (no output)",
                    command=command[:50],
                )
                return "Command completed successfully"

            # Command failed — Daytona returns combined stdout+stderr in "stdout"
            stdout = result.get("stdout", "")
            stderr = result.get("stderr", "")
            error_output = stderr or stdout or "Command execution failed (no output)"
            exit_code = result.get("exit_code", -1)

            logger.warning(
                "Bash command failed",
                command=command[:50],
                exit_code=exit_code,
                output_length=len(error_output),
            )

            return f"ERROR: Command failed (exit code {exit_code})\n{error_output}"

        except Exception as e:
            error_msg = f"Failed to execute bash command: {e!s}"
            logger.error(
                error_msg,
                command=command[:50],
                error=str(e),
                exc_info=True,
            )
            return f"ERROR: {error_msg}"

    # Patch the LLM-visible description with the actual configured working directory
    if _default_working_dir != "/home/workspace":
        Bash.description = Bash.description.replace("/home/workspace", _default_working_dir)

    return Bash
