"""Execute bash commands in the sandbox."""

from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

logger = structlog.get_logger(__name__)


def create_execute_bash_tool(sandbox: Any, thread_id: str = "") -> BaseTool:
    """Factory function to create Bash tool with injected dependencies.

    Args:
        sandbox: PTCSandbox instance for bash command execution
        thread_id: Short thread ID (first 8 chars) for thread-scoped script storage

    Returns:
        Configured Bash tool function
    """

    # Resolve the default working directory from sandbox config at tool creation time
    _default_working_dir = sandbox.config.filesystem.working_directory

    @tool
    async def Bash(
        command: str,
        description: str | None = None,
        timeout: int | None = 120000,
        run_in_background: bool | None = False,
        working_dir: str | None = _default_working_dir,
    ) -> str:
        """Execute bash commands in a persistent shell session.

        Use for: git, npm, docker, system commands, directory operations
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
            result = await sandbox.execute_bash_command(
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
