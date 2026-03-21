"""Get output and status of background bash commands."""

from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

logger = structlog.get_logger(__name__)


def create_bash_output_tool(sandbox: Any) -> BaseTool:
    """Factory function to create BashOutput tool with injected dependencies.

    Args:
        sandbox: PTCSandbox instance for querying background command output

    Returns:
        Configured BashOutput tool function
    """

    @tool
    async def BashOutput(command_id: str) -> str:
        """Get the output and status of a background bash command.

        Use this to check on commands started with run_in_background=True.
        To stop a background command, use the Bash tool (e.g. pkill -f '...').

        Args:
            command_id: The command_id returned when the background command was started

        Returns:
            Status and output of the background command
        """
        try:
            result = await sandbox.get_background_command_status(command_id)

            is_running = result["is_running"]
            exit_code = result["exit_code"]
            stdout = result.get("stdout", "")
            stderr = result.get("stderr", "")

            # Format status line
            if is_running:
                status = "RUNNING"
            elif exit_code == 0:
                status = "COMPLETED (success)"
            else:
                status = f"COMPLETED (exit code {exit_code})"

            parts = [f"Status: {status}"]
            if stdout:
                parts.append(f"Output:\n{stdout}")
            if stderr:
                parts.append(f"Errors:\n{stderr}")

            return "\n".join(parts)

        except Exception as e:
            error_msg = f"Failed to get background command output: {e!s}"
            logger.error(error_msg, command_id=command_id, exc_info=True)
            return f"ERROR: {error_msg}"

    return BashOutput
