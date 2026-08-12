"""Get output and status of background bash commands."""

from typing import Any, Literal

import structlog
from langchain_core.tools import BaseTool, tool

from ptc_agent.agent.backends.sandbox import SandboxBackend

logger = structlog.get_logger(__name__)


def create_bash_output_tool(backend: SandboxBackend) -> BaseTool:
    """Factory function to create BashOutput tool with injected dependencies.

    Args:
        backend: SandboxBackend wrapping the sandbox

    Returns:
        Configured BashOutput tool function
    """

    @tool("BashOutput", response_format="content_and_artifact")
    async def BashOutput(
        command_id: str, action: Literal["status", "stop"] = "status"
    ) -> tuple[str, dict[str, Any]]:
        """Get the output/status of a background command, or stop it.

        Use this to check on commands started with run_in_background=True.

        Args:
            command_id: The command_id returned when the background command was started
            action: "status" (default) to check output, or "stop" to terminate the command

        Returns:
            Status and output of the background command, or confirmation of stop.
            The artifact carries ``mcp_trace`` (provenance for MCP calls a
            backgrounded script made, surfaced once when the command finishes)
            and never enters the LLM context.
        """
        try:
            if action == "stop":
                stopped = await backend.astop_background_command(command_id)
                if stopped:
                    msg = f"Background command {command_id} stopped."
                else:
                    msg = f"No running background command found with id {command_id}."
                return msg, {"mcp_trace": []}

            result = await backend.aget_background_command_status(command_id)

            is_running = result["is_running"]
            exit_code = result["exit_code"]
            stdout = result.get("stdout", "")
            stderr = result.get("stderr", "")
            # MCP provenance for a backgrounded script's calls — present only on
            # the status read that observes completion. Stripped from the
            # artifact by the provenance middleware before it leaves the host.
            artifact = {"mcp_trace": list(result.get("mcp_trace") or [])}

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

            return "\n".join(parts), artifact

        except Exception as e:
            error_msg = f"Failed to get background command output: {e!s}"
            logger.error(error_msg, command_id=command_id, exc_info=True)
            return f"ERROR: {error_msg}", {"mcp_trace": []}

    return BashOutput
