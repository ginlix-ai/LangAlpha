"""Tests for the Bash tool's mcp_trace artifact (closes the bash provenance bypass).

The Bash tool returns ``(content, {"mcp_trace": [...]})`` so a script it runs that
calls MCP wrappers records the same provenance ExecuteCode does. These pin the
artifact wiring: trace passed through on success and failure, empty on a plain
command or a blocked memory path. Neutral placeholder data only.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.bash import create_execute_bash_tool


def _make_backend(result: dict) -> Any:
    backend = SimpleNamespace()
    backend.filesystem_config = SimpleNamespace(working_directory="/home/workspace")
    backend.aexecute_bash = AsyncMock(return_value=result)
    return backend


def _tool_call(command: str) -> dict:
    # Invoking with a ToolCall (not a bare args dict) makes a content_and_artifact
    # tool return a ToolMessage carrying .artifact, so we can assert on it.
    return {
        "type": "tool_call",
        "name": "Bash",
        "args": {"command": command},
        "id": "bash-1",
    }


class TestBashMcpTraceArtifact:
    @pytest.mark.asyncio
    async def test_success_surfaces_mcp_trace_on_artifact(self):
        trace = [{"server": "marketdata", "tool": "quote", "result_sha256": "a" * 64}]
        tool = create_execute_bash_tool(
            _make_backend(
                {
                    "success": True,
                    "stdout": "ok",
                    "stderr": "",
                    "exit_code": 0,
                    "mcp_trace": trace,
                }
            )
        )
        msg = await tool.ainvoke(_tool_call("python analysis.py"))
        assert msg.artifact == {"mcp_trace": trace}
        assert "ok" in msg.content

    @pytest.mark.asyncio
    async def test_failure_still_carries_mcp_trace(self):
        # A non-zero exit after a successful in-sandbox MCP call must keep the trace.
        trace = [{"server": "finance", "tool": "get_prices", "result_sha256": "b" * 64}]
        tool = create_execute_bash_tool(
            _make_backend(
                {
                    "success": False,
                    "stdout": "boom",
                    "stderr": "",
                    "exit_code": 1,
                    "mcp_trace": trace,
                }
            )
        )
        msg = await tool.ainvoke(_tool_call("python analysis.py"))
        assert msg.artifact == {"mcp_trace": trace}
        assert msg.content.startswith("ERROR")

    @pytest.mark.asyncio
    async def test_plain_command_has_empty_trace(self):
        tool = create_execute_bash_tool(
            _make_backend(
                {"success": True, "stdout": "", "stderr": "", "exit_code": 0}
            )
        )
        msg = await tool.ainvoke(_tool_call("mkdir foo"))
        assert msg.artifact == {"mcp_trace": []}

    @pytest.mark.asyncio
    async def test_memory_path_block_returns_empty_trace_and_skips_backend(self):
        backend = _make_backend({"success": True, "stdout": "", "exit_code": 0})
        tool = create_execute_bash_tool(backend)
        msg = await tool.ainvoke(_tool_call("cat .agents/user/memory/secret.md"))
        assert msg.artifact == {"mcp_trace": []}
        assert "ERROR" in msg.content
        backend.aexecute_bash.assert_not_called()
