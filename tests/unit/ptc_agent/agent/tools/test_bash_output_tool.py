"""Tests for the BashOutput tool's mcp_trace artifact (closes the bash bypass).

A backgrounded command's only result-bearing path is BashOutput, so it returns
``(content, {"mcp_trace": [...]})`` and surfaces the trace the backgrounded
script recorded — present once the command completes, empty on stop / no trace /
error. The artifact never enters the LLM context; the provenance middleware
consumes it. Neutral placeholder data only.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.bash_output import create_bash_output_tool


def _make_backend(*, status: dict | None = None, stopped: bool = True) -> Any:
    backend = SimpleNamespace()
    backend.aget_background_command_status = AsyncMock(return_value=status or {})
    backend.astop_background_command = AsyncMock(return_value=stopped)
    return backend


def _tool_call(command_id: str, action: str = "status") -> dict:
    # A ToolCall (not a bare args dict) makes a content_and_artifact tool return a
    # ToolMessage carrying .artifact, so we can assert on the trace.
    return {
        "type": "tool_call",
        "name": "BashOutput",
        "args": {"command_id": command_id, "action": action},
        "id": "bo-1",
    }


class TestBashOutputMcpTraceArtifact:
    @pytest.mark.asyncio
    async def test_completed_command_surfaces_trace(self):
        trace = [{"server": "marketdata", "tool": "quote", "result_sha256": "a" * 64}]
        tool = create_bash_output_tool(
            _make_backend(
                status={
                    "is_running": False,
                    "exit_code": 0,
                    "stdout": "done",
                    "stderr": "",
                    "mcp_trace": trace,
                }
            )
        )
        msg = await tool.ainvoke(_tool_call("cmd-1"))
        assert msg.artifact == {"mcp_trace": trace}
        assert "COMPLETED (success)" in msg.content
        assert "done" in msg.content

    @pytest.mark.asyncio
    async def test_running_command_has_empty_trace(self):
        tool = create_bash_output_tool(
            _make_backend(
                status={
                    "is_running": True,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "",
                    "mcp_trace": [],
                }
            )
        )
        msg = await tool.ainvoke(_tool_call("cmd-1"))
        assert msg.artifact == {"mcp_trace": []}
        assert "RUNNING" in msg.content

    @pytest.mark.asyncio
    async def test_stop_action_returns_empty_trace(self):
        # A stopped command yields no output, so there's nothing to attest.
        tool = create_bash_output_tool(_make_backend(stopped=True))
        msg = await tool.ainvoke(_tool_call("cmd-1", action="stop"))
        assert msg.artifact == {"mcp_trace": []}
        assert "stopped" in msg.content

    @pytest.mark.asyncio
    async def test_missing_trace_key_defaults_to_empty(self):
        # A status dict without mcp_trace (e.g. legacy/unknown cmd) is tolerated.
        tool = create_bash_output_tool(
            _make_backend(
                status={
                    "is_running": False,
                    "exit_code": 1,
                    "stdout": "",
                    "stderr": "boom",
                }
            )
        )
        msg = await tool.ainvoke(_tool_call("cmd-1"))
        assert msg.artifact == {"mcp_trace": []}
        assert "exit code 1" in msg.content

    @pytest.mark.asyncio
    async def test_backend_error_returns_empty_trace(self):
        backend = _make_backend()
        backend.aget_background_command_status = AsyncMock(
            side_effect=RuntimeError("sandbox gone")
        )
        tool = create_bash_output_tool(backend)
        msg = await tool.ainvoke(_tool_call("cmd-1"))
        assert msg.artifact == {"mcp_trace": []}
        assert msg.content.startswith("ERROR")
