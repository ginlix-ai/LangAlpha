"""What the Write tool tells the agent once a write lands.

A DB-backed route applies a write as row changes, and its report is the only
way the agent learns what those were, so the tool shows it verbatim in place
of the byte count. A plain file still gets the byte count.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.file_ops import create_filesystem_tools

ROOT = "/home/workspace"


def _backend(write_result: Any) -> Any:
    backend = SimpleNamespace()
    backend.normalize_path = lambda p: p
    backend.virtualize_path = lambda p: p
    backend.validate_path = lambda p: True
    backend.filesystem_config = SimpleNamespace(enable_path_validation=False)
    backend.workspace_dir = ROOT
    backend.computer_root = ROOT
    backend.awrite_text = AsyncMock(return_value=write_result)
    return backend


async def _write(backend: Any, path: str, content: str) -> str:
    _read, write, _edit = create_filesystem_tools(backend)
    return await write.ainvoke({"file_path": path, "content": content})


@pytest.mark.asyncio
async def test_a_routes_report_is_the_tool_result():
    report = 'Saved brief.json: created "Brief"; next run 2030-10-01T09:00:00-04:00'
    backend = _backend({"success": True, "message": report})

    result = await _write(backend, f"{ROOT}/.agents/user/automations/brief.json", "{}")

    assert result == report


@pytest.mark.asyncio
async def test_a_plain_write_reports_its_size():
    result = await _write(_backend(True), f"{ROOT}/notes.md", "héllo")

    assert result == f"Wrote 6 bytes to {ROOT}/notes.md"
