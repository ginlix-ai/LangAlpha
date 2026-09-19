"""The file tools refuse exactly what the sandbox validator refuses.

Each tool normalizes a path for the runtime and validates one for the answer.
Those have to be the same question: the validator reads an absolute path
outside the allowed roots as a virtual path and folds it back inside, so a
tool that handed back its own normalized path was told "allowed" about a
different file than the one it went on to write.

The backend here wires the real path functions from
``core.sandbox.path_resolution`` over a fake computer holding two workspace
folders, so the tools answer against the same rules a turn runs under.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.file_ops import create_filesystem_tools
from ptc_agent.agent.tools.glob import create_glob_tool
from ptc_agent.agent.tools.grep import create_grep_tool
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.project_context import ProjectContext, current_project
from ptc_agent.core.sandbox import path_resolution as _paths

ROOT = "/home/workspace"
OWN = "beta-c3d4"
SIBLING = "alpha-a1b2"

REFUSED = [
    "../../../etc/passwd",
    f"../{SIBLING}/.agents/tools/mcp_client_config.json",
    "../_internal/.manifest",
]


def _fake_workspace(sandbox: Any) -> Any:
    """``PTCSandbox.workspace``, which is how path resolution reaches the folder.

    Mirrored rather than stubbed to a constant: the deny list a refusal turns on
    is built through this call, and its ambient-project fallback is what lets a
    caller outside a turn resolve against the whole computer.
    """

    def workspace(project: ProjectContext | None = None) -> Any:
        ctx = project if project is not None else current_project()
        return sandbox.layout.for_workspace(ctx.dir_name if ctx is not None else None)

    return workspace


def _backend() -> Any:
    computer = SandboxLayout(ROOT)
    sandbox = SimpleNamespace(_work_dir=ROOT, layout=computer)
    sandbox.config = SimpleNamespace(
        filesystem=SimpleNamespace(
            working_directory=ROOT,
            allowed_directories=computer.allowed_directories,
            denied_directories=computer.denied_directories,
            enable_path_validation=True,
        )
    )
    project = ProjectContext(
        workspace_id="ws-b",
        dir_name=OWN,
        sibling_dir_names=(SIBLING,),
    )
    sandbox.workspace = _fake_workspace(sandbox)
    sandbox.normalize_path = lambda path, proj=None: _paths.normalize_path(
        sandbox, path, proj or project
    )
    sandbox._normalize_search_path = lambda path: _paths._normalize_search_path(
        sandbox, path
    )

    backend = SimpleNamespace()
    backend.normalize_path = sandbox.normalize_path
    backend.validate_path = lambda path: _paths.validate_path(sandbox, path, project)
    backend.virtualize_path = lambda path: _paths.virtualize_path(
        sandbox, path, project
    )
    backend.filesystem_config = sandbox.config.filesystem
    # Both bases the cap lookup in ``file_ops`` measures against, the way
    # ``SandboxBackend`` answers them: the turn's folder and the machine.
    backend.workspace_dir = sandbox.workspace(project).workspace
    backend.computer_root = ROOT
    backend.aread_range = AsyncMock(return_value="contents\n")
    backend.awrite_text = AsyncMock(return_value=True)
    backend.aedit_text = AsyncMock(return_value={"success": True, "occurrences": 1})
    backend.aglob_paths = AsyncMock(return_value=[])
    backend.agrep_rich = AsyncMock(return_value=[])
    return backend


@pytest.mark.asyncio
@pytest.mark.parametrize("spelling", REFUSED)
async def test_read_refuses_and_never_reaches_the_sandbox(spelling):
    backend = _backend()
    read, _write, _edit = create_filesystem_tools(backend)

    result = await read.ainvoke({"file_path": spelling})

    assert "Access denied" in result
    backend.aread_range.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("spelling", REFUSED)
async def test_write_refuses_and_never_reaches_the_sandbox(spelling):
    backend = _backend()
    _read, write, _edit = create_filesystem_tools(backend)

    result = await write.ainvoke({"file_path": spelling, "content": "X"})

    assert "Access denied" in result
    backend.awrite_text.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("spelling", REFUSED)
async def test_edit_refuses_and_never_reaches_the_sandbox(spelling):
    backend = _backend()
    _read, _write, edit = create_filesystem_tools(backend)

    result = await edit.ainvoke(
        {"file_path": spelling, "old_string": "a", "new_string": "b"}
    )

    assert "Access denied" in result
    backend.aedit_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_glob_and_grep_refuse_a_siblings_wrapper_directory():
    backend = _backend()
    search = f"../{SIBLING}/.agents/tools"

    glob_result = await create_glob_tool(backend).ainvoke(
        {"pattern": "*.py", "path": search}
    )
    grep_result = await create_grep_tool(backend).ainvoke(
        {"pattern": "servers", "path": search}
    )

    assert "Access denied" in glob_result
    assert "Access denied" in grep_result
    backend.aglob_paths.assert_not_awaited()
    backend.agrep_rich.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_turns_own_folder_and_a_siblings_deliverables_still_work():
    """Open decision 1: one computer exists so a turn can read prior work."""
    backend = _backend()
    read, write, _edit = create_filesystem_tools(backend)

    assert "Access denied" not in await read.ainvoke(
        {"file_path": f"../{SIBLING}/work/report.md"}
    )
    assert "Access denied" not in await write.ainvoke(
        {"file_path": "work/out.md", "content": "X"}
    )
    backend.awrite_text.assert_awaited_once()
    written = backend.awrite_text.await_args
    assert (written.args or (written.kwargs.get("file_path"),))[
        0
    ] == f"{ROOT}/{OWN}/work/out.md"
