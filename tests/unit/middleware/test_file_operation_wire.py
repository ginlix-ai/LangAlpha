"""The file-operation event carries the workspace-relative path as ``file_path``.

A workspace is a folder on a shared machine, so an absolute write lands at
``/home/workspace/<dir>/.agents/memory/x.md``. The browser classifies paths by
their relative prefix and strips only the machine roots, so the folder-qualified
spelling matched nothing and a memory write stopped reaching the Memory panel.
The absolute spelling still travels, as ``sandbox_path``.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ptc_agent.agent.middleware.file_operations.sse_middleware import (
    FileOperationMiddleware,
)

WORK_DIR = "/home/workspace/acme-ab12"


def _request(file_path: str):
    return SimpleNamespace(
        tool_call={"name": "Write", "id": "call-1", "args": {"file_path": file_path, "content": "x\n"}}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spelling",
    [
        f"{WORK_DIR}/.agents/memory/notes.md",
        ".agents/memory/notes.md",
        "/.agents/memory/notes.md",
    ],
)
async def test_the_wire_carries_the_relative_path_and_keeps_the_raw_one(spelling):
    mw = FileOperationMiddleware(work_dir=WORK_DIR)
    events = []
    with patch(
        "ptc_agent.agent.middleware.file_operations.sse_middleware.get_stream_writer",
        return_value=events.append,
    ):
        async def handler(_request):
            return "ok"

        assert await mw.awrap_tool_call(_request(spelling), handler) == "ok"

    (event,) = events
    assert event["status"] == "completed"
    assert event["payload"]["file_path"] == ".agents/memory/notes.md"
    assert event["payload"]["sandbox_path"] == spelling


@pytest.mark.asyncio
async def test_a_failed_operation_reports_the_same_pair():
    mw = FileOperationMiddleware(work_dir=WORK_DIR)
    events = []
    with patch(
        "ptc_agent.agent.middleware.file_operations.sse_middleware.get_stream_writer",
        return_value=events.append,
    ):
        async def handler(_request):
            raise RuntimeError("disk full")

        with pytest.raises(RuntimeError):
            await mw.awrap_tool_call(_request(f"{WORK_DIR}/report.md"), handler)

    (event,) = events
    assert event["status"] == "failed"
    assert event["payload"]["file_path"] == "report.md"
    assert event["payload"]["sandbox_path"] == f"{WORK_DIR}/report.md"
