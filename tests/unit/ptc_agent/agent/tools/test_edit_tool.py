"""An empty old_string never reaches a backend.

The empty string occurs between every pair of characters, so a backend counts
it as a match: with replace_all the file came back with the new text spliced
between each character, and the tool reported success.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.file_ops import create_filesystem_tools

ROOT = "/home/workspace"


def _backend() -> Any:
    backend = SimpleNamespace()
    backend.normalize_path = lambda p: p
    backend.virtualize_path = lambda p: p
    backend.validate_path = lambda p: True
    backend.filesystem_config = SimpleNamespace(enable_path_validation=False)
    backend.workspace_dir = ROOT
    backend.computer_root = ROOT
    backend.aread_text = AsyncMock(return_value="abc")
    backend.aedit_text = AsyncMock(return_value={"success": True, "message": "edited"})
    return backend


@pytest.mark.asyncio
@pytest.mark.parametrize("replace_all", [True, False])
async def test_an_empty_old_string_is_refused_before_the_backend(replace_all):
    backend = _backend()
    _read, _write, edit = create_filesystem_tools(backend)

    result = await edit.ainvoke(
        {"file_path": f"{ROOT}/notes.md", "old_string": "", "new_string": "x", "replace_all": replace_all}
    )

    backend.aedit_text.assert_not_awaited()
    assert result.startswith("ERROR: old_string is empty.")
