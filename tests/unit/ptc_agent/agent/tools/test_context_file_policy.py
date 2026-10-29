"""A Write or Edit to a capped context file reports its fill in the tool result.

The baseline block reads agent.md and the memory indexes under a cap and cuts
what is past it, and the block only rebuilds at compaction. So the write that
crosses the line is the moment to say so, in the result the model reads while
it still holds the file. Quiet below the warn ratio, a nudge above it, and a
plain statement past the cap; refusal is opt-in and only for Write, whose size
is known before anything is written.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.context_file_policy import (
    MAX_MEMORY_BLOCK_SIZE,
    MEMORY_FILL_WARN_RATIO,
)
from ptc_agent.agent.tools.file_ops import create_filesystem_tools

WORK_DIR = "/home/workspace"
MEMORY = f"{WORK_DIR}/.agents/user/memory/memory.md"


def _make_backend(
    *,
    write_ok: bool = True,
    edit_result: dict[str, Any] | None = None,
    dir_name: str = "",
) -> Any:
    """A backend stub covering only what Write and Edit touch.

    ``dir_name`` splits the computer, which moves the workspace root off the
    machine root and is the only shape that tells the two cap tiers apart.
    """
    workspace = f"{WORK_DIR}/{dir_name}" if dir_name else WORK_DIR
    backend = SimpleNamespace()
    backend.normalize_path = lambda p: p if p.startswith("/") else f"{workspace}/{p}"
    backend.virtualize_path = lambda p: p[len(WORK_DIR):] if p.startswith(WORK_DIR) else p
    backend.validate_path = lambda p: True
    backend.filesystem_config = SimpleNamespace(
        enable_path_validation=False, working_directory=WORK_DIR
    )
    # The two bases the cap lookup measures against. They are the same string
    # until the computer is split, which is what ``dir_name`` is for.
    backend.workspace_dir = workspace
    backend.computer_root = WORK_DIR
    backend.awrite_text = AsyncMock(return_value=write_ok)
    backend.aread_text = AsyncMock(return_value=None)
    backend.aedit_text = AsyncMock(
        return_value=edit_result
        if edit_result is not None
        else {"success": True, "message": "Edited memory.md"}
    )
    return backend


def _tools(backend: Any, **kwargs: Any):
    _read, write, edit = create_filesystem_tools(backend, **kwargs)
    return write, edit


@pytest.mark.asyncio
async def test_a_small_write_says_nothing_extra():
    write, _edit = _tools(_make_backend())
    result = await write.ainvoke({"file_path": MEMORY, "content": "x" * 100})
    assert result == "Wrote 100 bytes to /.agents/user/memory/memory.md"


@pytest.mark.asyncio
async def test_a_write_near_the_cap_carries_a_note():
    write, _edit = _tools(_make_backend())
    size = int(MAX_MEMORY_BLOCK_SIZE * MEMORY_FILL_WARN_RATIO) + 10

    result = await write.ainvoke({"file_path": MEMORY, "content": "x" * size})

    assert "\n\nNote: memory.md is at " in result
    assert f"of {MAX_MEMORY_BLOCK_SIZE:,} characters (85% full)" in result
    assert "Consolidate before adding more" in result


@pytest.mark.asyncio
async def test_a_write_past_the_cap_says_the_tail_is_invisible():
    write, _edit = _tools(_make_backend())

    result = await write.ainvoke(
        {"file_path": MEMORY, "content": "x" * (MAX_MEMORY_BLOCK_SIZE + 500)}
    )

    assert "The part past the cap is not visible in your context block" in result


@pytest.mark.asyncio
async def test_an_edit_takes_its_size_from_the_backend_result():
    backend = _make_backend(
        edit_result={
            "success": True,
            "message": "Edited memory.md",
            "size": MAX_MEMORY_BLOCK_SIZE + 1,
        }
    )
    _write, edit = _tools(backend)

    result = await edit.ainvoke(
        {"file_path": MEMORY, "old_string": "a", "new_string": "b"}
    )

    assert result.startswith("Edited memory.md\n\nNote: memory.md is at ")
    backend.aread_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_edit_whose_backend_reports_no_size_stays_silent():
    """No size, no note: guessing one would mean reading the file back."""
    _write, edit = _tools(_make_backend())

    result = await edit.ainvoke(
        {"file_path": MEMORY, "old_string": "a", "new_string": "b"}
    )

    assert result == "Edited memory.md"


@pytest.mark.asyncio
async def test_an_uncapped_file_is_never_annotated():
    write, _edit = _tools(_make_backend())

    result = await write.ainvoke(
        {
            "file_path": f"{WORK_DIR}/report.md",
            "content": "x" * (MAX_MEMORY_BLOCK_SIZE * 2),
        }
    )

    assert "Note:" not in result


@pytest.mark.asyncio
async def test_a_failed_write_carries_no_note():
    write, _edit = _tools(_make_backend(write_ok=False))

    result = await write.ainvoke(
        {"file_path": MEMORY, "content": "x" * (MAX_MEMORY_BLOCK_SIZE * 2)}
    )

    assert result == "ERROR: Write operation failed"


@pytest.mark.asyncio
async def test_refusal_is_opt_in_and_writes_nothing():
    backend = _make_backend()
    write, _edit = _tools(backend, refuse_over_cap=True)

    result = await write.ainvoke(
        {"file_path": MEMORY, "content": "x" * (MAX_MEMORY_BLOCK_SIZE + 1)}
    )

    backend.awrite_text.assert_not_awaited()
    assert result.startswith("ERROR: memory.md would be ")
    assert "Nothing was written" in result


@pytest.mark.asyncio
async def test_the_default_warns_and_lets_the_write_through():
    backend = _make_backend()
    write, _edit = _tools(backend)

    result = await write.ainvoke(
        {"file_path": MEMORY, "content": "x" * (MAX_MEMORY_BLOCK_SIZE + 1)}
    )

    backend.awrite_text.assert_awaited_once()
    assert "Note: memory.md is at" in result


@pytest.mark.asyncio
async def test_a_split_computer_still_caps_both_tiers():
    """The user memory index keeps its cap once the computer has folders.

    It is the one capped file the whole computer shares, so it is spelled from
    the machine root while agent.md is spelled from inside the folder. A single
    base finds one of them and silently drops the other.
    """
    write, _edit = _tools(_make_backend(dir_name="acme-a1b2"))
    size = MAX_MEMORY_BLOCK_SIZE + 100

    shared = await write.ainvoke({"file_path": MEMORY, "content": "x" * size})
    own = await write.ainvoke({"file_path": "agent.md", "content": "x" * size})
    plain = await write.ainvoke({"file_path": "work/notes.md", "content": "x" * size})

    assert f"Note: memory.md is at {size:,}" in shared
    assert f"Note: agent.md is at {size:,}" in own
    assert "Note:" not in plain
