"""A Write or Edit to a capped context file reports its fill in the tool result.

The baseline block reads agent.md and the memory indexes under a cap and cuts
what is past it, and the block only rebuilds at compaction. So the write that
crosses the line is the moment to say so, in the result the model reads while
it still holds the file. Quiet below the warn ratio, a nudge above it, a plain
statement over the budget, and a refusal for a write that grows the file past
its limit. Sizes are weighted, a Chinese, Japanese or Korean character
counting three, since what the cap protects is the token cost of every call.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.context_file_policy import (
    AGENT_MD_BUDGET,
    WORKSPACE_AGENT_MD,
    INDEX_LINE_LIMIT,
    MAX_AGENT_MD_SIZE,
    MAX_MEMORY_BLOCK_SIZE,
    FILL_WARN_RATIO,
    MEMORY_INDEX_BUDGET,
    weighted_prefix,
    weighted_size,
)
from ptc_agent.agent.tools.file_ops import create_filesystem_tools

WORK_DIR = "/home/workspace"
MEMORY = f"{WORK_DIR}/.agents/user/memory/memory.md"


def _make_backend(
    *,
    write_ok: bool = True,
    current: str | None = None,
    edit_result: dict[str, Any] | None = None,
    dir_name: str = "",
) -> Any:
    """A backend stub covering only what Write and Edit touch.

    ``current`` is what a read of any file returns. ``dir_name`` splits the
    computer, which moves the workspace root off the machine root and is the
    only shape that tells the two cap tiers apart.
    """
    workspace = f"{WORK_DIR}/{dir_name}" if dir_name else WORK_DIR
    backend = SimpleNamespace()
    backend.normalize_path = lambda p: p if p.startswith("/") else f"{workspace}/{p}"
    backend.virtualize_path = lambda p: p[len(WORK_DIR):] if p.startswith(WORK_DIR) else p
    backend.validate_path = lambda p: True
    backend.filesystem_config = SimpleNamespace(
        enable_path_validation=False, working_directory=WORK_DIR
    )
    backend.workspace_dir = workspace
    backend.computer_root = WORK_DIR
    backend.awrite_text = AsyncMock(return_value=write_ok)
    backend.aread_text = AsyncMock(return_value=current)
    backend.aedit_text = AsyncMock(
        return_value=edit_result
        if edit_result is not None
        else {"success": True, "message": "Edited memory.md"}
    )
    return backend


def _tools(backend: Any, **kwargs: Any):
    _read, write, edit = create_filesystem_tools(backend, **kwargs)
    return write, edit


def _lines(size: int) -> str:
    """``size`` characters of short lines, so no line note fires."""
    line = "x" * 99 + "\n"
    return (line * (size // 100 + 1))[:size]


def test_a_wide_character_weighs_three():
    assert weighted_size("ab") == 2
    assert weighted_size("中文") == 6
    assert weighted_size("한글かな，") == 15
    assert weighted_prefix("ab中文", 5) == 3
    assert weighted_prefix("ab中文", 100) == 4


@pytest.mark.asyncio
async def test_a_small_write_says_nothing_extra():
    write, _edit = _tools(_make_backend())
    result = await write.ainvoke({"file_path": MEMORY, "content": "x" * 100})
    assert result == "Wrote 100 bytes to /.agents/user/memory/memory.md"


@pytest.mark.asyncio
async def test_a_write_near_the_budget_carries_a_note():
    write, _edit = _tools(_make_backend())
    size = int(MEMORY_INDEX_BUDGET * FILL_WARN_RATIO) + 10

    result = await write.ainvoke({"file_path": MEMORY, "content": _lines(size)})

    assert f"\n\nNote: memory.md is at {size:,} of {MEMORY_INDEX_BUDGET:,} (85% full" in result
    assert "Shorten it before adding more" in result


@pytest.mark.asyncio
async def test_chinese_fills_the_budget_three_times_as_fast():
    write, _edit = _tools(_make_backend())
    chars = int(MEMORY_INDEX_BUDGET * FILL_WARN_RATIO / 3) + 10
    text = "\n".join("中" * 50 for _ in range(chars // 50 + 1))

    result = await write.ainvoke({"file_path": MEMORY, "content": text})

    assert "Note: memory.md is at " in result


@pytest.mark.asyncio
async def test_a_write_over_the_budget_names_the_limit():
    backend = _make_backend()
    write, _edit = _tools(backend)

    result = await write.ainvoke(
        {"file_path": MEMORY, "content": _lines(MEMORY_INDEX_BUDGET + 1)}
    )

    backend.awrite_text.assert_awaited_once()
    assert "over its budget" in result
    assert f"past {MAX_MEMORY_BLOCK_SIZE:,} is refused" in result


@pytest.mark.asyncio
async def test_a_write_past_the_limit_is_refused_and_writes_nothing():
    backend = _make_backend()
    write, _edit = _tools(backend)

    result = await write.ainvoke(
        {"file_path": MEMORY, "content": _lines(MAX_MEMORY_BLOCK_SIZE + 1)}
    )

    backend.awrite_text.assert_not_awaited()
    assert result.startswith("ERROR: memory.md would be ")
    assert "Nothing was written" in result


@pytest.mark.asyncio
async def test_a_write_that_shrinks_a_file_already_past_the_limit_goes_through():
    """A file written before the limit existed is consolidated in steps."""
    backend = _make_backend(current=_lines(MAX_AGENT_MD_SIZE * 4))
    write, _edit = _tools(backend)

    result = await write.ainvoke(
        {"file_path": "agent.md", "content": _lines(MAX_AGENT_MD_SIZE * 2)}
    )

    backend.awrite_text.assert_awaited_once()
    assert "The part past the limit is not visible" in result


@pytest.mark.asyncio
async def test_an_edit_that_grows_a_file_past_the_limit_is_refused():
    backend = _make_backend(current=_lines(MAX_AGENT_MD_SIZE - 10) + "END")
    _write, edit = _tools(backend)

    result = await edit.ainvoke(
        {"file_path": "agent.md", "old_string": "END", "new_string": "y" * 50}
    )

    backend.aedit_text.assert_not_awaited()
    assert result.startswith("ERROR: agent.md would be ")


@pytest.mark.asyncio
async def test_an_edit_measures_the_file_it_leaves():
    current = _lines(int(AGENT_MD_BUDGET * FILL_WARN_RATIO)) + "END"
    backend = _make_backend(current=current, edit_result={"success": True, "message": "Edited agent.md"})
    _write, edit = _tools(backend)

    result = await edit.ainvoke(
        {"file_path": "agent.md", "old_string": "END", "new_string": "end"}
    )

    backend.aedit_text.assert_awaited_once()
    assert result.startswith("Edited agent.md\n\nNote: agent.md is at ")


@pytest.mark.asyncio
async def test_an_edit_whose_match_is_not_unique_is_left_to_the_backend():
    backend = _make_backend(
        current="a a",
        edit_result={"success": False, "error": "old_string is not unique"},
    )
    _write, edit = _tools(backend)

    result = await edit.ainvoke({"file_path": MEMORY, "old_string": "a", "new_string": "b"})

    assert result == "ERROR: old_string is not unique"


@pytest.mark.asyncio
async def test_an_overlong_index_line_is_flagged():
    write, _edit = _tools(_make_backend())
    text = "- [short](a.md) - fine\n- [long](b.md) - " + "细节" * INDEX_LINE_LIMIT + "\n"

    result = await write.ainvoke({"file_path": MEMORY, "content": text})

    assert f"Note: 1 line(s) of memory.md run past {INDEX_LINE_LIMIT} (line 2)" in result


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
    assert not result.startswith("ERROR")


@pytest.mark.asyncio
async def test_a_failed_write_carries_no_note():
    write, _edit = _tools(_make_backend(write_ok=False))

    result = await write.ainvoke({"file_path": MEMORY, "content": _lines(MEMORY_INDEX_BUDGET)})

    assert result == "ERROR: Write operation failed"


@pytest.mark.asyncio
async def test_a_split_computer_still_caps_both_tiers():
    """The user memory index keeps its cap once the computer has folders.

    It is the one capped file the whole computer shares, so it is spelled from
    the machine root while agent.md is spelled from inside the folder. A single
    base finds one of them and silently drops the other.
    """
    write, _edit = _tools(_make_backend(dir_name="acme-a1b2"))

    shared = await write.ainvoke({"file_path": MEMORY, "content": _lines(MEMORY_INDEX_BUDGET + 100)})
    own = await write.ainvoke({"file_path": "agent.md", "content": _lines(AGENT_MD_BUDGET + 100)})
    plain = await write.ainvoke({"file_path": "work/notes.md", "content": _lines(AGENT_MD_BUDGET + 100)})

    assert f"Note: memory.md is at {MEMORY_INDEX_BUDGET + 100:,}" in shared
    assert f"Note: agent.md is at {AGENT_MD_BUDGET + 100:,}" in own
    assert "Note:" not in plain


_MAPPED = "# Notes\n\n## Purpose\nx\n\n## Reusable\n\n## Deliverables\n- a\n\n## Open\n"
_LEGACY = "# 笔记\n\n## 用途与执行约定\nx\n\n## Key Findings\n- a\n"


@pytest.mark.asyncio
async def test_an_analyst_notebook_in_another_layout_is_asked_to_move():
    write, edit = _tools(_make_backend(current=_LEGACY + "END"), agent_md=WORKSPACE_AGENT_MD)

    wrote = await write.ainvoke({"file_path": "agent.md", "content": _LEGACY})
    edited = await edit.ainvoke({"file_path": "agent.md", "old_string": "END", "new_string": "end"})

    for result in (wrote, edited):
        assert "Note: agent.md is not in the layout Workspace Notes gives" in result
        assert "## Purpose, ## Reusable, ## Open, ## Deliverables" in result


@pytest.mark.asyncio
async def test_a_notebook_in_the_four_sections_carries_no_layout_note():
    write, _edit = _tools(_make_backend(), agent_md=WORKSPACE_AGENT_MD)

    result = await write.ainvoke({"file_path": "agent.md", "content": _MAPPED})

    assert "Note:" not in result


@pytest.mark.asyncio
async def test_homes_notebook_keeps_its_own_layout():
    """Home's agent.md is laid out by the Chief of Staff role, not the four sections."""
    write, _edit = _tools(_make_backend())

    result = await write.ainvoke({"file_path": "agent.md", "content": _LEGACY})

    assert "Note:" not in result


@pytest.mark.asyncio
async def test_a_refused_notebook_in_another_layout_is_told_both():
    backend = _make_backend(current=_LEGACY)
    write, _edit = _tools(backend, agent_md=WORKSPACE_AGENT_MD)

    result = await write.ainvoke(
        {"file_path": "agent.md", "content": _LEGACY + _lines(MAX_AGENT_MD_SIZE)}
    )

    backend.awrite_text.assert_not_awaited()
    assert result.startswith("ERROR: agent.md would be ")
    assert "Note: agent.md is not in the layout" in result


@pytest.mark.asyncio
async def test_an_edit_that_keeps_an_over_limit_file_the_same_size_goes_through():
    """Only growth is refused, as every message says: renaming a heading in a
    notebook still past the limit is how its rewrite starts."""
    current = _lines(MAX_AGENT_MD_SIZE + 500) + "## Key Findings"
    backend = _make_backend(current=current, edit_result={"success": True, "message": "Edited agent.md"})
    _write, edit = _tools(backend)

    result = await edit.ainvoke(
        {"file_path": "agent.md", "old_string": "## Key Findings", "new_string": "## Deliverables"}
    )

    backend.aedit_text.assert_awaited_once()
    assert not result.startswith("ERROR")


@pytest.mark.asyncio
async def test_the_layout_ignores_line_endings_and_code_fences():
    write, _edit = _tools(_make_backend(), agent_md=WORKSPACE_AGENT_MD)
    crlf = _MAPPED.replace("\n", "\r\n")
    fenced = _MAPPED.replace("## Open", "```sh\n## a shell comment\n```\n\n## Open")

    for text in (crlf, fenced):
        result = await write.ainvoke({"file_path": "agent.md", "content": text})
        assert "Note:" not in result


@pytest.mark.asyncio
async def test_a_failed_read_leaves_the_write_unmeasured():
    """A passing read fault must not refuse a rewrite that shrinks a file
    already past the limit, which a new-file measure would."""
    backend = _make_backend(edit_result={"success": True, "message": "Edited agent.md"})
    backend.aread_text = AsyncMock(side_effect=OSError("sandbox gone"))
    write, edit = _tools(backend)

    wrote = await write.ainvoke({"file_path": "agent.md", "content": _lines(MAX_AGENT_MD_SIZE * 2)})
    edited = await edit.ainvoke({"file_path": "agent.md", "old_string": "a", "new_string": "b"})

    backend.awrite_text.assert_awaited_once()
    backend.aedit_text.assert_awaited_once()
    assert not wrote.startswith("ERROR")
    assert edited == "Edited agent.md"
