"""What Read shows of a transcript turn or run file.

One line is one event, and one event can hold a whole tool result or a pasted
document, so a turn file read at full width costs about what the turn did.
Each string over ``FIELD_CHARS`` keeps its start and says how to see the rest,
and the line stays JSON, so it reads as a Grep of the same file shows it. A
Read of one line is how the agent asks for an event in full.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from ptc_agent.agent.transcript.store import TASKS_DIR
from ptc_agent.core.paths import WorkspaceLayout

FIELD_CHARS = 1_000
# What one Read of a transcript shows at most, a quarter of any other file's.
READ_CHARS = 40_000
# A window longer than this is cut off the event loop: each long line is
# parsed and written back whole, about a millisecond per 100 KB.
_INLINE_WINDOW_CHARS = 256 * 1024

_SEGMENT = re.compile(
    rf"/{re.escape(WorkspaceLayout.TRANSCRIPTS_DIR)}/[0-9a-f]{{8}}/"
    rf"(?:turn-\d+|{TASKS_DIR}/[^/]+/run-\d+)\.jsonl$"
)


def is_segment(path: str) -> bool:
    """Whether ``path`` is a transcript's turn file or a task's run file."""
    return _SEGMENT.search(path) is not None


def _cut(value: Any, note: str) -> tuple[Any, bool]:
    if isinstance(value, str):
        if len(value) <= FIELD_CHARS:
            return value, False
        return f"{value[:FIELD_CHARS]}…[cut: {len(value):,} chars; {note}]", True
    if isinstance(value, dict):
        pairs = {key: _cut(item, note) for key, item in value.items()}
        return {key: item for key, (item, _) in pairs.items()}, any(
            cut for _, cut in pairs.values()
        )
    if isinstance(value, list):
        items = [_cut(item, note) for item in value]
        return [item for item, _ in items], any(cut for _, cut in items)
    return value, False


def cut_event(line: str, offset: int) -> str:
    """``line``, the file's line at ``offset``, with each long string cut."""
    if len(line) <= FIELD_CHARS:
        return line
    note = f"Read(offset={offset}, limit=1) shows it all"
    try:
        event = json.loads(line)
    except ValueError:
        return f"{line[:FIELD_CHARS]}…[cut: {len(line):,} chars; {note}]"
    cut_value, cut = _cut(event, note)
    # As the transcript writes it, so a line with nothing to cut is as stored.
    return json.dumps(cut_value, ensure_ascii=False, default=str) if cut else line


def _cut_window(lines: list[str], offset: int) -> list[str]:
    cut: list[str] = []
    size = 0
    for number, line in enumerate(lines, start=offset):
        cut.append(cut_event(line, number))
        size += len(cut[-1])
        if size > READ_CHARS:
            break
    return cut


async def cut_window(lines: list[str], offset: int) -> list[str]:
    """A Read window of a transcript, the file's ``lines`` from ``offset``
    on, each cut by ``cut_event``, up to the first that takes the window past
    ``READ_CHARS``: Read clips it there, so a later line would be parsed for
    nothing."""
    if sum(map(len, lines)) > _INLINE_WINDOW_CHARS:
        return await asyncio.to_thread(_cut_window, lines, offset)
    return _cut_window(lines, offset)
