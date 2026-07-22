"""Run-segment claims: split a task namespace into runs and attribute launches.

The content-matching claim path is legacy quarantine — ledgered launches join
by stamped task_run_id, but stamp/segment misalignment and pre-ledger data
still fall back to opener-text matching.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.server.services.history.projector import is_run_boundary_message
from src.server.utils.content_normalizer import normalize_text_content


def split_run_segments(messages: list[Any]) -> list[list[Any]]:
    """Split a task namespace's transcript at run boundaries (the plain
    HumanMessage each spawn/resume opens with). A defensive leading slice
    with no boundary attaches to the first run."""
    segments: list[list[Any]] = []
    current: list[Any] = []
    saw_boundary = False
    for message in messages:
        if is_run_boundary_message(message):
            if saw_boundary:
                segments.append(current)
                current = []
            saw_boundary = True
        current.append(message)
    if current:
        segments.append(current)
    return segments


@dataclass
class TaskRuns:
    history: Any
    segments: list[list[Any]]
    cursor: int = 0
    attributed: bool = False
    last_ctx: tuple[Any, str | None] | None = None
    # A live writer's in-flight run is owned by its stream, not replay: the
    # final launch of a live task claims no segment (tail mode commits the
    # launch while the run still writes — projecting it would duplicate the
    # epoch the stream replays from seq 1).
    live: bool = False
    remaining_launches: int = 0
    # Ledger join (M4): stamps[i] is the task_run_id the i-th run segment's
    # input boundary carries (None = pre-ledger run); run_status maps
    # task_run_id -> ledger status. Empty when the walk/ledger read failed —
    # every claim then takes the legacy content-matching path.
    stamps: list[str | None] = field(default_factory=list)
    run_status: dict[str, str] = field(default_factory=dict)


def segment_opener_text(segment: list[Any]) -> str | None:
    """Text of a segment's run-boundary HumanMessage (its launch input)."""
    for message in segment:
        if is_run_boundary_message(message):
            content = message.content
            if isinstance(content, str):
                return content.strip()
            text, _ = normalize_text_content(content)
            return text.strip() if text else None
    return None


def claim_segment_by_stamp(runs: TaskRuns, run_id: str) -> list[Any] | None:
    """Exact ledger join: the segment whose input boundary carries this
    launch's ``task_run_id``.

    Usable only when stamps align 1:1 with segments (both derive from the
    same run boundaries; a mismatch means the walk saw a boundary shape
    the splitter didn't) — misalignment or a missing stamp returns None
    and the caller falls back to the legacy content-matching claim.
    """
    if len(runs.stamps) != len(runs.segments):
        return None
    for idx in range(runs.cursor, len(runs.segments)):
        if runs.stamps[idx] == run_id:
            runs.cursor = idx + 1
            return runs.segments[idx]
    return None


# dies with Phase 7: ledgered launches always join by stamp; only pre-ledger
# v1 task lanes need the content match below.
def claim_segment(runs: TaskRuns, prompt: str | None) -> list[Any] | None:
    """The launch's run segment, or None if no segment belongs to it.

    Scans forward from the cursor for the segment whose boundary opener
    matches the launch prompt (skipped-over segments belong to earlier,
    already-projected turns and are never revisited). Verification needs
    both sides: a prompt-less artifact or a boundary-less segment (legacy
    data) can't be checked, so those claim positionally at the cursor —
    only a present-but-different opener refuses the pairing.
    """
    if runs.cursor >= len(runs.segments):
        return None
    candidate = runs.segments[runs.cursor]
    if prompt is None or segment_opener_text(candidate) is None:
        runs.cursor += 1
        return candidate
    for idx in range(runs.cursor, len(runs.segments)):
        if segment_opener_text(runs.segments[idx]) == prompt:
            segment = runs.segments[idx]
            runs.cursor = idx + 1
            return segment
    return None
