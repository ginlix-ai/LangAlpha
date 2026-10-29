"""Cross-writer change detection at the turn boundary.

The baseline block is frozen for an epoch, so a write that lands after it was
frozen is invisible to the model unless something says so. Detection is by
content hash at the turn boundary rather than by a write hook: a hook only sees
the writers it was installed on, and agent.md and memory both have writers
outside this process (the workspace-files API, a background subagent, another
worker). A hash comparison sees every writer, including the ones nobody
remembered to instrument.

What it emits is a diff against the frozen baseline text, not against the
previous turn: the baseline is what the model actually has in its context, so
the delta the model needs is always measured from there. This module owns the
reading and the diff; which of them becomes a row is decided once, in
``epoch.advance_epoch``.
"""

from __future__ import annotations

import difflib
import hashlib
from dataclasses import dataclass, field
from typing import Any

UPDATE_SCHEMA_VERSION = 1

# A diff longer than this renders truncated with a note; longer than
# ``DIFF_REWRITE_LINES`` it is not a diff any more and says so instead. Both are
# judgement calls about what is still readable, not measured constants.
DIFF_MAX_LINES = 60
DIFF_REWRITE_LINES = 240

# The line guards say nothing about width: a one-line file (a minified index,
# an agent.md written without newlines) diffs as three lines that carry the
# whole file twice. A row this wide is a copy, not a diff, and rides in every
# call until compaction, so it falls back to the rewrite notice the same way.
DIFF_MAX_CHARS = 4000

# Context lines around each hunk. Two is enough to locate a hunk in a file the
# model already has in front of it.
_DIFF_CONTEXT_LINES = 2


def sha256_text(text: str) -> str:
    """Stable content hash of a source, over its exact bytes."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class SourceRead:
    """One turn-boundary read of a baseline source.

    ``available`` is separate from an empty ``text`` on purpose: "the workspace
    has no notes" and "the sandbox did not answer" must never collapse into the
    same value, or a transient read failure looks like a deletion and the model
    is told to recreate a file that already exists. ``cap`` is the ceiling this
    source was read under, carried with the read so the freeze does not have to
    know which source it is holding.
    """

    kind: str
    update_kind: str
    path: str
    text: str | None = None
    available: bool = True
    provenance: dict[str, Any] = field(default_factory=dict)
    cap: int | None = None

    @property
    def content(self) -> str:
        return self.text or ""


def render_diff(
    baseline_text: str, current_text: str, path: str, *, readable: bool = True
) -> str:
    """A unified diff of the baseline against the current text, bounded.

    Bounded three ways because this rides in every model call until the next
    epoch: an unchanged pair says so in one line, an ordinary edit renders as a
    diff, and a wholesale rewrite renders as a pointer back at the file rather
    than as a second copy of it. A source the model cannot open (``readable``
    false: a harness block) has nothing to point back at, so a change too
    large to diff whole ships the current text instead, the only copy the
    model can read. The diff labels date the right side to the row ("this row"),
    because the row persists and a later row may supersede it; "now" would be
    false by the time it is read again.
    """
    if baseline_text == current_text:
        return f"{path} matches its frozen copy again."
    lines = list(
        difflib.unified_diff(
            baseline_text.splitlines(),
            current_text.splitlines(),
            fromfile=f"{path} (frozen copy)",
            tofile=f"{path} (this row)",
            lineterm="",
            n=_DIFF_CONTEXT_LINES,
        )
    )
    if not lines:
        return f"{path} matches its frozen copy again."
    if not readable and (
        len(lines) > DIFF_MAX_LINES or len("\n".join(lines)) > DIFF_MAX_CHARS
    ):
        if not current_text.strip():
            return f"{path} is empty as of this row."
        return (
            f"{path} changed too much to diff, so this row carries all of it:"
            f"\n\n{current_text}"
        )
    rewritten = len(lines) > DIFF_REWRITE_LINES
    if not rewritten:
        if len(lines) > DIFF_MAX_LINES:
            hidden = len(lines) - DIFF_MAX_LINES
            lines = [*lines[:DIFF_MAX_LINES], f"[... {hidden} more diff lines ...]"]
        text = "\n".join(lines)
        if len(text) <= DIFF_MAX_CHARS:
            return text
    return (
        f"{path} was rewritten, too extensively to diff here "
        f"({len(current_text.splitlines())} lines now). Read it before "
        "relying on the frozen copy."
    )
