"""The pointer a compaction summary ends with: the agent's transcript.

A summary alone reads as all that survived, so it names the transcript
directory, the turns it covers and how to search them, and the transcript is
saved from the messages in hand while the summary is written. Only a sandbox
the file mount serves has a transcript to point at, and only a save that
landed is pointed at.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from langchain_core.messages import AnyMessage

from ptc_agent.agent.transcript.render import message_turns
from ptc_agent.agent.transcript.store import TranscriptTarget, segment_file

if TYPE_CHECKING:
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.core.sandbox.livefs_mount import MountHandle

logger = logging.getLogger(__name__)

# The export shares the summary call's wall clock and is usually far shorter;
# this only caps a store that stopped answering.
_EXPORT_TIMEOUT = 30.0

_NOTE = (
    "\n\nThe full history before this point is saved in `{directory}/`, one "
    "`{unit}-NNNN.jsonl` per {unit}.{covers} For a detail the summary leaves "
    "out, Grep that directory (pass it as `path`) and Read the matching file."
)
_COVERS_NOTE = (
    " This summary covers {span}, and a file it names next to a point holds that "
    "point in full."
)
_RESUMES_NOTE = " {units} before {number} did not fit in this summary and are only there."
_RESUMES_PARTWAY_NOTE = (
    " This summary starts partway through {unit} {number}; everything before that "
    "is only there."
)

# (turn number, whether trimming cut into that turn's first kept message)
SummaryStart = tuple[int, bool]


@dataclass(frozen=True)
class TranscriptTurns:
    """Which transcript file holds each message.

    Numbered over the agent's whole checkpoint list, as the renderer numbers
    its files: a trimmed or summarized view would number from the wrong turn.
    """

    target: TranscriptTarget
    turns: Mapping[str, int]

    @classmethod
    def of(cls, target: TranscriptTarget, messages: Sequence[AnyMessage]) -> TranscriptTurns:
        return cls(target, message_turns(messages))

    def file(self, message_id: str | None) -> str | None:
        number = self.turns.get(message_id or "")
        return segment_file(self.target.unit, number) if number else None

    def path(self, message_id: str | None) -> str | None:
        name = self.file(message_id)
        return f"{self.target.directory}/{name}" if name else None


@dataclass(frozen=True)
class SummarySpan:
    """The turns a summary stands in for. ``start`` is set when trimming
    dropped the head of the summarized stretch, and names where it resumes."""

    first: int
    last: int
    start: SummaryStart | None = None


def _mount(backend: SandboxBackend | None) -> MountHandle | None:
    return backend.livefs if backend is not None else None


def transcript_target(
    backend: SandboxBackend | None, thread_id: str | None, checkpoint_ns: str = ""
) -> TranscriptTarget | None:
    if not thread_id or _mount(backend) is None:
        return None
    return TranscriptTarget.for_agent(str(thread_id), checkpoint_ns)


async def aexport_transcript(
    backend: SandboxBackend | None, transcript: TranscriptTarget | None, messages: list[AnyMessage]
) -> TranscriptTarget | None:
    """Bring the transcript up to date; the one a summary may point at, or
    None unless the save landed. Never raises.

    The turn-end export comes only after this agent finishes, so a pointer
    left on a failed save sends it for history that is not there for the
    rest of the turn.
    """
    mount = _mount(backend)
    if mount is None or transcript is None:
        return None
    try:
        saved = await asyncio.wait_for(
            mount.save_transcript(transcript, messages), timeout=_EXPORT_TIMEOUT
        )
    except Exception as e:
        logger.warning(
            "[Compaction] transcript save for %s failed: %r", transcript.directory, e
        )
        return None
    if not saved:
        logger.warning("[Compaction] transcript save for %s did not land", transcript.directory)
        return None
    return transcript


def transcript_note(transcript: TranscriptTarget, span: SummarySpan | None = None) -> str:
    unit = transcript.unit
    covers = ""
    if span is not None:
        first, last = segment_file(unit, span.first), segment_file(unit, span.last)
        covers = _COVERS_NOTE.format(
            span=f"{unit} {span.last} (`{last}`)"
            if span.first == span.last
            else f"{unit}s {span.first} to {span.last} (`{first}` to `{last}`)"
        )
    note = _NOTE.format(directory=transcript.directory, unit=unit, covers=covers)
    if span is None or span.start is None:
        return note
    number, partway = span.start
    if partway:
        note += _RESUMES_PARTWAY_NOTE.format(unit=unit, number=number)
    elif number > 1:
        note += _RESUMES_NOTE.format(units=f"{unit.capitalize()}s", number=number)
    return note


def summary_span(
    raw_messages: Sequence[AnyMessage],
    to_summarize: Sequence[AnyMessage],
    summarized: Sequence[AnyMessage],
) -> SummarySpan | None:
    """The turns the summary of ``to_summarize`` covers, ``summarized`` being
    what the summarizer was sent of them after trimming. A summary at the
    head of the stretch stands in for every turn before it, so an untrimmed
    span starts at the first turn."""
    turns = message_turns(raw_messages)
    last = next((turns[m.id] for m in reversed(to_summarize) if m.id in turns), None)
    if last is None:
        return None
    start = _resumes_at(turns, to_summarize, summarized)
    first = start[0] if start is not None else 1
    return SummarySpan(min(first, last), last, start)


def _resumes_at(
    turns: Mapping[str, int],
    to_summarize: Sequence[AnyMessage],
    summarized: Sequence[AnyMessage],
) -> SummaryStart | None:
    """Where the summary starts when trimming dropped the head, else None.

    Trimming keeps the newest tokens, so a long stretch loses its oldest turns,
    or the front of one huge message, without a trace; this names the turn the
    summary picks up in.
    """
    if not summarized or not to_summarize:
        return None
    first = summarized[0]
    original = next((m for m in to_summarize if m.id == first.id), None)
    partway = original is not None and original.content != first.content
    if original is to_summarize[0] and not partway:
        return None
    for message in summarized:
        number = turns.get(message.id or "")
        if number is not None:
            return number, partway and message is first
    return None
