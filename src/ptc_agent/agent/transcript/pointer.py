"""The pointer a compaction summary ends with: the agent's transcript.

A summary alone reads as all that survived, so it names the transcript
directory, the turns it covers, each turn's file beside the request that
opened it, and how to search them, and the transcript is saved from the
messages in hand before the summary is written. Only a sandbox
the file mount serves has a transcript to point at, and only a save that
landed, in a workspace folder whose links are in, is pointed at.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from langchain_core.messages import AnyMessage

from ptc_agent.agent.transcript.classify import is_summary_message
from ptc_agent.agent.transcript.identity import Window, newest_lines
from ptc_agent.agent.transcript.render import turn_map
from ptc_agent.agent.transcript.store import TranscriptTarget, segment_file

if TYPE_CHECKING:
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.core.sandbox.livefs_mount import MountHandle

logger = logging.getLogger(__name__)

# The summary call waits on the export, which usually takes milliseconds; this
# caps a store that stopped answering or links that are slow to go in.
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
_GAP_NOTE = (
    " {units} {first} to {last} did not fit in this summary in full; their files "
    "hold what it leaves out."
)
_GAP_ONE_NOTE = (
    " {unit} {number} did not fit in this summary in full; its file holds what it "
    "leaves out."
)
_INDEX_HEAD = "\n\nEach {unit}'s file and the request that opened it:\n"


@dataclass(frozen=True)
class TranscriptTurns:
    """Which transcript file holds each message.

    Numbered over the agent's checkpoint list from ``Window.runs`` on, as
    the renderer numbers its files: a summarized view would number from the
    wrong turn, and so would a list the window trimmed, counted from one.
    """

    target: TranscriptTarget
    turns: Mapping[str, int]
    #: The index text of each turn, by number (``Window.lines``).
    lines: Mapping[int, str]

    @classmethod
    def of(
        cls, target: TranscriptTarget, messages: Sequence[AnyMessage], *, window: Window
    ) -> TranscriptTurns:
        turns, requests = turn_map(messages, base=window.runs)
        return cls(target, turns, window.lines(requests))

    def file(self, message_id: str | None) -> str | None:
        number = self.turns.get(message_id or "")
        return segment_file(self.target.unit, number) if number else None

    def path(self, message_id: str | None) -> str | None:
        name = self.file(message_id)
        return f"{self.target.directory}/{name}" if name else None

    def last(self, messages: Sequence[AnyMessage]) -> int | None:
        """The newest turn any of ``messages`` belongs to."""
        return max((self.turns[m.id] for m in messages if m.id in self.turns), default=None)

    def index(self, last: int) -> list[str]:
        """Each file up to ``last`` with the request that opened it.

        A summary keeps no word of some turns, the ones an earlier summary
        stood in for or trimming dropped; this is what lets the summary cite
        them and the agent find them.
        """
        unit = self.target.unit
        newest = newest_lines(self.lines, last)
        earlier = next(iter(newest), 1) - 1
        lines = []
        if earlier:
            names = f"`{segment_file(unit, 1)}`"
            if earlier > 1:
                names += f" to `{segment_file(unit, earlier)}`"
            lines.append(
                f"- {names}: {earlier} earlier {unit}{'s' if earlier > 1 else ''}, "
                f"not listed here; `{self.target.manifest}` lists every file"
            )
        lines += [f"- `{segment_file(unit, n)}`: {text}" for n, text in newest.items()]
        return lines


@dataclass(frozen=True)
class SummarySpan:
    """The turns a summary stands in for, ``first`` to ``last``. ``gap`` is
    the first and last turn trimming left at least partly out of it."""

    first: int
    last: int
    gap: tuple[int, int] | None = None


def _mount(backend: SandboxBackend | None) -> MountHandle | None:
    return backend.livefs if backend is not None else None


def transcript_target(
    backend: SandboxBackend | None, thread_id: str | None, checkpoint_ns: str = ""
) -> TranscriptTarget | None:
    if not thread_id or _mount(backend) is None:
        return None
    return TranscriptTarget.for_agent(str(thread_id), checkpoint_ns)


async def aexport_transcript(
    backend: SandboxBackend | None,
    transcript: TranscriptTarget | None,
    messages: list[AnyMessage],
    *,
    workspace_id: str | None,
    budget: float = _EXPORT_TIMEOUT,
    window: Window,
) -> TranscriptTarget | None:
    """Bring the transcript up to date within ``budget`` seconds (at most
    ``_EXPORT_TIMEOUT``); the one a summary may point at, or None unless the
    save landed and the mount serves ``workspace_id``'s folder. Never raises.
    ``window`` is what was trimmed from the head of ``messages``, whose
    files the store keeps.

    The turn-end export comes only after this agent finishes, so a pointer
    left on a failed save sends it for history that is not there for the
    rest of the turn. Grep and Read reach the files straight through the
    folder's links and, unlike Bash, do not wait for links still going in, so
    a pointer is only as good as those links.
    """
    mount = _mount(backend)
    if backend is None or mount is None or transcript is None:
        return None

    async def save_and_settle() -> tuple[bool, MountHandle | None]:
        # A task group, not gather: when one fails, the other is cancelled
        # rather than left saving past the timeout.
        async with asyncio.TaskGroup() as group:
            saved = group.create_task(mount.save_transcript(transcript, messages, window=window))
            settled = group.create_task(backend.settled_livefs(workspace_id))
        return saved.result(), settled.result()

    try:
        saved, settled = await asyncio.wait_for(
            save_and_settle(), timeout=min(budget, _EXPORT_TIMEOUT)
        )
    except Exception as e:
        logger.warning(
            "[Compaction] transcript save for %s failed: %r", transcript.directory, e
        )
        return None
    if not saved:
        logger.warning("[Compaction] transcript save for %s did not land", transcript.directory)
        return None
    if settled is None:
        logger.warning(
            "[Compaction] transcript %s saved, but workspace %s has no file mount links",
            transcript.directory,
            workspace_id,
        )
        return None
    return transcript


def transcript_note(
    transcript: TranscriptTarget,
    span: SummarySpan | None = None,
    index: Sequence[str] = (),
) -> str:
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
    if span is not None and span.gap is not None:
        first, last = span.gap
        note += (
            _GAP_ONE_NOTE.format(unit=unit.capitalize(), number=first)
            if first == last
            else _GAP_NOTE.format(units=f"{unit.capitalize()}s", first=first, last=last)
        )
    if index:
        note += _INDEX_HEAD.format(unit=unit) + "\n".join(index)
    return note


def summary_span(
    turns: Mapping[str, int],
    to_summarize: Sequence[AnyMessage],
    summarized: Sequence[AnyMessage],
    earlier: SummarySpan | None = None,
) -> SummarySpan | None:
    """The turns the summary of ``to_summarize`` covers, ``summarized`` being
    what the summarizer was sent of them after trimming, numbered by
    ``turns`` (see ``TranscriptTurns.turns``). ``earlier`` is the span the
    earlier summary at the head of ``to_summarize`` recorded, if any.

    An earlier summary kept at the head stands in for the turns it covered,
    so the span starts where it did, at the first turn unless it recorded
    otherwise, and the turns it left out stay left out: it reaches the
    summarizer without its note saying so. Without one, the span starts at
    the first turn unless trimming dropped the head.
    """
    last = next((turns[m.id] for m in reversed(to_summarize) if m.id in turns), None)
    if last is None:
        return None
    gap = _left_out(turns, to_summarize, summarized)
    if summarized and is_summary_message(summarized[0]):
        if earlier is None:
            return SummarySpan(1, last, gap)
        return SummarySpan(min(earlier.first, last), last, _joined(earlier.gap, gap))
    first = next((turns[m.id] for m in summarized if m.id in turns), 1) if gap else 1
    return SummarySpan(min(first, last), last, gap)


def _joined(
    a: tuple[int, int] | None, b: tuple[int, int] | None
) -> tuple[int, int] | None:
    if a is None or b is None:
        return a or b
    return min(a[0], b[0]), max(a[1], b[1])


def _left_out(
    turns: Mapping[str, int],
    to_summarize: Sequence[AnyMessage],
    summarized: Sequence[AnyMessage],
) -> tuple[int, int] | None:
    """The first and last turn trimming left at least partly out.

    Trimming drops messages without a trace, or keeps only the end of one
    huge message under its id; either leaves its turn short. A dropped
    earlier summary takes every turn it stood in for with it.
    """
    sent = {m.id: m.content for m in summarized if m.id}
    numbers: list[int] = []
    for i, message in enumerate(to_summarize):
        if message.id in sent and sent[message.id] == message.content:
            continue
        if is_summary_message(message):
            after = next((turns[m.id] for m in to_summarize[i + 1 :] if m.id in turns), 1)
            numbers += [1, after]
        elif message.id in turns:
            numbers.append(turns[message.id])
    return (min(numbers), max(numbers)) if numbers else None
