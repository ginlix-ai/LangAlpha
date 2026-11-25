"""Where a thread's own history starts, which every thread has.

An ordinary thread starts empty: its transcript files and its turns' slices
begin at its first message. A snapshot fork starts at its cut: it holds one
checkpoint of its source's state there, its own transcript files number on
from the window at the cut, and the files before are its sources'
(``BorrowedFiles``). A reader resolves the start once, from a thread row's
``metadata->'fork'`` it already has, and passes it down, so an ordinary thread
pays no extra read and the code that uses it works the same on both.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from ptc_agent.agent.transcript import BorrowedFiles, Window, transcript_subdir
from src.server.services.forks.model import BorrowedSegment, ForkLineage
from src.server.services.forks.segments import segment_states


@dataclass(frozen=True)
class ThreadStart:
    #: The transcript window at the start: its runs are the files the thread
    #: borrows, and its own number on from ``head.runs + 1``.
    head: Window = Window()
    #: The checkpoint holding the state at the start; None for a thread that
    #: starts empty.
    checkpoint_id: Optional[str] = None
    #: The turns the thread shows from its sources, oldest first.
    ancestors: tuple[BorrowedSegment, ...] = ()

    @classmethod
    def of(cls, lineage: Optional[ForkLineage]) -> ThreadStart:
        borrow = lineage.borrow if lineage is not None else None
        if borrow is None:
            # A fork that copied its history holds every turn itself.
            return cls()
        return cls(borrow.head.window(), borrow.checkpoint_id, borrow.ancestors)

    @classmethod
    def parse(cls, raw: Any) -> ThreadStart:
        """From a thread's stored ``metadata->'fork'`` value."""
        return cls.of(ForkLineage.parse(raw))

    @property
    def borrowed(self) -> tuple[BorrowedFiles, ...]:
        return borrowed_files(self.ancestors)

    def header(self) -> dict[str, Any]:
        """What the thread's own transcript manifest says of the files it
        borrows, each directory a sibling of its own: a fork lives in its
        source's workspace."""
        borrowed = self.borrowed
        if not borrowed:
            return {}
        return {
            "forked_from": [
                files.as_manifest(transcript_subdir(files.thread_id[:8])) for files in borrowed
            ]
        }

    async def borrowed_now(self, reader: str | None = None) -> tuple[BorrowedFiles, ...]:
        """The borrowed files, each stating whether it still shows what the
        fork saw (``segments.segment_states``), to ``reader`` when it is a
        shared workspace's member."""
        files = self.borrowed
        if not files:
            return ()
        states = await segment_states(self.ancestors, reader=reader)
        return tuple(
            dataclasses.replace(f, state=states[f.thread_id].state)
            if f.thread_id in states
            else f
            for f in files
        )


def borrowed_files(segments: Iterable[BorrowedSegment]) -> tuple[BorrowedFiles, ...]:
    """The transcript files borrowed segments hold, by thread, oldest first.
    A segment ending at or before the one before it holds no files of its
    own: its turns' files were the earlier source's."""
    out: list[BorrowedFiles] = []
    last = 0
    for segment in segments:
        if segment.runs_through > last:
            out.append(BorrowedFiles(segment.thread_id, last + 1, segment.runs_through))
            last = segment.runs_through
    return tuple(out)
