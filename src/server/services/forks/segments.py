"""Whether a borrowed segment still shows what its fork saw.

One rule for every reader, the chat's replay and the agent's transcript
alike, so the divider a person sees and the note the agent reads agree. It is
decided from ledger rows read in one snapshot (``get_borrowed_rows``): the
thread gone, or its workspace deleted, is ``deleted``; the reader unable to
read it is ``unavailable``; the response that ended the segment still
standing, with no run on its turns since the fork, is ``shown``; otherwise
``changed``, through the turn before the first one a run since the fork
answered or none from before it still does.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable, Literal

from src.server.database.conversation.replay_rows import (
    BorrowedRows,
    ThreadRows,
    get_borrowed_rows,
)
from src.server.services.forks.model import BorrowedSegment

SegmentStateName = Literal["shown", "changed", "deleted", "unavailable"]

#: Segment reads one request runs at once: each holds a pool connection, and
#: a long chain of forks of forks lists one segment per thread in it.
READS_AT_ONCE = 4


@dataclass(frozen=True)
class SegmentState:
    state: SegmentStateName
    #: The last turn the segment shows, None when it shows none.
    shown_through: int | None

    def upper(self, segment: BorrowedSegment, before_turn: int) -> int | None:
        """The newest turn the segment shows below ``before_turn``."""
        if self.shown_through is None:
            return None
        top = min(self.shown_through, before_turn - 1)
        return top if top >= segment.first_turn else None


async def read_segment(
    segment: BorrowedSegment, *, readable: bool, with_rows: bool = False
) -> tuple[SegmentState, ThreadRows | None]:
    """The segment's state, and with ``with_rows`` its thread's replay rows
    from the same snapshot when it shows anything. ``readable`` is whether
    the reader may read the segment's thread, checked by the caller."""
    found = await get_borrowed_rows(
        segment.thread_id,
        response_id=segment.response_id,
        run_horizon=segment.run_horizon,
        first_turn=segment.first_turn,
        through_turn=segment.through_turn,
        with_rows=with_rows and readable,
    )
    state = _state_of(segment, found, readable)
    return state, found.rows if state.shown_through is not None else None


async def segment_states(
    segments: Iterable[BorrowedSegment], *, reader: str | None = None
) -> dict[str, SegmentState]:
    """Each segment's state by thread, for the fork's own agent, which works in
    its sources' workspace. It reads a source's transcript where the computer
    serves it, which in a shared workspace is only a thread shared with it;
    a turn the computer does not serve reads through ``reader``, its sender,
    only what they still reach (``ThreadHistory``)."""
    from src.server.database.conversation.thread_reach import served_on_computer

    segments = list(segments)
    room = asyncio.Semaphore(READS_AT_ONCE)

    async def state(segment: BorrowedSegment) -> SegmentState:
        async with room:
            if reader is None:
                readable = await served_on_computer(segment.thread_id)
            else:
                readable = await _reaches(segment.thread_id, reader)
            return (await read_segment(segment, readable=readable))[0]

    states = await asyncio.gather(*(state(s) for s in segments))
    return {s.thread_id: found for s, found in zip(segments, states)}


async def _reaches(thread_id: str, reader: str) -> bool:
    from fastapi import HTTPException

    from src.server.services.shared_workspaces.access import thread_access

    try:
        await thread_access(thread_id, reader)
    except HTTPException:
        return False
    return True


def _state_of(segment: BorrowedSegment, found: BorrowedRows, readable: bool) -> SegmentState:
    if not found.present:
        return SegmentState("deleted", None)
    if not readable:
        return SegmentState("unavailable", None)
    if found.kept:
        return SegmentState("shown", segment.through_turn)
    # Unchanged up to the first turn a run since the fork answered (an edit,
    # a regenerate or a retry) or no run from before it still does.
    through: int | None = None
    for turn_index, seen in found.seen:
        if not seen:
            break
        through = turn_index
    return SegmentState("changed", through)
