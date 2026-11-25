"""A thread's replay page when it is a fork that borrows its history.

A snapshot fork holds no rows for the turns before its
``Borrowing.turn_base``: each ancestor segment's turns are read through that
ancestor's own replay (its rows, stored lines and checkpoints, which name
its own thread and responses, so nothing is copied across threads),
truncated to what the segment still shows. One numbering runs across the
boundary: a page takes the fork's own turns first and fills what is left of
its limit and budget with borrowed ones, newest segment first.

What a segment shows is ``forks.segments``' rule, read in the same snapshot
as its rows. A ``fork_divider`` frame, built here at read time, says where
the borrowed turns end and the fork's own begin, once per replay. It leads its
page on the wire, since the client places it by turn number: a page cut off
partway then never shows borrowed turns as the fork's own.

A thread that borrows nothing replays exactly as ``read_replay_page`` has it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Literal

from fastapi import HTTPException

from src.server.database.conversation.replay_rows import ThreadRows
from src.server.database.shared_workspaces import is_holder
from src.server.services.forks.model import Borrowing, ForkLineage
from src.server.services.forks.segments import READS_AT_ONCE, SegmentState, read_segment
from src.server.services.history import replay
from src.server.services.history.replay import PageBudget, ReplayPage, _wire_bytes
from src.server.services.history.replay.lines import Line, line_of
from src.server.services.shared_workspaces.access import thread_access

DIVIDER_EVENT = "fork_divider"


@dataclass
class PagePart:
    """The lines a page takes from one thread. Task cards and senders are
    stamped against the thread that owns them (``finish_lines``,
    ``name_turn_senders``), so each part is finished on its own."""

    thread_id: str
    owner_id: str
    lines: list[Line] = field(default_factory=list)


@dataclass
class ThreadPage:
    #: Oldest first: borrowed segments, then the thread's own turns last.
    parts: list[PagePart]
    first_turn_index: Any | None
    has_more: bool
    #: ``stub`` when any part fell back to stubs, else ``checkpoint``.
    source: str
    #: Leads the page on the wire (see the module docstring).
    divider: Line | None = None

    @property
    def own(self) -> PagePart:
        return self.parts[-1]

    def lines(self) -> list[Line]:
        """The page in wire order, read after its parts are finished."""
        out = [self.divider] if self.divider is not None else []
        out.extend(line for part in self.parts for line in part.lines)
        return out

    async def finish(self, *, for_share: bool = False) -> list[Line]:
        """Stamp what only read time knows onto each part, against the thread
        its turns belong to, and return the page in wire order. A share keeps
        a failed task's reason off and names no senders."""
        from src.server.services.shared_workspaces.viewer import name_turn_senders

        for part in self.parts:
            await replay.finish_lines(part.thread_id, part.lines, status_only=for_share)
            if not for_share and is_holder(part.owner_id):
                await name_turn_senders(part.thread_id, part.lines)
        return self.lines()


async def read_thread_page(
    rows: ThreadRows,
    viewer: str,
    *,
    source: Literal["auto", "checkpoint"] = "auto",
    before_turn: Any | None = None,
    limit: int | None = None,
    budget: PageBudget | None = None,
) -> ThreadPage:
    """The page a thread replays as. ``viewer`` is who borrowed turns are
    read for: the caller, or for a public share the fork's maker."""
    lineage = ForkLineage.parse(rows.thread.get("fork"))
    borrow = lineage.borrow if lineage is not None else None
    if borrow is None or not borrow.ancestors:
        page, resolved = await replay.read_replay_page(
            rows, source=source, before_turn=before_turn, limit=limit, budget=budget
        )
        return ThreadPage(
            parts=[PagePart(rows.thread_id, rows.owner_id, page.lines)],
            first_turn_index=page.first_turn_index,
            has_more=page.has_more,
            source=resolved,
        )
    return await _BorrowedRead(rows, borrow, viewer, source).page(
        before_turn, limit, budget
    )


@dataclass(frozen=True)
class _Room:
    """What is left of a page's limit and budget for the turns still to come.
    A floor of 0 lets a segment's page take no turn at all, once the fork's
    own turns or a newer segment's met it."""

    limit: int | None
    budget: PageBudget | None

    def take(self, page: ReplayPage) -> _Room:
        """The room left once ``page`` is on the page."""
        budget = self.budget
        return _Room(
            limit=None if self.limit is None else self.limit - page.turns,
            budget=None
            if budget is None
            else PageBudget(
                min_turns=max(0, budget.min_turns - page.turns),
                max_bytes=budget.max_bytes - _wire_bytes(page.lines),
            ),
        )

    @property
    def has_room(self) -> bool:
        if self.limit is not None and self.limit <= 0:
            return False
        budget = self.budget
        return budget is None or budget.max_bytes > 0 or budget.min_turns > 0


class _BorrowedRead:
    """One read of a borrowing fork; holds the segment states it has read,
    so each is read once per request."""

    def __init__(
        self,
        rows: ThreadRows,
        borrow: Borrowing,
        viewer: str,
        source: Literal["auto", "checkpoint"],
    ) -> None:
        self.rows = rows
        self.ancestors = borrow.ancestors
        self.turn_base = borrow.turn_base
        self.viewer = viewer
        self.source = source
        self.states: dict[int, SegmentState] = {}
        self.stubbed = False

    async def page(
        self,
        before_turn: Any | None,
        limit: int | None,
        budget: PageBudget | None,
    ) -> ThreadPage:
        rows, turn_base = self.rows, self.turn_base
        own = PagePart(rows.thread_id, rows.owner_id)
        reads_own = before_turn is None or before_turn > turn_base
        has_own = any(q.get("turn_index") is not None for q in rows.queries)
        own_page: ReplayPage | None = None
        if reads_own and has_own:
            own_page, resolved = await replay.read_replay_page(
                rows,
                source=self.source,
                before_turn=before_turn,
                limit=limit,
                budget=budget,
            )
            own.lines = own_page.lines
            self.stubbed = resolved == "stub"
        # The divider goes on the page holding the fork's oldest own turn, or
        # the newest page while it has none.
        holds_oldest_own = (
            own_page is not None and own_page.turns > 0 and not own_page.has_more
        )
        newest_without_own = not has_own and before_turn is None
        divides = holds_oldest_own or newest_without_own
        first_turn_index = own_page.first_turn_index if own_page else None
        if own_page is not None and own_page.has_more:
            return self._done([own], first_turn_index, True, None)

        room = _Room(limit, budget)
        if own_page is not None:
            room = room.take(own_page)
        below = turn_base if before_turn is None else min(before_turn, turn_base)

        borrowed: list[PagePart] = []
        has_more = False
        for index in reversed(range(len(self.ancestors))):
            segment = self.ancestors[index]
            if segment.first_turn >= below:
                continue
            if not room.has_room:
                state = await self._state(index)
                if state.upper(segment, below) is not None:
                    has_more = True
                    break
                continue
            state, seg_rows = await self._read(index, with_rows=True)
            top = state.upper(segment, below)
            if top is None or seg_rows is None:
                continue
            page, resolved = await replay.read_replay_page(
                seg_rows,
                source=self.source,
                from_turn=segment.first_turn,
                before_turn=top + 1,
                limit=room.limit,
                budget=room.budget,
            )
            self.stubbed = self.stubbed or resolved == "stub"
            if page.turns:
                borrowed.insert(0, PagePart(seg_rows.thread_id, seg_rows.owner_id, page.lines))
                first_turn_index = page.first_turn_index
            if page.has_more:
                has_more = True
                break
            room = room.take(page)

        divider = await self._divider() if divides else None
        return self._done([*borrowed, own], first_turn_index, has_more, divider)

    def _done(
        self,
        parts: list[PagePart],
        first_turn_index: Any | None,
        has_more: bool,
        divider: Line | None,
    ) -> ThreadPage:
        return ThreadPage(
            parts=parts,
            first_turn_index=first_turn_index,
            has_more=has_more,
            source="stub" if self.stubbed else "checkpoint",
            divider=divider,
        )

    async def _state(self, index: int) -> SegmentState:
        if index not in self.states:
            await self._read(index, with_rows=False)
        return self.states[index]

    async def _read(
        self, index: int, *, with_rows: bool
    ) -> tuple[SegmentState, ThreadRows | None]:
        """The segment's state, and its thread's rows from the same snapshot
        when asked for and the segment shows anything."""
        segment = self.ancestors[index]
        readable = await self._readable(segment.thread_id)
        state, rows = await read_segment(segment, readable=readable, with_rows=with_rows)
        self.states[index] = state
        return state, rows

    async def _readable(self, thread_id: str) -> bool:
        try:
            await thread_access(thread_id, self.viewer)
        except HTTPException:
            return False
        return True

    async def _divider(self) -> Line:
        ancestors = self.ancestors
        missing = [i for i in range(len(ancestors)) if i not in self.states]
        room = asyncio.Semaphore(READS_AT_ONCE)

        async def read(index: int) -> None:
            async with room:
                await self._state(index)

        await asyncio.gather(*(read(i) for i in missing))
        shown = [
            (segment, self.states[i])
            for i, segment in enumerate(ancestors)
            if self.states[i].shown_through is not None
        ]
        # The source's own state names it; a fork of a fork whose earlier
        # source changed or went since shows a gap above it, which reads as
        # changed even while the source itself still matches.
        state = self.states[len(ancestors) - 1].state
        if state == "shown" and any(
            self.states[i].state != "shown" for i in range(len(ancestors) - 1)
        ):
            state = "changed"
        return line_of(
            {
                "event": DIVIDER_EVENT,
                "data": {
                    "source_thread_id": ancestors[-1].thread_id,
                    "turn_base": self.turn_base,
                    "state": state,
                    "shown_through": shown[-1][1].shown_through if shown else None,
                    "segments": [
                        {
                            "thread_id": segment.thread_id,
                            "first_turn": segment.first_turn,
                            "through_turn": state.shown_through,
                        }
                        for segment, state in shown
                    ],
                },
            }
        )

