"""Project the turns a read cannot serve from stored lines, and store them.

A batch at a time, each batch's stores running while the next one loads;
the whole-thread claims pass runs here, once, when a batch needs it. What a
turn projects to is ``turn``'s; this module decides only which turns are
projected together, what is written, and what a failure costs, so it is not
digested into the lines epoch.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.server.database.conversation import turn_slices as slices_db
from src.server.database.conversation.replay_rows import with_replay_facts
from src.server.database.pool import DATABASE_UNAVAILABLE
from src.server.services.history import slices
from src.server.services.history.reader import CheckpointHistoryReader, TurnAnchor
from src.server.services.history.replay import lines as replay_lines
from src.server.services.history.replay import run_lane
from src.server.services.history.replay.errors import (
    ClaimsNeeded,
    ClaimsPassNeeded,
    ClaimsUnavailable,
)
from src.server.services.history.replay.keys import lines_key
from src.server.services.history.replay.lines import Line, line_of
from src.server.services.history.replay.stubs import stub_lines
from src.server.services.history.replay.turn import (
    Inputs,
    project_turn,
    settled_at,
)
from src.server.services.history.slices import TurnSlice

logger = logging.getLogger(__name__)

# Turns projected per batch, which bounds the checkpoint states and replay
# facts in hand at once.
PROJECT_BATCH = 16

# Turns by index with their anchors on the branch, in turn order.
_Turns = list[tuple[Any, TurnAnchor]]


@dataclass(frozen=True)
class Policy:
    """What an entry point lets its read do about turns it must project."""

    # A turn that cannot be projected replays as its stub rather than
    # failing the read.
    turn_fallback: bool = False
    # A batch may run the whole-thread claims pass it needs, rather than
    # raise ClaimsPassNeeded.
    claims_pass: bool = True


@dataclass
class _Writes:
    """Rows to store, a statement per kind."""

    turns: list[slices_db.TurnRow] = field(default_factory=list)
    updates: list[slices_db.TurnUpdate] = field(default_factory=list)
    runs: list[slices_db.RunRow] = field(default_factory=list)

    async def store(self, thread_id: str) -> None:
        """Best effort, kind by kind: a turn that fails to store is
        projected again on the next read."""
        for kind, write, rows in (
            ("run slice", slices_db.upsert_run_slices, self.runs),
            ("turn slice", slices_db.upsert_turn_slices, self.turns),
            ("turn lines", slices_db.update_turns, self.updates),
        ):
            if not rows:
                continue
            try:
                await write(rows)
            except Exception:
                logger.warning(
                    "[REPLAY] %s store failed for %s (%d rows)",
                    kind,
                    thread_id,
                    len(rows),
                    exc_info=True,
                )


class _Pending:
    """Stores in flight while the next batch loads. Every one is awaited
    before the request returns, so a later read in it never misses its own
    writes, and the post-turn refresh and backfill still finish theirs."""

    def __init__(self, thread_id: str) -> None:
        self._thread_id = thread_id
        self._tasks: list[asyncio.Task[None]] = []

    def start(self, writes: _Writes) -> None:
        if writes.turns or writes.updates or writes.runs:
            self._tasks.append(asyncio.create_task(writes.store(self._thread_id)))

    async def drain(self) -> None:
        tasks, self._tasks = self._tasks, []
        if tasks:
            await asyncio.gather(*tasks)


@dataclass
class _Loaded:
    """A stale turn's slice, read but not yet projected."""

    turn: TurnSlice
    # Cut from checkpoints by this read, so its whole row is written.
    fresh: bool
    # The claims stored beside the slice it was read from.
    claims: list[str | None] | None


class _ColdRead:
    """One read's stale turns, projected a batch at a time. The batches
    share the run lane, the stores in flight, and the whole-thread claims
    pass, which runs at most once per read."""

    def __init__(
        self,
        reader: CheckpointHistoryReader,
        inputs: Inputs,
        anchored: _Turns,
        stale: _Turns,
        policy: Policy,
    ) -> None:
        self._reader = reader
        self._inputs = inputs
        self._anchored = anchored
        self._stale = [ti for ti, _ in stale]
        self._policy = policy
        self.lane = run_lane.RunLane(reader, inputs.thread_id, inputs.run_facts)
        self.pending = _Pending(inputs.thread_id)
        # Stale turns projected or replayed as stubs.
        self._done: set[Any] = set()
        # Slices read for turns not yet done: a batch the claims pass
        # interrupted, and the turns the pass cut from checkpoints, so each
        # is read once and written once.
        self._in_hand: dict[Any, _Loaded] = {}
        # Launches of the turns read so far, so the claims pass reads only
        # the rest.
        self._launched: dict[Any, list[run_lane.Launch]] = {}
        # Run claims of the whole thread, once the pass placed them.
        self._placed: dict[Any, list[str | None]] | None = None
        self._placement_failed = False

    async def project(self, batch: _Turns) -> dict[Any, list[Line]]:
        """Project a batch and start its stores, running the claims pass
        first when the batch needs it."""
        try:
            return await self._project(batch)
        except ClaimsNeeded as needed:
            await self._place_claims(needed)
            return await self._project(batch)

    async def recover(self, batch: _Turns, error: Exception) -> dict[Any, list[Line]]:
        """The batch's lines after ``error``, a turn at a time, so a turn
        falls back only on its own failure. That holds for run claims the
        whole-thread pass cannot place too: they fail only the turns that
        launched those runs, and the pass is not run again."""
        if len(batch) == 1:
            return await self._fall_back(batch, error)
        logger.warning(
            "[REPLAY] batch projection failed for %s; retrying per turn",
            self._inputs.thread_id,
            exc_info=error,
        )
        out: dict[Any, list[Line]] = {}
        for turn in batch:
            try:
                out.update(await self.project([turn]))
            except DATABASE_UNAVAILABLE:
                raise
            except Exception as err:
                out.update(await self._fall_back([turn], err))
        return out

    async def close(self) -> None:
        """Store the runs a failed batch queued but never took (they settled
        whatever became of the turns that launched them), then wait for
        every store."""
        self.pending.start(_Writes(runs=self.lane.take_writes()))
        await self.pending.drain()

    async def _project(self, batch: _Turns) -> dict[Any, list[Line]]:
        inputs = self._inputs
        loaded = await self._load(batch)
        self._launched.update(
            (ti, run_lane.launches_in(loaded[ti].turn)) for ti, _ in batch
        )
        claims = {
            ti: loaded[ti].claims if self._placed is None else self._placed.get(ti)
            for ti, _ in batch
        }
        await self.lane.prepare(
            [
                (
                    loaded[ti].turn,
                    claims[ti],
                    settled_at(inputs.responses_by_turn.get(ti)),
                )
                for ti, _ in batch
            ]
        )

        # Only turns projected afresh need their row's replay facts (and the
        # version of the row those facts came from).
        responses = await with_replay_facts(
            inputs.responses_by_turn, [ti for ti, _ in batch]
        )
        writes = _Writes()
        out: dict[Any, list[Line]] = {}
        for ti, anchor in batch:
            response = responses.get(ti)
            turn = loaded[ti].turn
            segment, sealed = project_turn(inputs, ti, turn, response, self.lane)
            out[ti] = [line_of(item) for item in segment]
            self._add_turn_write(
                writes,
                ti,
                anchor,
                turn,
                response,
                claims[ti] if self.lane.can_store else None,
                out[ti] if sealed else None,
                fresh=loaded[ti].fresh,
            )
        # Taken once the batch projected: runs a failed batch queued stay
        # queued for the next store.
        writes.runs = self.lane.take_writes()
        self.pending.start(writes)
        self._finish(batch)
        return out

    async def _load(self, batch: _Turns) -> dict[Any, _Loaded]:
        missing = [(ti, a) for ti, a in batch if ti not in self._in_hand]
        turns, claims, fresh = await load_turns(self._reader, self._inputs, missing)
        self._in_hand.update(
            (ti, _Loaded(turns[ti], ti in fresh, claims[ti])) for ti, _ in missing
        )
        return {ti: self._in_hand[ti] for ti, _ in batch}

    async def _fall_back(self, batch: _Turns, error: Exception) -> dict[Any, list[Line]]:
        """The batch as stubs, never stored, so the next read projects it
        again. A turn that answered and archived no stream replays without
        its answer, which is logged as an error: the user sees it missing."""
        inputs = self._inputs
        turns = [ti for ti, _ in batch]
        lines, lost = await stub_lines(
            inputs.thread_id,
            turns,
            inputs.queries_by_turn,
            inputs.responses_by_turn,
            inputs.open_runs,
        )
        self._finish(batch)
        if lost:
            logger.error(
                "[REPLAY] turns %s of %s cannot be projected and archived no "
                "stream; they replay without their answer",
                lost,
                inputs.thread_id,
                exc_info=error,
            )
        else:
            logger.warning(
                "[REPLAY] turns %s of %s cannot be projected; they replay as stubs",
                turns,
                inputs.thread_id,
                exc_info=error,
            )
        return lines

    def _finish(self, batch: _Turns) -> None:
        for ti, _ in batch:
            self._in_hand.pop(ti, None)
            self._done.add(ti)

    async def _place_claims(self, needed: ClaimsNeeded) -> None:
        """Run the whole-thread claims pass, and store what it settled for
        the turns this read will not project."""
        if not self._policy.claims_pass:
            raise ClaimsPassNeeded(str(needed)) from needed
        if self._placement_failed or self._placed is not None:
            # The pass reads the whole thread, and again would read it the
            # same way.
            raise ClaimsUnavailable(str(needed)) from needed
        try:
            self._placed, extracted = await _place_launches(
                self._reader,
                self._inputs,
                self.lane,
                self._anchored,
                self._launched,
            )
        except DATABASE_UNAVAILABLE:
            raise
        except Exception as err:
            self._placement_failed = True
            raise ClaimsUnavailable(str(err)) from err
        waiting = {ti for ti in self._stale if ti not in self._done}
        self._in_hand.update(
            (ti, _Loaded(turn, fresh=True, claims=None))
            for ti, turn in extracted.items()
            if ti in waiting
        )
        # Claims for turns already projected update the rows this read
        # stored, so those stores land first.
        await self.pending.drain()
        self.pending.start(self._claims_writes(extracted, skip=waiting))

    def _claims_writes(
        self, extracted: dict[Any, TurnSlice], *, skip: set[Any]
    ) -> _Writes:
        """What the claims pass leaves to store for turns this read will not
        project (``skip`` holds the ones it will): a slice cut afresh with
        its claims, or the claims alone beside a stored slice. Pinning the
        claims is what lets the pass run once per thread."""
        writes = _Writes(runs=self.lane.take_writes())
        for ti, anchor in self._anchored:
            if ti in skip:
                continue
            claims = (self._placed or {}).get(ti) if self.lane.can_store else None
            if ti not in extracted and claims is None:
                continue
            self._add_turn_write(
                writes,
                ti,
                anchor,
                extracted.get(ti),
                self._inputs.responses_by_turn.get(ti),
                claims,
                None,
                fresh=ti in extracted,
            )
        return writes

    def _add_turn_write(
        self,
        writes: _Writes,
        turn_index: Any,
        anchor: TurnAnchor,
        turn: TurnSlice | None,
        response: dict[str, Any] | None,
        claims: list[str | None] | None,
        lines: list[Line] | None,
        *,
        fresh: bool,
    ) -> None:
        """Queue a turn's one write: its whole row when its slice was cut
        afresh, otherwise whatever this read learned beside the stored slice.
        Lines key off the row the projection read, which may be newer than
        the one the page was keyed by."""
        if response is None or anchor.tail_checkpoint_id is None:
            return
        inputs = self._inputs
        key = (
            lines_key(inputs, turn_index, anchor, response)
            if lines is not None
            else None
        )
        text = replay_lines.encode(lines) if lines is not None else None
        response_id = str(response["conversation_response_id"])
        if fresh and turn is not None:
            codec, data = slices.encode(self._reader.serde, turn)
            writes.turns.append(
                slices_db.TurnRow(
                    response_id,
                    inputs.thread_id,
                    slices_db.StoredSlice(
                        input_checkpoint_id=anchor.input_checkpoint_id,
                        tail_checkpoint_id=anchor.tail_checkpoint_id,
                        slice_key=slices.SLICE_KEY,
                        codec=codec,
                        data=data,
                        lines_key=key,
                        lines=text,
                        claims=claims,
                    ),
                )
            )
        elif text is not None or claims is not None:
            writes.updates.append(
                slices_db.TurnUpdate(
                    response_id, anchor.tail_checkpoint_id, key, text, claims
                )
            )


async def project_stale(
    reader: CheckpointHistoryReader,
    inputs: Inputs,
    anchored: _Turns,
    stale: _Turns,
    policy: Policy,
) -> dict[Any, list[Line]]:
    """Project and store stale turns a batch at a time: each checkpoint
    state holds the thread's whole transcript up to its turn, and each
    response row its replay facts, so a thread read at once would hold both
    for every turn. A batch's stores run while the next batch loads.

    With ``policy.turn_fallback``, a batch that fails is retried a turn at a
    time, and a turn that still fails replays as its stub (``stubs``), never
    stored, so it is projected again on the next read. A database fault ends
    the read: it would fail every turn alike.
    """
    read = _ColdRead(reader, inputs, anchored, stale, policy)
    out: dict[Any, list[Line]] = {}
    try:
        for start in range(0, len(stale), PROJECT_BATCH):
            batch = stale[start : start + PROJECT_BATCH]
            try:
                out.update(await read.project(batch))
            except DATABASE_UNAVAILABLE:
                raise
            except Exception as e:
                if not policy.turn_fallback:
                    raise
                out.update(await read.recover(batch, e))
    finally:
        await read.close()
    return out


async def load_turns(
    reader: CheckpointHistoryReader,
    inputs: Inputs,
    batch: list[tuple[Any, TurnAnchor]],
) -> tuple[dict[Any, TurnSlice], dict[Any, list[str | None] | None], set[Any]]:
    """``(turns, claims, fresh)``: the batch's turns by ``slices.load_turns``.
    Claims count only beside the slice they were placed against."""
    stored = await slices_db.get_turn_slices(
        [rid for ti, _ in batch if (rid := inputs.response_id(ti))]
    )
    rows = {
        ti: row for ti, _ in batch if (row := stored.get(inputs.response_id(ti) or ""))
    }
    turns, fresh = await slices.load_turns(reader, inputs.thread_id, batch, rows)
    claims = {ti: None if ti in fresh else rows[ti].claims for ti, _ in batch}
    return turns, claims, fresh


async def _place_launches(
    reader: CheckpointHistoryReader,
    inputs: Inputs,
    lane: run_lane.RunLane,
    anchored: list[tuple[Any, TurnAnchor]],
    known: dict[Any, list[run_lane.Launch]],
) -> tuple[dict[Any, list[str | None]], dict[Any, TurnSlice]]:
    """``(claims, extracted)``: every run launch of the thread claimed in
    order (``assign_claims``), and the turns this pass had to cut from
    checkpoints.

    Every turn's launches are needed, so the turns whose launches are not
    ``known`` are read a batch at a time, stored slices first. Only their
    launches are kept, except for the turns cut afresh: the caller writes
    each of those once, with its claims.
    """
    launches: list[tuple[list[run_lane.Launch], datetime | None]] = []
    extracted: dict[Any, TurnSlice] = {}
    for start in range(0, len(anchored), PROJECT_BATCH):
        batch = anchored[start : start + PROJECT_BATCH]
        turns, _claims, fresh = await load_turns(
            reader, inputs, [(ti, a) for ti, a in batch if ti not in known]
        )
        extracted.update((ti, turns[ti]) for ti in fresh)
        for ti, _anchor in batch:
            launches.append(
                (
                    known[ti] if ti in known else run_lane.launches_in(turns[ti]),
                    settled_at(inputs.responses_by_turn.get(ti)),
                )
            )

    placed = await lane.assign_claims(launches)
    return {ti: c for (ti, _), c in zip(anchored, placed) if c}, extracted
