"""Assemble a thread's replay for the replay endpoint.

Produces the live wire's ``{"event": type, "data": dict}`` stream, sourcing
the transcript from checkpoints (via ``CheckpointHistoryReader`` + the pure
projector) and the non-derivable remainder from table rows:

- ``steering_delivered`` and ``context_window`` (token_usage, summarize,
  offload) are projected from checkpoint state — steering payloads and
  summarize fields are stamped into message ``additional_kwargs`` at emit
  time, token usage comes from ``usage_metadata``, offload counts and the
  summarize event from private-state deltas.
- ``provenance`` / ``credit_usage`` are table-sourced (provenance_records /
  conversation_usages rows, written at persist time); answered ``interrupt``
  cards project from the resume boundary's ``__interrupt__`` pending writes;
  the terminal ``error`` event replays the frame the live stream sent, kept
  in the response row's replay facts (or placed by a legacy turn's facts),
  with the row's kind stamped on it, and is rebuilt from the row when
  neither holds one.
- Sandbox image paths in projected text resolve through the replay facts of
  the run that wrote them (the turn's response row, a subagent run's row),
  and through ``image_capture`` ui records for a turn settled before its row
  held them.
- What only the live stream measured or returned (reasoning durations, a
  subagent run's returned steering) comes from the run's replay facts
  (``facts``).
- A turn settled while its live stream was archived carries what only that
  archive kept in its row's legacy facts (``legacy``): widget data files,
  evicted results, signal rows in their streamed positions, image URLs.

A settled turn is projected once: its slice (what the turn added to the
checkpoint) and its projected lines are stored with its response row, and a
read streams the lines while their key still matches the inputs. Only a turn
whose key moved is projected again, from its stored slice when the branch
still agrees with it, and from checkpoints otherwise.

A turn that cannot be projected replays as a stub (its user message, the
stream its row archived if it kept one, and its closing rows) on the read
paths that ask for it, so one bad turn costs only itself. A stub of a turn
that answered and archived nothing closes on an error frame
(``stubs.unavailable_item``), never as a turn the assistant silently left
unanswered.

Package layout:

- this root pairs checkpoint turns with persisted ones, pages them, and
  serves the stored lines whose key still matches;
- ``cold`` projects and stores the rest, a batch at a time;
- ``turn`` projects one turn: ``items`` builds its table-sourced events,
  ``run_lane`` its background-task runs, ``legacy`` applies its legacy
  facts, and ``facts`` applies what runs recorded on their rows;
- ``lines`` holds the stored wire format, and ``widgets`` inlines offloaded
  payloads on the way out;
- ``keys`` decides whether stored lines still stand, ``stubs`` is what a
  turn that cannot be projected replays as, ``refresh`` projects turns as
  they settle, and ``errors`` says why a read gives up.

Only the modules that decide what lines hold are digested into the lines
key (``keys.projection_sources``).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from src.server.database import conversation as conversation_db
from src.server.database.conversation import turn_slices as slices_db
from src.server.database.conversation.replay_rows import ThreadRows
from src.server.database.pool import DATABASE_UNAVAILABLE
from src.server.services.history import slices
from src.server.services.history import task_status
from src.server.services.history.reader import (
    CheckpointHistoryReader,
    TurnAnchor,
    pair_turns,
)
from src.server.services.history.replay import cold
from src.server.services.history.replay import items
from src.server.services.history.replay import lines as replay_lines
from src.server.services.history.replay import widgets
from src.server.services.history.replay.errors import (
    CheckpointReplayUnavailable,
    ClaimsPassNeeded,
)
from src.server.services.history.replay.keys import lines_epoch, lines_key
from src.server.services.history.replay.lines import Line, line_of
from src.server.services.history.replay.stubs import stub_lines
from src.server.services.history.replay.turn import Inputs, group_queries
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

__all__ = [
    "CheckpointReplayUnavailable",
    "ClaimsPassNeeded",
    "PageBudget",
    "ReplayPage",
    "build_replay_page",
    "ThreadRows",
    "finish_lines",
    "lines_epoch",
    "load_thread_inputs",
    "pair_anchors",
    "project_turns",
    "read_replay_page",
    "stub_page",
    "unserved_turns",
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PageBudget:
    """How much of the thread a page may carry: the newest turns until their
    lines pass ``max_bytes``, never fewer than ``min_turns``.

    Turns vary from kilobytes to megabytes (a subagent's internals ride its
    parent turn), so a page counted only in turns can carry tens of megabytes.
    The floor is the client's: the turns that fill its screen, or on a reload
    every turn back to where the reader had paged."""

    min_turns: int
    max_bytes: int


@dataclass
class ReplayPage:
    lines: list[Line]
    # The oldest turn in this page; the next page asks for turns before it.
    first_turn_index: Any | None
    has_more: bool


async def load_thread_inputs(thread_id: str) -> tuple[ThreadRows, str] | None:
    """A thread's replay rows and the commit pointer to read its branch
    from, or None when the thread is unknown or never committed a turn."""
    rows = await conversation_db.get_replay_thread_data(thread_id)
    if rows is None:
        return None
    tip = rows.thread.get("latest_checkpoint_id")
    return (rows, tip) if tip else None


async def build_replay_page(
    rows: ThreadRows,
    branch_tip_checkpoint_id: str,
    *,
    before_turn: Any | None = None,
    limit: int | None = None,
    budget: PageBudget | None = None,
    turn_fallback: bool = False,
) -> ReplayPage:
    """Replay lines for one page of the thread: with ``limit``, the newest N
    checkpointed turns before ``before_turn`` (all of them without it), plus
    the persisted turns with no committed boundary among them.

    Turns pair to persisted query rows by stamped ``turn_index`` metadata
    (the next persisted turn for a resume or a pre-stamping thread, see
    ``pair_turns``); a persisted turn with no committed boundary (the
    in-flight active turn) replays as its stub. Raises ``CheckpointReplayUnavailable`` when
    coverage cannot be established (missing checkpoints, inconsistent
    pairing); the endpoint's ``auto`` mode falls back to stubs on that
    signal. Steered threads replay natively (the steering message is
    checkpointed mid-slice). A completed turn with no boundary (a legacy
    steering-*backfilled* turn) raises too, unless ``turn_fallback``, which
    replays that turn alone as its stub.

    With ``budget``, the page stops short of ``limit`` once its lines pass
    the budget (``_fill_page``), stubs counted with projected turns.

    Lines are wire-ready but not final: task cards and offloaded widget data
    are stamped on the way out (``finish_lines``). ``turn_fallback``: see
    ``cold.project_stale``.
    """
    reader = CheckpointHistoryReader.get_instance()
    branch = await _branch(reader, rows, branch_tip_checkpoint_id)
    if branch is None:
        raise CheckpointReplayUnavailable("no checkpoint turns found")
    inputs, anchored, tip_id = branch
    policy = cold.Policy(turn_fallback=turn_fallback)

    lost: list[Any] = []

    async def read(turns: list[tuple[Any, TurnAnchor | None]]) -> dict[Any, list[Line]]:
        """Projected lines for anchored turns, stubs for the rest, so the
        budget counts every turn the page sends."""
        projected = [(ti, a) for ti, a in turns if a is not None]
        stubbed = [ti for ti, a in turns if a is None]
        if not stubbed:
            return await _turn_lines(reader, inputs, anchored, projected, policy)
        lines, (stubs, gone) = await asyncio.gather(
            _turn_lines(reader, inputs, anchored, projected, policy),
            stub_lines(
                rows.thread_id,
                stubbed,
                inputs.queries_by_turn,
                inputs.responses_by_turn,
                rows.open_runs,
            ),
        )
        lost.extend(gone)
        return lines | stubs

    async def page_lines() -> tuple[
        list[tuple[Any, TurnAnchor | None]], bool, dict[Any, list[Line]]
    ]:
        candidates, has_more = _select_page(anchored, inputs, before_turn, limit)
        page, lines_by_turn = await _fill_page(
            read, candidates, budget, whole=not has_more
        )
        _check_unanchored(inputs, page, turn_fallback)
        return page, has_more or len(page) < len(candidates), lines_by_turn

    tip_interrupts: list[dict[str, Any]] = []
    if before_turn is None:
        # The newest page ends with the tip's pending interrupts, a checkpoint
        # read the lines never wait on. Both settle before either error is
        # raised, the lines' first, so a failed page leaves no read behind.
        results = await asyncio.gather(
            page_lines(),
            reader.aget_tip_interrupts(rows.thread_id, tip_id),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        (page, has_more, lines_by_turn), tip_interrupts = results
    else:
        page, has_more, lines_by_turn = await page_lines()
    on_page = {turn_index for turn_index, _ in page}
    lost = [turn_index for turn_index in lost if turn_index in on_page]
    if lost:
        logger.error(
            "[REPLAY] turns %s of %s have no checkpoint boundary and archived "
            "no stream; they replay without their answer",
            lost,
            rows.thread_id,
        )
    out = [line for turn_index, _ in page for line in lines_by_turn[turn_index]]
    for interrupt in tip_interrupts:
        out.append(line_of(items._interrupt_item(rows.thread_id, interrupt)))
    return ReplayPage(
        lines=out,
        first_turn_index=page[0][0] if page else None,
        has_more=has_more,
    )


async def read_replay_page(
    rows: ThreadRows,
    *,
    source: Literal["auto", "checkpoint"] = "auto",
    before_turn: Any | None = None,
    limit: int | None = None,
    budget: PageBudget | None = None,
) -> tuple[ReplayPage, str]:
    """The page a thread replays as, and its source (``checkpoint`` or ``stub``).

    One assembly for every reader that shows a thread, so the owner and a
    share viewer see the same turns. ``auto`` projects from checkpoints and
    replays only a turn that cannot be projected as its stub; the whole page
    goes to stubs only when the thread's turns cannot be paired at all.
    ``checkpoint`` raises ``CheckpointReplayUnavailable`` rather than fall
    back.
    """
    cause: BaseException | None = None
    try:
        if rows.thread.get("latest_checkpoint_id") is None:
            # The commit pointer (stamped at turn persist) is the only tip
            # checkpoint replay may read: without it the reader would walk
            # the newest checkpoint, which mid-run is uncommitted state.
            raise CheckpointReplayUnavailable(
                "thread has no committed checkpoint pointer"
            )
        page = await build_replay_page(
            rows,
            rows.thread.get("latest_checkpoint_id"),
            before_turn=before_turn,
            limit=limit,
            budget=budget,
            turn_fallback=source == "auto",
        )
        return page, "checkpoint"
    except DATABASE_UNAVAILABLE:
        # A database that is busy or unreachable, either pool, is not a thread
        # checkpoints cannot cover: stubs would show every answer missing, so
        # the reader is told to retry instead.
        raise
    except CheckpointReplayUnavailable as e:
        if source == "checkpoint":
            raise
        cause = e
        logger.info(
            "[REPLAY] Checkpoint replay unavailable for %s, falling back to stubs: %s",
            rows.thread_id,
            e,
        )
    except Exception as e:
        if source == "checkpoint":
            raise
        cause = e
        logger.warning(
            "[REPLAY] Checkpoint replay failed for %s, falling back to stubs",
            rows.thread_id,
            exc_info=True,
        )
    return await stub_page(rows, before_turn, limit, budget, cause=cause), "stub"


async def stub_page(
    rows: ThreadRows,
    before_turn: Any | None = None,
    limit: int | None = None,
    budget: PageBudget | None = None,
    *,
    cause: BaseException | None = None,
) -> ReplayPage:
    """A page of stubs (``stubs.stub_lines``), over the same window the
    checkpoint path pages by, counted in persisted turns and held to the same
    budget.

    Turns that answered and archived nothing are logged as an error with
    ``cause``, the failure that sent the read here: a thread whose turns
    cannot pair is logged only at info, as older threads legitimately are."""
    queries_by_turn = group_queries(rows.queries)
    turns = sorted(
        ti
        for ti in queries_by_turn
        if ti is not None and (before_turn is None or ti < before_turn)
    )
    candidates = [(ti, None) for ti in (turns[-limit:] if limit is not None else turns)]
    lost: list[Any] = []

    async def read(selected: list[tuple[Any, None]]) -> dict[Any, list[Line]]:
        lines, gone = await stub_lines(
            rows.thread_id,
            [ti for ti, _ in selected],
            queries_by_turn,
            rows.responses_by_turn,
            rows.open_runs,
        )
        lost.extend(gone)
        return lines

    page, lines = await _fill_page(
        read, candidates, budget, whole=len(candidates) == len(turns)
    )
    page_turns = [ti for ti, _ in page]
    on_page = set(page_turns)
    lost = [ti for ti in lost if ti in on_page]
    if lost:
        logger.error(
            "[REPLAY] %s replays as stubs, and turns %s archived no stream; "
            "they replay without their answer",
            rows.thread_id,
            lost,
            exc_info=cause,
        )
    return ReplayPage(
        lines=[line for ti in page_turns for line in lines[ti]],
        first_turn_index=page_turns[0] if page_turns else None,
        has_more=len(page_turns) < len(turns),
    )


async def finish_lines(
    thread_id: str, lines: list[Line], *, status_only: bool = False
) -> None:
    """Stamp what only read time knows onto flagged lines, in place: a task
    card's status from current liveness, a widget's offloaded data. Every
    other line streams as stored. ``status_only`` leaves a failed task's
    reason off, as ``stamp_task_artifact_data`` does for a share viewer."""
    flagged = [i for i, line in enumerate(lines) if line.flags]
    if not flagged:
        return
    decoded = [lines[i].item() for i in flagged]
    await widgets._resolve_widget_data_refs(
        [
            item
            for i, item in zip(flagged, decoded)
            if replay_lines.FLAG_WIDGET in lines[i].flags
        ]
    )
    await task_status.stamp_replay_task_status(
        thread_id, decoded, status_only=status_only
    )
    for i, item in zip(flagged, decoded):
        lines[i] = line_of(item)


async def project_turns(
    rows: ThreadRows,
    branch_tip_checkpoint_id: str,
    *,
    turn_indexes: list[Any] | None = None,
    last_n_turns: int | None = None,
    claims_pass: bool = True,
) -> dict[Any, list[Line]]:
    """Bring the given turns' stored lines up to date (or the newest
    ``last_n_turns``), so the next read streams them without projecting.
    Returns the lines of every selected turn with a checkpoint boundary.

    Without ``claims_pass``, turns that need the thread's run launches placed
    raise ``ClaimsPassNeeded`` rather than read the whole thread for them."""
    reader = CheckpointHistoryReader.get_instance()
    branch = await _branch(reader, rows, branch_tip_checkpoint_id)
    if branch is None:
        return {}
    inputs, anchored, _tip = branch
    wanted = set(turn_indexes or ())
    selected = [
        (ti, a)
        for k, (ti, a) in enumerate(anchored)
        if ti in wanted
        or (last_n_turns is not None and k >= len(anchored) - last_n_turns)
    ]
    return await _turn_lines(
        reader,
        inputs,
        anchored,
        selected,
        cold.Policy(claims_pass=claims_pass),
    )


async def unserved_turns(
    rows: ThreadRows,
    branch_tip_checkpoint_id: str,
    *,
    reader: CheckpointHistoryReader | None = None,
) -> list[Any] | None:
    """The branch's turns a read would project rather than stream from their
    stored slice and lines, or None when the branch has no turns.

    Checkpoint storage maintenance reads this to know a thread's history no
    longer depends on its old checkpoint states. Raises
    ``CheckpointReplayUnavailable`` when the turns cannot be paired.
    """
    reader = reader or CheckpointHistoryReader.get_instance()
    branch = await _branch(reader, rows, branch_tip_checkpoint_id)
    if branch is None:
        return None
    inputs, anchored, _tip = branch
    _, stale = await _stored_lines(inputs, anchored)
    return [ti for ti, _ in stale]


# ---------------------------------------------------------------- pairing


def pair_anchors(
    anchors: list[TurnAnchor], turn_indexes: list[Any]
) -> list[tuple[Any, TurnAnchor]]:
    """Pair checkpoint turns with persisted turn_indexes (``pair_turns``).

    Raises on anything a projection could silently mislabel: a stamped index
    missing from the rows, a turn left without one, or non-monotonic pairing.
    """
    known = set(turn_indexes)
    pairs: list[tuple[Any, TurnAnchor]] = []
    for anchor, ti in zip(anchors, pair_turns(anchors, turn_indexes)):
        if ti is None:
            raise CheckpointReplayUnavailable(
                "more checkpoint turns than persisted turns"
            )
        if ti not in known:
            raise CheckpointReplayUnavailable(
                f"checkpoint turn_index {ti} has no persisted turn"
            )
        pairs.append((ti, anchor))

    paired = [ti for ti, _ in pairs]
    if paired != sorted(set(paired)):
        raise CheckpointReplayUnavailable("turn pairing is not monotonic")
    return pairs


def _select_page(
    anchored: list[tuple[Any, TurnAnchor]],
    inputs: Inputs,
    before_turn: Any | None,
    limit: int | None,
) -> tuple[list[tuple[Any, TurnAnchor | None]], bool]:
    """The page's ``(turn_index, anchor | None)`` pairs in order, and whether
    older turns remain.

    A ``None`` anchor is a persisted turn with no committed boundary (the
    in-flight active turn, or a run that never checkpointed), replayed as its
    stub. One older than the page's oldest checkpointed turn belongs to an
    earlier page.
    """
    in_range = [
        (ti, a) for ti, a in anchored if before_turn is None or ti < before_turn
    ]
    has_more = limit is not None and len(in_range) > limit
    chosen: list[tuple[Any, TurnAnchor | None]] = list(
        in_range[-limit:] if has_more else in_range
    )
    oldest = chosen[0][0] if has_more else None

    paired = {ti for ti, _ in anchored}
    for ti in inputs.turn_indexes:
        if ti in paired:
            continue
        if oldest is not None and ti < oldest:
            continue
        if before_turn is not None and ti >= before_turn:
            continue
        chosen.append((ti, None))
    chosen.sort(key=lambda p: p[0])
    return chosen, has_more


def _check_unanchored(
    inputs: Inputs,
    page: list[tuple[Any, TurnAnchor | None]],
    turn_fallback: bool,
) -> None:
    """A completed turn persists its boundary, so checkpoints cannot cover
    one without it. The pairing that placed every other turn held, so under
    ``turn_fallback`` only that turn is a stub."""
    for turn_index, anchor in page:
        response = inputs.responses_by_turn.get(turn_index) or {}
        if anchor is None and response.get("status") == "completed" and not turn_fallback:
            raise CheckpointReplayUnavailable(
                f"persisted turn {turn_index} completed but has no checkpoint boundary"
            )


def _wire_bytes(lines: list[Line]) -> int:
    return sum(_line_bytes(ln) for ln in lines)


def _line_bytes(line: Line) -> int:
    """The line's bytes as sent: a widget whose data sits in object storage
    goes out with it inlined (``finish_lines``), and that data is the large
    part, so its stored size counts too."""
    size = len(line.event) + len(line.data_json.encode())
    if replay_lines.FLAG_WIDGET in line.flags:
        ref = (json.loads(line.data_json).get("payload") or {}).get("data_ref") or {}
        if isinstance(ref.get("size"), int):
            size += ref["size"]
    return size


Candidate = tuple[Any, TurnAnchor | None]
ReadLines = Callable[[list[Candidate]], Awaitable[dict[Any, list[Line]]]]


async def _fill_page(
    read: ReadLines,
    candidates: list[Candidate],
    budget: PageBudget | None,
    *,
    whole: bool,
) -> tuple[list[Candidate], dict[Any, list[Line]]]:
    """The page, the newest of ``candidates`` the budget holds (all of them
    without one), and its lines. ``whole``: the candidates are every turn in
    range, none older left behind."""
    count, got = len(candidates), {}
    if budget is not None and candidates:
        count, got = await _budgeted_lines(read, candidates, budget, whole=whole)
    page = candidates[len(candidates) - count :]
    return page, got | await read([p for p in page if p[0] not in got])


async def _budgeted_lines(
    read: ReadLines,
    candidates: list[Candidate],
    budget: PageBudget,
    *,
    whole: bool,
) -> tuple[int, dict[Any, list[Line]]]:
    """How many of the newest candidates the page takes, and the lines read
    on the way.

    A turn's size is known only once its lines are read or projected, so
    turns are read newest first in batches, each sized from the bytes per
    turn of the batch before: a cold thread projects about the turns it
    sends, not ``limit`` of them, and one heavy newest turn does not shrink
    every later batch to a single turn. Whole turns only, at least
    ``min_turns``. A cut that would leave fewer than ``min_turns`` behind
    takes them too, unless they alone pass the budget: a straggler page too
    short to fill the screen it lands on leaves a gap above the transcript,
    but a second budget's worth on the first page costs every open of the
    thread.
    """
    n = len(candidates)
    floor = min(budget.min_turns, n)
    got: dict[Any, list[Line]] = {}
    taken = total = 0
    want = floor
    while taken < n:
        batch = candidates[n - want : n - taken]
        got |= await read(batch)
        batch_bytes = 0
        for ti, _ in reversed(batch):
            size = _wire_bytes(got[ti])
            if taken >= floor and total + size > budget.max_bytes:
                return await _take_small_remainder(candidates, taken, budget, got, read, whole)
            total += size
            batch_bytes += size
            taken += 1
        room = budget.max_bytes - total
        if taken < n and taken >= floor and room <= 0:
            return await _take_small_remainder(candidates, taken, budget, got, read, whole)
        want = min(n, taken + max(1, room // max(batch_bytes // len(batch), 1)))
    return taken, got


async def _take_small_remainder(
    candidates: list[Candidate],
    taken: int,
    budget: PageBudget,
    got: dict[Any, list[Line]],
    read: ReadLines,
    whole: bool,
) -> tuple[int, dict[Any, list[Line]]]:
    """The page cut at ``taken``, or every candidate when they are the whole
    range and what the cut would leave behind is fewer than ``min_turns`` and
    fits the budget on its own."""
    left = candidates[: len(candidates) - taken]
    if not whole or not 0 < len(left) < budget.min_turns:
        return taken, got
    got |= await read([p for p in left if p[0] not in got])
    if sum(_wire_bytes(got[ti]) for ti, _ in left) > budget.max_bytes:
        return taken, got
    return len(candidates), got


async def _branch(
    reader: CheckpointHistoryReader,
    rows: ThreadRows,
    branch_tip_checkpoint_id: str | None,
) -> tuple[Inputs, list[tuple[Any, TurnAnchor]], str] | None:
    """The rows indexed, the branch's checkpoint turns paired with them, and
    the tip the branch was read from; None when it has no turns, which each
    entry point answers its own way."""
    try:
        anchors, tip_id = await reader.aget_turn_anchors(
            rows.thread_id, branch_tip_checkpoint_id
        )
    except CheckpointBranchTipNotFound as e:
        raise CheckpointReplayUnavailable(str(e)) from e
    if not anchors or tip_id is None:
        return None
    inputs = Inputs.of(rows)
    return inputs, pair_anchors(anchors, inputs.turn_indexes), tip_id


# ---------------------------------------------------------------- lines


async def _turn_lines(
    reader: CheckpointHistoryReader,
    inputs: Inputs,
    anchored: list[tuple[Any, TurnAnchor]],
    selected: list[tuple[Any, TurnAnchor]],
    policy: cold.Policy,
) -> dict[Any, list[Line]]:
    """Lines for the selected turns: stored ones whose key still matches,
    the rest projected (and stored when final)."""
    if not selected:
        return {}
    out, stale = await _stored_lines(inputs, selected)
    if stale:
        out.update(await cold.project_stale(reader, inputs, anchored, stale, policy))
    return out


async def _stored_lines(
    inputs: Inputs,
    selected: list[tuple[Any, TurnAnchor]],
) -> tuple[dict[Any, list[Line]], list[tuple[Any, TurnAnchor]]]:
    """The selected turns' stored lines a read serves as they are, and the
    stale turns it has to project."""
    response_ids = [rid for ti, _ in selected if (rid := inputs.response_id(ti))]
    stored = await slices_db.get_turn_lines(response_ids)
    out: dict[Any, list[Line]] = {}
    stale: list[tuple[Any, TurnAnchor]] = []
    for ti, anchor in selected:
        row = stored.get(inputs.response_id(ti) or "")
        if (
            slices.matches(row, anchor)
            and row.lines is not None
            and row.lines_key
            == lines_key(inputs, ti, anchor, inputs.responses_by_turn.get(ti))
        ):
            try:
                out[ti] = replay_lines.decode(row.lines)
                continue
            except Exception:
                # Projected again and stored over, rather than failing every
                # read of this thread on one bad row.
                logger.warning(
                    "[REPLAY] stored lines undecodable for %s turn %s",
                    inputs.thread_id,
                    ti,
                    exc_info=True,
                )
        stale.append((ti, anchor))
    return out, stale
