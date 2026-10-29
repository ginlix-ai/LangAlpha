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
import logging
from dataclasses import dataclass
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

    Lines are wire-ready but not final: task cards and offloaded widget data
    are stamped on the way out (``finish_lines``). ``turn_fallback``: see
    ``cold.project_stale``.
    """
    reader = CheckpointHistoryReader.get_instance()
    branch = await _branch(reader, rows, branch_tip_checkpoint_id)
    if branch is None:
        raise CheckpointReplayUnavailable("no checkpoint turns found")
    inputs, anchored, tip_id = branch
    page, has_more = _select_page(anchored, inputs, before_turn, limit)
    for turn_index, anchor in page:
        # A completed turn persists its boundary, so checkpoints cannot cover
        # one without it. The pairing that placed every other turn held, so
        # under ``turn_fallback`` only that turn is a stub.
        response = inputs.responses_by_turn.get(turn_index) or {}
        if anchor is None and response.get("status") == "completed" and not turn_fallback:
            raise CheckpointReplayUnavailable(
                f"persisted turn {turn_index} completed but has no checkpoint boundary"
            )

    lines = _turn_lines(
        reader,
        inputs,
        anchored,
        [(ti, a) for ti, a in page if a is not None],
        cold.Policy(turn_fallback=turn_fallback),
    )
    tip_interrupts: list[dict[str, Any]] = []
    if before_turn is None:
        # The newest page ends with the tip's pending interrupts, a checkpoint
        # read the lines never wait on. Both settle before either error is
        # raised, the lines' first, so a failed page leaves no read behind.
        results = await asyncio.gather(
            lines,
            reader.aget_tip_interrupts(rows.thread_id, tip_id),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        lines_by_turn, tip_interrupts = results
    else:
        lines_by_turn = await lines
    stubs, lost = await stub_lines(
        rows.thread_id,
        [turn_index for turn_index, anchor in page if anchor is None],
        inputs.queries_by_turn,
        inputs.responses_by_turn,
        rows.open_runs,
    )
    if lost:
        logger.error(
            "[REPLAY] turns %s of %s have no checkpoint boundary and archived "
            "no stream; they replay without their answer",
            lost,
            rows.thread_id,
        )
    out: list[Line] = []
    for turn_index, anchor in page:
        out.extend(stubs[turn_index] if anchor is None else lines_by_turn[turn_index])
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
    return await stub_page(rows, before_turn, limit, cause=cause), "stub"


async def stub_page(
    rows: ThreadRows,
    before_turn: Any | None = None,
    limit: int | None = None,
    *,
    cause: BaseException | None = None,
) -> ReplayPage:
    """A page of stubs (``stubs.stub_lines``), over the same window the
    checkpoint path pages by, counted in persisted turns.

    Turns that answered and archived nothing are logged as an error with
    ``cause``, the failure that sent the read here: a thread whose turns
    cannot pair is logged only at info, as older threads legitimately are."""
    queries_by_turn = group_queries(rows.queries)
    turns = sorted(
        ti
        for ti in queries_by_turn
        if ti is not None and (before_turn is None or ti < before_turn)
    )
    page_turns = turns[-limit:] if limit is not None else turns
    lines, lost = await stub_lines(
        rows.thread_id,
        page_turns,
        queries_by_turn,
        rows.responses_by_turn,
        rows.open_runs,
    )
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
