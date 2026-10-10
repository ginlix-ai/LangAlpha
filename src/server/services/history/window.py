"""The messages a window trimmed, read back from the turn slices.

The main agent's checkpoint keeps only the transcript runs it can still see
(``compaction.window``). The messages it drops stay readable in two places:
the checkpoints written before the trim, and the turn slices replay and the
transcripts read. The agent asks here before each trim, so a thread is
trimmed only once its stored slices, read in branch order, start with exactly
the messages the trim drops, and replay of the trimmed turns never falls back
to decoding the checkpoints that still hold them.

The check is by identity (``Window``), not by counting turns or runs: a turn
can open no run (an input killed before it ran) or two (two user messages in
one request), a request can open with an assistant message that belongs to
the run before it, and a thread compacted by the old summarizer holds a
summary where its slices hold the messages it replaced. A count misreads
each; a thread whose slices do not match is simply not trimmed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from langchain_core.messages import AnyMessage

from ptc_agent.agent.transcript import Window
from src.server.database.conversation import turn_slices as slices_db
from src.server.database.conversation.turn_slices import StoredSlice
from src.server.database.conversation.threads_read import get_thread_checkpoint_id
from src.server.services.history import slices
from src.server.services.history.reader import CheckpointHistoryReader, TurnAnchor
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

logger = logging.getLogger(__name__)

#: Turns whose stored slices one query reads.
_BATCH = 32


async def _anchors(thread_id: str, tip: str | None) -> list[TurnAnchor] | None:
    try:
        anchors, _ = await CheckpointHistoryReader.get_instance().aget_turn_anchors(
            thread_id, tip
        )
    except CheckpointBranchTipNotFound:
        logger.debug("window: tip %s of %s is not on the graph", tip, thread_id)
        return None
    return anchors


def _decoded(
    serde: Any,
    rows: Mapping[tuple[str, str | None], StoredSlice],
    turns: list[TurnAnchor],
    need: int,
) -> list[list[AnyMessage] | None]:
    """Each turn's messages from its stored row, in order, until ``need`` are
    in hand; None for a turn whose row replay would not serve either
    (``slices.matches``, ``slices.decoded``). CPU only."""
    out: list[list[AnyMessage] | None] = []
    have = 0
    for anchor in turns:
        if have >= need:
            break
        row = rows.get((anchor.input_checkpoint_id, anchor.tail_checkpoint_id))
        span = slices.decoded(serde, row) if slices.matches(row, anchor) else None
        messages = span.messages if span is not None else None
        out.append(messages)
        have += len(messages or ())
    return out


async def _head(
    thread_id: str, turns: list[TurnAnchor], need: int, *, extract: bool
) -> list[AnyMessage] | None:
    """The first ``need`` messages ``turns`` added, in order: from their
    stored slices, and with ``extract`` from the checkpoints for a turn no
    stored slice serves. None when the turns hold fewer, or without
    ``extract`` once one has no stored slice to read."""
    reader = CheckpointHistoryReader.get_instance()
    out: list[AnyMessage] = []
    for start in range(0, len(turns), _BATCH):
        if len(out) >= need:
            break
        batch = turns[start : start + _BATCH]
        rows = {
            (row.input_checkpoint_id, row.tail_checkpoint_id): row
            for row in await slices_db.get_slices_at(
                thread_id, slices.SLICE_KEY, [a.input_checkpoint_id for a in batch]
            )
        }
        by_turn = await asyncio.to_thread(
            _decoded, reader.serde, rows, batch, need - len(out)
        )
        missing = [i for i, messages in enumerate(by_turn) if messages is None]
        if missing and not extract:
            return None
        if missing:
            extracted = await reader.aget_turn_slices(
                thread_id, [batch[i] for i in missing]
            )
            for i, turn in zip(missing, extracted):
                by_turn[i] = turn.messages
        out.extend(m for messages in by_turn for m in messages or ())
    return out[:need] if len(out) >= need else None


async def _read_head(
    thread_id: str, tip: str | None, head: Window, *, extract: bool
) -> list[AnyMessage] | None:
    """``head``'s messages on the branch ending at ``tip``, read as ``_head``
    reads them, or None when that branch's turns do not start with exactly
    them."""
    if not head.messages:
        return []
    anchors = await _anchors(thread_id, tip)
    messages = await _head(thread_id, anchors or [], head.messages, extract=extract)
    if messages is not None and await asyncio.to_thread(head.holds, messages):
        return messages
    # Only a read back is owed the messages; a check that misses leaves the
    # thread untrimmed, as it was.
    logger.log(
        logging.WARNING if extract else logging.INFO,
        "window: %s's turns at %s do not start with its %d trimmed messages",
        thread_id,
        tip,
        head.messages,
    )
    return None


async def runs_held(thread_id: str, head: Window) -> bool:
    """Whether the stored slices of the committed branch's turns, read in
    order, start with exactly ``head``'s messages: every slice up to there is
    one replay would serve, and together they hash to its digest.

    One indexed read settles the common miss (a thread not yet backfilled
    holds no slices); only a thread that may be covered pays for the branch
    walk, which reads checkpoint metadata, never state, and then for decoding
    the slices a trim would leave as those messages' only copy.
    """
    if not head.messages:
        return True
    if not await slices_db.has_turn_slices(thread_id, slices.SLICE_KEY):
        return False
    tip = await get_thread_checkpoint_id(thread_id)
    return bool(tip) and await _read_head(thread_id, tip, head, extract=False) is not None


async def earlier_runs(
    thread_id: str, tip: str | None, head: Window
) -> list[AnyMessage] | None:
    """``head``'s messages, the ones the window trimmed, on the branch ending
    at ``tip`` (the newest checkpoint without one): from the turns' stored
    slices, and from the checkpoints for a turn whose slice replay would not
    serve. None when that branch's turns do not start with exactly them.
    """
    return await _read_head(thread_id, tip, head, extract=True)
