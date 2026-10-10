"""What a settled turn's main agent said, for a tool reading another thread.

The secretary's ``agent_output`` reads a thread's newest turns while its own
turn waits. A turn's stored slice serves it while the slice is the turn's on
the current branch, by the rule replay applies (``slices.stored_spans``);
only a turn without one waits for its projection. Replay's stored lines would
serve the same text, but checking them reads every row of the thread, which
costs this path several times what the slice read does.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from src.server.database.conversation import turn_slices as slices_db
from src.server.database.conversation.replay_rows import get_branch_pointer
from src.server.services.history import slices
from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.reader import CheckpointHistoryReader, TurnAnchor
from src.server.services.history.replay.errors import CheckpointReplayUnavailable
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

logger = logging.getLogger(__name__)


async def committed_texts(thread_id: str, turns: Mapping[str, Any]) -> dict[str, str]:
    """The main agent's text in each of ``turns`` (response id to turn
    index), cut from the turn's stored slice, which a turn without one has
    projected first, as its settle would have. A response left out has no
    slice to read (no checkpoint boundary, a turn waiting on the thread's
    claims pass, a failed read), so its stored events are the caller's to
    read."""
    from src.server.services.history.replay.refresh import refresh_thread

    anchors = await _anchors(thread_id, turns)
    texts = await _slice_texts(thread_id, anchors)
    missing = {rid for rid in anchors if rid not in texts}
    if missing:
        await refresh_thread(thread_id, missing, claims_pass=False)
        texts.update(
            await _slice_texts(
                thread_id, {rid: anchors[rid] for rid in missing}
            )
        )
    return texts


async def _anchors(
    thread_id: str, turns: Mapping[str, Any]
) -> dict[str, TurnAnchor]:
    """Each response's turn on the current branch, paired as replay pairs
    them; none when the branch cannot be read or paired."""
    from src.server.services.history.replay import pair_anchors

    try:
        pointer = await get_branch_pointer(thread_id)
        if pointer is None:
            return {}
        tip, persisted = pointer
        anchors, _ = await CheckpointHistoryReader.get_instance().aget_turn_anchors(
            thread_id, tip
        )
        paired = dict(pair_anchors(anchors, persisted))
    except (CheckpointBranchTipNotFound, CheckpointReplayUnavailable) as e:
        logger.debug("no branch to read for %s: %s", thread_id, e)
        return {}
    except Exception:
        logger.warning(
            "branch read failed for %s; using stored events", thread_id, exc_info=True
        )
        return {}
    return {rid: paired[ti] for rid, ti in turns.items() if ti in paired}


async def _slice_texts(
    thread_id: str, anchors: Mapping[str, TurnAnchor]
) -> dict[str, str]:
    """Main-lane text by response id, for the turns with a usable slice."""
    if not anchors:
        return {}
    try:
        serde = CheckpointHistoryReader.get_instance().serde
        rows = await slices_db.get_turn_slices(list(anchors))
    except Exception:
        logger.warning("turn slice read failed; using stored events", exc_info=True)
        return {}
    out: dict[str, str] = {}
    for response_id, span in slices.stored_spans(serde, anchors, rows).items():
        try:
            items = history_events_to_sse(
                messages_to_history_events(span.messages), thread_id=thread_id
            )
        except Exception:
            logger.warning(
                "turn slice projection failed for response %s; using stored events",
                response_id,
                exc_info=True,
            )
            continue
        out[response_id] = "".join(
            d["content"]
            for i in items
            if i.get("event") == "message_chunk"
            and isinstance(d := i.get("data"), dict)
            and d.get("content_type") == "text"
            and isinstance(d.get("content"), str)
            and d["content"]
        )
    return out
