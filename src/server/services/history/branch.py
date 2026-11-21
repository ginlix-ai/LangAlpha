"""The turns on a thread's current branch, as edit, regenerate and a fork
name them: by the ``turn_index`` the chat shows, each with the checkpoint an
edit or a regenerate of it starts from."""

import logging
from typing import NamedTuple

from fastapi import HTTPException

from src.server.database.conversation import queries as queries_db
from src.server.models.workflow import TurnCheckpointInfo
from src.server.services.history.reader import pair_turns, turn_anchors
from src.server.utils.checkpoint_helpers import (
    FORK_ROOT_CHECKPOINT_ID,
    get_checkpointer,
    walk_current_branch_boundaries,
)

logger = logging.getLogger(__name__)


class BranchTurns(NamedTuple):
    turns: list[TurnCheckpointInfo]
    tip_id: str | None
    # The last entry of ``turns`` when it stands for turns past the branch's
    # last boundary, which never reached the graph.
    unstarted: TurnCheckpointInfo | None


async def branch_turns(
    thread_id: str, branch_tip_checkpoint_id: str | None
) -> BranchTurns:
    try:
        boundaries, tip_id = await walk_current_branch_boundaries(
            get_checkpointer(), thread_id, branch_tip_checkpoint_id
        )
        persisted = await queries_db.get_query_turn_indexes(thread_id)
    except Exception as e:
        logger.error(f"[CHECKPOINT] Failed to list checkpoints for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve checkpoint history")

    anchors = turn_anchors(boundaries, tip_id) if tip_id else []
    numbers = pair_turns(anchors, persisted)
    turns = [
        TurnCheckpointInfo(
            turn_index=turn_index,
            # The thread's first input builds on no saved checkpoint.
            edit_checkpoint_id=(
                None
                if anchor.is_resume
                else anchor.parent_checkpoint_id or FORK_ROOT_CHECKPOINT_ID
            ),
            regenerate_checkpoint_id=anchor.regenerate_checkpoint_id,
        )
        for anchor, turn_index in zip(anchors, numbers)
        if turn_index is not None
    ]
    # A run that failed before its first step, an edit's included, leaves its
    # turn's rows past the branch's last boundary and nothing on the branch.
    # Its message forks from the tip, the state before it, and so does any
    # such turn before it, so one entry under the last turn number serves
    # them all, the way the next checkpointed turn serves one mid-thread. With
    # no checkpoint at all, the state before them is the empty fork root.
    last_kept = max((n for n in numbers if n is not None), default=None)
    past = [n for n in persisted if last_kept is None or n > last_kept]
    unstarted = None
    if past:
        base = tip_id or FORK_ROOT_CHECKPOINT_ID
        unstarted = TurnCheckpointInfo(
            turn_index=past[-1],
            edit_checkpoint_id=base,
            regenerate_checkpoint_id=base,
        )
        turns.append(unstarted)
    return BranchTurns(turns, tip_id, unstarted)


def stale_fork_detail() -> dict:
    """The 409 detail for a fork whose branch is no longer the thread's.

    One wording for every refusal: the route's, when the checkpoint forks no
    turn of the current branch or the client's view missed a newer run, and
    START's, when another worker moved the branch after that check.
    """
    return {
        "code": "stale_fork",
        "message": "This chat changed since it was loaded. Reload it and try again.",
    }
