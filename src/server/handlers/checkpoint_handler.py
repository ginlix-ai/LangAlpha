"""
Checkpoint Handler — Business logic for checkpoint history and thread turn operations.

Provides endpoints for:
- Listing turn-boundary checkpoints (for edit/regenerate/retry)
- Naming the turn an edit/regenerate replaces, from the checkpoint it forks at
- Retrying failed/interrupted threads from the appropriate checkpoint
"""

import logging
from bisect import bisect_right
from typing import Any

from fastapi import HTTPException

from src.server.database.conversation import queries as queries_db
from src.server.database.conversation import threads_read
from src.server.utils.checkpoint_helpers import (
    build_checkpoint_config,
    get_checkpointer,
    walk_current_branch_boundaries,
)
from src.server.models.workflow import (
    TurnCheckpointInfo,
    ThreadTurnsResponse,
)

logger = logging.getLogger(__name__)


def _turn_numbers(boundaries: list[Any], persisted: list[int]) -> list[int | None]:
    """Each boundary's persisted turn_index.

    A turn's input checkpoint carries the turn_index its run was admitted
    under. A resume's checkpoint belongs to the run it answers, and threads
    from before the stamp carry none, so those take the next persisted turn
    after the one before: wherever replay's pairing holds, this is the turn it
    names. None when no persisted turn is left to name.
    """
    numbers: list[int | None] = []
    previous: int | None = None
    for cp_tuple in boundaries:
        metadata = cp_tuple.metadata or {}
        number = metadata.get("turn_index") if metadata.get("source") == "input" else None
        if number is None:
            at = 0 if previous is None else bisect_right(persisted, previous)
            number = persisted[at] if at < len(persisted) else None
        numbers.append(number)
        if number is not None:
            previous = number
    return numbers


async def get_thread_turns(
    thread_id: str, branch_tip_checkpoint_id: str | None = None
) -> ThreadTurnsResponse:
    """Turn boundaries on the thread's current branch, for edit/regenerate.

    Edit/regenerate fork the checkpoint graph, so only ancestors of the branch
    tip count (tip: ``branch_tip_checkpoint_id`` when on the graph, else the
    newest checkpoint). Each turn is named by its persisted ``turn_index``,
    the number its rows and replayed bubbles carry, never by its position
    among the boundaries: a turn whose run died before its first checkpoint
    has rows and no boundary, so every later turn sits a place lower than its
    number. A boundary with no persisted turn to name is left out.
    """
    try:
        boundaries, tip_id = await walk_current_branch_boundaries(
            get_checkpointer(), thread_id, branch_tip_checkpoint_id
        )
        persisted = (
            await queries_db.get_query_turn_indexes(thread_id) if boundaries else []
        )
    except Exception as e:
        logger.error(f"[CHECKPOINT] Failed to list checkpoints for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve checkpoint history")

    turns = []
    for cp_tuple, turn_index in zip(boundaries, _turn_numbers(boundaries, persisted)):
        if turn_index is None:
            continue
        cp_id = cp_tuple.config["configurable"]["checkpoint_id"]
        # The parent checkpoint is the state BEFORE this turn — only meaningful
        # for a source=input turn (a resume shares its interrupted turn's state).
        is_source_input = (cp_tuple.metadata or {}).get("source") == "input"
        edit_checkpoint_id = None
        if is_source_input and cp_tuple.parent_config:
            edit_checkpoint_id = cp_tuple.parent_config["configurable"].get("checkpoint_id")

        turns.append(TurnCheckpointInfo(
            turn_index=turn_index,
            edit_checkpoint_id=edit_checkpoint_id,
            regenerate_checkpoint_id=cp_id,
        ))

    return ThreadTurnsResponse(
        thread_id=thread_id,
        turns=turns,
        retry_checkpoint_id=tip_id,
    )


async def resolve_fork_turn(
    thread_id: str,
    checkpoint_id: str,
    *,
    regenerate: bool,
    requested: int | None = None,
) -> int:
    """The turn an edit or regenerate at ``checkpoint_id`` replaces.

    The fork deletes rows from this turn onward, so the turn is read off the
    current branch by the checkpoint it forks at (an edit at the parent of the
    turn's input checkpoint, a regenerate at the boundary itself), not taken
    from the client's count. An edit keeps the client's ``requested`` turn
    only when nothing but turns that never checkpointed lie between it and
    the forked one, so rewriting a failed message works and no kept turn is
    deleted. A checkpoint that forks no turn of the current branch is
    refused: the client read a branch that has since been replaced.
    """
    branch_tip = await threads_read.get_thread_checkpoint_id(thread_id)
    turns = await get_thread_turns(thread_id, branch_tip_checkpoint_id=branch_tip)
    kept: int | None = None
    for turn in turns.turns:
        forks_at = (
            turn.regenerate_checkpoint_id if regenerate else turn.edit_checkpoint_id
        )
        if forks_at == checkpoint_id:
            if (
                not regenerate
                and requested is not None
                and (kept is None or requested > kept)
                and requested <= turn.turn_index
            ):
                return requested
            return turn.turn_index
        kept = turn.turn_index if kept is None else max(kept, turn.turn_index)
    raise HTTPException(
        status_code=409,
        detail={
            "code": "stale_fork",
            "message": "That message is no longer on this thread's current "
            "branch. Reload the thread and try again.",
        },
    )


async def get_retry_checkpoint(thread_id: str, checkpoint_id: str | None = None) -> str:
    """
    Determine the appropriate checkpoint ID for retrying a failed/interrupted thread.

    If checkpoint_id is provided, validates it exists and returns it.
    Otherwise, auto-detects the latest checkpoint.

    Args:
        thread_id: Thread identifier
        checkpoint_id: Optional explicit checkpoint ID

    Returns:
        The checkpoint ID to retry from

    Raises:
        HTTPException: If no checkpoint is found
    """
    checkpointer = get_checkpointer()

    if checkpoint_id:
        # Validate the provided checkpoint exists
        config = build_checkpoint_config(thread_id, checkpoint_id)
        cp_tuple = await checkpointer.aget_tuple(config)
        if not cp_tuple:
            raise HTTPException(
                status_code=404,
                detail=f"Checkpoint {checkpoint_id} not found for thread {thread_id}",
            )
        return checkpoint_id

    # Auto-detect: get the latest checkpoint
    config = build_checkpoint_config(thread_id)
    cp_tuple = await checkpointer.aget_tuple(config)
    if not cp_tuple:
        raise HTTPException(
            status_code=404,
            detail=f"No checkpoints found for thread {thread_id}",
        )

    return cp_tuple.config["configurable"]["checkpoint_id"]
