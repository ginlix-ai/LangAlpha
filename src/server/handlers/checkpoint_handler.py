"""
Checkpoint Handler — Business logic for checkpoint history and thread turn operations.

Provides endpoints for:
- Listing turn-boundary checkpoints (for edit/regenerate/retry)
- Naming the turn an edit/regenerate replaces, from the checkpoint it forks at
- Retrying failed/interrupted threads from the appropriate checkpoint
"""

import logging
from typing import NamedTuple

from fastapi import HTTPException

from src.server.database.conversation import queries as queries_db
from src.server.database.conversation import threads_read
from src.server.services.history.reader import pair_turns, turn_anchors
from src.server.utils.checkpoint_helpers import (
    FORK_ROOT_CHECKPOINT_ID,
    build_checkpoint_config,
    checkpoint_on_branch,
    get_checkpointer,
    turn_base_checkpoint_id,
    walk_current_branch_boundaries,
)
from src.server.utils.pg_sanitize import normalize_uuid
from src.server.models.chat import ChatRequest
from src.server.models.workflow import (
    TurnCheckpointInfo,
    ThreadTurnsResponse,
)

logger = logging.getLogger(__name__)


class _BranchTurns(NamedTuple):
    turns: list[TurnCheckpointInfo]
    tip_id: str | None
    # The last entry of ``turns`` when it stands for turns past the branch's
    # last boundary, which never reached the graph.
    unstarted: TurnCheckpointInfo | None


async def _branch_turns(
    thread_id: str, branch_tip_checkpoint_id: str | None
) -> _BranchTurns:
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
    return _BranchTurns(turns, tip_id, unstarted)


async def get_thread_turns(
    thread_id: str, branch_tip_checkpoint_id: str | None = None
) -> ThreadTurnsResponse:
    """Turn boundaries on the thread's current branch, for edit/regenerate.

    Edit/regenerate fork the checkpoint graph, so only ancestors of the branch
    tip count (tip: ``branch_tip_checkpoint_id`` when on the graph, else the
    newest checkpoint). Each turn is named by its persisted ``turn_index``
    (``pair_turns``), the number its rows and replayed bubbles carry; a turn
    with no persisted turn to name is left out. Turns past the last boundary
    share one last entry at the tip, or at the empty fork root on a thread
    with no checkpoint, which is also where the first turn's edit forks.
    """
    listing = await _branch_turns(thread_id, branch_tip_checkpoint_id)
    return ThreadTurnsResponse(
        thread_id=thread_id,
        turns=listing.turns,
        retry_checkpoint_id=listing.tip_id,
    )


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


class ForkPoint(NamedTuple):
    """The turn a fork replaces, the branch tip it was read off, and the
    newest run's ``run_seq`` read with that tip. ``unstarted`` marks a turn
    that never reached the graph, whose input no checkpoint holds."""

    turn_index: int
    branch_tip: str | None
    run_horizon: int | None = None
    unstarted: bool = False


async def resolve_fork_turn(
    thread_id: str,
    checkpoint_id: str,
    *,
    regenerate: bool,
    requested: int | None = None,
) -> ForkPoint:
    """The turn an edit or regenerate at ``checkpoint_id`` replaces.

    The fork deletes rows from this turn onward, so the turn is read off the
    current branch by the checkpoint it forks at (an edit at the parent of the
    turn's input checkpoint, a regenerate at its ``regenerate_checkpoint_id``),
    not taken from the client's count. An edit keeps the client's
    ``requested`` turn only when nothing but turns that never checkpointed lie
    between it and the forked one, so rewriting a failed message works and no
    kept turn is deleted. A checkpoint that forks no turn of the current
    branch is refused: the client read a branch that has since been replaced.

    The answer holds only while the branch does, and this runs outside the
    transaction that admits the fork, so the tip it read travels with it, and
    the run horizon read with it: START refuses the fork once a run started
    after this read or the fork's checkpoint left the thread's branch.
    """
    branch = await threads_read.get_thread_branch_read(thread_id)
    branch_tip, horizon = branch.checkpoint_id, branch.run_horizon
    listing = await _branch_turns(thread_id, branch_tip)
    turns = listing.turns
    if (
        regenerate
        and listing.unstarted is not None
        and requested == listing.unstarted.turn_index
    ):
        # A last turn whose graph wrote no more than its input checkpoint ends
        # on it, so the unstarted turn after it forks from the same tip; the
        # client's turn decides between the two.
        turns = [listing.unstarted, *turns]
    kept: int | None = None
    for turn in turns:
        forks_at = (
            turn.regenerate_checkpoint_id if regenerate else turn.edit_checkpoint_id
        )
        if forks_at == checkpoint_id:
            unstarted = turn is listing.unstarted
            if (
                not regenerate
                and requested is not None
                and (kept is None or requested > kept)
                and requested <= turn.turn_index
            ):
                return ForkPoint(requested, branch_tip, horizon, unstarted)
            return ForkPoint(turn.turn_index, branch_tip, horizon, unstarted)
        kept = turn.turn_index if kept is None else max(kept, turn.turn_index)
    raise HTTPException(status_code=409, detail=stale_fork_detail())


def _is_run(run: dict | None, run_id: str) -> bool:
    expected = normalize_uuid(run_id)
    return (
        run is not None
        and expected is not None
        and normalize_uuid(run["conversation_response_id"]) == expected
    )


async def settle_fork_request(thread_id: str, request: ChatRequest) -> ChatRequest:
    """The fork ``request`` with its turn read off the current branch and the
    branch read START holds it to.

    A regenerate of a turn that never reached the graph has no input on the
    branch to replay, so it runs as the edit that sends the turn's stored
    message again.
    """
    fork = await resolve_fork_turn(
        thread_id,
        request.checkpoint_id,
        regenerate=not request.messages,
        requested=request.fork_from_turn,
    )
    if fork.turn_index != request.fork_from_turn:
        logger.warning(
            f"[CHAT] fork turn {request.fork_from_turn} from the client "
            f"is turn {fork.turn_index} on the branch thread_id={thread_id}"
        )
    unstarted_regenerate = fork.unstarted and not request.messages
    latest = None
    if request.expected_latest_run_id is not None or unstarted_regenerate:
        from src.server.database.runs.lifecycle import get_latest_attempt

        latest = await get_latest_attempt(thread_id)
    # The branch was read as it is now, a turn this client never saw on it
    # included, so only the client's own last run tells a fork made from a
    # stale view apart.
    if request.expected_latest_run_id is not None and not _is_run(
        latest, request.expected_latest_run_id
    ):
        raise HTTPException(status_code=409, detail=stale_fork_detail())
    if unstarted_regenerate:
        from src.server.handlers.chat.admission_gate import admission_conflict_detail
        from src.server.services.runs.pending_input import (
            unstarted_regenerate_request,
        )

        # An open run is the unstarted turn still starting: refused as any
        # fork into a running turn is.
        if latest is not None and latest["status"] == "in_progress":
            raise HTTPException(
                status_code=409, detail=admission_conflict_detail("running")
            )
        request = await unstarted_regenerate_request(
            thread_id, request, fork.turn_index, latest
        )
    return request.model_copy(
        update={
            "fork_from_turn": fork.turn_index,
            "fork_branch_tip": fork.branch_tip,
            "fork_run_horizon": fork.run_horizon,
        }
    )


async def get_retry_checkpoint(thread_id: str, checkpoint_id: str | None = None) -> str:
    """
    Determine the appropriate checkpoint ID for retrying a failed/interrupted thread.

    If checkpoint_id is provided, validates it exists and returns it.
    Otherwise, auto-detects the latest checkpoint of the thread's branch
    (``turn_base_checkpoint_id``). The empty fork root is never one: it holds
    no input to replay, and a first message's edit that failed before its
    graph took it reruns from its ``pending_input`` stamp, so one left without
    a stamp has nothing a retry can run.

    Args:
        thread_id: Thread identifier
        checkpoint_id: Optional explicit checkpoint ID

    Returns:
        The checkpoint ID to retry from

    Raises:
        HTTPException: If no checkpoint is found, or 409 ``not_retryable`` at
            the empty fork root
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
        replay = checkpoint_id
    else:
        replay = await turn_base_checkpoint_id(checkpointer, thread_id)
        if replay is None:
            config = build_checkpoint_config(thread_id)
            cp_tuple = await checkpointer.aget_tuple(config)
            if not cp_tuple:
                raise HTTPException(
                    status_code=404,
                    detail=f"No checkpoints found for thread {thread_id}",
                )
            replay = cp_tuple.config["configurable"]["checkpoint_id"]

    if replay == FORK_ROOT_CHECKPOINT_ID:
        from src.server.services.runs.pending_input import _message_gone

        raise _message_gone()
    return replay


async def settle_resume_request(thread_id: str, request: ChatRequest) -> ChatRequest:
    """The HITL answer ``request``, refused if its pause is on a branch a fork
    replaced, else stamped with the run horizon START holds it to.

    A resume that names no checkpoint loads the newest, and after a fork
    whose run failed before its first step that is the replaced branch's
    pause, which no reader shows any more (``turn_base_checkpoint_id``). One
    that names its checkpoint resumes there, which must be on the branch the
    thread's pointer names. A fork admitted after this check is caught at
    START, which refuses the answer if a run started after the horizon; read
    first, so no fork can fall between the check and the horizon.
    """
    branch = await threads_read.get_thread_branch_read(thread_id)
    checkpointer = get_checkpointer()
    if not request.checkpoint_id:
        replaced = await turn_base_checkpoint_id(checkpointer, thread_id) is not None
    elif branch.checkpoint_id is None:
        replaced = False
    else:
        replaced = not await checkpoint_on_branch(
            thread_id,
            request.checkpoint_id,
            branch.checkpoint_id,
            checkpointer=checkpointer,
        )
    if replaced:
        raise HTTPException(status_code=409, detail=stale_fork_detail())
    return request.model_copy(update={"resume_run_horizon": branch.run_horizon})
