"""Resolving a cut to the checkpoint a fork starts from and the turns it keeps.

Turns are read off the source's current branch by the same walk edit and
regenerate use (``history.branch.branch_turns``), so a fork names turns by
the ``turn_index`` the chat shows. The pointer and the run horizon are read in
one snapshot; the copy refuses itself if a run rewrote a kept turn after it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.server.database import thread_fork
from src.server.database.conversation import threads_read
from src.server.services.history.branch import branch_turns
from src.server.services.forks.model import (
    BeforeTurn,
    Cut,
    ForkKind,
    ForkRefused,
    ThroughTurn,
    stale_refusal,
)
from src.server.utils.checkpoint_helpers import FORK_ROOT_CHECKPOINT_ID


@dataclass(frozen=True)
class Snapshot:
    source_thread_id: str
    #: The checkpoint the fork starts from, or None when it keeps no history.
    checkpoint_id: Optional[str]
    #: The persisted turns the fork keeps, ascending.
    turn_indexes: list[int]
    #: The newest turn the fork reads, which a run started after the
    #: snapshot must not have rewritten: the cut turn for both cuts, since a
    #: ``BeforeTurn`` cut hands its message on as ``prefill``.
    read_through: int
    #: The newest ``run_seq`` read with the branch.
    run_horizon: int
    #: The thread's pointer as read, when the cut was taken from the branch's
    #: tip: a turn ending after that read moves the tip under the cut, which
    #: the copy checks for (``service._plan``). None when the cut is a
    #: checkpoint a later turn already built on, which no ending moves.
    tip_read: Optional[str]
    #: A ``BeforeTurn`` cut's message.
    prefill: Optional[str]


async def take_snapshot(source_thread_id: str, cut: Cut, kind: ForkKind) -> Snapshot:
    if not isinstance(cut, kind.cuts):
        raise ForkRefused(400, "unsupported_cut", "This kind of copy can't start there.")
    branch = await threads_read.get_thread_branch_read(source_thread_id)
    listing = await branch_turns(source_thread_id, branch.checkpoint_id)
    turns = listing.turns
    # A turn that never checkpointed has no entry of its own: the next entry
    # stands for it, as for an edit, and forks from the same state.
    at = next((i for i, t in enumerate(turns) if t.turn_index >= cut.turn_index), None)
    newest, message = await thread_fork.ledger_view(source_thread_id, cut.turn_index)
    if at is None or cut.turn_index not in newest:
        raise stale_refusal()
    turn = turns[at]
    stands_in = turn.turn_index != cut.turn_index
    persisted = sorted(newest)

    if isinstance(cut, ThroughTurn):
        if newest.get(cut.turn_index) == "in_progress":
            raise ForkRefused(
                409, "turn_running", "This reply is still being written. Try again once it's done."
            )
        ends_at = cut.turn_index
        from_tip = at + 1 >= len(turns)
        if stands_in:
            # Nothing of the turn reached the graph: it ends where it began.
            from_tip = listing.unstarted is turn
            checkpoint_id = turn.edit_checkpoint_id
        elif not from_tip:
            # None when the next turn answers this one's question to the user.
            checkpoint_id = turns[at + 1].edit_checkpoint_id
        elif listing.unstarted is turn:
            checkpoint_id = turn.edit_checkpoint_id
        else:
            checkpoint_id = listing.tip_id
        kept = [t for t in persisted if t <= cut.turn_index]
        prefill = None
    else:
        assert isinstance(cut, BeforeTurn)
        ends_at = max((t for t in persisted if t < cut.turn_index), default=None)
        from_tip = listing.unstarted is turn
        checkpoint_id = turn.edit_checkpoint_id
        kept = [t for t in persisted if t < cut.turn_index]
        prefill = message
    # A cut never sits at a pause: what answers it (a question, an order's
    # approval, credits) belongs to the turn that paused, in its own thread.
    if checkpoint_id is None or newest.get(ends_at) == "interrupted":
        raise ForkRefused(
            409,
            "turn_waiting",
            "This reply is waiting for an answer, so it can't be copied yet."
            if isinstance(cut, ThroughTurn)
            else "This message answers a question, so it can't start a new chat.",
        )

    if checkpoint_id == FORK_ROOT_CHECKPOINT_ID:
        checkpoint_id = None
    return Snapshot(
        source_thread_id=source_thread_id,
        checkpoint_id=checkpoint_id,
        turn_indexes=kept if checkpoint_id else [],
        read_through=cut.turn_index,
        run_horizon=branch.run_horizon,
        tip_read=branch.checkpoint_id if from_tip else None,
        prefill=prefill,
    )
