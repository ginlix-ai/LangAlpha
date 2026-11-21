"""A turn that failed before its first checkpoint has no branch entry of its
own: the next entry stands for it, so a fork cut at it resolves to that
entry's state instead of being refused as stale."""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.conversation.threads_read import ThreadBranchRead
from src.server.models.workflow import TurnCheckpointInfo
from src.server.services.forks import snapshot
from src.server.services.forks.model import DURABLE, BeforeTurn, ForkRefused, ThroughTurn
from src.server.services.history.branch import BranchTurns

TIP = "cp-tip"


def _listing() -> BranchTurns:
    # Turn 0 checkpointed; turns 1 and 2 both failed before their first step,
    # so one entry under turn 2 stands for both, forking from the tip.
    first = TurnCheckpointInfo(turn_index=0, edit_checkpoint_id="cp-root", regenerate_checkpoint_id="cp-0")
    unstarted = TurnCheckpointInfo(turn_index=2, edit_checkpoint_id=TIP, regenerate_checkpoint_id=TIP)
    return BranchTurns([first, unstarted], TIP, unstarted)


async def _take(cut):
    with patch.object(
        snapshot.threads_read, "get_thread_branch_read", AsyncMock(return_value=ThreadBranchRead(TIP, 7))
    ), patch.object(snapshot, "branch_turns", AsyncMock(return_value=_listing())), patch.object(
        snapshot.thread_fork,
        "ledger_view",
        AsyncMock(return_value=({0: "completed", 1: "error", 2: "error"}, "the message")),
    ):
        return await snapshot.take_snapshot("src", cut, DURABLE)


@pytest.mark.asyncio
async def test_through_an_earlier_failed_turn_forks_from_the_tip():
    snap = await _take(ThroughTurn(1))
    assert snap.checkpoint_id == TIP
    assert snap.turn_indexes == [0, 1]
    assert snap.tip_read == TIP
    assert snap.read_through == 1


@pytest.mark.asyncio
async def test_before_an_earlier_failed_turn_keeps_the_turns_before_it():
    snap = await _take(BeforeTurn(1))
    assert snap.checkpoint_id == TIP
    assert snap.turn_indexes == [0]
    assert snap.prefill == "the message"


@pytest.mark.asyncio
async def test_a_turn_with_no_rows_is_still_stale():
    with pytest.raises(ForkRefused) as refused:
        await _take(ThroughTurn(5))
    assert refused.value.detail["code"] == "stale_fork"
