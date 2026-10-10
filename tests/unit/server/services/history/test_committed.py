"""A turn's committed text is read from its stored slice only while replay
would serve that slice: cut on the branch the thread now ends in, by the code
running now. Any other turn is projected first, as its settle would have."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from src.server.database.conversation.turn_slices import StoredSlice
from src.server.services.history import committed, slices
from src.server.services.history.reader import TurnAnchor

pytestmark = pytest.mark.asyncio

_SERDE = JsonPlusSerializer()
_ANCHOR = TurnAnchor(
    turn_ordinal=0, input_checkpoint_id="in-1", tail_checkpoint_id="tail-1", turn_index=1
)
_REFRESH = "src.server.services.history.replay.refresh.refresh_thread"


def _row(text: str, *, tail: str = "tail-1") -> StoredSlice:
    codec, data = _SERDE.dumps_typed(
        {"messages": [HumanMessage(content="q", id="h1"), AIMessage(content=text, id="a1")]}
    )
    return StoredSlice("in-1", tail, slices.SLICE_KEY, codec, data)


class _Reader:
    serde = _SERDE

    def __init__(self, anchors):
        self.aget_turn_anchors = AsyncMock(return_value=(anchors, "tip"))


def _branch(stored, *, anchors=(_ANCHOR,), persisted=(1,)):
    """The thread's pointer, its branch, and its stored slices (a list is
    one read after another)."""
    reads = stored if isinstance(stored, list) else [stored]
    return (
        patch.object(
            committed,
            "get_branch_pointer",
            AsyncMock(return_value=("tip", list(persisted))),
        ),
        patch.object(
            committed.CheckpointHistoryReader,
            "get_instance",
            return_value=_Reader(list(anchors)),
        ),
        patch.object(committed.slices_db, "get_turn_slices", AsyncMock(side_effect=reads)),
    )


async def test_a_turn_reads_its_text_from_the_slice_replay_would_serve():
    refresh = AsyncMock()
    pointer, reader, rows = _branch({"r-1": _row("from the slice")})
    with pointer, reader, rows, patch(_REFRESH, refresh):
        texts = await committed.committed_texts("t-1", {"r-1": 1})
    assert texts == {"r-1": "from the slice"}
    refresh.assert_not_awaited()


async def test_a_slice_cut_on_another_branch_is_projected_again_first():
    refresh = AsyncMock()
    pointer, reader, rows = _branch(
        [{"r-1": _row("old branch", tail="tail-0")}, {"r-1": _row("this branch")}]
    )
    with pointer, reader, rows, patch(_REFRESH, refresh):
        texts = await committed.committed_texts("t-1", {"r-1": 1})
    assert texts == {"r-1": "this branch"}
    refresh.assert_awaited_once_with("t-1", {"r-1"}, claims_pass=False)


async def test_a_turn_still_without_a_slice_is_left_to_the_caller():
    pointer, reader, rows = _branch([{}, {}])
    with pointer, reader, rows, patch(_REFRESH, AsyncMock()):
        assert await committed.committed_texts("t-1", {"r-1": 1}) == {}


async def test_a_branch_that_does_not_pair_reads_and_projects_nothing():
    refresh = AsyncMock()
    stamped_elsewhere = TurnAnchor(
        turn_ordinal=0, input_checkpoint_id="in-1", tail_checkpoint_id="tail-1", turn_index=7
    )
    pointer, reader, rows = _branch({}, anchors=(stamped_elsewhere,))
    with pointer, reader, rows as read, patch(_REFRESH, refresh):
        assert await committed.committed_texts("t-1", {"r-1": 1}) == {}
    read.assert_not_awaited()
    refresh.assert_not_awaited()
