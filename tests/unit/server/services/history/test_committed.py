"""A turn's committed text is read from its stored slice only while replay
would serve that slice: cut on the branch the thread now ends in, by the code
running now. Any other turn is projected first, as its settle would have, and
one the projection leaves for the thread's claims pass is read straight from
its checkpoints. A legacy turn its checkpoints never covered reads the stream
its row archived."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import psycopg
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


@pytest.fixture(autouse=True)
def _rows():
    """No row archived a stream unless a test says so."""
    with patch.object(
        committed, "get_replay_responses", AsyncMock(return_value={})
    ) as rows:
        yield rows


def _row(text: str, *, tail: str = "tail-1") -> StoredSlice:
    codec, data = _SERDE.dumps_typed(
        {"messages": [HumanMessage(content="q", id="h1"), AIMessage(content=text, id="a1")]}
    )
    return StoredSlice("in-1", tail, slices.SLICE_KEY, codec, data)


class _Reader:
    serde = _SERDE

    def __init__(self, anchors, turns=(), read_error=None):
        self.aget_turn_anchors = AsyncMock(return_value=(anchors, "tip"))
        self.aget_turn_slices = AsyncMock(
            return_value=list(turns), side_effect=read_error
        )


def _branch(stored, *, anchors=(_ANCHOR,), persisted=(1,), turns=(), read_error=None):
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
            return_value=_Reader(list(anchors), turns, read_error),
        ),
        patch.object(committed.slices_db, "get_turn_slices", AsyncMock(side_effect=reads)),
    )


async def test_a_turn_reads_its_text_from_the_slice_replay_would_serve(_rows):
    refresh = AsyncMock()
    pointer, reader, rows = _branch({"r-1": _row("from the slice")})
    with pointer, reader, rows, patch(_REFRESH, refresh):
        texts = await committed.committed_texts("t-1", {"r-1": 1})
    assert texts == {"r-1": "from the slice"}
    refresh.assert_not_awaited()
    _rows.assert_not_awaited()


async def test_a_slice_cut_on_another_branch_is_projected_again_first():
    refresh = AsyncMock()
    pointer, reader, rows = _branch(
        [{"r-1": _row("old branch", tail="tail-0")}, {"r-1": _row("this branch")}]
    )
    with pointer, reader, rows, patch(_REFRESH, refresh):
        texts = await committed.committed_texts("t-1", {"r-1": 1})
    assert texts == {"r-1": "this branch"}
    refresh.assert_awaited_once_with("t-1", {"r-1"}, claims_pass=False)


async def test_a_turn_still_without_a_slice_is_read_from_its_checkpoints():
    # Its lanes wait on the claims pass, which its main lane never needs.
    turn = slices.TurnSlice(
        anchor=_ANCHOR,
        messages=[
            HumanMessage(content="q", id="h1"),
            AIMessage(content="from the checkpoints", id="a1"),
        ],
    )
    pointer, reader, rows = _branch([{}, {}], turns=[turn])
    with pointer, reader, rows, patch(_REFRESH, AsyncMock()):
        texts = await committed.committed_texts("t-1", {"r-1": 1})
    assert texts == {"r-1": "from the checkpoints"}


async def test_a_turn_whose_checkpoints_cannot_be_read_is_left_to_the_caller():
    pointer, reader, rows = _branch([{}, {}], read_error=RuntimeError("pool closed"))
    with pointer, reader, rows, patch(_REFRESH, AsyncMock()):
        assert await committed.committed_texts("t-1", {"r-1": 1}) == {}


async def test_a_database_fault_raises_rather_than_read_as_no_text():
    # A turn left out reads as one that said nothing, which a fault is not.
    fault = psycopg.OperationalError("server closed")
    pointer, reader, rows = _branch([{}, {}], read_error=fault)
    with pointer, reader, rows, patch(_REFRESH, AsyncMock()):
        with pytest.raises(psycopg.OperationalError):
            await committed.committed_texts("t-1", {"r-1": 1})

    pointer, reader, rows = _branch({})
    with (
        patch.object(committed, "get_branch_pointer", AsyncMock(side_effect=fault)),
        reader,
        rows,
    ):
        with pytest.raises(psycopg.OperationalError):
            await committed.committed_texts("t-1", {"r-1": 1})


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


def _chunk(content, *, agent="main", content_type="text"):
    return {
        "event": "message_chunk",
        "data": {"agent": agent, "content": content, "content_type": content_type},
    }


async def test_a_legacy_turn_its_checkpoints_never_covered_reads_its_archive(_rows):
    _rows.return_value = {
        "r-1": {
            "replay_facts": {
                "archive": [
                    _chunk("the answer, "),
                    _chunk("thinking", content_type="reasoning"),
                    _chunk("a subagent's words", agent="task:abc"),
                    {"event": "tool_calls", "data": {"agent": "main"}},
                    _chunk("in full"),
                ]
            }
        },
        "r-2": {"replay_facts": {}},
    }
    with patch.object(committed, "get_branch_pointer", AsyncMock(return_value=None)):
        texts = await committed.committed_texts("t-1", {"r-1": 0, "r-2": 1})
    assert texts == {"r-1": "the answer, in full"}
    _rows.assert_awaited_once_with(["r-1", "r-2"])
