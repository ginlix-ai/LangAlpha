"""A first page capped by its bytes (``PageBudget``): whole turns, newest
first, never fewer than the floor, and no straggler page left behind that a
budget's worth would cover. Turns are read in batches, so a cold thread
projects about the turns it sends."""

from __future__ import annotations

import json

import pytest

from src.server.services.history import replay
from src.server.services.history.replay import PageBudget
from src.server.services.history.replay.lines import Line
from tests.unit.server.services.history.replay_builders import replay_rows

pytestmark = pytest.mark.asyncio

# A line's wire bytes are its event name plus its data, so a turn's size is
# exact: the event name ``message_chunk`` is 13 bytes.
_EVENT = "message_chunk"


async def _page(sizes: list[int], *, limit: int, min_turns: int, max_bytes: int, reads=None):
    """Turns 0..n-1 (oldest first) of the given sizes: how many the page
    takes, and the turns it keeps. ``reads`` collects each batch read."""

    async def read(selected):
        if reads is not None:
            reads.append([ti for ti, _ in selected])
        return {ti: [Line(_EVENT, "", "x" * (sizes[ti] - len(_EVENT)))] for ti, _ in selected}

    in_range = [(ti, object()) for ti in range(len(sizes))]
    count, _ = await replay._budgeted_lines(
        read, in_range[-limit:], PageBudget(min_turns, max_bytes), whole=len(in_range) <= limit
    )
    return count, list(range(len(sizes) - count, len(sizes)))


async def test_heavy_turns_stop_at_the_floor_in_one_read():
    reads: list[list[int]] = []

    count, kept = await _page([2_000_000] * 40, limit=20, min_turns=5, max_bytes=3_000_000, reads=reads)

    assert count == 5
    assert kept == [35, 36, 37, 38, 39]
    assert reads == [[35, 36, 37, 38, 39]]


async def test_light_turns_fill_the_limit():
    reads: list[list[int]] = []

    count, kept = await _page([40_000] * 40, limit=20, min_turns=5, max_bytes=3_000_000, reads=reads)

    assert count == 20
    assert kept == list(range(20, 40))
    # The second read is sized from bytes per turn so far, not one turn at a time.
    assert len(reads) == 2


async def test_a_cut_takes_a_small_remainder_too():
    count, kept = await _page([1_000_000] * 3, limit=20, min_turns=2, max_bytes=1_500_000)

    assert count == 3
    assert kept == [0, 1, 2]


async def test_a_remainder_over_the_budget_is_left_for_its_own_page():
    sizes = [5_000_000] + [100_000] * 9
    count, kept = await _page(sizes, limit=20, min_turns=5, max_bytes=3_000_000)

    assert count == 9
    assert kept == list(range(1, 10))


async def test_fewer_turns_than_the_floor_are_all_taken():
    count, kept = await _page([1_000_000] * 2, limit=20, min_turns=5, max_bytes=3_000_000)

    assert (count, kept) == (2, [0, 1])


async def test_one_heavy_newest_turn_does_not_shrink_every_later_read():
    reads: list[list[int]] = []
    sizes = [5_000] * 39 + [2_900_000]

    count, kept = await _page(sizes, limit=20, min_turns=5, max_bytes=3_000_000, reads=reads)

    assert count == 20
    assert kept == list(range(20, 40))
    assert len(reads) <= 3


async def test_offloaded_widget_data_counts_toward_the_budget():
    widget = {"artifact_type": "html_widget", "payload": {"data_ref": {"key": "k", "size": 2_000_000}}}
    line = Line("artifact", replay.replay_lines.FLAG_WIDGET, json.dumps(widget))
    small = Line(_EVENT, "", "x" * 1_000)

    async def read(selected):
        return {ti: [line if ti >= 2 else small] for ti, _ in selected}

    in_range = [(ti, object()) for ti in range(4)]
    count, _ = await replay._budgeted_lines(read, in_range, PageBudget(1, 3_000_000), whole=True)

    # Two newest turns each send 2 MB once their data is inlined: the second passes 3 MB.
    assert count == 1


async def test_a_stub_turn_counts_toward_the_budget_like_a_projected_one():
    """A turn with no checkpoint boundary sends its archived stream as a stub,
    so the walk reads it with the rest instead of adding it after the cut."""
    sizes = {0: 1_000, 1: 1_000, 2: 2_500_000, 3: 1_000_000}

    async def read(selected):
        return {ti: [Line(_EVENT, "", "x" * (sizes[ti] - len(_EVENT)))] for ti, _ in selected}

    candidates = [(0, object()), (1, object()), (2, None), (3, object())]
    page, _ = await replay._fill_page(read, candidates, PageBudget(1, 3_000_000), whole=True)

    assert [ti for ti, _ in page] == [3]


async def test_the_stub_fallback_holds_the_budget(monkeypatch):
    thread = {"conversation_thread_id": "t-1", "latest_checkpoint_id": None}
    queries = [{"turn_index": ti, "content": "q"} for ti in range(6)]
    rows = replay_rows(thread, queries, [])
    reads: list[list[int]] = []

    async def fake_stub_lines(thread_id, turn_indexes, *_args):
        reads.append(list(turn_indexes))
        return {ti: [Line(_EVENT, "", "x" * 1_000_000)] for ti in turn_indexes}, []

    monkeypatch.setattr(replay, "stub_lines", fake_stub_lines)

    page = await replay.stub_page(rows, limit=20, budget=PageBudget(2, 2_500_000))

    assert page.first_turn_index == 4
    assert page.has_more is True
    assert len(page.lines) == 2
