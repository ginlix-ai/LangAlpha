"""The sweep's slices: the walk's place and rest live in the database, so the
deployment walks once; an unread answer's retries come from its row; a
thread whose indexing raised is passed over a while, longer each time."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from src.config.models import ThreadSearchConfig
from src.server.database.search import index as db
from src.server.services.search import index, sweep

pytestmark = pytest.mark.asyncio

AT = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def clock(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(sweep.time, "monotonic", lambda: now[0])
    return now


@pytest.fixture
def config(monkeypatch):
    config = ThreadSearchConfig(interval_seconds=60, batch_threads=2, pause_seconds=0.5)
    monkeypatch.setattr("src.config.settings.get_thread_search_config", lambda: config)
    return config


@pytest.fixture
def wired(monkeypatch):
    """The database as the slice sees it: nothing due, nothing stale."""
    mocks = {
        "stale_since": AsyncMock(return_value=[]),
        "due_retries": AsyncMock(return_value=[]),
        "defer_retries": AsyncMock(),
        "read_walk": AsyncMock(return_value=db.Walk(None, 0.0, False)),
        "save_walk": AsyncMock(),
        "walk_threads": AsyncMock(return_value=[]),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(db, name, mock)
    return mocks


async def test_a_thread_whose_indexing_raised_waits_longer_each_time(
    monkeypatch, clock, config
):
    index_turns = AsyncMock(side_effect=RuntimeError("slice unreadable"))
    monkeypatch.setattr(index, "index_turns", index_turns)
    sweeper = sweep.ThreadSearchSweeper()
    tally = sweep._Tally()

    async def attempt():
        await sweeper._index("t-1", [4], tally)
        return index_turns.await_count

    assert await attempt() == 1
    assert index_turns.await_args.kwargs == {"turn_indexes": {3, 4}}
    clock[0] += 59
    assert await attempt() == 1
    clock[0] += 1
    assert await attempt() == 2
    clock[0] += 60
    assert await attempt() == 2  # the second failure waits twice as long
    clock[0] += 60
    assert await attempt() == 3

    index_turns.side_effect = None
    index_turns.return_value = index.Indexed(written=2, unread=0)
    clock[0] += 240
    assert await attempt() == 4
    assert tally.written == 2 and "t-1" not in sweeper._raised


async def test_a_slice_walks_from_the_stored_place_and_stores_where_it_stopped(
    monkeypatch, clock, config, wired
):
    wired["read_walk"].return_value = db.Walk((AT, "t-0"), 0.0, False)
    wired["walk_threads"].return_value = [
        db.WalkedThread("t-1", AT, [2]),
        db.WalkedThread("t-2", AT, []),
    ]

    async def index_turns(thread_id, *, turn_indexes):
        clock[0] += sweep.WALK_SLICE_S + 5  # a slow thread ends the slice
        return index.Indexed(written=1, unread=0)

    monkeypatch.setattr(index, "index_turns", index_turns)
    rest = await sweep.ThreadSearchSweeper()._pass(config, sweep._Tally())

    assert wired["walk_threads"].await_args.kwargs["after"] == (AT, "t-0")
    # It rests as long as it worked, up to the cap.
    assert rest == sweep.WALK_SLICE_S + 5
    wired["save_walk"].assert_awaited_once_with(
        (AT, "t-2"), rest_s=sweep.WALK_SLICE_S + 5, finished=False
    )


async def test_the_walks_end_waits_a_day_for_the_next(config, wired):
    wired["walk_threads"].return_value = [db.WalkedThread("t-1", AT, [])]
    rest = await sweep.ThreadSearchSweeper()._pass(config, sweep._Tally())
    assert rest == sweep.REWALK_S
    wired["save_walk"].assert_awaited_once_with(None, rest_s=sweep.REWALK_S, finished=True)


async def test_a_slice_not_due_leaves_the_walk_alone(config, wired):
    wired["read_walk"].return_value = db.Walk((AT, "t-0"), 42.0, False)
    assert await sweep.ThreadSearchSweeper()._pass(config, sweep._Tally()) == 42.0
    wired["walk_threads"].assert_not_awaited()
    wired["save_walk"].assert_not_awaited()


async def test_due_retries_run_whether_or_not_the_walk_is_due(
    monkeypatch, config, wired
):
    wired["read_walk"].return_value = db.Walk(None, 3600.0, True)
    wired["due_retries"].side_effect = [
        [db.StaleThread("t-1", [5]), db.StaleThread("t-2", [7])],
        # A raised read leaves its row due; it is not tried twice a pass.
        [db.StaleThread("t-2", [7])],
    ]

    async def index_turns(thread_id, *, turn_indexes):
        if thread_id == "t-2":
            raise RuntimeError("checkpoint unreadable")
        return index.Indexed(written=1, unread=0)

    monkeypatch.setattr(index, "index_turns", index_turns)
    tally = sweep._Tally()
    await sweep.ThreadSearchSweeper()._pass(config, tally)

    assert tally.written == 1
    # A retry reads the unread turns alone, not the turn before.
    wired["defer_retries"].assert_awaited_once_with("t-2", [7])
