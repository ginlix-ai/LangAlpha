"""Paging reaches every match: a search inside one thread lists all of its
matching turns across pages, where a wider search keeps a few per thread."""

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from src.server.database.search import read
from src.server.services.search import query

pytestmark = pytest.mark.asyncio

THREAD = "6f1c2a3e-0000-4000-8000-000000000001"
_START = datetime(2026, 11, 9, tzinfo=timezone.utc)
MATCHES = [
    read.TurnMatch(
        thread_id=THREAD,
        thread_title="t",
        workspace_id="6f1c2a3e-0000-4000-8000-0000000000ff",
        workspace_name="w",
        turn_index=i,
        turn_at=_START + timedelta(minutes=i),
        in_user=True,
        text=f"send {i}",
    )
    for i in range(30)
]


@pytest.fixture
def calls(monkeypatch):
    """``search_turns`` over ``MATCHES``, honouring its cap and keyset as the
    SQL does."""
    seen = []

    async def search_turns(user_id, terms, *, limit, per_thread, after=None, **scope):
        seen.append(per_thread)
        rows = sorted(MATCHES, key=lambda r: r.key, reverse=True)
        if per_thread is not None:
            rows = rows[:per_thread]
        if after is not None:
            params: dict = {}
            read._TURN_PAGE.after(after, params)
            rows = [r for r in rows if r.key < tuple(params.values())]
        return rows[:limit]

    monkeypatch.setattr(read, "search_turns", search_turns)
    return seen


async def _all_pages(**scope) -> list[int]:
    found, cursor = [], None
    while True:
        page = await query.search(
            "u", "send", kinds=["turns"], limit=8, cursor=cursor, **scope
        )
        found += [hit.turn_index for hit in page.turns]
        if (cursor := page.next_cursor.turns) is None:
            return found


async def test_inside_a_thread_every_match_is_paged_to(calls):
    assert await _all_pages(thread_id=THREAD) == list(range(29, -1, -1))
    assert set(calls) == {None}


async def test_a_wider_search_keeps_a_few_per_thread(calls):
    assert await _all_pages() == [29, 28, 27]
    assert set(calls) == {query.TURNS_PER_THREAD}


async def test_a_kind_that_times_out_leaves_the_others_whole(calls, monkeypatch):
    async def too_slow(*args, **kwargs):
        raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

    monkeypatch.setattr(read, "search_files", too_slow)
    monkeypatch.setattr(read, "search_threads", too_slow)

    page = await query.search("u", "send", kinds=["turns", "files"], limit=8)
    assert [hit.turn_index for hit in page.turns] == [29, 28, 27]
    assert page.files == [] and page.failed == ["files"]

    with pytest.raises(query.SearchTimedOut):
        await query.search("u", "send", kinds=["threads", "files"], limit=8)
