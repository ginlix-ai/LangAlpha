"""Holding the sources the egress relay read off paid replies."""

from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest

from ptc_agent.agent.provenance.trace import RelayedSource, sources_from_trace
from src.server.database import relayed_sources


class _Cursor:
    def __init__(self, held: list[str]):
        self.held = held
        self.executed: list[str] = []
        self.inserted: list[tuple] = []
        self.rowcount = 0

    async def execute(self, sql, params=()):
        self.executed.append(sql)
        self.params = params

    async def executemany(self, sql, rows):
        self.inserted.extend(rows)
        self.rowcount = len(rows)

    async def fetchall(self):
        return [(identifier, None) for identifier in self.held]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _db(cursor):
    conn = MagicMock()
    conn.cursor = lambda **kwargs: cursor

    @asynccontextmanager
    async def connection():
        yield conn

    return connection


def _page(n: int) -> RelayedSource:
    return RelayedSource(
        source_type="web_fetch", identifier=f"https://example.com/{n}", body="x" * 1024
    )


@pytest.mark.asyncio
async def test_a_crawl_poll_sends_only_the_pages_not_held_yet():
    """Each status poll lists every page so far, and every page's body went to
    Postgres for the unique index to throw away all but the new ones."""
    cursor = _Cursor(held=["https://example.com/1", "https://example.com/2"])
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        staged = await relayed_sources.stage(
            user_id="u1",
            call_id="call-1",
            job_ref="crawl:job-1",
            server="firecrawl",
            tool="firecrawl_crawl_status",
            sources=[_page(1), _page(2), _page(3), _page(3)],
        )

    assert staged == 1
    assert [row[3].obj["identifier"] for row in cursor.inserted] == [
        "https://example.com/3"
    ]


@pytest.mark.asyncio
async def test_a_reply_with_no_job_is_sent_whole_and_a_poll_with_nothing_new_inserts_nothing():
    cursor = _Cursor(held=[])
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        await relayed_sources.stage(
            user_id="u1", call_id="c", job_ref=None, server="firecrawl",
            tool="firecrawl_scrape", sources=[_page(1), _page(1)],
        )
    assert len(cursor.inserted) == 2
    assert not any("SELECT" in sql for sql in cursor.executed)

    cursor = _Cursor(held=["https://example.com/1"])
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        staged = await relayed_sources.stage(
            user_id="u1", call_id="c", job_ref="crawl:job-1", server="firecrawl",
            tool="firecrawl_crawl_status", sources=[_page(1)],
        )
    assert staged == 0
    assert cursor.inserted == []


@pytest.mark.asyncio
async def test_a_hold_older_than_the_relays_clock_is_not_still_settling():
    """A hold whose finish never came stays held until the sweep, and every
    notice naming its command waited the full settle bound on it."""
    cursor = _Cursor(held=["call-a"])
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        got = await relayed_sources.settling("u1", ("call-a", "call-b"), 55.0)

    assert got == {"call-a"}
    (sql,) = cursor.executed
    assert "status = 'held'" in sql and "created_at > NOW() - make_interval(secs => %s)" in sql
    assert "user_id = %s" in sql
    assert cursor.params == (["call-a", "call-b"], "u1", 55.0)


class _CollectCursor:
    """A cursor whose first read is the taking and second the calls held."""

    def __init__(self, taken: list[dict], calls: list[dict]):
        self.results = [taken, calls]
        self.executed: list[str] = []

    async def execute(self, sql, params=()):
        self.executed.append(sql)

    async def fetchall(self):
        return self.results.pop(0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_staged_sources_name_their_tool_and_collect_names_the_calls_as_traces_do():
    """By server, a free tool of a paid server lost its trace entry, which is
    its only source; the calls are named as the trace names its entries."""
    cursor = _Cursor(held=[])
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        await relayed_sources.stage(
            user_id="u1", call_id="c", job_ref=None, server="firecrawl",
            tool="firecrawl_scrape", sources=[_page(1)],
        )
    stored = cursor.inserted[0][3].obj
    assert (stored["server"], stored["tool"]) == ("firecrawl", "firecrawl_scrape")

    long_tool = "t" * 300
    cursor = _CollectCursor(
        taken=[{"id": 1, "source": stored, "body": "x"}],
        calls=[
            {"server": "firecrawl", "tool": "firecrawl_scrape"},
            {"server": "firecrawl", "tool": long_tool},
        ],
    )
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        batch = await relayed_sources.collect("u1", "c")

    trace = sources_from_trace(
        [
            {"server": "firecrawl", "tool": "firecrawl_scrape"},
            {"server": "firecrawl", "tool": long_tool},
            {"server": "firecrawl", "tool": "alexandria_library_search"},
        ]
    )
    assert [s.identifier in batch.calls for s in trace] == [True, True, False]
    assert [s.identifier for s in batch.sources] == ["https://example.com/1"]


@pytest.mark.asyncio
async def test_a_nul_in_a_vendors_text_is_stripped_before_it_is_held():
    """Neither column takes NUL, so one in a title or body failed the insert
    and the reply's sources were lost."""
    cursor = _Cursor(held=[])
    source = RelayedSource(
        source_type="web_fetch",
        identifier="https://example.com/1",
        title="Q3\x00 results",
        body="page\x00text",
    )
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        await relayed_sources.stage(
            user_id="u1", call_id="c", job_ref=None, server="firecrawl",
            tool="firecrawl_scrape", sources=[source],
        )
    stored, body = cursor.inserted[0][3], cursor.inserted[0][4]
    assert "\\u0000" not in stored.dumps(stored.obj)
    assert body == "pagetext"


@pytest.mark.asyncio
async def test_a_failed_read_of_the_calls_rolls_the_taking_back():
    """The pool autocommits, so the sources were taken and their bodies
    dropped by the first statement even when the second failed."""

    class _Failing(_CollectCursor):
        async def fetchall(self):
            if len(self.results) == 1:
                raise RuntimeError("connection lost")
            return await super().fetchall()

    exits: list[type | None] = []

    @asynccontextmanager
    async def transaction():
        try:
            yield
        except BaseException as e:
            exits.append(type(e))
            raise

    cursor = _Failing(taken=[{"id": 1, "source": {}, "body": "x"}], calls=[])
    conn = MagicMock()
    conn.cursor = lambda **kwargs: cursor
    conn.transaction = transaction

    @asynccontextmanager
    async def connection():
        yield conn

    with patch.object(relayed_sources, "get_db_connection", connection):
        with pytest.raises(RuntimeError):
            await relayed_sources.collect("u1", "c")

    assert exits == [RuntimeError] and len(cursor.executed) == 2


@pytest.mark.asyncio
async def test_holding_sources_leaves_the_retention_purge_to_the_sweep():
    """Every paid reply's staging ran the table's retention DELETE."""
    cursor = _Cursor([])
    with patch.object(relayed_sources, "get_db_connection", _db(cursor)):
        await relayed_sources.stage(
            user_id="u1", call_id="c", job_ref="crawl:j", server="firecrawl",
            tool="firecrawl_crawl", sources=[_page(1)],
        )

    assert not any("DELETE" in sql for sql in cursor.executed)
