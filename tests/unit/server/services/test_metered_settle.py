"""Regressions in charging paid relay calls.

A crawl whose reply kept its estimate was held with no job, so a later status
poll's running total was not netted against it and the crawl was charged
twice. A call with no thread was taken at turn start and counted only on the
turn's in-memory tracker, so a turn that died lost the charge.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services import metered_charges
from src.server.services.egress.firecrawl_meter import FirecrawlMeter
from src.server.services.egress.jsonrpc import CanonicalRequest
from src.server.services.egress.metering import MeteredCall

USER = "user-1"


def _reply(payload: dict) -> bytes:
    text = json.dumps(payload)
    return json.dumps(
        {"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": text}]}}
    ).encode()


def _call(tool: str, arguments: dict) -> MeteredCall:
    prepared = MagicMock()
    prepared.grant = {"user_id": USER, "server_name": "firecrawl"}
    prepared.canonical = CanonicalRequest(
        body=b"", method="tools/call", tool_name=tool, is_notification=False, arguments=arguments
    )
    return MeteredCall(
        meter=FirecrawlMeter(),
        prepared=prepared,
        hold_id=7,
        call_id="abcdef0123456789",
        thread_id=None,
        catalog={},
    )


@pytest.mark.asyncio
async def test_a_crawl_kept_at_its_estimate_is_held_under_its_job():
    call = _call("firecrawl_crawl", {"url": "https://example.com", "limit": 5})
    call.dispatch.status = 200
    call.observe(_reply({"success": True, "status": "completed", "id": "job-1", "data": []}))
    settle = AsyncMock()
    mark = AsyncMock()
    with (
        patch("src.server.services.egress.metering.settle_call", new=settle),
        patch("src.server.services.egress.metering.mark_job", new=mark),
        patch("src.server.services.egress.metering.stage", new=AsyncMock()),
    ):
        await call.finish(200, complete=True)

    settle.assert_awaited_once_with(USER, 7, credits=None, job_ref="crawl:job-1")
    # Named on the hold before the settle, so a hold swept instead is netted.
    mark.assert_awaited_once_with(7, "crawl:job-1", None)


@asynccontextmanager
async def _connection(conn):
    yield conn


@pytest.mark.asyncio
async def test_a_call_with_no_thread_is_charged_to_the_starting_thread_in_the_take():
    conn = MagicMock()
    conn.transaction = lambda: _connection(None)
    taken = [
        {"id": 1, "vendor": "firecrawl", "tool": "firecrawl_map", "vendor_credits": 1.0,
         "thread_id": None, "thread_workspace_id": None},
        {"id": 2, "vendor": "firecrawl", "tool": "firecrawl_map", "vendor_credits": 2.0,
         "thread_id": "own", "thread_workspace_id": "ws-own"},
    ]
    write = AsyncMock(return_value=True)
    stamp = AsyncMock()
    with (
        patch("src.server.database.pool.get_db_connection", new=lambda: _connection(conn)),
        patch("src.server.database.metered_vendor_calls.has_owed", new=AsyncMock(return_value=True)),
        patch("src.server.database.metered_vendor_calls.take_owed", new=AsyncMock(return_value=taken)),
        patch("src.server.database.metered_vendor_calls.stamp_collected", new=stamp),
        patch("src.server.services.persistence.usage.persist_between_turns", new=write),
    ):
        await metered_charges.charge_owed(USER, "starting", "ws-start")

    # The take is stamped in the same transaction, so the charge path ran whole.
    stamp.assert_awaited_once_with(conn, [1, 2])

    charged = {
        (c.kwargs["thread_id"], c.kwargs["workspace_id"]): c.kwargs["tool_usage"]
        for c in write.await_args_list
    }
    assert charged == {
        ("starting", "ws-start"): {"FirecrawlCredits": 1},
        ("own", "ws-own"): {"FirecrawlCredits": 2},
    }
    # On the take's own connection, so the charge commits or rolls back with it.
    assert all(c.kwargs["conn"] is conn for c in write.await_args_list)


# ---------------------------------------------------------------------------
# Admission: what a call is held at, and whether the hold outlives a cancel
# ---------------------------------------------------------------------------


def _prepared(tool: str, arguments: dict) -> MagicMock:
    prepared = _call(tool, arguments).prepared
    prepared.grant.update(destination_url="https://firecrawl.example/mcp")
    prepared.platform.account.meter = FirecrawlMeter()
    prepared.credential.headers = {}
    prepared.claims = MagicMock(computer_id=None, workspace_id=None)
    return prepared


@pytest.mark.asyncio
async def test_a_hold_committed_as_admission_is_cancelled_is_freed():
    """A cancellation between the hold's commit and the call reaching the
    route left a hold nothing owned, swept later at its estimate for a call
    never sent."""
    import asyncio

    from src.server.services.egress import metering

    async def _reserve(**kwargs):
        kwargs["on_hold"](11)
        raise asyncio.CancelledError

    release = AsyncMock()
    with (
        patch.object(metering, "spendable_credits", AsyncMock(return_value=None)),
        patch.object(metering, "user_credits", lambda meter, n: float(n)),
        patch.object(metering, "reserve", _reserve),
        patch.object(metering, "release", release),
    ):
        with pytest.raises(asyncio.CancelledError):
            await metering.admit(_prepared("firecrawl_map", {"url": "https://a.example"}), {})

    release.assert_awaited_once_with(11)


@pytest.mark.parametrize("limit", [-100000, 0, float("nan"), float("inf"), 10**400])
def test_a_search_limit_that_is_not_a_count_is_refused(limit):
    """A negative limit priced the call below zero, and its hold offset what
    the user owed; NaN and infinity raised inside admission (a bare 500)."""
    from src.server.services.egress import RelayError

    refusal = FirecrawlMeter().refusal("firecrawl_search", {"query": "q", "limit": limit})

    assert refusal is not None and refusal.code is RelayError.BAD_REQUEST


@pytest.mark.asyncio
async def test_a_negative_hold_is_refused_before_the_ledger():
    from src.server.database.metered_vendor_calls import reserve

    with pytest.raises(ValueError):
        await reserve(
            user_id=USER, computer_id=None, workspace_id=None, call_id=None, thread_id=None,
            vendor="firecrawl", tool="firecrawl_search", estimate=-5.0, owed_cap=1000,
            allowed=None,
        )


class _Cursor:
    def __init__(self, rows: list):
        self.rows = list(rows)
        self.executed: list[tuple[str, tuple]] = []

    async def execute(self, sql, params=()):
        self.executed.append((sql, params))

    async def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    async def fetchall(self):
        return []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _ledger(cursor: _Cursor):
    conn = MagicMock()
    conn.transaction = lambda: _connection(None)
    conn.cursor = lambda **kwargs: cursor
    return lambda: _connection(conn)


@pytest.mark.asyncio
@pytest.mark.parametrize(("allowed", "verdict"), [(None, "charges_pending"), (5000.0, "ok")])
async def test_with_no_balance_the_cap_counts_the_call_being_admitted(allowed, verdict):
    """Owing 900 of a 1000 cap, a 200-credit call passed when nothing but the
    cap bounded it. A funded user's call is bounded by the balance instead."""
    from src.server.database import metered_vendor_calls

    cursor = _Cursor([{"owed": 900}, {"id": 3}])
    with patch.object(metered_vendor_calls, "get_db_connection", _ledger(cursor)):
        got, _ = await metered_vendor_calls.reserve(
            user_id=USER, computer_id=None, workspace_id=None, call_id=None, thread_id=None,
            vendor="firecrawl", tool="firecrawl_search", estimate=200.0, owed_cap=1000,
            allowed=allowed,
        )

    assert got == verdict


@pytest.mark.asyncio
@pytest.mark.parametrize(("allowed", "verdict"), [(1.5, "exceeds"), (2.0, "ok")])
async def test_a_fractional_price_is_admitted_in_the_units_it_is_charged(allowed, verdict):
    """1.1 credits passed against 1.5 left, then was charged as 2; the ledger
    sums are read rounded per call for the same reason."""
    from src.server.database import metered_vendor_calls

    cursor = _Cursor([{"owed": 0, "unseen": 0}, {"id": 3}])
    with patch.object(metered_vendor_calls, "get_db_connection", _ledger(cursor)):
        got, _ = await metered_vendor_calls.reserve(
            user_id=USER, computer_id=None, workspace_id=None, call_id=None, thread_id=None,
            vendor="firecrawl", tool="firecrawl_scrape", estimate=1.1, owed_cap=1000,
            allowed=allowed,
        )

    assert got == verdict
    assert "SUM(CEIL(vendor_credits))" in cursor.executed[1][0]


@pytest.mark.parametrize("runs", [[], "", 0, False, {}])
def test_a_scrape_with_no_catalog_runs_is_priced_as_a_page(runs):
    """A falsy ``alexandria`` beside a url was classed as a catalog call,
    estimated at nothing and charged from a cost field a page reply lacks."""
    meter = FirecrawlMeter()
    arguments = {"url": "https://example.com", "alexandria": runs}

    assert meter.estimate("firecrawl_scrape", arguments, {}) == 1.0
    assert meter.charge(
        "firecrawl_scrape", arguments, {"markdown": "x", "metadata": {"creditsUsed": 1}}
    ).credits == 1


def test_catalog_runs_are_priced_on_library_read_and_refused_on_scrape():
    """Firecrawl moved catalog runs from ``firecrawl_scrape(alexandria=)`` to
    ``alexandria_library_read(calls=)``. Unpriced, the new tool was refused.
    The old frame is still run by Firecrawl, but how it bills one sent beside
    a url is unknown, so it is refused rather than priced as a catalog run."""
    from src.server.services.egress import RelayError
    from src.server.services.egress.firecrawl_catalog import Capability

    meter = FirecrawlMeter()
    catalog = _catalog(lookup=Capability("L", 15.0, False))
    run = {"provider": "p", "capability": "lookup", "options": {"ticker": "NVDA"}}
    reply = {"data": {"creditsCost": 15, "alexandria": [{**run, "data": {"x": 1}}]}}
    read = "alexandria_library_read"

    assert meter.is_paid(read)
    assert meter.refusal(read, {"calls": [run]}) is None
    assert meter.estimate(read, {"calls": [run]}, catalog) == 15.0
    assert meter.charge(read, {"calls": [run]}, reply).credits == 15
    legacy = {"url": "https://x.com/a/status/1", "alexandria": run}
    assert meter.refusal("firecrawl_scrape", legacy).code is RelayError.BAD_REQUEST
    assert not meter.is_paid("alexandria_library_search")


def test_every_paid_call_is_held_at_least_one_credit():
    meter = FirecrawlMeter()

    assert meter.estimate("firecrawl_check_crawl_status", {"id": "job"}, {}) == 1.0
    assert meter.estimate("firecrawl_unlisted", {}, {}) == 1.0


@pytest.mark.parametrize("proxy", ["enhanced", "stealth"])
def test_a_page_on_any_proxy_is_held_as_a_basic_one(proxy):
    """Enhanced and stealth pages were held at 4 credits over a basic page,
    which Firecrawl no longer bills, refusing calls the balance covered."""
    meter = FirecrawlMeter()

    assert meter.estimate("firecrawl_scrape", {"url": "https://a.com", "proxy": proxy}, {}) == 1.0
    assert meter.estimate(
        "firecrawl_search", {"query": "q", "limit": 3, "scrapeOptions": {"proxy": proxy}}, {}
    ) == 5.0


def test_a_capped_pdf_parse_is_held_at_its_page_cap():
    """A PDF is billed a credit a page in place of the page's one, and was
    held at the page's 1 credit whatever the cap allowed."""
    meter = FirecrawlMeter()
    capped = {"url": "https://a.com/r.pdf", "parsers": [{"type": "pdf", "maxPages": 20}]}

    assert meter.estimate("firecrawl_scrape", capped, {}) == 20.0
    # Redaction bills 4 on the page and 4 on each PDF page past the first.
    assert meter.estimate("firecrawl_scrape", {**capped, "redactPII": True}, {}) == 100.0


def test_an_x_scrape_is_held_at_xs_own_price():
    """X was held at 34 before formats, so a JSON scrape billed 34 was held
    at 38 and a plain one billed 30 at 34."""
    meter = FirecrawlMeter()

    assert meter.estimate("firecrawl_scrape", {"url": "https://x.com/a/status/1"}, {}) == 30.0
    assert meter.estimate(
        "firecrawl_scrape", {"url": "https://x.com/a/status/1", "formats": ["json"]}, {}
    ) == 34.0
    assert meter.estimate("firecrawl_scrape", {"url": "https://a.com/r.pdf", "parsers": ["pdf"]}, {}) == 1.0


def test_a_url_that_does_not_parse_is_priced_as_a_page():
    """Parsing ``http://[`` raised out of admission as an internal error."""
    meter = FirecrawlMeter()

    assert meter.estimate("firecrawl_scrape", {"url": "http://["}, {}) == 1.0


def test_every_search_branch_becomes_a_source():
    """Only ``web`` hits were taken, and they stood in for the whole trace
    entry, so news and image results vanished from provenance."""
    meter = FirecrawlMeter()
    result = {"data": {
        "web": [{"url": "https://w.com", "title": "W"}],
        "news": [{"url": "https://n.com", "title": "N"}],
        "images": [{"url": "https://i.com", "imageUrl": "https://i.com/x.png"}],
    }}

    sources, _ = meter.sources(
        "firecrawl_search", {"query": "q"}, result, catalog={}, call_cost=3.0
    )

    assert [s.identifier for s in sources] == ["https://w.com", "https://n.com", "https://i.com"]


def test_a_crawl_start_takes_its_job_only_from_the_reply():
    """A start that named an ``id`` of its own had its total netted against
    that job's earlier charges."""
    meter = FirecrawlMeter()
    arguments = {"url": "https://example.com", "limit": 5, "id": "someone-elses-job"}

    assert meter.job_ref("firecrawl_crawl", arguments, {"success": True}) is None
    assert meter.job_ref("firecrawl_crawl", arguments, {"id": "job-2"}) == "crawl:job-2"
    assert meter.job_ref("firecrawl_check_crawl_status", {"id": "job-2"}, {}) == "crawl:job-2"


@pytest.mark.asyncio
async def test_a_call_id_reads_only_its_users_calls_and_sources():
    """The sandbox picks its call ids, so one user's code could name another
    user's call and read its charges or take its sources."""
    from src.server.database import metered_vendor_calls, relayed_sources

    calls = _Cursor([])
    with patch.object(metered_vendor_calls, "get_db_connection", _ledger(calls)):
        await metered_vendor_calls.calls_of(USER, "abcdef0123456789")
    sources = _Cursor([])
    with patch.object(relayed_sources, "get_db_connection", _ledger(sources)):
        await relayed_sources.collect(USER, "abcdef0123456789")

    for sql, params in calls.executed + sources.executed:
        assert "user_id = %s" in sql and USER in params


@pytest.mark.asyncio
async def test_a_paid_call_with_no_price_per_unit_is_refused():
    """Unpriced, a call was charged nothing and the balance bounded nothing."""
    from src.server.services.egress import RelayError, RelayRejection, metering

    spendable = AsyncMock(return_value=100.0)
    with (
        patch.object(metering, "user_credits", lambda meter, n: 0.0),
        patch.object(metering, "spendable_credits", spendable),
    ):
        with pytest.raises(RelayRejection) as refused:
            await metering.admit(_prepared("firecrawl_map", {"url": "https://a.example"}), {})

    assert refused.value.code is RelayError.CREDIT_CHECK_UNAVAILABLE
    spendable.assert_not_awaited()


# ---------------------------------------------------------------------------
# Review follow-ups
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_reply_is_parsed_off_the_loop_and_a_non_200_never_buffered():
    """``finish`` parsed up to 16 MiB of JSON twice on the event loop, and
    buffered the body of answers that are free whatever they say."""
    from src.server.services.egress import metering

    refused = _call("firecrawl_map", {"url": "https://a.example"})
    refused.dispatch.status = 500
    refused.observe(b"x" * 100)
    assert refused._chunks == []

    call = _call("firecrawl_map", {"url": "https://a.example"})
    call.dispatch.status = 200
    call.observe(_reply({"success": True, "links": []}))
    threads = []
    real = asyncio.to_thread

    async def _to_thread(fn, *args):
        threads.append(fn.__name__)
        return await real(fn, *args)

    with (
        patch.object(metering.asyncio, "to_thread", _to_thread),
        patch.object(metering, "settle_call", AsyncMock()),
    ):
        await call.finish(200, complete=True)

    assert threads == ["_read"] and call._chunks == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, None])
async def test_a_200_cut_before_its_first_byte_keeps_its_estimate(status):
    """A 200 whose stream ended before any body (headers out, the tool still
    running) was released as free though the vendor took the call."""
    from src.server.services.egress import metering

    call = _call("firecrawl_scrape", {"url": "https://a.example"})
    call.dispatch.status = 200
    release, settle = AsyncMock(), AsyncMock()
    with (
        patch.object(metering, "release", release),
        patch.object(metering, "settle_call", settle),
    ):
        await call.finish(status, complete=False)

    release.assert_not_awaited()
    assert settle.await_args.kwargs["credits"] is None

    whole = _call("firecrawl_scrape", {"url": "https://a.example"})
    whole.dispatch.status = 200
    release, settle = AsyncMock(), AsyncMock()
    with (
        patch.object(metering, "release", release),
        patch.object(metering, "settle_call", settle),
    ):
        await whole.finish(200, complete=True)

    release.assert_awaited_once()
    settle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_settle_stuck_on_the_database_gives_up_at_the_bound():
    """The writes are shielded from the request's cancellation, so a starved
    pool held the route open forever."""
    from src.server.services.egress import metering

    async def _stuck(*args, **kwargs):
        await asyncio.sleep(30)

    call = _call("firecrawl_map", {"url": "https://a.example"})
    call.dispatch.status = 200
    call.observe(_reply({"success": True, "links": []}))
    with (
        patch.object(metering, "settle_call", _stuck),
        patch.object(metering, "_FINISH_BUDGET_S", 0.1),
    ):
        await asyncio.wait_for(call.finish(200, complete=True), 2)


@pytest.mark.asyncio
async def test_sources_of_a_call_charged_at_its_estimate_carry_its_share():
    """A reply with no readable cost staged its sources at no cost though the
    call was charged its estimate; shares now round as the charge does."""
    from src.server.services.egress import metering

    call = _call("firecrawl_crawl", {"url": "https://example.com", "limit": 5})
    call.estimate = 4.5
    call.dispatch.status = 200
    pages = [{"markdown": "p", "metadata": {"sourceURL": f"https://example.com/{i}"}}
             for i in range(2)]
    call.observe(_reply({"success": True, "id": "job-1", "data": pages}))
    stage = AsyncMock()
    with (
        patch.object(metering, "settle_call", AsyncMock()),
        patch.object(metering, "mark_job", AsyncMock()),
        patch.object(metering, "stage", stage),
        patch.object(metering, "user_credits", lambda meter, n: 4.0 * n),
    ):
        await call.finish(200, complete=True)

    # ceil(4.5) = 5 Firecrawl credits over two pages, 4 of ours each.
    assert [s.cost_credits for s in stage.await_args.kwargs["sources"]] == [10.0, 10.0]


@pytest.mark.asyncio
async def test_a_threadless_call_lands_in_its_own_workspace():
    """A call whose thread was unknown was charged to whatever thread the
    user's next turn started on, in any workspace."""
    conn = MagicMock()
    conn.transaction = lambda: _connection(None)
    taken = [
        {"id": 7, "vendor": "firecrawl", "tool": "firecrawl_map", "vendor_credits": 1.0,
         "thread_id": None, "thread_workspace_id": None,
         "workspace_id": "ws-call", "workspace_thread_id": "latest-in-ws-call"},
    ]
    write = AsyncMock(return_value=True)
    stamp = AsyncMock()
    with (
        patch("src.server.database.pool.get_db_connection", new=lambda: _connection(conn)),
        patch("src.server.database.metered_vendor_calls.has_owed", new=AsyncMock(return_value=True)),
        patch("src.server.database.metered_vendor_calls.take_owed", new=AsyncMock(return_value=taken)),
        patch("src.server.database.metered_vendor_calls.stamp_collected", new=stamp),
        patch("src.server.services.persistence.usage.persist_between_turns", new=write),
    ):
        await metered_charges.charge_owed(USER, "starting", "ws-start")

    stamp.assert_awaited_once_with(conn, [7])

    assert [(c.kwargs["thread_id"], c.kwargs["workspace_id"]) for c in write.await_args_list] == [
        ("latest-in-ws-call", "ws-call")
    ]


@pytest.mark.asyncio
async def test_the_sweep_opens_no_transaction_when_nothing_is_owed():
    """The sweep ran only while a server was metered, so calls left by one an
    operator removed were never charged; it now runs whenever anything is
    owed, behind one probe."""
    take = AsyncMock()
    with (
        patch("src.server.database.metered_vendor_calls.has_owed", new=AsyncMock(return_value=False)),
        patch("src.server.database.metered_vendor_calls.take_owed", new=take),
    ):
        await metered_charges.charge_owed(USER, "starting", "ws-start")

    take.assert_not_awaited()


@pytest.mark.parametrize(
    ("status", "kind", "code"),
    [
        (429, "credit_limit", "credits_exhausted"),
        (429, "monthly_credit_limit", "credits_exhausted"),
        (429, "payment_failed", "credits_exhausted"),
        (429, "service_unavailable", "credit_check_unavailable"),
        (503, "service_unavailable", "credit_check_unavailable"),
    ],
)
def test_a_denial_maps_by_kind_and_carries_the_message(status, kind, code):
    """Every 429 read as out of credits, the auth service's own outage
    included, and its message was dropped."""
    from fastapi import HTTPException

    from src.server.services.egress.metering import _quota_refusal

    refusal = _quota_refusal(
        HTTPException(status, detail={"type": kind, "message": "Why, in words."})
    )

    assert refusal.code.value == code
    if code == "credits_exhausted":
        assert refusal.detail == "Why, in words."


@pytest.mark.asyncio
async def test_a_call_charged_after_the_balance_read_counts_against_it():
    """Another worker's call settled between the balance read and the hold
    was neither owed nor in the balance, so the hold overshot by its cost."""
    import time

    from src.server.database import metered_vendor_calls

    cursor = _Cursor([{"owed": 0, "unseen": 3}, {"id": 3}])
    with patch.object(metered_vendor_calls, "get_db_connection", _ledger(cursor)):
        got, _ = await metered_vendor_calls.reserve(
            user_id=USER, computer_id=None, workspace_id=None, call_id=None, thread_id=None,
            vendor="firecrawl", tool="firecrawl_scrape", estimate=3.0, owed_cap=1000,
            allowed=5.0, balance_read_at=time.monotonic() - 4.0,
        )

    assert got == "exceeds"
    # The read's age, taken under the lock, is compared in the database's
    # clock, never an app timestamp against ``collected_at``.
    (age,) = [p["age"] for _, p in cursor.executed if isinstance(p, dict)]
    assert 4.0 + metered_vendor_calls._READ_MARGIN_S <= age < 5.0 + metered_vendor_calls._READ_MARGIN_S


def _catalog(**capabilities):
    from src.server.services.egress.firecrawl_catalog import Provider

    return {"p": Provider(name="P", domain="", capabilities=capabilities)}


def test_a_per_record_run_is_priced_at_its_declared_count_or_refused():
    """A per-record run with no limit and no list was priced at one record,
    a run the catalog does not list at nothing, and an unpriced tool at one
    credit."""
    from src.server.services.egress import RelayError, RelayRejection
    from src.server.services.egress.firecrawl_catalog import Capability

    meter = FirecrawlMeter()

    def run(capability, options=None):
        return {"calls": [{"provider": "p", "capability": capability, "options": options or {}}]}

    read = "alexandria_library_read"
    catalog = _catalog(
        search=Capability("S", 5.0, True, records_option="limit", records_default=2),
        size=Capability("Z", 5.0, True, records_option="size", records_default=None),
        lookup=Capability("L", 10.0, True),
    )
    assert meter.estimate(read, run("search"), catalog) == 10.0
    assert meter.estimate(read, run("search", {"limit": 6}), catalog) == 30.0
    assert meter.estimate(read, run("size", {"size": 3}), catalog) == 15.0
    assert meter.estimate(read, run("lookup"), catalog) == 10.0
    for unpriceable in (run("size"), run("missing")):
        with pytest.raises(RelayRejection) as refused:
            meter.estimate(read, unpriceable, catalog)
        assert refused.value.code is RelayError.BAD_REQUEST
    assert meter.refusal("firecrawl_extract", {}).code is RelayError.TOOL_BLOCKED


def test_the_owed_probe_and_user_tables_follow_the_user():
    """The tables had no tie to ``users``, so a deleted user's key, ledger
    and sources stayed behind."""
    from pathlib import Path

    text = (
        Path(__file__).resolve().parents[4] / "migrations" / "versions" / "131_firecrawl_accounts.py"
    ).read_text()
    for table in (
        "user_firecrawl_accounts",
        "metered_vendor_calls",
        "relayed_sources",
    ):
        create = text[text.index(f"CREATE TABLE IF NOT EXISTS {table}"):]
        create = create[: create.index(")\n    \"\"\")")]
        assert "REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE" in create, table


@pytest.mark.asyncio
async def test_no_account_is_created_for_a_user_with_nothing_to_spend():
    """The first relay call of any kind provisioned an account, billed to the
    deployment and counted against the partner cap, for every sign-up."""
    from src.server.services import firecrawl_accounts
    from src.server.services.egress import RelayError, RelayRejection

    partner = AsyncMock()
    with (
        patch.object(firecrawl_accounts.env, "FIRECRAWL_PLATFORM_KEY", "pk"),
        patch.object(firecrawl_accounts.db, "get_account_key", AsyncMock(return_value=None)),
        patch.object(firecrawl_accounts, "_remembered_refusal", AsyncMock(return_value=None)),
        patch(
            "src.server.services.egress.metering.spendable_credits",
            AsyncMock(return_value=0.0),
        ),
        patch.object(firecrawl_accounts, "_partner_post", partner),
    ):
        with pytest.raises(RelayRejection) as refused:
            await firecrawl_accounts.firecrawl_key_for(USER)

    assert refused.value.code is RelayError.CREDITS_EXHAUSTED
    partner.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_partner_refusal_is_remembered():
    """A refusal the partner API repeats (its account cap) was asked again on
    every relay call; remembered, it is answered to the relay and to another
    accept without asking the user's terms or the partner again."""
    from src.server.services import firecrawl_accounts
    from src.server.services.egress import AccountUnavailable

    store: dict = {}

    class _Cache:
        async def mget(self, keys):
            return [store.get(k) for k in keys]

        async def set(self, key, value, ttl=None):
            store[key] = value

    partner = AsyncMock(side_effect=AccountUnavailable("cap_reached", "Firecrawl refused"))
    with (
        patch.object(firecrawl_accounts.env, "FIRECRAWL_PLATFORM_KEY", "pk"),
        patch.object(firecrawl_accounts.env, "FIRECRAWL_ACCOUNT_EMAIL_DOMAIN", "example.com"),
        patch.object(firecrawl_accounts.db, "get_account_key", AsyncMock(return_value=None)),
        patch("src.utils.cache.redis_cache.get_cache_client", lambda: _Cache()),
        patch(
            "src.server.services.egress.metering.spendable_credits",
            AsyncMock(return_value=50.0),
        ),
        patch.object(firecrawl_accounts, "_partner_post", partner),
    ):
        calls = (
            firecrawl_accounts.provision_firecrawl_account,
            firecrawl_accounts.firecrawl_key_for,
            firecrawl_accounts.provision_firecrawl_account,
        )
        for call in calls:
            with pytest.raises(AccountUnavailable) as refused:
                await call(USER)
            assert refused.value.code == "cap_reached"

    assert partner.await_count == 1


def test_a_count_that_is_not_a_number_is_refused():
    """A count the meter could not read fell back to the default, which
    bounds nothing when the vendor reads the value itself."""
    from src.server.services.egress import RelayError, RelayRejection
    from src.server.services.egress.firecrawl_catalog import Capability

    catalog = _catalog(search=Capability("S", 5.0, True, records_option="limit",
                                         records_default=2))
    with pytest.raises(RelayRejection) as refused:
        FirecrawlMeter().estimate(
            "alexandria_library_read",
            {"calls": [{"provider": "p", "capability": "search", "options": {"limit": "50"}}]},
            catalog,
        )
    assert refused.value.code is RelayError.BAD_REQUEST


@pytest.mark.asyncio
async def test_a_charge_is_stamped_by_its_transactions_last_statement():
    """``collected_at`` was the transaction's start, so a settle that began
    before a balance read but committed after it read as charged before."""
    from src.server.database import metered_vendor_calls

    order: list[str] = []
    conn = MagicMock()
    conn.transaction = lambda: _connection(None)

    async def _settle(c, hold_id, **kwargs):
        order.append("settle")
        return {"id": 9, "vendor": "firecrawl", "tool": "t", "vendor_credits": 1.0,
                "thread_id": "t", "thread_workspace_id": "w"}

    async def _write(**kwargs):
        order.append("usage")

    async def _stamp(c, ids):
        order.append(f"stamp {list(ids)}")

    with (
        patch("src.server.database.pool.get_db_connection", new=lambda: _connection(conn)),
        patch.object(metered_vendor_calls, "settle", _settle),
        patch.object(metered_vendor_calls, "stamp_collected", _stamp),
        patch("src.server.services.persistence.usage.persist_between_turns", new=_write),
    ):
        await metered_charges.settle_call(USER, 9, credits=1.0, job_ref=None)

    assert order == ["settle", "usage", "stamp [9]"]


@pytest.mark.asyncio
async def test_a_recording_past_the_budget_is_never_cut_off():
    """The budget cancelled the writes themselves: a free call's release cut
    off was swept at its estimate, and a transaction's rollback cut off."""
    from src.server.services.egress import metering

    released = asyncio.Event()

    async def _slow_release(hold_id):
        await asyncio.sleep(0.3)
        released.set()

    call = _call("firecrawl_map", {"url": "https://a.example"})
    call.dispatch.status = 500
    with (
        patch.object(metering, "release", _slow_release),
        patch.object(metering, "_FINISH_BUDGET_S", 0.05),
    ):
        await call.finish(500, complete=True)
        assert not released.is_set()
        await asyncio.wait_for(released.wait(), 2)


@pytest.mark.asyncio
async def test_a_rejected_platform_key_is_retryable_and_briefly_remembered():
    """One 401 cached a non-retryable refusal for every new user for five
    minutes."""
    from src.server.services import firecrawl_accounts

    stored: dict = {}

    class _Cache:
        async def set(self, key, value, ttl=None):
            stored[key] = (value, ttl)

    refusal = firecrawl_accounts._unavailable(MagicMock(status_code=401, json=lambda: {}))
    with patch("src.utils.cache.redis_cache.get_cache_client", lambda: _Cache()):
        await firecrawl_accounts._remember_refusal(USER, refusal)

    assert refusal.retryable
    ((value, ttl),) = stored.values()
    assert value == {"code": "platform_key_rejected", "retryable": True} and ttl <= 30


class _SweepCursor(_Cursor):
    def __init__(self, owed: list, charged: float):
        super().__init__([{"charged": charged}])
        self.owed = owed

    async def fetchall(self):
        sql = self.executed[-1][0]
        return list(self.owed) if "FOR UPDATE" in sql else []


def _stale(id_: int, created: int, estimate: float, job: str = "crawl:j") -> dict:
    from datetime import UTC, datetime

    return {"id": id_, "vendor": "firecrawl", "job_ref": job, "status": "held",
            "created_at": datetime(2026, 11, 15, 0, created, tzinfo=UTC),
            "vendor_credits": estimate}


async def _sweep(owed: list, charged: float) -> _SweepCursor:
    from src.server.database import metered_vendor_calls

    cursor = _SweepCursor(owed, charged=charged)
    conn = MagicMock()
    conn.cursor = lambda **kwargs: cursor
    await metered_vendor_calls.take_owed(conn, USER)
    return cursor


@pytest.mark.asyncio
async def test_the_sweep_nets_a_job_hold_under_the_jobs_lock_after_its_rows():
    """The sweep netted a stale job hold without the job's lock, so a poll
    settling at that moment was counted twice. The rows are locked first,
    as a settle locks its hold before the job, so the two cannot deadlock."""
    from src.server.database import metered_vendor_calls

    cursor = await _sweep([_stale(7, 1, 5.0)], charged=4.0)

    assert "FOR UPDATE" in cursor.executed[0][0]
    assert cursor.executed[1] == (
        "SELECT pg_advisory_xact_lock(%s)",
        (metered_vendor_calls.advisory_key("metered_vendor_job", USER, "firecrawl", "crawl:j"),),
    )
    assert "SUM(vendor_credits)" in cursor.executed[2][0]
    # Only the rows locked first are taken.
    assert cursor.executed[-1][1] == ([7],)


@pytest.mark.asyncio
async def test_a_jobs_stale_holds_are_netted_together_oldest_first():
    """A stale start and a stale poll of one job were each netted only
    against the job's charged rows, so they overlapped."""
    # The job has charged 4 so far; the start was held at 5, a later poll at 6.
    cursor = await _sweep([_stale(8, 2, 6.0), _stale(7, 1, 5.0)], charged=4.0)

    netted = [p for sql, p in cursor.executed if sql.startswith("UPDATE metered_vendor_calls SET")]
    # Oldest first, each against what the job charged before it: 5 - 4, then
    # 6 - 5. The job is charged 6 in all, its largest total, not 4 + 1 + 2.
    assert netted == [(1.0, 7), (1.0, 8)]


@pytest.mark.asyncio
async def test_a_stale_hold_its_job_already_covers_is_dropped():
    cursor = await _sweep([_stale(7, 1, 3.0)], charged=4.0)

    assert ("DELETE FROM metered_vendor_calls WHERE id = %s", (7,)) in cursor.executed


# ---------------------------------------------------------------------------
# A balance that is not a number, and a crawl total whose settle never landed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("spendable", [float("nan"), float("inf"), "nan", "-inf", "lots", [3]])
async def test_a_balance_that_is_not_a_finite_number_is_a_check_to_retry(spendable):
    """NaN passed through, and every comparison with it is false, so the
    owed cap bounded nothing; a non-numeric value raised a 500."""
    from src.server.services.egress.metering import spendable_for_paid_calls
    from src.server.services.egress import RelayRejection

    with patch(
        "src.server.dependencies.usage_limits._admitted_quota",
        new=AsyncMock(return_value={"allowed": True, "spendable_credits": spendable}),
    ):
        with pytest.raises(RelayRejection) as refused:
            await spendable_for_paid_calls(USER)

    assert refused.value.code.value == "credit_check_unavailable"
    assert refused.value.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize(("spendable", "read"), [(12.5, 12.5), ("3", 3.0), (-2, -2.0), (None, None)])
async def test_a_finite_balance_is_read_as_sent(spendable, read):
    from src.server.services.egress.metering import spendable_for_paid_calls

    with patch(
        "src.server.dependencies.usage_limits._admitted_quota",
        new=AsyncMock(return_value={"allowed": True, "spendable_credits": spendable}),
    ):
        assert await spendable_for_paid_calls(USER) == read


@pytest.mark.asyncio
async def test_a_crawl_poll_whose_settle_fails_keeps_its_reported_total_for_the_sweep():
    """The hold kept its admission estimate when its settle failed, so the
    sweep netted that against what the job had charged and the poll's
    reported growth was lost."""
    call = _call("firecrawl_check_crawl_status", {"id": "job-1"})
    call.dispatch.status = 200
    call.observe(_reply({"status": "scraping", "id": "job-1", "creditsUsed": 9, "data": []}))
    mark = AsyncMock()
    with (
        patch(
            "src.server.services.egress.metering.settle_call",
            new=AsyncMock(side_effect=RuntimeError("pool closed")),
        ),
        patch("src.server.services.egress.metering.mark_job", new=mark),
        patch("src.server.services.egress.metering.stage", new=AsyncMock()),
    ):
        await call.finish(200, complete=True)

    mark.assert_awaited_once_with(7, "crawl:job-1", 9.0)
    # The job had charged 4 on earlier polls: the sweep charges the growth.
    cursor = await _sweep([_stale(7, 1, 9.0, job="crawl:job-1")], charged=4.0)
    netted = [p for sql, p in cursor.executed if sql.startswith("UPDATE metered_vendor_calls SET")]
    assert netted == [(5.0, 7)]


@pytest.mark.asyncio
async def test_mark_job_records_a_reported_total_and_keeps_the_estimate_otherwise():
    from src.server.database import metered_vendor_calls

    conn = MagicMock()
    conn.execute = AsyncMock()
    with patch.object(metered_vendor_calls, "get_db_connection", new=lambda: _connection(conn)):
        await metered_vendor_calls.mark_job(7, "crawl:j", 9.0)
        await metered_vendor_calls.mark_job(7, "crawl:j", None)

    (sql, reported), (_, unreported) = (c.args for c in conn.execute.await_args_list)
    assert "vendor_credits = COALESCE(%s, vendor_credits)" in sql
    assert "WHERE id = %s AND status = 'held'" in sql
    assert reported == ("crawl:j", 9.0, 7) and unreported == ("crawl:j", None, 7)


def test_a_crawls_new_pages_share_what_its_held_pages_leave_of_the_total():
    """A poll's even split was kept only for its new pages, so a page held at
    1 from an earlier poll and one new at 2.5 showed 3.5 of a 5 credit job."""
    from ptc_agent.agent.provenance.trace import RelayedSource
    from src.server.database.relayed_sources import _share_rest

    batch = [RelayedSource("web_fetch", u, cost_credits=2.5) for u in ("a", "b")]

    fresh = _share_rest(batch[1:], batch, held_cost=1.0)

    assert [s.cost_credits for s in fresh] == [4.0]
    assert _share_rest(batch[1:], [RelayedSource("web_fetch", "a")], 1.0) == batch[1:]


@pytest.mark.parametrize("reported", [-1, -0.5])
def test_a_negative_reported_cost_settles_at_the_estimate(reported):
    """Taken as is, a negative cost settled the hold as free."""
    assert FirecrawlMeter().charge(
        "firecrawl_scrape",
        {"url": "https://example.com"},
        {"markdown": "x", "metadata": {"creditsUsed": reported}},
    ) is None


@pytest.mark.parametrize(
    ("arguments", "held"),
    [
        # 12 results: Firecrawl billed this 4 credits live (2026-11-16).
        ({"limit": 6, "sources": [{"type": "web"}, {"type": "news"}]}, 4.0),
        ({"limit": 6, "sources": ["web", "news", "images"]}, 4.0),
        ({"limit": 10, "sources": ["web", "news"], "scrapeOptions": {}}, 24.0),
        ({"limit": 10, "enterprise": ["zdr"]}, 10.0),
        ({"limit": 10, "enterprise": ["zdr"], "scrapeOptions": {}}, 30.0),
        ({"limit": 10, "enterprise": ["anon"]}, 2.0),
    ],
)
def test_a_search_is_held_for_every_source_and_its_tier(arguments, held):
    """The hold read ``limit`` as the whole result count and ignored the
    enterprise tier, admitting searches that cost several times the hold."""
    assert FirecrawlMeter().estimate("firecrawl_search", {"query": "q", **arguments}, {}) == held


# ---------------------------------------------------------------------------
# A free call's release that fails, and admission's two reads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_free_calls_failed_release_is_retried_behind_the_reply():
    """One failed release left the hold for the sweep to charge at its
    estimate, for a call that cost nothing."""
    from src.server.services.egress import metering

    call = _call("firecrawl_map", {"url": "https://a.example"})
    call.dispatch.status = 500
    release = AsyncMock(side_effect=[RuntimeError("pool closed"), RuntimeError("again"), None])
    waits = asyncio.Event()
    real_sleep = asyncio.sleep

    async def _sleep(delay):
        await waits.wait()

    with (
        patch.object(metering, "release", release),
        patch.object(metering.asyncio, "sleep", _sleep),
    ):
        await call.finish(500, complete=True)
        # The route is back after the first try; the retries run behind it.
        assert release.await_count == 1
        waits.set()
        async with asyncio.timeout(1):
            while release.await_count < 3:
                await real_sleep(0)

    assert [c.args for c in release.await_args_list] == [(7,)] * 3


@pytest.mark.asyncio
async def test_admissions_reads_overlap_and_a_balance_refusal_still_comes_first():
    from src.server.services.egress import RelayError, RelayRejection, metering

    both = asyncio.Barrier(2)

    async def _balance(user_id):
        await both.wait()
        raise RelayRejection(402, RelayError.CREDITS_EXHAUSTED, "out")

    async def _catalog(*args, **kwargs):
        await both.wait()
        raise RuntimeError("catalog down")

    prepared = _prepared("firecrawl_map", {"url": "https://a.example"})
    with (
        patch.object(metering, "spendable_for_paid_calls", _balance),
        patch.object(prepared.platform.account.meter, "load_catalog", _catalog),
        patch.object(metering, "user_credits", lambda meter, n: float(n)),
    ):
        with pytest.raises(RelayRejection) as refused:
            await asyncio.wait_for(metering.admit(prepared, {}), 1)

    assert refused.value.code is RelayError.CREDITS_EXHAUSTED
