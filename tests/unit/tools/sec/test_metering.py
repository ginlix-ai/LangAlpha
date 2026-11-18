"""Request meter: path classes, interval math, and hit/revalidated/network classification."""

from __future__ import annotations

import asyncio
import json
from email.utils import formatdate

import pytest
from httpxthrottlecache._compat import httpx
from pyrate_limiter import Limiter, Rate
from httpxthrottlecache.filecache.transport import CachingTransport
from httpxthrottlecache.ratelimiter import (
    AsyncRateLimitingTransport,
    RateLimitingTransport,
    create_rate_limiter,
)

from src.tools.sec import metering
from src.tools.sec.metering import (
    CallMeter,
    classify_path,
    current_meter,
    fmp_event_hooks,
    install_hooks,
    metered_call,
    note_cache,
    overlap,
)

RULES = {r"www\.sec\.gov": {r"/Archives/.*": 3600}}
URL = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.txt"
LAST_MODIFIED = formatdate(timeval=1_700_000_000, usegmt=True)


@pytest.mark.parametrize(
    ("host", "path", "expected"),
    [
        ("www.sec.gov", "/files/company_tickers.json", "ticker_map"),
        ("www.sec.gov", "/files/company_tickers_exchange.json", "ticker_map"),
        ("www.sec.gov", "/include/ticker.txt", "ticker_map"),
        ("data.sec.gov", "/submissions/CIK0000320193.json", "submissions"),
        ("data.sec.gov", "/api/xbrl/companyfacts/CIK0000320193.json", "companyfacts"),
        ("www.sec.gov", "/Archives/edgar/data/320193/0000320193-24-000123.txt", "filing_text"),
        ("www.sec.gov", "/Archives/edgar/data/320193/000032019324000123/0000320193-24-000123-index.htm", "filing_index"),
        ("www.sec.gov", "/Archives/edgar/data/320193/000032019324000123/index.json", "filing_index"),
        ("www.sec.gov", "/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm", "filing_document"),
        ("efts.sec.gov", "/LATEST/search-index", "full_text_search"),
        ("financialmodelingprep.com", "/stable/earning-call-transcript-dates", "fmp_call_dates"),
        ("financialmodelingprep.com", "/api/v4/earning_call_transcript_dates", "fmp_call_dates"),
        ("financialmodelingprep.com", "/stable/earning-call-transcript", "fmp_transcript"),
        ("financialmodelingprep.com", "/stable/quote", "fmp_other"),
        ("www.sec.gov", "/cgi-bin/browse-edgar", "other"),
        ("example.com", "/Archives/edgar/data/1/x.txt", "other"),
    ],
)
def test_classify_path(host, path, expected):
    assert classify_path(host, path) == expected


def test_union_and_overlap():
    union, peak = overlap([(0.0, 1.0), (0.5, 1.5), (3.0, 4.0), (4.0, 5.0)])
    assert union == pytest.approx(3500.0)
    assert peak == 2
    assert overlap([]) == (0.0, 0)


def test_summary_overlap_and_cache_counters():
    meter = CallMeter()
    base = meter.started
    for start, end, cache in [(0.0, 1.0, "network"), (0.5, 1.5, "network"), (2.0, 2.5, "revalidated"), (0.1, 0.1, "hit")]:
        meter.add(
            metering.RequestRecord(
                host="www.sec.gov",
                path_class="filing_text",
                started=base + start,
                ended=base + end,
                status=200,
                bytes=10,
                cache=cache,
            )
        )
    meter.cache("sections", "hit")
    meter.cache("sections", "miss")
    summary = meter.summary()
    assert summary["network_count"] == 3
    assert summary["http_cache_hits"] == 1
    assert summary["revalidations"] == 1
    assert summary["network_ms_sum"] == pytest.approx(2500.0)
    assert summary["critical_path_ms"] == pytest.approx(2000.0)
    assert summary["max_concurrency"] == 2
    assert summary["bytes_in"] == 20
    assert summary["cache"]["sections"] == {"hit": 1, "miss": 1, "hit_rate": 0.5}


class _Body(httpx.SyncByteStream, httpx.AsyncByteStream):
    def __iter__(self):
        yield b"x" * 500

    async def __aiter__(self):
        yield b"x" * 500


class _Backend:
    """Stands in for the network under the rate limiter; answers 304 to a conditional GET."""

    calls = 0

    def __call__(self, request):
        _Backend.calls += 1
        if "if-modified-since" in request.headers:
            return httpx.Response(304, request=request)
        return httpx.Response(
            200,
            stream=_Body(),
            headers={"Last-Modified": LAST_MODIFIED, "Date": formatdate(usegmt=True)},
            request=request,
        )


@pytest.fixture
def hooked(monkeypatch):
    install_hooks()
    install_hooks()
    backend = _Backend()
    _Backend.calls = 0
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", lambda self, request: backend(request))

    async def async_backend(self, request):
        return backend(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", async_backend)
    return backend


def _sync_client(tmp_path):
    inner = RateLimitingTransport(create_rate_limiter(1000))
    return httpx.Client(transport=CachingTransport(tmp_path, RULES, transport=inner))


def test_hit_revalidated_network_classification(hooked, tmp_path):
    client = _sync_client(tmp_path)
    with metered_call(case="sync") as meter:
        assert len(client.get(URL).content) == 500
        assert len(client.get(URL).content) == 500
        for meta in tmp_path.rglob("*.meta"):
            meta.write_text(json.dumps({**json.loads(meta.read_text()), "fetched": 0}))
        assert client.get(URL).status_code == 304
    outcomes = [r.cache for r in meter.records]
    assert outcomes == ["network", "hit", "revalidated"]
    network = meter.records[0]
    assert network.bytes == 500
    assert network.limiter_wait_ms is not None
    assert network.ended >= network.started
    assert meter.records[1].limiter_wait_ms is None
    assert _Backend.calls == 2
    summary = meter.summary()
    assert (summary["network_count"], summary["http_cache_hits"], summary["revalidations"]) == (2, 1, 1)
    assert summary["requests_by_host"] == {"www.sec.gov": {"network": 1, "hit": 1, "revalidated": 1}}


@pytest.mark.asyncio
async def test_async_classification_and_stream_stays_unread(hooked, tmp_path):
    inner = AsyncRateLimitingTransport(create_rate_limiter(1000))
    client = httpx.AsyncClient(transport=CachingTransport(tmp_path, RULES, transport=inner))
    with metered_call(case="async") as meter:
        request = client.build_request("GET", URL)
        response = await client.send(request, stream=True)
        assert not response.is_stream_consumed
        assert meter.records[0].cache == "network"
        await response.aread()
        await response.aclose()
        await client.get(URL)
    assert [r.cache for r in meter.records] == ["network", "hit"]
    assert meter.records[0].bytes == 500
    await client.aclose()


def test_no_meter_is_passthrough(hooked, tmp_path):
    client = _sync_client(tmp_path)
    assert current_meter.get() is None
    assert client.get(URL).status_code == 200


def test_metered_call_logs_error_and_propagates(caplog):
    with caplog.at_level("INFO", logger=metering.logger.name):
        with pytest.raises(ValueError):
            with metered_call(symbol="AAPL"):
                raise ValueError("boom")
    record = next(r for r in caplog.records if r.getMessage() == "sec_filing.metrics")
    assert record.sec_filing_metrics["error"].startswith("ValueError")
    assert record.sec_filing_case == {"symbol": "AAPL"}
    assert current_meter.get() is None


def test_error_names_the_type_and_status_never_the_url(caplog):
    url = "https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey=SECRET"
    request = httpx.Request("GET", url)
    exc = httpx.HTTPStatusError(f"429 for {url}", request=request, response=httpx.Response(429, request=request))
    with caplog.at_level("INFO", logger=metering.logger.name):
        with pytest.raises(httpx.HTTPStatusError):
            with metered_call():
                raise exc
    record = next(r for r in caplog.records if r.getMessage() == "sec_filing.metrics")
    assert record.sec_filing_metrics["error"] == "HTTPStatusError 429"


def test_a_handled_failure_and_cache_notes_reach_the_summary(caplog):
    note_cache("fmp_transcript", "hit")
    with caplog.at_level("INFO", logger=metering.logger.name):
        with metered_call() as meter:
            note_cache("fmp_transcript", "miss")
            meter.fail("NotAFiler")
    record = next(r for r in caplog.records if r.getMessage() == "sec_filing.metrics")
    assert record.sec_filing_metrics["error"] == "NotAFiler"
    assert record.sec_filing_metrics["cache"] == {"fmp_transcript": {"miss": 1, "hit_rate": 0.0}}


@pytest.mark.asyncio
async def test_limiter_wait_is_not_network_time(hooked, tmp_path):
    inner = AsyncRateLimitingTransport(Limiter(Rate(1, 200)))
    client = httpx.AsyncClient(transport=CachingTransport(tmp_path, RULES, transport=inner))
    with metered_call() as meter:
        await asyncio.gather(client.get(URL), client.get(URL.replace(".txt", "-2.txt")))
    await client.aclose()
    summary = meter.summary()
    assert summary["network_count"] == 2
    assert summary["max_concurrency"] == 1
    assert summary["limiter_wait_ms_sum"] >= 150
    assert summary["network_ms_sum"] < 100


@pytest.mark.asyncio
async def test_fmp_hooks_record_into_meter():
    import httpx as fmp_httpx

    def handler(request):
        return fmp_httpx.Response(200, json={"ok": 1})

    with metered_call() as meter:
        async with fmp_httpx.AsyncClient(
            transport=fmp_httpx.MockTransport(handler), event_hooks=fmp_event_hooks()
        ) as client:
            await client.get("https://financialmodelingprep.com/stable/earning-call-transcript?symbol=AAPL")
    (record,) = meter.records
    assert (record.path_class, record.status, record.cache) == ("fmp_transcript", 200, "network")
    assert record.bytes > 0 and record.ended is not None


@pytest.mark.asyncio
async def test_an_fmp_attempt_that_failed_ends_when_it_is_retried():
    import httpx as fmp_httpx

    attempts = []

    def handler(request):
        attempts.append(1)
        if len(attempts) == 1:
            raise fmp_httpx.ConnectError("reset", request=request)
        return fmp_httpx.Response(200, json={"ok": 1})

    url = "https://financialmodelingprep.com/stable/earning-call-transcript?symbol=AAPL"
    with metered_call() as meter:
        async with fmp_httpx.AsyncClient(transport=fmp_httpx.MockTransport(handler), event_hooks=fmp_event_hooks()) as c:
            with pytest.raises(fmp_httpx.ConnectError):
                await c.get(url)
            await c.get(url)
    failed, retried = meter.records
    assert failed.status is None and failed.ended is not None and failed.ended <= retried.started
    assert retried.status == 200


def test_hooks_install_is_idempotent():
    install_hooks()
    wrapped = CachingTransport.handle_request
    install_hooks()
    assert CachingTransport.handle_request is wrapped
    assert wrapped.__wrapped__ is not wrapped
