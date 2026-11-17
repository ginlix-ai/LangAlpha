"""Regressions in reading Firecrawl's catalog during admission.

Providers were fetched one at a time with a fresh client, a name Firecrawl
does not list failed the whole read and was asked about again on every call,
concurrent misses each fetched the same provider, and nothing bounded the read
inside the relay's clock.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from src.server.services.egress import firecrawl_catalog
from src.server.services.egress.firecrawl_catalog import (
    CatalogUnavailable,
    Capability,
    Provider,
)


class _Cache:
    def __init__(self):
        self.store: dict[str, object] = {}
        self.mgets = 0

    async def mget(self, keys):
        self.mgets += 1
        return [self.store.get(k) for k in keys]

    async def set(self, key, value, ttl=None):
        self.store[key] = value
        return True

    async def acquire_lock(self, key, token, ttl_ms):
        return True


@pytest.fixture
def cache():
    c = _Cache()
    with patch("src.utils.cache.redis_cache.get_cache_client", lambda: c):
        yield c


def _fetching(calls: list[str], *, delay: float = 0.05, unknown=frozenset()):
    async def _fetch(client, target, headers, name):
        calls.append(name)
        await asyncio.sleep(delay)
        if name in unknown:
            return None
        return Provider(name=name, domain="", capabilities={
            "c": Capability(name="C", credits_cost=1.0, per_record=False)})

    async def _pin(url, **kwargs):
        return object()

    return (
        patch.object(firecrawl_catalog, "_fetch", _fetch),
        patch("src.server.utils.egress_guard.pin_public_url", _pin),
        patch("src.server.services.egress.relay.get_relay_client", lambda: None),
    )


@pytest.mark.asyncio
async def test_concurrent_misses_share_one_fetch_per_provider(cache):
    calls: list[str] = []
    a, b, c = _fetching(calls)
    with a, b, c:
        results = await asyncio.gather(*(
            firecrawl_catalog.providers(["fred", "eodhd"], url="https://x", headers={})
            for _ in range(5)
        ))

    assert sorted(calls) == ["eodhd", "fred"]
    assert all(set(r) == {"fred", "eodhd"} for r in results)
    # One round trip per read for what is cached.
    assert cache.mgets == 5


@pytest.mark.asyncio
async def test_an_unlisted_provider_is_left_out_and_remembered(cache):
    calls: list[str] = []
    a, b, c = _fetching(calls, unknown={"nope"})
    with a, b, c:
        first = await firecrawl_catalog.providers(["fred", "nope"], url="https://x", headers={})
        second = await firecrawl_catalog.providers(["nope"], url="https://x", headers={})

    assert set(first) == {"fred"} and second == {}
    assert calls.count("nope") == 1


@pytest.mark.asyncio
async def test_a_hung_catalog_is_unavailable_within_its_budget(cache):
    calls: list[str] = []
    a, b, c = _fetching(calls, delay=30)
    with a, b, c, patch.object(firecrawl_catalog, "_BUDGET_S", 0.1):
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(CatalogUnavailable):
            await firecrawl_catalog.providers(["fred"], url="https://x", headers={})

    assert loop.time() - started < 1


def test_a_per_record_capability_keeps_its_declared_count():
    cap = firecrawl_catalog._capability({
        "capability": "companies/search", "name": "Search", "creditsCost": 5, "perRecord": True,
        "options": [{"name": "offset", "type": "number", "max": 10000},
                    {"name": "limit", "type": "number", "default": 2, "max": 6}],
    })

    assert (cap.records_option, cap.records_default) == ("limit", 2)


# ---------------------------------------------------------------------------
# Second review
# ---------------------------------------------------------------------------


def test_a_long_or_junk_run_list_is_refused_before_any_lookup():
    """Thousands of runs naming made-up providers each cost two catalog
    requests and a DNS lookup on the relay's shared client, at once."""
    from src.server.services.egress import RelayError
    from src.server.services.egress.firecrawl_meter import FirecrawlMeter

    meter = FirecrawlMeter()
    many = {"calls": [{"provider": f"p{i}", "capability": "c"} for i in range(11)]}
    junk = {"calls": [{"provider": "../../x?y", "capability": "c"}]}

    for arguments in (many, junk):
        refusal = meter.refusal("alexandria_library_read", arguments)
        assert refusal is not None and refusal.code is RelayError.BAD_REQUEST


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"calls": []},
        {"calls": {"provider": "acme", "capability": "c"}},
        {"calls": [{"provider": "acme", "capability": "c"}, "junk"]},
        {"calls": [{"provider": "acme", "capability": "c"}],
         "alexandria": [{"provider": "acme", "capability": "big"}]},
        {"calls": [{"provider": "acme", "capability": "c", "version": "v1"}]},
    ],
    ids=["no-calls", "empty", "not-a-list", "unreadable-run", "second-key", "pinned-version"],
)
def test_a_catalog_frame_the_meter_cannot_fully_price_is_refused(arguments):
    """Each was held at the one-credit minimum while Firecrawl could run
    more than the meter read: a run it skipped, runs under a key it does
    not read, or a pinned version priced at the current one's price."""
    from src.server.services.egress import RelayError
    from src.server.services.egress.firecrawl_meter import FirecrawlMeter

    refusal = FirecrawlMeter().refusal("alexandria_library_read", arguments)
    assert refusal is not None and refusal.code is RelayError.BAD_REQUEST


@pytest.mark.asyncio
async def test_fetches_are_bounded_and_junk_names_never_looked_up(cache):
    calls: list[str] = []
    running = peak = 0

    async def _fetch(client, target, headers, name):
        nonlocal running, peak
        calls.append(name)
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1
        return None

    a, b, c = _fetching([])
    with a, b, c, patch.object(firecrawl_catalog, "_fetch", _fetch):
        await firecrawl_catalog.providers(
            [f"p{i}" for i in range(10)] + ["Not An Id", "x" * 200],
            url="https://x", headers={},
        )
        with pytest.raises(ValueError):
            await firecrawl_catalog.providers(
                [f"q{i}" for i in range(11)], url="https://x", headers={}
            )

    assert sorted(calls) == sorted(f"p{i}" for i in range(10))
    assert peak <= firecrawl_catalog._FETCHES_AT_ONCE


class _Response:
    status_code = 200

    def __init__(self, run):
        import json

        text = json.dumps({"data": {"alexandria": [run]}})
        self.content = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": text}]}}
        ).encode()


class _Client:
    def __init__(self, answer):
        self.answer = answer

    async def post(self, url, *, headers, extensions, json):
        return _Response(self.answer(json["params"]["arguments"]))


class _Target:
    def pinned_kwargs(self, headers):
        return "https://x", dict(headers), {}


def _rejected(name):
    return {"error": {"code": "invalid_option",
                      "message": f'Unknown or unavailable providers. Rejected: "{name}".'}}


@pytest.mark.asyncio
async def test_only_a_clean_providers_answer_is_a_miss():
    """A refused tools page, or a listing without the entry, was cached as a
    miss for ten minutes and answered as a final "not in the catalog"."""
    listed = {"data": {"items": [{"provider": "fred", "name": "FRED"}]}}

    def tools_refused(args):
        return _rejected("fred") if args["level"] == "tools" else listed

    def absent_from_listing(args):
        return {"data": {"items": []}} if args["level"] == "providers" else {
            "data": {"items": [{"provider": "fred", "capability": "c", "creditsCost": 1}]}}

    def unlisted(args):
        return _rejected("nope")

    for answer in (tools_refused, absent_from_listing):
        with pytest.raises(CatalogUnavailable):
            await firecrawl_catalog._fetch(_Client(answer), _Target(), {}, "fred")
    assert await firecrawl_catalog._fetch(_Client(unlisted), _Target(), {}, "nope") is None


def test_count_options_are_read_as_the_catalog_spells_them():
    cap = firecrawl_catalog._capability({
        "capability": "s", "creditsCost": 1, "perRecord": True,
        "options": [{"name": "pageSize", "type": "integer", "max": 25}],
    })

    assert (cap.records_option, cap.records_default) == ("pageSize", 25)


def test_a_runs_identifier_is_redacted_as_its_args_are():
    """A credential-valued option reached the stored and shown identifier,
    though the same run's ``args`` hid it."""
    from src.server.services.egress.firecrawl_meter import FirecrawlMeter

    catalog = {"acme": Provider(name="Acme", domain="acme.example", capabilities={
        "quotes": Capability(name="Quotes", credits_cost=1.0, per_record=False)})}
    arguments = {"calls": [{"provider": "acme", "capability": "quotes",
                            "options": {"ticker": "AAPL", "api_key": "sk-live-123"}}]}
    result = {"data": {"alexandria": [
        {"provider": "acme", "capability": "quotes", "data": {"px": 1}}]}}

    sources, _ = FirecrawlMeter().sources(
        "alexandria_library_read", arguments, result, catalog=catalog, call_cost=1.0
    )

    assert "sk-live-123" not in sources[0].identifier
    assert "ticker=AAPL" in sources[0].identifier


def test_a_catalog_calls_sources_add_up_to_its_rounded_charge():
    """Each run showed its raw price, so two runs at 0.5 and 0.25 credits
    showed 0.75 between them while the call was billed at 1."""
    from src.server.services.egress.firecrawl_meter import FirecrawlMeter

    catalog = {"acme": Provider(name="Acme", domain="acme.example", capabilities={
        "quotes": Capability(name="Quotes", credits_cost=0.5, per_record=False),
        "news": Capability(name="News", credits_cost=0.25, per_record=False)})}
    arguments = {"calls": [
        {"provider": "acme", "capability": "quotes", "options": {"ticker": "AAPL"}},
        {"provider": "acme", "capability": "news", "options": {"ticker": "AAPL"}},
    ]}
    result = {"data": {"creditsCost": 0.75, "alexandria": [
        {"provider": "acme", "capability": "quotes", "data": {"px": 1}},
        {"provider": "acme", "capability": "news", "data": {"items": []}},
    ]}}

    sources, _ = FirecrawlMeter().sources(
        "alexandria_library_read", arguments, result, catalog=catalog, call_cost=1.0
    )

    assert [s.vendor_credits for s in sources] == pytest.approx([2 / 3, 1 / 3])
