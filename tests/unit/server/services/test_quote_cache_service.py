"""QuoteCacheService — per-symbol keys, batched fill, dedup, negative cache."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from market_protocol import to_canonical
from src.server.services.cache import quote_cache_service as qcs
from src.server.services.cache.quote_cache_service import QuoteCacheService, _quote_ttl


def _row(symbol: str, price: float = 100.0) -> dict:
    return {"symbol": symbol, "name": f"{symbol} Inc", "price": price, "change": 1.0}


def _refs(*symbols: str, index: bool = False):
    """Refs as the snapshot route resolves them for the stocks or indexes endpoint."""
    hint = qcs.AssetClass.INDEX if index else qcs.AssetClass.EQUITY
    return [to_canonical(s, asset_class=hint) for s in symbols]


def _symbols(quotes) -> list[str]:
    return [row["symbol"] for _, row in quotes]


class _StubCache:
    def __init__(self) -> None:
        self.store: dict[str, dict] = {}
        self.set_ttls: dict[str, int] = {}
        self.set_many_calls = 0

    async def get(self, key):
        return self.store.get(key)

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def set(self, key, value, ttl=None):
        self.store[key] = value
        self.set_ttls[key] = ttl

    async def set_many(self, items):
        self.set_many_calls += 1
        for key, value, ttl in items:
            self.store[key] = value
            self.set_ttls[key] = ttl
        return True


class _StubProvider:
    def __init__(self, rows: dict[str, dict], gate: asyncio.Event | None = None):
        self.rows = rows
        self.gate = gate
        self.calls: list[list[str]] = []
        self.started = asyncio.Event()  # set once a call enters — gate on it, not sleeps

    async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
        self.calls.append(list(symbols))
        self.started.set()
        if self.gate is not None:
            await self.gate.wait()
        return [self.rows[s] for s in symbols if s in self.rows]


class _AssetTypeRecordingProvider:
    """Records the asset_type of each snapshot call; returns the row only when
    asked with asset_type='indices' (mimics an index-only upstream)."""

    def __init__(self, symbol: str, row: dict):
        self._symbol = symbol
        self._row = row
        self.calls: list[tuple[list[str], str]] = []

    async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
        self.calls.append((list(symbols), asset_type))
        if asset_type == "indices":
            return [self._row for s in symbols if s == self._symbol]
        return []


@pytest.fixture
def service(monkeypatch):
    QuoteCacheService._instance = None
    cache = _StubCache()
    monkeypatch.setattr(qcs, "get_cache_client", lambda: cache)

    def install(provider):
        async def _get():
            return provider
        monkeypatch.setattr(qcs, "get_market_data_provider", _get)

    yield QuoteCacheService.get_instance(), cache, install
    QuoteCacheService._instance = None


@pytest.mark.asyncio
async def test_misses_fill_via_one_batched_call(service):
    svc, cache, install = service
    provider = _StubProvider({"AAPL": _row("AAPL"), "MSFT": _row("MSFT")})
    install(provider)

    out = await svc.get_quotes(_refs("AAPL", "MSFT"))
    assert _symbols(out) == ["AAPL", "MSFT"]
    assert provider.calls == [["AAPL", "MSFT"]]
    # ONE pipelined write-through, not a connection per symbol: the batch cap
    # is 250, which alone exceeded the 150-slot pool.
    assert cache.set_many_calls == 1

    # Second request: pure cache hits, no upstream call.
    out = await svc.get_quotes(_refs("AAPL", "MSFT"))
    assert len(out) == 2 and len(provider.calls) == 1


@pytest.mark.asyncio
async def test_partial_hit_fetches_only_misses(service):
    svc, cache, install = service
    provider = _StubProvider({"AAPL": _row("AAPL"), "MSFT": _row("MSFT")})
    install(provider)
    await svc.get_quotes(_refs("AAPL"))

    out = await svc.get_quotes(_refs("AAPL", "MSFT"))
    assert _symbols(out) == ["AAPL", "MSFT"]
    assert provider.calls == [["AAPL"], ["MSFT"]]


@pytest.mark.asyncio
async def test_concurrent_requests_share_one_fetch(service):
    svc, cache, install = service
    gate = asyncio.Event()
    provider = _StubProvider({"AAPL": _row("AAPL")}, gate=gate)
    install(provider)

    t1 = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    t2 = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await asyncio.sleep(0.01)  # both admitted; one leader, one follower
    gate.set()
    r1, r2 = await asyncio.gather(t1, t2)
    assert r1 == r2 and _symbols(r1) == ["AAPL"]
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_unknown_symbol_dropped_and_negative_cached(service):
    svc, cache, install = service
    provider = _StubProvider({"AAPL": _row("AAPL")})
    install(provider)

    out = await svc.get_quotes(_refs("AAPL", "ZZZFAKE"))
    assert _symbols(out) == ["AAPL"]
    # Negative sentinel written with short TTL...
    neg_key = svc.quote_key(to_canonical("ZZZFAKE"))
    assert cache.store[neg_key] == {"__no_data__": True}
    assert cache.set_ttls[neg_key] == qcs._TTL_NEGATIVE
    # ...so the repeat does not re-fan out upstream.
    out = await svc.get_quotes(_refs("AAPL", "ZZZFAKE"))
    assert _symbols(out) == ["AAPL"]
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_spellings_collapse_to_one_key(service):
    svc, cache, install = service
    provider = _StubProvider({"GSPC": _row("GSPC")})
    install(provider)

    out = await svc.get_quotes(_refs("^GSPC", "GSPC", "I:SPX", index=True))
    assert len(out) == 1
    assert provider.calls == [["GSPC"]]
    assert svc.quote_key(to_canonical("GSPC", asset_class=qcs.AssetClass.INDEX)) in cache.store


@pytest.mark.asyncio
async def test_provider_error_propagates_and_clears_inflight(service):
    svc, cache, install = service

    class _Boom:
        async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
            raise RuntimeError("upstream down")

    install(_Boom())
    with pytest.raises(RuntimeError):
        await svc.get_quotes(_refs("AAPL"))
    assert svc._inflight == {}

    # Recovers on the next call once the provider is healthy again.
    provider = _StubProvider({"AAPL": _row("AAPL")})
    install(provider)
    out = await svc.get_quotes(_refs("AAPL"))
    assert _symbols(out) == ["AAPL"]


@pytest.mark.asyncio
async def test_leader_cancellation_clears_inflight_and_recovers(service):
    """Cancelling the batch leader must clear _inflight — a stranded future
    would make every later request a follower awaiting a dead fetch forever."""
    svc, cache, install = service
    gate = asyncio.Event()
    provider = _StubProvider({"AAPL": _row("AAPL")}, gate=gate)
    install(provider)

    leader = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await provider.started.wait()  # leader is blocked inside the provider call

    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader
    assert svc._inflight == {}  # cancelled leader stranded no per-symbol future

    # A fresh request with a healthy provider completes (no hang on a dead future).
    provider2 = _StubProvider({"AAPL": _row("AAPL")})
    install(provider2)
    out = await svc.get_quotes(_refs("AAPL"))
    assert _symbols(out) == ["AAPL"]
    assert provider2.calls == [["AAPL"]]


@pytest.mark.asyncio
async def test_follower_survives_leader_cancellation(service):
    """A follower awaiting the leader's shared future must not die with the
    leader — one client disconnect would otherwise fail every concurrent
    request that deduped onto the same symbol."""
    svc, cache, install = service
    gate = asyncio.Event()
    provider = _StubProvider({"AAPL": _row("AAPL")}, gate=gate)
    install(provider)

    leader = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await provider.started.wait()  # leader is blocked inside the provider call
    follower = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await asyncio.sleep(0.01)  # follower admitted, awaiting the shared future

    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader

    # Miss for this cycle (next poll refetches) — not a CancelledError.
    assert await follower == []


@pytest.mark.asyncio
async def test_follower_survives_leader_provider_error(service):
    """The leader's upstream failure is the leader's error to raise; a
    follower treats the errored shared future as a miss."""
    svc, cache, install = service
    started = asyncio.Event()
    gate = asyncio.Event()

    class _BlockingBoom:
        async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
            started.set()
            await gate.wait()
            raise RuntimeError("upstream down")

    install(_BlockingBoom())
    leader = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await started.wait()
    follower = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await asyncio.sleep(0.01)
    gate.set()

    with pytest.raises(RuntimeError):
        await leader
    assert await follower == []


@pytest.mark.asyncio
async def test_a_failing_partition_misses_only_its_own_symbols(service):
    """An index outage in a mixed watchlist batch failed the stock half too,
    and every request deduped onto those symbols with it. The failed symbols
    are a miss for this cycle, never negative-cached: an outage is not "no
    provider has it"."""
    svc, cache, install = service
    started = asyncio.Event()
    gate = asyncio.Event()

    class _IndexesDown:
        async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
            started.set()
            await gate.wait()
            if asset_type == "indices":
                raise RuntimeError("503 Service Unavailable")
            return [_row(s) for s in symbols]

    install(_IndexesDown())
    refs = [*_refs("AAPL"), *_refs("^GSPC", index=True)]
    leader = asyncio.create_task(svc.get_quotes(refs))
    await started.wait()
    follower = asyncio.create_task(svc.get_quotes(refs))
    await asyncio.sleep(0.01)
    gate.set()

    assert _symbols(await leader) == ["AAPL"]
    assert _symbols(await follower) == ["AAPL"]
    assert svc.quote_key(refs[1]) not in cache.store
    assert svc._inflight == {}


@pytest.mark.asyncio
async def test_an_empty_partition_does_not_hide_another_partitions_outage(service):
    """A stock outage beside an index no provider has answered 200 with no
    rows: the empty index answer counted as the batch answering, so the
    outage read as unknown symbols. Only a row makes an outage moot."""
    svc, cache, install = service

    class _StocksDownIndexUnknown:
        async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
            if asset_type == "stocks":
                raise RuntimeError("503 Service Unavailable")
            return []

    install(_StocksDownIndexUnknown())
    refs = [*_refs("AAPL"), *_refs("^GSPC", index=True)]

    with pytest.raises(RuntimeError):
        await svc.get_quotes(refs)
    assert svc.quote_key(refs[0]) not in cache.store
    assert cache.store[svc.quote_key(refs[1])] == qcs._NO_DATA
    assert svc._inflight == {}


@pytest.mark.asyncio
async def test_a_failed_fill_keeps_cache_hits_and_the_daily_fallback(service, monkeypatch):
    """A watchlist whose only uncached symbol was a listing no provider could
    quote answered 500: the fill's error dropped the cached rows with it, and
    the daily fallback never ran. It errors only when nothing answers."""
    svc, cache, install = service
    aapl, bj = to_canonical("AAPL"), to_canonical("920395.BJ")
    cache.store[svc.quote_key(aapl)] = _row("AAPL")

    class _Down:
        async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
            raise RuntimeError("503 Service Unavailable")

    async def _fallback(quotes, refs, user_id):
        return [*quotes, (bj, {"symbol": "920395.BJ", "price": 6.68, "source": "daily"})]

    async def _names(symbols):
        return dict.fromkeys(symbols, (None, None))

    install(_Down())
    monkeypatch.setattr("src.data_client.ginlix_data.directory.display_names_many", _names)
    assert _symbols(await svc.get_quotes([aapl, bj])) == ["AAPL"]

    monkeypatch.setattr(qcs, "fill_from_daily", _fallback)
    assert _symbols(await svc.get_quotes([bj], daily_fallback=True)) == ["920395.BJ"]

    with pytest.raises(RuntimeError):
        await svc.get_quotes([to_canonical("MSFT")])


@pytest.mark.asyncio
async def test_follower_own_cancellation_still_propagates(service):
    """Shield semantics: cancelling the FOLLOWER request must cancel it (the
    leader keeps fetching) — the miss fallback is only for a dead leader."""
    svc, cache, install = service
    gate = asyncio.Event()
    provider = _StubProvider({"AAPL": _row("AAPL")}, gate=gate)
    install(provider)

    leader = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await provider.started.wait()
    follower = asyncio.create_task(svc.get_quotes(_refs("AAPL")))
    await asyncio.sleep(0.01)

    follower.cancel()
    with pytest.raises(asyncio.CancelledError):
        await follower

    gate.set()
    out = await leader
    assert _symbols(out) == ["AAPL"]


@pytest.mark.asyncio
async def test_caret_index_via_stocks_fetches_index_endpoint(service):
    """A caret-spelled index requested as stocks fetches via the index endpoint
    (resolved asset class), so a miss there can't negative-cache its own key."""
    svc, cache, install = service
    row = _row("GSPC")
    provider = _AssetTypeRecordingProvider("GSPC", row)
    install(provider)

    out = await svc.get_quotes(_refs("^GSPC"))

    assert _symbols(out) == ["GSPC"]
    # Partitioned to the indices endpoint despite the stocks-spelled request.
    assert provider.calls == [(["GSPC"], "indices")]
    # Canonical index key holds the row, never the negative sentinel.
    idx_key = svc.quote_key(to_canonical("GSPC", asset_class=qcs.AssetClass.INDEX))
    assert cache.store[idx_key] == row
    assert cache.store[idx_key] != qcs._NO_DATA


@pytest.mark.asyncio
async def test_an_equity_and_an_index_echoing_one_string_keep_their_own_rows(service):
    """GSPC the equity and ^GSPC the index both come back spelled ``GSPC``; each
    takes its row from its own asset-class call, never the other's."""
    svc, cache, install = service

    class _EchoesBare:
        async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
            price = 5000.0 if asset_type == "indices" else 12.0
            return [_row(s, price) for s in symbols]

    install(_EchoesBare())
    out = await svc.get_quotes(_refs("GSPC", "^GSPC"))
    assert [(ref.asset_class.value, row["price"]) for ref, row in out] == [
        ("equity", 12.0), ("index", 5000.0),
    ]


@pytest.mark.asyncio
async def test_an_index_hinted_miss_does_not_blank_the_equity_read(service):
    """0700.HK under an index hint shares the equity's instrument_key; its
    negative-cache write must not land on the key the stocks read uses."""
    svc, cache, install = service

    class _StocksOnly:
        async def get_snapshots(self, symbols, asset_type="stocks", user_id=None):
            return [_row(s) for s in symbols] if asset_type == "stocks" else []

    install(_StocksOnly())

    assert await svc.get_quotes(_refs("0700.HK", index=True)) == []
    out = await svc.get_quotes(_refs("0700.HK"))

    assert _symbols(out) == ["0700.HK"]


class TestQuoteTTL:
    def test_us_regular_session_short_ttl(self):
        ref = to_canonical("AAPL")
        at = datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc)  # 11:00 ET Wed — open
        assert _quote_ttl(ref, at) == qcs._TTL_REGULAR

    def test_us_premarket_extended_ttl(self):
        ref = to_canonical("AAPL")
        at = datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc)  # 05:00 ET Wed — pre
        assert _quote_ttl(ref, at) == qcs._TTL_EXTENDED

    def test_closed_holds_until_next_open(self):
        ref = to_canonical("AAPL")
        at = datetime(2026, 7, 4, 16, 0, tzinfo=timezone.utc)  # Saturday
        ttl = _quote_ttl(ref, at)
        assert qcs._TTL_CLOSED_MIN <= ttl <= qcs._TTL_CLOSED_MAX
        assert ttl > qcs._TTL_EXTENDED

    def test_crypto_always_regular(self):
        ref = to_canonical("BTC-USD.CRYPTO")
        at = datetime(2026, 7, 5, 3, 0, tzinfo=timezone.utc)  # Sunday
        assert _quote_ttl(ref, at) == qcs._TTL_REGULAR

    # Fri 2026-09-18 is a session on XHKG, XSHG and XNYS.
    @pytest.mark.parametrize("symbol, tz, closed_at", [
        ("0700.HK", "Asia/Hong_Kong", (16, 0)),   # before the closing auction prints
        ("600519.SH", "Asia/Shanghai", (15, 0)),
        ("AAPL", "America/New_York", (20, 0)),     # after-hours ends the session
    ])
    def test_a_row_read_just_after_the_close_is_not_held_to_the_open(self, symbol, tz, closed_at):
        """A row fetched minutes after a Friday close can predate the session's
        last prints; holding it to Monday's open served it for ~65h."""
        from zoneinfo import ZoneInfo

        ref = to_canonical(symbol)

        def ttl(minutes_after: int, tier):
            at = datetime(2026, 9, 18, *closed_at, tzinfo=ZoneInfo(tz))
            return _quote_ttl(ref, at.replace(minute=minutes_after), tier=tier)

        assert ttl(6, "realtime") == qcs._TTL_CLOSED_MIN
        assert ttl(20, "realtime") > 24 * 3600  # settled: held to the next open
        # A delayed (or undeclared) feed still trails by its delay.
        assert ttl(20, "delayed_15m") == qcs._TTL_CLOSED_MIN
        assert ttl(20, None) == qcs._TTL_CLOSED_MIN
        assert ttl(31, "delayed_15m") > 24 * 3600


@pytest.mark.asyncio
async def test_a_row_from_an_earlier_session_is_not_held_to_the_open(service, monkeypatch):
    """Friday evening, a vendor still quoting Thursday's close: the calendar
    alone would hold that row all weekend, so it is refetched instead. A row
    that printed in Friday's session, however early, is still held."""
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    at = datetime(2026, 9, 18, 21, 0, tzinfo=et)  # Fri, after-hours over and settled

    class _PinnedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz)

    def _ms(d: datetime) -> int:
        return int(d.timestamp() * 1000)

    monkeypatch.setattr(qcs, "datetime", _PinnedClock)
    svc, cache, install = service
    install(_StubProvider({
        "AAPL": {**_row("AAPL"), "tier": "realtime", "as_of": _ms(datetime(2026, 9, 17, 16, 0, tzinfo=et))},
        "MSFT": {**_row("MSFT"), "tier": "realtime", "as_of": _ms(datetime(2026, 9, 18, 11, 0, tzinfo=et))},
        "NVDA": {**_row("NVDA"), "tier": "realtime"},
    }))
    aapl, msft, nvda = _refs("AAPL", "MSFT", "NVDA")

    await svc.get_quotes([aapl, msft, nvda])

    assert cache.set_ttls[svc.quote_key(aapl)] == qcs._TTL_CLOSED_MIN
    assert cache.set_ttls[svc.quote_key(msft)] > 24 * 3600
    # No print time is no evidence of lag: the calendar decides.
    assert cache.set_ttls[svc.quote_key(nvda)] > 24 * 3600


@pytest.mark.asyncio
async def test_daily_fallback_rows_get_display_names(service, monkeypatch):
    """A closing quote built from daily bars is named like any served row."""
    svc, _, install = service
    install(_StubProvider({}))
    bj = to_canonical("920300.BJ")

    async def _fallback(quotes, refs, user_id):
        return [*quotes, (bj, {"symbol": "920300.BJ", "price": 9.93, "source": "daily"})]

    async def _names(symbols):
        return {s: ("甲公司", "Alpha Co.") for s in symbols}

    monkeypatch.setattr(qcs, "fill_from_daily", _fallback)
    monkeypatch.setattr("src.data_client.ginlix_data.directory.display_names_many", _names)

    [(_, row)] = await svc.get_quotes([bj], daily_fallback=True)
    assert (row["name"], row["name_local"], row["name_en"]) == ("Alpha Co.", "甲公司", "Alpha Co.")
