"""QuoteCacheService — per-symbol keys, batched fill, dedup, negative cache."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from src.market_protocol import to_canonical
from src.server.services.cache import quote_cache_service as qcs
from src.server.services.cache.quote_cache_service import QuoteCacheService, _quote_ttl


def _row(symbol: str, price: float = 100.0) -> dict:
    return {"symbol": symbol, "name": f"{symbol} Inc", "price": price, "change": 1.0}


class _StubCache:
    def __init__(self) -> None:
        self.store: dict[str, dict] = {}
        self.set_ttls: dict[str, int] = {}

    async def get(self, key):
        return self.store.get(key)

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]

    async def set(self, key, value, ttl=None):
        self.store[key] = value
        self.set_ttls[key] = ttl


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

    out = await svc.get_quotes(["AAPL", "MSFT"])
    assert [r["symbol"] for r in out] == ["AAPL", "MSFT"]
    assert provider.calls == [["AAPL", "MSFT"]]

    # Second request: pure cache hits, no upstream call.
    out = await svc.get_quotes(["AAPL", "MSFT"])
    assert len(out) == 2 and len(provider.calls) == 1


@pytest.mark.asyncio
async def test_partial_hit_fetches_only_misses(service):
    svc, cache, install = service
    provider = _StubProvider({"AAPL": _row("AAPL"), "MSFT": _row("MSFT")})
    install(provider)
    await svc.get_quotes(["AAPL"])

    out = await svc.get_quotes(["AAPL", "MSFT"])
    assert [r["symbol"] for r in out] == ["AAPL", "MSFT"]
    assert provider.calls == [["AAPL"], ["MSFT"]]


@pytest.mark.asyncio
async def test_concurrent_requests_share_one_fetch(service):
    svc, cache, install = service
    gate = asyncio.Event()
    provider = _StubProvider({"AAPL": _row("AAPL")}, gate=gate)
    install(provider)

    t1 = asyncio.create_task(svc.get_quotes(["AAPL"]))
    t2 = asyncio.create_task(svc.get_quotes(["AAPL"]))
    await asyncio.sleep(0.01)  # both admitted; one leader, one follower
    gate.set()
    r1, r2 = await asyncio.gather(t1, t2)
    assert r1 == r2 and r1[0]["symbol"] == "AAPL"
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_unknown_symbol_dropped_and_negative_cached(service):
    svc, cache, install = service
    provider = _StubProvider({"AAPL": _row("AAPL")})
    install(provider)

    out = await svc.get_quotes(["AAPL", "ZZZFAKE"])
    assert [r["symbol"] for r in out] == ["AAPL"]
    # Negative sentinel written with short TTL...
    neg_key = svc.quote_key(to_canonical("ZZZFAKE"))
    assert cache.store[neg_key] == {"__no_data__": True}
    assert cache.set_ttls[neg_key] == qcs._TTL_NEGATIVE
    # ...so the repeat does not re-fan out upstream.
    out = await svc.get_quotes(["AAPL", "ZZZFAKE"])
    assert [r["symbol"] for r in out] == ["AAPL"]
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_spellings_collapse_to_one_key(service):
    svc, cache, install = service
    provider = _StubProvider({"GSPC": _row("GSPC")})
    install(provider)

    out = await svc.get_quotes(["^GSPC", "GSPC", "I:SPX"], asset_type="indices")
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
        await svc.get_quotes(["AAPL"])
    assert svc._inflight == {}

    # Recovers on the next call once the provider is healthy again.
    provider = _StubProvider({"AAPL": _row("AAPL")})
    install(provider)
    out = await svc.get_quotes(["AAPL"])
    assert out[0]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_leader_cancellation_clears_inflight_and_recovers(service):
    """Cancelling the batch leader must clear _inflight — a stranded future
    would make every later request a follower awaiting a dead fetch forever."""
    svc, cache, install = service
    gate = asyncio.Event()
    provider = _StubProvider({"AAPL": _row("AAPL")}, gate=gate)
    install(provider)

    leader = asyncio.create_task(svc.get_quotes(["AAPL"]))
    await provider.started.wait()  # leader is blocked inside the provider call

    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader
    assert svc._inflight == {}  # cancelled leader stranded no per-symbol future

    # A fresh request with a healthy provider completes (no hang on a dead future).
    provider2 = _StubProvider({"AAPL": _row("AAPL")})
    install(provider2)
    out = await svc.get_quotes(["AAPL"])
    assert out[0]["symbol"] == "AAPL"
    assert provider2.calls == [["AAPL"]]


@pytest.mark.asyncio
async def test_caret_index_via_stocks_fetches_index_endpoint(service):
    """A caret-spelled index requested as stocks fetches via the index endpoint
    (resolved asset class), so a miss there can't negative-cache its own key."""
    svc, cache, install = service
    row = _row("GSPC")
    provider = _AssetTypeRecordingProvider("GSPC", row)
    install(provider)

    out = await svc.get_quotes(["^GSPC"], asset_type="stocks")

    assert [r["symbol"] for r in out] == ["GSPC"]
    # Partitioned to the indices endpoint despite the stocks-spelled request.
    assert provider.calls == [(["GSPC"], "indices")]
    # Canonical index key holds the row, never the negative sentinel.
    idx_key = svc.quote_key(to_canonical("GSPC", asset_class=qcs.AssetClass.INDEX))
    assert cache.store[idx_key] == row
    assert cache.store[idx_key] != qcs._NO_DATA


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
