"""Pinned known-bug behaviors — each test asserts TODAY'S behavior.

When a refactor phase fixes the underlying bug, the paired test here MUST be
flipped to the corrected expectation in the same PR. A phase diff that changes
one of these without flipping its test is regressing something silently.

Status (see plan: mighty-gathering-clover):
  FLIPPED in Phase 1: FMP ET-stamp on non-US bars (now exchange-local),
                      provider-dependent bar ordering (now ascending)
  Phase 1 (reorder commit) flips: non-US routing through FMP first
  Phase 2 flips: unknown-symbol null rows in batch snapshots
  FLIPPED in Phase 3: HK cache-hit misfire (calendar-correct staleness)
  FLIPPED in Phase 3 (key cutover): canonical cache-key format
  FLIPPED in Phase 4: VOD.L GBX→major-unit conversion on the fmp/yfinance
                      legacy bar + snapshot paths
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from .conftest import (
    HK_STOCK,
    LSE_STOCK,
    UNKNOWN_SYMBOL,
    US_INDEX,
    US_STOCK,
    assert_snapshot_shape,
)

pytestmark = pytest.mark.regression

_HKT = ZoneInfo("Asia/Hong_Kong")
# XHKG session (incl. lunch, generous edges): 09:00–16:30 HKT
_HK_SESSION_HOURS = range(9, 17)


def test_hk_intraday_timestamps_land_in_hkt_session(http):
    """FLIPPED (Phase 1): FMP timestamps localize in the exchange tz now —
    every recent 0700.HK bar anchor falls inside the XHKG session grid."""
    r = http.get(f"/intraday/stocks/{HK_STOCK}", params={"interval": "1hour"})
    assert r.status_code == 200
    bars = r.json()["data"][-50:]
    in_session = sum(
        1 for b in bars
        if datetime.fromtimestamp(b["time"] / 1000, tz=timezone.utc).astimezone(_HKT).hour in _HK_SESSION_HOURS
    )
    assert in_session == len(bars), (
        f"{len(bars) - in_session}/{len(bars)} HK bars fall outside the HKT session — "
        "the FMP exchange-tz localization regressed"
    )


def test_lse_prices_are_major_units(http):
    """FLIPPED (Phase 4): LSE quotes arrive in GBX (pence) upstream; the
    fmp/yfinance legacy bar + snapshot paths now convert ×0.01 to major units
    (pounds). VOD ≈ £0.6–1.2, so a converted price reads well under 20 —
    pence-scale would be ~60–120."""
    snap = http.get("/snapshots/stocks", params={"symbols": LSE_STOCK}).json()["snapshots"][0]
    assert snap["price"] is not None and 0 < snap["price"] < 20, (
        f"VOD.L price {snap['price']} looks pence-scale — GBX→major conversion regressed"
    )
    daily = http.get(f"/daily/stocks/{LSE_STOCK}").json()["data"]
    assert 0 < daily[0]["close"] < 20


def test_bar_ordering_is_ascending_for_all_providers(http):
    """FLIPPED (Phase 1): every provider returns ascending bars now — order is
    part of the contract, not a provider accident."""
    us = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": "1hour"}).json()["data"]
    hk = http.get(f"/intraday/stocks/{HK_STOCK}", params={"interval": "1hour"}).json()["data"]
    assert us[0]["time"] < us[-1]["time"], "US bars no longer ascending"
    assert hk[0]["time"] < hk[-1]["time"], "HK bars no longer ascending"


def test_cache_key_format_is_canonical(http):
    """FLIPPED (Phase 3 key cutover): cache keys are
    `ohlcv:{instrument_key}:{schema}` — every spelling of one instrument
    collapses to one key, and the publisher moved off the key into the
    envelope header (single live data key per instrument; the pin decides
    who fills it). Routing is no longer visible in the key — the provider
    chain is pinned by unit tests on the config instead."""
    us = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": "1hour"}).json()["cache"]["cache_key"]
    hk = http.get(f"/intraday/stocks/{HK_STOCK}", params={"interval": "1hour"}).json()["cache"]["cache_key"]
    idx = http.get(f"/intraday/indexes/{US_INDEX}", params={"interval": "1hour"}).json()["cache"]["cache_key"]
    assert us == "ohlcv:AAPL.XNAS:ohlcv-1h"
    assert hk == "ohlcv:0700.XHKG:ohlcv-1h"
    assert idx == "ohlcv:SPX.INDEX:ohlcv-1h"


def test_hk_intraday_repeat_calls_cache_hit(http):
    """FLIPPED (Phase 3): staleness is calendar-correct per instrument — a
    0700.HK envelope is judged against the XHKG session (incl. lunch), so an
    immediate repeat call within TTL always cache-hits. Under the old US grid
    this symbol NEVER cache-hit outside the HK session (Context bug #6)."""
    first = http.get(f"/intraday/stocks/{HK_STOCK}", params={"interval": "1hour"}).json()
    second = http.get(f"/intraday/stocks/{HK_STOCK}", params={"interval": "1hour"}).json()
    assert first["data"], "no HK bars returned"
    assert second["cache"]["cached"] is True, (
        "0700.HK repeat call missed cache — calendar-correct staleness regressed"
    )


def test_fresh_historical_window_is_duplicate_free(http):
    """Long-lived live envelopes accumulate duplicate bars (observed 10× on
    AAPL 5min/1hour — open merge bug on main). This pins the healthy half:
    fresh historical-window fetches must stay duplicate-free."""
    r = http.get(f"/intraday/stocks/{US_STOCK}", params={"interval": "1hour", "from": "2026-06-22", "to": "2026-06-26"})
    assert r.status_code == 200
    times = [b["time"] for b in r.json()["data"]]
    assert len(times) == len(set(times)), "historical window now returns duplicate bars"


def test_unknown_symbol_dropped_from_batch(http):
    """FLIPPED (Phase 2): null-field rows no longer count as resolved — the
    provider chain retries the symbol downstream, and after exhaustion the
    quote service drops it (and negative-caches it) instead of returning a
    null-field row. `count` reflects only resolved symbols."""
    r = http.get("/snapshots/stocks", params={"symbols": f"{US_STOCK},{UNKNOWN_SYMBOL}"})
    assert r.status_code == 200
    payload = r.json()
    assert payload["count"] == 1, "unknown symbol re-appeared in batch snapshots"
    returned = [s["symbol"] for s in payload["snapshots"]]
    assert returned == [US_STOCK]
    assert_snapshot_shape(payload["snapshots"][0], context=US_STOCK)
