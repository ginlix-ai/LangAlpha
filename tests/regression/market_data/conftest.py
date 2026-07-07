"""Live-API regression baseline for the CMDP market-data refactor.

Locks today's wire behavior — response shape, semantic invariants, and pinned
known-bug behaviors — against a running server + live upstream providers.
Every refactor phase must keep this suite green; tests that pin a known bug
flip their expectation in the phase that fixes it (see test_pinned_behaviors.py).

Run:
    uv run pytest tests/regression/ -v -m regression

Env:
    REGRESSION_BASE_URL     target server (default http://localhost:8000)
    INTERNAL_SERVICE_TOKEN  REST auth (falls back to repo-root .env)
    REGRESSION_USER_ID      X-User-Id header (default local-dev-user)
    REGRESSION_WS_TOKEN     Supabase JWT for live WS frames (optional; WS auth
                            has no service-token path — without it only the
                            unauthenticated WS contracts run)
"""

import os
import time
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).parents[3]

BASE_URL = os.getenv("REGRESSION_BASE_URL", "http://localhost:8000")
API = f"{BASE_URL}/api/v1/market-data"
USER_ID = os.getenv("REGRESSION_USER_ID", "local-dev-user")

# Symbol matrix — one representative per regime the refactor touches
US_STOCK = "AAPL"
US_STOCK_2 = "MSFT"
HK_STOCK = "0700.HK"   # non-US, FMP-served, ET-timestamp bug regime
LSE_STOCK = "VOD.L"    # non-US, GBp (pence) regime
US_INDEX = "GSPC"
US_INDEX_2 = "IXIC"
UNKNOWN_SYMBOL = "ZZZZFAKE1"

STOCK_SNAPSHOT_FIELDS = {
    "symbol", "name", "price", "change", "change_percent", "previous_close",
    "open", "high", "low", "volume", "market_status", "regular_trading_change",
    "early_trading_change_percent", "late_trading_change_percent", "source",
}
BAR_FIELDS = {"time", "open", "high", "low", "close", "volume"}
CACHE_META_FIELDS = {
    "cached", "cache_key", "ttl_remaining", "refreshed_in_background",
    "watermark", "complete", "market_phase", "truncated",
}

_EPOCH_2000_MS = 946_684_800_000


def _env_file_value(key: str) -> str:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text().splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def service_token() -> str:
    return os.getenv("INTERNAL_SERVICE_TOKEN") or _env_file_value("INTERNAL_SERVICE_TOKEN")


@pytest.fixture(scope="session")
def auth_headers() -> dict:
    token = service_token()
    if not token:
        pytest.skip("INTERNAL_SERVICE_TOKEN not set (env or repo .env)")
    return {"X-Service-Token": token, "X-User-Id": USER_ID}


@pytest.fixture(scope="session")
def http(auth_headers) -> httpx.Client:
    try:
        probe = httpx.get(f"{BASE_URL}/ws/v1/market-data/status", timeout=3)
        probe.raise_for_status()
    except Exception as e:
        pytest.skip(f"regression target {BASE_URL} not reachable: {e}")
    with httpx.Client(base_url=API, headers=auth_headers, timeout=60) as client:
        yield client


# ---------------------------------------------------------------------------
# Shared invariant helpers
# ---------------------------------------------------------------------------

def assert_bar_shape(bar: dict, *, context: str = "") -> None:
    assert set(bar.keys()) == BAR_FIELDS, f"{context}: bar fields drifted: {sorted(bar.keys())}"
    assert isinstance(bar["time"], int), f"{context}: time must be int ms"
    now_ms = int(time.time() * 1000)
    # +2h headroom: forming-bar anchors plus clock skew (the FMP ET-stamp bug
    # that pushed non-US bars ~13h forward was fixed in Phase 1)
    assert _EPOCH_2000_MS < bar["time"] < now_ms + 2 * 3_600_000, f"{context}: time {bar['time']} out of range"
    for f in ("open", "high", "low", "close"):
        assert isinstance(bar[f], (int, float)), f"{context}: {f} must be numeric"
        assert bar[f] > 0, f"{context}: {f} must be positive"
    assert isinstance(bar["volume"], int), f"{context}: volume must be int (server coerces floats)"
    assert bar["volume"] >= 0, f"{context}: volume must be >= 0"
    assert bar["low"] <= bar["high"], f"{context}: low > high"
    eps = max(0.05, bar["high"] * 0.001)  # tolerate provider rounding, catch unit-mixing
    for f in ("open", "close"):
        assert bar["low"] - eps <= bar[f] <= bar["high"] + eps, f"{context}: {f} outside [low, high]"


def assert_bars_monotonic(bars: list[dict], *, context: str = "") -> str:
    """Returns 'asc' or 'desc'. Direction is provider-dependent today (pinned separately).

    Non-strict: long-lived live cache envelopes currently accumulate exact
    duplicate bars (observed 10× on AAPL 5min/1hour — merge bug, predates this
    suite). Fresh fetches are duplicate-free; see test_pinned_behaviors.
    """
    times = [b["time"] for b in bars]
    first_diff = next((i for i in range(1, len(times)) if times[i] != times[i - 1]), None)
    if first_diff is None:
        return "asc"
    direction = "asc" if times[first_diff] > times[first_diff - 1] else "desc"
    ordered = sorted(times, reverse=(direction == "desc"))
    assert times == ordered, f"{context}: timestamps not monotonic {direction}"
    return direction


def assert_cache_meta(meta: dict, *, context: str = "") -> None:
    assert set(meta.keys()) == CACHE_META_FIELDS, f"{context}: cache meta fields drifted: {sorted(meta.keys())}"
    assert isinstance(meta["cached"], bool), context
    assert meta["cache_key"] is None or isinstance(meta["cache_key"], str), context
    if meta["market_phase"] is not None:
        assert meta["market_phase"] in ("pre", "open", "post", "closed"), f"{context}: phase {meta['market_phase']}"


def assert_ohlcv_response(payload: dict, *, symbol: str, expect_interval: str | None = None) -> None:
    # Intraday responses carry "interval"; daily responses do not — both pinned.
    expected_keys = {"symbol", "interval", "data", "count", "cache"} if expect_interval else {"symbol", "data", "count", "cache"}
    assert set(payload.keys()) == expected_keys, f"{symbol}: response keys drifted: {sorted(payload.keys())}"
    assert payload["symbol"] == symbol
    if expect_interval is not None:
        assert payload["interval"] == expect_interval
    assert payload["count"] == len(payload["data"]), f"{symbol}: count != len(data)"
    assert payload["count"] > 0, f"{symbol}: no bars returned"
    for bar in payload["data"]:
        assert_bar_shape(bar, context=symbol)
    assert_bars_monotonic(payload["data"], context=symbol)
    assert_cache_meta(payload["cache"], context=symbol)


def assert_snapshot_shape(snap: dict, *, context: str = "") -> None:
    assert set(snap.keys()) == STOCK_SNAPSHOT_FIELDS, f"{context}: snapshot fields drifted: {sorted(snap.keys())}"
    assert isinstance(snap["symbol"], str) and snap["symbol"], context
    for f in ("price", "change", "change_percent", "previous_close", "open", "high", "low"):
        assert snap[f] is None or isinstance(snap[f], (int, float)), f"{context}: {f} wrong type"
    assert snap["volume"] is None or isinstance(snap["volume"], int), f"{context}: volume wrong type"
