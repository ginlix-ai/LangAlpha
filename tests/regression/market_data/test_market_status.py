"""Shape locks for market status + auth boundary contracts."""

import httpx
import pytest

from .conftest import API, US_STOCK

pytestmark = pytest.mark.regression

STATUS_KEYS = {"market", "afterHours", "earlyHours", "serverTime", "exchanges", "providers"}


def test_market_status_shape(http):
    r = http.get("/market-status")
    assert r.status_code == 200
    payload = r.json()
    assert set(payload.keys()) == STATUS_KEYS, f"status keys drifted: {sorted(payload.keys())}"
    assert payload["market"] in ("open", "closed", "extended-hours")
    assert isinstance(payload["afterHours"], bool)
    assert isinstance(payload["earlyHours"], bool)
    assert isinstance(payload["exchanges"], dict)
    assert isinstance(payload["providers"], list) and payload["providers"]


def test_status_alias_matches_market_status(http):
    alias = http.get("/status")
    canonical = http.get("/market-status")
    assert alias.status_code == canonical.status_code == 200
    assert set(alias.json().keys()) == set(canonical.json().keys())
    assert alias.json()["market"] == canonical.json()["market"]


def test_provider_chain_order(http):
    # FLIPPED (Phase 1): per-capability routing slots yfinance ahead of FMP
    # for non-US intraday; the deduped chain order is now
    # ginlix-data → yfinance → fmp. Daily/snapshot routing unchanged
    # (capability overrides — see config.yaml market_data.providers).
    payload = http.get("/market-status").json()
    assert payload["providers"] == ["ginlix-data", "yfinance", "fmp"]


def test_rest_requires_auth():
    r = httpx.get(f"{API}/market-status", timeout=10)
    assert r.status_code == 401


def test_rest_rejects_bad_service_token():
    r = httpx.get(
        f"{API}/snapshots/stocks",
        params={"symbols": US_STOCK},
        headers={"X-Service-Token": "wrong-token", "X-User-Id": "local-dev-user"},
        timeout=10,
    )
    assert r.status_code == 401
