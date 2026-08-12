"""Shape locks for snapshot endpoints — the Phase 2 quote-service wrapper contract."""

import pytest

from .conftest import (
    LSE_STOCK,
    US_INDEX,
    US_INDEX_2,
    US_STOCK,
    US_STOCK_2,
    assert_snapshot_shape,
)

pytestmark = pytest.mark.regression


def test_batch_stock_snapshots_shape(http):
    r = http.get("/snapshots/stocks", params={"symbols": f"{US_STOCK},{US_STOCK_2},{LSE_STOCK}"})
    assert r.status_code == 200
    payload = r.json()
    assert set(payload.keys()) == {"snapshots", "count"}
    assert payload["count"] == len(payload["snapshots"]) == 3
    returned = {s["symbol"] for s in payload["snapshots"]}
    assert returned == {US_STOCK, US_STOCK_2, LSE_STOCK}
    for snap in payload["snapshots"]:
        assert_snapshot_shape(snap, context=snap["symbol"])
        if snap["symbol"] in (US_STOCK, US_STOCK_2):
            assert snap["price"] and snap["price"] > 0
            assert snap["previous_close"] and snap["previous_close"] > 0


def test_batch_index_snapshots_shape(http):
    r = http.get("/snapshots/indexes", params={"symbols": f"{US_INDEX},{US_INDEX_2}"})
    assert r.status_code == 200
    payload = r.json()
    assert payload["count"] == len(payload["snapshots"]) == 2
    for snap in payload["snapshots"]:
        assert_snapshot_shape(snap, context=snap["symbol"])


def test_single_snapshot_same_shape_as_batch(http):
    single = http.get(f"/snapshots/stocks/{US_STOCK}")
    assert single.status_code == 200
    batch = http.get("/snapshots/stocks", params={"symbols": US_STOCK}).json()
    assert_snapshot_shape(single.json(), context="single")
    assert set(single.json().keys()) == set(batch["snapshots"][0].keys()), (
        "single and batch snapshot rows must stay shape-identical"
    )


def test_empty_symbols_rejected(http):
    r = http.get("/snapshots/stocks", params={"symbols": " , "})
    assert r.status_code == 422
