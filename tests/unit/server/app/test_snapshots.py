"""Batch snapshot endpoint guards on the market-data router.

The per-request symbol cap is enforced before any upstream fetch; the quote
cache service is stubbed so no Redis/provider is touched.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_MAX = 250  # documented max symbols per batch snapshot request


@pytest_asyncio.fixture
async def client():
    from src.server.app.market_data import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def _stub_quotes(rows=None):
    """Patch QuoteCacheService so the endpoint's fetch layer returns ``rows``."""
    service = MagicMock()
    service.get_quotes = AsyncMock(return_value=rows or [])
    return patch(
        "src.server.app.market_data.QuoteCacheService.get_instance",
        return_value=service,
    )


async def test_too_many_symbols_is_422(client):
    symbols = ",".join(f"A{i}" for i in range(_MAX + 1))  # 251 distinct tickers
    with _stub_quotes():
        resp = await client.get(f"/api/v1/market-data/snapshots/stocks?symbols={symbols}")
    assert resp.status_code == 422
    assert "Too many symbols" in resp.json()["detail"]


async def test_symbol_cap_boundary_passes_validation(client):
    # Exactly at the cap: not rejected — the request reaches the cache service.
    symbols = ",".join(f"A{i}" for i in range(_MAX))  # 250 distinct tickers
    with _stub_quotes() as get_instance:
        resp = await client.get(f"/api/v1/market-data/snapshots/stocks?symbols={symbols}")
    assert resp.status_code == 200
    get_instance.return_value.get_quotes.assert_awaited_once()


async def test_snapshot_row_source_passes_through(client):
    # The provider chain stamps each resolved row with the filling provider;
    # the endpoint model must expose it, absent-source rows serialize null.
    rows = [
        {"symbol": "AAPL", "price": 190.0, "source": "ginlix-data"},
        {"symbol": "MSFT", "price": 420.0},
    ]
    with _stub_quotes(rows):
        resp = await client.get("/api/v1/market-data/snapshots/stocks?symbols=AAPL,MSFT")
    assert resp.status_code == 200
    snaps = {s["symbol"]: s for s in resp.json()["snapshots"]}
    assert snaps["AAPL"]["source"] == "ginlix-data"
    assert snaps["MSFT"]["source"] is None
