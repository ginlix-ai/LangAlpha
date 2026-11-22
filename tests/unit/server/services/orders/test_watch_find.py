"""When a watch's reads count as having looked for the order."""

from __future__ import annotations

import pytest

from src.server.services.orders import watch
from src.server.services.orders.status_read import Listing


class _Client:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _Reader:
    def __init__(self, *listings):
        self.listings = list(listings)

    def client(self):
        return _Client()

    async def read(self, _client, _query):
        item = self.listings.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _Query:
    tool = "get_equity_orders"


@pytest.mark.asyncio
async def test_a_complete_list_without_the_order_is_an_absence():
    reader = _Reader(Listing(orders=[], complete=True))
    assert await watch._find(reader, [_Query()], "ord_1") is None


@pytest.mark.parametrize(
    "listings",
    [
        [Listing(orders=[], complete=False)],
        [RuntimeError("relay down"), Listing(orders=[], complete=False)],
    ],
    ids=["cut short", "failed then cut short"],
)
@pytest.mark.asyncio
async def test_no_complete_list_is_a_failed_read_not_an_absence(listings):
    reader = _Reader(*listings)
    with pytest.raises(watch.WatchRefused) as e:
        await watch._find(reader, [_Query()] * len(listings), "ord_1")
    assert e.value.reason == "failed"
