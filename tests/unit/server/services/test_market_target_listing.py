"""A target is read by a price automation's rules, stored under the price
monitor's key, and watched under a spelling the live feed reads as the same
listing."""

import pytest

from src.server.models.automation import MarketType
from src.server.services.market_targets.listing import (
    TargetListing,
    target_listing,
    watch_spelling,
)


@pytest.mark.parametrize(
    "symbol, expected",
    [
        ("NVDA", TargetListing("NVDA", MarketType.STOCK, "NVDA")),
        ("600519.SH", TargetListing("600519.SH", MarketType.STOCK, "600519.SH")),
        ("BRK.B", TargetListing("BRK.B", MarketType.STOCK, "BRK.B")),
        # A caret spelling is the index, keyed by the monitor's family name.
        ("^GSPC", TargetListing("SPX", MarketType.INDEX, "^GSPC")),
        ("^SPX", TargetListing("SPX", MarketType.INDEX, "^GSPC")),
        ("^HSI", TargetListing("HSI", MarketType.INDEX, "^HSI")),
        # A bare index family reads as the index, as on a price automation,
        # and is watched by the caret spelling the feed reads as that index.
        ("SPX", TargetListing("SPX", MarketType.INDEX, "^GSPC")),
        ("GSPC", TargetListing("SPX", MarketType.INDEX, "^GSPC")),
        ("COMP", TargetListing("COMP", MarketType.INDEX, "^IXIC")),
    ],
)
def test_listing(symbol, expected):
    listing, refusal = target_listing(symbol)
    assert refusal is None
    assert listing == expected


@pytest.mark.parametrize("symbol", ["BTC-USD", "EURUSD=X", "ES=F", "ETHUSDT"])
def test_pairs_are_refused(symbol):
    listing, refusal = target_listing(symbol)
    assert listing is None
    assert "crypto, currency or futures" in refusal


def test_unknown_suffix_is_refused():
    listing, refusal = target_listing("ZZZZ.ZZ")
    assert listing is None
    assert "never be checked" in refusal


@pytest.mark.parametrize("symbol", ["123456", "0700", "7203"])
def test_a_bare_number_is_refused(symbol):
    listing, refusal = target_listing(symbol)
    assert listing is None
    assert "add its exchange suffix" in refusal


def test_watch_spelling_of_stored_rows():
    assert watch_spelling("SPX", MarketType.INDEX) == "^GSPC"
    assert watch_spelling("HSI", MarketType.INDEX) == "^HSI"
    assert watch_spelling("NVDA", MarketType.STOCK) == "NVDA"
