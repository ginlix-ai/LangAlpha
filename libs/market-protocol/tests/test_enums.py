"""These are closed wire vocabularies, so renaming or adding a value is a breaking change every reader's pin must move for."""

import pytest

from market_protocol import AssetClass, FeedScope, MarketPhase, PriceTreatment, Tier
from market_protocol.routing import PROBED_SURFACES, Surface

VOCABULARIES = {
    AssetClass: ["equity", "index", "fund", "crypto", "fx"],
    MarketPhase: ["pre", "regular", "lunch", "post", "closed", "halted"],
    PriceTreatment: ["raw", "split_adjusted", "dividend_adjusted"],
    Tier: ["realtime", "delayed_15m", "eod"],
    FeedScope: ["composite", "venue"],
    Surface: ["intraday", "daily", "snapshot", "fundamentals", "directory", "status", "news"],
}


@pytest.mark.parametrize(
    ("enum", "values"), VOCABULARIES.items(), ids=[e.__name__ for e in VOCABULARIES]
)
def test_values(enum, values):
    assert [m.value for m in enum] == values


def test_probed_surfaces():
    assert [s.value for s in PROBED_SURFACES] == ["intraday", "daily", "snapshot"]
