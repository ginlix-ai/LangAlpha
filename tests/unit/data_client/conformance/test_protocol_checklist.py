"""Protocol-level conformance checklist (plan Part I) — green from Phase 0.

Provider-normalizer items live in test_provider_conformance.py (xfail until
Phase 1); Redis spelling-collapse is asserted at the symbology layer here and
at the cache-key layer from Phase 3.
"""

import pytest
from pydantic import ValidationError

from src.market_protocol import (
    OhlcvBar,
    PriceTreatment,
    Series,
    SeriesHeader,
    Tier,
    to_canonical,
)


class TestSpellingCollapse:
    """Every spelling of five representative instruments ⇒ one canonical key."""

    MATRIX = {
        "SPX.INDEX": ["GSPC", "^GSPC", "I:SPX", "SPX", "SPX.INDEX"],
        "COMP.INDEX": ["IXIC", "^IXIC", "I:COMP", "COMP"],
        "AAPL.XNAS": ["AAPL", "aapl", "AAPL.US", "AAPL.XNAS"],
        "0700.XHKG": ["0700.HK", "0700.hk", "0700.XHKG"],
        "VOD.XLON": ["VOD.L", "vod.l", "VOD.XLON"],
    }

    @pytest.mark.parametrize("expected_key", MATRIX)
    def test_one_key_per_instrument(self, expected_key):
        keys = {to_canonical(s).instrument_key for s in self.MATRIX[expected_key]}
        assert keys == {expected_key}


class TestChecklistModelItems:
    def test_time_alias_present_on_every_record(self):
        bar = OhlcvBar(ts_event=1_750_000_000_000, open=1, high=1, low=1, close=1, volume=1)
        assert bar.model_dump()["time"] == 1_750_000_000_000

    def test_index_bars_null_volume_round_trip(self):
        bar = OhlcvBar(ts_event=1, open=1, high=1, low=1, close=1, volume=None)
        again = OhlcvBar.model_validate(bar.model_dump(mode="json"))
        assert again.volume is None

    def test_price_treatment_never_null(self):
        with pytest.raises(ValidationError):
            SeriesHeader.model_validate({
                "instrument_key": "AAPL.XNAS", "schema": "ohlcv-1h",
                "price_treatment": None, "publisher": "fmp", "tier": Tier.REALTIME,
                "price_currency": "USD", "display_decimals": 2,
                "asof": 1, "fetched_at": 1,
            })

    def test_series_wire_shape_is_stable(self):
        """The v4 envelope contract: header + records, schema by wire name."""
        header = SeriesHeader.model_validate({
            "instrument_key": "0700.XHKG", "schema": "ohlcv-1h",
            "price_treatment": PriceTreatment.SPLIT_ADJUSTED, "publisher": "fmp",
            "tier": Tier.REALTIME, "price_currency": "HKD", "display_decimals": 2,
            "asof": 1, "fetched_at": 1,
        })
        wire = Series(header=header, records=[]).to_wire()
        assert set(wire) == {"header", "records"}
        assert wire["header"]["schema"] == "ohlcv-1h"
        assert wire["header"]["ts_unit"] == "ms"
