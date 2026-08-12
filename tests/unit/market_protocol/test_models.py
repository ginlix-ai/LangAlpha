"""Protocol model invariants: aliases, nullability, wire shape."""

import pytest
from pydantic import ValidationError

from src.market_protocol import (
    Coverage,
    FeedScope,
    Gap,
    OhlcvBar,
    PriceTreatment,
    Series,
    SeriesHeader,
    Tier,
)


def _bar(**overrides):
    base = {"ts_event": 1_750_000_000_000, "open": 1.0, "high": 2.0,
            "low": 0.5, "close": 1.5, "volume": 100.0}
    base.update(overrides)
    return OhlcvBar.model_validate(base)


def _header(**overrides):
    base = {
        "instrument_key": "AAPL.XNAS",
        "schema": "ohlcv-1h",
        "price_treatment": PriceTreatment.SPLIT_ADJUSTED,
        "publisher": "ginlix-data",
        "tier": Tier.REALTIME,
        "price_currency": "USD",
        "display_decimals": 2,
        "asof": 1_750_000_000_000,
        "fetched_at": 1_750_000_000_000,
    }
    base.update(overrides)
    return SeriesHeader.model_validate(base)


class TestOhlcvBar:
    def test_time_alias_on_input_and_output(self):
        from_legacy = OhlcvBar.model_validate(
            {"time": 123, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
        )
        assert from_legacy.ts_event == 123
        dumped = from_legacy.model_dump()
        assert dumped["ts_event"] == 123
        assert dumped["time"] == 123  # transitional alias always present

    def test_round_trip_with_both_fields(self):
        bar = _bar()
        again = OhlcvBar.model_validate(bar.model_dump())
        assert again.ts_event == bar.ts_event

    def test_volume_required_but_nullable(self):
        assert _bar(volume=None).volume is None  # null = not applicable (index)
        with pytest.raises(ValidationError):
            OhlcvBar.model_validate(
                {"ts_event": 1, "open": 1, "high": 1, "low": 1, "close": 1}
            )

    def test_head_bar_defaults_not_final(self):
        assert _bar().is_final is False

    def test_optional_enrichment_fields(self):
        bar = _bar(vwap=1.23, trades=42, is_final=True)
        assert (bar.vwap, bar.trades, bar.is_final) == (1.23, 42, True)


class TestSeriesHeader:
    def test_schema_wire_name(self):
        header = _header()
        assert header.schema_id == "ohlcv-1h"
        wire = header.model_dump(by_alias=True)
        assert wire["schema"] == "ohlcv-1h"
        assert "schema_id" not in wire

    def test_accepts_internal_name_too(self):
        payload = _header().model_dump(by_alias=True)
        payload["schema_id"] = payload.pop("schema")
        assert SeriesHeader.model_validate(payload).schema_id == "ohlcv-1h"

    def test_declared_semantics_are_required(self):
        # publisher/asof/fetched_at are nullable (empty/legacy envelopes lack
        # lineage); the rest of the declared semantics stay required.
        for missing in ("price_treatment", "tier", "price_currency"):
            payload = _header().model_dump(by_alias=True)
            payload.pop(missing)
            with pytest.raises(ValidationError):
                SeriesHeader.model_validate(payload)

    def test_lineage_fields_nullable(self):
        for missing in ("publisher", "asof", "fetched_at"):
            payload = _header().model_dump(by_alias=True)
            payload.pop(missing)
            assert getattr(SeriesHeader.model_validate(payload), missing) is None

    def test_defaults(self):
        header = _header()
        assert header.feed_scope == FeedScope.COMPOSITE
        assert header.ts_unit == "ms"
        assert header.revision == 0
        assert header.schema_version == 1
        assert header.coverage.is_complete is False


class TestSeries:
    def test_to_wire_round_trip(self):
        series = Series(header=_header(), records=[_bar(), _bar(ts_event=2)])
        wire = series.to_wire()
        assert wire["header"]["schema"] == "ohlcv-1h"
        assert wire["records"][0]["time"] == wire["records"][0]["ts_event"]
        again = Series.model_validate(wire)
        assert again.header.schema_id == "ohlcv-1h"
        assert [r.ts_event for r in again.records] == [1_750_000_000_000, 2]

    def test_index_series_null_volume_round_trip(self):
        series = Series(header=_header(instrument_key="SPX.INDEX"),
                        records=[_bar(volume=None)])
        again = Series.model_validate(series.to_wire())
        assert again.records[0].volume is None


class TestCoverage:
    def test_gap_bookkeeping(self):
        cov = Coverage(
            requested_start=0, requested_end=100,
            returned_start=0, returned_end=100,
            gaps=[Gap(start=10, end=20)],
        )
        assert cov.gaps[0].start == 10
        assert cov.is_complete is False
