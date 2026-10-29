"""Protocol model invariants: aliases, nullability, wire shape."""

import copy
import math

import pytest
from pydantic import ValidationError

from market_protocol import (
    Coverage,
    FeedScope,
    Gap,
    OhlcvBar,
    PriceTreatment,
    Quote,
    Series,
    SeriesHeader,
    Tier,
)

NON_FINITE = [math.nan, math.inf, -math.inf]

# Wire names, fed through model_validate: a renamed field drops its value
# here and fails the equality pins below, where a keyword would be ignored.
COVERAGE_WIRE = {
    "requested_start": 0, "requested_end": 100,
    "returned_start": 10, "returned_end": 90,
    "truncated": True, "is_complete": True,
    "gaps": [{"start": 40, "end": 50}],
}


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


def _quote(**overrides):
    base = {"instrument_key": "XYZ.XNYS", "symbol": "XYZ", "price": 10.0,
            "currency": "USD", "tier": Tier.REALTIME, "publisher": "test"}
    base.update(overrides)
    return Quote.model_validate(base)


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

    @pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume", "vwap"])
    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_non_finite_rejected(self, field, bad):
        with pytest.raises(ValidationError):
            _bar(**{field: bad})

    @pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume", "vwap"])
    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_non_finite_assignment_rejected(self, field, bad):
        bar = _bar(vwap=1.0)
        with pytest.raises(ValidationError):
            setattr(bar, field, bad)
        assert math.isfinite(getattr(bar, field))


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

    @pytest.mark.parametrize("spelling", ["ohlcv-1h", "1h", "1hour"])
    def test_schema_reads_as_the_canonical_id(self, spelling):
        assert _header(schema=spelling).schema_id == "ohlcv-1h"

    def test_an_unknown_schema_is_refused(self):
        with pytest.raises(ValidationError, match="Unknown interval"):
            _header(schema="1week")

    def test_declared_semantics_are_required(self):
        # publisher/asof/fetched_at are nullable (empty/legacy envelopes lack
        # lineage); the rest of the declared semantics stay required.
        for missing in ("price_treatment", "tier", "price_currency"):
            payload = _header().model_dump(by_alias=True)
            payload.pop(missing)
            with pytest.raises(ValidationError):
                SeriesHeader.model_validate(payload)

    def test_price_treatment_never_null(self):
        with pytest.raises(ValidationError):
            _header(price_treatment=None)

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


def _wire():
    header = _header(coverage=COVERAGE_WIRE)
    return Series(header=header, records=[_bar(vwap=1.2, trades=3, is_final=True)]).to_wire()


class TestSeries:
    def test_wire_field_names(self):
        wire = _wire()
        assert set(wire["header"]) == {
            "instrument_key", "schema", "price_treatment", "publisher", "tier",
            "feed_scope", "price_currency", "display_decimals", "display_unit",
            "ts_unit", "latest_trading_date", "revision", "asof", "coverage",
            "fetched_at", "watermark", "schema_version",
        }
        assert set(wire["records"][0]) == {
            "ts_event", "time", "open", "high", "low", "close", "volume", "vwap",
            "trades", "is_final",
        }

    def test_unknown_keys_ignored_at_every_level(self):
        # Readers ignoring what they do not know keeps SCHEMA_VERSION additive between two pins.
        wire = _wire()
        newer = copy.deepcopy(wire)
        newer["header"]["added_later"] = 1
        newer["header"]["coverage"]["added_later"] = 1
        newer["header"]["coverage"]["gaps"][0]["added_later"] = 1
        newer["records"][0]["added_later"] = 1
        assert Series.model_validate(newer).to_wire() == wire

    def test_to_wire_round_trip(self):
        series = Series(header=_header(), records=[_bar(), _bar(ts_event=2)])
        wire = series.to_wire()
        assert wire["header"]["schema"] == "ohlcv-1h"
        assert wire["records"][0]["time"] == wire["records"][0]["ts_event"]
        again = Series.model_validate(wire)
        assert again.header.schema_id == "ohlcv-1h"
        assert [r.ts_event for r in again.records] == [1_750_000_000_000, 2]

    def test_wire_envelope_shape(self):
        wire = Series(header=_header(), records=[]).to_wire()
        assert set(wire) == {"header", "records"}
        assert wire["header"]["ts_unit"] == "ms"

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

    def test_wire_field_names(self):
        coverage = _wire()["header"]["coverage"]
        assert set(coverage) == {
            "requested_start", "requested_end", "returned_start", "returned_end",
            "truncated", "is_complete", "gaps",
        }
        assert set(coverage["gaps"][0]) == {"start", "end"}
        assert coverage == COVERAGE_WIRE


class TestQuote:
    def test_wire_field_names(self):
        wire = _quote(as_of=1_750_000_000_000).model_dump(by_alias=True, mode="json")
        assert set(wire) == {
            "instrument_key", "symbol", "name", "name_local", "price", "change",
            "change_percent", "previous_close", "open", "high", "low", "volume",
            "amount", "currency", "tier", "publisher", "as_of",
        }
        assert (wire["as_of"], wire["tier"]) == (1_750_000_000_000, "realtime")

    def test_required_fields(self):
        required = {name for name, f in Quote.model_fields.items() if f.is_required()}
        assert required == {"instrument_key", "symbol", "price", "currency", "tier", "publisher"}
        assert _quote().as_of is None  # unknown print time stays null, never 0

    @pytest.mark.parametrize("field", ["price", "change", "change_percent", "volume"])
    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_non_finite_rejected(self, field, bad):
        with pytest.raises(ValidationError):
            _quote(**{field: bad})

    @pytest.mark.parametrize("field", ["price", "change", "change_percent", "volume"])
    @pytest.mark.parametrize("bad", NON_FINITE)
    def test_non_finite_assignment_rejected(self, field, bad):
        # Unchecked, a NaN price dumps as NaN and model_dump_json() writes the required price as null.
        quote = _quote(change=0.1, change_percent=1.0, volume=5.0)
        with pytest.raises(ValidationError):
            setattr(quote, field, bad)
        assert math.isfinite(getattr(quote, field))

    def test_unknown_key_ignored(self):
        wire = _quote().model_dump(by_alias=True, mode="json")
        again = Quote.model_validate({**wire, "added_later": 1})
        assert again.model_dump(by_alias=True, mode="json") == wire
