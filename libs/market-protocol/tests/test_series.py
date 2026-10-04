"""build_series: the served unit, row hygiene, ordering and the header currency."""

import math

import pytest

from market_protocol import (
    PriceTreatment,
    Tier,
    build_series,
    served_display_decimals,
    served_display_unit,
    to_canonical,
)

T0 = 1_750_000_000_000
# Past float and int64 range: float() raises on it rather than returning inf.
HUGE = pytest.param(10**400, id="10**400")


def _row(ts=T0, **overrides):
    base = {"time": ts, "open": 100.0, "high": 110.0, "low": 90.0,
            "close": 105.0, "volume": 1000.0}
    base.update(overrides)
    return base


def _series(rows, ref=None, schema="ohlcv-1d", **kw):
    return build_series(
        rows,
        ref=ref or to_canonical("XYZ"),
        schema=schema,
        publisher="test",
        price_treatment=PriceTreatment.SPLIT_ADJUSTED,
        tier=Tier.DELAYED_15M,
        **kw,
    )


def _ts(series):
    return [r.ts_event for r in series.records]


class TestServedUnit:
    def test_gbx_rows_serve_in_pounds(self):
        series = _series([_row(vwap=102.0)], ref=to_canonical("ABC.L"))
        bar = series.records[0]
        assert (bar.open, bar.high, bar.low, bar.close, bar.vwap) == (1.0, 1.1, 0.9, 1.05, 1.02)
        assert bar.volume == 1000.0  # a share count, never scaled
        assert series.header.display_unit is None
        assert series.header.price_currency == "GBP"

    def test_an_index_on_a_pence_venue_serves_points(self):
        series = _series([_row()], ref=to_canonical("^FTSE.L"))
        assert series.records[0].close == 105.0
        assert series.header.display_decimals == 2

    def test_pence_divide_exactly(self):
        # Multiplying by 0.01 would serve 1.2345000000000002.
        bar = _series([_row(open=123.45, close=54.32)], ref=to_canonical("ABC.L")).records[0]
        assert (bar.open, bar.close) == (1.2345, 0.5432)

    def test_pence_keep_their_decimals_in_pounds(self):
        # GBP shows 2 places; a pence series served in pounds needs 2 more, or 54.32p shows as 0.54.
        assert _series([_row()], ref=to_canonical("ABC.L")).header.display_decimals == 4
        assert _series([_row()]).header.display_decimals == 2

    @pytest.mark.parametrize("spelling", ["ABC.L", "AAPL", "EURUSD=X"])
    def test_a_header_built_elsewhere_gets_the_same_decimals(self, spelling):
        ref = to_canonical(spelling)
        assert served_display_decimals(ref) == _series([_row()], ref=ref).header.display_decimals

    def test_major_unit_rows_pass_through(self):
        bar = _series([_row()]).records[0]
        assert (bar.open, bar.close) == (100.0, 105.0)

    @pytest.mark.parametrize(("unit", "served"), [("GBX", None), (None, None), ("USD", "USD")])
    def test_served_display_unit(self, unit, served):
        assert served_display_unit(unit) == served


class TestRowHygiene:
    @pytest.mark.parametrize("ts", [0, -1])
    def test_non_positive_timestamp_dropped(self, ts):
        assert _ts(_series([_row(ts=ts), _row()])) == [T0]

    @pytest.mark.parametrize("ts", [None, "bad", math.nan, math.inf, HUGE, True, False])
    def test_unreadable_timestamp_drops_only_its_row(self, ts):
        assert _ts(_series([_row(ts=ts), _row()])) == [T0]

    def test_missing_timestamp_drops_only_its_row(self):
        partial = _row()
        del partial["time"]
        assert _ts(_series([partial, _row(ts=T0 + 1)])) == [T0 + 1]

    def test_failing_ts_of_drops_only_its_row(self):
        def ts_of(row):
            if row["close"] == 0:
                raise ValueError("unparseable date")
            return row["time"]

        assert _ts(_series([_row(ts=T0 + 1, close=0), _row()], ts_of=ts_of)) == [T0]

    @pytest.mark.parametrize("field", ["open", "high", "low", "close"])
    @pytest.mark.parametrize("bad", [None, math.nan, math.inf, -math.inf, "n/a", HUGE, True, False])
    def test_unusable_price_drops_the_row(self, field, bad):
        series = _series([_row(ts=T0 + 1, **{field: bad}), _row()])
        assert _ts(series) == [T0]

    @pytest.mark.parametrize("field", ["open", "high", "low", "close"])
    def test_missing_price_drops_the_row(self, field):
        partial = _row(ts=T0 + 1)
        del partial[field]
        assert _ts(_series([partial, _row()])) == [T0]

    @pytest.mark.parametrize("bad", [None, math.nan, math.inf, HUGE, True])
    def test_non_finite_volume_and_vwap_become_null(self, bad):
        bar = _series([_row(volume=bad, vwap=bad)]).records[0]
        assert (bar.volume, bar.vwap) == (None, None)

    def test_negative_volume_is_null(self):
        assert _series([_row(volume=-1)]).records[0].volume is None

    def test_absent_volume_is_null(self):
        row = _row()
        del row["volume"]
        assert _series([row]).records[0].volume is None


class TestEnrichment:
    def test_trades_and_is_final_carried(self):
        bar = _series([_row(trades=17, is_final=True)]).records[0]
        assert (bar.trades, bar.is_final) == (17, True)

    def test_absent_enrichment_keeps_defaults(self):
        bar = _series([_row()]).records[0]
        assert (bar.trades, bar.is_final) == (None, False)

    @pytest.mark.parametrize("trades", [17.0, "17"])
    def test_integral_trades_accepted(self, trades):
        assert _series([_row(trades=trades)]).records[0].trades == 17

    @pytest.mark.parametrize("trades", [-1, 1.5, math.nan, math.inf, True, "n/a", HUGE])
    def test_unusable_trades_become_null(self, trades):
        assert _series([_row(trades=trades)]).records[0].trades is None

    @pytest.mark.parametrize("is_final", [1, "true", "yes", None])
    def test_is_final_needs_a_real_bool(self, is_final):
        assert _series([_row(is_final=is_final)]).records[0].is_final is False


class TestOrdering:
    def test_rows_sorted_ascending(self):
        assert _ts(_series([_row(ts=T0 + 2), _row(ts=T0), _row(ts=T0 + 1)])) == [
            T0, T0 + 1, T0 + 2,
        ]

    def test_last_row_wins_on_a_repeated_timestamp(self):
        series = _series([
            _row(ts=T0, close=101.0),
            _row(ts=T0 + 1),
            _row(ts=T0, close=102.0),
        ])
        assert _ts(series) == [T0, T0 + 1]
        assert series.records[0].close == 102.0

    def test_unusable_repeat_keeps_the_earlier_row(self):
        series = _series([_row(close=101.0), _row(close=math.nan)])
        assert [r.close for r in series.records] == [101.0]

    def test_watermark_is_last_timestamp(self):
        assert _series([_row(ts=T0 + 5), _row(ts=T0)]).header.watermark == T0 + 5

    def test_empty_series_has_no_watermark(self):
        series = _series([])
        assert series.records == []
        assert series.header.watermark is None


class TestHeaderCurrency:
    def test_usd_quoted_fund_on_a_gbp_venue(self):
        # No seed quotes an XLON listing in USD yet; such a seed sets
        # price_currency and clears the pence unit, which this ref mirrors.
        ref = to_canonical("ABC.L").model_copy(
            update={"price_currency": "USD", "display_unit": None}
        )
        series = _series([_row()], ref=ref)
        assert series.records[0].close == 105.0
        assert (series.header.price_currency, series.header.display_unit) == ("USD", None)

    @pytest.mark.parametrize(("price_currency", "decimals"), [("USD", 2), ("JPY", 0)])
    def test_decimals_follow_the_served_currency(self, price_currency, decimals):
        ref = to_canonical("XYZ").model_copy(update={"price_currency": price_currency})
        header = _series([_row()], ref=ref).header
        assert (header.price_currency, header.display_decimals) == (price_currency, decimals)


class TestHeaderSchema:
    @pytest.mark.parametrize("spelling", ["ohlcv-1m", "1m", "1min"])
    def test_any_spelling_goes_out_canonical(self, spelling):
        assert _series([_row()], schema=spelling).header.schema_id == "ohlcv-1m"

    def test_an_unknown_schema_is_refused(self):
        with pytest.raises(ValueError, match="Unknown interval"):
            _series([_row()], schema="1week")
