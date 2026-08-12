"""Tests for chart selection context utilities.

Covers parse_chart_selection_contexts (dict + model variants, type filtering),
build_chart_selection_reminder (empty / region / price_level / multi / truncation
note), and the ChartSelectionContext geometry validation surfaced through them.
"""

import pytest
from pydantic import ValidationError

from src.server.models.additional_context import (
    ChartSelectionContext,
    DirectiveContext,
    MultimodalContext,
)
from src.server.utils.chart_selection_context import (
    build_chart_selection_reminder,
    parse_chart_selection_contexts,
    serialize_chart_selections_for_metadata,
)


_REGION_DICT = {
    "type": "chart_selection",
    "symbol": "nvda",
    "timeframe": "1day",
    "selection_type": "region",
    "time_start": "2024-01-03T00:00:00.000Z",
    "time_end": "2024-02-15T00:00:00.000Z",
    "price_low": 180.5,
    "price_high": 195.0,
    "bars": [
        {
            "time": "2024-01-03T00:00:00.000Z",
            "open": 1,
            "high": 2,
            "low": 1,
            "close": 2,
            "volume": 1000,
        }
    ],
    "bars_truncated": False,
    "label": None,
}

_PRICE_LEVEL_DICT = {
    "type": "chart_selection",
    "symbol": "aapl",
    "timeframe": "1hour",
    "selection_type": "price_level",
    "price_low": 200.0,
    "price_high": 200.0,
    "bars": [],
}


# ---------------------------------------------------------------------------
# parse_chart_selection_contexts
# ---------------------------------------------------------------------------


class TestParseChartSelectionContexts:
    def test_none_input_returns_empty(self):
        assert parse_chart_selection_contexts(None) == []

    def test_empty_list_returns_empty(self):
        assert parse_chart_selection_contexts([]) == []

    def test_dict_region_round_trips(self):
        result = parse_chart_selection_contexts([_REGION_DICT])
        assert len(result) == 1
        sel = result[0]
        assert isinstance(sel, ChartSelectionContext)
        assert sel.symbol == "nvda"
        assert sel.timeframe == "1day"
        assert sel.selection_type == "region"
        assert sel.time_start == "2024-01-03T00:00:00.000Z"
        assert sel.price_low == 180.5
        assert sel.price_high == 195.0
        assert len(sel.bars) == 1

    def test_pydantic_model_passes_through(self):
        ctx = ChartSelectionContext(
            symbol="NVDA",
            timeframe="1day",
            selection_type="price_level",
            price_low=205.0,
            price_high=205.0,
        )
        result = parse_chart_selection_contexts([ctx])
        assert result == [ctx]

    def test_filters_out_other_context_types(self):
        result = parse_chart_selection_contexts(
            [
                {"type": "directive", "content": "ignore me"},
                {"type": "image", "data": "data:image/jpeg;base64,xxx"},
                _REGION_DICT,
            ]
        )
        assert len(result) == 1
        assert result[0].selection_type == "region"

    def test_coexists_with_directive_and_multimodal(self):
        mixed = [
            DirectiveContext(type="directive", content="hello"),
            MultimodalContext(type="image", data="data:image/jpeg;base64,xxx"),
            ChartSelectionContext(
                symbol="NVDA",
                timeframe="1day",
                selection_type="price_level",
                price_low=200.0,
                price_high=200.0,
            ),
        ]
        result = parse_chart_selection_contexts(mixed)
        assert len(result) == 1
        assert result[0].symbol == "NVDA"


# ---------------------------------------------------------------------------
# build_chart_selection_reminder
# ---------------------------------------------------------------------------


class TestBuildChartSelectionReminder:
    def test_empty_returns_none(self):
        assert build_chart_selection_reminder([]) is None

    def test_reminder_guard_contract(self):
        """Empty -> None (handler skips append); populated -> a <chart-selection> string."""
        assert build_chart_selection_reminder([]) is None
        [sel] = parse_chart_selection_contexts([_PRICE_LEVEL_DICT])
        reminder = build_chart_selection_reminder([sel])
        assert isinstance(reminder, str)
        assert "<chart-selection" in reminder

    def test_region_renders_bounds_table_and_draw_back(self):
        [sel] = parse_chart_selection_contexts([_REGION_DICT])
        result = build_chart_selection_reminder([sel])
        assert result is not None
        # Envelope shape matches the widget reminder.
        assert result.startswith("\n\n<system-reminder>\n")
        assert result.endswith("\n</system-reminder>")
        # chart_id upper-cases the symbol.
        assert "<chart-selection chart_id='NVDA:1day' selection_type='region'>" in result
        # Bounds.
        assert "Time range: 2024-01-03T00:00:00.000Z → 2024-02-15T00:00:00.000Z" in result
        assert "180.5" in result and "195" in result
        # OHLCV table.
        assert "| time | open | high | low | close | volume |" in result
        # Prices render via %g (FP-noise stripped, trailing .0 dropped); volume
        # stays a plain float string.
        assert "| 2024-01-03T00:00:00.000Z | 1 | 2 | 1 | 2 | 1000.0 |" in result
        # Draw-back line: rectangle corners map to (start, high) / (end, low).
        assert "To annotate this back onto the chart, call draw_chart_annotation(" in result
        assert '"type": "rectangle"' in result
        assert '"point1": {"time": "2024-01-03T00:00:00.000Z", "price": 195}' in result
        assert '"point2": {"time": "2024-02-15T00:00:00.000Z", "price": 180.5}' in result

    def test_price_level_renders_single_price_and_price_line(self):
        [sel] = parse_chart_selection_contexts([_PRICE_LEVEL_DICT])
        result = build_chart_selection_reminder([sel])
        assert result is not None
        assert "<chart-selection chart_id='AAPL:1hour' selection_type='price_level'>" in result
        assert "Price level: 200" in result
        # No region-only time range line.
        assert "Time range:" not in result
        # Empty bars → no OHLCV table.
        assert "| time | open | high | low | close | volume |" not in result
        assert '"type": "price_line", "price": 200' in result

    def test_price_noise_stripped_in_render(self):
        # Pixel→price interpolation can deliver 195.10000000000002; the rendered
        # level and draw-back hint should show the clean 195.1.
        sel = ChartSelectionContext(
            symbol="NVDA",
            timeframe="1day",
            selection_type="price_level",
            price_low=195.10000000000002,
            price_high=195.10000000000002,
        )
        result = build_chart_selection_reminder([sel])
        assert result is not None
        assert "195.10000000000002" not in result
        assert "Price level: 195.1" in result
        assert '"price": 195.1' in result

    def test_includes_explainer_preamble(self):
        [sel] = parse_chart_selection_contexts([_REGION_DICT])
        result = build_chart_selection_reminder([sel])
        assert result is not None
        assert "selected" in result.lower()
        # Preamble appears before the first block.
        assert result.find("The user selected") < result.find("<chart-selection")

    def test_multiple_selections_share_one_envelope(self):
        sels = parse_chart_selection_contexts([_REGION_DICT, _PRICE_LEVEL_DICT])
        result = build_chart_selection_reminder(sels)
        assert result is not None
        assert result.count("<system-reminder>") == 1
        assert result.count("</system-reminder>") == 1
        assert result.count("<chart-selection ") == 2
        assert result.count("</chart-selection>") == 2

    def test_user_note_rendered_when_label_present(self):
        sel = ChartSelectionContext(
            symbol="NVDA",
            timeframe="1day",
            selection_type="price_level",
            price_low=205.0,
            price_high=205.0,
            label="watch this level",
        )
        result = build_chart_selection_reminder([sel])
        assert result is not None
        assert "User note: watch this level" in result

    def test_bars_truncation_note(self):
        bars = [
            {
                "time": f"2024-01-{(i % 28) + 1:02d}T00:00:00.000Z",
                "open": i,
                "high": i + 1,
                "low": i,
                "close": i + 1,
                "volume": 100 * i,
            }
            for i in range(120)
        ]
        sel = ChartSelectionContext(
            symbol="NVDA",
            timeframe="1day",
            selection_type="region",
            time_start="2024-01-01T00:00:00.000Z",
            time_end="2024-04-30T00:00:00.000Z",
            price_low=100.0,
            price_high=200.0,
            bars=bars,
        )
        result = build_chart_selection_reminder([sel])
        assert result is not None
        assert "(truncated, 100 of 120 bars shown)" in result


# ---------------------------------------------------------------------------
# ChartSelectionContext geometry validation
# ---------------------------------------------------------------------------


class TestChartSelectionGeometry:
    def test_price_low_high_swapped_when_inverted(self):
        sel = ChartSelectionContext(
            symbol="NVDA",
            timeframe="1day",
            selection_type="region",
            time_start="2024-01-03T00:00:00.000Z",
            time_end="2024-02-15T00:00:00.000Z",
            price_low=195.0,
            price_high=180.5,
        )
        assert sel.price_low == 180.5
        assert sel.price_high == 195.0

    def test_region_missing_times_rejected(self):
        with pytest.raises(ValidationError):
            ChartSelectionContext(
                symbol="NVDA",
                timeframe="1day",
                selection_type="region",
                price_low=180.5,
                price_high=195.0,
            )

    def test_price_level_allows_missing_times(self):
        sel = ChartSelectionContext(
            symbol="NVDA",
            timeframe="1day",
            selection_type="price_level",
            price_low=200.0,
            price_high=200.0,
        )
        assert sel.time_start is None

    def test_bad_timeframe_rejected(self):
        with pytest.raises(ValidationError):
            ChartSelectionContext(
                symbol="NVDA",
                timeframe="2day",
                selection_type="price_level",
                price_low=200.0,
                price_high=200.0,
            )

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nan_inf_prices_rejected(self, bad):
        with pytest.raises(ValidationError):
            ChartSelectionContext(
                symbol="NVDA",
                timeframe="1day",
                selection_type="price_level",
                price_low=bad,
                price_high=bad,
            )

    def test_oversize_bars_capped_and_flagged(self):
        bars = [{"time": "2024-01-01T00:00:00.000Z", "close": 1}] * 600
        sel = ChartSelectionContext(
            symbol="NVDA",
            timeframe="1day",
            selection_type="price_level",
            price_low=200.0,
            price_high=200.0,
            bars=bars,
        )
        assert len(sel.bars) == 500
        assert sel.bars_truncated is True

    def test_oversize_label_rejected(self):
        with pytest.raises(ValidationError):
            ChartSelectionContext(
                symbol="NVDA",
                timeframe="1day",
                selection_type="price_level",
                price_low=200.0,
                price_high=200.0,
                label="x" * 501,
            )


# ---------------------------------------------------------------------------
# serialize_chart_selections_for_metadata
# ---------------------------------------------------------------------------


class TestSerializeChartSelectionsForMetadata:
    def test_empty_returns_empty(self):
        assert serialize_chart_selections_for_metadata([]) == []

    def test_region_emits_camelcase_snapshot_with_bounds_and_bars(self):
        sels = parse_chart_selection_contexts([{**_REGION_DICT, "label": "  retest  "}])
        out = serialize_chart_selections_for_metadata(sels)
        assert out == [
            {
                "selectionType": "region",
                "symbol": "nvda",
                "timeframe": "1day",
                "priceLow": 180.5,
                "priceHigh": 195.0,
                "comment": "  retest  ",
                "timeStart": "2024-01-03T00:00:00.000Z",
                "timeEnd": "2024-02-15T00:00:00.000Z",
                # model_dump emits the lowercase OHLCV keys the frontend snapshot
                # reads, with numeric fields coerced to float.
                "bars": [
                    {
                        "time": "2024-01-03T00:00:00.000Z",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 1.0,
                        "close": 2.0,
                        "volume": 1000.0,
                    }
                ],
                "barsTruncated": False,
            }
        ]

    def test_price_level_omits_comment_and_time_bounds(self):
        sels = parse_chart_selection_contexts([_PRICE_LEVEL_DICT])
        out = serialize_chart_selections_for_metadata(sels)
        assert out == [
            {
                "selectionType": "price_level",
                "symbol": "aapl",
                "timeframe": "1hour",
                "priceLow": 200.0,
                "priceHigh": 200.0,
                "bars": [],
                "barsTruncated": False,
            }
        ]
        assert "comment" not in out[0]
        assert "timeStart" not in out[0]
        assert "timeEnd" not in out[0]
