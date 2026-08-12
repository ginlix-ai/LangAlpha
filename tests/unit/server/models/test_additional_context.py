"""Tests for the AdditionalContext discriminated union.

Verifies that all five context types parse correctly through the union and
that required-field validation fires for each.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from src.server.models.additional_context import (
    AdditionalContext,
    ChartSelectionContext,
    DirectiveContext,
    MultimodalContext,
    SelectionBar,
    SkillContext,
    WidgetContext,
)


_adapter = TypeAdapter(AdditionalContext)


class TestDiscriminator:
    def test_skills_routes_to_skill_context(self):
        ctx = _adapter.validate_python(
            {"type": "skills", "name": "user-profile"}
        )
        assert isinstance(ctx, SkillContext)
        assert ctx.name == "user-profile"

    def test_image_routes_to_multimodal(self):
        ctx = _adapter.validate_python(
            {"type": "image", "data": "data:image/png;base64,xx"}
        )
        assert isinstance(ctx, MultimodalContext)
        assert ctx.type == "image"

    def test_pdf_routes_to_multimodal(self):
        ctx = _adapter.validate_python(
            {"type": "pdf", "data": "data:application/pdf;base64,xx"}
        )
        assert isinstance(ctx, MultimodalContext)
        assert ctx.type == "pdf"

    def test_directive_routes_to_directive_context(self):
        ctx = _adapter.validate_python(
            {"type": "directive", "content": "follow this"}
        )
        assert isinstance(ctx, DirectiveContext)
        assert ctx.content == "follow this"

    def test_widget_routes_to_widget_context(self):
        ctx = _adapter.validate_python(
            {
                "type": "widget",
                "widget_type": "markets.chart",
                "widget_id": "abc",
                "label": "NVDA",
                "text": "<widget-context>...</widget-context>",
                "data": {"bars": []},
            }
        )
        assert isinstance(ctx, WidgetContext)
        assert ctx.widget_type == "markets.chart"
        assert ctx.label == "NVDA"

    def test_chart_selection_routes_to_chart_selection_context(self):
        ctx = _adapter.validate_python(
            {
                "type": "chart_selection",
                "symbol": "NVDA",
                "timeframe": "1day",
                "selection_type": "region",
                "time_start": "2024-01-03T00:00:00.000Z",
                "time_end": "2024-02-15T00:00:00.000Z",
                "price_low": 180.5,
                "price_high": 195.0,
            }
        )
        assert isinstance(ctx, ChartSelectionContext)
        assert ctx.symbol == "NVDA"
        assert ctx.selection_type == "region"


class TestChartSelectionContextValidation:
    def test_region_requires_both_times(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python(
                {
                    "type": "chart_selection",
                    "symbol": "NVDA",
                    "timeframe": "1day",
                    "selection_type": "region",
                    "time_start": "2024-01-03T00:00:00.000Z",
                    "price_low": 180.5,
                    "price_high": 195.0,
                }
            )

    def test_price_level_collapses_to_single_price(self):
        # A price_level is one price: the range is ordered, then the stray high
        # is collapsed onto price_low so render/replay can't disagree.
        ctx = _adapter.validate_python(
            {
                "type": "chart_selection",
                "symbol": "NVDA",
                "timeframe": "1day",
                "selection_type": "price_level",
                "price_low": 195.0,
                "price_high": 180.5,
            }
        )
        assert ctx.price_low == 180.5
        assert ctx.price_high == 180.5

    def test_region_price_range_ordered(self):
        ctx = _adapter.validate_python(
            {
                "type": "chart_selection",
                "symbol": "NVDA",
                "timeframe": "1day",
                "selection_type": "region",
                "time_start": "2024-01-03T00:00:00.000Z",
                "time_end": "2024-02-15T00:00:00.000Z",
                "price_low": 195.0,
                "price_high": 180.5,
            }
        )
        assert ctx.price_low == 180.5
        assert ctx.price_high == 195.0

    def test_bad_timeframe_rejected(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python(
                {
                    "type": "chart_selection",
                    "symbol": "NVDA",
                    "timeframe": "2day",
                    "selection_type": "price_level",
                    "price_low": 200.0,
                    "price_high": 200.0,
                }
            )

    def test_nan_price_rejected(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python(
                {
                    "type": "chart_selection",
                    "symbol": "NVDA",
                    "timeframe": "1day",
                    "selection_type": "price_level",
                    "price_low": float("nan"),
                    "price_high": float("nan"),
                }
            )


class TestSelectionBar:
    def test_nan_inf_open_rejected(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValidationError):
                SelectionBar(time="2024-01-03T00:00:00.000Z", open=bad)

    def test_unknown_keys_dropped(self):
        bar = SelectionBar.model_validate(
            {
                "time": "2024-01-03T00:00:00.000Z",
                "open": 1.0,
                "vwap": 1.5,
                "extra": "ignored",
            }
        )
        assert bar.open == 1.0
        assert not hasattr(bar, "vwap")
        assert "vwap" not in bar.model_dump()

    def test_oversize_time_rejected(self):
        with pytest.raises(ValidationError):
            SelectionBar(time="x" * 41)


class TestWidgetContextValidation:
    def test_missing_required_widget_type(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python(
                {
                    "type": "widget",
                    "widget_id": "abc",
                    "label": "NVDA",
                    "text": "<widget-context>...</widget-context>",
                }
            )

    def test_missing_required_widget_id(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python(
                {
                    "type": "widget",
                    "widget_type": "markets.chart",
                    "label": "NVDA",
                    "text": "<widget-context>...</widget-context>",
                }
            )

    def test_missing_required_label(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python(
                {
                    "type": "widget",
                    "widget_type": "markets.chart",
                    "widget_id": "abc",
                    "text": "<widget-context>...</widget-context>",
                }
            )

    def test_missing_required_text(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python(
                {
                    "type": "widget",
                    "widget_type": "markets.chart",
                    "widget_id": "abc",
                    "label": "NVDA",
                }
            )

    def test_data_defaults_to_empty_dict(self):
        ctx = _adapter.validate_python(
            {
                "type": "widget",
                "widget_type": "x",
                "widget_id": "x",
                "label": "x",
                "text": "<widget-context>x</widget-context>",
            }
        )
        assert ctx.data == {}

    def test_optional_fields_accept_none(self):
        ctx = _adapter.validate_python(
            {
                "type": "widget",
                "widget_type": "x",
                "widget_id": "x",
                "label": "x",
                "text": "<widget-context>x</widget-context>",
                "captured_at": None,
                "description": None,
            }
        )
        assert ctx.captured_at is None
        assert ctx.description is None

    def test_unknown_type_rejected(self):
        with pytest.raises(ValidationError):
            _adapter.validate_python({"type": "unknown_type"})


class TestMixedList:
    def test_list_of_mixed_context_types(self):
        items = [
            {"type": "directive", "content": "x"},
            {
                "type": "widget",
                "widget_type": "watchlist.list",
                "widget_id": "w1",
                "label": "L",
                "text": "<widget-context>w</widget-context>",
            },
            {"type": "image", "data": "data:image/jpeg;base64,xxx"},
        ]
        parsed = [_adapter.validate_python(i) for i in items]
        assert isinstance(parsed[0], DirectiveContext)
        assert isinstance(parsed[1], WidgetContext)
        assert isinstance(parsed[2], MultimodalContext)
