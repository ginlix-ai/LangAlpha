"""Tests for widget context utilities.

Covers parse_widget_contexts (dict + model variants), build_widget_context_reminder
(empty / single / multi-widget), and serialize_widget_contexts_for_metadata.
"""

from datetime import datetime, timezone

import pytest

from src.server.models.additional_context import (
    DirectiveContext,
    MultimodalContext,
    WidgetContext,
)
from src.server.utils.widget_context import (
    build_widget_context_reminder,
    parse_widget_contexts,
    serialize_widget_contexts_for_metadata,
)


# ---------------------------------------------------------------------------
# parse_widget_contexts
# ---------------------------------------------------------------------------


class TestParseWidgetContexts:
    def test_none_input_returns_empty(self):
        assert parse_widget_contexts(None) == []

    def test_empty_list_returns_empty(self):
        assert parse_widget_contexts([]) == []

    def test_dict_widget_round_trips(self):
        result = parse_widget_contexts(
            [
                {
                    "type": "widget",
                    "widget_type": "markets.chart",
                    "widget_id": "abc-123",
                    "label": "NVDA · 1d Chart",
                    "text": "<widget-context type='markets.chart'>...</widget-context>",
                    "data": {"bars": [], "summary": {}},
                    "captured_at": "2026-04-26T11:42:08+00:00",
                    "description": "120 daily bars",
                }
            ]
        )
        assert len(result) == 1
        w = result[0]
        assert isinstance(w, WidgetContext)
        assert w.widget_type == "markets.chart"
        assert w.widget_id == "abc-123"
        assert w.label == "NVDA · 1d Chart"
        assert w.text.startswith("<widget-context")
        assert w.data == {"bars": [], "summary": {}}
        assert w.captured_at is not None
        assert w.captured_at.tzinfo is not None
        assert w.description == "120 daily bars"

    def test_dict_with_z_suffix_iso_string(self):
        result = parse_widget_contexts(
            [
                {
                    "type": "widget",
                    "widget_type": "news.feed",
                    "widget_id": "x",
                    "label": "News",
                    "text": "<widget-context>news</widget-context>",
                    "captured_at": "2026-04-26T11:42:08Z",
                }
            ]
        )
        assert result[0].captured_at is not None

    def test_dict_with_invalid_captured_at_returns_none(self):
        result = parse_widget_contexts(
            [
                {
                    "type": "widget",
                    "widget_type": "x",
                    "widget_id": "x",
                    "label": "x",
                    "text": "<widget-context>x</widget-context>",
                    "captured_at": "not-a-date",
                }
            ]
        )
        assert result[0].captured_at is None

    def test_pydantic_model_passes_through(self):
        ctx = WidgetContext(
            type="widget",
            widget_type="watchlist.list",
            widget_id="ws-1",
            label="Tech Watch",
            text="<widget-context>...</widget-context>",
            data={"rows": []},
        )
        result = parse_widget_contexts([ctx])
        assert result == [ctx]

    def test_filters_out_other_context_types(self):
        result = parse_widget_contexts(
            [
                {
                    "type": "directive",
                    "content": "ignore me",
                },
                {
                    "type": "image",
                    "data": "data:image/jpeg;base64,xxx",
                },
                {
                    "type": "widget",
                    "widget_type": "tv.heatmap",
                    "widget_id": "tv-1",
                    "label": "Heatmap",
                    "text": "<widget-context>tv</widget-context>",
                },
            ]
        )
        assert len(result) == 1
        assert result[0].widget_type == "tv.heatmap"

    def test_coexists_with_directive_and_multimodal(self):
        """Mixed additional_context list — only widget items are returned."""
        mixed = [
            DirectiveContext(type="directive", content="hello"),
            MultimodalContext(type="image", data="data:image/jpeg;base64,xxx"),
            WidgetContext(
                type="widget",
                widget_type="markets.chart",
                widget_id="w1",
                label="Chart",
                text="<widget-context>...</widget-context>",
            ),
        ]
        result = parse_widget_contexts(mixed)
        assert len(result) == 1
        assert result[0].widget_id == "w1"

    def test_dict_with_missing_optional_fields_uses_defaults(self):
        result = parse_widget_contexts(
            [
                {
                    "type": "widget",
                    "widget_type": "x.y",
                    "widget_id": "id",
                    "label": "L",
                    "text": "<widget-context>t</widget-context>",
                }
            ]
        )
        assert result[0].data == {}
        assert result[0].captured_at is None
        assert result[0].description is None


# ---------------------------------------------------------------------------
# build_widget_context_reminder
# ---------------------------------------------------------------------------


class TestBuildWidgetContextReminder:
    def test_empty_returns_none(self):
        assert build_widget_context_reminder([]) is None

    def test_single_widget_wraps_in_system_reminder(self):
        widgets = [
            WidgetContext(
                type="widget",
                widget_type="markets.chart",
                widget_id="w1",
                label="NVDA",
                text="<widget-context type='markets.chart' symbol='NVDA'>chart data</widget-context>",
            )
        ]
        result = build_widget_context_reminder(widgets)
        assert result is not None
        assert result.startswith("\n\n<system-reminder>\n")
        assert result.endswith("\n</system-reminder>")
        assert "<widget-context type='markets.chart' symbol='NVDA'>" in result

    def test_multiple_widgets_concatenated(self):
        widgets = [
            WidgetContext(
                type="widget",
                widget_type="markets.chart",
                widget_id="w1",
                label="NVDA",
                text="<widget-context type='markets.chart'>NVDA</widget-context>",
            ),
            WidgetContext(
                type="widget",
                widget_type="news.feed",
                widget_id="w2",
                label="News",
                text="<widget-context type='news.feed'>headline</widget-context>",
            ),
        ]
        result = build_widget_context_reminder(widgets)
        assert result is not None
        assert "NVDA" in result
        assert "headline" in result
        # Both widgets share one envelope
        assert result.count("<system-reminder>") == 1
        assert result.count("</system-reminder>") == 1
        # Both pre-rendered widget tags survive intact. Count closing tags
        # because the system-reminder preamble mentions `<widget-context>`
        # by name in prose.
        assert result.count("</widget-context>") == 2

    def test_reminder_includes_explainer_preamble(self):
        """The reminder must explain to the agent that the blocks are
        user-attached dashboard snapshots and should be evaluated for
        relevance — otherwise the agent can't tell apart load-bearing
        context from incidental clicks."""
        widgets = [
            WidgetContext(
                type="widget",
                widget_type="markets.chart",
                widget_id="w1",
                label="x",
                text="<widget-context>chart</widget-context>",
            )
        ]
        result = build_widget_context_reminder(widgets)
        assert result is not None
        # User-action framing
        assert "user attached" in result.lower()
        # Relevance evaluation framing
        assert "relevant" in result.lower() or "relevance" in result.lower()
        # Preamble appears before the first widget block
        preamble_end = result.find("<widget-context>chart")
        assert preamble_end > len("\n\n<system-reminder>\n")

    def test_widget_with_empty_text_skipped(self):
        widgets = [
            WidgetContext(
                type="widget",
                widget_type="x",
                widget_id="x",
                label="x",
                text="   ",
            ),
        ]
        assert build_widget_context_reminder(widgets) is None

    def test_mixed_empty_and_non_empty(self):
        widgets = [
            WidgetContext(
                type="widget",
                widget_type="x",
                widget_id="x",
                label="x",
                text="",
            ),
            WidgetContext(
                type="widget",
                widget_type="y",
                widget_id="y",
                label="y",
                text="<widget-context>kept</widget-context>",
            ),
        ]
        result = build_widget_context_reminder(widgets)
        assert result is not None
        assert "kept" in result
        # Count closing tags so the preamble's literal mention of
        # `<widget-context>` doesn't inflate the count.
        assert result.count("</widget-context>") == 1


# ---------------------------------------------------------------------------
# serialize_widget_contexts_for_metadata
# ---------------------------------------------------------------------------


class TestSerializeForMetadata:
    def test_keeps_text_and_data(self):
        widgets = [
            WidgetContext(
                type="widget",
                widget_type="markets.chart",
                widget_id="w1",
                label="Chart",
                text="<widget-context>huge prompt-only payload</widget-context>",
                data={"bars": [{"o": 1, "c": 2}]},
                captured_at=datetime(2026, 4, 26, 11, 42, 8, tzinfo=timezone.utc),
                description="120 bars",
            )
        ]
        out = serialize_widget_contexts_for_metadata(widgets)
        assert len(out) == 1
        assert out[0]["widget_type"] == "markets.chart"
        assert out[0]["widget_id"] == "w1"
        assert out[0]["label"] == "Chart"
        assert out[0]["data"] == {"bars": [{"o": 1, "c": 2}]}
        assert out[0]["captured_at"] == "2026-04-26T11:42:08+00:00"
        assert out[0]["description"] == "120 bars"
        # Text is kept so the chip preview UI can show what the agent saw
        assert out[0]["text"] == "<widget-context>huge prompt-only payload</widget-context>"

    def test_handles_missing_captured_at(self):
        widgets = [
            WidgetContext(
                type="widget",
                widget_type="x",
                widget_id="x",
                label="x",
                text="<widget-context>x</widget-context>",
            )
        ]
        out = serialize_widget_contexts_for_metadata(widgets)
        assert out[0]["captured_at"] is None
