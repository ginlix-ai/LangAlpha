"""Tests for the AdditionalContext discriminated union.

Verifies that all five context types parse correctly through the union and
that required-field validation fires for each.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from src.server.models.additional_context import (
    AdditionalContext,
    DirectiveContext,
    MultimodalContext,
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
