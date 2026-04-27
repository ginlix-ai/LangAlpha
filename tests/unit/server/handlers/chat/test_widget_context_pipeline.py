"""Pipeline test: WidgetContext + MultimodalContext + DirectiveContext coexist.

Verifies that parsing + reminder building + injection compose without conflict.
This is the unit-level "integration" check called for in the plan — the full
HTTP round-trip would require live DB/Redis/API keys; the parsers themselves
are pure and easy to compose into a deterministic test.
"""

from src.server.handlers.chat._common import _append_to_last_user_message
from src.server.utils.directive_context import (
    build_directive_reminder,
    parse_directive_contexts,
)
from src.server.utils.multimodal_context import (
    inject_multimodal_context,
    parse_multimodal_contexts,
)
from src.server.utils.widget_context import (
    build_widget_context_reminder,
    parse_widget_contexts,
    serialize_widget_contexts_for_metadata,
)


def _seed_messages():
    return [
        {"role": "user", "content": "compare the chart and the news"},
    ]


def test_widget_directive_lands_in_last_user_message():
    raw = [
        {
            "type": "widget",
            "widget_type": "markets.chart",
            "widget_id": "w1",
            "label": "NVDA",
            "text": "<widget-context type='markets.chart' symbol='NVDA'>chart payload</widget-context>",
            "data": {"bars": []},
        }
    ]
    widgets = parse_widget_contexts(raw)
    reminder = build_widget_context_reminder(widgets)
    assert reminder is not None

    messages = _seed_messages()
    _append_to_last_user_message(messages, reminder)
    last = messages[-1]
    # User message content is now a list of content blocks (the helper wraps
    # text + reminder), or a string with the reminder appended. Either is OK.
    rendered = last["content"] if isinstance(last["content"], str) else "".join(
        b.get("text", "") for b in last["content"] if isinstance(b, dict)
    )
    assert "<system-reminder>" in rendered
    assert "<widget-context type='markets.chart' symbol='NVDA'>" in rendered
    assert "chart payload" in rendered
    assert "compare the chart and the news" in rendered


def test_widget_image_rides_multimodal_channel():
    """Frontend emits widget image as a sibling MultimodalContext(type='image').
    Verify the existing multimodal pipeline picks it up unchanged."""
    raw = [
        {
            "type": "widget",
            "widget_type": "markets.chart",
            "widget_id": "w1",
            "label": "NVDA",
            "text": "<widget-context>chart</widget-context>",
        },
        {
            "type": "image",
            "data": "data:image/jpeg;base64,Zm9vYmFy",  # base64('foobar')
            "description": "NVDA · 1d Chart",
        },
    ]
    multimodal = parse_multimodal_contexts(raw)
    assert len(multimodal) == 1
    assert multimodal[0].type == "image"

    messages = _seed_messages()
    inject_multimodal_context(messages, multimodal)
    # After injection, last user message should contain an image content block.
    last_content = messages[-1]["content"]
    assert isinstance(last_content, list)
    has_image_block = any(
        (isinstance(b, dict) and b.get("type") in {"image", "image_url"})
        for b in last_content
    )
    assert has_image_block


def test_widget_directive_image_and_skill_coexist_without_conflict():
    """The most realistic case: widget directive + widget image + a directive
    + a skill all in the same additional_context list. Each parser only
    extracts its own type."""
    raw = [
        {"type": "skills", "name": "user-profile"},
        {"type": "directive", "content": "Be terse."},
        {
            "type": "widget",
            "widget_type": "markets.chart",
            "widget_id": "w1",
            "label": "NVDA",
            "text": "<widget-context>chart</widget-context>",
        },
        {
            "type": "image",
            "data": "data:image/jpeg;base64,Zm9vYmFy",
            "description": "NVDA chart",
        },
        {
            "type": "widget",
            "widget_type": "news.feed/row",
            "widget_id": "w1/n1",
            "label": "News headline",
            "text": "<widget-context>news</widget-context>",
        },
    ]

    widgets = parse_widget_contexts(raw)
    multimodal = parse_multimodal_contexts(raw)
    directives = parse_directive_contexts(raw)

    assert len(widgets) == 2
    assert len(multimodal) == 1
    assert len(directives) == 1

    # Build reminders
    widget_reminder = build_widget_context_reminder(widgets)
    directive_reminder = build_directive_reminder(directives)

    assert widget_reminder is not None
    assert directive_reminder is not None
    # Reminders are independent envelopes; both reach the user message.
    assert "chart" in widget_reminder
    assert "news" in widget_reminder
    assert "Be terse" in directive_reminder

    messages = _seed_messages()
    _append_to_last_user_message(messages, directive_reminder)
    _append_to_last_user_message(messages, widget_reminder)
    inject_multimodal_context(messages, multimodal)

    # Pull the rendered text from the last user message
    last = messages[-1]
    rendered = last["content"] if isinstance(last["content"], str) else "".join(
        b.get("text", "") for b in last["content"] if isinstance(b, dict)
    )

    assert "Be terse" in rendered
    assert "<widget-context>chart" in rendered
    assert "<widget-context>news" in rendered

    # And the image content block survived
    if isinstance(last["content"], list):
        has_image_block = any(
            (isinstance(b, dict) and b.get("type") in {"image", "image_url"})
            for b in last["content"]
        )
        assert has_image_block


def test_widget_metadata_serialization_keeps_text_and_data():
    raw = [
        {
            "type": "widget",
            "widget_type": "markets.chart",
            "widget_id": "w1",
            "label": "NVDA",
            "text": "<widget-context>huge prompt-only payload</widget-context>",
            "data": {"bars": [{"o": 1, "c": 2}]},
            "captured_at": "2026-04-26T11:42:08+00:00",
        }
    ]
    widgets = parse_widget_contexts(raw)
    persisted = serialize_widget_contexts_for_metadata(widgets)
    assert len(persisted) == 1
    assert persisted[0]["widget_type"] == "markets.chart"
    assert persisted[0]["data"] == {"bars": [{"o": 1, "c": 2}]}
    # Text is kept so the chip preview UI can show exactly what the agent saw.
    assert persisted[0]["text"] == "<widget-context>huge prompt-only payload</widget-context>"


def test_empty_additional_context_produces_no_widget_reminder():
    assert parse_widget_contexts(None) == []
    assert parse_widget_contexts([]) == []
    assert build_widget_context_reminder([]) is None
