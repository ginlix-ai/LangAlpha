"""Tests for LLM content extraction and formatting utilities.

Covers extract_content_with_type, get_message_content, format_llm_content,
extract_json_from_content, and helpers in src/llms/content_utils.py.
"""

from types import SimpleNamespace

import pytest

from src.llms.content_utils import (
    _extract_text_from_summary,
    _is_metadata_object,
    extract_content_with_type,
    extract_json_from_content,
    format_llm_content,
    get_message_content,
)


# ---------------------------------------------------------------------------
# _is_metadata_object
# ---------------------------------------------------------------------------


class TestIsMetadataObject:
    """Detect metadata-only dicts."""

    def test_metadata_only(self):
        assert _is_metadata_object({"id": "x", "index": 0}) is True

    def test_with_content(self):
        assert _is_metadata_object({"id": "x", "text": "hello"}) is False

    def test_non_dict(self):
        assert _is_metadata_object("hello") is False

    def test_empty_dict(self):
        assert _is_metadata_object({}) is True


# ---------------------------------------------------------------------------
# _extract_text_from_summary
# ---------------------------------------------------------------------------


class TestExtractTextFromSummary:
    """Extract text from OpenAI reasoning summary format."""

    def test_summary_text_type(self):
        summary = [{"type": "summary_text", "text": "thought A"}]
        assert _extract_text_from_summary(summary) == "thought A"

    def test_multiple_items(self):
        summary = [
            {"type": "summary_text", "text": "A"},
            {"type": "summary_text", "text": "B"},
        ]
        result = _extract_text_from_summary(summary)
        assert "A" in result
        assert "B" in result

    def test_non_list(self):
        assert _extract_text_from_summary("not a list") is None

    def test_empty_list(self):
        assert _extract_text_from_summary([]) is None


# ---------------------------------------------------------------------------
# extract_content_with_type
# ---------------------------------------------------------------------------


class TestExtractContentWithType:
    """Core content extraction from various LLM formats."""

    def test_plain_string(self):
        text, ctype = extract_content_with_type("Hello")
        assert text == "Hello"
        assert ctype == "text"

    def test_empty_string(self):
        text, ctype = extract_content_with_type("")
        assert text is None
        assert ctype is None

    def test_none(self):
        text, ctype = extract_content_with_type(None)
        assert text is None

    def test_thinking_dict(self):
        content = {"type": "thinking", "thinking": "analysis here"}
        text, ctype = extract_content_with_type(content)
        assert text == "analysis here"
        assert ctype == "reasoning"

    def test_reasoning_with_summary(self):
        content = {
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "thought"}],
        }
        text, ctype = extract_content_with_type(content)
        assert text == "thought"
        assert ctype == "reasoning"

    def test_text_dict(self):
        text, ctype = extract_content_with_type({"text": "response"})
        assert text == "response"
        assert ctype == "text"

    def test_metadata_only_dict(self):
        text, ctype = extract_content_with_type({"id": "abc", "index": 0})
        assert text is None
        assert ctype is None

    def test_list_with_mixed_content(self):
        content = [
            {"type": "thinking", "thinking": "step 1"},
            {"text": "result"},
        ]
        text, ctype = extract_content_with_type(content)
        assert "step 1" in text
        assert "result" in text
        assert ctype == "reasoning"

    def test_list_text_only(self):
        content = [{"text": "A"}, {"text": "B"}]
        text, ctype = extract_content_with_type(content)
        assert text == "AB"
        assert ctype == "text"

    def test_list_keeps_whitespace_only_thinking_delta(self):
        # DeepSeek streams the space before a number as its own thinking
        # token: " " then "9". Dropping it turns "Oct 9" into "Oct9".
        text, ctype = extract_content_with_type(
            [{"type": "thinking", "thinking": " ", "index": 0}]
        )
        assert text == " "
        assert ctype == "reasoning"

    def test_unknown_dict(self):
        text, ctype = extract_content_with_type({"result": "data"})
        assert text is None
        assert ctype is None


# ---------------------------------------------------------------------------
# get_message_content
# ---------------------------------------------------------------------------


class TestGetMessageContent:
    """Extract content from LangChain message objects."""

    def test_content_blocks_preferred(self):
        msg = SimpleNamespace(
            content_blocks=[{"type": "text", "text": "hello"}],
            content="fallback",
        )
        result = get_message_content(msg)
        assert result == [{"type": "text", "text": "hello"}]

    def test_fallback_to_content(self):
        msg = SimpleNamespace(content="plain text")
        result = get_message_content(msg)
        assert result == "plain text"

    def test_str_fallback(self):
        result = get_message_content(42)
        assert result == "42"


# ---------------------------------------------------------------------------
# format_llm_content
# ---------------------------------------------------------------------------


class TestFormatLlmContent:
    """Normalize LLM content into {reasoning, text} dict."""

    def test_string_content(self):
        result = format_llm_content("Hello world")
        assert result["text"] == "Hello world"
        assert result["reasoning"] is None

    def test_none_content(self):
        result = format_llm_content(None)
        assert result["text"] == ""
        assert result["reasoning"] is None

    def test_list_with_reasoning(self):
        content = [
            {"type": "reasoning", "reasoning": "thinking..."},
            {"type": "text", "text": "answer"},
        ]
        result = format_llm_content(content)
        assert result["text"] == "answer"
        assert "thinking..." in result["reasoning"]

    def test_list_with_thinking(self):
        content = [
            {"type": "thinking", "thinking": "deep thought"},
            {"type": "text", "text": "response"},
        ]
        result = format_llm_content(content)
        assert "deep thought" in result["reasoning"]
        assert result["text"] == "response"

    def test_additional_kwargs_reasoning(self):
        result = format_llm_content("text", additional_kwargs={"reasoning_content": "my reasoning"})
        assert result["text"] == "text"
        assert "my reasoning" in result["reasoning"]

    def test_additional_kwargs_fallback_field(self):
        result = format_llm_content("text", additional_kwargs={"reasoning": "fallback reasoning"})
        assert "fallback reasoning" in result["reasoning"]


# ---------------------------------------------------------------------------
# extract_json_from_content
# ---------------------------------------------------------------------------


class TestExtractJsonFromContent:
    """Extract JSON text, skipping reasoning blocks."""

    def test_string(self):
        assert extract_json_from_content('{"a": 1}') == '{"a": 1}'

    def test_none(self):
        assert extract_json_from_content(None) == ""

    def test_list_skips_reasoning(self):
        content = [
            {"type": "reasoning", "summary": [{"text": "thinking..."}]},
            {"type": "text", "text": '{"answer": 42}'},
        ]
        result = extract_json_from_content(content)
        assert result == '{"answer": 42}'

    def test_list_skips_thinking(self):
        content = [
            {"type": "thinking", "thinking": "deep thought"},
            {"type": "text", "text": '{"result": true}'},
        ]
        result = extract_json_from_content(content)
        assert result == '{"result": true}'

    def test_dict_text(self):
        result = extract_json_from_content({"text": '{"ok": true}'})
        assert result == '{"ok": true}'

    def test_dict_reasoning_only(self):
        result = extract_json_from_content({"type": "reasoning"})
        assert result == ""
