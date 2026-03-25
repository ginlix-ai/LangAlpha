"""Unit tests for the ShowWidget tool and its validation helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import src.ptc_agent.agent.tools.show_widget as _mod
from src.ptc_agent.agent.tools.show_widget import (
    _detect_outer_wrapper_issues,
    _load_widget_guidelines,
    _validate_html,
    create_show_widget_tool,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_cache():
    """Reset the module-level skill content cache before each test."""
    _mod._SKILL_CONTENT_CACHE = None
    yield
    _mod._SKILL_CONTENT_CACHE = None


# ---------------------------------------------------------------------------
# _validate_html
# ---------------------------------------------------------------------------


class TestValidateHtml:
    def test_clean_html_returns_no_violations(self):
        html = '<div style="color: red;">Hello</div>'
        assert _validate_html(html) == []

    def test_resize_observer_detected(self):
        html = "<script>const ro = new ResizeObserver(cb);</script>"
        violations = _validate_html(html)
        assert len(violations) == 1
        assert "[ResizeObserver]" in violations[0]

    def test_position_fixed_detected(self):
        html = '<div style="position: fixed; top: 0;">bar</div>'
        violations = _validate_html(html)
        assert any("[position:fixed]" in v for v in violations)

    def test_parent_post_message_detected(self):
        html = "<script>parent.postMessage('hi', '*');</script>"
        violations = _validate_html(html)
        assert any("[parent.postMessage]" in v for v in violations)

    def test_frame_escape_window_top(self):
        html = "<script>window.top.location = '/';</script>"
        violations = _validate_html(html)
        assert any("[frame escape]" in v for v in violations)

    def test_frame_escape_window_parent(self):
        html = "<script>window.parent.foo();</script>"
        violations = _validate_html(html)
        assert any("[frame escape]" in v for v in violations)

    def test_multiple_violations_detected(self):
        html = (
            '<div style="position: fixed;">'
            "<script>new ResizeObserver(cb); parent.postMessage('x','*');</script>"
            "</div>"
        )
        violations = _validate_html(html)
        names = {v.split("]")[0].strip("[") for v in violations}
        assert "ResizeObserver" in names
        assert "position:fixed" in names
        assert "parent.postMessage" in names


# ---------------------------------------------------------------------------
# _detect_outer_wrapper_issues
# ---------------------------------------------------------------------------


class TestDetectOuterWrapperIssues:
    def test_no_style_attribute_no_issues(self):
        html = "<div>No style here</div>"
        assert _detect_outer_wrapper_issues(html) == []

    def test_first_element_with_prefix_content_no_issues(self):
        # When there is text before the styled element, it is not outermost
        html = 'some prefix text <div style="background: red;">inner</div>'
        assert _detect_outer_wrapper_issues(html) == []

    def test_background_non_transparent_flagged(self):
        html = '<div style="background: #fff;">content</div>'
        issues = _detect_outer_wrapper_issues(html)
        assert len(issues) == 1
        assert "background" in issues[0].lower()

    def test_background_transparent_not_flagged(self):
        html = '<div style="background: transparent;">content</div>'
        assert _detect_outer_wrapper_issues(html) == []

    def test_border_non_none_flagged(self):
        html = '<div style="border: 1px solid red;">content</div>'
        issues = _detect_outer_wrapper_issues(html)
        assert len(issues) == 1
        assert "border" in issues[0].lower()

    def test_border_none_not_flagged(self):
        html = '<div style="border: none;">content</div>'
        assert _detect_outer_wrapper_issues(html) == []

    def test_border_radius_flagged(self):
        html = '<div style="border-radius: 8px;">content</div>'
        issues = _detect_outer_wrapper_issues(html)
        assert len(issues) == 1
        assert "border-radius" in issues[0].lower()

    def test_inner_element_with_background_not_flagged(self):
        # Outer div has no style; inner div has background — should not flag
        html = '<div><div style="background: blue;">inner</div></div>'
        # The styled element is not outermost because prefix "<div>" is present
        assert _detect_outer_wrapper_issues(html) == []


# ---------------------------------------------------------------------------
# _load_widget_guidelines
# ---------------------------------------------------------------------------

_SKILLS_MOD = "ptc_agent.agent.middleware.skills"


class TestLoadWidgetGuidelines:
    @patch(f"{_SKILLS_MOD}.load_skill_content", return_value="# Widget Guidelines\nDetailed rules here.")
    def test_successful_load_caches_content(self, mock_load):
        result1 = _load_widget_guidelines()
        result2 = _load_widget_guidelines()

        assert result1 == "# Widget Guidelines\nDetailed rules here."
        assert result2 == result1
        # Called only once because the second call uses the cache
        mock_load.assert_called_once_with("inline-widget")

    @patch(f"{_SKILLS_MOD}.load_skill_content", side_effect=RuntimeError("file not found"))
    def test_failed_load_returns_fallback_without_caching(self, mock_load):
        result = _load_widget_guidelines()
        assert "Outermost element" in result  # fallback text
        # Not cached — next call should retry
        assert _mod._SKILL_CONTENT_CACHE is None

        # Second call retries the load
        _load_widget_guidelines()
        assert mock_load.call_count == 2

    @patch(f"{_SKILLS_MOD}.load_skill_content", return_value="")
    def test_empty_string_returns_fallback(self, mock_load):
        result = _load_widget_guidelines()
        assert "Outermost element" in result
        assert _mod._SKILL_CONTENT_CACHE is None


# ---------------------------------------------------------------------------
# ShowWidget tool (async)
# ---------------------------------------------------------------------------


def _tool_call(args: dict, call_id: str = "call_test_123") -> dict:
    """Build a ToolCall-shaped dict so ainvoke returns a ToolMessage."""
    return {
        "name": "ShowWidget",
        "args": args,
        "id": call_id,
        "type": "tool_call",
    }


class TestShowWidgetTool:
    @pytest.fixture()
    def tool(self):
        return create_show_widget_tool()

    @pytest.mark.asyncio
    async def test_valid_html_returns_content_and_artifact(self, tool):
        mock_writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=mock_writer):
            result = await tool.ainvoke(
                _tool_call({"html": "<div>Hello</div>", "title": "My Widget"})
            )

        assert "My Widget" in result.content
        assert result.artifact["type"] == "html_widget"
        assert result.artifact["html"] == "<div>Hello</div>"
        assert result.artifact["title"] == "My Widget"
        # Stream writer should have been called with the artifact payload
        mock_writer.assert_called_once()
        call_payload = mock_writer.call_args[0][0]
        assert call_payload["artifact_type"] == "html_widget"

    @pytest.mark.asyncio
    async def test_invalid_html_returns_error_and_empty_dict(self, tool):
        bad_html = "<script>new ResizeObserver(cb);</script>"
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(_tool_call({"html": bad_html}))

        assert "rejected" in result.content.lower()
        assert "[ResizeObserver]" in result.content
        assert result.artifact == {}

    @pytest.mark.asyncio
    async def test_title_reflected_in_artifact(self, tool):
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(
                _tool_call({"html": "<p>chart</p>", "title": "Revenue Chart"})
            )

        assert result.artifact["title"] == "Revenue Chart"
        assert "Revenue Chart" in result.content

    @pytest.mark.asyncio
    async def test_no_title_uses_empty_string(self, tool):
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(_tool_call({"html": "<p>data</p>"}))

        assert result.artifact["title"] == ""
        # Content should contain the widget_id fallback since no title
        assert "widget_" in result.content
