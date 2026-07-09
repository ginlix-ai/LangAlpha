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
        return create_show_widget_tool(MagicMock())

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


# ---------------------------------------------------------------------------
# _is_text_file
# ---------------------------------------------------------------------------

from src.ptc_agent.agent.tools.show_widget import _is_text_file


class TestIsTextFile:
    def test_json_is_text(self):
        assert _is_text_file("data.json") is True

    def test_csv_is_text(self):
        assert _is_text_file("report.csv") is True

    def test_png_is_not_text(self):
        assert _is_text_file("image.png") is False

    def test_jpg_is_not_text(self):
        assert _is_text_file("photo.jpg") is False

    def test_uppercase_extension_is_text(self):
        assert _is_text_file("DATA.JSON") is True

    def test_no_extension_is_not_text(self):
        assert _is_text_file("README") is False


# ---------------------------------------------------------------------------
# _resolve_data_files
# ---------------------------------------------------------------------------

from unittest.mock import AsyncMock

from src.ptc_agent.agent.tools.show_widget import _resolve_data_files


class TestResolveDataFiles:
    @pytest.mark.asyncio
    async def test_text_file_reads_via_aread_file_text(self):
        backend = AsyncMock()
        backend.aread_text.return_value = '{"key": "value"}'

        result = await _resolve_data_files(backend, ["/work/data.json"])

        backend.aread_text.assert_awaited_once_with("/work/data.json")
        assert result == {"data.json": '{"key": "value"}'}

    @pytest.mark.asyncio
    async def test_binary_file_reads_via_adownload_file_bytes(self):
        backend = AsyncMock()
        raw_bytes = b"\x89PNG\r\n\x1a\n"
        backend.adownload_file_bytes.return_value = raw_bytes

        result = await _resolve_data_files(backend, ["/work/chart.png"])

        backend.adownload_file_bytes.assert_awaited_once_with("/work/chart.png")
        assert "chart.png" in result
        value = result["chart.png"]
        assert value.startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_empty_filename_is_skipped(self):
        backend = AsyncMock()

        result = await _resolve_data_files(backend, ["/"])

        assert result == {}
        backend.aread_text.assert_not_awaited()
        backend.adownload_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_file_not_found_returns_none_skipped(self):
        backend = AsyncMock()
        backend.aread_text.return_value = None

        result = await _resolve_data_files(backend, ["/work/missing.json"])

        assert result == {}

    @pytest.mark.asyncio
    async def test_read_exception_is_skipped_and_logs_warning(self):
        backend = AsyncMock()
        backend.aread_text.side_effect = OSError("disk error")

        with patch.object(_mod.logger, "warning") as mock_warn:
            result = await _resolve_data_files(backend, ["/work/bad.csv"])

        assert result == {}
        mock_warn.assert_called_once()
        assert "failed to read" in mock_warn.call_args[0][0].lower()

    @pytest.mark.asyncio
    async def test_size_cap_exceeded_skips_file(self):
        backend = AsyncMock()
        backend.aread_text.return_value = "x" * 100

        with patch.object(_mod, "_INLINE_DATA_CAP", 50):
            result = await _resolve_data_files(backend, ["/work/big.json"])

        assert result == {}

    @pytest.mark.asyncio
    async def test_multiple_files_returns_all(self):
        backend = AsyncMock()
        backend.aread_text.return_value = "text content"
        raw_bytes = b"\xff\xd8\xff\xe0"
        backend.adownload_file_bytes.return_value = raw_bytes

        result = await _resolve_data_files(
            backend, ["/work/notes.txt", "/work/thumb.jpg"]
        )

        assert "notes.txt" in result
        assert "thumb.jpg" in result
        assert result["notes.txt"] == "text content"
        assert result["thumb.jpg"].startswith("data:")


# ---------------------------------------------------------------------------
# ShowWidget tool with data_files
# ---------------------------------------------------------------------------


class TestShowWidgetWithDataFiles:
    @pytest.mark.asyncio
    async def test_data_files_with_sandbox_resolves_data_in_stream(self):
        mock_backend = AsyncMock()
        mock_backend.aread_text.return_value = '["a","b"]'

        tool = create_show_widget_tool(backend=mock_backend)
        mock_writer = MagicMock()

        with patch("langgraph.config.get_stream_writer", return_value=mock_writer):
            result = await tool.ainvoke(
                _tool_call({
                    "html": "<div>chart</div>",
                    "title": "Chart",
                    "data_files": ["/work/data.json"],
                })
            )

        assert result.artifact["type"] == "html_widget"
        mock_writer.assert_called_once()
        stream_payload = mock_writer.call_args[0][0]["payload"]
        assert "data" in stream_payload
        assert "data.json" in stream_payload["data"]

    @pytest.mark.asyncio
    async def test_data_files_without_sandbox_no_resolution(self):
        tool = create_show_widget_tool(backend=None)
        mock_writer = MagicMock()

        with patch("langgraph.config.get_stream_writer", return_value=mock_writer):
            result = await tool.ainvoke(
                _tool_call({
                    "html": "<div>chart</div>",
                    "title": "Chart",
                    "data_files": ["/work/data.json"],
                })
            )

        assert result.artifact["type"] == "html_widget"
        mock_writer.assert_called_once()
        stream_payload = mock_writer.call_args[0][0]["payload"]
        assert "data" not in stream_payload


# ---------------------------------------------------------------------------
# _attach_widget_data / widget_data_storage_key
# ---------------------------------------------------------------------------

import hashlib
import json

from src.ptc_agent.agent.tools.show_widget import (
    _attach_widget_data,
    widget_data_storage_key,
)


class TestAttachWidgetData:
    @pytest.mark.asyncio
    async def test_small_data_inlines_into_artifact(self, monkeypatch):
        upload = MagicMock()
        monkeypatch.setattr(_mod, "is_storage_enabled", lambda: True)
        monkeypatch.setattr(_mod, "upload_bytes", upload)
        artifact = {"type": "html_widget"}

        await _attach_widget_data(artifact, {"data.json": "[1,2]"})

        assert artifact["data"] == {"data.json": "[1,2]"}
        assert "data_ref" not in artifact
        upload.assert_not_called()

    @pytest.mark.asyncio
    async def test_large_data_uploads_content_addressed_ref(self, monkeypatch):
        uploads = []
        monkeypatch.setattr(_mod, "is_storage_enabled", lambda: True)
        monkeypatch.setattr(
            _mod, "upload_bytes", lambda *a: uploads.append(a) or True
        )
        monkeypatch.setattr(_mod, "_INLINE_ARTIFACT_CAP", 10)
        artifact = {"type": "html_widget"}
        data = {"data.json": "x" * 100}

        await _attach_widget_data(artifact, data)

        payload = json.dumps(data, ensure_ascii=False, sort_keys=True).encode()
        sha = hashlib.sha256(payload).hexdigest()
        assert artifact["data_ref"] == {
            "key": f"widgets/{sha}.json",
            "sha256": sha,
            "size": len(payload),
        }
        assert "data" not in artifact
        assert uploads == [(f"widgets/{sha}.json", payload, "application/json")]

    @pytest.mark.asyncio
    async def test_large_data_inlines_when_storage_disabled(self, monkeypatch):
        monkeypatch.setattr(_mod, "is_storage_enabled", lambda: False)
        monkeypatch.setattr(_mod, "_INLINE_ARTIFACT_CAP", 10)
        artifact = {"type": "html_widget"}
        data = {"data.json": "x" * 100}

        await _attach_widget_data(artifact, data)

        assert artifact["data"] == data
        assert "data_ref" not in artifact

    @pytest.mark.asyncio
    async def test_upload_failure_attaches_neither(self, monkeypatch):
        monkeypatch.setattr(_mod, "is_storage_enabled", lambda: True)
        monkeypatch.setattr(_mod, "upload_bytes", lambda *a: False)
        monkeypatch.setattr(_mod, "_INLINE_ARTIFACT_CAP", 10)
        artifact = {"type": "html_widget"}

        await _attach_widget_data(artifact, {"data.json": "x" * 100})

        assert "data" not in artifact
        assert "data_ref" not in artifact

    def test_widget_data_storage_key_thread_scoping(self):
        sha = "ab" * 32
        assert widget_data_storage_key("t1", sha) == f"widgets/t1/{sha}.json"
        assert widget_data_storage_key("", sha) == f"widgets/{sha}.json"

    @pytest.mark.asyncio
    async def test_tool_checkpointed_artifact_carries_data(self, monkeypatch):
        # End-to-end through the tool: small resolved data must land in the
        # ToolMessage artifact (checkpointed truth), not only the stream event.
        mock_backend = AsyncMock()
        mock_backend.aread_text.return_value = '["a","b"]'
        monkeypatch.setattr(_mod, "is_storage_enabled", lambda: False)
        tool = create_show_widget_tool(backend=mock_backend)

        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(
                _tool_call({
                    "html": "<div>chart</div>",
                    "data_files": ["/work/data.json"],
                })
            )

        assert result.artifact["data"] == {"data.json": '["a","b"]'}
