"""Unit tests for the ShowWidget tool, its data-file delivery and render report."""

from __future__ import annotations

import base64
import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import ptc_agent.agent.tools.show_widget as _mod
from ptc_agent.agent.tools.show_widget import (
    DataFilesRefused,
    _attach_widget_data,
    _describe,
    _is_text_file,
    _resolve_data_files,
    create_show_widget_tool,
    widget_data_storage_key,
)
from src.server.services.widget_render import Inspection, WidgetRenderReport


def _tool_call(args: dict, call_id: str = "call_test_123") -> dict:
    """Build a ToolCall-shaped dict so ainvoke returns a ToolMessage."""
    return {
        "name": "ShowWidget",
        "args": args,
        "id": call_id,
        "type": "tool_call",
    }


def _report(
    *,
    inspected: bool = True,
    images_total: int = 0,
    images_broken: int = 0,
    narrow_width: int | None = 360,
    narrow_scroll_width: int | None = 360,
    **fields,
) -> WidgetRenderReport:
    """A clean render report; tests override only the field they exercise."""
    answer = {
        key: fields.pop(key)
        for key in (
            "height", "width", "scroll_width", "text_chars", "elements",
            "canvases", "svgs", "fixed_elements", "data_keys", "visible_text",
        )
        if key in fields
    }
    inspection = Inspection(**{
        "height": 300, "width": 720, "scroll_width": 720, "text_chars": 40,
        "elements": 5, "visible_text": "hi",
        "images": {"total": images_total, "broken": images_broken},
        **answer,
    })
    narrow = (
        Inspection(width=narrow_width, scroll_width=narrow_scroll_width, height=300)
        if narrow_width is not None and narrow_scroll_width is not None
        else None
    )
    return WidgetRenderReport(
        inspection=inspection if inspected else None,
        narrow=narrow if inspected else None,
        **{"settled": True, **fields},
    )


async def _outcome(backend, data_files) -> tuple[dict[str, str], list[str]]:
    """What ``_resolve_data_files`` delivers, and what it refused instead."""
    try:
        return await _resolve_data_files(backend, data_files), []
    except DataFilesRefused as refused:
        return {}, refused.problems


@pytest.fixture(autouse=True)
def _no_render(monkeypatch):
    """Default to no headless browser; tests that need a report patch it."""
    monkeypatch.setattr(_mod, "_render", AsyncMock(return_value=None))


# ---------------------------------------------------------------------------
# ShowWidget tool
# ---------------------------------------------------------------------------


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
        mock_writer.assert_called_once()
        assert mock_writer.call_args[0][0]["artifact_type"] == "html_widget"

    @pytest.mark.asyncio
    async def test_tool_call_id_is_the_stream_artifact_id(self, tool):
        mock_writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=mock_writer):
            await tool.ainvoke(_tool_call({"html": "<p>x</p>"}, call_id="call_1"))

        assert mock_writer.call_args[0][0]["artifact_id"] == "call_1"

    @pytest.mark.asyncio
    async def test_scripts_are_not_judged_by_the_tool(self, tool):
        # Scripts that the old validator refused are shown; the render report
        # is what tells the agent whether they work.
        html = "<script>new ResizeObserver(cb); window.top.x = 1;</script>"
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(_tool_call({"html": html}))

        assert result.artifact["html"] == html

    @pytest.mark.asyncio
    @pytest.mark.parametrize("html", ["", "   \n\t "])
    async def test_empty_html_is_refused_without_streaming(self, tool, html):
        mock_writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=mock_writer):
            result = await tool.ainvoke(_tool_call({"html": html}))

        assert result.content.startswith("Widget not shown")
        assert "empty" in result.content
        assert result.artifact == {}
        mock_writer.assert_not_called()
        _mod._render.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("platform", "shown"),
        [
            ("slack", False),
            ("imessage", False),
            ("web", True),
            ("market_view:AAPL", True),
            (None, True),
        ],
    )
    async def test_chat_app_turn_is_refused(self, tool, monkeypatch, platform, shown):
        # A chat app has nowhere to draw a widget, so a call there would tell
        # the agent "Shown to the user" when nobody sees it.
        monkeypatch.setattr(_mod, "configurable", lambda: {"platform": platform})
        mock_writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=mock_writer):
            result = await tool.ainvoke(_tool_call({"html": "<p>x</p>"}))

        if shown:
            assert result.content.startswith("Shown to the user")
            mock_writer.assert_called_once()
        else:
            assert result.content.startswith("Widget not shown: this turn came from ")
            assert "send_message" in result.content
            assert result.artifact == {}
            mock_writer.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_title_uses_widget_in_text_and_empty_title(self, tool):
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(_tool_call({"html": "<p>data</p>"}))

        assert result.artifact["title"] == ""
        assert "Shown to the user: widget." in result.content

    @pytest.mark.asyncio
    async def test_render_none_says_it_could_not_be_checked(self, tool):
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(
                _tool_call({"html": "<p>x</p>", "title": "Revenue"})
            )

        assert result.content == (
            "Shown to the user: Revenue. Its rendering could not be checked, so there is no report or screenshot."
        )
        assert result.artifact["title"] == "Revenue"

    @pytest.mark.asyncio
    async def test_render_receives_html_and_resolved_data(self, monkeypatch):
        backend = AsyncMock()
        backend.aread_text.return_value = "[1]"
        render = AsyncMock(return_value=None)
        monkeypatch.setattr(_mod, "_render", render)
        monkeypatch.setattr(_mod, "is_storage_enabled", lambda: False)
        tool = create_show_widget_tool(backend)

        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            await tool.ainvoke(
                _tool_call({"html": "<p>x</p>", "data_files": ["/w/a.json"]})
            )

        render.assert_awaited_once_with("<p>x</p>", {"a.json": "[1]"})

    @pytest.mark.asyncio
    async def test_report_without_screenshot_returns_text(self, tool, monkeypatch):
        monkeypatch.setattr(_mod, "_render", AsyncMock(return_value=_report()))
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(_tool_call({"html": "<p>x</p>", "title": "T"}))

        assert isinstance(result.content, str)
        assert result.content.startswith("Shown to the user: T.\nRendered 720 x 300 px")

    @pytest.mark.asyncio
    async def test_report_with_screenshot_returns_text_and_image_blocks(
        self, tool, monkeypatch
    ):
        png = b"\x89PNG\r\n\x1a\nfake"
        monkeypatch.setattr(
            _mod, "_render", AsyncMock(return_value=_report(screenshot=png))
        )
        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(_tool_call({"html": "<p>x</p>", "title": "T"}))

        blocks = result.content
        assert isinstance(blocks, list) and len(blocks) == 2
        assert blocks[0]["type"] == "text"
        assert "Below is how it looks in the light theme." in blocks[0]["text"]
        assert blocks[1] == {
            "type": "image_url",
            "image_url": {"url": "data:image/png;base64," + base64.b64encode(png).decode()},
        }
        assert result.artifact["type"] == "html_widget"


# ---------------------------------------------------------------------------
# _describe
# ---------------------------------------------------------------------------


class TestDescribe:
    def test_clean_report_is_two_summary_lines(self):
        text = _describe("Chart", _report(canvases=1, data_keys=["a.json"]))

        assert text.splitlines() == [
            "Shown to the user: Chart.",
            "Rendered 720 x 300 px with 1 canvas.",
            "__WIDGET_DATA__ keys: a.json.",
        ]

    def test_phone_width_unmeasured_says_nothing(self):
        text = _describe("W", _report(narrow_width=None, narrow_scroll_width=None))
        assert "phone" not in text

    def test_problems_follow_the_summary_lines(self):
        report = _report(
            errors=["TypeError: x is undefined"],
            load_failed=["img https://x/y.png"],
            text_chars=0,
            fixed_elements=2,
            scroll_width=900,
            narrow_scroll_width=700,
            settled=False,
        )
        lines = _describe("W", report).splitlines()

        assert lines[0] == "Shown to the user: W."
        assert lines[1].startswith("Rendered ")
        rest = "\n".join(lines[2:])
        assert "Errors:" in rest and "TypeError: x is undefined" in rest
        assert "Failed to load:" in rest and "https://x/y.png" in rest
        assert "looks blank" in rest
        assert "2 elements use position: fixed" in rest
        assert "900 px wide in a 720 px frame" in rest
        assert "On a phone (360 px) its content is 700 px wide" in rest
        assert "still changing" in rest
        # Nothing about problems precedes the two summary lines.
        assert "Errors:" not in "\n".join(lines[:2])

    def test_screenshot_sentence_only_with_a_screenshot(self):
        assert "light theme" not in _describe("W", _report())
        assert "Below is how it looks in the light theme." in _describe(
            "W", _report(screenshot=b"png")
        )

    def test_broken_images_are_counted(self):
        text = _describe("W", _report(images_total=4, images_broken=3))
        assert "3 of 4 images did not load." in text

    def test_a_frame_that_never_answered_reads_as_hung(self):
        text = _describe("W", _report(inspected=False, settled=False, reported_height=240))
        lines = text.splitlines()
        assert lines[1] == "Rendered ? x 240 px."
        assert "It looks blank: no text, image, canvas or SVG rendered." in lines
        assert lines[-1] == (
            "It never finished loading: a script may be stuck in a loop or "
            "blocking the page."
        )

    def test_long_lists_are_truncated(self):
        text = _describe("W", _report(errors=[f"e{n}" for n in range(12)]))
        assert "and 4 more" in text


# ---------------------------------------------------------------------------
# _is_text_file
# ---------------------------------------------------------------------------

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


class TestResolveDataFiles:
    @pytest.mark.asyncio
    async def test_text_file_reads_via_aread_text(self):
        backend = AsyncMock()
        backend.aread_text.return_value = '{"key": "value"}'

        data, refused = await _outcome(backend, ["/work/data.json"])

        backend.aread_text.assert_awaited_once_with("/work/data.json")
        assert refused == []
        assert data == {"data.json": '{"key": "value"}'}

    @pytest.mark.asyncio
    async def test_binary_file_becomes_a_data_url(self):
        backend = AsyncMock()
        backend.adownload_file_bytes.return_value = b"\x89PNG\r\n\x1a\n"

        data, refused = await _outcome(backend, ["/work/chart.png"])

        backend.adownload_file_bytes.assert_awaited_once_with("/work/chart.png")
        assert refused == []
        assert data["chart.png"].startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_empty_filename_is_skipped(self):
        backend = AsyncMock()

        data, refused = await _outcome(backend, ["/"])

        assert (data, refused) == ({}, [])
        backend.aread_text.assert_not_awaited()
        backend.adownload_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_file_is_refused_naming_the_path(self):
        backend = AsyncMock()
        backend.aread_text.return_value = None

        data, refused = await _outcome(backend, ["/work/missing.json"])

        assert data == {}
        assert len(refused) == 1
        assert "/work/missing.json" in refused[0]
        assert "not found" in refused[0]

    @pytest.mark.asyncio
    async def test_unreadable_file_is_refused_and_logs_warning(self):
        backend = AsyncMock()
        backend.aread_text.side_effect = OSError("disk error")

        with patch.object(_mod.logger, "warning") as mock_warn:
            data, refused = await _outcome(backend, ["/work/bad.csv"])

        assert data == {}
        assert "/work/bad.csv" in refused[0]
        mock_warn.assert_called_once()
        assert "failed to read" in mock_warn.call_args[0][0].lower()

    @pytest.mark.asyncio
    async def test_one_missing_file_withholds_the_files_that_were_fine(self):
        backend = AsyncMock()
        backend.aread_text.side_effect = lambda path: (
            "ok" if path.endswith("a.txt") else None
        )

        data, refused = await _outcome(
            backend, ["/work/a.txt", "/work/b.txt"]
        )

        assert data == {}
        assert len(refused) == 1 and "/work/b.txt" in refused[0]

    @pytest.mark.asyncio
    async def test_duplicate_basename_is_refused_naming_both_paths(self):
        backend = AsyncMock()
        backend.aread_text.return_value = "x"

        data, refused = await _outcome(
            backend, ["/work/q1/data.csv", "/work/q2/data.csv"]
        )

        assert data == {}
        assert len(refused) == 1
        assert "/work/q2/data.csv" in refused[0]
        assert "/work/q1/data.csv" in refused[0]
        assert "file name" in refused[0]

    @pytest.mark.asyncio
    async def test_over_cap_refusal_lists_every_file_with_its_size(self):
        backend = AsyncMock()
        contents = {"/work/a.txt": "x" * 3000, "/work/b.txt": "y" * 40}
        backend.aread_text.side_effect = lambda path: contents[path]

        with patch.object(_mod, "_INLINE_DATA_CAP", 2048):
            data, refused = await _outcome(backend, list(contents))

        assert data == {}
        assert len(refused) == 1
        line = refused[0]
        assert "/work/a.txt 3 KB" in line
        assert "/work/b.txt 40 bytes" in line
        assert "2 KB in all" in line
        assert "relative URL" in line

    @pytest.mark.asyncio
    async def test_binary_counts_at_base64_size_against_the_cap(self):
        backend = AsyncMock()
        backend.adownload_file_bytes.return_value = b"\x00" * 1000

        # 1000 raw bytes fit a 1100-byte cap; their data URL (~1370) does not.
        with patch.object(_mod, "_INLINE_DATA_CAP", 1100):
            data, refused = await _outcome(backend, ["/work/p.png"])

        assert data == {}
        assert len(refused) == 1 and "/work/p.png" in refused[0]

    @pytest.mark.asyncio
    async def test_multiple_files_returns_all(self):
        backend = AsyncMock()
        backend.aread_text.return_value = "text content"
        backend.adownload_file_bytes.return_value = b"\xff\xd8\xff\xe0"

        data, refused = await _outcome(
            backend, ["/work/notes.txt", "/work/thumb.jpg"]
        )

        assert refused == []
        assert data["notes.txt"] == "text content"
        assert data["thumb.jpg"].startswith("data:")


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
        assert "data" not in mock_writer.call_args[0][0]["payload"]

    @pytest.mark.asyncio
    async def test_missing_file_refuses_the_call_and_names_the_path(self):
        backend = AsyncMock()
        backend.aread_text.return_value = None
        tool = create_show_widget_tool(backend=backend)
        mock_writer = MagicMock()

        with patch("langgraph.config.get_stream_writer", return_value=mock_writer):
            result = await tool.ainvoke(
                _tool_call({"html": "<p>x</p>", "data_files": ["/work/gone.json"]})
            )

        assert result.content.startswith("Widget not shown")
        assert "/work/gone.json" in result.content
        assert result.artifact == {}
        mock_writer.assert_not_called()
        _mod._render.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_over_cap_returns_empty_artifact_and_never_streams(self):
        backend = AsyncMock()
        contents = {"/work/a.txt": "x" * 3000, "/work/b.txt": "y" * 3000}
        backend.aread_text.side_effect = lambda path: contents[path]
        tool = create_show_widget_tool(backend=backend)
        mock_writer = MagicMock()

        with (
            patch.object(_mod, "_INLINE_DATA_CAP", 2048),
            patch("langgraph.config.get_stream_writer", return_value=mock_writer),
        ):
            result = await tool.ainvoke(
                _tool_call({"html": "<p>x</p>", "data_files": list(contents)})
            )

        assert result.content.startswith("Widget not shown")
        assert "/work/a.txt" in result.content and "/work/b.txt" in result.content
        assert "3 KB" in result.content
        assert result.artifact == {}
        mock_writer.assert_not_called()

    @pytest.mark.asyncio
    async def test_duplicate_basename_refuses_the_call(self):
        backend = AsyncMock()
        backend.aread_text.return_value = "x"
        tool = create_show_widget_tool(backend=backend)

        with patch("langgraph.config.get_stream_writer", return_value=MagicMock()):
            result = await tool.ainvoke(
                _tool_call({
                    "html": "<p>x</p>",
                    "data_files": ["/w/a/d.csv", "/w/b/d.csv"],
                })
            )

        assert result.content.startswith("Widget not shown")
        assert "/w/b/d.csv" in result.content
        assert result.artifact == {}


# ---------------------------------------------------------------------------
# _attach_widget_data / widget_data_storage_key
# ---------------------------------------------------------------------------

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
    async def test_upload_failure_inlines_the_data(self, monkeypatch):
        # The artifact is the only copy replay reads: a pointer to an object
        # that was never written, or no data at all, reloads the widget empty.
        monkeypatch.setattr(_mod, "is_storage_enabled", lambda: True)
        monkeypatch.setattr(_mod, "upload_bytes", lambda *a: False)
        monkeypatch.setattr(_mod, "_INLINE_ARTIFACT_CAP", 10)
        artifact = {"type": "html_widget"}
        data = {"data.json": "x" * 100}

        await _attach_widget_data(artifact, data)

        assert artifact["data"] == data
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
