"""Tests for multimodal context utilities.

Covers parse_multimodal_contexts, build_attachment_metadata,
inject_multimodal_context, and the sanitize_storage_key helper.
"""

from unittest.mock import patch

import pytest

from src.server.models.additional_context import MultimodalContext
from src.server.utils.multimodal_context import (
    build_attachment_metadata,
    inject_multimodal_context,
    parse_multimodal_contexts,
    pdf_page_counts,
)
from src.utils.storage import sanitize_storage_key


# ---------------------------------------------------------------------------
# sanitize_storage_key
# ---------------------------------------------------------------------------


class TestSanitizeStorageKey:
    def test_multiline_takes_first_line(self):
        result = sanitize_storage_key("Chart: GOOGL\nChart mode: Light\nInterval: Daily")
        assert "\n" not in result
        assert result.startswith("Chart: GOOGL")

    def test_crlf_line_endings(self):
        result = sanitize_storage_key("Title\r\nSecond line\r\nThird")
        assert "\r" not in result
        assert "\n" not in result
        assert result == "Title"

    def test_only_newlines_falls_back(self):
        assert sanitize_storage_key("\n\n\n") == "file"

    def test_empty_string_falls_back(self):
        assert sanitize_storage_key("") == "file"

    def test_none_falls_back(self):
        assert sanitize_storage_key(None) == "file"

    def test_forward_slash_replaced(self):
        result = sanitize_storage_key("path/to/file")
        assert "/" not in result
        assert result == "path_to_file"

    def test_truncates_long_name(self):
        long_name = "A" * 200
        result = sanitize_storage_key(long_name)
        # 120 chars max before extension
        assert len(result) <= 120

    def test_png_extension_from_data_url(self):
        result = sanitize_storage_key("chart", "data:image/png;base64,abc")
        assert result == "chart.png"

    def test_jpeg_extension_from_data_url(self):
        result = sanitize_storage_key("photo", "data:image/jpeg;base64,abc")
        assert result == "photo.jpeg"

    def test_webp_extension_from_data_url(self):
        result = sanitize_storage_key("img", "data:image/webp;base64,abc")
        assert result == "img.webp"

    def test_pdf_extension_from_data_url(self):
        result = sanitize_storage_key("doc", "data:application/pdf;base64,abc")
        assert result == "doc.pdf"

    def test_no_data_url_no_extension(self):
        result = sanitize_storage_key("chart")
        assert result == "chart"

    def test_no_duplicate_extension(self):
        result = sanitize_storage_key("chart.png", "data:image/png;base64,abc")
        assert result == "chart.png"
        assert not result.endswith(".png.png")

    def test_svg_xml_mime_falls_back_to_png(self):
        result = sanitize_storage_key("chart", "data:image/svg+xml;base64,abc")
        assert result == "chart.png"

    def test_truncation_leaves_room_for_extension(self):
        long_name = "A" * 200
        result = sanitize_storage_key(long_name, "data:image/png;base64,abc")
        assert result.endswith(".png")
        assert len(result) <= 124  # 120 + len(".png")


# ---------------------------------------------------------------------------
# parse_multimodal_contexts
# ---------------------------------------------------------------------------


class TestParseMultimodalContexts:
    def test_none_returns_empty(self):
        assert parse_multimodal_contexts(None) == []

    def test_empty_list_returns_empty(self):
        assert parse_multimodal_contexts([]) == []

    def test_dict_with_image_type(self):
        result = parse_multimodal_contexts([
            {"type": "image", "data": "data:image/png;base64,abc", "description": "test"},
        ])
        assert len(result) == 1
        assert isinstance(result[0], MultimodalContext)
        assert result[0].data == "data:image/png;base64,abc"
        assert result[0].description == "test"

    def test_dict_without_image_type_skipped(self):
        result = parse_multimodal_contexts([
            {"type": "text", "data": "hello"},
        ])
        assert result == []

    def test_multimodal_context_passes_through(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc")
        result = parse_multimodal_contexts([ctx])
        assert result == [ctx]

    def test_object_with_type_attr(self):
        class FakeCtx:
            type = "image"
            data = "data:image/png;base64,abc"
            description = "fake"

        result = parse_multimodal_contexts([FakeCtx()])
        assert len(result) == 1
        assert result[0].description == "fake"

    def test_unrecognized_ctx_skipped(self):
        result = parse_multimodal_contexts([42, "string", None])
        assert result == []

    def test_mixed_inputs(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc")
        result = parse_multimodal_contexts([
            {"type": "image", "data": "data:image/jpeg;base64,xyz"},
            ctx,
            {"type": "text", "data": "skip me"},
            42,
        ])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# build_attachment_metadata
# ---------------------------------------------------------------------------


class TestBuildAttachmentMetadata:
    @pytest.fixture()
    def _disable_storage(self):
        with patch("src.server.utils.multimodal_context.is_storage_enabled", return_value=False):
            yield

    @pytest.fixture()
    def _enable_storage(self):
        with patch("src.server.utils.multimodal_context.is_storage_enabled", return_value=True):
            yield

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_disable_storage")
    async def test_storage_disabled_no_upload(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="chart")
        result = await build_attachment_metadata([ctx])
        assert len(result) == 1
        assert result[0]["name"] == "chart"
        assert result[0]["type"] == "image"
        assert "url" not in result[0]

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_enable_storage")
    async def test_storage_enabled_upload_success(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="chart")
        with (
            patch("src.server.utils.multimodal_context.upload_base64", return_value=True) as mock_upload,
            patch("src.server.utils.multimodal_context.get_public_url", return_value="https://cdn.example.com/key"),
        ):
            result = await build_attachment_metadata([ctx], thread_id="t123")
            assert result[0]["url"] == "https://cdn.example.com/key"
            # The key passed to upload should be sanitized (no newlines)
            uploaded_key = mock_upload.call_args[0][0]
            assert "\n" not in uploaded_key
            assert uploaded_key.startswith("attachments/t123/")

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_enable_storage")
    async def test_storage_enabled_upload_failure(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="chart")
        with patch("src.server.utils.multimodal_context.upload_base64", return_value=False):
            result = await build_attachment_metadata([ctx])
            assert "url" not in result[0]

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_enable_storage")
    async def test_storage_enabled_upload_exception(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="chart")
        with patch("src.server.utils.multimodal_context.upload_base64", side_effect=RuntimeError("boom")):
            result = await build_attachment_metadata([ctx])
            assert "url" not in result[0]

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_disable_storage")
    async def test_pdf_detection(self):
        ctx = MultimodalContext(type="image", data="data:application/pdf;base64,abc")
        result = await build_attachment_metadata([ctx])
        assert result[0]["type"] == "pdf"

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_disable_storage")
    async def test_description_absent_falls_back(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc")
        result = await build_attachment_metadata([ctx])
        assert result[0]["name"] == "file"

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_disable_storage")
    async def test_thread_id_in_prefix(self):
        """When thread_id is provided, the storage key prefix includes it."""
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="img")
        with (
            patch("src.server.utils.multimodal_context.is_storage_enabled", return_value=True),
            patch("src.server.utils.multimodal_context.upload_base64", return_value=True) as mock_upload,
            patch("src.server.utils.multimodal_context.get_public_url", return_value="url"),
        ):
            await build_attachment_metadata([ctx], thread_id="tid-abc")
            key = mock_upload.call_args[0][0]
            assert "tid-abc" in key

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_disable_storage")
    async def test_no_thread_id_in_prefix(self):
        """When thread_id is empty, the key has no thread segment."""
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="img")
        with (
            patch("src.server.utils.multimodal_context.is_storage_enabled", return_value=True),
            patch("src.server.utils.multimodal_context.upload_base64", return_value=True) as mock_upload,
            patch("src.server.utils.multimodal_context.get_public_url", return_value="url"),
        ):
            await build_attachment_metadata([ctx])
            key = mock_upload.call_args[0][0]
            # Should be attachments/<batch_id>/img.png, no double //
            parts = key.split("/")
            assert parts[0] == "attachments"
            assert len(parts) == 3  # attachments / batch / filename

    # --- REGRESSION: newline in description must not reach S3 key ---

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_enable_storage")
    async def test_multiline_description_sanitized_in_key(self):
        """Regression: multi-line chart description must not produce newlines in S3 key."""
        desc = (
            "Chart: GOOGL (Alphabet Inc. Class A Common Stock)\n"
            "Chart mode: Light\n"
            "Interval: Daily\n"
            "Date range: 2024-04-01 to 2026-03-27 (500 bars)"
        )
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description=desc)
        with (
            patch("src.server.utils.multimodal_context.upload_base64", return_value=True) as mock_upload,
            patch("src.server.utils.multimodal_context.get_public_url", return_value="url"),
        ):
            await build_attachment_metadata([ctx], thread_id="t1")
            key = mock_upload.call_args[0][0]
            assert "\n" not in key
            assert "\r" not in key
            assert "%0A" not in key
            # Should use first line only
            assert "GOOGL" in key
            assert "Chart mode" not in key


# ---------------------------------------------------------------------------
# inject_multimodal_context
# ---------------------------------------------------------------------------


class TestInjectMultimodalContext:
    def test_empty_contexts_returns_unchanged(self):
        msgs = [{"role": "user", "content": "hello"}]
        result = inject_multimodal_context(msgs, [])
        assert result == msgs

    def test_empty_messages_returns_unchanged(self):
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc")
        result = inject_multimodal_context([], [ctx])
        assert result == []

    def test_image_merges_into_user_message(self):
        msgs = [{"role": "user", "content": "describe this"}]
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="chart")
        result = inject_multimodal_context(msgs, [ctx])
        assert len(result) == 1  # merged, not inserted
        blocks = result[0]["content"]
        assert isinstance(blocks, list)
        assert any(b.get("type") == "image_url" for b in blocks)
        assert any("chart" in b.get("text", "") for b in blocks)
        # Original text is last
        assert blocks[-1] == {"type": "text", "text": "describe this"}

    def test_a_message_with_no_words_gets_no_empty_text_block(self):
        """The onboarding kickoff opens with images and no text of the user's;
        an empty text block is one some providers refuse."""
        msgs = [{"role": "user", "content": ""}]
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc", description="positions")
        blocks = inject_multimodal_context(msgs, [ctx])[0]["content"]
        assert any(b.get("type") == "image_url" for b in blocks)
        assert all(b.get("text", "x").strip() for b in blocks if b.get("type") == "text")

    def test_pdf_merges_file_block(self):
        msgs = [{"role": "user", "content": "summarize"}]
        ctx = MultimodalContext(type="image", data="data:application/pdf;base64,abc", description="report")
        result = inject_multimodal_context(msgs, [ctx])
        assert len(result) == 1  # merged
        blocks = result[0]["content"]
        assert any(b.get("type") == "file" for b in blocks)
        assert any("report" in b.get("text", "") for b in blocks)

    def test_merges_into_last_user_message(self):
        msgs = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
            {"role": "user", "content": "second"},
        ]
        ctx = MultimodalContext(type="image", data="data:image/png;base64,abc")
        result = inject_multimodal_context(msgs, [ctx])
        # Same message count, last user message now has list content
        assert len(result) == 4
        assert isinstance(result[3]["content"], list)
        # First user message untouched
        assert result[1]["content"] == "first"


class TestPdfPageCounts:
    @pytest.mark.asyncio
    async def test_a_request_counts_its_pdfs_in_one_thread(self):
        """A thread per PDF would hold as many of the shared executor's workers
        as one request carries PDFs."""
        import base64
        import io
        import threading
        import time

        import pypdf

        def pdf(pages: int) -> str:
            writer = pypdf.PdfWriter()
            for _ in range(pages):
                writer.add_blank_page(width=612, height=792)
            out = io.BytesIO()
            writer.write(out)
            return "data:application/pdf;base64," + base64.b64encode(out.getvalue()).decode()

        contexts = [
            *(MultimodalContext(type="pdf", data=pdf(n)) for n in (1, 2, 3)),
            MultimodalContext(type="image", data="data:image/png;base64,abc"),
        ]
        threads = set()
        real = pypdf.PdfReader

        def reader(stream, *args, **kwargs):
            threads.add(threading.current_thread())
            time.sleep(0.02)  # long enough that a parse per thread would overlap
            return real(stream, *args, **kwargs)

        with patch.object(pypdf, "PdfReader", reader):
            assert await pdf_page_counts(contexts) == [1, 2, 3, None]
        assert len(threads) == 1 and threading.main_thread() not in threads
