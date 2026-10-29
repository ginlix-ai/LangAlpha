import base64
from unittest.mock import AsyncMock

import pytest

from src.server.utils.multimodal_context import (
    _ext_from_desc,
    build_file_reminder,
    build_unsupported_reminder,
    filter_multimodal_by_capability,
    inject_multimodal_context,
    parse_multimodal_contexts,
    upload_to_sandbox,
)
from src.server.models.additional_context import MultimodalContext


def _make_image_context():
    """Create a minimal image context with base64 data URL."""
    b64 = base64.b64encode(b"fake-png-data").decode()
    return MultimodalContext(
        type="image",
        data=f"data:image/png;base64,{b64}",
        description="test-image.png",
    )


def _make_pdf_context():
    """Create a minimal PDF context with base64 data URL."""
    b64 = base64.b64encode(b"%PDF-1.4 fake").decode()
    return MultimodalContext(
        type="pdf",
        data=f"data:application/pdf;base64,{b64}",
        description="test-document.pdf",
    )


def _make_csv_context():
    """Create a minimal CSV context with base64 data URL."""
    b64 = base64.b64encode(b"col1,col2\n1,2").decode()
    return MultimodalContext(
        type="file",
        data=f"data:text/csv;base64,{b64}",
        description="data.csv",
    )


def _mock_sandbox():
    sandbox = AsyncMock()
    sandbox.normalize_path = lambda p, project=None: f"/home/workspace/{p}"
    sandbox.virtualize_path = lambda p: p.replace("/home/workspace/", "/")
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)
    return sandbox


# -- filter_multimodal_by_capability ------------------------------------------


class TestFilterMultimodalByCapability:
    def test_all_supported(self):
        contexts = [_make_image_context(), _make_image_context()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image", "pdf"]
        )
        assert len(supported) == 2
        assert len(unsupported) == 0
        assert len(file_only) == 0

    def test_all_unsupported(self):
        contexts = [_make_image_context(), _make_pdf_context()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text"]
        )
        assert len(supported) == 0
        assert len(unsupported) == 2
        assert len(file_only) == 0

    def test_mixed_image_supported_pdf_not(self):
        contexts = [_make_image_context(), _make_pdf_context()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image"]
        )
        assert len(supported) == 1  # image
        assert len(unsupported) == 1  # pdf
        assert len(file_only) == 0

    def test_pdf_supported_image_not(self):
        contexts = [_make_image_context(), _make_pdf_context()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "pdf"]
        )
        assert len(supported) == 1  # pdf
        assert len(unsupported) == 1  # image
        assert len(file_only) == 0

    def test_empty_contexts(self):
        supported, unsupported, file_only = filter_multimodal_by_capability([], ["text"])
        assert supported == []
        assert unsupported == []
        assert file_only == []

    def test_file_only_bypasses_capability_check(self):
        """Non-image/non-PDF files go to file_only, not unsupported."""
        contexts = [_make_csv_context()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text"]
        )
        assert len(supported) == 0
        assert len(unsupported) == 0
        assert len(file_only) == 1

    def test_mixed_all_three_categories(self):
        """Image + PDF + CSV split into supported, unsupported, file_only."""
        contexts = [_make_image_context(), _make_pdf_context(), _make_csv_context()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image"]  # image supported, pdf not
        )
        assert len(supported) == 1  # image
        assert len(unsupported) == 1  # pdf
        assert len(file_only) == 1  # csv


# -- upload_to_sandbox ---------------------------------------------------------


class TestUploadToSandbox:
    @pytest.mark.asyncio
    async def test_image_upload_returns_relative_path(self):
        sandbox = _mock_sandbox()
        contexts = [_make_image_context()]
        paths = await upload_to_sandbox(contexts, sandbox)
        assert len(paths) == 1
        assert paths[0] is not None
        assert "uploads/" in paths[0]
        assert not paths[0].startswith("/")  # must be relative
        assert paths[0].startswith("work/uploads/")
        sandbox.aupload_file_bytes.assert_called_once()

    @pytest.mark.asyncio
    async def test_pdf_upload_returns_path(self):
        sandbox = _mock_sandbox()
        contexts = [_make_pdf_context()]
        paths = await upload_to_sandbox(contexts, sandbox)
        assert len(paths) == 1
        assert paths[0] is not None
        assert ".pdf" in paths[0]

    @pytest.mark.asyncio
    async def test_csv_upload_returns_path_with_ext(self):
        sandbox = _mock_sandbox()
        contexts = [_make_csv_context()]
        paths = await upload_to_sandbox(contexts, sandbox)
        assert len(paths) == 1
        assert paths[0] is not None
        assert ".csv" in paths[0]

    @pytest.mark.asyncio
    async def test_upload_failure_returns_none(self):
        sandbox = _mock_sandbox()
        sandbox.aupload_file_bytes = AsyncMock(side_effect=Exception("upload failed"))
        contexts = [_make_image_context()]
        paths = await upload_to_sandbox(contexts, sandbox)
        assert len(paths) == 1
        assert paths[0] is None

    @pytest.mark.asyncio
    async def test_multiple_uploads_returns_parallel_list(self):
        sandbox = _mock_sandbox()
        contexts = [_make_image_context(), _make_pdf_context()]
        paths = await upload_to_sandbox(contexts, sandbox)
        assert len(paths) == 2
        assert all(p is not None for p in paths)

    @pytest.mark.asyncio
    async def test_upload_false_returns_none(self):
        sandbox = _mock_sandbox()
        sandbox.aupload_file_bytes = AsyncMock(return_value=False)
        contexts = [_make_image_context()]
        paths = await upload_to_sandbox(contexts, sandbox)
        assert paths[0] is None


# -- parse_multimodal_contexts ------------------------------------------------


class TestParseMultimodalContexts:
    def test_parse_image_dict(self):
        b64 = base64.b64encode(b"fake").decode()
        raw = [{"type": "image", "data": f"data:image/png;base64,{b64}", "description": "img.png"}]
        result = parse_multimodal_contexts(raw)
        assert len(result) == 1
        assert result[0].type == "image"

    def test_parse_pdf_dict(self):
        b64 = base64.b64encode(b"fake").decode()
        raw = [{"type": "pdf", "data": f"data:application/pdf;base64,{b64}", "description": "doc.pdf"}]
        result = parse_multimodal_contexts(raw)
        assert len(result) == 1
        assert result[0].type == "pdf"

    def test_parse_file_dict(self):
        b64 = base64.b64encode(b"fake").decode()
        raw = [{"type": "file", "data": f"data:text/csv;base64,{b64}", "description": "data.csv"}]
        result = parse_multimodal_contexts(raw)
        assert len(result) == 1
        assert result[0].type == "file"

    def test_ignores_unknown_type(self):
        raw = [{"type": "skills", "name": "foo"}]
        result = parse_multimodal_contexts(raw)
        assert len(result) == 0

    def test_multimodal_context_passthrough(self):
        ctx = _make_image_context()
        result = parse_multimodal_contexts([ctx])
        assert len(result) == 1
        assert result[0] is ctx

    def test_empty_returns_empty(self):
        assert parse_multimodal_contexts(None) == []
        assert parse_multimodal_contexts([]) == []


# -- inject_multimodal_context (single-message merge) -------------------------


class TestInjectMultimodalContext:
    def test_string_content_converted_to_list(self):
        messages = [{"role": "user", "content": "Analyze this"}]
        result = inject_multimodal_context(messages, [_make_image_context()])
        content = result[0]["content"]
        assert isinstance(content, list)
        # Image blocks prepended, then text
        assert content[-1] == {"type": "text", "text": "Analyze this"}
        assert content[0]["type"] == "text"
        assert "[Attached image:" in content[0]["text"]
        assert content[1]["type"] == "image_url"

    def test_list_content_blocks_prepended(self):
        messages = [{"role": "user", "content": [
            {"type": "text", "text": "existing text"},
        ]}]
        result = inject_multimodal_context(messages, [_make_image_context()])
        content = result[0]["content"]
        # Image blocks come first, then existing text
        assert content[-1] == {"type": "text", "text": "existing text"}
        assert content[0]["type"] == "text"
        assert "[Attached image:" in content[0]["text"]

    def test_with_file_paths_includes_path_note(self):
        messages = [{"role": "user", "content": "Look at this"}]
        result = inject_multimodal_context(
            messages, [_make_image_context()], file_paths=["/uploads/test.png"]
        )
        content = result[0]["content"]
        assert "(saved to /uploads/test.png)" in content[0]["text"]

    def test_without_file_paths_no_path_note(self):
        messages = [{"role": "user", "content": "Look at this"}]
        result = inject_multimodal_context(messages, [_make_image_context()])
        content = result[0]["content"]
        assert "(saved to" not in content[0]["text"]

    def test_non_image_non_pdf_skipped(self):
        messages = [{"role": "user", "content": "Check data"}]
        result = inject_multimodal_context(messages, [_make_csv_context()])
        # CSV should be skipped, content stays as string
        assert result[0]["content"] == "Check data"

    def test_empty_messages_unchanged(self):
        assert inject_multimodal_context([], [_make_image_context()]) == []

    def test_no_user_message_unchanged(self):
        messages = [{"role": "assistant", "content": "hello"}]
        result = inject_multimodal_context(messages, [_make_image_context()])
        assert result[0]["content"] == "hello"

    def test_pdf_content_block(self):
        messages = [{"role": "user", "content": "Read this"}]
        result = inject_multimodal_context(messages, [_make_pdf_context()])
        content = result[0]["content"]
        assert isinstance(content, list)
        assert any(b.get("type") == "file" for b in content)
        assert any("[Attached PDF:" in b.get("text", "") for b in content)

    def test_single_message_not_separate(self):
        """Verify no separate message is inserted (regression test)."""
        messages = [
            {"role": "assistant", "content": "hi"},
            {"role": "user", "content": "Check this"},
        ]
        result = inject_multimodal_context(messages, [_make_image_context()])
        assert len(result) == 2  # same count, no insertion


# -- _ext_from_desc -----------------------------------------------------------


class TestExtFromDesc:
    def test_csv(self):
        assert _ext_from_desc("report.csv") == ".csv"

    def test_no_extension(self):
        assert _ext_from_desc("data") == ""

    def test_tar_gz(self):
        assert _ext_from_desc("file.tar.gz") == ".gz"

    def test_uppercase(self):
        assert _ext_from_desc("Photo.JPEG") == ".jpeg"

    def test_empty(self):
        assert _ext_from_desc("") == ""


# -- build_file_reminder / build_unsupported_reminder -------------------------


class TestBuildReminders:
    def test_unsupported_reminder_contains_cannot_view(self):
        result = build_unsupported_reminder(["file note"])
        assert "cannot view" in result.lower()
        assert "<system-reminder>" in result

    def test_file_reminder_omits_cannot_view(self):
        result = build_file_reminder(["file note"])
        assert "cannot view" not in result.lower()
        assert "<system-reminder>" in result

    def test_file_reminder_includes_notes(self):
        result = build_file_reminder(["note1", "note2"])
        assert "note1" in result
        assert "note2" in result

    def test_unsupported_reminder_includes_notes(self):
        result = build_unsupported_reminder(["attached image"])
        assert "attached image" in result
