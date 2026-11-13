import base64
from unittest.mock import AsyncMock

import pytest

from src.server.models.additional_context import MultimodalContext
from src.server.models.chat_attachment import NATIVE_MAX_BYTES
from src.server.services.chat_attachment_turns import save_attachments
from src.server.utils.multimodal_context import (
    _ext_from_desc,
    build_file_reminder,
    build_unsupported_reminder,
    inject_multimodal_context,
    inline_attachment,
    parse_multimodal_contexts,
    route_for,
)


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


def _image():
    return inline_attachment(_make_image_context())


def _pdf():
    return inline_attachment(_make_pdf_context())


def _csv():
    return inline_attachment(_make_csv_context())


def _native(att, path=None):
    att.route = "native"
    att.path = path
    return att


def _mock_sandbox():
    sandbox = AsyncMock()
    sandbox.normalize_path = lambda p, project=None: f"/home/workspace/{p}"
    sandbox.virtualize_path = lambda p: p.replace("/home/workspace/", "/")
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)
    return sandbox


# -- route_for -------------------------------------------------------------------


class TestRouteFor:
    def test_what_the_model_takes_goes_in_natively(self):
        assert [route_for(a, ["text", "image", "pdf"]) for a in (_image(), _pdf())] == ["native", "native"]

    def test_what_it_does_not_is_unsupported(self):
        assert [route_for(a, ["text"]) for a in (_image(), _pdf())] == ["unsupported", "unsupported"]

    def test_mixed_image_supported_pdf_not(self):
        assert [route_for(a, ["text", "image"]) for a in (_image(), _pdf())] == ["native", "unsupported"]

    def test_file_only_bypasses_capability_check(self):
        """Non-image/non-PDF files are files on every model, never unsupported."""
        assert route_for(_csv(), ["text"]) == "file"
        assert route_for(_csv(), ["text", "image", "pdf"]) == "file"

    def test_an_unreadable_one_is_a_file_on_every_model(self):
        assert route_for(_image(), ["text", "image"], readable=False) == "file"
        assert route_for(_image(), ["text"], readable=False) == "file"

    def test_one_too_large_for_a_request_is_a_file(self):
        att = _image()
        att.size = NATIVE_MAX_BYTES + 1
        assert route_for(att, ["text", "image"]) == "file"


# -- save_attachments ------------------------------------------------------------


class TestSaveAttachments:
    @pytest.mark.asyncio
    async def test_image_is_saved_at_a_relative_path(self):
        sandbox = _mock_sandbox()
        [att] = files = [_image()]
        await save_attachments(files, sandbox, project=None)
        assert att.path is not None
        assert not att.path.startswith("/")  # must be relative
        assert att.path.startswith("work/uploads/")
        sandbox.aupload_file_bytes.assert_called_once_with(f"/home/workspace/{att.path}", b"fake-png-data")

    @pytest.mark.asyncio
    async def test_extensions_come_from_the_type(self):
        sandbox = _mock_sandbox()
        files = [_pdf(), _csv()]
        await save_attachments(files, sandbox, project=None)
        assert files[0].path.endswith(".pdf") and files[1].path.endswith(".csv")

    @pytest.mark.asyncio
    async def test_a_failed_write_leaves_no_path(self):
        sandbox = _mock_sandbox()
        sandbox.aupload_file_bytes = AsyncMock(side_effect=Exception("upload failed"))
        [att] = files = [_image()]
        await save_attachments(files, sandbox, project=None)
        assert att.path is None

    @pytest.mark.asyncio
    async def test_a_refused_write_leaves_no_path(self):
        sandbox = _mock_sandbox()
        sandbox.aupload_file_bytes = AsyncMock(return_value=False)
        [att] = files = [_image()]
        await save_attachments(files, sandbox, project=None)
        assert att.path is None


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
        result = inject_multimodal_context(messages, [_native(_image())])
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
        result = inject_multimodal_context(messages, [_native(_image())])
        content = result[0]["content"]
        # Image blocks come first, then existing text
        assert content[-1] == {"type": "text", "text": "existing text"}
        assert content[0]["type"] == "text"
        assert "[Attached image:" in content[0]["text"]

    def test_with_file_paths_includes_path_note(self):
        messages = [{"role": "user", "content": "Look at this"}]
        result = inject_multimodal_context(messages, [_native(_image(), "/uploads/test.png")])
        content = result[0]["content"]
        assert "(saved to /uploads/test.png)" in content[0]["text"]

    def test_without_file_paths_no_path_note(self):
        messages = [{"role": "user", "content": "Look at this"}]
        result = inject_multimodal_context(messages, [_native(_image())])
        content = result[0]["content"]
        assert "(saved to" not in content[0]["text"]

    def test_files_not_routed_native_are_skipped(self):
        messages = [{"role": "user", "content": "Check data"}]
        result = inject_multimodal_context(messages, [_csv(), _image()])
        assert result[0]["content"] == "Check data"

    def test_empty_messages_unchanged(self):
        assert inject_multimodal_context([], [_native(_image())]) == []

    def test_no_user_message_unchanged(self):
        messages = [{"role": "assistant", "content": "hello"}]
        result = inject_multimodal_context(messages, [_native(_image())])
        assert result[0]["content"] == "hello"

    def test_pdf_content_block(self):
        messages = [{"role": "user", "content": "Read this"}]
        result = inject_multimodal_context(messages, [_native(_pdf())])
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
        result = inject_multimodal_context(messages, [_native(_image())])
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
