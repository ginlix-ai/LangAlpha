"""Integration tests for the multimodal upload + inject + filter pipeline.

Exercises the full flow against a real PTCSandbox (MemoryProvider):
  parse_multimodal_contexts → upload_to_sandbox → filter_multimodal_by_capability
  → inject_multimodal_context / build_file_reminder / build_unsupported_reminder

No mocks — files are actually written to the sandbox filesystem and read back.
"""

from __future__ import annotations

import base64
import asyncio
from types import SimpleNamespace

import pytest

from src.server.models.additional_context import MultimodalContext
from src.server.utils.multimodal_context import (
    build_file_reminder,
    build_unsupported_reminder,
    filter_multimodal_by_capability,
    inject_multimodal_context,
    parse_multimodal_contexts,
    upload_to_sandbox,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


def _b64(content: bytes) -> str:
    return base64.b64encode(content).decode()


def _make_image():
    """Minimal PNG-like context."""
    return MultimodalContext(
        type="image",
        data=f"data:image/png;base64,{_b64(b'fake-png-bytes')}",
        description="chart.png",
    )


def _make_pdf():
    return MultimodalContext(
        type="pdf",
        data=f"data:application/pdf;base64,{_b64(b'%PDF-1.4 fake')}",
        description="report.pdf",
    )


def _make_xlsx():
    return MultimodalContext(
        type="file",
        data=f"data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{_b64(b'PK-fake-xlsx')}",
        description="AMD_DCF_Analysis.xlsx",
    )


def _make_csv():
    return MultimodalContext(
        type="file",
        data=f"data:text/csv;base64,{_b64(b'col1,col2\n1,2\n3,4')}",
        description="data.csv",
    )


class TestMultimodalUploadPipeline:
    """Full pipeline: upload → filter → inject → verify messages."""

    # -- Upload to real sandbox -----------------------------------------------

    async def test_request_uploads_use_each_resolved_project_before_turn_binding(self, shared_sandbox):
        from ptc_agent.core.project_context import ProjectContext, current_project
        from src.server.handlers.chat.attachments import attach_request_files
        from src.server.models.chat import ChatRequest

        assert current_project() is None
        projects = (ProjectContext("upload-a", "upload-a"), ProjectContext("upload-b", "upload-b"))

        async def upload(project):
            ctx = _make_csv()
            request = ChatRequest(messages=[{"role": "user", "content": "read it"}], additional_context=[ctx])
            messages = await attach_request_files(
                [{"role": "user", "content": "read it"}], request,
                SimpleNamespace(sandbox=shared_sandbox), None, None, project=project,
            )
            text = messages[0]["content"]
            path = text.split("It has been saved to ", 1)[1].split(". Use Python", 1)[0]
            absolute = shared_sandbox.normalize_path(path, project=project)
            assert absolute.startswith(shared_sandbox.working_dir + "/" + project.dir_name + "/work/uploads/")
            assert await shared_sandbox.adownload_file_bytes(absolute) == b"col1,col2\n1,2\n3,4"
            return path

        paths = await asyncio.gather(*(upload(project) for project in projects))
        assert paths[0] != paths[1]
        assert current_project() is None

    async def test_upload_image_creates_file(self, shared_sandbox):
        """Image bytes are written to sandbox and path is relative."""
        paths = await upload_to_sandbox([_make_image()], shared_sandbox)
        assert len(paths) == 1
        assert paths[0] is not None
        assert paths[0].startswith("work/uploads/")
        assert not paths[0].startswith("/")
        assert "chart" in paths[0]
        assert paths[0].endswith(".png")

        # Verify file exists on disk
        abs_path = shared_sandbox.normalize_path(paths[0])
        content = await shared_sandbox.adownload_file_bytes(abs_path)
        assert content == b"fake-png-bytes"

    async def test_upload_pdf_creates_file(self, shared_sandbox):
        paths = await upload_to_sandbox([_make_pdf()], shared_sandbox)
        assert paths[0] is not None
        assert paths[0].endswith(".pdf")
        abs_path = shared_sandbox.normalize_path(paths[0])
        content = await shared_sandbox.adownload_file_bytes(abs_path)
        assert content == b"%PDF-1.4 fake"

    async def test_upload_xlsx_creates_file(self, shared_sandbox):
        """Arbitrary file types upload correctly with extension from MIME map."""
        paths = await upload_to_sandbox([_make_xlsx()], shared_sandbox)
        assert paths[0] is not None
        assert paths[0].endswith(".xlsx")
        abs_path = shared_sandbox.normalize_path(paths[0])
        content = await shared_sandbox.adownload_file_bytes(abs_path)
        assert content == b"PK-fake-xlsx"

    async def test_upload_csv_uses_ext_from_mime(self, shared_sandbox):
        paths = await upload_to_sandbox([_make_csv()], shared_sandbox)
        assert paths[0] is not None
        assert paths[0].endswith(".csv")

    async def test_upload_multiple_parallel_paths(self, shared_sandbox):
        """All files get unique paths, parallel to the input list."""
        contexts = [_make_image(), _make_pdf(), _make_xlsx(), _make_csv()]
        paths = await upload_to_sandbox(contexts, shared_sandbox)
        assert len(paths) == 4
        assert all(p is not None for p in paths)
        # All paths unique
        assert len(set(paths)) == 4

    # -- Filter by capability -------------------------------------------------

    async def test_filter_image_pdf_csv_three_way(self, shared_sandbox):
        """Image, PDF, and CSV split correctly into supported/unsupported/file_only."""
        contexts = [_make_image(), _make_pdf(), _make_csv()]

        # Model that supports image but not PDF
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image"]
        )
        assert len(supported) == 1  # image
        assert supported[0].description == "chart.png"
        assert len(unsupported) == 1  # pdf
        assert unsupported[0].description == "report.pdf"
        assert len(file_only) == 1  # csv
        assert file_only[0].description == "data.csv"

    async def test_filter_xlsx_never_unsupported(self, shared_sandbox):
        """xlsx goes to file_only even when model supports image+pdf."""
        supported, unsupported, file_only = filter_multimodal_by_capability(
            [_make_xlsx()], ["text", "image", "pdf"]
        )
        assert len(supported) == 0
        assert len(unsupported) == 0
        assert len(file_only) == 1

    # -- Full PTC pipeline: upload → filter → inject → verify ----------------

    async def test_ptc_claude_image_merged_with_path(self, shared_sandbox):
        """PTC + Claude + image: native block merged into user message with path."""
        contexts = [_make_image()]
        file_paths = await upload_to_sandbox(contexts, shared_sandbox)
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image", "pdf"]
        )
        assert len(supported) == 1
        assert len(unsupported) == 0
        assert len(file_only) == 0

        messages = [{"role": "user", "content": "describe this chart"}]
        messages = inject_multimodal_context(messages, supported, file_paths=file_paths)

        assert len(messages) == 1  # merged, not inserted
        blocks = messages[0]["content"]
        assert isinstance(blocks, list)
        # Label with path
        assert any("chart.png" in b.get("text", "") for b in blocks)
        assert any("work/uploads/" in b.get("text", "") for b in blocks)
        # Native image block
        assert any(b.get("type") == "image_url" for b in blocks)
        # Original text last
        assert blocks[-1] == {"type": "text", "text": "describe this chart"}

    async def test_ptc_deepseek_image_unsupported_reminder(self, shared_sandbox):
        """PTC + DeepSeek (text only) + image: upload + unsupported warning."""
        contexts = [_make_image()]
        file_paths = await upload_to_sandbox(contexts, shared_sandbox)
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text"]  # no image support
        )
        assert len(supported) == 0
        assert len(unsupported) == 1

        # Build reminder for unsupported
        notes = []
        for i, ctx in enumerate(contexts):
            if ctx in unsupported:
                desc = ctx.description or "file"
                fpath = file_paths[i]
                notes.append(
                    f"The user attached a file ({desc}). "
                    f"It has been saved to {fpath}. "
                    f"Use Python to process it."
                )
        reminder = build_unsupported_reminder(notes)
        assert "cannot view" in reminder.lower()
        assert "chart.png" in reminder
        assert "work/uploads/" in reminder

    async def test_ptc_claude_xlsx_file_only_reminder(self, shared_sandbox):
        """PTC + Claude + xlsx: upload + file_only note, no 'cannot view' warning."""
        contexts = [_make_xlsx()]
        file_paths = await upload_to_sandbox(contexts, shared_sandbox)
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image", "pdf"]  # Claude supports all native types
        )
        assert len(supported) == 0
        assert len(unsupported) == 0
        assert len(file_only) == 1

        notes = []
        for i, ctx in enumerate(contexts):
            if ctx in file_only:
                desc = ctx.description or "file"
                fpath = file_paths[i]
                notes.append(
                    f"The user attached a file ({desc}). "
                    f"It has been saved to {fpath}. "
                    f"Use Python to process it."
                )
        reminder = build_file_reminder(notes)
        assert "cannot view" not in reminder.lower()
        assert "AMD_DCF_Analysis.xlsx" in reminder
        assert "work/uploads/" in reminder
        assert "Use Python to process it" in reminder

    async def test_ptc_mixed_image_pdf_xlsx(self, shared_sandbox):
        """PTC + Claude + image + PDF + xlsx: all uploaded, the image gets a block,
        the saved PDF a note naming its path, the xlsx a file note."""
        image, pdf, xlsx = _make_image(), _make_pdf(), _make_xlsx()
        contexts = [image, pdf, xlsx]

        # Upload all
        file_paths = await upload_to_sandbox(contexts, shared_sandbox)
        assert len(file_paths) == 3
        assert all(p is not None for p in file_paths)

        # Filter (Claude supports image + pdf)
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image", "pdf"]
        )
        assert len(supported) == 2  # image + pdf
        assert len(file_only) == 1  # xlsx

        # Inject supported
        supported_paths = [
            file_paths[i] for i, ctx in enumerate(contexts) if ctx in supported
        ]
        messages = [{"role": "user", "content": "analyze all files"}]
        messages = inject_multimodal_context(messages, supported, file_paths=supported_paths)

        blocks = messages[0]["content"]
        assert any(b.get("type") == "image_url" for b in blocks)
        # Saved, the PDF is a note to Read it from, not the document inline.
        note = {"type": "text", "text": f"[Attached PDF: report.pdf (saved to {supported_paths[1]})]"}
        assert note in blocks
        assert not any(b.get("type") == "file" for b in blocks)
        assert blocks[-1] == {"type": "text", "text": "analyze all files"}

        # File-only reminder (no "cannot view")
        file_notes = []
        for i, ctx in enumerate(contexts):
            if ctx in file_only:
                file_notes.append(
                    f"The user attached a file ({ctx.description}). "
                    f"It has been saved to {file_paths[i]}. "
                    f"Use Python to process it."
                )
        reminder = build_file_reminder(file_notes)
        assert "cannot view" not in reminder.lower()
        assert "AMD_DCF_Analysis.xlsx" in reminder

    # -- No-sandbox pipeline -------------------------------------------------

    async def test_no_sandbox_image_supported_no_upload(self, shared_sandbox):
        """No sandbox + Claude + image: native block merged, no file_paths."""
        contexts = [_make_image()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text", "image"]
        )
        messages = [{"role": "user", "content": "what is this?"}]
        messages = inject_multimodal_context(messages, supported)
        blocks = messages[0]["content"]
        assert any(b.get("type") == "image_url" for b in blocks)
        # No path note (no file_paths passed)
        label = next(b for b in blocks if "Attached image" in b.get("text", ""))
        assert "saved to" not in label["text"]

    async def test_no_sandbox_image_unsupported(self, shared_sandbox):
        """No sandbox + DeepSeek + image: unsupported reminder, no upload."""
        contexts = [_make_image()]
        supported, unsupported, file_only = filter_multimodal_by_capability(
            contexts, ["text"]
        )
        assert len(supported) == 0
        assert len(unsupported) == 1
        reminder = build_unsupported_reminder(["The user attached image file(s)."])
        assert "cannot view" in reminder.lower()

    # -- Parse round-trip ----------------------------------------------------

    async def test_parse_all_types_round_trip(self, shared_sandbox):
        """parse_multimodal_contexts handles image, pdf, and file types from dicts."""
        raw = [
            {"type": "image", "data": f"data:image/png;base64,{_b64(b'img')}", "description": "img.png"},
            {"type": "pdf", "data": f"data:application/pdf;base64,{_b64(b'pdf')}", "description": "doc.pdf"},
            {"type": "file", "data": f"data:text/csv;base64,{_b64(b'csv')}", "description": "data.csv"},
            {"type": "skills", "name": "ignored"},  # should be skipped
        ]
        contexts = parse_multimodal_contexts(raw)
        assert len(contexts) == 3
        assert contexts[0].type == "image"
        assert contexts[1].type == "pdf"
        assert contexts[2].type == "file"

        # Upload all three
        file_paths = await upload_to_sandbox(contexts, shared_sandbox)
        assert len(file_paths) == 3
        assert all(p is not None for p in file_paths)
        assert file_paths[0].endswith(".png")
        assert file_paths[1].endswith(".pdf")
        assert file_paths[2].endswith(".csv")
