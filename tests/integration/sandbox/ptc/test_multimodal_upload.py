"""Integration tests for the attachment save + route + inject pipeline.

Exercises the full flow against a real PTCSandbox (MemoryProvider):
  inline_attachment → save_attachments → route_for
  → inject_multimodal_context / build_file_reminder / build_unsupported_reminder

No mocks: files are actually written to the sandbox filesystem and read back.
"""

from __future__ import annotations

import asyncio
import base64
from types import SimpleNamespace

import pytest

from src.server.models.additional_context import MultimodalContext
from src.server.models.chat_attachment import TurnAttachment, TurnAttachments
from src.server.services.chat_attachment_turns import save_attachments
from src.server.utils.multimodal_context import (
    build_file_reminder,
    build_unsupported_reminder,
    file_note,
    inject_multimodal_context,
    inline_attachment,
    parse_multimodal_contexts,
    route_for,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


def _b64(content: bytes) -> str:
    return base64.b64encode(content).decode()


def _attachment(type_: str, mime: str, raw: bytes, name: str) -> TurnAttachment:
    att = inline_attachment(
        MultimodalContext(type=type_, data=f"data:{mime};base64,{_b64(raw)}", description=name)
    )
    assert att is not None
    return att


def _make_image():
    return _attachment("image", "image/png", b"fake-png-bytes", "chart.png")


def _make_pdf():
    return _attachment("pdf", "application/pdf", b"%PDF-1.4 fake", "report.pdf")


def _make_xlsx():
    return _attachment(
        "file",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        b"PK-fake-xlsx",
        "AMD_DCF_Analysis.xlsx",
    )


def _make_csv():
    return _attachment("file", "text/csv", b"col1,col2\n1,2\n3,4", "data.csv")


def _routed(files: list[TurnAttachment], modalities: list[str]) -> list[TurnAttachment]:
    for att in files:
        att.route = route_for(att, modalities)
    return files


class TestMultimodalUploadPipeline:
    """Full pipeline: save → route → inject → verify messages."""

    # -- Save to real sandbox -------------------------------------------------

    async def test_request_uploads_use_each_resolved_project_before_turn_binding(self, shared_sandbox):
        from ptc_agent.core.project_context import ProjectContext, current_project
        from src.server.handlers.chat.attachments import attach_request_files

        assert current_project() is None
        projects = (ProjectContext("upload-a", "upload-a"), ProjectContext("upload-b", "upload-b"))

        async def upload(project):
            messages = await attach_request_files(
                [{"role": "user", "content": "read it"}],
                TurnAttachments([_make_csv()]),
                SimpleNamespace(sandbox=shared_sandbox),
                None,
                None,
                project=project,
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

    @pytest.mark.parametrize(
        "make, ext, raw",
        [
            (_make_image, ".png", b"fake-png-bytes"),
            (_make_pdf, ".pdf", b"%PDF-1.4 fake"),
            (_make_xlsx, ".xlsx", b"PK-fake-xlsx"),
            (_make_csv, ".csv", b"col1,col2\n1,2\n3,4"),
        ],
    )
    async def test_each_type_is_written_at_a_relative_path(self, shared_sandbox, make, ext, raw):
        [att] = files = [make()]
        await save_attachments(files, shared_sandbox, project=None)
        assert att.path is not None
        assert att.path.startswith("work/uploads/") and att.path.endswith(ext)
        content = await shared_sandbox.adownload_file_bytes(shared_sandbox.normalize_path(att.path))
        assert content == raw

    async def test_several_files_get_unique_paths(self, shared_sandbox):
        files = [_make_image(), _make_pdf(), _make_xlsx(), _make_csv()]
        await save_attachments(files, shared_sandbox, project=None)
        paths = [a.path for a in files]
        assert all(paths) and len(set(paths)) == 4

    # -- Full PTC pipeline: save → route → inject → verify -------------------

    async def test_ptc_claude_image_merged_with_path(self, shared_sandbox):
        files = [_make_image()]
        await save_attachments(files, shared_sandbox, project=None)
        _routed(files, ["text", "image", "pdf"])

        messages = inject_multimodal_context([{"role": "user", "content": "describe this chart"}], files)

        assert len(messages) == 1  # merged, not inserted
        blocks = messages[0]["content"]
        assert any("chart.png" in b.get("text", "") and "work/uploads/" in b.get("text", "") for b in blocks)
        assert any(b.get("type") == "image_url" for b in blocks)
        assert blocks[-1] == {"type": "text", "text": "describe this chart"}

    async def test_ptc_deepseek_image_unsupported_reminder(self, shared_sandbox):
        files = [_make_image()]
        await save_attachments(files, shared_sandbox, project=None)
        _routed(files, ["text"])
        assert [a.route for a in files] == ["unsupported"]

        reminder = build_unsupported_reminder([file_note(a) for a in files])
        assert "cannot view" in reminder.lower()
        assert "chart.png" in reminder and "work/uploads/" in reminder

    async def test_ptc_mixed_image_pdf_xlsx(self, shared_sandbox):
        """All saved; the image gets a block, the saved PDF a note naming its
        path, the xlsx a file note."""
        files = [_make_image(), _make_pdf(), _make_xlsx()]
        await save_attachments(files, shared_sandbox, project=None)
        _routed(files, ["text", "image", "pdf"])
        assert [a.route for a in files] == ["native", "native", "file"]

        messages = inject_multimodal_context([{"role": "user", "content": "analyze all files"}], files)
        blocks = messages[0]["content"]
        assert any(b.get("type") == "image_url" for b in blocks)
        note = {"type": "text", "text": f"[Attached PDF: report.pdf (saved to {files[1].path})]"}
        assert note in blocks
        assert not any(b.get("type") == "file" for b in blocks)
        assert blocks[-1] == {"type": "text", "text": "analyze all files"}

        reminder = build_file_reminder([file_note(a) for a in files if a.route == "file"])
        assert "cannot view" not in reminder.lower()
        assert "AMD_DCF_Analysis.xlsx" in reminder and "Use Python to process it" in reminder

    # -- No-sandbox pipeline -------------------------------------------------

    async def test_no_sandbox_image_goes_in_without_a_path(self, shared_sandbox):
        files = _routed([_make_image()], ["text", "image"])
        messages = inject_multimodal_context([{"role": "user", "content": "what is this?"}], files)
        blocks = messages[0]["content"]
        assert any(b.get("type") == "image_url" for b in blocks)
        label = next(b for b in blocks if "Attached image" in b.get("text", ""))
        assert "saved to" not in label["text"]

    # -- Parse round-trip ----------------------------------------------------

    async def test_parse_all_types_round_trip(self, shared_sandbox):
        raw = [
            {"type": "image", "data": f"data:image/png;base64,{_b64(b'img')}", "description": "img.png"},
            {"type": "pdf", "data": f"data:application/pdf;base64,{_b64(b'pdf')}", "description": "doc.pdf"},
            {"type": "file", "data": f"data:text/csv;base64,{_b64(b'csv')}", "description": "data.csv"},
            {"type": "skills", "name": "ignored"},
        ]
        contexts = parse_multimodal_contexts(raw)
        assert [c.type for c in contexts] == ["image", "pdf", "file"]

        files = [inline_attachment(c) for c in contexts]
        await save_attachments(files, shared_sandbox, project=None)
        assert [a.path.rsplit(".", 1)[1] for a in files] == ["png", "pdf", "csv"]
