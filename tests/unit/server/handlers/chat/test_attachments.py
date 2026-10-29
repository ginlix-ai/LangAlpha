"""A composer PDF the sandbox holds reaches the model as a note, not a document."""

from __future__ import annotations

import base64
import io
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pypdf
import pytest
from PIL import Image

from src.server.handlers.chat.attachments import attach_request_files
from src.server.models.chat import ChatRequest

pytestmark = pytest.mark.asyncio

PDF_MODEL = SimpleNamespace(input_modalities=["text", "image", "pdf"])
TEXT_MODEL = SimpleNamespace(input_modalities=["text"])


def _png() -> str:
    # Decodable, since an image the route cannot be given goes in as a file.
    buffer = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(buffer, "PNG")
    return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode()


PNG = _png()


def _pdf(pages: int, *, password: str | None = None) -> str:
    writer = pypdf.PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=72, height=72)
    if password:
        writer.encrypt(user_password=password)
    buf = io.BytesIO()
    writer.write(buf)
    return "data:application/pdf;base64," + base64.b64encode(buf.getvalue()).decode()


def _session(*, uploads: bool = True):
    sandbox = SimpleNamespace(
        normalize_path=lambda rel, project=None: f"/home/workspace/{rel}",
        aupload_file_bytes=AsyncMock(return_value=uploads),
    )
    return SimpleNamespace(sandbox=sandbox)


async def _attach(files: list[tuple[str, str]], session, config=PDF_MODEL) -> list:
    request = ChatRequest(
        messages=[{"role": "user", "content": "look"}],
        additional_context=[
            {"type": "image" if data.startswith("data:image") else "pdf", "data": data, "description": name}
            for name, data in files
        ],
    )
    messages = [{"role": "user", "content": "look"}]
    out = await attach_request_files(messages, request, session, "model", config)
    return out[-1]["content"]


def _texts(content: list) -> list[str]:
    return [b["text"] for b in content if b.get("type") == "text"]


def _saved(text: str) -> str:
    return text.split("(saved to ", 1)[1].rstrip(")]")


async def test_a_saved_pdf_is_a_note_with_its_page_count():
    content = await _attach([("report.pdf", _pdf(3))], _session())

    assert not any(b.get("type") == "file" for b in content)
    note, words = _texts(content)
    assert note == f"[Attached PDF: report.pdf, 3 pages (saved to {_saved(note)})]"
    assert _saved(note).startswith("work/uploads/report.pdf_")
    assert words == "look"


@pytest.mark.parametrize("data", [_pdf(2, password="secret"), "data:application/pdf;base64,JVBERi0xLjQKbm90"])
async def test_a_pdf_whose_pages_cannot_be_counted_is_noted_without_them(data):
    note, _ = _texts(await _attach([("locked.pdf", data)], _session()))

    assert note == f"[Attached PDF: locked.pdf (saved to {_saved(note)})]"


async def test_a_pdf_with_nowhere_to_be_saved_is_inlined_with_its_pages_stamped():
    for session in (None, _session(uploads=False)):
        content = await _attach([("report.pdf", _pdf(4))], session)

        assert content[0] == {"type": "text", "text": "[Attached PDF: report.pdf]"}
        assert content[1]["type"] == "file" and content[1]["pages"] == 4
        assert content[1]["unsaved"] is True


async def test_an_inlined_pdf_whose_pages_cannot_be_counted_is_not_stamped():
    content = await _attach([("locked.pdf", _pdf(2, password="secret"))], None)

    assert content[1]["type"] == "file" and "pages" not in content[1]


async def test_an_image_still_goes_in_as_itself():
    content = await _attach([("chart.png", PNG), ("report.pdf", _pdf(1))], _session())

    assert [b["type"] for b in content] == ["text", "image_url", "text", "text"]
    assert content[0]["text"].startswith("[Attached image: chart.png (saved to ")
    assert content[2]["text"].startswith("[Attached PDF: report.pdf, 1 page (saved to ")


async def test_a_model_without_pdf_input_keeps_the_unsupported_reminder():
    content = await _attach([("report.pdf", _pdf(3))], _session(), config=TEXT_MODEL)

    assert isinstance(content, str)
    assert "The user attached a file (report.pdf, application/pdf). It has been saved to" in content
    assert "cannot view the attached file(s) directly" in content
