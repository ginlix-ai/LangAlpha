"""Reading text out of Word and PDF files: the Word reader, the statuses
the child answers, the child's bounds, and how a version's text is read."""

from __future__ import annotations

import asyncio
import io
import sys
import zipfile
import pytest

from src.server.services.drive import extract_child as child
from src.server.services.drive import extract

W = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'


def _docx(body: str, *, compression: int = zipfile.ZIP_DEFLATED, ns: str = W) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression) as z:
        z.writestr("word/document.xml", f"<w:document {ns}><w:body>{body}</w:body></w:document>")
    return out.getvalue()


def _r(text: str) -> str:
    return f'<w:r><w:t xml:space="preserve">{text}</w:t></w:r>'


def test_docx_paragraphs_tabs_and_breaks():
    body = (
        f"<w:p>{_r('Thesis')}<w:r><w:tab/></w:r>{_r('margins')}</w:p>"
        f"<w:p><w:pPr><w:tabs><w:tab w:val='left' w:pos='720'/></w:tabs></w:pPr>"
        f"{_r('18x')}<w:r><w:br/></w:r>{_r('forward')}</w:p>"
        "<w:tbl><w:tr><w:tc><w:p>" + _r("cell") + "</w:p></w:tc></w:tr></w:tbl>"
    )
    assert child.docx_text(_docx(body)) == ("Thesis\tmargins\n18x\nforward\ncell", False)


def test_docx_reads_a_strict_open_xml_file():
    """Word's Strict save names the same elements under another namespace."""
    strict = 'xmlns:w="http://purl.oclc.org/ooxml/wordprocessingml/main"'
    body = (
        f"<w:p>{_r('Approved')}<w:del w:id='2'><w:r><w:delText> not</w:delText></w:r></w:del>"
        f"<w:r><w:tab/></w:r>{_r('today')}</w:p>"
    )
    assert child.docx_text(_docx(body, ns=strict)) == ("Approved\ttoday", False)


def test_docx_text_that_just_fits_the_cap_is_whole():
    one, two = f"<w:p>{_r('abcde')}</w:p>", f"<w:p>{_r('ab')}</w:p><w:p>{_r('cd')}</w:p>"
    for body in (one, two):
        assert child.docx_text(_docx(body), 5)[1] is False
        assert child.docx_text(_docx(body), 4)[1] is True


def test_docx_keeps_insertions_and_skips_deletions():
    body = (
        f"<w:p>{_r('Risks: rates')}"
        f"<w:ins w:id='1' w:author='A'>{_r(' and credit')}</w:ins>"
        "<w:del w:id='2' w:author='A'><w:r><w:delText> and FX</w:delText></w:r></w:del>"
        "</w:p>"
    )
    assert child.docx_text(_docx(body)) == ("Risks: rates and credit", False)


def test_docx_refuses_a_bomb(monkeypatch):
    monkeypatch.setattr(child, "DOCX_XML_MAX_BYTES", 10_000)
    with pytest.raises(child.DocxTooLarge):
        child.docx_text(_docx("<w:p>" + _r("x" * 20_000) + "</w:p>"))


def test_docx_refuses_a_bomb_whose_header_lies(monkeypatch):
    """A header claiming less than the entry holds never reads past the
    claim: it ends in an error the extraction caches as ``failed``."""
    data = bytearray(_docx("<w:p>" + _r("x" * 20_000) + "</w:p>", compression=zipfile.ZIP_STORED))
    monkeypatch.setattr(child, "DOCX_XML_MAX_BYTES", 10_000)
    real = zipfile.ZipFile.getinfo

    def small(self, name):
        info = real(self, name)
        info.file_size = 100
        return info

    monkeypatch.setattr(zipfile.ZipFile, "getinfo", small)
    with pytest.raises((child.DocxTooLarge, zipfile.BadZipFile)):
        child.docx_text(bytes(data))


def test_docx_refuses_a_dtd():
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as z:
        z.writestr(
            "word/document.xml",
            '<!DOCTYPE x [<!ENTITY a "aaaa">]>' f"<w:document {W}><w:body/></w:document>",
        )
    with pytest.raises(ValueError):
        child.docx_text(raw.getvalue())


def test_docx_refuses_a_dtd_in_any_encoding():
    raw = io.BytesIO()
    xml = (
        '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE x [<!ENTITY a "aaaa">]>'
        f"<w:document {W}><w:body/></w:document>"
    )
    with zipfile.ZipFile(raw, "w") as z:
        z.writestr("word/document.xml", xml.encode("utf-16"))
    with pytest.raises(ValueError, match="DTD"):
        child.docx_text(raw.getvalue())


def test_docx_streams_a_flood_of_empty_paragraphs():
    """Millions of ``<w:p/>`` cost the text they yield, cut at the cap,
    not a tree of millions of elements."""
    text, cut = child.docx_text(_docx("<w:p/>" * 300_000), cap=1000)
    assert cut and len(text) <= 1001


def test_docx_reads_nested_paragraphs_in_document_order():
    body = f"<w:p>{_r('before')}<w:r><w:txbxContent><w:p>{_r('box')}</w:p></w:txbxContent></w:r></w:p>"
    assert child.docx_text(_docx(body)) == ("before\nbox", False)
    # Cut inside the box, the paragraph around it keeps what it had read.
    assert child.docx_text(_docx(body), 2) == ("before\nbox", True)


# --- statuses ------------------------------------------------------------------


def _readers(monkeypatch, plumber, pypdf):
    from ptc_agent.agent.memo import pdf

    monkeypatch.setattr(pdf, "_pdfplumber_extract", plumber)
    monkeypatch.setattr(pdf, "_pypdf_extract", pypdf)


def _raise(error):
    def read(data):
        raise error

    return read


def test_pdf_statuses(monkeypatch):
    from ptc_agent.agent.memo.pdf import MemoPdfTooManyPagesError

    long = "Revenue grew twelve percent year over year, margins up."
    _readers(monkeypatch, lambda d: long, _raise(AssertionError("not reached")))
    assert child.read_out("pdf", b"", 1 << 20)[0] == "ok"
    _readers(monkeypatch, _raise(MemoPdfTooManyPagesError("600 pages")), lambda d: long)
    assert child.read_out("pdf", b"", 1 << 20)[0] == "too_large"
    _readers(monkeypatch, lambda d: " \n", lambda d: "")
    assert child.read_out("pdf", b"", 1 << 20)[0] == "empty"
    # A text layer of one word is text; too short only means the other reader is tried.
    _readers(monkeypatch, lambda d: "", lambda d: "Approved")
    assert child.read_out("pdf", b"", 1 << 20) == ("ok", b"Approved", False)
    _readers(monkeypatch, _raise(ValueError("bad xref")), lambda d: long)
    assert child.read_out("pdf", b"", 1 << 20)[0] == "ok"
    _readers(monkeypatch, _raise(ValueError("bad xref")), _raise(KeyError("Root")))
    assert child.read_out("pdf", b"", 1 << 20)[0] == "failed"
    _readers(monkeypatch, _raise(MemoryError()), lambda d: long)
    assert child.read_out("pdf", b"", 1 << 20)[0] == "too_large"


def _wrapped_memory_error():
    error = ValueError("Unable to allocate output buffer.")
    error.__cause__ = MemoryError()
    return error


class LimitReachedError(Exception):
    pass


@pytest.mark.parametrize(
    "plumber, pypdf",
    [
        # Each resource error beside an ordinary failure of the other reader, so
        # either classification alone has to carry the verdict.
        (_wrapped_memory_error(), KeyError("Root")),
        (ValueError("bad xref"), LimitReachedError("decompressing")),
    ],
)
def test_a_pdf_a_reader_cannot_hold_is_too_large(monkeypatch, plumber, pypdf):
    _readers(monkeypatch, _raise(plumber), _raise(pypdf))
    assert child.read_out("pdf", b"", 1 << 20)[0] == "too_large"


def test_lone_surrogates_are_stored(monkeypatch):
    text = "Broken font map \ud800 here; " * 5
    _readers(monkeypatch, lambda d: text, lambda d: text)
    status, out, cut = child.read_out("pdf", b"", 1 << 20)
    assert status == "ok" and not cut
    assert "\ud800" not in out.decode("utf-8")


def test_the_cap_cuts_on_a_character():
    out, cut = child.capped("é" * 10, 5)
    assert cut and out.decode("utf-8") == "éé"


def _script(tmp_path, body: str) -> str:
    path = tmp_path / "child.py"
    path.write_text(body)
    return str(path)


@pytest.mark.asyncio
async def test_a_child_past_its_deadline_is_killed_and_failed(monkeypatch, tmp_path):
    # Left to finish, the child answers ok: only the deadline makes this failed.
    late = "import sys, time\ntime.sleep(5)\nsys.stdout.write('ok 0\\n')\n"
    monkeypatch.setattr(extract, "_CHILD", _script(tmp_path, late))
    monkeypatch.setattr(extract, "EXTRACT_DEADLINE_S", 0.5)
    started, start = [], asyncio.create_subprocess_exec

    async def watched(*args, **kwargs):
        started.append(await start(*args, **kwargs))
        return started[-1]

    monkeypatch.setattr(asyncio, "create_subprocess_exec", watched)
    try:
        assert await extract._run_child("pdf", b"x") == ("failed", None, False)
        assert started[0].returncode is not None, "no child is left running"
    finally:
        if started and started[0].returncode is None:
            started[0].kill()


@pytest.mark.asyncio
async def test_a_killed_child_whose_wait_never_ends_does_not_hold_the_slot(monkeypatch):
    """A paused stdout reader may never see the pipe close, so the killed
    child's wait never returns: the reap gives up rather than hold the slot."""

    class Stuck:
        returncode = None
        killed = False

        async def communicate(self, data):
            await asyncio.Event().wait()

        def kill(self):
            self.killed = True

        async def wait(self):
            await asyncio.Event().wait()

    proc = Stuck()

    async def spawn(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(extract, "EXTRACT_DEADLINE_S", 0.05)
    monkeypatch.setattr(extract, "_REAP_WAIT_S", 0.05)
    result = await asyncio.wait_for(extract._run_child("pdf", b"x"), 2)
    assert result == ("failed", None, False) and proc.killed


@pytest.mark.asyncio
async def test_a_child_that_dies_is_failed(monkeypatch, tmp_path):
    monkeypatch.setattr(extract, "_CHILD", _script(tmp_path, "import os\nos.abort()\n"))
    assert await extract._run_child("docx", b"x") == ("failed", None, False)


@pytest.mark.asyncio
async def test_a_real_child_reads_a_docx():
    body = f"<w:p>{_r('Recommendation: Buy')}</w:p><w:p>{_r('Risks')}</w:p>"
    assert await extract._run_child("docx", _docx(body)) == ("ok", "Recommendation: Buy\nRisks", False)


@pytest.mark.asyncio
async def test_a_bomb_in_a_real_child_is_too_large():
    data = _docx("<w:p/>" * (9 * 1024 * 1024))  # 54 MiB unpacked, 50 KiB packed
    assert await extract._run_child("docx", data) == ("too_large", None, False)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="RLIMIT_AS is enforced on Linux only")
@pytest.mark.asyncio
async def test_the_memory_limit_ends_a_child_as_too_large(monkeypatch):
    monkeypatch.setattr(extract, "EXTRACT_MEMORY_BYTES", 80 * 1024 * 1024)
    data = _docx("<w:p>" + _r("x" * (45 * 1024 * 1024)) + "</w:p>")
    status, _, _ = await extract._run_child("docx", data)
    assert status == "too_large"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="RLIMIT_AS is enforced on Linux only")
@pytest.mark.asyncio
async def test_the_memory_limit_covers_the_file_the_child_reads_in(monkeypatch):
    """Not a Word file at all: it ends as too large, not as unreadable,
    because the limit stops it on the way in."""
    monkeypatch.setattr(extract, "EXTRACT_MEMORY_BYTES", 80 * 1024 * 1024)
    status, _, _ = await extract._run_child("docx", b"x" * (120 * 1024 * 1024))
    assert status == "too_large"


@pytest.mark.parametrize(
    ("kind", "status", "retry", "answers"),
    [
        ("pdf", "ok", False, True),
        ("pdf", "failed", False, True),
        ("pdf", "failed", True, False),
        ("docx", "failed", False, False),
        ("docx", "ok", False, True),
        ("docx", "too_large", False, False),
        ("docx", "empty", False, False),
    ],
)
def test_only_text_the_other_reader_read_answers_for_this_one(kind, status, retry, answers):
    """The same bytes under ``.docx`` and ``.pdf`` names share one cached
    row: what one reader could not read, the other still tries."""
    from src.server.database.drive.texts import Extract

    cached = Extract(kind=kind, status=status, text=None, truncated=False, retry=retry)
    assert extract.answers(cached, "pdf") is answers
    assert extract.answers(None, "pdf") is False


@pytest.mark.asyncio
async def test_extractions_on_a_worker_run_two_at_a_time_and_wait_holding_nothing(
    monkeypatch, tmp_path
):
    import asyncio
    import time as clock
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    held = peak = 0

    @asynccontextmanager
    async def holding(size):
        nonlocal held, peak
        held += 1
        peak = max(peak, held)
        try:
            yield
        finally:
            held -= 1

    async def read_range(found, start, size):
        return b""

    monkeypatch.setattr(extract, "_CHILD", _script(tmp_path, "import time\ntime.sleep(0.4)\n"))
    monkeypatch.setattr(extract, "_children", None)
    monkeypatch.setattr(extract, "holding", holding)
    monkeypatch.setattr(extract, "read_range", read_range)
    found = SimpleNamespace(size=1)
    started = clock.monotonic()
    await asyncio.gather(*(extract._read_out(found, "pdf") for _ in range(4)))
    assert clock.monotonic() - started >= 0.8
    assert peak == 2, "a reading waiting for its turn holds no bytes"


def test_the_child_gets_the_import_path_and_none_of_the_workers_secrets(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret")
    env = extract._child_env()
    assert "PYTHONPATH" in env
    assert set(env) <= {"PYTHONPATH", "PATH", "LANG", "LC_ALL"}


def test_each_version_is_read_by_its_own_type():
    assert extract.text_kind("x.pdf", "text/markdown", True) == "text", "renamed from .md"
    assert extract.text_kind("x.pdf", "application/pdf", True) == "extracted", "an ASCII PDF"
    assert extract.text_kind("x.docx", "application/octet-stream", False) == "extracted"
    assert extract.text_kind("x.md", "application/pdf", False) == "extracted", "renamed from .pdf"
    assert extract.text_kind("x.pdf", "image/png", False) == "none"
    assert extract.text_kind("x.csv", "text/csv", True) == "text"
