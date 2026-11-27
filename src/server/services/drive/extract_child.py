"""Reads the text out of one Word or PDF file, in a process of its own.

The server runs this file as a child (``extract._run_child``) so a hostile
file costs that child, never the worker: a PDF stream that inflates to
blank space, or Word XML of millions of empty paragraphs, can take a
gigabyte inside a parser no caller can bound. The child caps its own
address space (Linux; macOS does not enforce ``RLIMIT_AS``, so there only
the parent's wall-clock kill bounds it), caps the text it returns, and
the parent kills it at its deadline.

Protocol: ``python -P extract_child.py <kind> <memory bytes> <text cap>``
with the file's bytes on stdin. Stdout is one status line,
``<status> <truncated 0|1>``, then the text as UTF-8. Stdlib only at import,
so the Word path loads nothing else and the server process never loads
the PDF libraries.
"""

from __future__ import annotations

import io
import sys
import zipfile
from typing import Literal
from xml.etree import ElementTree
from xml.parsers import expat

#: The most of ``word/document.xml`` a Word file may unpack to.
DOCX_XML_MAX_BYTES = 50 * 1024 * 1024

#: Statuses as ``drive_extracts`` keeps them; the one list of them.
Status = Literal["ok", "empty", "failed", "too_large"]
OK, EMPTY, FAILED, TOO_LARGE = "ok", "empty", "failed", "too_large"
STATUSES: tuple[Status, ...] = (OK, EMPTY, FAILED, TOO_LARGE)


class TooLarge(Exception):
    """The file is past what the reader takes."""


class DocxTooLarge(TooLarge, ValueError):
    pass


_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
#: The same elements as Word's Strict Open XML save names them.
_W_STRICT = "{http://purl.oclc.org/ooxml/wordprocessingml/main}"
_MC = "{http://schemas.openxmlformats.org/markup-compatibility/2006}"
#: Not shown as the document's text: deletions and text moved away (tracked
#: changes), field codes, and properties, whose ``w:tabs`` are tab stops.
_SKIPPED = {
    _W + "del",
    _W + "moveFrom",
    _W + "delText",
    _W + "instrText",
    _W + "pPr",
    _W + "rPr",
    _MC + "Fallback",
}


class _PrologRead(Exception):
    pass


def _refuse_dtd(xml: bytes) -> None:
    """Raise if the document declares a DTD, which Word never writes and is
    only there to expand entities. Asked of the parser, up to the root
    element, so it holds in whatever encoding the document is in."""

    def dtd(*_: object) -> None:
        raise ValueError("word/document.xml declares a DTD")

    def root(*_: object) -> None:
        raise _PrologRead

    probe = expat.ParserCreate()
    probe.StartDoctypeDeclHandler = dtd
    probe.StartElementHandler = root
    try:
        probe.Parse(xml, True)
    except _PrologRead:
        pass


def docx_text(data: bytes, cap: int | None = None) -> tuple[str, bool]:
    """The text of a Word file and whether it was cut at ``cap`` bytes: one
    line per paragraph, tabs and breaks as written, tracked insertions in
    and deletions out. Refused past ``DOCX_XML_MAX_BYTES`` unpacked, and
    parsed as a stream, each element dropped once read, so a document of
    millions of paragraphs holds only the text it yields."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        info = z.getinfo("word/document.xml")
        if info.file_size > DOCX_XML_MAX_BYTES:
            raise DocxTooLarge("word/document.xml unpacks too large")
        # zipfile reads no more than the size the header claims.
        with z.open(info) as f:
            xml = f.read(DOCX_XML_MAX_BYTES + 1)
    if len(xml) > DOCX_XML_MAX_BYTES:
        raise DocxTooLarge("word/document.xml unpacks too large")
    _refuse_dtd(xml)
    lines: list[str] = []
    size = 0
    #: Text of each paragraph open around the parser, innermost last (a
    #: text box's paragraphs sit inside another's), with the line it fills:
    #: a paragraph reads where it starts, before the boxes anchored in it.
    paragraphs: list[list[str]] = []
    slots: list[int] = []
    open_elements: list[ElementTree.Element] = []
    skipping = 0
    for event, element in ElementTree.iterparse(io.BytesIO(xml), events=("start", "end")):
        tag = element.tag
        if tag.startswith(_W_STRICT):
            tag = _W + tag[len(_W_STRICT) :]
        if event == "start":
            open_elements.append(element)
            if tag in _SKIPPED:
                skipping += 1
            elif tag == _W + "p" and not skipping:
                paragraphs.append([])
                slots.append(len(lines))
                lines.append("")
            continue
        open_elements.pop()
        if tag in _SKIPPED:
            skipping -= 1
        elif not skipping:
            if tag == _W + "p":
                line = "".join(paragraphs.pop())
                lines[slots.pop()] = line
                size += len(line.encode("utf-8", "replace")) + 1
                # Joined, the last line has no newline after it.
                if cap is not None and size - 1 > cap:
                    # A paragraph still open around this one keeps what it read.
                    for slot, parts in zip(slots, paragraphs):
                        lines[slot] = "".join(parts)
                    return "\n".join(lines), True
            elif paragraphs:
                if tag == _W + "t":
                    paragraphs[-1].append(element.text or "")
                elif tag == _W + "tab":
                    paragraphs[-1].append("\t")
                elif tag in (_W + "br", _W + "cr"):
                    paragraphs[-1].append("\n")
                elif tag == _W + "noBreakHyphen":
                    paragraphs[-1].append("-")
        # Read at its end, it is not needed again: drop it from its parent,
        # whose earlier children are all closed and read too.
        element.clear()
        if open_elements:
            del open_elements[-1][:]
    del xml
    return "\n".join(lines), False


#: A reader that finds fewer characters than this (non-blank) may have
#: missed the text layer, so the other reader is tried.
MIN_CHARS = 50


def pdf_text(data: bytes) -> str:
    """pdfplumber, then pypdf, with the memo store's own page readers.
    ``TooLarge`` past their page cap; an error only when both fail. Their
    names are private to that module, which stays as it is until the memo
    store moves into the Drive and whose own tests patch them by name."""
    from ptc_agent.agent.memo.pdf import (
        MemoPdfTooManyPagesError,
        _pdfplumber_extract,
        _pypdf_extract,
    )

    best = None
    errors: list[Exception] = []
    for read in (_pdfplumber_extract, _pypdf_extract):
        try:
            text = read(data)
        except MemoryError:
            raise
        except MemoPdfTooManyPagesError as e:
            raise TooLarge(str(e)) from e
        except Exception as e:  # noqa: BLE001 - the other reader may manage
            errors.append(e)
            continue
        if len(text.strip()) >= MIN_CHARS:
            return text
        best = text if best is None or len(text) > len(best) else best
    if best is not None:
        return best
    if any(_out_of_room(e) for e in errors):
        raise TooLarge("the file inflates past what a reader may hold")
    raise errors[-1]


def _out_of_room(error: BaseException | None) -> bool:
    """The readers wrap running out of memory: pdfminer in its own
    exception over a ``MemoryError``, pypdf as its decompression limit."""
    seen = 0
    while error is not None and seen < 10:
        if isinstance(error, MemoryError) or type(error).__name__ == "LimitReachedError":
            return True
        error = error.__cause__ or error.__context__
        seen += 1
    return False


def capped(text: str, cap: int) -> tuple[bytes, bool]:
    """``text`` as UTF-8 at most ``cap`` bytes, lone surrogates (a PDF's
    broken font maps) replaced so it can be stored at all."""
    data = text.replace("\x00", "").encode("utf-8", "replace")
    if len(data) <= cap:
        return data, False
    return data[:cap].decode("utf-8", "ignore").encode("utf-8"), True


def read_out(kind: str, data: bytes, cap: int) -> tuple[str, bytes, bool]:
    """``(status, text, truncated)``."""
    try:
        if kind == "docx":
            text, truncated = docx_text(data, cap)
        else:
            text, truncated = pdf_text(data), False
    except (MemoryError, TooLarge):
        return TOO_LARGE, b"", False
    except Exception:  # noqa: BLE001 - an unreadable file is an answer
        return FAILED, b"", False
    if not text.strip():
        # Blank as far as the cap reached is not a file with no text.
        return (OK, b"", True) if truncated else (EMPTY, b"", False)
    out, cut = capped(text, cap)
    return OK, out, truncated or cut


def _limit_memory(limit: int) -> None:
    try:
        import resource

        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (ImportError, ValueError, OSError):
        pass  # macOS refuses or ignores it: the parent's deadline still holds


def main() -> None:
    kind, limit, cap = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    # Before the file is read in, so the file counts against the limit too.
    _limit_memory(limit)
    data = b""
    try:
        data = sys.stdin.buffer.read()
        status, text, truncated = read_out(kind, data, cap)
    except MemoryError:
        status, text, truncated = TOO_LARGE, b"", False
    del data
    sys.stdout.buffer.write(f"{status} {int(truncated)}\n".encode() + text)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
