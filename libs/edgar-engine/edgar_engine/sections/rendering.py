"""Lossless line rendering of filing documents.

Every heading rule works on line starts, so the rendering's one job besides keeping all text is to start
each paragraph, heading and table row on its own line. Nothing is deleted: page numbers, running
headers and table cells stay, because a number-only line is often a table cell. A table row with two or
more cells becomes one line, cells joined by " | "; a cell holding only a currency sign or an opening
parenthesis joins the next cell, and a closing parenthesis or percent sign the previous one, so a figure
reads "$ 1,234" or "(56)" rather than "$ | 1,234".
"""
from __future__ import annotations

import bisect
import html
import re
import unicodedata
from dataclasses import dataclass, field

import lxml.html
from lxml import etree

from .documents import Document
from .model import DocumentInfo, Span

_BLOCK = {"p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table", "hr", "center",
          "blockquote", "dt", "dd", "title", "ul", "ol", "dl", "section", "article", "body", "pre", "caption"}
_DROP = {"script", "style", "head", "noscript", "title"}
_HTML_SNIFF = re.compile(r"<(html|body|p|div|font|br|span)[\s>/]", re.I)
_SGML_LAYOUT = re.compile(r"</?(PAGE|S|C|TABLE|CAPTION|FN|F\d+|R\d*)>", re.I)
_WS = re.compile(r"\s+")
_OPENS = re.compile(r"[$€£¥]*\(?|[$€£¥]+")
_CLOSES = re.compile(r"\)?%?\)?")
_INLINE = re.compile(r"</?(?:font|basefont|a|span|strong|em|b|i|u|small|big|strike|s|tt)\b[^>]*>", re.I)


@dataclass
class Rendering:
    """The rendered documents: their lines, the document each line comes from, and the text the lines join
    into, with offsets[i] where line i starts in it (offsets[len(lines)] is its end). Document 0 is the main one."""
    lines: list[str] = field(default_factory=list)
    line_doc: list[int] = field(default_factory=list)
    documents: list[DocumentInfo] = field(default_factory=list)
    text: str = field(init=False)
    offsets: list[int] = field(init=False)

    def __post_init__(self):
        self.offsets, pos = [], 0
        for ln in self.lines:
            self.offsets.append(pos)
            pos += len(ln) + 1
        self.offsets.append(pos)
        self.text = "\n".join(self.lines) + ("\n" if self.lines else "")

    @property
    def main_end(self) -> int:
        """The line after the main document's last."""
        return self.documents[0].line_end if self.documents else 0

    def doc_range(self, d: int) -> tuple[int, int]:
        return self.documents[d].line_start, self.documents[d].line_end

    def span(self, a: int, b: int) -> Span:
        """The text of lines a..b-1."""
        return Span(self.offsets[a], self.offsets[b])

    def text_of(self, spans: list[Span]) -> str:
        return "\n".join(self.text[s.start:s.end].rstrip("\n") for s in spans)

    def line_at(self, pos: int) -> int:
        return bisect.bisect_right(self.offsets, pos) - 1


def _clean(s: str) -> str:
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf")
    return _WS.sub(" ", s).strip()


def _row_line(cells: list[str]) -> str:
    out: list[str] = []
    pending = ""
    for c in map(_clean, cells):
        if not c:
            continue
        if _OPENS.fullmatch(c):
            pending += c if c.endswith("(") else c + " "
            continue
        if _CLOSES.fullmatch(c) and out and not pending:
            out[-1] += c
            continue
        out.append(pending + c)
        pending = ""
    if pending.strip():
        out.append(pending.strip())
    return " | ".join(out)


def _layout_row(cells) -> bool:
    """A row whose cells hold nested tables or whole passages lays out the page; it is walked, not joined."""
    return any(c.find(".//table") is not None for c in cells) or sum(len(c.text_content()) for c in cells) > 1500


def _hidden(el) -> bool:
    return "display:none" in (el.get("style") or "").replace(" ", "").lower()


def _parse(body: str):
    """The document tree, and whether libxml2 cut it short: past nesting depth 256 it stops reading and
    reports the limit only in its error log."""
    parser = lxml.html.HTMLParser(encoding="utf-8", remove_comments=True)
    root = lxml.html.document_fromstring(body.encode("utf-8", "surrogatepass"), parser=parser)
    return root, any(e.type_name == "ERR_RESOURCE_LIMIT" for e in parser.error_log)


def html_lines(body: str) -> list[str]:
    """Old filings that open a <FONT> on every paragraph and never close it nest past libxml2's depth limit;
    they are parsed again without their inline formatting tags, and read as plain text if still too deep."""
    body = re.sub(r"</?XBRL>", "", body, flags=re.I)
    try:
        root, cut = _parse(body)
        if cut:
            root, cut = _parse(_INLINE.sub("", body))
    except (etree.ParserError, ValueError):
        cut = True
    if cut:
        flat = re.sub(r"<[^>]+>", " ", re.sub(r"<(?:p|br|div|tr|li|h[1-6]|table)\b[^>]*>", "\n", body, flags=re.I))
        return text_lines(html.unescape(flat))
    out: list[str] = []
    cur: list[str] = []

    def flush():
        line = _clean("".join(cur))
        cur.clear()
        if line:
            out.append(line)

    def text_of(el) -> str:
        parts: list[str] = []

        def rec(e):
            if isinstance(e.tag, str):
                local = e.tag.rsplit("}", 1)[-1].lower().split(":")[-1]
                if local in _DROP or _hidden(e):
                    if e.tail:
                        parts.append(e.tail)
                    return
                if e.text:
                    parts.append(e.text)
                for c in e:
                    rec(c)
                if local in ("br", "p", "div"):
                    parts.append(" ")
            if e.tail:
                parts.append(e.tail)

        if el.text:
            parts.append(el.text)
        for c in el:
            rec(c)
        return "".join(parts)

    def walk(el, pre=False):
        if not isinstance(el.tag, str):
            if el.tail:
                cur.append(el.tail)
            return
        tag = el.tag.lower()
        local = tag.rsplit("}", 1)[-1].split(":")[-1]
        if local in _DROP or (local == "header" and "ix" in tag) or _hidden(el):
            if el.tail:
                cur.append(el.tail)
            return
        if local == "tr":
            cells = [c for c in el if isinstance(c.tag, str) and c.tag.rsplit("}", 1)[-1].lower() in ("td", "th")]
            if len(cells) > 1 and not _layout_row(cells):
                flush()
                line = _row_line([text_of(c) for c in cells])
                if line:
                    out.append(line)
                if el.tail:
                    cur.append(el.tail)
                return
        in_pre = pre or local == "pre"
        block = local in _BLOCK
        if block:
            flush()
        if el.text:
            _append(el.text, in_pre)
        for child in el:
            walk(child, in_pre)
        if block:
            flush()
        if el.tail:
            _append(el.tail, pre)

    def _append(s: str, in_pre: bool):
        if not in_pre:
            cur.append(s)
            return
        parts = s.split("\n")
        for j, p in enumerate(parts):
            if j:
                flush()
            cur.append(p)

    walk(root)
    flush()
    return out


def text_lines(body: str) -> list[str]:
    out = []
    for raw in body.splitlines():
        line = _SGML_LAYOUT.sub("", raw).expandtabs().rstrip()
        line = "".join(ch for ch in line if unicodedata.category(ch) != "Cf")
        if line.strip():
            out.append(line.replace("\xa0", " "))
    return out


def is_html(doc: Document) -> bool:
    return doc.filename.lower().endswith((".htm", ".html")) or bool(_HTML_SNIFF.search(doc.body[:20000]))


def render_documents(docs: list[Document]) -> Rendering:
    lines: list[str] = []
    line_doc: list[int] = []
    documents: list[DocumentInfo] = []
    for i, d in enumerate(docs):
        start = len(lines)
        doc_lines = [] if d.is_binary else (html_lines(d.body) if is_html(d) else text_lines(d.body))
        lines.extend(doc_lines)
        line_doc.extend([i] * len(doc_lines))
        documents.append(DocumentInfo(d.seq, d.type, d.filename, start, len(lines)))
    return Rendering(lines, line_doc, documents)
