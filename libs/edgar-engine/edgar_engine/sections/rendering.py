"""Lossless line rendering of filing documents.

Every heading rule works on line starts, so the rendering's one job besides keeping all text is to start
each paragraph, heading and table row on its own line. Nothing is deleted: page numbers, running
headers and table cells stay, because a number-only line is often a table cell. A table row with two or
more cells becomes one line, cells joined by " | "; a cell holding only a currency sign or an opening
parenthesis joins the next cell, and a closing parenthesis or percent sign the previous one, so a figure
reads "$ 1,234" or "(56)" rather than "$ | 1,234".
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

import lxml.html
from lxml import etree

from .documents import Document

_BLOCK = {"p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table", "hr", "center",
          "blockquote", "dt", "dd", "title", "ul", "ol", "dl", "section", "article", "body", "pre", "caption"}
_DROP = {"script", "style", "head", "noscript", "title"}
_HTML_SNIFF = re.compile(r"<(html|body|p|div|font|br|span)[\s>/]", re.I)
_SGML_LAYOUT = re.compile(r"</?(PAGE|S|C|TABLE|CAPTION|FN|F\d+|R\d*)>", re.I)
_WS = re.compile(r"\s+")
_OPENS = re.compile(r"[$€£¥]*\(?|[$€£¥]+")
_CLOSES = re.compile(r"\)?%?\)?")


@dataclass
class Rendering:
    lines: list[str] = field(default_factory=list)
    line_doc: list[int] = field(default_factory=list)
    documents: list[dict] = field(default_factory=list)

    def text_and_offsets(self) -> tuple[str, list[int]]:
        offs, pos = [], 0
        for ln in self.lines:
            offs.append(pos)
            pos += len(ln) + 1
        offs.append(pos)
        return "\n".join(self.lines) + ("\n" if self.lines else ""), offs


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


def html_lines(body: str) -> list[str]:
    body = re.sub(r"</?XBRL>", "", body, flags=re.I)
    try:
        root = lxml.html.document_fromstring(body.encode("utf-8", "surrogatepass"),
                                             parser=lxml.html.HTMLParser(encoding="utf-8", remove_comments=True))
    except (etree.ParserError, ValueError):
        return text_lines(re.sub(r"<[^>]+>", " ", body))
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
    r = Rendering()
    for i, d in enumerate(docs):
        start = len(r.lines)
        lines = [] if d.is_binary else (html_lines(d.body) if is_html(d) else text_lines(d.body))
        r.lines.extend(lines)
        r.line_doc.extend([i] * len(lines))
        r.documents.append({"seq": d.seq, "type": d.type, "filename": d.filename, "line_start": start,
                            "line_end": len(r.lines)})
    return r
