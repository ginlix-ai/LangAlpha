"""Namespace-blind XML reading for the SEC's XML forms (ownership documents, 13F primary documents and
information tables).

Filers and filing agents declare these documents' namespaces in every order and under every prefix, and a reader
that resolves the "document namespace" from the declarations picks the wrong one when, say, xsi is declared
first. Element names are unique within these schemas, so the engine parses the document as filed and reads
elements by local name. A document whose only fault is in its namespaces (a prefix it never declares, a namespace
name that is not a valid URI) is read with the prefixes and declarations dropped from its markup. Parsing is strict: a document that is cut short or not well formed is no document, never a partial one.
"""
from __future__ import annotations

import re
from datetime import date

from lxml import etree

_PREFIXED_TAG = re.compile(r"<(/?)[A-Za-z_][\w.-]*:")
_PREFIXED_ATTR = re.compile(r"""\s[A-Za-z_][\w.-]*:[\w.-]+\s*=\s*(?:"[^"]*"|'[^']*')""")
_XMLNS_ATTR = re.compile(r"""\sxmlns\s*=\s*(?:"[^"]*"|'[^']*')""")
_VERBATIM = re.compile(r"(<!\[CDATA\[.*?\]\]>|<!--.*?-->)", re.S)
_VERBATIM_OPEN = re.compile(rb"<!\[CDATA\[|<!--")


def _local_names(root: etree._Element) -> etree._Element:
    for el in root.iter():
        if isinstance(el.tag, str) and el.tag.startswith("{"):
            el.tag = etree.QName(el).localname
        for name in [k for k in el.attrib if k.startswith("{")]:
            del el.attrib[name]
    etree.cleanup_namespaces(root)
    return root


def element(source: bytes | str, root: str) -> etree._Element | None:
    """The first complete <root>...</root> element in source (a submission's bytes or text), under any prefix, read
    by local name; None when there is none or it is not well formed. Bytes are decoded over the element alone, so
    another document of the same submission in another encoding cannot garble it."""
    raw = source.encode("utf-8") if isinstance(source, str) else source
    name = re.escape(root.encode())
    m = re.search(rb"<(?:[\w.-]+:)?" + name + rb"[\s>/]", raw)
    if not m:
        return None
    gt = raw.find(b">", m.start())
    if gt > 0 and raw[gt - 1:gt] == b"/":
        span = raw[m.start():gt + 1]
    else:
        close = re.compile(rb"</(?:[\w.-]+:)?" + name + rb"\s*>")
        pos = m.end()
        while (end := close.search(raw, pos)) and (opened := _VERBATIM_OPEN.search(raw, pos, end.start())):
            closed = raw.find(b"]]>" if opened.group() == b"<![CDATA[" else b"-->", opened.end())
            if closed < 0:
                return None
            pos = closed + 3
        if not end:
            return None
        span = raw[m.start():end.end()]
    try:
        doc = span.decode("utf-8")
    except UnicodeDecodeError:
        doc = span.decode("cp1252", errors="replace")
    parser = _parser()
    try:
        return _local_names(etree.fromstring(doc.encode("utf-8"), parser))
    except ValueError:
        return None
    except etree.XMLSyntaxError:
        if not all(e.domain_name == "NAMESPACE" for e in parser.error_log):
            return None
    plain = "".join(part if i % 2 else _XMLNS_ATTR.sub("", _PREFIXED_ATTR.sub("", _PREFIXED_TAG.sub(r"<\1", part)))
                    for i, part in enumerate(_VERBATIM.split(doc)))
    try:
        return etree.fromstring(plain.encode("utf-8"), _parser())
    except (etree.XMLSyntaxError, ValueError):
        return None


def _parser() -> etree.XMLParser:
    """A parser of its own per document: its error log holds only that document's errors (an exception's log is
    shared across parses)."""
    return etree.XMLParser(huge_tree=True, resolve_entities=False, no_network=True)


def first(el: etree._Element | None, *path: str) -> etree._Element | None:
    """The first descendant along path, each step a direct child by name."""
    for name in path:
        if el is None:
            return None
        el = el.find(name)
    return el


def text(el: etree._Element | None, *path: str) -> str | None:
    """Stripped text of the element at path; an element that wraps its text in <value> is read through it."""
    el = first(el, *path)
    if el is None:
        return None
    v = el.find("value")
    s = (v.text if v is not None else el.text) or ""
    s = " ".join(s.split())
    return s or None


def number(s: str | None) -> float | None:
    if s is None:
        return None
    s = s.replace(",", "").replace("$", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def flag(s: str | None) -> bool | None:
    if s is None:
        return None
    s = s.strip().lower()
    if s in ("1", "true", "y", "yes"):
        return True
    if s in ("0", "false", "n", "no"):
        return False
    return None


_ISO = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_US = re.compile(r"(\d{2})-(\d{2})-(\d{4})")


def iso_date(s: str | None) -> str | None:
    """YYYY-MM-DD from an ISO date (a time-zone suffix is dropped) or an MM-DD-YYYY date; None otherwise, a date
    that is not on the calendar (06-31-2024) included."""
    if not s:
        return None
    if m := _ISO.match(s.strip()):
        out = m.group(0)
    elif m := _US.match(s.strip()):
        out = f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    else:
        return None
    try:
        date.fromisoformat(out)
    except ValueError:
        return None
    return out
