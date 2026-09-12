#!/usr/bin/env python3
"""Shared OOXML plumbing for the docx scripts: surgical zip rewriting, part
access, and paragraph/run surgery.

The rewrite is surgical on purpose. A Word document carries parts no library
models (themes, glossary, custom XML, embedded fonts) and formatting nobody
asked us to touch, so every part a script does not explicitly rewrite is copied
through unchanged. That is what makes "edit in place" safe on a document a
human has been working in.
"""

from __future__ import annotations

import datetime as _dt
import os
import random
import re
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import unquote

from lxml import etree

NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "w14": "http://schemas.microsoft.com/office/word/2010/wordml",
    "w15": "http://schemas.microsoft.com/office/word/2012/wordml",
    "w16cid": "http://schemas.microsoft.com/office/word/2016/wordml/cid",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "mc": "http://schemas.openxmlformats.org/markup-compatibility/2006",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    "ct": "http://schemas.openxmlformats.org/package/2006/content-types",
    "xml": "http://www.w3.org/XML/1998/namespace",
}

CT_COMMENTS = "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml"
CT_COMMENTS_EXTENDED = "application/vnd.openxmlformats-officedocument.wordprocessingml.commentsExtended+xml"
RT_COMMENTS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments"
RT_COMMENTS_EXTENDED = "http://schemas.microsoft.com/office/2011/relationships/commentsExtended"

# Parts that carry running text and therefore revisions and comment anchors.
STORY_RE = re.compile(r"^word/(document\d*\.xml|header\d+\.xml|footer\d+\.xml|footnotes\.xml|endnotes\.xml)$")

# Elements inside a run that contribute characters to the paragraph's text.
_RUN_TEXT = {"t": None, "delText": None, "tab": "\t", "br": "\n", "cr": "\n", "noBreakHyphen": "-", "softHyphen": ""}


def qn(tag: str) -> str:
    prefix, local = tag.split(":", 1)
    return "{%s}%s" % (NS[prefix], local)


def local(el) -> str:
    return etree.QName(el).localname


def now_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Package:
    """A .docx opened for surgical rewriting.

    Parts are read as raw bytes and handed back unchanged unless a script parses
    and marks them dirty, so an edit to word/document.xml cannot perturb
    styles.xml, numbering.xml or a header.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        with zipfile.ZipFile(self.path) as zf:
            bad = zf.testzip()
            if bad is not None:
                raise ValueError(f"corrupt zip entry: {bad}")
            self.infos = list(zf.infolist())
            self.raw = {i.filename: zf.read(i.filename) for i in self.infos}
        self._trees: dict[str, etree._Element] = {}
        self._dirty: set[str] = set()
        self._added: list[str] = []

    # -- part access -------------------------------------------------------
    def has(self, name: str) -> bool:
        return name in self.raw

    def names(self) -> list[str]:
        return [i.filename for i in self.infos] + self._added

    def tree(self, name: str) -> etree._Element:
        if name not in self._trees:
            self._trees[name] = etree.fromstring(self.raw[name])
        return self._trees[name]

    def touch(self, name: str) -> etree._Element:
        """Parse a part and mark it for rewrite."""
        root = self.tree(name)
        self._dirty.add(name)
        return root

    def set_tree(self, name: str, root: etree._Element) -> None:
        self._trees[name] = root
        self._dirty.add(name)
        if name not in self.raw:
            self._added.append(name)
            self.raw[name] = b""

    def stories(self) -> list[str]:
        return [n for n in self.raw if STORY_RE.match(n)]

    def save(self, dst: str | Path) -> Path:
        """Write the package to a temporary sibling, then move it onto `dst`.

        Every script here defaults `dst` to the file it read, so opening the destination
        for writing would truncate the user's only copy and an interruption anywhere in
        the loop below would leave a half-written document behind.
        """
        dst = Path(dst).expanduser().resolve()
        handle, name = tempfile.mkstemp(dir=dst.parent, prefix=f".{dst.name}.", suffix=".tmp")
        os.close(handle)
        tmp = Path(name)
        try:
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as out:
                for info in self.infos:
                    data = self._serialize(info.filename)
                    zi = zipfile.ZipInfo(info.filename, date_time=info.date_time)
                    zi.compress_type = info.compress_type
                    zi.external_attr = info.external_attr
                    out.writestr(zi, data)
                for added in self._added:
                    out.writestr(added, self._serialize(added))
            os.replace(tmp, dst)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return dst

    def _serialize(self, name: str) -> bytes:
        if name in self._dirty:
            return etree.tostring(self._trees[name], xml_declaration=True, encoding="UTF-8", standalone=True)
        return self.raw[name]

    # -- package wiring ----------------------------------------------------
    def ensure_override(self, part_name: str, content_type: str) -> None:
        root = self.touch("[Content_Types].xml")
        for ov in root.findall(qn("ct:Override")):
            if ov.get("PartName") == "/" + part_name:
                return
        ov = etree.SubElement(root, qn("ct:Override"))
        ov.set("PartName", "/" + part_name)
        ov.set("ContentType", content_type)

    def ensure_rel(self, source: str, rel_type: str, target: str) -> str:
        rels_name = str(Path(source).parent / "_rels" / (Path(source).name + ".rels"))
        if rels_name not in self.raw:
            root = etree.Element(qn("rel:Relationships"), nsmap={None: NS["rel"]})
            self.set_tree(rels_name, root)
            self.ensure_override(rels_name, "application/vnd.openxmlformats-package.relationships+xml")
        else:
            root = self.touch(rels_name)
        used = set()
        for rel in root.findall(qn("rel:Relationship")):
            used.add(rel.get("Id"))
            if rel.get("Type") == rel_type and rel.get("Target") == target:
                return rel.get("Id")
        n = 1
        while f"rId{n}" in used:
            n += 1
        rel = etree.SubElement(root, qn("rel:Relationship"))
        rel.set("Id", f"rId{n}")
        rel.set("Type", rel_type)
        rel.set("Target", target)
        return f"rId{n}"

    def remove(self, part_name: str) -> bool:
        """Drop a part together with the wiring that points at it.

        A content-type override or a relationship left behind after the bytes
        are gone is a file Word refuses to open, so the three always move
        together.
        """
        if part_name not in self.raw:
            return False
        del self.raw[part_name]
        self._trees.pop(part_name, None)
        self._dirty.discard(part_name)
        if part_name in self._added:
            self._added.remove(part_name)
        self.infos = [i for i in self.infos if i.filename != part_name]
        ct = self.tree("[Content_Types].xml")
        for ov in ct.findall(qn("ct:Override")):
            if ov.get("PartName") == "/" + part_name:
                self.touch("[Content_Types].xml").remove(ov)
        for rels_name in [n for n in self.raw if n.endswith(".rels")]:
            root = self.tree(rels_name)
            stale = [r for r in root.findall(qn("rel:Relationship")) if rel_target(rels_name, r) == part_name]
            if stale:
                self.touch(rels_name)
                for rel in stale:
                    root.remove(rel)
        return True


def rel_target(rels_name: str, rel: etree._Element) -> str | None:
    """The package part a relationship points at, or None when it is external.

    Targets are written relative to the part that owns the .rels file, so they
    only compare against part names once resolved against that directory.
    """
    if (rel.get("TargetMode") or "Internal") == "External":
        return None
    target = unquote(rel.get("Target") or "")
    if target.startswith("/"):
        return target.lstrip("/")
    resolved = (Path(rels_name).parent.parent / target).as_posix().replace("/./", "/")
    while "/../" in resolved:
        resolved = re.sub(r"[^/]+/\.\./", "", resolved, count=1)
    return resolved


def ensure_nsmap(root: etree._Element, prefixes: list[str]) -> etree._Element:
    """Return a root that declares every requested prefix.

    lxml cannot add a namespace declaration to a live element, so a root missing
    one is rebuilt with the wider nsmap and its children moved across. Word keys
    off the namespace URI, but mc:Ignorable lists prefixes, so the declaration
    has to be on the root for a strict consumer.
    """
    missing = [p for p in prefixes if p not in root.nsmap]
    if not missing:
        return root
    nsmap = dict(root.nsmap)
    for p in missing:
        nsmap[p] = NS[p]
    new = etree.Element(root.tag, nsmap=nsmap)
    for k, v in root.attrib.items():
        new.set(k, v)
    for child in list(root):
        new.append(child)
    ignorable = new.get(qn("mc:Ignorable"))
    if ignorable is not None:
        words = ignorable.split()
        words += [p for p in missing if p not in words and p != "mc"]
        new.set(qn("mc:Ignorable"), " ".join(words))
    parent = root.getparent()
    if parent is not None:
        parent.replace(root, new)
    return new


# -- paragraph and run helpers --------------------------------------------

def iter_paragraphs(root: etree._Element) -> list[etree._Element]:
    """Every w:p in document order, table cells included.

    Paragraph indices printed by these scripts count this list. python-docx's
    `document.paragraphs` skips paragraphs inside tables, so the two numberings
    diverge in any document with a table; use the script's index.
    """
    return root.findall(".//" + qn("w:p"))


def _collect(el, out: list[str], mode: str) -> None:
    for child in el:
        tag = local(child)
        if tag == "pPr":
            continue
        if tag in ("ins", "moveTo"):
            if mode != "reject":
                _collect(child, out, mode)
        elif tag in ("del", "moveFrom"):
            if mode != "accept":
                _collect(child, out, mode)
        elif tag == "r":
            for gc in child:
                gtag = local(gc)
                if gtag in ("t", "delText"):
                    out.append(gc.text or "")
                elif gtag in _RUN_TEXT:
                    out.append(_RUN_TEXT[gtag] or "")
        else:
            _collect(child, out, mode)


def para_text(p: etree._Element, mode: str = "accept") -> str:
    """Paragraph text under a revision policy.

    `accept` reads the document as it would be with every change applied,
    `reject` as it was before them, `all` concatenates both sides.
    """
    out: list[str] = []
    _collect(p, out, mode)
    return "".join(out)


def run_text(r: etree._Element) -> str:
    out: list[str] = []
    for gc in r:
        gtag = local(gc)
        if gtag in ("t", "delText"):
            out.append(gc.text or "")
        elif gtag in _RUN_TEXT:
            out.append(_RUN_TEXT[gtag] or "")
    return "".join(out)


def editable_runs(p: etree._Element) -> list[tuple[etree._Element, int, int]]:
    """Runs carrying live text, as (run, start_offset, end_offset).

    Offsets index para_text(p, "accept"). Runs inside w:del are skipped: that
    text is already deleted and cannot be deleted twice.
    """
    found: list[tuple[etree._Element, int, int]] = []
    pos = 0

    def walk(el):
        nonlocal pos
        for child in el:
            tag = local(child)
            if tag == "pPr":
                continue
            if tag in ("del", "moveFrom"):
                continue
            if tag == "r":
                text = run_text(child)
                found.append((child, pos, pos + len(text)))
                pos += len(text)
            else:
                walk(child)

    walk(p)
    return found


def split_run(r: etree._Element, offset: int) -> tuple[etree._Element, etree._Element]:
    """Split a run at a character offset, returning (left, right).

    Children that carry no text (w:tab, w:br, w:fldChar, w:drawing) travel with
    the side they sit on, and a w:t straddling the cut is sliced into both, so a
    split never silently drops a tab or a field.
    """
    right = etree.Element(r.tag, nsmap=r.nsmap)
    for k, v in r.attrib.items():
        right.set(k, v)
    rpr = r.find(qn("w:rPr"))
    if rpr is not None:
        right.append(_clone(rpr))
    pos = 0
    moving = False
    for child in list(r):
        tag = local(child)
        if tag == "rPr":
            continue
        if moving:
            right.append(child)
            continue
        if tag in ("t", "delText"):
            text = child.text or ""
            if pos + len(text) <= offset:
                pos += len(text)
                continue
            cut = offset - pos
            tail = text[cut:]
            child.text = text[:cut]
            _preserve_space(child)
            if child.text == "":
                r.remove(child)
            nt = etree.SubElement(right, child.tag)
            nt.text = tail
            _preserve_space(nt)
            pos = offset
            moving = True
        else:
            width = len(_RUN_TEXT.get(tag) or "")
            if pos + width <= offset:
                pos += width
                continue
            right.append(child)
            moving = True
    r.addnext(right)
    return r, right


def _clone(el):
    return etree.fromstring(etree.tostring(el))


def _preserve_space(t_el) -> None:
    text = t_el.text or ""
    if text != text.strip():
        t_el.set(qn("xml:space"), "preserve")


def make_run(template: etree._Element | None, text: str, deleted: bool = False) -> etree._Element:
    r = etree.Element(qn("w:r"))
    if template is not None:
        rpr = template.find(qn("w:rPr"))
        if rpr is not None:
            r.append(_clone(rpr))
    t = etree.SubElement(r, qn("w:delText") if deleted else qn("w:t"))
    t.text = text
    _preserve_space(t)
    return r


def revision_attrs(el: etree._Element, rid: int, author: str, date: str) -> None:
    el.set(qn("w:id"), str(rid))
    el.set(qn("w:author"), author)
    el.set(qn("w:date"), date)


def max_revision_id(pkg: Package) -> int:
    biggest = 0
    for name in pkg.stories():
        for el in pkg.tree(name).iter():
            if local(el) in ("ins", "del", "moveFrom", "moveTo") or local(el).endswith("PrChange"):
                try:
                    biggest = max(biggest, int(el.get(qn("w:id")) or 0))
                except ValueError:
                    pass
    return biggest


def wrap_deleted(runs: list[etree._Element], rid: int, author: str, date: str) -> etree._Element:
    """Move a contiguous run group into a w:del and retag its text as delText."""
    first = runs[0]
    dele = etree.Element(qn("w:del"))
    revision_attrs(dele, rid, author, date)
    first.addprevious(dele)
    for r in runs:
        dele.append(r)
        for t in r.findall(qn("w:t")):
            t.tag = qn("w:delText")
    return dele


def used_para_ids(*roots: etree._Element) -> set[str]:
    seen: set[str] = set()
    for root in roots:
        if root is None:
            continue
        for el in root.iter():
            for attr in (qn("w14:paraId"), qn("w15:paraId"), qn("w16cid:paraId")):
                value = el.get(attr)
                if value:
                    seen.add(value.upper())
    return seen


def new_para_id(taken: set[str]) -> str:
    """An 8-hex-digit paragraph id. Word rejects 0 and the high bit."""
    while True:
        value = "%08X" % random.randint(1, 0x7FFFFFFF)
        if value not in taken:
            taken.add(value)
            return value
