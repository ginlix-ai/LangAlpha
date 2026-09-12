#!/usr/bin/env python3
"""Check a .docx against the package rules and the conventions in SKILL.md.

Usage:
    python validate.py <file.docx> [--strict] [--final]

Checks, in order of importance:

    package      zip integrity, python-docx opens it, [Content_Types].xml covers
                 every part, every relationship target resolves
    xml_order    property elements carry their children in the ECMA-376 order,
                 which Word enforces and LibreOffice does not
    headings     Heading N styles rather than bolded body text, no level skips,
                 no direct w:outlineLvl, no reference to a missing style
    tables       header row set to repeat, column widths declared, table fits
                 inside the text area
    text         no placeholder tokens, no U+2011 or soft hyphens, no word split
                 across identically formatted runs
    fonts        declared fonts are metric-safe, so the LibreOffice render and
                 the reader's Word agree on pagination
    fields       a TOC is a real field and settings.xml asks Word to update it
    review       how many tracked changes and open comment threads are shipping
    final        --final only: tracked revisions and comments left behind in a
                 document being delivered as the clean copy

`fail` blocks delivery, `warn` is a judgement call, `info` is context. Exit is 0
unless the file cannot be read, or --strict is given and a `fail` exists.

Revisions and comments are `info` by default because a review copy is meant to
carry them. Pass --final on the copy the user actually receives and the same
nodes become `final_revisions` and `final_comments` at `fail`.
"""

from __future__ import annotations

import json
import re
import sys
import zipfile
from pathlib import Path
from urllib.parse import unquote

from lxml import etree

from docx_parts import NS, Package, iter_paragraphs, local, para_text, qn

METRIC_SAFE = {"arial", "calibri", "cambria", "times new roman", "courier new"}
PLACEHOLDERS = [
    (re.compile(r"\bTODO\b|\bFIXME\b|\bXXX\b"), "fail"),
    (re.compile(r"\[INSERT|\[TBD\]|\{\{.*?\}\}|<placeholder", re.I), "fail"),
    (re.compile(r"lorem ipsum", re.I), "fail"),
    (re.compile(r"\bTBD\b|\bplaceholder\b", re.I), "warn"),
]
ODD_CHARS = {"‑": ("fail", "non-breaking hyphen U+2011; use an ASCII hyphen"),
             "­": ("fail", "soft hyphen U+00AD; invisible in Word and copied into extracted text"),
             "�": ("fail", "replacement character U+FFFD; the text was decoded with the wrong encoding")}
HEADING_RE = re.compile(r"^heading\s*([1-9])$", re.I)
EXAMPLES = 20
REVISION_TAGS = {"ins", "del", "moveFrom", "moveTo", "moveFromRangeStart", "moveFromRangeEnd",
                 "moveToRangeStart", "moveToRangeEnd", "cellIns", "cellDel", "cellMerge",
                 "numberingChange"}
COMMENT_ANCHORS = {"commentRangeStart", "commentRangeEnd", "commentReference"}
COMMENT_PART_RE = re.compile(r"^word/comments\w*\.xml$")
FINAL_COMMENTS_MESSAGE = "review comments in a document delivered as final; run comments.py strip first"


class Finding:
    def __init__(self, check: str, level: str, message: str):
        self.check, self.level, self.message = check, level, message
        self.items: list[str] = []
        self.count = 0

    def add(self, where: str) -> None:
        self.count += 1
        if len(self.items) < EXAMPLES:
            self.items.append(where)

    def as_dict(self) -> dict:
        return {"check": self.check, "level": self.level, "count": self.count,
                "message": self.message, "examples": self.items}


class Report:
    def __init__(self):
        self.findings: dict[str, Finding] = {}

    def f(self, check: str, level: str, message: str) -> Finding:
        if check not in self.findings:
            self.findings[check] = Finding(check, level, message)
        return self.findings[check]


def on_off(parent, path: str) -> bool:
    """Whether an OOXML on/off property is switched on, which presence alone gets wrong.

    Word writes a toggle it has turned back off as the element carrying
    w:val="false", so w:tblHeader and w:b are on only when the attribute is
    absent or affirmative.
    """
    el = parent.find(path)
    return el is not None and (el.get(qn("w:val")) or "true").lower() not in ("false", "0", "off")


# -- OOXML element order ---------------------------------------------------

# ECMA-376 Part 1 defines each property element as an ordered sequence
# (CT_PPr, CT_RPr, CT_TblPr, CT_TblPrEx, CT_TrPr, CT_TcPr, CT_SectPr). Word
# enforces the order and offers to repair a file that breaks it; LibreOffice
# accepts any order, so a shuffled part renders correctly here and still fails
# for the reader. A nested tuple is a group whose members may appear in any
# order among themselves. Names absent from a tuple, extension elements such as
# w14 included, are ignored rather than flagged. The run and row property bases
# are repeated choices in the schema, so their members form one group: only
# the revision markers ahead of them and the *Change element behind them have
# a fixed place. pandoc writes bCs before b, and Word opens that without
# complaint.
_RPR_BASE = ((
    "rStyle", "rFonts", "b", "bCs", "i", "iCs", "caps", "smallCaps", "strike",
    "dstrike", "outline", "shadow", "emboss", "imprint", "noProof", "snapToGrid",
    "vanish", "webHidden", "color", "spacing", "w", "kern", "position", "sz",
    "szCs", "highlight", "u", "effect", "bdr", "shd", "fitText", "vertAlign",
    "rtl", "cs", "em", "lang", "eastAsianLayout", "specVanish", "oMath",
),)
_TBL_PR_EX = (
    "tblW", "jc", "tblCellSpacing", "tblInd", "tblBorders", "shd", "tblLayout",
    "tblCellMar", "tblLook",
)
_SEQUENCES: dict[str, tuple] = {
    "pPr": (
        "pStyle", "keepNext", "keepLines", "pageBreakBefore", "framePr",
        "widowControl", "numPr", "suppressLineNumbers", "pBdr", "shd", "tabs",
        "suppressAutoHyphens", "kinsoku", "wordWrap", "overflowPunct",
        "topLinePunct", "autoSpaceDE", "autoSpaceDN", "bidi", "adjustRightInd",
        "snapToGrid", "spacing", "ind", "contextualSpacing", "mirrorIndents",
        "suppressOverlap", "jc", "textDirection", "textAlignment",
        "textboxTightWrap", "outlineLvl", "divId", "cnfStyle",
        "rPr", "sectPr", "pPrChange",
    ),
    "rPr": _RPR_BASE + ("rPrChange",),
    "tblPr": (
        "tblStyle", "tblpPr", "tblOverlap", "bidiVisual", "tblStyleRowBandSize",
        "tblStyleColBandSize", *_TBL_PR_EX, "tblCaption", "tblDescription",
        "tblPrChange",
    ),
    "tblPrEx": _TBL_PR_EX + ("tblPrExChange",),
    "trPr": (
        ("cnfStyle", "divId", "gridBefore", "gridAfter", "wBefore", "wAfter",
         "cantSplit", "trHeight", "tblHeader", "tblCellSpacing", "jc", "hidden"),
        "ins", "del", "trPrChange",
    ),
    "tcPr": (
        "cnfStyle", "tcW", "gridSpan", "hMerge", "vMerge", "tcBorders", "shd",
        "noWrap", "tcMar", "textDirection", "tcFitText", "vAlign", "hideMark",
        ("cellIns", "cellDel", "cellMerge"), "tcPrChange",
    ),
    "sectPr": (
        ("headerReference", "footerReference"), "footnotePr", "endnotePr",
        "type", "pgSz", "pgMar", "paperSrc", "pgBorders", "lnNumType",
        "pgNumType", "cols", "formProt", "vAlign", "noEndnote", "titlePg",
        "textDirection", "bidi", "rtlGutter", "docGrid", "printerSettings",
        "sectPrChange",
    ),
}
ORDER_MESSAGE = ("property element children are out of the ECMA-376 sequence; Word offers to "
                 "repair the file, LibreOffice renders it without complaint")


def _ranks(sequence: tuple) -> dict[str, int]:
    out: dict[str, int] = {}
    for i, entry in enumerate(sequence):
        for name in (entry,) if isinstance(entry, str) else entry:
            out[name] = i
    return out


_ORDER = {name: _ranks(seq) for name, seq in _SEQUENCES.items()}
# w:pPr/w:rPr is CT_ParaRPr, not CT_RPr: the paragraph mark's own revision
# markers lead, and the run properties follow them.
_PARA_RPR = _ranks(("ins", "del", "moveFrom", "moveTo") + _RPR_BASE + ("rPrChange",))
_ORDER_TAGS = tuple(qn("w:" + name) for name in (*_SEQUENCES, "p", "r", "body"))
_W = "{%s}" % NS["w"]


def _w_children(el) -> list:
    return [c for c in el if isinstance(c.tag, str) and c.tag.startswith(_W)]


def _out_of_order(el, ranks: dict[str, int]) -> tuple[str, str] | None:
    """The first child that sits earlier in the sequence than a child before it."""
    high, earlier = -1, ""
    for child in _w_children(el):
        rank = ranks.get(local(child))
        if rank is None:
            continue
        if rank < high:
            return local(child), earlier
        high, earlier = rank, local(child)
    return None


def _para_label(el, marks: dict[int, str], part: str) -> str:
    node = el
    while node is not None:
        mark = marks.get(id(node))
        if mark is not None:
            return f"{part} {mark}"
        node = node.getparent()
    return part


def _order_problems(root, part: str) -> list[str]:
    found: list[str] = []
    paragraphs = iter_paragraphs(root)  # held so the id() keys stay live
    marks = {id(p): f"p{i}" for i, p in enumerate(paragraphs)}
    for el in root.iter(*_ORDER_TAGS):
        name = local(el)
        where = _para_label(el, marks, part)
        if name in _ORDER:
            parent = el.getparent()
            in_para_mark = name == "rPr" and parent is not None and local(parent) == "pPr"
            pair = _out_of_order(el, _PARA_RPR if in_para_mark else _ORDER[name])
            if pair is not None:
                found.append(f"{where} w:{name} {pair[0]} after {pair[1]}")
        elif name in ("p", "r"):
            lead = "pPr" if name == "p" else "rPr"
            names = [local(c) for c in _w_children(el)]
            if lead in names and names[0] != lead:
                found.append(f"{where} w:{name} {lead} after {names[0]}")
        else:  # w:body, where only the closing w:sectPr has a fixed position
            children = _w_children(el)
            for i, child in enumerate(children[:-1]):
                if local(child) == "sectPr":
                    found.append(f"{where} w:body sectPr before {local(children[i + 1])}")
    return found


def check_xml_order(pkg: Package, rep: Report) -> None:
    """Child order inside the property elements.

    These scripts never hand-write XML, but a file a human or another tool has
    been through might, and a shuffled w:pPr is invisible in the LibreOffice
    render and a repair prompt in the reader's Word.
    """
    for part in sorted(pkg.stories()):
        for problem in _order_problems(pkg.tree(part), Path(part).name):
            rep.f("xml_order", "fail", ORDER_MESSAGE).add(problem)


def check_package(pkg: Package, rep: Report) -> None:
    ct = pkg.tree("[Content_Types].xml")
    defaults = {d.get("Extension", "").lower() for d in ct.findall(qn("ct:Default"))}
    overrides = {o.get("PartName") for o in ct.findall(qn("ct:Override"))}
    for name in pkg.names():
        if name == "[Content_Types].xml":
            continue
        if "/" + name in overrides or name.rsplit(".", 1)[-1].lower() in defaults:
            continue
        rep.f("package_content_types", "fail", "part has no content type; Word refuses to open the file").add(name)
    for name in [n for n in pkg.names() if n.endswith(".rels")]:
        base = Path(name).parent.parent
        for rel in pkg.tree(name).findall(qn("rel:Relationship")):
            if (rel.get("TargetMode") or "Internal") == "External":
                continue
            target = unquote(rel.get("Target") or "")
            if target.startswith("/"):
                resolved = target.lstrip("/")
            else:
                resolved = str((base / target).as_posix()).replace("/./", "/")
                while "/../" in resolved:
                    resolved = re.sub(r"[^/]+/\.\./", "", resolved, count=1)
            if resolved not in pkg.raw:
                rep.f("package_relationships", "fail", "relationship points at a part that is not in the package").add(f"{name} -> {target}")


def sect_text_width(sect) -> int | None:
    pg, mar = sect.find(qn("w:pgSz")), sect.find(qn("w:pgMar"))
    if pg is None:
        return None
    try:
        width = int(pg.get(qn("w:w")))
        left = int(mar.get(qn("w:left"))) if mar is not None else 0
        right = int(mar.get(qn("w:right"))) if mar is not None else 0
        return width - left - right
    except (TypeError, ValueError):
        return None


def tables_by_section(root):
    """Pair every table with the text width of the section that governs it.

    A paragraph's w:pPr/w:sectPr closes the section that paragraph sits in, so a
    body element obeys the next w:sectPr at or after it and the last section
    obeys the trailing w:body/w:sectPr. Measured against the portrait section
    above it, a landscape appendix reads as an overflow that is not there.
    """
    body = root.find(qn("w:body"))
    sect_path = qn("w:pPr") + "/" + qn("w:sectPr")
    pending: list = []
    for child in _w_children(body) if body is not None else []:
        pending.extend(child.iter(qn("w:tbl")))
        sect = child if local(child) == "sectPr" else child.find(sect_path)
        if sect is None:
            continue
        width = sect_text_width(sect)
        for tbl in pending:
            yield tbl, width
        pending = []
    for tbl in pending:
        yield tbl, None


def check_structure(pkg: Package, rep: Report, stats: dict) -> None:
    root = pkg.tree("word/document.xml")
    known = set()
    if pkg.has("word/styles.xml"):
        known = {s.get(qn("w:styleId")) for s in pkg.tree("word/styles.xml").findall(qn("w:style"))}
    pstyle_path = qn("w:pPr") + "/" + qn("w:pStyle")
    level = 0
    for i, p in enumerate(iter_paragraphs(root)):
        style_el = p.find(pstyle_path)
        style = style_el.get(qn("w:val")) if style_el is not None else None
        text = para_text(p, "accept").strip()
        if style and known and style not in known:
            rep.f("styles_missing", "fail", "paragraph uses a style the document does not define").add(f"p{i} {style}")
        if p.find(qn("w:pPr") + "/" + qn("w:outlineLvl")) is not None:
            rep.f("headings_direct_outline", "fail",
                  "outline level applied as direct formatting; use a Heading style so the navigation pane and the TOC field see it").add(f"p{i}")
        match = HEADING_RE.match(style or "")
        if match:
            stats["headings"] += 1
            new = int(match.group(1))
            if new > level + 1:
                rep.f("headings_skip", "fail", "heading level skips a level; Word's TOC and navigation pane read the gap as a broken outline").add(f"p{i} Heading{level} -> Heading{new}")
            level = new
        elif text and not style:
            runs = [r for r in p.findall(qn("w:r"))]
            sizes = [int(sz.get(qn("w:val"))) for r in runs for sz in r.findall(qn("w:rPr") + "/" + qn("w:sz")) if sz.get(qn("w:val"))]
            bold = runs and all(on_off(r, qn("w:rPr") + "/" + qn("w:b")) for r in runs)
            if bold and sizes and max(sizes) >= 28 and len(text) < 120:
                rep.f("headings_faked", "fail",
                      "bold oversized body text used as a heading; apply Heading 1..3 instead").add(f"p{i} {text[:50]!r}")


def check_tables(pkg: Package, rep: Report, stats: dict) -> None:
    root = pkg.tree("word/document.xml")
    for t, (tbl, limit) in enumerate(tables_by_section(root)):
        stats["tables"] += 1
        rows = tbl.findall(qn("w:tr"))
        if not rows:
            continue
        if not on_off(rows[0], qn("w:trPr") + "/" + qn("w:tblHeader")):
            rep.f("tables_header_row", "warn",
                  "first row is not marked to repeat; set w:tblHeader so the header follows the table across a page break").add(f"table{t}")
        grid = [int(g.get(qn("w:w"))) for g in tbl.findall(qn("w:tblGrid") + "/" + qn("w:gridCol")) if g.get(qn("w:w"))]
        tblw = tbl.find(qn("w:tblPr") + "/" + qn("w:tblW"))
        kind = tblw.get(qn("w:type")) if tblw is not None else None
        if not grid and kind in (None, "auto"):
            rep.f("tables_widths", "warn", "table has no declared column widths; Word and LibreOffice will lay it out differently").add(f"table{t}")
        width = sum(grid) if grid else (int(tblw.get(qn("w:w"))) if kind == "dxa" and tblw.get(qn("w:w")) else None)
        if width and limit and width > limit + 20:
            rep.f("tables_overflow", "fail",
                  f"table is wider than the {limit} twip text area; columns will be clipped in print").add(f"table{t} {width} twips")


def _rpr_bytes(run) -> bytes:
    rpr = run.find(qn("w:rPr"))
    return b"" if rpr is None else etree.tostring(rpr)


def check_text(pkg: Package, rep: Report, stats: dict) -> None:
    for name in sorted(pkg.stories()):
        root = pkg.tree(name)
        for i, p in enumerate(iter_paragraphs(root)):
            text = para_text(p, "all")
            if text.strip():
                stats["paragraphs_with_text_all_parts"] += 1
            where = f"{Path(name).name} p{i}"
            for pattern, level in PLACEHOLDERS:
                m = pattern.search(text)
                if m:
                    rep.f("text_placeholder", level, "placeholder or tool token left in the document").add(f"{where} {m.group(0)!r}")
            for char, (level, message) in ODD_CHARS.items():
                if char in text:
                    rep.f("text_char_%04x" % ord(char), level, message).add(where)
            runs = p.findall(qn("w:r"))
            for a, b in zip(runs, runs[1:]):
                ta, tb = a.findtext(qn("w:t")) or "", b.findtext(qn("w:t")) or ""
                if not ta or not tb or not ta[-1].isalnum() or not tb[0].isalnum():
                    continue
                if _rpr_bytes(a) == _rpr_bytes(b):
                    rep.f("text_split_runs", "info",
                          "a word is split across two runs with the same formatting; merge them so search and text extraction see one word").add(where)


def check_fonts(pkg: Package, rep: Report, stats: dict) -> None:
    """Only fonts the document actually reaches.

    A stock Word template defines styles nothing uses (HTML Preformatted brings
    Courier along), and flagging those trains the agent to ignore the check.
    """
    names: set[str] = set()
    doc = pkg.tree("word/document.xml")
    used_styles = {el.get(qn("w:val")) for el in doc.iter()
                   if local(el) in ("pStyle", "rStyle", "tblStyle") and el.get(qn("w:val"))}
    for fonts in doc.iter(qn("w:rFonts")):
        for attr in ("w:ascii", "w:hAnsi", "w:cs"):
            if fonts.get(qn(attr)):
                names.add(fonts.get(qn(attr)))
    if pkg.has("word/styles.xml"):
        styles = pkg.tree("word/styles.xml")
        defaults = styles.find(qn("w:docDefaults"))
        scopes = [defaults] if defaults is not None else []
        scopes += [st for st in styles.findall(qn("w:style")) if st.get(qn("w:styleId")) in used_styles]
        for scope in scopes:
            for fonts in scope.iter(qn("w:rFonts")):
                for attr in ("w:ascii", "w:hAnsi", "w:cs"):
                    if fonts.get(qn(attr)):
                        names.add(fonts.get(qn(attr)))
    if pkg.has("word/theme/theme1.xml"):
        a = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
        for latin in pkg.tree("word/theme/theme1.xml").iter(a + "latin"):
            if latin.get("typeface"):
                names.add(latin.get("typeface"))
    stats["fonts"] = sorted(names)
    for name in sorted(names):
        if name.lower() not in METRIC_SAFE and not name.startswith("+"):
            rep.f("fonts_unsafe", "warn",
                  "font is outside the metric-safe set (Arial, Calibri, Cambria, Times New Roman, Georgia); "
                  "a reader without it gets different pagination from the render").add(name)


def check_fields(pkg: Package, rep: Report, stats: dict) -> None:
    root = pkg.tree("word/document.xml")
    instructions = " ".join((el.text or "") for el in root.iter(qn("w:instrText")))
    has_toc_field = "TOC" in instructions
    stats["toc_field"] = has_toc_field
    update = False
    if pkg.has("word/settings.xml"):
        el = pkg.tree("word/settings.xml").find(qn("w:updateFields"))
        update = el is not None and (el.get(qn("w:val")) or "true").lower() in ("1", "true", "on")
    if has_toc_field and not update:
        rep.f("fields_update", "warn",
              "TOC field present but settings.xml has no w:updateFields; the reader sees the placeholder until they press F9").add("word/settings.xml")
    if not has_toc_field:
        styled = [p for p in iter_paragraphs(root)
                  if (p.find(qn("w:pPr") + "/" + qn("w:pStyle")) is not None
                      and (p.find(qn("w:pPr") + "/" + qn("w:pStyle")).get(qn("w:val")) or "").upper().startswith("TOC"))]
        heading = [i for i, p in enumerate(iter_paragraphs(root))
                   if re.fullmatch(r"(table of )?contents", para_text(p, "accept").strip(), re.I)]
        if styled or heading:
            rep.f("fields_static_toc", "warn",
                  "contents list is typed text, not a TOC field; it will not renumber when the document changes").add(
                      f"{len(styled)} TOC-styled paragraphs" if styled else f"p{heading[0]}")


def _revisions(root) -> int:
    return sum(1 for el in root.iter()
               if local(el) in REVISION_TAGS or local(el).endswith("PrChange"))


def check_review(pkg: Package, rep: Report, stats: dict) -> None:
    revisions = sum(_revisions(pkg.tree(name)) for name in pkg.stories())
    stats["tracked_changes"] = revisions
    if revisions:
        rep.f("review_tracked_changes", "info",
              "unresolved tracked changes; deliberate when the human is reviewing, a defect when the document is final").add(f"{revisions} revisions")
    if pkg.has("word/comments.xml"):
        total = len(pkg.tree("word/comments.xml").findall(qn("w:comment")))
        done = 0
        if pkg.has("word/commentsExtended.xml"):
            done = sum(1 for e in pkg.tree("word/commentsExtended.xml").findall(qn("w15:commentEx"))
                       if (e.get(qn("w15:done")) or "0").lower() in ("1", "true", "on"))
        stats["comments"] = total
        stats["comments_resolved"] = done
        if total > done:
            rep.f("review_open_comments", "info", "comments still open; answer or resolve them before delivery").add(f"{total - done} of {total}")


def check_final(pkg: Package, rep: Report) -> None:
    """The same nodes check_review reports as context, read as defects.

    A document handed over as the clean copy is finished, so a surviving
    revision or comment is either an unaccepted edit or review traffic the
    reader was never meant to see.
    """
    for name in sorted(pkg.stories()):
        root = pkg.tree(name)
        revisions = _revisions(root)
        if revisions:
            rep.f("final_revisions", "fail",
                  "tracked changes in a document delivered as final; run redline.py accept or reject first").add(
                      f"{Path(name).name} {revisions} nodes")
        anchors = sum(1 for el in root.iter() if local(el) in COMMENT_ANCHORS)
        if anchors:
            rep.f("final_comments", "fail", FINAL_COMMENTS_MESSAGE).add(f"{Path(name).name} {anchors} anchors")
    for name in sorted(n for n in pkg.names() if COMMENT_PART_RE.match(n)):
        rep.f("final_comments", "fail", FINAL_COMMENTS_MESSAGE).add(f"{name} {len(pkg.tree(name))} nodes")


def validate(path: Path, final: bool = False) -> dict:
    with zipfile.ZipFile(path) as zf:
        bad = zf.testzip()
    if bad is not None:
        return {"status": "error", "file": str(path), "message": f"corrupt zip entry: {bad}"}
    pkg = Package(path)
    rep = Report()
    # Three paragraph counts on purpose: body only, body including table cells, and every
    # story part (headers, footers, notes) that carries text. They are not meant to reconcile.
    stats = {"body_paragraphs": 0, "body_paragraphs_incl_tables": 0, "paragraphs_with_text_all_parts": 0, "headings": 0, "tables": 0}
    try:
        import docx  # noqa: PLC0415 - opening with the library is itself the check

        stats["body_paragraphs"] = len(docx.Document(str(path)).paragraphs)
    except Exception as exc:  # noqa: BLE001
        rep.f("package_opens", "fail", f"python-docx cannot open the file: {exc}").add(str(path))
    check_package(pkg, rep)
    if pkg.has("word/document.xml"):
        stats["body_paragraphs_incl_tables"] = len(iter_paragraphs(pkg.tree("word/document.xml")))
        check_xml_order(pkg, rep)
        check_structure(pkg, rep, stats)
        check_tables(pkg, rep, stats)
        check_text(pkg, rep, stats)
        check_fonts(pkg, rep, stats)
        check_fields(pkg, rep, stats)
        check_review(pkg, rep, stats)
        if final:
            check_final(pkg, rep)
    findings = [f.as_dict() for f in rep.findings.values()]
    findings.sort(key=lambda d: {"fail": 0, "warn": 1, "info": 2}[d["level"]])
    fails = sum(1 for d in findings if d["level"] == "fail")
    return {"status": "fail" if fails else "pass", "file": str(path), "stats": stats, "findings": findings}


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args = [a for a in argv if not a.startswith("--")]
    if not args:
        print(json.dumps({"status": "error", "message": "usage: validate.py <file.docx> [--strict] [--final]"}))
        sys.exit(1)
    path = Path(args[0]).expanduser().resolve()
    if not path.exists():
        print(json.dumps({"status": "error", "message": f"no such file: {path}"}))
        sys.exit(1)
    try:
        report = validate(path, final="--final" in argv)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"status": "error", "file": str(path), "message": str(exc)}))
        sys.exit(1)
    print(json.dumps(report, indent=2))
    if report["status"] == "error":
        sys.exit(1)
    if "--strict" in argv and report["status"] == "fail":
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
