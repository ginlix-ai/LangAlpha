#!/usr/bin/env python3
"""Read and write Word tracked changes.

Usage:
    python redline.py report  <file.docx> [--paragraphs]
    python redline.py accept  <file.docx> [--out FILE]
    python redline.py reject  <file.docx> [--out FILE]
    python redline.py replace <file.docx> --find "old" --with "new"
                              [--paragraph N] [--all] [--author A] [--date D] [--out FILE]
    python redline.py insert  <file.docx> --after-paragraph N --text "..."
                              [--style NAME] [--author A] [--date D] [--out FILE]
    python redline.py delete  <file.docx> --paragraph N [--author A] [--date D] [--out FILE]

`replace`, `insert` and `delete` edit the file in place unless --out is given:
the point of a tracked change is that one document accumulates the review.
`accept` and `reject` always write a copy, defaulting to <stem>_accepted.docx
and <stem>_rejected.docx, because resolving a revision is destructive.

Paragraph indices count every w:p in document order, tables included, which is
`report --paragraphs`' numbering and not python-docx's `document.paragraphs`.
Revisions in headers, footers, footnotes and endnotes are reported and resolved
along with the body.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from lxml import etree

from docx_parts import (
    Package,
    editable_runs,
    is_element,
    iter_elements,
    iter_paragraphs,
    local,
    make_run,
    max_revision_id,
    now_stamp,
    para_text,
    qn,
    revision_attrs,
    split_run,
    wrap_deleted,
)

# The whole set: w:tblPrExChange records a row's own table-property exception and
# w:tblGridChange does not end in PrChange, so neither turns up by pattern. One left
# out resolves as nothing and surfaces only when validate.py --final reads the copy.
CHANGE_TAGS = {"pPrChange", "rPrChange", "tblPrChange", "tblPrExChange", "trPrChange",
               "tcPrChange", "sectPrChange", "tblGridChange"}
MARKER_TAGS = {
    "moveFromRangeStart", "moveFromRangeEnd", "moveToRangeStart", "moveToRangeEnd",
    "customXmlInsRangeStart", "customXmlInsRangeEnd", "customXmlDelRangeStart", "customXmlDelRangeEnd",
    "customXmlMoveFromRangeStart", "customXmlMoveFromRangeEnd",
    "customXmlMoveToRangeStart", "customXmlMoveToRangeEnd", "numberingChange",
}
CELL_LABELS = {"cellIns": "cell-insert", "cellDel": "cell-delete", "cellMerge": "cell-merge"}
# CT_Body and CT_Tc let these sit between two paragraphs, and each is a marker
# with no content of its own, so a paragraph mark deleted in front of one still
# joins the paragraph beyond it. Anything else in between - a w:tbl, w:sdt,
# w:sectPr, a block-level w:customXml - is content, and merging across it would
# move prose over the block.
BETWEEN_PARAGRAPHS = {
    "bookmarkStart", "bookmarkEnd", "commentRangeStart", "commentRangeEnd",
    "permStart", "permEnd", "proofErr",
    "moveFromRangeStart", "moveFromRangeEnd", "moveToRangeStart", "moveToRangeEnd",
    "customXmlInsRangeStart", "customXmlInsRangeEnd",
    "customXmlDelRangeStart", "customXmlDelRangeEnd",
    "customXmlMoveFromRangeStart", "customXmlMoveFromRangeEnd",
    "customXmlMoveToRangeStart", "customXmlMoveToRangeEnd",
}
# True where the cell exists only after the change, so `accept` keeps it and
# `reject` takes it away; cellMerge is absent because neither side of a merge
# can be replayed from what the file still holds (see cell_merges).
CELL_EXISTS_AFTER = {"cellIns": True, "cellDel": False}
# A *Change record saves the base property type only, so these children are
# never inside it and have to outlive a restore: the paragraph mark's w:rPr, a
# section's w:sectPr and its header and footer references, and the w:ins or
# w:del marking the paragraph or row itself. Each entry is (leading, trailing)
# so a survivor goes back on the side of the restored properties the ECMA-376
# sequence puts it on.
MARK_TAGS = ("ins", "del", "moveFrom", "moveTo")
SURVIVES_RESTORE = {
    "pPr": ((), ("rPr", "sectPr")),
    "rPr": (MARK_TAGS, ()),
    "trPr": ((), MARK_TAGS),
    # A w:cellIns/w:cellDel is a pending revision of its own rather than a property, so it has to
    # survive a w:tcPrChange restore for the cells pass to still find the cell it marks.
    "tcPr": ((), ("cellIns", "cellDel", "cellMerge")),
    "sectPr": (("headerReference", "footerReference"), ()),
}
TYPE_NAMES = {"ins": "insert", "del": "delete", "moveTo": "move-to", "moveFrom": "move-from"}
PSTYLE = qn("w:pPr") + "/" + qn("w:pStyle")


def fail(message: str) -> None:
    print(json.dumps({"status": "error", "message": message}))
    sys.exit(1)


def kind_of(el) -> str:
    parent = el.getparent()
    if parent is None:
        return "content"
    ptag = local(parent)
    if ptag == "rPr":
        grand = parent.getparent()
        if grand is not None and local(grand) == "pPr":
            return "para-mark"
    if ptag == "trPr":
        return "row"
    if ptag == "numPr" and local(el) == "ins":
        # w:ins is the only track-change child CT_NumPr defines. Anything else
        # under w:numPr is malformed, and guessing at its side is worse than
        # leaving it to the content path.
        return "numbering"
    return "content"


def attached(el, root) -> bool:
    node = el
    while node is not None:
        if node is root:
            return True
        node = node.getparent()
    return False


def nearest(el, tag: str):
    node = el
    while node is not None:
        if local(node) == tag:
            return node
        node = node.getparent()
    return None


def unwrap(el) -> None:
    parent = el.getparent()
    index = parent.index(el)
    for offset, child in enumerate(list(el)):
        parent.insert(index + offset, child)
    parent.remove(el)


def drop(el) -> None:
    parent = el.getparent()
    if parent is not None:
        parent.remove(el)


def restore(change) -> None:
    """Put the pre-change properties back and discard the current ones."""
    parent = change.getparent()
    old = next(iter(change), None)
    lead, trail = SURVIVES_RESTORE.get(local(parent), ((), ()))
    survives = set(lead) | set(trail)
    keep = [el for el in parent if local(el) in survives]
    for child in list(parent):
        parent.remove(child)
    for el in keep:
        if local(el) in lead:
            parent.append(el)
    if old is not None:
        # A record holding one of the survivors is malformed, and the current
        # element keeps the only copy rather than ending up with two.
        for child in list(old):
            if local(child) not in survives:
                parent.append(child)
    for el in keep:
        if local(el) in trail:
            parent.append(el)


def next_block(p):
    """The next sibling that is content, past the markers Word puts between paragraphs.

    An XML comment or processing instruction is not content either, so it is
    stepped over with them rather than walling the paragraph off from the one
    beyond it.
    """
    nxt = p.getnext()
    while nxt is not None and (not is_element(nxt) or local(nxt) in BETWEEN_PARAGRAPHS):
        nxt = nxt.getnext()
    return nxt


def bare_paragraph(p) -> bool:
    """True where nothing is left of a paragraph but the paragraph mark's own properties.

    A w:sectPr in the w:pPr is not nothing: it carries the page setup for every
    paragraph before it, so a paragraph holding one is a section break rather
    than a line of prose, and it stays whatever else was taken out of it.
    """
    if any(is_element(child) and local(child) != "pPr" for child in p):
        return False
    ppr = p.find(qn("w:pPr"))
    return ppr is None or ppr.find(qn("w:sectPr")) is None


def sole_paragraph(p) -> bool:
    """True where p is the only w:p its body or table cell has.

    Word keeps at least one either way: a w:tc with no w:p is invalid and a
    story has to end on a paragraph, so this is the one a resolve cannot take
    away however little of it survived.
    """
    parent = p.getparent()
    return parent is None or len(parent.findall(qn("w:p"))) <= 1


def merge_with_next(p) -> bool:
    """Join a paragraph to the following one, the way removing its mark would.

    The surviving paragraph mark is the next paragraph's, so its w:pPr wins.
    Only a paragraph can absorb the content: `getnext()` stays inside the same
    parent, so a merge never leaves a table cell or the body, and a w:tbl,
    w:sdt or w:sectPr in between is a wall rather than something to search
    past, because reaching the paragraph beyond it would lift that paragraph's
    prose over the block and out of reading order.
    """
    nxt = next_block(p)
    if nxt is None or local(nxt) != "p":
        return False
    ppr_p = p.find(qn("w:pPr"))
    ppr_n = nxt.find(qn("w:pPr"))
    for child in list(nxt):
        if child is ppr_n:
            continue
        p.append(child)
    if ppr_p is not None:
        p.remove(ppr_p)
    if ppr_n is not None:
        p.insert(0, ppr_n)
    drop(nxt)
    return True


def resolve(root, accept: bool) -> tuple[dict, list[str]]:
    content, rows, marks, numbering = [], [], [], []
    changes, markers, cells = [], [], []
    for el in iter_elements(root):
        tag = local(el)
        if tag in TYPE_NAMES:
            bucket = {"content": content, "row": rows, "para-mark": marks,
                      "numbering": numbering}[kind_of(el)]
            bucket.append(el)
        elif tag in CHANGE_TAGS:
            changes.append(el)
        elif tag in MARKER_TAGS:
            markers.append(el)
        elif tag in CELL_EXISTS_AFTER:
            cells.append(el)

    counts: dict[str, int] = {}
    warnings: list[str] = []

    def tally(name: str) -> None:
        counts[name] = counts.get(name, 0) + 1

    for el in content:
        if not attached(el, root):
            continue
        inserted = local(el) in ("ins", "moveTo")
        tally(TYPE_NAMES[local(el)])
        if inserted == accept:
            if not inserted:
                for t in el.iter(qn("w:delText")):
                    t.tag = qn("w:t")
            unwrap(el)
        else:
            drop(el)

    for el in changes:
        if not attached(el, root):
            continue
        tally("format-change")
        drop(el) if accept else restore(el)

    for el in numbering:
        # Detached means a w:pPrChange on the same paragraph has already put the
        # previous w:pPr back, and that record outranks this marker.
        if not attached(el, root):
            continue
        tally("numbering-insert")
        # The revision records numbering applied to a paragraph that had none,
        # which is the shape of ECMA-376's own example, so a reject owes the
        # reader the whole w:numPr back. Dropping only the marker would leave
        # the paragraph numbered with nothing left to say anyone asked for it.
        drop(el) if accept else drop(el.getparent())

    for el in cells:
        if not attached(el, root):
            continue
        inserted = CELL_EXISTS_AFTER[local(el)]
        tally(CELL_LABELS[local(el)])
        if inserted == accept:
            drop(el)
            continue
        # The losing side of a cell revision is the cell, not just the marker:
        # dropping the marker alone leaves the column the reviewer added, or the
        # one they deleted, standing in the resolved copy. A row left with no
        # cells goes too. w:tblGrid is not pruned to match: a pending column
        # delete carries no tblGridChange to restore, and dropping a gridCol
        # safely means resolving gridSpan across every row of the table.
        cell = nearest(el, "tc")
        if cell is None:  # only valid inside a w:tcPr; a stray marker is all there is to take
            drop(el)
            continue
        row = nearest(cell, "tr")
        drop(cell)
        if row is not None and not row.findall(qn("w:tc")):
            drop(row)

    for el in rows:
        if not attached(el, root):
            continue
        inserted = local(el) in ("ins", "moveTo")
        tally("row-" + ("insert" if inserted else "delete"))
        drop(el) if inserted == accept else drop(el.getparent().getparent())

    for el in marks:
        if not attached(el, root):
            continue
        inserted = local(el) in ("ins", "moveTo")
        tally("paragraph-mark-" + ("insert" if inserted else "delete"))
        p = nearest(el, "p")
        drop(el)
        if inserted == accept or p is None or merge_with_next(p):
            continue
        # Nothing here can absorb the paragraph: Word will not merge one into a
        # table, and at the end of a cell or a story there is no next paragraph
        # at all.
        if inserted and bare_paragraph(p) and not sole_paragraph(p):
            # The paragraph was wholly the reviewer's - inserted mark, and
            # content that has just gone with the reject - so there is nothing
            # of the original to preserve and leaving it adds a blank line the
            # document never had. Where it is all its container has left it
            # stays behind instead, emptied, and falls through to the warning.
            drop(p)
            continue
        # The marker is gone and the paragraph stays whole, which is the only
        # reading-order-preserving answer, so the report names it.
        nxt = next_block(p)
        blocked = f"a w:{local(nxt)} follows it" if nxt is not None else "it ends its story or table cell"
        warnings.append(
            f"paragraph mark revision resolved but {blocked}, so the paragraph was left in place: "
            f"{para_text(p, 'accept')[:60]!r}"
        )

    for el in markers:
        if attached(el, root):
            drop(el)
    return counts, warnings


def cell_merges(pkg: Package) -> list[str]:
    """Where every w:cellMerge sits, as `part tableN rowM cellK` labels.

    A merge is the one cell revision neither side can replay: joining cells
    deletes the ones absorbed, so the file no longer holds the widths and
    content a reject would have to put back, and accept cannot tell a merge
    already applied from one still pending.
    """
    found: list[str] = []
    for name in sorted(pkg.stories()):
        for t, tbl in enumerate(pkg.tree(name).iter(qn("w:tbl"))):
            for r, tr in enumerate(tbl.findall(qn("w:tr"))):
                for c, tc in enumerate(tr.findall(qn("w:tc"))):
                    pr = tc.find(qn("w:tcPr"))
                    if pr is not None and pr.find(qn("w:cellMerge")) is not None:
                        found.append(f"{name} table{t} row{r} cell{c}")
    return found


def numbering_changes(pkg: Package) -> list[str]:
    """Where every unrecoverable w:numberingChange sits, as `part pN` labels.

    The element is empty, and its w:original attribute caches the shape of the
    previous numbering (per level, `%level:value:format:separator`) rather than
    the w:numId and w:ilvl a reject would have to put back. So the prior
    numbering survives only where the paragraph also carries a w:pPrChange,
    which holds the whole previous w:pPr and is what ECMA-376 tells a producer
    to write instead; numberingChange is the legacy fallback.
    """
    found: list[str] = []
    for name in sorted(pkg.stories()):
        root = pkg.tree(name)
        paragraphs = iter_paragraphs(root)  # held so the id() keys stay live
        index = {id(p): i for i, p in enumerate(paragraphs)}
        for el in root.iter(qn("w:numberingChange")):
            if nearest(el, "pPrChange") is not None:
                continue  # part of a saved w:pPr, so resolving that record settles it
            ppr = nearest(el, "pPr")
            if ppr is not None and ppr.find(qn("w:pPrChange")) is not None:
                continue
            p = nearest(el, "p")
            found.append(f"{name} p{index[id(p)]}" if p is not None else name)
    return found


# -- report ---------------------------------------------------------------

def report(pkg: Package, want_paragraphs: bool) -> dict:
    revisions, authors = [], {}
    paragraphs = []
    for name in sorted(pkg.stories()):
        root = pkg.tree(name)
        if want_paragraphs and name == "word/document.xml":
            for i, p in enumerate(iter_paragraphs(root)):
                style = p.find(PSTYLE)
                paragraphs.append({
                    "index": i,
                    "style": style.get(qn("w:val")) if style is not None else None,
                    "text": para_text(p, "accept")[:200],
                })
        # One document-order walk counts paragraphs as it goes. lxml hands out
        # transient element proxies, so an identity map keyed on the elements
        # themselves silently mismatches once a proxy is collected.
        seen = -1
        for el in iter_elements(root):
            tag = local(el)
            if tag == "p":
                seen += 1
                continue
            at = seen
            if tag in TYPE_NAMES:
                kind = kind_of(el)
                if kind == "para-mark":
                    label = "paragraph-mark-" + ("insert" if tag in ("ins", "moveTo") else "delete")
                    text = ""
                elif kind == "numbering":
                    label, text = "numbering-insert", ""
                elif kind == "row":
                    label = "row-" + ("insert" if tag in ("ins", "moveTo") else "delete")
                    row = el.getparent().getparent()
                    text = " | ".join(para_text(p, "all") for p in iter_paragraphs(row))
                    at = seen + 1
                else:
                    label = TYPE_NAMES[tag]
                    text = para_text(el, "all")
            elif tag in CHANGE_TAGS:
                label, text = "format-change", ""
            elif tag in CELL_LABELS:
                label, text = CELL_LABELS[tag], ""
            elif tag == "numberingChange":
                label, text = "numbering-change", ""
            else:
                continue
            author = el.get(qn("w:author")) or ""
            authors[author] = authors.get(author, 0) + 1
            revisions.append({
                "part": name,
                "id": el.get(qn("w:id")),
                "type": label,
                "element": tag,
                "author": author,
                "date": el.get(qn("w:date")),
                "paragraph": at if at >= 0 else None,
                "text": text,
            })
    counts: dict[str, int] = {}
    for rev in revisions:
        counts[rev["type"]] = counts.get(rev["type"], 0) + 1
    out = {
        "status": "ok",
        "action": "report",
        "file": str(pkg.path),
        "total": len(revisions),
        "counts": counts,
        "authors": authors,
        "revisions": revisions,
    }
    if want_paragraphs:
        out["paragraphs"] = paragraphs
    return out


# -- tracked edits --------------------------------------------------------

class Ids:
    def __init__(self, start: int):
        self.n = start

    def next(self) -> int:
        self.n += 1
        return self.n


def groups_of(runs: list) -> list[list]:
    """Contiguous same-parent run groups, so one w:del never spans two parents."""
    out: list[list] = []
    for r in runs:
        if out and out[-1][-1].getparent() is r.getparent() and out[-1][-1].getnext() is r:
            out[-1].append(r)
        else:
            out.append([r])
    return out


def split_past_ins(el, ids: Ids):
    """A w:del lifted out of the w:ins wrappers around it, which hold no place of their own.

    A wrapper that runs on past the deletion is cut in two first, so that it
    does end there: its remainder moves into a w:ins of its own, keeping the
    earlier reviewer's author and date and taking a fresh w:id, placed straight
    after the one being anchored on. Skip the cut and the replacement is written
    after the whole of that earlier insertion, so accepting the change reads its
    tail ahead of the new text.
    """
    while local(el.getparent()) == "ins":
        ins = el.getparent()
        children = list(ins)
        tail = children[children.index(el) + 1:]
        if tail:
            rest = etree.Element(ins.tag, nsmap=ins.nsmap)
            for k, v in ins.attrib.items():
                rest.set(k, v)
            rest.set(qn("w:id"), str(ids.next()))
            for child in tail:
                rest.append(child)
            ins.addnext(rest)
        el = ins
    return el


def insertion_anchor(p, dels: list, ids: Ids):
    """The element a replacement's w:ins goes after, given the w:del elements it made.

    Beside the deletion while they all share a parent, so a phrase wholly
    inside a w:hyperlink keeps its replacement inside the link. A phrase that
    crossed a container anchors in the paragraph instead, after the last
    deletion sitting there or else after the first deletion's container:
    anchoring on the last deletion puts the new text inside a trailing
    hyperlink that only the tail of the match was in, and a replacement for
    half-unlinked text comes out clickable. Every run between the first and the
    last deletion is deleted, so both paragraph-level points read the same once
    the change is applied.
    """
    anchors = [split_past_ins(d, ids) for d in dels]
    parent = anchors[0].getparent()
    if all(a.getparent() is parent for a in anchors):
        return anchors[-1]
    in_paragraph = [a for a in anchors if a.getparent() is p]
    if in_paragraph:
        return in_paragraph[-1]
    anchor = anchors[0]
    while anchor.getparent() is not p and anchor.getparent() is not None:
        anchor = anchor.getparent()
    return anchor


def mark_replace(p, start: int, end: int, replacement: str, ids: Ids, author: str, date: str) -> dict:
    for r, s, e in editable_runs(p):
        if s < end < e:
            split_run(r, end - s)
            break
    for r, s, e in editable_runs(p):
        if s < start < e:
            split_run(r, start - s)
            break
    covered = [r for r, s, e in editable_runs(p) if s >= start and e <= end and e > s]
    if not covered:
        raise ValueError("the matched text resolved to no runs")
    del_ids, dels = [], []
    for group in groups_of(covered):
        rid = ids.next()
        del_ids.append(rid)
        dels.append(wrap_deleted(group, rid, author, date))
    ins_id = None
    if replacement:
        ins = etree.Element(qn("w:ins"))
        ins_id = ids.next()
        revision_attrs(ins, ins_id, author, date)
        ins.append(make_run(covered[0], replacement))
        insertion_anchor(p, dels, ids).addnext(ins)
    return {"del_ids": del_ids, "ins_id": ins_id}


def para_rpr(p):
    """The paragraph mark's run properties, created in schema order if absent."""
    ppr = p.find(qn("w:pPr"))
    if ppr is None:
        ppr = etree.Element(qn("w:pPr"))
        p.insert(0, ppr)
    rpr = ppr.find(qn("w:rPr"))
    if rpr is None:
        rpr = etree.Element(qn("w:rPr"))
        tail = ppr.find(qn("w:sectPr"))
        if tail is None:
            tail = ppr.find(qn("w:pPrChange"))
        if tail is None:
            ppr.append(rpr)
        else:
            ppr.insert(ppr.index(tail), rpr)
    return rpr


def mark_paragraph_deleted(p, ids: Ids, author: str, date: str) -> dict:
    del_ids = []
    for group in groups_of([r for r, _s, _e in editable_runs(p)]):
        rid = ids.next()
        del_ids.append(rid)
        wrap_deleted(group, rid, author, date)
    marker = etree.Element(qn("w:del"))
    mark_id = ids.next()
    revision_attrs(marker, mark_id, author, date)
    para_rpr(p).insert(0, marker)
    return {"del_ids": del_ids, "mark_id": mark_id}


def build_inserted_paragraph(ref, text: str, style: str | None, ids: Ids, author: str, date: str):
    p = etree.Element(qn("w:p"))
    ppr = etree.SubElement(p, qn("w:pPr"))
    if style is None:
        ref_style = ref.find(PSTYLE)
        value = ref_style.get(qn("w:val")) if ref_style is not None else None
        style = value if value and not value.lower().startswith("heading") else None
    if style:
        etree.SubElement(ppr, qn("w:pStyle")).set(qn("w:val"), style)
    rpr = etree.SubElement(ppr, qn("w:rPr"))
    mark = etree.SubElement(rpr, qn("w:ins"))
    mark_id = ids.next()
    revision_attrs(mark, mark_id, author, date)
    ins = etree.SubElement(p, qn("w:ins"))
    ins_id = ids.next()
    revision_attrs(ins, ins_id, author, date)
    template = next((r for r, _s, _e in editable_runs(ref)), None)
    ins.append(make_run(template, text))
    return p, {"ins_id": ins_id, "mark_id": mark_id, "style": style}


# -- cli ------------------------------------------------------------------

USAGE = "usage: redline.py report|accept|reject|replace|insert|delete <file.docx> [...]; --help for the flags"
TAKES_VALUE = {
    "--paragraphs": False, "--all": False, "--out": True, "--find": True, "--with": True,
    "--paragraph": True, "--after-paragraph": True, "--text": True, "--style": True,
    "--author": True, "--date": True,
}


def parse_args(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token TAKES_VALUE does not name.

    A misspelt --out used to fall through unread, so `replace --uot copy.docx`
    rewrote in place the document the caller was trying to leave untouched, and
    said ok.
    """
    args: list[str] = []
    flags: dict[str, str | bool] = {}
    i = 0
    while i < len(argv):
        token = argv[i]
        if not token.startswith("--"):
            args.append(token)
            i += 1
        elif token not in TAKES_VALUE:
            fail(f"unknown option: {token}; {USAGE}")
        elif token in flags:
            fail(f"repeated option: {token}; {USAGE}")
        elif not TAKES_VALUE[token]:
            flags[token] = True
            i += 1
        elif i + 1 >= len(argv) or argv[i + 1].startswith("--"):
            fail(f"{token} requires a value; {USAGE}")
        else:
            flags[token] = argv[i + 1]
            i += 2
    return args, flags


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args, flags = parse_args(argv)
    if len(args) < 2:
        fail(USAGE)
    action, path = args[0], Path(args[1]).expanduser().resolve()
    if not path.exists():
        fail(f"no such file: {path}")
    try:
        pkg = Package(path)
    except Exception as exc:  # noqa: BLE001 - the message is the diagnosis
        fail(f"cannot open {path}: {exc}")

    author = flags.get("--author", "LangAlpha")
    date = flags.get("--date", now_stamp())
    out = flags.get("--out")

    if action == "report":
        print(json.dumps(report(pkg, "--paragraphs" in flags), indent=2))
        return

    if action in ("accept", "reject"):
        dst = Path(out).expanduser().resolve() if out else path.with_name(f"{path.stem}_{action}ed.docx")
        # `path` is resolved too, so this also catches a relative spelling of the
        # input and a symlink pointing at it. Saving there would leave the
        # resolved document where the tracked one was, and the revisions are
        # then gone from the only file that held them.
        if dst == path:
            fail(f"--out names the input file; {action} writes a copy, so the revisions survive it. "
                 f"Give --out a different path than {path}")
        merged = cell_merges(pkg)
        if merged:
            fail(f"cannot {action} a tracked cell merge; resolve it in Word, then rerun. "
                 f"cellMerge at: {', '.join(merged)}")
        renumbered = numbering_changes(pkg) if action == "reject" else []
        if renumbered:
            fail("cannot reject a tracked numbering change; w:numberingChange records what the "
                 "previous numbering looked like, not the w:numId and w:ilvl that would put it "
                 f"back, so resolve it in Word, then rerun. numberingChange at: {', '.join(renumbered)}")
        counts, warnings = {}, []
        for name in sorted(pkg.stories()):
            c, w = resolve(pkg.touch(name), action == "accept")
            for k, v in c.items():
                counts[k] = counts.get(k, 0) + v
            warnings += w
        pkg.save(dst)
        print(json.dumps({"status": "ok", "action": action, "file": str(path), "out": str(dst),
                          "resolved": sum(counts.values()), "by_type": counts, "warnings": warnings}, indent=2))
        return

    root = pkg.touch("word/document.xml")
    paras = iter_paragraphs(root)
    ids = Ids(max_revision_id(pkg))
    result: dict

    if action == "replace":
        find, new = flags.get("--find"), flags.get("--with", "")
        if not find:
            fail("replace needs --find \"old text\"")
        only = flags.get("--paragraph")
        # Before the lookup: an out-of-range index raises IndexError there, and a
        # traceback is not a diagnosis the caller can act on.
        if only is not None and not 0 <= int(only) < len(paras):
            fail(f"--paragraph {only} out of range (0..{len(paras) - 1})")
        candidates = [(int(only), paras[int(only)])] if only is not None else list(enumerate(paras))
        edits = []
        for index, p in candidates:
            cursor = 0
            while True:
                text = para_text(p, "accept")
                at = text.find(find, cursor)
                if at < 0:
                    break
                try:
                    marks = mark_replace(p, at, at + len(find), new, ids, author, date)
                except ValueError as exc:
                    fail(str(exc))
                edits.append({"paragraph": index, "find": find, "with": new, **marks})
                cursor = at + len(new)
                if "--all" not in flags:
                    break
            if edits and "--all" not in flags:
                break
        if not edits:
            fail(f"text not found: {find!r}")
        result = {"action": "replace", "count": len(edits), "edits": edits}

    elif action == "insert":
        n, text = flags.get("--after-paragraph"), flags.get("--text")
        if n is None or text is None:
            fail("insert needs --after-paragraph N and --text \"...\"")
        if not 0 <= int(n) < len(paras):
            fail(f"--after-paragraph {n} out of range (0..{len(paras) - 1})")
        ref = paras[int(n)]
        new_p, marks = build_inserted_paragraph(ref, text, flags.get("--style"), ids, author, date)
        ref.addnext(new_p)
        result = {"action": "insert", "after_paragraph": int(n), "text": text, **marks}

    elif action == "delete":
        n = flags.get("--paragraph")
        if n is None:
            fail("delete needs --paragraph N")
        if not 0 <= int(n) < len(paras):
            fail(f"--paragraph {n} out of range (0..{len(paras) - 1})")
        p = paras[int(n)]
        marks = mark_paragraph_deleted(p, ids, author, date)
        result = {"action": "delete", "paragraph": int(n), "text": para_text(p, "all"), **marks}

    else:
        fail(f"unknown action: {action}")
        return

    dst = pkg.save(Path(out) if out else path)
    print(json.dumps({"status": "ok", "file": str(path), "out": str(dst), "author": author, "date": date, **result}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
