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

CHANGE_TAGS = {"pPrChange", "rPrChange", "tblPrChange", "trPrChange", "tcPrChange", "sectPrChange", "tblGridChange"}
MARKER_TAGS = {
    "moveFromRangeStart", "moveFromRangeEnd", "moveToRangeStart", "moveToRangeEnd",
    "customXmlInsRangeStart", "customXmlInsRangeEnd", "customXmlDelRangeStart", "customXmlDelRangeEnd",
    "customXmlMoveFromRangeStart", "customXmlMoveFromRangeEnd",
    "customXmlMoveToRangeStart", "customXmlMoveToRangeEnd", "numberingChange",
}
CELL_TAGS = {"cellIns", "cellDel", "cellMerge"}
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
    keep = parent.find(qn("w:rPr")) if local(parent) == "pPr" else None
    for child in list(parent):
        parent.remove(child)
    if old is not None:
        for child in list(old):
            parent.append(child)
    if keep is not None:
        parent.append(keep)


def merge_with_next(p) -> bool:
    """Join a paragraph to the following one, the way removing its mark would.

    The surviving paragraph mark is the next paragraph's, so its w:pPr wins.
    `getnext()` stays inside the same parent, so a merge never crosses a table
    cell or the end of the body.
    """
    nxt = p.getnext()
    while nxt is not None and local(nxt) != "p":
        nxt = nxt.getnext()
    if nxt is None:
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
    content, rows, marks, changes, markers, cells = [], [], [], [], [], []
    for el in root.iter():
        tag = local(el)
        if tag in TYPE_NAMES:
            bucket = {"content": content, "row": rows, "para-mark": marks}[kind_of(el)]
            bucket.append(el)
        elif tag in CHANGE_TAGS:
            changes.append(el)
        elif tag in MARKER_TAGS:
            markers.append(el)
        elif tag in CELL_TAGS:
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

    for el in cells:
        if not attached(el, root):
            continue
        tally("cell-change")
        warnings.append(f"{local(el)} marker dropped; cell-level revisions are reported, not replayed")
        drop(el)

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
        if inserted != accept and p is not None and not merge_with_next(p):
            warnings.append("a paragraph-mark revision at the end of a story could not be merged")

    for el in markers:
        if attached(el, root):
            drop(el)
    return counts, warnings


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
        for el in root.iter():
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
            elif tag in CELL_TAGS:
                label, text = "cell-change", ""
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
    del_ids, last_del = [], None
    for group in groups_of(covered):
        rid = ids.next()
        del_ids.append(rid)
        last_del = wrap_deleted(group, rid, author, date)
    ins_id = None
    if replacement:
        anchor = last_del
        while local(anchor.getparent()) == "ins":
            anchor = anchor.getparent()
        ins = etree.Element(qn("w:ins"))
        ins_id = ids.next()
        revision_attrs(ins, ins_id, author, date)
        ins.append(make_run(covered[0], replacement))
        anchor.addnext(ins)
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

def flag(argv: list[str], name: str, default=None):
    return argv[argv.index(name) + 1] if name in argv else default


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    if len(argv) < 2:
        fail("usage: redline.py report|accept|reject|replace|insert|delete <file.docx> [...]")
    action, path = argv[0], Path(argv[1]).expanduser().resolve()
    if not path.exists():
        fail(f"no such file: {path}")
    try:
        pkg = Package(path)
    except Exception as exc:  # noqa: BLE001 - the message is the diagnosis
        fail(f"cannot open {path}: {exc}")

    author = flag(argv, "--author", "LangAlpha")
    date = flag(argv, "--date", now_stamp())
    out = flag(argv, "--out")

    if action == "report":
        print(json.dumps(report(pkg, "--paragraphs" in argv), indent=2))
        return

    if action in ("accept", "reject"):
        counts, warnings = {}, []
        for name in sorted(pkg.stories()):
            c, w = resolve(pkg.touch(name), action == "accept")
            for k, v in c.items():
                counts[k] = counts.get(k, 0) + v
            warnings += w
        dst = Path(out) if out else path.with_name(f"{path.stem}_{action}ed.docx")
        pkg.save(dst)
        print(json.dumps({"status": "ok", "action": action, "file": str(path), "out": str(dst),
                          "resolved": sum(counts.values()), "by_type": counts, "warnings": warnings}, indent=2))
        return

    root = pkg.touch("word/document.xml")
    paras = iter_paragraphs(root)
    ids = Ids(max_revision_id(pkg))
    result: dict

    if action == "replace":
        find, new = flag(argv, "--find"), flag(argv, "--with", "")
        if not find:
            fail("replace needs --find \"old text\"")
        only = flag(argv, "--paragraph")
        candidates = [(int(only), paras[int(only)])] if only is not None else list(enumerate(paras))
        if only is not None and not 0 <= int(only) < len(paras):
            fail(f"--paragraph {only} out of range (0..{len(paras) - 1})")
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
                if "--all" not in argv:
                    break
            if edits and "--all" not in argv:
                break
        if not edits:
            fail(f"text not found: {find!r}")
        result = {"action": "replace", "count": len(edits), "edits": edits}

    elif action == "insert":
        n, text = flag(argv, "--after-paragraph"), flag(argv, "--text")
        if n is None or text is None:
            fail("insert needs --after-paragraph N and --text \"...\"")
        if not 0 <= int(n) < len(paras):
            fail(f"--after-paragraph {n} out of range (0..{len(paras) - 1})")
        ref = paras[int(n)]
        new_p, marks = build_inserted_paragraph(ref, text, flag(argv, "--style"), ids, author, date)
        ref.addnext(new_p)
        result = {"action": "insert", "after_paragraph": int(n), "text": text, **marks}

    elif action == "delete":
        n = flag(argv, "--paragraph")
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
