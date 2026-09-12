#!/usr/bin/env python3
"""Read and write Word comment threads.

Usage:
    python comments.py list    <file.docx>
    python comments.py add     <file.docx> --paragraph N --text "..."
                               [--find "anchor text"] [--author A] [--initials XX] [--date D] [--out FILE]
    python comments.py reply   <file.docx> --to ID --text "..." [--author A] [--initials XX] [--date D] [--out FILE]
    python comments.py resolve <file.docx> ID | --id ID [--out FILE]
    python comments.py reopen  <file.docx> ID | --id ID [--out FILE]
    python comments.py strip   <file.docx> [--out FILE]

Word keeps a comment's body in word/comments.xml and its thread state (who a
reply answers, whether the thread is resolved) in word/commentsExtended.xml,
keyed by the w14:paraId of the comment's paragraph. Writing one without the
other gives you comments Word shows as a flat, never-resolvable list, so these
commands always maintain both, and backfill a paraId onto comments written by a
tool that omitted it.

Commands write in place unless --out is given. `resolve` marks a thread done
and `reopen` clears it, both across every reply in the thread; `strip` removes
every comment and its anchors, for a final copy. Paragraph indices are
redline.py's numbering (every w:p in document order, tables included).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from lxml import etree

from docx_parts import (
    CT_COMMENTS,
    CT_COMMENTS_EXTENDED,
    NS,
    RT_COMMENTS,
    RT_COMMENTS_EXTENDED,
    Package,
    editable_runs,
    ensure_nsmap,
    iter_paragraphs,
    local,
    make_run,
    new_para_id,
    now_stamp,
    para_text,
    qn,
    split_run,
    used_para_ids,
)

DOC = "word/document.xml"
COMMENTS = "word/comments.xml"
EXTENDED = "word/commentsExtended.xml"
IDS = "word/commentsIds.xml"
EXTENSIBLE = "word/commentsExtensible.xml"

# Every part a comment can live in, and the elements that anchor one in the text.
COMMENT_PARTS = (COMMENTS, EXTENDED, IDS, EXTENSIBLE)
ANCHOR_TAGS = ("commentRangeStart", "commentRangeEnd", "commentReference")


def fail(message: str) -> None:
    print(json.dumps({"status": "error", "message": message}))
    sys.exit(1)


def flag(argv: list[str], name: str, default=None):
    return argv[argv.index(name) + 1] if name in argv else default


def initials_for(name: str) -> str:
    words = name.split()
    if len(words) > 1:
        return "".join(w[0] for w in words[:2]).upper()
    return (words[0][:2].upper() if words else "LA")


# -- parts ----------------------------------------------------------------

def comments_root(pkg: Package, create: bool):
    if pkg.has(COMMENTS):
        root = ensure_nsmap(pkg.touch(COMMENTS), ["w", "w14"])
        pkg.set_tree(COMMENTS, root)
        return root
    if not create:
        return None
    root = etree.Element(qn("w:comments"), nsmap={"w": NS["w"], "w14": NS["w14"], "mc": NS["mc"]})
    root.set(qn("mc:Ignorable"), "w14")
    pkg.set_tree(COMMENTS, root)
    pkg.ensure_override(COMMENTS, CT_COMMENTS)
    pkg.ensure_rel(DOC, RT_COMMENTS, "comments.xml")
    return root


def extended_root(pkg: Package, create: bool):
    if pkg.has(EXTENDED):
        root = ensure_nsmap(pkg.touch(EXTENDED), ["w15"])
        pkg.set_tree(EXTENDED, root)
        return root
    if not create:
        return None
    root = etree.Element(qn("w15:commentsEx"), nsmap={"w": NS["w"], "w14": NS["w14"], "w15": NS["w15"], "mc": NS["mc"]})
    root.set(qn("mc:Ignorable"), "w14 w15")
    pkg.set_tree(EXTENDED, root)
    pkg.ensure_override(EXTENDED, CT_COMMENTS_EXTENDED)
    pkg.ensure_rel(DOC, RT_COMMENTS_EXTENDED, "commentsExtended.xml")
    return root


def comment_elements(root) -> list:
    return [] if root is None else root.findall(qn("w:comment"))


def comment_text(comment) -> str:
    out = []
    for el in comment.iter():
        if local(el) in ("t", "delText"):
            out.append(el.text or "")
        elif local(el) == "tab":
            out.append("\t")
    return "".join(out)


def ensure_para_id(comment, taken: set[str]) -> str:
    """Every comment needs a paraId before it can be threaded or resolved."""
    paras = comment.findall(qn("w:p"))
    if not paras:
        para = etree.SubElement(comment, qn("w:p"))
        paras = [para]
    last = paras[-1]
    value = last.get(qn("w14:paraId"))
    if not value:
        value = new_para_id(taken)
        last.set(qn("w14:paraId"), value)
    return value.upper()


def ex_entry(ex_root, para_id: str, create: bool = True):
    for entry in ex_root.findall(qn("w15:commentEx")):
        if (entry.get(qn("w15:paraId")) or "").upper() == para_id.upper():
            return entry
    if not create:
        return None
    entry = etree.SubElement(ex_root, qn("w15:commentEx"))
    entry.set(qn("w15:paraId"), para_id)
    entry.set(qn("w15:done"), "0")
    return entry


def register_durable_id(pkg: Package, para_id: str, taken: set[str]) -> None:
    """Keep word/commentsIds.xml consistent when the document already has one.

    The part is optional and Word regenerates it, so it is never created from
    scratch here: an absent part is safer than a synthesised one.
    """
    if not pkg.has(IDS):
        return
    root = ensure_nsmap(pkg.touch(IDS), ["w16cid"])
    pkg.set_tree(IDS, root)
    for entry in root.findall(qn("w16cid:commentId")):
        if (entry.get(qn("w16cid:paraId")) or "").upper() == para_id.upper():
            return
    entry = etree.SubElement(root, qn("w16cid:commentId"))
    entry.set(qn("w16cid:paraId"), para_id)
    entry.set(qn("w16cid:durableId"), new_para_id(taken))


def anchors(root) -> tuple[dict[str, str], dict[str, int]]:
    """Anchored text and paragraph index per comment id, in one ordered walk.

    Deleted runs are skipped, so an anchor that overlaps a tracked deletion
    still reads as prose instead of interleaving both sides of the revision.
    """
    active: set[str] = set()
    texts: dict[str, list[str]] = {}
    paras: dict[str, int] = {}
    seen = -1
    for el in root.iter():
        tag = local(el)
        if tag == "p":
            seen += 1
        elif tag == "commentRangeStart":
            cid = el.get(qn("w:id"))
            active.add(cid)
            texts.setdefault(cid, [])
            paras.setdefault(cid, seen)
        elif tag == "commentRangeEnd":
            active.discard(el.get(qn("w:id")))
        elif tag == "commentReference":
            paras.setdefault(el.get(qn("w:id")), seen)
        elif tag == "t" and active:
            for cid in active:
                texts[cid].append(el.text or "")
    return {cid: "".join(parts) for cid, parts in texts.items()}, paras


def style_exists(pkg: Package, style_id: str) -> bool:
    if not pkg.has("word/styles.xml"):
        return False
    for style in pkg.tree("word/styles.xml").findall(qn("w:style")):
        if style.get(qn("w:styleId")) == style_id:
            return True
    return False


def reference_run(pkg: Package, cid: int):
    r = etree.Element(qn("w:r"))
    if style_exists(pkg, "CommentReference"):
        rpr = etree.SubElement(r, qn("w:rPr"))
        etree.SubElement(rpr, qn("w:rStyle")).set(qn("w:val"), "CommentReference")
    etree.SubElement(r, qn("w:commentReference")).set(qn("w:id"), str(cid))
    return r


def build_comment(pkg: Package, cid: int, text: str, author: str, initials: str, date: str, taken: set[str]):
    comment = etree.Element(qn("w:comment"))
    comment.set(qn("w:id"), str(cid))
    comment.set(qn("w:author"), author)
    comment.set(qn("w:initials"), initials)
    comment.set(qn("w:date"), date)
    p = etree.SubElement(comment, qn("w:p"))
    p.set(qn("w14:paraId"), new_para_id(taken))
    if style_exists(pkg, "CommentText"):
        ppr = etree.SubElement(p, qn("w:pPr"))
        etree.SubElement(ppr, qn("w:pStyle")).set(qn("w:val"), "CommentText")
    marker = etree.SubElement(p, qn("w:r"))
    if style_exists(pkg, "CommentReference"):
        rpr = etree.SubElement(marker, qn("w:rPr"))
        etree.SubElement(rpr, qn("w:rStyle")).set(qn("w:val"), "CommentReference")
    etree.SubElement(marker, qn("w:annotationRef"))
    p.append(make_run(None, text))
    return comment, p.get(qn("w14:paraId"))


# -- commands -------------------------------------------------------------

def cmd_list(pkg: Package) -> dict:
    croot = comments_root(pkg, create=False)
    exroot = extended_root(pkg, create=False)
    anchored, paras = anchors(pkg.tree(DOC)) if pkg.has(DOC) else ({}, {})
    by_para_id: dict[str, str] = {}
    records = []
    for comment in comment_elements(croot):
        cid = comment.get(qn("w:id"))
        paragraphs = comment.findall(qn("w:p"))
        para_id = (paragraphs[-1].get(qn("w14:paraId")) if paragraphs else None) or ""
        if para_id:
            by_para_id[para_id.upper()] = cid
        records.append({
            "id": cid,
            "author": comment.get(qn("w:author")),
            "initials": comment.get(qn("w:initials")),
            "date": comment.get(qn("w:date")),
            "text": comment_text(comment),
            "anchored_text": anchored.get(cid, ""),
            "paragraph": paras.get(cid),
            "para_id": para_id.upper() or None,
            "resolved": False,
            "parent_id": None,
        })
    if exroot is not None:
        state = {}
        for entry in exroot.findall(qn("w15:commentEx")):
            pid = (entry.get(qn("w15:paraId")) or "").upper()
            state[pid] = (
                (entry.get(qn("w15:done")) or "0").lower() in ("1", "true", "on"),
                (entry.get(qn("w15:paraIdParent")) or "").upper() or None,
            )
        for record in records:
            done, parent = state.get(record["para_id"] or "", (False, None))
            record["resolved"] = done
            record["parent_id"] = by_para_id.get(parent) if parent else None
    open_threads = sum(1 for r in records if r["parent_id"] is None and not r["resolved"])
    return {
        "status": "ok",
        "action": "list",
        "file": str(pkg.path),
        "total": len(records),
        "open_threads": open_threads,
        "has_thread_state": exroot is not None,
        "comments": records,
    }


def next_id(croot) -> int:
    biggest = -1
    for comment in comment_elements(croot):
        try:
            biggest = max(biggest, int(comment.get(qn("w:id")) or 0))
        except ValueError:
            pass
    return biggest + 1


def anchor_runs(p, find: str | None) -> list:
    runs = editable_runs(p)
    if not find:
        return [r for r, s, e in runs if e > s]
    text = para_text(p, "accept")
    at = text.find(find)
    if at < 0:
        return []
    start, end = at, at + len(find)
    for r, s, e in editable_runs(p):
        if s < end < e:
            split_run(r, end - s)
            break
    for r, s, e in editable_runs(p):
        if s < start < e:
            split_run(r, start - s)
            break
    return [r for r, s, e in editable_runs(p) if s >= start and e <= end and e > s]


def cmd_add(pkg: Package, argv: list[str]) -> dict:
    n, text = flag(argv, "--paragraph"), flag(argv, "--text")
    if n is None or text is None:
        fail('add needs --paragraph N and --text "..."')
    root = pkg.touch(DOC)
    paras = iter_paragraphs(root)
    if not 0 <= int(n) < len(paras):
        fail(f"--paragraph {n} out of range (0..{len(paras) - 1})")
    p = paras[int(n)]
    find = flag(argv, "--find")
    runs = anchor_runs(p, find)
    if not runs:
        fail(f"no text to anchor on in paragraph {n}" + (f" matching {find!r}" if find else ""))
    croot = comments_root(pkg, create=True)
    exroot = extended_root(pkg, create=True)
    taken = used_para_ids(croot, exroot, pkg.tree(IDS) if pkg.has(IDS) else None, root)
    cid = next_id(croot)
    author = flag(argv, "--author", "LangAlpha")
    comment, para_id = build_comment(pkg, cid, text, author,
                                     flag(argv, "--initials", initials_for(author)),
                                     flag(argv, "--date", now_stamp()), taken)
    croot.append(comment)
    ex_entry(exroot, para_id)
    register_durable_id(pkg, para_id, taken)
    start = etree.Element(qn("w:commentRangeStart"))
    start.set(qn("w:id"), str(cid))
    end = etree.Element(qn("w:commentRangeEnd"))
    end.set(qn("w:id"), str(cid))
    runs[0].addprevious(start)
    runs[-1].addnext(end)
    end.addnext(reference_run(pkg, cid))
    return {"action": "add", "id": cid, "paragraph": int(n), "para_id": para_id,
            "anchored_text": find or "".join(r.findtext(qn("w:t"), "") for r in runs)}


def cmd_reply(pkg: Package, argv: list[str]) -> dict:
    target, text = flag(argv, "--to"), flag(argv, "--text")
    if target is None or text is None:
        fail('reply needs --to ID and --text "..."')
    croot = comments_root(pkg, create=False)
    if croot is None:
        fail("the document has no comments to reply to")
    parent = next((c for c in comment_elements(croot) if c.get(qn("w:id")) == str(target)), None)
    if parent is None:
        fail(f"no comment with id {target}")
    root = pkg.touch(DOC)
    exroot = extended_root(pkg, create=True)
    taken = used_para_ids(croot, exroot, pkg.tree(IDS) if pkg.has(IDS) else None, root)
    parent_para = ensure_para_id(parent, taken)
    ex_entry(exroot, parent_para)
    cid = next_id(croot)
    author = flag(argv, "--author", "LangAlpha")
    comment, para_id = build_comment(pkg, cid, text, author,
                                     flag(argv, "--initials", initials_for(author)),
                                     flag(argv, "--date", now_stamp()), taken)
    croot.append(comment)
    entry = ex_entry(exroot, para_id)
    entry.set(qn("w15:paraIdParent"), parent_para)
    register_durable_id(pkg, para_id, taken)
    # Word nests a reply inside the parent's own range, so the two share the
    # highlighted text and the thread renders as one balloon.
    parent_start = parent_ref = None
    for el in root.iter():
        tag = local(el)
        if tag == "commentRangeStart" and el.get(qn("w:id")) == str(target):
            parent_start = el
        elif tag == "commentReference" and el.get(qn("w:id")) == str(target):
            parent_ref = el
    if parent_ref is None:
        fail(f"comment {target} has no reference in the document body")
    ref_run = parent_ref.getparent()
    if parent_start is not None:
        start = etree.Element(qn("w:commentRangeStart"))
        start.set(qn("w:id"), str(cid))
        parent_start.addnext(start)
        end = etree.Element(qn("w:commentRangeEnd"))
        end.set(qn("w:id"), str(cid))
        ref_run.addnext(end)
        end.addnext(reference_run(pkg, cid))
    else:
        ref_run.addnext(reference_run(pkg, cid))
    return {"action": "reply", "id": cid, "to": str(target), "para_id": para_id, "parent_para_id": parent_para}


def cmd_resolve(pkg: Package, argv: list[str], done: bool) -> dict:
    target = flag(argv, "--id") or (argv[2] if len(argv) > 2 and not argv[2].startswith("--") else None)
    if target is None:
        fail("resolve needs --id ID")
    croot = comments_root(pkg, create=False)
    if croot is None:
        fail("the document has no comments")
    comment = next((c for c in comment_elements(croot) if c.get(qn("w:id")) == str(target)), None)
    if comment is None:
        fail(f"no comment with id {target}")
    exroot = extended_root(pkg, create=True)
    taken = used_para_ids(croot, exroot, pkg.tree(IDS) if pkg.has(IDS) else None, pkg.tree(DOC))
    para_id = ensure_para_id(comment, taken)
    # Word marks every comment in the thread, so a resolved thread stays
    # collapsed whichever member of it the reader clicks.
    root_para = para_id
    entry = ex_entry(exroot, para_id)
    parent = (entry.get(qn("w15:paraIdParent")) or "").upper()
    if parent:
        root_para = parent
    touched = []
    for ex in [ex_entry(exroot, root_para)] + [
        e for e in exroot.findall(qn("w15:commentEx"))
        if (e.get(qn("w15:paraIdParent")) or "").upper() == root_para
    ]:
        ex.set(qn("w15:done"), "1" if done else "0")
        touched.append(ex.get(qn("w15:paraId")))
    register_durable_id(pkg, para_id, taken)
    return {"action": "resolve" if done else "reopen", "id": str(target),
            "para_id": para_id, "thread_para_ids": touched, "resolved": done}


def strip_anchors(root) -> None:
    """Drop a story's comment markers, taking the reference glyph's run with it.

    The run goes whole only when it carries nothing but the reference, so a
    reference a tool tucked in beside real text never takes the text with it.
    """
    for el in list(root.iter()):
        tag = local(el)
        if tag in ("commentRangeStart", "commentRangeEnd"):
            el.getparent().remove(el)
        elif tag == "commentReference":
            run = el.getparent()
            bare = local(run) == "r" and all(local(c) in ("rPr", "commentReference") for c in run)
            target = run if bare else el
            target.getparent().remove(target)


def cmd_strip(pkg: Package) -> dict:
    croot = pkg.tree(COMMENTS) if pkg.has(COMMENTS) else None
    removed = len(comment_elements(croot))
    for name in pkg.stories():
        if any(local(el) in ANCHOR_TAGS for el in pkg.tree(name).iter()):
            strip_anchors(pkg.touch(name))
    return {"action": "strip", "removed": removed,
            "parts_removed": [name for name in COMMENT_PARTS if pkg.remove(name)]}


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    if len(argv) < 2:
        fail("usage: comments.py list|add|reply|resolve|strip <file.docx> [...]")
    action, path = argv[0], Path(argv[1]).expanduser().resolve()
    if not path.exists():
        fail(f"no such file: {path}")
    try:
        pkg = Package(path)
    except Exception as exc:  # noqa: BLE001 - the message is the diagnosis
        fail(f"cannot open {path}: {exc}")
    if not pkg.has(DOC):
        fail("not a Word document: word/document.xml is missing")

    if action == "list":
        print(json.dumps(cmd_list(pkg), indent=2))
        return
    if action == "add":
        result = cmd_add(pkg, argv)
    elif action == "reply":
        result = cmd_reply(pkg, argv)
    elif action in ("resolve", "reopen"):
        result = cmd_resolve(pkg, argv, action == "resolve")
    elif action == "strip":
        result = cmd_strip(pkg)
    else:
        fail(f"unknown action: {action}")
        return
    out = flag(argv, "--out")
    dst = pkg.save(Path(out) if out else path)
    print(json.dumps({"status": "ok", "file": str(path), "out": str(dst), **result}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
