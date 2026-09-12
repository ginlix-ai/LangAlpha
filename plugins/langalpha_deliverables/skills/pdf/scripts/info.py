#!/usr/bin/env python3
"""Report everything a PDF will tell you before you read a word of it.

Usage:
    python info.py <file.pdf> [--password PW] [--no-fonts] [--max-pages N]

One JSON object on stdout: page count and page sizes, encryption and the
permission bits, AcroForm and XFA presence, fonts with their embedded flag,
per-page text length and image count, metadata, and the SHA-256 of the input.

The point of running this first is that the cheap facts change the plan. An
encrypted file needs a password before anything else works, an AcroForm means
forms.py rather than a rewrite, a font that is not embedded means the render
will show tofu on a machine that lacks it, and a page with images and no text
is reported as `possibly_scanned` rather than as an empty page. There is no OCR
here, so a scanned page stays unread: say so and ask the user for a text copy.

Exit code is 1 only when the file cannot be opened or the password is wrong.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pdfplumber
from pypdf import PdfReader
from pypdf.constants import UserAccessPermissions as UAP
from pypdf.errors import PdfReadError

PAPER = {
    (612, 792): "Letter portrait",
    (792, 612): "Letter landscape",
    (612, 1008): "Legal portrait",
    (1008, 612): "Legal landscape",
    (595, 842): "A4 portrait",
    (842, 595): "A4 landscape",
    (842, 1191): "A3 portrait",
    (420, 595): "A5 portrait",
    (792, 1224): "Tabloid portrait",
}
FIELD_TYPES = {"/Tx": "text", "/Btn": "button", "/Ch": "choice", "/Sig": "signature"}
# The standard 14: every reader ships them, so `emb no` on these is normal, not a defect.
# reportlab writes an unused Helvetica into the resources of every page it draws.
BASE_14 = {
    "Helvetica", "Helvetica-Bold", "Helvetica-Oblique", "Helvetica-BoldOblique",
    "Times-Roman", "Times-Bold", "Times-Italic", "Times-BoldItalic",
    "Courier", "Courier-Bold", "Courier-Oblique", "Courier-BoldOblique",
    "Symbol", "ZapfDingbats",
}


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


def as_bool(value) -> bool:
    """pypdf's BooleanObject has no __bool__, so bool(BooleanObject(False)) is True."""
    return bool(getattr(value, "value", value))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def paper_name(width: float, height: float) -> str:
    key = (round(width), round(height))
    for (w, h), name in PAPER.items():
        if abs(key[0] - w) <= 2 and abs(key[1] - h) <= 2:
            return name
    return "custom"


def permissions(reader: PdfReader) -> dict | None:
    """The /P bits as plain booleans; a denied bit needs the owner password to override."""
    try:
        p = reader.user_access_permissions
    except Exception:
        return None
    if p is None:
        return None
    # `p.PRINT` on an instance is the class constant and always truthy; mask instead.
    return {
        "print": bool(p & UAP.PRINT),
        "print_high_resolution": bool(p & UAP.PRINT_TO_REPRESENTATION),
        "modify_contents": bool(p & UAP.MODIFY),
        "modify_annotations": bool(p & UAP.ADD_OR_MODIFY),
        "fill_form_fields": bool(p & UAP.FILL_FORM_FIELDS),
        "extract_text": bool(p & UAP.EXTRACT),
        "extract_for_accessibility": bool(p & UAP.EXTRACT_TEXT_AND_GRAPHICS),
        "assemble_document": bool(p & UAP.ASSEMBLE_DOC),
    }


def read_fonts(path: Path, password: str | None) -> tuple[list[dict], list[str]]:
    """pdffonts is the only tool here that reports the embedded flag per font."""
    if shutil.which("pdffonts") is None:
        return [], ["pdffonts is not on PATH; font embedding not checked"]
    cmd = ["pdffonts"]
    if password:
        cmd += ["-upw", password]
    cmd.append(str(path))
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    lines = proc.stdout.splitlines()
    if len(lines) < 2 or not lines[1].startswith("---"):
        return [], [f"pdffonts returned no font table: {proc.stderr.strip()[:200]}"]
    # The rule line under the header marks each column, so slice by position:
    # font names and type names both contain spaces and defeat a token split.
    spans, start = [], None
    for i, ch in enumerate(lines[1] + " "):
        if ch == "-" and start is None:
            start = i
        elif ch != "-" and start is not None:
            spans.append((start, i))
            start = None
    fonts, seen = [], set()
    for line in lines[2:]:
        if not line.strip():
            continue
        cols = [line[a:b].strip() for a, b in spans]
        if len(cols) < 6:
            continue
        name = cols[0] or "(unnamed)"
        entry = {
            "name": name,
            "type": cols[1],
            "encoding": cols[2],
            "embedded": cols[3].lower() == "yes",
            "subset": cols[4].lower() == "yes",
            "unicode_map": cols[5].lower() == "yes",
            "standard_14": name.split("+")[-1] in BASE_14,
        }
        key = (entry["name"], entry["type"], entry["embedded"])
        if key in seen:
            continue
        seen.add(key)
        fonts.append(entry)
    return fonts, []


def page_content(path: Path, password: str | None, max_pages: int) -> tuple[list[dict], list[str]]:
    notes: list[str] = []
    out: list[dict] = []
    try:
        with pdfplumber.open(str(path), password=password or "") as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                if i > max_pages:
                    notes.append(f"page text scan stopped at {max_pages} pages; pass --max-pages to go further")
                    break
                try:
                    text = page.extract_text() or ""
                except Exception as exc:  # a damaged content stream is a per-page fact
                    text = ""
                    notes.append(f"page {i}: text extraction failed ({type(exc).__name__})")
                images = len(page.images)
                out.append(
                    {
                        "page": i,
                        "text_chars": len(text.strip()),
                        "words": len(text.split()),
                        "images": images,
                        "possibly_scanned": not text.strip() and images > 0,
                        "blank": not text.strip() and images == 0,
                    }
                )
                page.flush_cache()
    except Exception as exc:
        notes.append(f"pdfplumber could not read page content: {type(exc).__name__}: {exc}")
    return out, notes


def inspect(path: Path, password: str | None, want_fonts: bool, max_pages: int) -> dict:
    try:
        reader = PdfReader(str(path))
    except PdfReadError as exc:
        fail(f"cannot parse {path}: {exc}")
    encrypted = reader.is_encrypted
    opened_with_password = False
    if encrypted:
        try:
            ok = reader.decrypt(password or "")
        except Exception as exc:
            fail(f"cannot decrypt {path}: {exc}", encrypted=True, password_required=True)
        if not ok:
            fail(
                "encrypted file; supply --password. An owner-restricted file still needs "
                "its user password to open, and the owner password to override a denied /P bit.",
                encrypted=True,
                password_required=True,
            )
        opened_with_password = True

    notes: list[str] = []
    pages = []
    for i, page in enumerate(reader.pages, start=1):
        box = page.mediabox
        w, h = float(box.width), float(box.height)
        entry = {
            "page": i,
            "width_pt": round(w, 2),
            "height_pt": round(h, 2),
            "rotation": int(page.rotation or 0),
            "paper": paper_name(w, h),
        }
        crop = page.cropbox
        if (round(float(crop.width), 1), round(float(crop.height), 1)) != (round(w, 1), round(h, 1)):
            entry["cropbox_pt"] = [round(float(crop.width), 2), round(float(crop.height), 2)]
        pages.append(entry)

    root = reader.trailer.get("/Root", {})
    acroform = root.get("/AcroForm")
    form = {"has_acroform": acroform is not None, "field_count": 0, "xfa": False, "need_appearances": False}
    if acroform is not None:
        acroform = acroform.get_object()
        form["xfa"] = "/XFA" in acroform
        form["need_appearances"] = as_bool(acroform.get("/NeedAppearances", False))
        try:
            fields = reader.get_fields() or {}
        except Exception as exc:
            fields = {}
            notes.append(f"AcroForm present but field enumeration failed: {type(exc).__name__}")
        form["field_count"] = len(fields)
        form["field_types"] = {}
        for value in fields.values():
            kind = FIELD_TYPES.get(str(value.get("/FT", "")), "unknown")
            form["field_types"][kind] = form["field_types"].get(kind, 0) + 1
        if form["xfa"]:
            notes.append("XFA form: the AcroForm layer may be a shell. Values written here will not "
                         "appear in an XFA-aware viewer; ask the user for an AcroForm copy.")
        if not fields:
            notes.append("AcroForm with zero fields; blanks on the page are drawn, not fillable.")
    else:
        notes.append("No AcroForm. That is not the same as no form: visible blanks may have been "
                     "flattened into the page or drawn by hand.")

    content, content_notes = page_content(path, password, max_pages)
    notes += content_notes
    scanned = [p["page"] for p in content if p["possibly_scanned"]]
    if scanned:
        notes.append(
            f"pages {scanned} carry images and no text layer: possibly scanned. There is no OCR in "
            "this toolchain, so their content cannot be read. Tell the user and ask for a text PDF."
        )

    fonts, font_notes = ([], []) if not want_fonts else read_fonts(path, password)
    notes += font_notes
    not_embedded = sorted({f["name"] for f in fonts if not f["embedded"] and not f["standard_14"]})
    base14 = sorted({f["name"] for f in fonts if f["standard_14"]})
    if not_embedded:
        notes.append(
            f"fonts not embedded: {not_embedded}. They render from the reader's own copy, so the same "
            "file can look different or show tofu boxes on a machine that lacks them. Embed them."
        )
    if base14:
        notes.append(
            f"standard-14 fonts present ({base14}); every reader supplies these, so an unembedded "
            "one is not a defect. They cover WinAnsi only: accented, CJK or symbol text needs an "
            "embedded TrueType face. reportlab leaves an unused Helvetica in every page it writes."
        )

    meta = {}
    try:
        for key, value in (reader.metadata or {}).items():
            meta[str(key).lstrip("/")] = str(value)
    except Exception:
        notes.append("metadata dictionary could not be decoded")

    return {
        "status": "ok",
        "file": str(path),
        "sha256": sha256(path),
        "bytes": path.stat().st_size,
        "pdf_version": (reader.pdf_header or "").lstrip("%"),
        "pages": len(reader.pages),
        "page_sizes": pages,
        "encryption": {
            "encrypted": encrypted,
            "opened_with_password": opened_with_password,
            "permissions": permissions(reader) if encrypted else None,
        },
        "form": form,
        "fonts": fonts,
        "fonts_not_embedded": not_embedded,
        "fonts_standard_14": base14,
        "page_content": content,
        "possibly_scanned_pages": scanned,
        "metadata": meta,
        "notes": notes,
    }



def parse_args(argv: list[str], with_value: tuple[str, ...], switches: tuple[str, ...]) -> tuple[dict, list[str]]:
    """Flag values are taken by position, so `--out x.pdf` still works when x.pdf is also an input name."""
    opts: dict[str, object] = {}
    positional: list[str] = []
    i = 0
    while i < len(argv):
        token = argv[i]
        if token in with_value:
            if i + 1 >= len(argv):
                fail(f"{token} needs a value")
            opts[token] = argv[i + 1]
            i += 2
        elif token in switches:
            opts[token] = True
            i += 1
        elif token.startswith("--"):
            fail(f"unknown option {token}")
        else:
            positional.append(token)
            i += 1
    return opts, positional


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    opts, positional = parse_args(argv, ("--password", "--max-pages"), ("--no-fonts",))
    if not positional:
        fail("usage: info.py <file.pdf> [--password PW] [--no-fonts] [--max-pages N]")
    path = Path(positional[0]).expanduser().resolve()
    if not path.exists():
        fail(f"file not found: {path}")
    report = inspect(
        path,
        opts.get("--password"),
        "--no-fonts" not in opts,
        int(str(opts.get("--max-pages", 500))),
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
