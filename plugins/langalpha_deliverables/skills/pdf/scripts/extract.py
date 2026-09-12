#!/usr/bin/env python3
"""Pull text and tables out of a PDF and say honestly what could not be read.

Usage:
    python extract.py <file.pdf> [--pages 1-3] [--tables] [--raw-tables]
                      [--layout] [--out FILE] [--password PW] [--max-chars N]

Default engine is pdfplumber, which reads the text layer in reading order.
`--layout` switches to `pdftotext -layout`, which keeps columns and aligned
numbers where they sit on the page: use it for financial statements, anything in
columns, and any page whose pdfplumber output reads scrambled.

`--tables` adds pdfplumber's table detection per page as JSON rows, tidied
before they are emitted: cells stripped, rows and columns that are empty
everywhere dropped, and a cell holding nothing but a currency sign or a lone
bracket folded into the number beside it, so `$` + `307,003` reads as
`$307,003`. Each table reports `tidied`, `dropped_rows`, `dropped_columns` and
`merged_cells`, and its `rows`/`columns` describe the grid as emitted. Pass
`--raw-tables` for pdfplumber's untouched grid. Ruled tables come out clean; a
table separated only by whitespace usually needs `text` strategies, and the
settings to try are in the skill's Pitfalls section.

A page with images and no text layer is reported as `possibly_scanned`, never as
an empty page. There is no OCR here, so its words are simply not available:
report that page as unread and ask the user for a text copy rather than guessing
at the content or presenting a partial read as complete.

Each page's text is truncated at --max-chars (default 4000) in the JSON. With
--out FILE the text leaves the JSON entirely: the page entry carries
`text_written: true` and no `text` and no `truncated` key, while `chars`,
`words`, `images` and `possibly_scanned` stay where they were, and the absolute
path of the file is the top-level `text_file`. That file is plain text and not
JSON: each page starts with a line reading `--- page N ---`, carrying the real
page number, followed by the page's text, and the blocks are joined by a blank
line.
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


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_pages(spec: str, total: int) -> list[int]:
    wanted: list[int] = []
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            first, _, last = part.partition("-")
            start = int(first) if first else 1
            end = int(last) if last else total
        else:
            start = end = int(part)
        if start < 1 or end > total or start > end:
            fail(f"page range {part!r} is outside 1-{total}")
        wanted += list(range(start, end + 1))
    return sorted(set(wanted))


def layout_text(path: Path, pages: list[int], password: str | None) -> dict[int, str]:
    if shutil.which("pdftotext") is None:
        fail("pdftotext (poppler-utils) is not on PATH; drop --layout to use pdfplumber")
    out: dict[int, str] = {}
    for page in pages:
        cmd = ["pdftotext", "-layout", "-f", str(page), "-l", str(page)]
        if password:
            cmd += ["-upw", password]
        cmd += [str(path), "-"]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            fail(f"pdftotext failed on page {page}: {proc.stderr.strip()[:300]}")
        out[page] = proc.stdout.replace("\f", "")
    return out


# A cell holding only one of these is a fragment of the value beside it, not a
# column: the prefixes belong to the number on their right, `)` to the one on
# its left.
PREFIX_ONLY = {"$", "\u20ac", "\u00a3", "\u00a5", "("}
SUFFIX_ONLY = {")"}


def drop_empty_columns(rows: list[list[str]]) -> tuple[list[list[str]], int]:
    width = max((len(row) for row in rows), default=0)
    keep = [i for i in range(width) if any(row[i] for row in rows)]
    if len(keep) == width:
        return rows, 0
    return [[row[i] for i in keep] for row in rows], width - len(keep)


def merge_stray_signs(row: list[str]) -> int:
    """The joined value keeps the left cell's index, because a row without a `$`
    puts its figure in the `$` column of the rows that have one: parking
    `$307,003` one cell further right would split one column of numbers in two.
    Prefixes run right to left so a `$` lands on a `(` that has already taken
    its number rather than on the bare bracket.
    """
    merged = 0
    for i in range(len(row) - 1, -1, -1):
        if row[i] in PREFIX_ONLY:
            target = next((j for j in range(i + 1, len(row)) if row[j]), None)
            if target is not None:
                row[i] = row[i] + row[target]
                row[target] = ""
                merged += 1
    for i in range(len(row)):
        if row[i] in SUFFIX_ONLY:
            target = next((j for j in range(i - 1, -1, -1) if row[j]), None)
            if target is not None:
                row[target] = row[target] + row[i]
                row[i] = ""
                merged += 1
    return merged


def tidy_table(grid: list[list[str | None]]) -> tuple[list[list[str]], dict[str, int]]:
    """pdfplumber pads a financial statement out to every ruled gap it can see,
    so the numbers are right but the shape is not: spacer rows, columns that are
    empty top to bottom, and `$` in a cell of its own. Squeeze that back to the
    grid the page shows without interpreting any of it.
    """
    width = max((len(row) for row in grid), default=0)
    rows = [
        [("" if cell is None else str(cell).strip()) for cell in row] + [""] * (width - len(row))
        for row in grid
    ]

    kept = [row for row in rows if any(row)]
    dropped_rows = len(rows) - len(kept)

    kept, dropped_columns = drop_empty_columns(kept)
    merged_cells = sum(merge_stray_signs(row) for row in kept)
    kept, dropped_after_merge = drop_empty_columns(kept)

    return kept, {
        "tidied": True,
        "dropped_rows": dropped_rows,
        "dropped_columns": dropped_columns + dropped_after_merge,
        "merged_cells": merged_cells,
    }


def table_entry(index: int, grid: list[list[str | None]], tidy: bool) -> dict:
    if tidy:
        data, stats = tidy_table(grid)
    else:
        data = [[("" if cell is None else str(cell)) for cell in row] for row in grid]
        stats = {"tidied": False, "dropped_rows": 0, "dropped_columns": 0, "merged_cells": 0}
    return {
        "index": index,
        "rows": len(data),
        "columns": max((len(row) for row in data), default=0),
        **stats,
        "data": data,
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
    values, positional = parse_args(
        argv,
        ("--pages", "--out", "--password", "--max-chars"),
        ("--tables", "--raw-tables", "--layout"),
    )
    if not positional:
        fail(
            "usage: extract.py <file.pdf> [--pages 1-3] [--tables] [--raw-tables] [--layout] "
            "[--out FILE] [--password PW]"
        )
    src = Path(positional[0]).expanduser().resolve()
    if not src.exists():
        fail(f"file not found: {src}")
    password = values.get("--password")
    max_chars = int(str(values.get("--max-chars", 4000)))
    want_tables = "--tables" in values or "--raw-tables" in values
    tidy_tables = "--raw-tables" not in values
    use_layout = "--layout" in values

    reader = PdfReader(str(src))
    if reader.is_encrypted and not reader.decrypt(password or ""):
        fail(f"{src} is encrypted; pass --password", password_required=True)
    total = len(reader.pages)
    pages = parse_pages(str(values["--pages"]), total) if "--pages" in values else list(range(1, total + 1))

    notes: list[str] = []
    layout = layout_text(src, pages, password) if use_layout else {}
    results: list[dict] = []
    full: list[str] = []
    try:
        with pdfplumber.open(str(src), password=password or "") as pdf:
            for number in pages:
                page = pdf.pages[number - 1]
                try:
                    plumber = page.extract_text() or ""
                except Exception as exc:
                    plumber = ""
                    notes.append(f"page {number}: pdfplumber text extraction failed ({type(exc).__name__})")
                text = layout.get(number, plumber) if use_layout else plumber
                images = len(page.images)
                entry = {
                    "page": number,
                    "chars": len(text.strip()),
                    "words": len(text.split()),
                    "images": images,
                    "possibly_scanned": not text.strip() and images > 0,
                }
                if want_tables:
                    try:
                        tables = page.extract_tables()
                    except Exception as exc:
                        tables = []
                        notes.append(f"page {number}: table extraction failed ({type(exc).__name__})")
                    entry["tables"] = [
                        table_entry(i, t, tidy_tables) for i, t in enumerate(tables)
                    ]
                    entry["table_count"] = len(tables)
                full.append(f"--- page {number} ---\n{text}")
                if "--out" in values:
                    entry["text_written"] = True
                else:
                    entry["text"] = text[:max_chars]
                    entry["truncated"] = len(text) > max_chars
                results.append(entry)
                page.flush_cache()
    except Exception as exc:
        fail(f"pdfplumber could not open {src}: {type(exc).__name__}: {exc}")

    out_file = None
    if "--out" in values:
        out_file = Path(str(values["--out"])).expanduser().resolve()
        if out_file == src:
            fail("output path must differ from the input")
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("\n\n".join(full), encoding="utf-8")

    scanned = [r["page"] for r in results if r["possibly_scanned"]]
    if scanned:
        notes.append(
            f"pages {scanned} have images and no text layer: possibly scanned. No OCR is available "
            "here, so their text was not read. Report them as unread and ask the user for a text PDF; "
            "do not present the rest as a complete extraction."
        )
    if want_tables and not any(r.get("table_count") for r in results):
        notes.append("no tables detected; if the page clearly shows one it is probably ruled by "
                     "whitespace, so retry with the text-based table settings from the skill.")
    print(
        json.dumps(
            {
                "status": "ok",
                "file": str(src),
                "sha256": sha256(src),
                "engine": "pdftotext -layout" if use_layout else "pdfplumber",
                "pages_in_file": total,
                "pages_read": pages,
                "total_chars": sum(r["chars"] for r in results),
                "possibly_scanned_pages": scanned,
                "text_file": str(out_file) if out_file else None,
                "pages": results,
                "notes": notes,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main(sys.argv[1:])
