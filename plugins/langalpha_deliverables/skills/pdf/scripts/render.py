#!/usr/bin/env python3
"""Rasterise PDF pages to PNGs so you can look at them.

Usage:
    python render.py <file.pdf> [--pages 1-3] [--out DIR] [--dpi N] [--password PW]

pdftoppm writes DIR/page-<n>.png, numbered by the real page number, and the JSON
report names every file. Default DIR is <stem>_render next to the input, default
DPI 150 (readable body text; drop to 100 for a quick look at a long document,
raise to 200 when checking a dense table). DIR's own page PNGs are cleared first,
so `images` is this render and not a wider `--pages` from the last one.

Looking is the verification step no library call replaces. A form field can hold
the right value and still be clipped by its box, a font that is not embedded
renders as tofu boxes, a chart can come out as a black rectangle, and a table can
run off the page. All of that is invisible to pdfplumber and obvious in a PNG.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

from pypdf import PdfReader

PAGE_PNG = re.compile(r"^page-(\d+)\.png$")


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


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


def page_files(out: Path) -> list[Path]:
    """This script's own PNGs, in page order.

    Sorted by the number rather than the name, because pdftoppm pads the index to the page
    count and page-9 would otherwise come after page-10 in a document that crossed 100.
    """
    found = [(int(m.group(1)), p) for p in out.glob("page-*.png") if (m := PAGE_PNG.match(p.name))]
    return [p for _, p in sorted(found)]


def runs(pages: list[int]) -> list[tuple[int, int]]:
    """Contiguous blocks, because pdftoppm takes one -f/-l window per call."""
    blocks: list[tuple[int, int]] = []
    for page in pages:
        if blocks and page == blocks[-1][1] + 1:
            blocks[-1] = (blocks[-1][0], page)
        else:
            blocks.append((page, page))
    return blocks



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
    if shutil.which("pdftoppm") is None:
        fail("pdftoppm (poppler-utils) is not on PATH")
    values, positional = parse_args(argv, ("--pages", "--out", "--dpi", "--password"), ())
    if not positional:
        fail("usage: render.py <file.pdf> [--pages 1-3] [--out DIR] [--dpi N] [--password PW]")
    src = Path(positional[0]).expanduser().resolve()
    if not src.exists():
        fail(f"file not found: {src}")
    dpi = int(str(values.get("--dpi", 150)))
    password = values.get("--password")

    reader = PdfReader(str(src))
    if reader.is_encrypted and not reader.decrypt(password or ""):
        fail(f"{src} is encrypted; pass --password", password_required=True)
    total = len(reader.pages)
    pages = parse_pages(str(values["--pages"]), total) if "--pages" in values else list(range(1, total + 1))

    out = Path(str(values["--out"])).expanduser().resolve() if "--out" in values else src.with_name(src.stem + "_render")
    out.mkdir(parents=True, exist_ok=True)
    # A run over a different --pages selection leaves the earlier PNGs behind, and those
    # pages would then be listed as this render's.
    for stale in page_files(out):
        stale.unlink()
    for first, last in runs(pages):
        cmd = ["pdftoppm", "-r", str(dpi), "-png", "-f", str(first), "-l", str(last)]
        if password:
            cmd += ["-upw", password]
        cmd += [str(src), str(out / "page")]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        if proc.returncode != 0:
            fail(f"pdftoppm failed on pages {first}-{last}: {proc.stderr.strip()[:400]}")

    images = page_files(out)
    print(
        json.dumps(
            {
                "status": "ok" if images else "error",
                "file": str(src),
                "pages_in_file": total,
                "pages_rendered": pages,
                "dpi": dpi,
                "out_dir": str(out),
                "images": [str(p) for p in images],
                "notes": ["Open every PNG and look at it before delivering. Check for clipped text, "
                          "tofu boxes from fonts that are not embedded, black rectangles where a "
                          "chart should be, and tables running past the margin."],
            },
            indent=2,
        )
    )
    if not images:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
