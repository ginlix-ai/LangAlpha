#!/usr/bin/env python3
"""Rasterise PDF pages to PNGs so you can look at them.

Usage:
    python render.py <file.pdf> [--pages 1-3] [--out DIR] [--dpi N] [--password PW]

pdftoppm writes DIR/page-<n>.png, numbered by the real page number, and the JSON
report names every file. Default DIR is <stem>_render next to the input, default
DPI 150 (readable body text; drop to 100 for a quick look at a long document,
raise to 200 when checking a dense table). DIR's own page PNGs are replaced once this
run has produced every page of its own, so `images` is this render and not a wider
`--pages` from the last one, and a run that fails leaves the last good set in place.

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
import tempfile
from pathlib import Path

from pypdf import PdfReader

PAGE_PNG = re.compile(r"^page-(\d+)\.png$")


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


def pdf_reader(path: Path) -> PdfReader:
    """A PdfReader over a file that may not be a readable PDF at all.

    pypdf raises out of the constructor on an empty, truncated or non-PDF file, and a
    traceback is not this script's contract: every other failure here is a JSON report.
    """
    try:
        return PdfReader(str(path))
    except Exception as exc:
        fail(f"cannot read {path} as a PDF: {type(exc).__name__}: {exc}", unreadable=True)


MAX_DPI = 600


def parse_dpi(value) -> int:
    """Rasterising a long document at an unbounded dpi fills the disk before it fails."""
    try:
        dpi = int(str(value))
    except ValueError:
        fail(f"--dpi must be a whole number of dots per inch, not {value!r}")
    if not 1 <= dpi <= MAX_DPI:
        fail(f"--dpi must be between 1 and {MAX_DPI}; {dpi} would rasterise pages nothing can open")
    return dpi


def parse_pages(spec: str, total: int) -> list[int]:
    """The pages an explicit `--pages` asks for, as a sorted list.

    A spec that selects nothing is an error, not an empty render: `--pages ""` and
    `--pages ,` asked for pages, and this script deletes the previous run's PNGs before it
    rasterises, so reporting `ok` over an empty selection leaves an empty directory behind.
    A token that is not a number gets the same JSON error rather than a traceback.
    """
    wanted: list[int] = []
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        try:
            if "-" in part:
                first, _, last = part.partition("-")
                start = int(first) if first else 1
                end = int(last) if last else total
            else:
                start = end = int(part)
        except ValueError:
            fail(f"--pages {part!r} is not a page number or range; write 3, 2-5, -4 or 7-")
        if start < 1 or end > total or start > end:
            fail(f"page range {part!r} is outside 1-{total}")
        wanted += list(range(start, end + 1))
    if not wanted:
        fail(f"--pages {spec!r} selects no page; give a page number or range within 1-{total}")
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



USAGE = "render.py <file.pdf> [--pages 1-3] [--out DIR] [--dpi N] [--password PW]"
# Every option this script accepts, and whether each takes a value. The table is what
# makes an unrecognised token an error instead of a second input file, so an option
# added below has to be added here too.
TAKES_VALUE: dict[str, bool] = {
    "--pages": True,
    "--out": True,
    "--dpi": True,
    "--password": True,
}


def parse_args(argv: list[str], takes_value: dict[str, bool], usage: str) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token `takes_value` does not name.

    A value is still read by position, so `--out x` works when x is also an input name, but
    it is never read off another option: `--out --dpi 300` used to rasterise into a
    directory called `--dpi`, at the default 150 because the 300 had been dropped as a
    stray positional, and report all of that as a success. A repeated option is the same
    kind of mistake, since the last one silently won and the directory the first one named
    was never written.
    """
    args: list[str] = []
    flags: dict[str, str | bool] = {}
    i = 0
    while i < len(argv):
        token = argv[i]
        if not token.startswith("--"):
            args.append(token)
            i += 1
        elif token not in takes_value:
            fail(f"unknown option: {token}; usage: {usage}")
        elif token in flags:
            fail(f"repeated option: {token}; usage: {usage}")
        elif not takes_value[token]:
            flags[token] = True
            i += 1
        elif i + 1 >= len(argv) or argv[i + 1].startswith("--"):
            fail(f"{token} requires a value; usage: {usage}")
        else:
            flags[token] = argv[i + 1]
            i += 2
    return args, flags


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    if shutil.which("pdftoppm") is None:
        fail("pdftoppm (poppler-utils) is not on PATH")
    positional, values = parse_args(argv, TAKES_VALUE, USAGE)
    if not positional:
        fail(f"usage: {USAGE}")
    src = Path(positional[0]).expanduser().resolve()
    if not src.exists():
        fail(f"file not found: {src}")
    dpi = parse_dpi(values.get("--dpi", 150))
    password = values.get("--password")

    reader = pdf_reader(src)
    if reader.is_encrypted:
        # A broken /Encrypt dictionary raises out of decrypt rather than returning 0,
        # and an unreadable file is a report like any other.
        try:
            opened = reader.decrypt(password or "")
        except Exception as exc:
            fail(f"cannot decrypt {src}: {type(exc).__name__}: {exc}", password_required=True)
        if not opened:
            fail(f"{src} is encrypted; pass --password", password_required=True)
    total = len(reader.pages)
    pages = parse_pages(str(values["--pages"]), total) if "--pages" in values else list(range(1, total + 1))

    out = Path(str(values["--out"])).expanduser().resolve() if "--out" in values else src.with_name(src.stem + "_render")
    out.mkdir(parents=True, exist_ok=True)
    # Rasterise next door and swap the pages in once they all exist. Clearing `out` up
    # front costs a caller whose rerun fails both the new render and the last good one,
    # and the render they still had was the thing they came to look at.
    staging = Path(tempfile.mkdtemp(dir=out.parent, prefix=".render-"))
    for first, last in runs(pages):
        cmd = ["pdftoppm", "-r", str(dpi), "-png", "-f", str(first), "-l", str(last)]
        if password:
            # The caller has one password and it may be either role; poppler refuses the
            # file when the password it was handed is the other one.
            cmd += ["-upw", password, "-opw", password]
        cmd += [str(src), str(staging / "page")]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        if proc.returncode != 0:
            shutil.rmtree(staging, ignore_errors=True)
            fail(f"pdftoppm failed on pages {first}-{last}: {proc.stderr.strip()[:400]}", previous_render_kept=True)

    rendered = page_files(staging)
    if rendered:
        # A run over a different --pages selection leaves the earlier PNGs behind, and
        # those pages would then be listed as this render's.
        for stale in page_files(out):
            stale.unlink()
    images = [staged.replace(out / staged.name) for staged in rendered]
    shutil.rmtree(staging, ignore_errors=True)
    report = {
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
    }
    if not images:
        # Nothing was rasterised, so nothing in `out` was cleared to make room for it.
        report["previous_render_kept"] = True
    print(json.dumps(report, indent=2))
    if not images:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
