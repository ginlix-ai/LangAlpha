#!/usr/bin/env python3
"""Render a document's pages to PNG for visual review.

Usage:
    python render.py <file.docx> [--out DIR] [--dpi N] [--keep-pdf]

LibreOffice paginates the document the way Word does closely enough for
proofing, then pdftoppm rasterises the PDF. Look at the PNGs for clipped
tables, a heading orphaned at the foot of a page, an image pushed off the text
area, and a TOC field that never got updated. Tracked changes render marked
up (deleted text struck through beside the insertion), so a page with open
revisions is not the reader's final page; comment balloons never appear.
Read both with redline.py and comments.py. Output files are DIR/page-<n>.png, default DIR is
<file stem>_render next to the input. DIR's own page PNGs are replaced once this run has
produced every page of its own, so the list printed is this render rather than what a
longer one left behind, and a run that fails leaves the last good set in place.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


class RenderFailed(Exception):
    """A render that did not finish, so the staging directory it half filled is discarded.

    Raised rather than reported on the spot so that one place owns both the message and
    the pages of the last good render it must not have thrown away.
    """


def _soffice(src: Path, out_dir: Path, fmt: str, timeout: int) -> None:
    profile = tempfile.mkdtemp(prefix="lo_profile_")
    try:
        subprocess.run(
            [
                "soffice",
                f"-env:UserInstallation=file://{profile}",
                "--headless",
                "--norestore",
                "--nologo",
                "--convert-to",
                fmt,
                "--outdir",
                str(out_dir),
                str(src),
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    finally:
        shutil.rmtree(profile, ignore_errors=True)


def convert_to_pdf(src: Path, out_dir: Path, timeout: int = 180) -> Path:
    pdf = out_dir / (src.stem + ".pdf")
    _soffice(src, out_dir, "pdf", timeout)
    if pdf.exists():
        return pdf
    # A document with unusual OOXML can fail the direct route but survive ODT.
    _soffice(src, out_dir, "odt", timeout)
    odt = out_dir / (src.stem + ".odt")
    if odt.exists():
        _soffice(odt, out_dir, "pdf", timeout)
    if not pdf.exists():
        raise RenderFailed(f"LibreOffice produced no PDF for {src}")
    return pdf


PAGE_PNG = re.compile(r"^page-(\d+)\.png$")


def page_files(out: Path) -> list[Path]:
    """This script's own PNGs, in page order.

    Sorted by the number rather than the name, because pdftoppm pads the index to the page
    count and page-9 would otherwise come after page-10 in a document that crossed 100.
    """
    found = [(int(m.group(1)), p) for p in out.glob("page-*.png") if (m := PAGE_PNG.match(p.name))]
    return [p for _, p in sorted(found)]


def page_count(pdf: Path) -> int:
    info = subprocess.run(["pdfinfo", str(pdf)], capture_output=True, text=True, check=False).stdout
    match = re.search(r"^Pages:\s+(\d+)", info, re.M)
    return int(match.group(1)) if match else 0


def rasterise(src: Path, staging: Path, dpi: int, keep_pdf: bool) -> int:
    """Put this render's whole page set in `staging`, and report what pdfinfo counted.

    Nothing here writes into the output directory, so every failure below leaves the
    render already sitting there to be looked at.
    """
    with tempfile.TemporaryDirectory(prefix="render_") as tmp:
        pdf = convert_to_pdf(src, Path(tmp))
        expected = page_count(pdf)
        try:
            subprocess.run(["pdftoppm", "-r", str(dpi), "-png", str(pdf), str(staging / "page")], check=True)
        except subprocess.CalledProcessError as exc:
            raise RenderFailed(f"pdftoppm exited {exc.returncode} rasterising {src}") from None
        if not page_files(staging):
            raise RenderFailed(f"pdftoppm wrote no page images for {src}")
        if keep_pdf:
            shutil.copyfile(pdf, staging / pdf.name)
    return expected


USAGE = "usage: render.py <file.docx> [--out DIR] [--dpi N] [--keep-pdf]"
TAKES_VALUE = {"--out": True, "--dpi": True, "--keep-pdf": False}
MAX_DPI = 600


def fail(message: str) -> None:
    sys.exit(message)


def parse_args(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token TAKES_VALUE does not name.

    A misspelt --out used to be dropped on the floor, so `render.py doc.docx
    --uot review` rendered into the default directory and cleared the pages
    sitting there instead of the ones the caller named.
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
    if len(args) != 1:
        fail(USAGE)
    src = Path(args[0]).expanduser().resolve()
    if not src.exists():
        fail(f"no such file: {src}")
    out = (Path(str(flags["--out"])).expanduser().resolve() if "--out" in flags
           else src.with_name(src.stem + "_render"))
    # --out is a directory of pages, so a caller who reached for the file
    # spelling gets told that rather than a traceback out of mkdir.
    if out.exists() and not out.is_dir():
        fail(f"--out wants a directory and {out} is a file; {USAGE}")
    raw_dpi = str(flags.get("--dpi", 110))
    if not raw_dpi.isdigit():
        fail(f"--dpi wants a positive whole number, got {raw_dpi!r}; {USAGE}")
    dpi = int(raw_dpi)
    if not 1 <= dpi <= MAX_DPI:
        # Rasterising a long document at an unbounded dpi fills the disk before it fails.
        fail(f"--dpi must be between 1 and {MAX_DPI}; {dpi} would rasterise pages nothing can open")
    # Everything above this line is a read: a rejected argument must not have
    # cost the caller the render already sitting in `out`.
    out.mkdir(parents=True, exist_ok=True)
    # Rasterise next door and swap the pages in once they all exist. Clearing `out` up
    # front costs a caller whose rerun fails both the new render and the last good one,
    # and the render they still had was the thing they were about to look at.
    staging = Path(tempfile.mkdtemp(dir=out.parent, prefix=".render-"))
    try:
        expected = rasterise(src, staging, dpi, "--keep-pdf" in flags)
    except RenderFailed as exc:
        shutil.rmtree(staging, ignore_errors=True)
        fail(f"{exc}; the previous render in {out} is kept")
    # A shorter document leaves the tail of the last render behind, and those pages would
    # then be listed as this one's.
    for stale in page_files(out):
        stale.unlink()
    for staged in sorted(staging.iterdir()):
        staged.replace(out / staged.name)  # a rename, because the staging directory is out's sibling
    shutil.rmtree(staging, ignore_errors=True)
    pages = page_files(out)
    print(f"{len(pages)} page(s) rendered to {out}" + ("" if len(pages) == expected else f" (pdfinfo reports {expected})"))
    for p in pages:
        print(p)


if __name__ == "__main__":
    main(sys.argv[1:])
