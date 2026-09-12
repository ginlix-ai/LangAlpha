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
<file stem>_render next to the input. DIR's own page PNGs are cleared first, so
the list printed is this render rather than what a longer one left behind.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


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
        sys.exit(f"LibreOffice produced no PDF for {src}")
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


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args = [a for a in argv if not a.startswith("--")]
    if not args:
        sys.exit("usage: render.py <file.docx> [--out DIR] [--dpi N] [--keep-pdf]")
    src = Path(args[0]).expanduser().resolve()
    out = Path(argv[argv.index("--out") + 1]) if "--out" in argv else src.with_name(src.stem + "_render")
    dpi = int(argv[argv.index("--dpi") + 1]) if "--dpi" in argv else 110
    out.mkdir(parents=True, exist_ok=True)
    # A shorter document leaves the tail of the last render behind, and those pages would
    # then be listed as this one's.
    for stale in page_files(out):
        stale.unlink()
    with tempfile.TemporaryDirectory(prefix="render_") as tmp:
        pdf = convert_to_pdf(src, Path(tmp))
        expected = page_count(pdf)
        subprocess.run(["pdftoppm", "-r", str(dpi), "-png", str(pdf), str(out / "page")], check=True)
        if "--keep-pdf" in argv:
            shutil.copyfile(pdf, out / pdf.name)
    pages = page_files(out)
    print(f"{len(pages)} page(s) rendered to {out}" + ("" if len(pages) == expected else f" (pdfinfo reports {expected})"))
    for p in pages:
        print(p)


if __name__ == "__main__":
    main(sys.argv[1:])
