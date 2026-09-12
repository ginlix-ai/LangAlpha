#!/usr/bin/env python3
"""Render a workbook's sheets to PNG pages for visual review.

Usage:
    python render.py <file.xlsx> [--out DIR] [--dpi N] [--keep-pdf]

LibreOffice prints each sheet's used range across as many pages as it needs,
then pdftoppm rasterises the PDF. Look at the PNGs for clipped headers,
`###` overflow in narrow columns, spilled text and missing number formats;
the values shown are the cached ones in the file, so run recalc.py first.
Output files are DIR/page-<n>.png, default DIR is <file stem>_render next to
the input; pages from an earlier render of the same file are removed first.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def convert_to_pdf(src: Path, out_dir: Path, timeout: int = 120) -> Path:
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
                "pdf",
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
    pdf = out_dir / (src.stem + ".pdf")
    if not pdf.exists():
        sys.exit(f"LibreOffice produced no PDF for {src}")
    return pdf


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args = [a for a in argv if not a.startswith("--")]
    if not args:
        sys.exit("usage: render.py <file.xlsx> [--out DIR] [--dpi N] [--keep-pdf]")
    src = Path(args[0]).expanduser().resolve()
    out = Path(argv[argv.index("--out") + 1]) if "--out" in argv else src.with_name(src.stem + "_render")
    dpi = int(argv[argv.index("--dpi") + 1]) if "--dpi" in argv else 110
    if shutil.which("soffice") is None or shutil.which("pdftoppm") is None:
        sys.exit("render.py needs soffice (LibreOffice) and pdftoppm (poppler) on PATH")
    out.mkdir(parents=True, exist_ok=True)
    for stale in out.glob("page-*.png"):
        stale.unlink()
    with tempfile.TemporaryDirectory(prefix="render_") as tmp:
        pdf = convert_to_pdf(src, Path(tmp))
        subprocess.run(["pdftoppm", "-r", str(dpi), "-png", str(pdf), str(out / "page")], check=True)
        if "--keep-pdf" in argv:
            shutil.copyfile(pdf, out / pdf.name)
    pages = sorted(out.glob("page-*.png"), key=lambda p: int(re.sub(r"\D", "", p.stem) or 0))
    print(f"{len(pages)} page(s) rendered to {out}")
    for p in pages:
        print(p)


if __name__ == "__main__":
    main(sys.argv[1:])
