#!/usr/bin/env python3
"""Render a workbook's sheets to PNG pages for visual review.

Usage:
    python render.py <file.xlsx> [--out DIR] [--dpi N] [--keep-pdf]

LibreOffice prints each sheet's used range across as many pages as it needs,
then pdftoppm rasterises the PDF. Look at the PNGs for clipped headers,
`###` overflow in narrow columns, spilled text and missing number formats;
the values shown are the cached ones in the file, so run recalc.py first.
Output files are DIR/page-<n>.png, default DIR is <file stem>_render next to
the input. An earlier render's pages are replaced only once this run has produced
every page of its own, so a rerun that fails leaves the last good set in place and
reports `previous_render_kept`.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


USAGE = "usage: render.py <file.xlsx> [--out DIR] [--dpi N] [--keep-pdf]"
TAKES_VALUE = {"--out": True, "--dpi": True, "--keep-pdf": False}
MAX_DPI = 600


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}))
    sys.exit(1)


class RenderFailed(Exception):
    """A render that did not finish, carrying the extra fields its report names.

    Raised rather than reported on the spot so that one place owns both the report and
    the staging directory it has to throw away.
    """

    def __init__(self, message: str, **extra):
        super().__init__(message)
        self.extra = extra


def convert_to_pdf(src: Path, out_dir: Path, timeout: int = 120) -> Path:
    profile = tempfile.mkdtemp(prefix="lo_profile_")
    try:
        proc = subprocess.run(
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
    except subprocess.TimeoutExpired:
        raise RenderFailed(f"LibreOffice timed out after {timeout}s converting {src}") from None
    finally:
        shutil.rmtree(profile, ignore_errors=True)
    pdf = out_dir / (src.stem + ".pdf")
    if not pdf.exists():
        raise RenderFailed(
            f"LibreOffice produced no PDF for {src}",
            stderr=proc.stderr.strip()[-2000:],
            stdout=proc.stdout.strip()[-500:],
        )
    return pdf


def rasterise(src: Path, staging: Path, dpi: int, keep_pdf: bool) -> None:
    """Put this render's whole page set in `staging`; anything short of that raises.

    Nothing here writes into the output directory, so every failure below leaves the
    render already sitting there to be looked at.
    """
    with tempfile.TemporaryDirectory(prefix="render_") as tmp:
        pdf = convert_to_pdf(src, Path(tmp))
        raster = subprocess.run(
            ["pdftoppm", "-r", str(dpi), "-png", str(pdf), str(staging / "page")],
            capture_output=True,
            text=True,
            check=False,
        )
        if raster.returncode != 0:
            raise RenderFailed(f"pdftoppm exited {raster.returncode}", stderr=raster.stderr.strip()[:400])
        if not any(staging.glob("page-*.png")):
            raise RenderFailed("pdftoppm wrote no page images")
        if keep_pdf:
            shutil.copyfile(pdf, staging / pdf.name)


def parse_args(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token TAKES_VALUE does not name.

    A misspelt --out used to fall back to the default directory, which this script
    then cleared of page-*.png before rendering somewhere the caller never asked for.
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
    if not args:
        fail(USAGE)
    src = Path(args[0]).expanduser().resolve()
    out = Path(flags["--out"]) if "--out" in flags else src.with_name(src.stem + "_render")
    try:
        dpi = int(flags.get("--dpi", 110))
    except ValueError:
        fail(f"--dpi must be a whole number of dots per inch; {USAGE}")
    if not 1 <= dpi <= MAX_DPI:
        # Rasterising a wide sheet at an unbounded dpi fills the disk before it fails.
        fail(f"--dpi must be between 1 and {MAX_DPI}; {dpi} would rasterise pages nothing can open")
    missing = [t for t in ("soffice", "pdftoppm") if shutil.which(t) is None]
    if missing:
        fail(f"render.py needs soffice (LibreOffice) and pdftoppm (poppler) on PATH; missing: {', '.join(missing)}")
    out.mkdir(parents=True, exist_ok=True)
    # Rasterise next door and swap the pages in once they all exist. Clearing `out` up
    # front costs a caller whose rerun fails both the new render and the last good one,
    # and the render they still had was the thing they were about to look at.
    staging = Path(tempfile.mkdtemp(dir=out.parent, prefix=".render-"))
    try:
        rasterise(src, staging, dpi, "--keep-pdf" in flags)
    except RenderFailed as exc:
        shutil.rmtree(staging, ignore_errors=True)
        fail(str(exc), previous_render_kept=True, **exc.extra)
    for stale in out.glob("page-*.png"):
        stale.unlink()
    for page in sorted(staging.iterdir()):
        page.replace(out / page.name)  # a rename, because the staging directory is out's sibling
    shutil.rmtree(staging, ignore_errors=True)
    pages = sorted(out.glob("page-*.png"), key=lambda p: int(re.sub(r"\D", "", p.stem) or 0))
    print(f"{len(pages)} page(s) rendered to {out}")
    for p in pages:
        print(p)


if __name__ == "__main__":
    main(sys.argv[1:])
