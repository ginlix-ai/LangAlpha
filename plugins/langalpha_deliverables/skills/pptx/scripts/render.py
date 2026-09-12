#!/usr/bin/env python3
"""Rasterise a deck to one PNG per slide, plus an optional contact sheet.

Usage:
    python render.py <deck.pptx> [--out DIR] [--dpi N] [--montage] [--cols N] [--keep-pdf]

LibreOffice converts the deck to PDF and pdftoppm rasterises it, so the pixels
come from a real layout engine rather than from the code that wrote the file.
That is the only way to see clipped text, a chart legend covering a bar, a font
that did not resolve, or a slide that renders blank. Some decks fail Impress's
direct PDF export but survive a round trip through ODP, so that fallback runs
before giving up.

Look at the montage first to judge the deck as a deck, then open the individual
slides for anything that looks wrong. Output goes to DIR/slide-<n>.png with the
montage at DIR/montage.png; the default DIR is <stem>_render next to the input.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

EMU_PER_IN = 914400


def soffice(src: Path, out_dir: Path, fmt: str, timeout: int) -> None:
    profile = tempfile.mkdtemp(prefix="lo_profile_")
    try:
        subprocess.run(
            [
                "soffice",
                f"-env:UserInstallation=file://{profile}",
                "--headless",
                "--invisible",
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


def convert_to_pdf(src: Path, tmp: Path, timeout: int) -> Path | None:
    soffice(src, tmp, "pdf", timeout)
    pdf = tmp / (src.stem + ".pdf")
    if pdf.exists():
        return pdf
    # Saving to ODP normalises constructs the PPTX filter chokes on, and the
    # ODP export path then produces a PDF for decks the direct route drops.
    soffice(src, tmp, "odp", timeout)
    odp = tmp / (src.stem + ".odp")
    if odp.exists():
        soffice(odp, tmp, "pdf", timeout)
        if pdf.exists():
            return pdf
    return None


def slide_count(path: Path) -> int | None:
    try:
        from pptx import Presentation

        return len(Presentation(str(path)).slides)
    except Exception:
        return None


def page_key(path: Path) -> int:
    match = re.search(r"(\d+)$", path.stem)
    return int(match.group(1)) if match else 0


def build_montage(pages: list[Path], out: Path, cols: int, cell_w: int) -> Path:
    """Contact sheet of every slide, numbered, on a neutral ground.

    The grid is what catches deck-level problems a single slide never shows:
    a title that jumps, one slide twice as dense as its neighbours, a colour
    used for two different meanings.
    """
    from PIL import Image, ImageDraw, ImageFont, ImageOps

    cell_h = round(cell_w * 9 / 16)
    with Image.open(pages[0]) as first:
        cell_h = round(cell_w * first.height / first.width)
    gap, label_h = 14, 22
    rows = (len(pages) + cols - 1) // cols
    canvas = Image.new(
        "RGB",
        (cols * cell_w + (cols + 1) * gap, rows * (cell_h + label_h) + (rows + 1) * gap),
        (238, 236, 232),
    )
    draw = ImageDraw.Draw(canvas)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 15)
    except Exception:
        font = ImageFont.load_default()
    for i, page in enumerate(pages):
        col, row = i % cols, i // cols
        x0 = gap + col * (cell_w + gap)
        y0 = gap + row * (cell_h + label_h + gap)
        with Image.open(page) as img:
            tile = ImageOps.contain(img.convert("RGB"), (cell_w, cell_h), Image.Resampling.LANCZOS)
        px = x0 + (cell_w - tile.width) // 2
        py = y0 + (cell_h - tile.height) // 2
        canvas.paste(tile, (px, py))
        draw.rectangle([px - 1, py - 1, px + tile.width, py + tile.height], outline=(176, 172, 166))
        draw.text((x0 + cell_w // 2 - 6, y0 + cell_h + 4), str(i + 1), font=font, fill=(60, 58, 55))
    canvas.save(out)
    return out


def flag(argv: list[str], name: str, default: int) -> int:
    return int(argv[argv.index(name) + 1]) if name in argv else default


def positional(argv: list[str], value_flags: set[str]) -> list[str]:
    """Arguments that are neither a flag nor a flag's value.

    Matching a flag's value by string would eat the file path when the two
    happen to be equal, which is exactly the kind of bug nobody reproduces.
    """
    out, skip = [], False
    for arg in argv:
        if skip:
            skip = False
            continue
        if arg.startswith("--"):
            skip = arg in value_flags
            continue
        out.append(arg)
    return out


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args = positional(argv, {"--out", "--dpi", "--cols"})
    if not args:
        print(json.dumps({"status": "error", "message": "usage: render.py <deck.pptx> [--out DIR] [--dpi N] [--montage] [--cols N] [--keep-pdf]"}))
        sys.exit(1)
    src = Path(args[0]).expanduser().resolve()
    if not src.exists():
        print(json.dumps({"status": "error", "message": f"no such file: {src}"}))
        sys.exit(1)
    out = Path(argv[argv.index("--out") + 1]).expanduser().resolve() if "--out" in argv else src.with_name(src.stem + "_render")
    dpi = flag(argv, "--dpi", 110)
    cols = flag(argv, "--cols", 3)
    out.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="render_") as tmp:
        pdf = convert_to_pdf(src, Path(tmp), timeout=240)
        if pdf is None:
            print(json.dumps({"status": "error", "file": str(src), "message": "LibreOffice produced no PDF, directly or through ODP"}))
            sys.exit(1)
        for stale in out.glob("slide-*.png"):
            stale.unlink()
        subprocess.run(["pdftoppm", "-r", str(dpi), "-png", str(pdf), str(out / "slide")], check=True)
        if "--keep-pdf" in argv:
            shutil.copyfile(pdf, out / (src.stem + ".pdf"))

    pages = sorted(out.glob("slide-*.png"), key=page_key)
    report: dict = {
        "status": "success",
        "file": str(src),
        "out": str(out),
        "dpi": dpi,
        "pages_rendered": len(pages),
        "slides_in_deck": slide_count(src),
        "pages": [str(p) for p in pages],
    }
    if not pages:
        report["status"] = "error"
        report["message"] = "pdftoppm produced no images"
        print(json.dumps(report, indent=2))
        sys.exit(1)
    if report["slides_in_deck"] not in (None, len(pages)):
        report["warning"] = "rendered page count does not match the deck's slide count; LibreOffice dropped or split a slide"
    if "--montage" in argv:
        report["montage"] = str(build_montage(pages, out / "montage.png", cols, 640))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
