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
LibreOffice leaves a hidden slide out of the PDF, so <n> is the deck slide number
rather than the page number whenever hidden slides explain a short render. DIR's own
slide PNGs, contact sheet and kept PDF are replaced once this run has produced the
whole set, the contact sheet included, so a run that fails anywhere in it leaves the
last good render in place and says `previous_render_kept`. Whatever this run did not
produce is removed rather than left to be read as part of it, so a rerun without
--montage takes the old contact sheet with it instead of leaving one that shows an
earlier deck.

A token this usage does not name is an error, not a token to skip: a misspelt
--montage used to drop out of the parse unread, so the deck rendered without the
contact sheet and the run still reported success.
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
USAGE = "usage: render.py <deck.pptx> [--out DIR] [--dpi N] [--montage] [--cols N] [--keep-pdf]"
TAKES_VALUE = {"--out": True, "--dpi": True, "--montage": False, "--cols": True, "--keep-pdf": False}
MAX_DPI = 600
MAX_COLS = 12


def fail(message: str) -> None:
    print(json.dumps({"status": "error", "message": message}))
    sys.exit(1)


class RenderFailed(Exception):
    """A render that did not finish, so the staging directory it half filled is discarded.

    Raised rather than reported on the spot so that one place owns both the report and
    the slides of the last good render it must not have thrown away.
    """


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


def deck_slides(path: Path) -> tuple[int | None, list[int]]:
    """The deck's slide count and the numbers of the slides marked hidden.

    A hidden slide is `<p:sld show="0">`, which LibreOffice leaves out of the
    PDF, so it is the everyday reason a render comes back a page short.
    """
    try:
        from pptx import Presentation

        slides = list(Presentation(str(path)).slides)
        hidden = [n for n, slide in enumerate(slides, start=1)
                  if slide.element.get("show") in ("0", "false")]
    except Exception:
        return None, []
    return len(slides), hidden


def page_key(path: Path) -> int:
    match = re.search(r"(\d+)$", path.stem)
    return int(match.group(1)) if match else 0


def renumber_pages(pages: list[Path], numbers: list[int]) -> list[Path]:
    """Rename each rendered page to the deck slide it actually shows.

    pdftoppm numbers the pages it was given, so one hidden slide shifts every
    page after it and slide-2.png is deck slide 3. The highest number is
    renamed first, or a rename lands on a file still waiting its turn.
    """
    width = len(str(max(numbers)))
    for page in pages:
        digits = re.search(r"(\d+)$", page.stem)
        width = max(width, len(digits.group(1)) if digits else 1)
    renamed = []
    for page, number in sorted(zip(pages, numbers), key=lambda pair: pair[1], reverse=True):
        target = page.with_name(f"slide-{number:0{width}d}.png")
        if target != page:
            page.replace(target)
        renamed.append(target)
    return sorted(renamed, key=page_key)


def build_montage(pages: list[Path], out: Path, cols: int, cell_w: int,
                  labels: list[int] | None = None) -> Path:
    """Contact sheet of every slide, numbered, on a neutral ground.

    The grid is what catches deck-level problems a single slide never shows:
    a title that jumps, one slide twice as dense as its neighbours, a colour
    used for two different meanings. Tiles carry the deck slide numbers when
    the caller passes them, so a skipped hidden slide is visible as a gap
    rather than renumbering the deck under the reader.
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
        label = str(labels[i] if labels else i + 1)
        draw.text((x0 + cell_w // 2 - 6, y0 + cell_h + 4), label, font=font, fill=(60, 58, 55))
    canvas.save(out)
    return out


def parse_args(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token TAKES_VALUE does not name.

    A flag's value is taken by position, so a deck named `--montage` is still the deck
    and not the flag.
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


def whole_number(name: str, value, low: int, high: int) -> int:
    """A count the rest of the run can rely on, rather than one that raises mid-render."""
    try:
        number = int(str(value))
    except ValueError:
        fail(f"{name} takes a whole number, not {value!r}; {USAGE}")
    if not low <= number <= high:
        fail(f"{name} must be between {low} and {high}, and {number} is not")
    return number


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args, flags = parse_args(argv)
    if not args:
        fail(USAGE)
    if len(args) > 1:
        fail(f"one deck at a time, and {args[1]} is a second; {USAGE}")
    src = Path(args[0]).expanduser().resolve()
    if not src.exists():
        fail(f"no such file: {src}")
    out = (
        Path(str(flags["--out"])).expanduser().resolve()
        if "--out" in flags
        else src.with_name(src.stem + "_render")
    )
    dpi = whole_number("--dpi", flags.get("--dpi", 110), 1, MAX_DPI)
    cols = whole_number("--cols", flags.get("--cols", 3), 1, MAX_COLS)
    out.mkdir(parents=True, exist_ok=True)

    # Rasterise next door and swap the render in once every part of it exists, the
    # contact sheet included: it is drawn from the staged pages, so a deck Pillow cannot
    # fit in memory fails with the last good render untouched rather than half replaced.
    # Clearing `out` first costs a caller whose rerun fails both the new render and the
    # last good one, and the render they still had was the thing they came to look at.
    staging = Path(tempfile.mkdtemp(dir=out.parent, prefix=".render-"))
    try:
        with tempfile.TemporaryDirectory(prefix="render_") as tmp:
            pdf = convert_to_pdf(src, Path(tmp), timeout=240)
            if pdf is None:
                raise RenderFailed("LibreOffice produced no PDF, directly or through ODP")
            try:
                subprocess.run(["pdftoppm", "-r", str(dpi), "-png", str(pdf), str(staging / "slide")], check=True)
            except subprocess.CalledProcessError as exc:
                raise RenderFailed(f"pdftoppm exited {exc.returncode}") from None
            if not any(staging.glob("slide-*.png")):
                raise RenderFailed("pdftoppm produced no images")
            if "--keep-pdf" in flags:
                shutil.copyfile(pdf, staging / (src.stem + ".pdf"))
        rendered = sorted(staging.glob("slide-*.png"), key=page_key)
        slides, hidden = deck_slides(src)
        # A short render whose shortfall is exactly the hidden slides is explained,
        # so the pages can be numbered by deck slide instead of silently sliding up.
        slide_of_page: list[int] | None = None
        if rendered and slides is not None and hidden and slides - len(rendered) == len(hidden):
            slide_of_page = [n for n in range(1, slides + 1) if n not in set(hidden)]
            rendered = renumber_pages(rendered, slide_of_page)
        if "--montage" in flags:
            try:
                build_montage(rendered, staging / "montage.png", cols, 640, slide_of_page)
            except Exception as exc:  # Pillow out of memory, a page it cannot open, a disk with no room
                raise RenderFailed(f"contact sheet: {exc}") from None
    except RenderFailed as exc:
        shutil.rmtree(staging, ignore_errors=True)
        print(json.dumps({"status": "error", "file": str(src), "message": str(exc), "previous_render_kept": True}))
        sys.exit(1)

    # The staged set is this render, whole, so a file under one of the names this
    # script writes that the staging directory does not carry is left over from an
    # earlier run. The contact sheet is the one that misleads: a reader who takes
    # the advice to look at the montage first would be judging an older deck beside
    # the new pages. Only those names are cleared, never the whole directory, since
    # --out can be somewhere the caller keeps files of their own.
    staged_names = {path.name for path in staging.iterdir()}
    owned = [*out.glob("slide-*.png"), out / "montage.png", out / (src.stem + ".pdf")]
    for stale in owned:
        if stale.name not in staged_names and stale.is_file():
            stale.unlink()
    for staged in sorted(staging.iterdir()):
        staged.replace(out / staged.name)  # a rename, because the staging directory is out's sibling
    shutil.rmtree(staging, ignore_errors=True)
    pages = [out / p.name for p in rendered]
    report: dict = {
        "status": "success",
        "file": str(src),
        "out": str(out),
        "dpi": dpi,
        "pages_rendered": len(pages),
        "slides_in_deck": slides,
        "pages": [str(p) for p in pages],
    }
    if hidden:
        report["hidden_slides"] = hidden
    if slides not in (None, len(pages)):
        report["slide_of_page"] = slide_of_page
        report["warning"] = (
            "hidden slides account for the missing pages; LibreOffice leaves them out of the PDF, and the "
            "images and montage tiles are numbered by deck slide, not by page"
            if slide_of_page is not None else
            "rendered page count does not match the deck's slide count; LibreOffice dropped or split a slide, "
            "so which deck slide each page shows is unknown"
        )
    if "--montage" in flags:
        report["montage"] = str(out / "montage.png")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
