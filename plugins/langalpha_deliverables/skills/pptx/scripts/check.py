#!/usr/bin/env python3
"""Audit a deck against the rules in SKILL.md, whoever built it.

Usage:
    python check.py <deck.pptx> [--strict] [--require-notes] [--slide N]

Checks, in order of importance:

    package         the zip is intact, every slide part is reachable, no orphan media
    bounds          no shape crosses a slide edge
    overlap         no pair of shapes covers enough of each other to hide content
    text_overflow   no text box holds more text than its height can show
    placeholder     no "Click to add", "Lorem", "TODO" or "[INSERT" left behind
    font_size       body text at 10pt or above, table text at 14pt or above
    fonts           at most two families, all of them metric-safe
    title           the title sits in the same place on every content slide
    notes           speaker notes present on every content slide (--require-notes)
    charts          numeric charts are native chart parts, not pictures of charts

This reads the saved file, so it judges a deck the same way whether pptxgenjs
wrote it, python-pptx edited it, or a human sent it over, and it sees what the
file actually contains, including what a layout or master contributed.

Overflow is an estimate: a proportional font at N points averages close to N/2
points per character, so the line count follows from the box width and the
character count. It is deliberately loose, and it reports lines rather than
pixels, so treat a finding as "go look at the render" rather than as a
measurement. Geometry checks cover top-level shapes; a group is measured as one
box, a table by the sum of its row heights (what PowerPoint draws) rather than by
the frame height stored in the file, and text inside a group is still read for
the content checks.

Findings carry a level: "fail" blocks delivery, "warn" is a judgement call,
"info" is context. With --strict the exit code is 1 when any fail exists.

`--slide N` counts from 1 and is an error when the deck has no such slide, rather
than a clean pass that audited nothing.
"""

from __future__ import annotations

import json
import re
import sys
import zipfile
from pathlib import Path

from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE, PP_PLACEHOLDER
from pptx.enum.text import MSO_AUTO_SIZE
from pptx.exc import InvalidXmlError

EMU_PER_IN = 914400
A_NS = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
P_NS = "{http://schemas.openxmlformats.org/presentationml/2006/main}"
EXAMPLES = 25

EDGE_TOLERANCE_IN = 0.02
OVERLAP_AREA_RATIO = 0.15
OVERLAP_MIN_IN = 0.10
MIN_BODY_PT = 10.0
MIN_TABLE_PT = 14.0
MAX_FONT_FAMILIES = 2
TITLE_DRIFT_IN = 0.05
BIG_PICTURE_AREA = 0.15  # of the slide, above which a bitmap is worth a second look
ASSUMED_PT = 18.0
GLYPH_WIDTH_RATIO = 0.50  # average advance of a proportional face, in ems
BOLD_WIDTH_RATIO = 0.54
LINE_SPACING = 1.20

PLACEHOLDER_TEXT = re.compile(r"click to add|lorem|\bTODO\b|\[INSERT", re.I)
CHART_LIKE_NAME = re.compile(r"chart|graph|plot|\bfig(ure)?\b", re.I)
CONNECTOR_GEOM = {"line", "straightConnector1", "bentConnector2", "bentConnector3", "curvedConnector3"}
# Faces with a metric-compatible clone in every renderer we care about, so the
# render you inspect wraps its lines where PowerPoint will wrap them.
METRIC_SAFE = {
    "arial", "liberation sans", "helvetica",
    "calibri", "carlito",
    "cambria", "caladea",
    "times new roman", "liberation serif",
    "courier new", "liberation mono",
}


class Finding:
    def __init__(self, check: str, level: str, message: str):
        self.check, self.level, self.message = check, level, message
        self.examples: list[str] = []
        self.count = 0

    def add(self, where: str, note: str = "") -> None:
        self.count += 1
        if len(self.examples) < EXAMPLES:
            self.examples.append(f"{where}" + (f" ({note})" if note else ""))

    def as_dict(self) -> dict:
        return {"check": self.check, "level": self.level, "count": self.count,
                "message": self.message, "examples": self.examples}


def emu_in(value) -> float | None:
    return None if value is None else value / EMU_PER_IN


def rows_height_emu(shape) -> int:
    total = 0
    for row in shape.table.rows:
        try:
            total += row.height
        except InvalidXmlError:  # a row that declares no height, which no writer emits
            continue
    return total


def geometry(shape) -> tuple[float, float, float, float] | None:
    """Left, top, width and height in inches, as the slide renders them.

    PowerPoint draws a table at the sum of its row heights and ignores the frame
    height, which pptxgenjs writes as one inch whatever the table holds, so a
    table is measured by the taller of the two.
    """
    try:
        left, top, width, height = shape.left, shape.top, shape.width, shape.height
    except (AttributeError, ValueError):
        return None
    if None in (left, top, width, height):
        return None
    if getattr(shape, "has_table", False):
        height = max(height, rows_height_emu(shape))
    return emu_in(left), emu_in(top), emu_in(width), emu_in(height)


def is_connector(shape) -> bool:
    if shape.shape_type == MSO_SHAPE_TYPE.LINE:
        return True
    geom = shape.element.find(f".//{A_NS}prstGeom")
    return geom is not None and geom.get("prst") in CONNECTOR_GEOM


def alt_text(shape) -> str:
    node = shape.element.find(f".//{P_NS}cNvPr")
    return (node.get("descr") or "") if node is not None else ""


def walk(shapes):
    """Every shape including group members, for content checks."""
    for shape in shapes:
        yield shape
        if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
            yield from walk(shape.shapes)


def shape_text(shape) -> str:
    if shape.has_text_frame:
        return shape.text_frame.text
    if getattr(shape, "has_table", False):
        return "\n".join(c.text for row in shape.table.rows for c in row.cells)
    return ""


def run_points(run, paragraph) -> float | None:
    for source in (run.font.size, paragraph.font.size):
        if source is not None:
            return source.pt
    return None


def estimate_lines(text_frame, width_in: float) -> tuple[float, int]:
    """Points of text height needed, and the line count behind that number."""
    inset_l = emu_in(text_frame.margin_left) if text_frame.margin_left is not None else 0.1
    inset_r = emu_in(text_frame.margin_right) if text_frame.margin_right is not None else 0.1
    usable_pt = max(1.0, (width_in - inset_l - inset_r) * 72)
    needed_pt, lines = 0.0, 0
    for paragraph in text_frame.paragraphs:
        sizes = [p for p in (run_points(r, paragraph) for r in paragraph.runs) if p]
        size_pt = max(sizes) if sizes else ASSUMED_PT
        bold = any(r.font.bold for r in paragraph.runs)
        ratio = BOLD_WIDTH_RATIO if bold else GLYPH_WIDTH_RATIO
        chars_per_line = max(1, int(usable_pt / (size_pt * ratio)))
        text = paragraph.text
        count = 1 if not text else max(1, -(-len(text) // chars_per_line))
        if text_frame.word_wrap is False:
            count = 1
        lines += count
        needed_pt += count * size_pt * LINE_SPACING
        after = getattr(paragraph, "space_after", None)
        if after is not None:
            needed_pt += after.pt
    return needed_pt, lines


def usable_height_pt(text_frame, height_in: float) -> float:
    inset_t = emu_in(text_frame.margin_top) if text_frame.margin_top is not None else 0.05
    inset_b = emu_in(text_frame.margin_bottom) if text_frame.margin_bottom is not None else 0.05
    return max(1.0, (height_in - inset_t - inset_b) * 72)


def title_of(slide, slide_height: float):
    """The title placeholder, or the biggest line of text in the top band."""
    for shape in slide.shapes:
        if shape.is_placeholder and shape.placeholder_format.type in (
            PP_PLACEHOLDER.TITLE, PP_PLACEHOLDER.CENTER_TITLE
        ):
            return shape
    best, best_pt = None, 0.0
    for shape in slide.shapes:
        box = geometry(shape)
        if not box or not shape.has_text_frame or not shape.text_frame.text.strip():
            continue
        if box[1] > slide_height * 0.30:
            continue
        sizes = [
            p for para in shape.text_frame.paragraphs
            for p in (run_points(r, para) for r in para.runs) if p
        ]
        size = max(sizes) if sizes else ASSUMED_PT
        if size > best_pt:
            best, best_pt = shape, size
    return best


def package_report(path: Path, slide_count: int, findings) -> dict:
    stats = {"chart_parts": 0, "images": 0, "slide_parts": 0}
    with zipfile.ZipFile(path) as zf:
        if zf.testzip() is not None:
            findings("package", "fail", "the .pptx zip is corrupt").add(str(path))
        names = zf.namelist()
        stats["slide_parts"] = len([n for n in names if re.fullmatch(r"ppt/slides/slide\d+\.xml", n)])
        stats["chart_parts"] = len([n for n in names if re.fullmatch(r"ppt/charts/chart\d+\.xml", n)])
        media = {n for n in names if n.startswith("ppt/media/") and not n.endswith("/")}
        stats["images"] = len(media)
        referenced = set()
        for name in names:
            if name.endswith(".rels"):
                for target in re.findall(rb'Target="([^"]+)"', zf.read(name)):
                    referenced.add("ppt/" + target.decode().replace("../", ""))
        orphans = sorted(media - referenced)
        if orphans:
            findings("package_media", "warn",
                     "media in the package that no slide references; delete it or place it").add(", ".join(orphans[:5]))
    if stats["slide_parts"] != slide_count:
        findings("package_slides", "fail",
                 "slide parts in the package do not match the slides the presentation lists"
                 ).add(f"{stats['slide_parts']} parts, {slide_count} listed")
    return stats


def fail(message: str) -> None:
    print(json.dumps({"status": "error", "message": message}))
    sys.exit(1)


def check(path: Path, only_slide: int | None, require_notes: bool) -> dict:
    prs = Presentation(str(path))
    if only_slide is not None and not 1 <= only_slide <= len(prs.slides):
        fail(f"--slide {only_slide} selects no slide; this deck has {len(prs.slides)}, numbered from 1")
    slide_w = emu_in(prs.slide_width)
    slide_h = emu_in(prs.slide_height)
    registry: dict[str, Finding] = {}

    def finding(name: str, level: str, message: str) -> Finding:
        if name not in registry:
            registry[name] = Finding(name, level, message)
        return registry[name]

    stats = package_report(path, len(prs.slides), finding)
    stats.update({
        "slides": len(prs.slides),
        "slide_size_in": [round(slide_w, 3), round(slide_h, 3)],
        "aspect": round(slide_w / slide_h, 3) if slide_h else None,
        "tables": 0, "text_shapes": 0, "pictures": 0, "notes_slides": 0,
        "runs_without_explicit_size": 0,
    })
    if stats["aspect"] and abs(stats["aspect"] - 16 / 9) > 0.02:
        finding("aspect", "warn",
                "not 16:9; every screen built this decade is 16:9, so confirm this is what the audience uses"
                ).add(f"{slide_w:.2f} x {slide_h:.2f} in, ratio {stats['aspect']:.3f}")

    fonts_explicit: set[str] = set()
    fonts_inherited = False
    titles: list[tuple[int, float, float]] = []

    for index, slide in enumerate(prs.slides, start=1):
        if only_slide is not None and index != only_slide:
            continue
        where = f"slide {index}"
        boxes: list[tuple[int, str, tuple[float, float, float, float], bool]] = []

        for shape in slide.shapes:
            box = geometry(shape)
            if box is None:
                finding("geometry_unknown", "info",
                        "shape with no position; it inherits one from the layout and was not measured").add(where, shape.shape_type and str(shape.shape_type))
                continue
            left, top, width, height = box
            if width <= 0 or height <= 0:
                finding("zero_size", "warn", "shape with zero width or height").add(where, shape.name)
                continue
            edges = []
            if left < -EDGE_TOLERANCE_IN:
                edges.append(f"left {left:.2f}")
            if top < -EDGE_TOLERANCE_IN:
                edges.append(f"top {top:.2f}")
            if left + width > slide_w + EDGE_TOLERANCE_IN:
                edges.append(f"right {left + width:.2f} > {slide_w:.2f}")
            if top + height > slide_h + EDGE_TOLERANCE_IN:
                edges.append(f"bottom {top + height:.2f} > {slide_h:.2f}")
            if edges:
                finding("bounds", "fail", "shape crosses a slide edge; part of it will never be seen"
                        ).add(where, f"{shape.name}: " + ", ".join(edges))
            if not is_connector(shape):
                has_text = bool(shape_text(shape).strip())
                boxes.append((len(boxes), shape.name, box, has_text))

        for i, (_, name_a, box_a, text_a) in enumerate(boxes):
            for _, name_b, box_b, text_b in boxes[i + 1:]:
                ax, ay, aw, ah = box_a
                bx, by, bw, bh = box_b
                ix = max(ax, bx)
                iy = max(ay, by)
                iw = min(ax + aw, bx + bw) - ix
                ih = min(ay + ah, by + bh) - iy
                if iw <= 0 or ih <= 0:
                    continue
                a_holds_b = ax <= bx and ay <= by and ax + aw >= bx + bw and ay + ah >= by + bh
                b_holds_a = bx <= ax and by <= ay and bx + bw >= ax + aw and by + bh >= ay + ah
                # A card or a full-slide background hides nothing as long as it
                # carries no text itself; an outer box that does, and two boxes
                # sharing one rectangle, are exactly how text goes missing.
                if (a_holds_b and not text_a) or (b_holds_a and not text_b):
                    continue
                ratio = (iw * ih) / min(aw * ah, bw * bh)
                if ratio < OVERLAP_AREA_RATIO or iw < OVERLAP_MIN_IN or ih < OVERLAP_MIN_IN:
                    continue
                level = "fail" if (text_a or text_b) else "warn"
                key = "overlap" if level == "fail" else "overlap_shapes"
                finding(key, level,
                        "shapes cover each other enough to hide content" if level == "fail"
                        else "shapes overlap without text underneath; confirm it is deliberate"
                        ).add(where, f"{name_a} and {name_b}, {iw:.2f} x {ih:.2f} in, {ratio:.0%} of the smaller")

        slide_has_chart = any(getattr(s, "has_chart", False) for s in walk(slide.shapes))
        for shape in walk(slide.shapes):
            text = shape_text(shape)
            if text and PLACEHOLDER_TEXT.search(text):
                snippet = PLACEHOLDER_TEXT.search(text).group(0)
                finding("placeholder", "fail", "template or draft text left in the deck").add(where, f"{shape.name}: {snippet}")
            if shape.shape_type == MSO_SHAPE_TYPE.PICTURE:
                stats["pictures"] += 1
                label = f"{shape.name} {alt_text(shape)}"
                box = geometry(shape)
                area = (box[2] * box[3]) / (slide_w * slide_h) if box else 0
                named_like_a_chart = bool(CHART_LIKE_NAME.search(label))
                # A bitmap is a bitmap; nothing in the file says whether it is a
                # photo or a picture of a bar chart. Name and size are the only
                # signals, so this stays a warning for a human to resolve.
                if named_like_a_chart or (area >= BIG_PICTURE_AREA and not slide_has_chart):
                    finding("charts_as_pictures", "warn",
                            "a picture where a chart may be hiding; a native chart keeps its numbers readable, "
                            "editable, and sharp at any zoom"
                            ).add(where, f"{shape.name}, {area:.0%} of the slide"
                                  + (", named like a chart" if named_like_a_chart else ""))
            if getattr(shape, "has_table", False):
                stats["tables"] += 1
                for row in shape.table.rows:
                    for cell in row.cells:
                        for paragraph in cell.text_frame.paragraphs:
                            for run in paragraph.runs:
                                points = run_points(run, paragraph)
                                if points is None:
                                    stats["runs_without_explicit_size"] += 1
                                elif points < MIN_TABLE_PT:
                                    finding("font_size_table", "fail",
                                            f"table text below {MIN_TABLE_PT:.0f}pt is unreadable from a seat"
                                            ).add(where, f"{points:.0f}pt: {run.text[:30]}")
                                if run.font.name:
                                    fonts_explicit.add(run.font.name)
                                else:
                                    fonts_inherited = True
                continue
            if not shape.has_text_frame:
                continue
            frame = shape.text_frame
            if frame.text.strip():
                stats["text_shapes"] += 1
            for paragraph in frame.paragraphs:
                for run in paragraph.runs:
                    points = run_points(run, paragraph)
                    if points is None:
                        stats["runs_without_explicit_size"] += 1
                    elif points < MIN_BODY_PT and run.text.strip():
                        finding("font_size", "fail",
                                f"body text below {MIN_BODY_PT:.0f}pt; a footnote goes at 10pt, not smaller"
                                ).add(where, f"{points:.0f}pt: {run.text[:30]}")
                    if run.font.name:
                        fonts_explicit.add(run.font.name)
                    elif run.text.strip():
                        fonts_inherited = True

        for shape in slide.shapes:
            if not shape.has_text_frame or not shape.text_frame.text.strip():
                continue
            box = geometry(shape)
            if box is None:
                continue
            frame = shape.text_frame
            if frame.auto_size == MSO_AUTO_SIZE.SHAPE_TO_FIT_TEXT:
                continue
            needed_pt, lines = estimate_lines(frame, box[2])
            available_pt = usable_height_pt(frame, box[3])
            ratio = needed_pt / available_pt
            note = f"{shape.name}: about {lines} line(s) need {needed_pt:.0f}pt in a {available_pt:.0f}pt box"
            if ratio > 1.15:
                finding("text_overflow", "fail",
                        "more text than the box can show; it will be clipped or spill over the next element").add(where, note)
            elif ratio > 1.0:
                finding("text_tight", "warn",
                        "text fills its box with no margin for a font substitution; check the render").add(where, note)

        title = title_of(slide, slide_h)
        if title is not None:
            box = geometry(title)
            if box:
                titles.append((index, round(box[0], 3), round(box[1], 3)))
        if slide.has_notes_slide and slide.notes_slide.notes_text_frame.text.strip():
            stats["notes_slides"] += 1
        elif require_notes and index > 1:
            finding("notes", "fail", "content slide with no speaker notes").add(where)

    content_titles = titles[1:]  # the cover legitimately places its title elsewhere
    if len(content_titles) >= 3:
        modal = max({(x, y) for _, x, y in content_titles},
                    key=lambda pos: sum(1 for _, x, y in content_titles if (x, y) == pos))
        for index, x, y in content_titles:
            if abs(x - modal[0]) > TITLE_DRIFT_IN or abs(y - modal[1]) > TITLE_DRIFT_IN:
                finding("title_drift", "warn",
                        f"title away from the deck's own title position ({modal[0]:.2f}, {modal[1]:.2f} in); "
                        "a title that moves between slides reads as a jitter"
                        ).add(f"slide {index}", f"{x:.2f}, {y:.2f}")

    fonts_used = set(fonts_explicit)
    theme_fonts = read_theme_fonts(path)
    if fonts_inherited:
        fonts_used |= set(theme_fonts.values())
    if len(fonts_used) > MAX_FONT_FAMILIES:
        finding("fonts", "fail",
                f"more than {MAX_FONT_FAMILIES} font families; a deck reads as one document with one or two"
                ).add(", ".join(sorted(fonts_used)))
    unsafe = sorted(f for f in fonts_used if f.lower() not in METRIC_SAFE)
    if unsafe:
        finding("fonts_metric", "warn",
                "font with no metric-compatible clone in the renderer; the layout you check here is not the "
                "layout the reader gets").add(", ".join(unsafe))
    if stats["runs_without_explicit_size"]:
        finding("font_size_inherited", "info",
                "text runs with no explicit size; they take whatever the layout hands them, so the size rule "
                "could not be checked on them").add(f"{stats['runs_without_explicit_size']} run(s)")

    stats["fonts_used"] = sorted(fonts_used)
    stats["fonts_theme"] = theme_fonts
    results = [f.as_dict() for f in registry.values()]
    results.sort(key=lambda d: ({"fail": 0, "warn": 1, "info": 2}[d["level"]], d["check"]))
    fails = sum(1 for d in results if d["level"] == "fail")
    return {"status": "fail" if fails else "pass", "file": str(path), "stats": stats, "findings": results}


def read_theme_fonts(path: Path) -> dict:
    fonts = {}
    try:
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if re.fullmatch(r"ppt/theme/theme\d+\.xml", n)]
            if not names:
                return fonts
            xml = zf.read(sorted(names)[0]).decode("utf-8", "replace")
    except Exception:
        return fonts
    for key, tag in (("major", "majorFont"), ("minor", "minorFont")):
        match = re.search(rf"<a:{tag}>\s*<a:latin typeface=\"([^\"]*)\"", xml)
        if match and match.group(1):
            fonts[key] = match.group(1)
    return fonts


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
    args = positional(argv, {"--slide"})
    only_slide = int(argv[argv.index("--slide") + 1]) if "--slide" in argv else None
    if not args:
        fail("usage: check.py <deck.pptx> [--strict] [--require-notes] [--slide N]")
    path = Path(args[0]).expanduser().resolve()
    if not path.exists():
        fail(f"no such file: {path}")
    report = check(path, only_slide, "--require-notes" in argv)
    print(json.dumps(report, indent=2))
    if "--strict" in argv and report["status"] == "fail":
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
