#!/usr/bin/env python3
"""Read a deck out as JSON: text, notes, tables, charts, and a shape inventory.

Usage:
    python extract.py <deck.pptx> [--slide N] [--text-only]

Read a deck before editing it. The shape inventory carries the names and
coordinates the edit will need, which is what separates changing three words in
a human's deck from rebuilding it: `markitdown deck.pptx` gives the prose faster
but drops every position, so it answers "what does this say" and never "which
box do I write into".

Charts come back with their series and categories, so a refresh can replace the
numbers in place instead of deleting the chart and adding a new one. Both are
optional in the format, and a chart carrying only the workbook ranges it reads
from reports those instead; a plot type python-pptx does not model is named from
the element in the part and marked unread rather than stopping the run, since a
chart is one shape on one slide and the rest of the deck is still worth reading.

`--slide N` counts from 1 and is an error when the deck has no such slide, rather
than a success carrying an empty `slides` list. A token this usage does not name
is an error too: a misspelt `--slide` used to drop out of the parse unread, so
the whole deck came back as though the flag had never been typed.

A group member is reported where the slide places it, not where the file stores
it. Moving or resizing a group rewrites the group and leaves every child alone,
so a member's stored position is written in a coordinate space of its own and
only the group's own numbers say where that space lands. Rotation is the one
thing left out, on purpose: a rotated shape or group reports its unrotated
frame, which is the number an edit assigns back, and check.py is the script
that sweeps the turned box.

A shape the file marks hidden contributes no text, no table and no chart, the
way check.py leaves one out of every pass, so what comes back is what the slide
shows: a stale figure left hidden beside the live one is not a contradiction and
a hidden `Source:` line does not cite anything. The shape inventory still names
it, carrying `"hidden": true`, since an edit may be what unhides it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.exc import InvalidXmlError

EMU_PER_IN = 914400
A_NS = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
C_NS = "{http://schemas.openxmlformats.org/drawingml/2006/chart}"
P_NS = "{http://schemas.openxmlformats.org/presentationml/2006/main}"
USAGE = "usage: extract.py <deck.pptx> [--slide N] [--text-only]"
TAKES_VALUE = {"--slide": True, "--text-only": False}
SLIDE_SPACE = (0.0, 0.0, 1.0, 1.0)  # shift x, shift y, scale x, scale y, in slide inches
ASSUMED_PT = 18.0  # the size check.py assumes when nothing in the file states one
HIDDEN = {"1", "true"}  # how the file spells a shape nobody is meant to see
FLIPPED = {"1", "true"}  # and how it spells a transform mirrored about its own box
# python-pptx models nine plot types; for the rest its lax element layer hands back a
# plain lxml element, so a chart it does not know raises under any of these four names.
UNMODELLED_CHART = (AttributeError, IndexError, ValueError, NotImplementedError)


def inches(value, scale: float = 1.0, shift: float = 0.0):
    return None if value is None else round(shift + value / EMU_PER_IN * scale, 3)


def near_edge(start, size, scale: float, shift: float):
    """The edge of a frame an edit assigns back, mirrored groups included.

    A flipped group carries a negative scale, which puts a child's stored near
    edge on the far side of the box; the smaller of the pair is the one the
    slide draws first and the one every other coordinate here is measured from.
    """
    if start is None:
        return None
    far = start if size is None else start + size
    return min(inches(start, scale, shift), inches(far, scale, shift))


def rows_height_emu(shape) -> int:
    total = 0
    for row in shape.table.rows:
        try:
            total += row.height
        except InvalidXmlError:  # a row that declares no height, which no writer emits
            continue
    return total


def run_points(run, paragraph) -> float | None:
    """The size this run renders at: its own, or the paragraph default standing behind it.

    check.py resolves a size the same way, under the same name. A box whose size
    is set once on the paragraph carries no run-level size at all, so reading
    only the run reports it as unsized and the title rule below then treats the
    largest text on the slide as the smallest.
    """
    for source in (run.font.size, paragraph.font.size):
        if source is not None:
            return source.pt
    return None


def is_hidden(shape) -> bool:
    """Whether the file marks this shape hidden, which means no renderer draws it.

    The flag sits on the shape's own `p:cNvPr`, and that is the one under its
    non-visual properties: searching its descendants instead would let a member
    of a group answer for the group.
    """
    for child in shape.element:  # the non-visual properties come first
        node = child.find(f"{P_NS}cNvPr")
        if node is not None:
            return node.get("hidden") in HIDDEN
    return False


def kind_of(shape) -> str:
    if getattr(shape, "has_chart", False):
        return "chart"
    if getattr(shape, "has_table", False):
        return "table"
    if shape.shape_type == MSO_SHAPE_TYPE.PICTURE:
        return "picture"
    if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
        return "group"
    if shape.is_placeholder:
        return "placeholder"
    if shape.has_text_frame and shape.text_frame.text.strip():
        return "text"
    return "shape"


def child_space(group, outer: tuple) -> tuple:
    """`outer`, extended by the map from this group's child space onto the slide.

    Resizing a group rewrites its ext and leaves chExt, and every child, alone:
    a child's stored width is the width it had before the resize, and only the
    ratio of the two says how wide the slide draws it, and a flip on the group
    mirrors every member about the group's own box, which is that scale turned
    negative and measured from the far edge. check.py applies the same transform
    and then sweeps any rotation, so the coordinates in this report are the
    frames it audits before the turn.
    """
    xfrm = group.element.find(f"{P_NS}grpSpPr/{A_NS}xfrm")
    try:
        off, ext = xfrm.find(f"{A_NS}off"), xfrm.find(f"{A_NS}ext")
        ch_off, ch_ext = xfrm.find(f"{A_NS}chOff"), xfrm.find(f"{A_NS}chExt")
        scale_x = int(ext.get("cx")) / int(ch_ext.get("cx"))
        scale_y = int(ext.get("cy")) / int(ch_ext.get("cy"))
        if scale_x <= 0 or scale_y <= 0:  # a group scaled to nothing draws nothing
            return outer
        flip_x = xfrm.get("flipH") in FLIPPED
        flip_y = xfrm.get("flipV") in FLIPPED
        scale_x = -scale_x if flip_x else scale_x
        scale_y = -scale_y if flip_y else scale_y
        edge_x = int(ext.get("cx")) if flip_x else 0
        edge_y = int(ext.get("cy")) if flip_y else 0
        shift_x = (int(off.get("x")) + edge_x - int(ch_off.get("x")) * scale_x) / EMU_PER_IN
        shift_y = (int(off.get("y")) + edge_y - int(ch_off.get("y")) * scale_y) / EMU_PER_IN
    except (AttributeError, TypeError, ValueError, ZeroDivisionError):
        return outer
    dx, dy, sx, sy = outer
    return dx + shift_x * sx, dy + shift_y * sy, scale_x * sx, scale_y * sy


def describe_shape(shape, transform: tuple = SLIDE_SPACE) -> dict:
    dx, dy, sx, sy = transform
    entry = {
        "name": shape.name,
        "kind": kind_of(shape),
        "left": near_edge(shape.left, shape.width, sx, dx),
        "top": near_edge(shape.top, shape.height, sy, dy),
        "width": inches(shape.width, abs(sx)),
        "height": inches(shape.height, abs(sy)),
    }
    # The inventory keeps a hidden shape, since an edit may be what unhides it, but
    # nothing it holds reaches `text`, `tables` or `charts`: PowerPoint draws none of it.
    if is_hidden(shape):
        entry["hidden"] = True
    # The frame above is the unrotated one, which is what an edit assigns back; the
    # angle sits beside it so a reader knows the slide turns that frame.
    if getattr(shape, "rotation", 0):
        entry["rotation"] = round(shape.rotation, 2)
    if getattr(shape, "has_table", False):
        # PowerPoint draws a table at the sum of its row heights and ignores the
        # frame height, so `height` is the extent on the slide and the two parts
        # are reported beside it when they disagree.
        rows_height = rows_height_emu(shape)
        entry["height"] = inches(max(shape.height or 0, rows_height), abs(sy))
        entry["frame_height"] = inches(shape.height, abs(sy))
        entry["rows_height"] = inches(rows_height, abs(sy))
    if shape.is_placeholder:
        try:
            entry["placeholder"] = str(shape.placeholder_format.type)
            entry["placeholder_idx"] = shape.placeholder_format.idx
        except (AttributeError, ValueError):
            pass
    if shape.has_text_frame:
        text = shape.text_frame.text
        entry["chars"] = len(text)
        sizes, fonts = set(), set()
        for paragraph in shape.text_frame.paragraphs:
            for run in paragraph.runs:
                points = run_points(run, paragraph)
                if points is not None:
                    sizes.add(round(points, 1))
                if run.font.name:
                    fonts.add(run.font.name)
        if sizes:
            entry["font_sizes_pt"] = sorted(sizes)
        if fonts:
            entry["fonts"] = sorted(fonts)
    if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
        inner = child_space(shape, transform)
        entry["members"] = [describe_shape(child, inner) for child in shape.shapes]
    return entry


def describe_table(shape) -> dict:
    table = shape.table
    rows = [[cell.text for cell in row.cells] for row in table.rows]
    return {
        "shape": shape.name,
        "rows": len(rows),
        "columns": len(rows[0]) if rows else 0,
        "cells": rows,
    }


def cached_numbers(series, tag: str) -> list:
    """The numbers behind one of a series' data sources: `xVal`, `bubbleSize`.

    python-pptx reports only the y values of an XY or bubble series, through
    `.values`, so the x values and the bubble sizes come off the element.
    """
    source = getattr(series._element, tag, None)
    if source is None:
        return []
    try:
        return [source.pt_v(index) for index in range(source.ptCount_val)]
    except ValueError:  # a cache holding labels rather than numbers
        return []


def uncached_range(source) -> str | None:
    """The workbook range a chart data source names, when it caches no points itself.

    `c:numCache` and `c:strCache` are optional: a chart may carry the reference
    alone and PowerPoint redraws it from the workbook embedded beside it. An
    empty list on its own then reads as a series with no numbers rather than as
    one whose numbers this report cannot reach, so the range goes in its place.
    """
    if source is None:
        return None
    if source.find(f"{C_NS}numRef/{C_NS}numCache") is not None:
        return None
    if source.find(f"{C_NS}strRef/{C_NS}strCache") is not None:
        return None
    node = source.find(f".//{C_NS}f")
    return node.text if node is not None and node.text else None


def chart_kind(chart) -> str | None:
    """This chart's type, by the name python-pptx gives it or by the element in the part.

    python-pptx models nine plot types and raises on every other one, so a 3-D
    pie or a stock chart has no `chart_type` at all. The element name is what is
    left, and it still tells the reader what they are looking at.
    """
    try:
        return str(chart.chart_type)
    except UNMODELLED_CHART:
        pass
    plot_area = chart._chartSpace.find(f"{C_NS}chart/{C_NS}plotArea")
    for node in plot_area if plot_area is not None else ():
        if node.tag.endswith("Chart"):
            return node.tag.split("}")[-1]
    return None


def describe_series(series) -> dict:
    x_values = cached_numbers(series, "xVal")
    if x_values:
        entry = {"name": series.name, "x_values": x_values, "y_values": list(series.values)}
        sizes = cached_numbers(series, "bubbleSize")
        if sizes:
            entry["bubble_sizes"] = sizes
    else:
        entry = {"name": series.name, "values": list(series.values)}
    if not entry.get("values") and not entry.get("y_values"):
        ref = uncached_range(getattr(series._element, "yVal" if x_values else "val", None))
        if ref:
            entry["values_ref"] = ref
    return entry


def describe_chart(shape) -> dict:
    """A chart's type, categories and series, or as much of them as it will give up.

    A chart is one shape on one slide, so a plot type python-pptx does not model
    must not reach the caller: it would raise through the whole run and cost the
    inventory of every other slide in the deck.
    """
    chart = shape.chart
    entry = {"shape": shape.name, "type": chart_kind(chart), "series": []}
    try:
        entry["categories"] = [str(c) for c in chart.plots[0].categories]
    except UNMODELLED_CHART:
        entry["categories"] = []
    try:
        for series in chart.series:
            entry["series"].append(describe_series(series))
    except UNMODELLED_CHART as exc:
        entry["series_unread"] = f"python-pptx could not read this plot type ({type(exc).__name__})"
    if not entry["categories"]:
        ref = uncached_range(chart._chartSpace.find(f".//{C_NS}ser/{C_NS}cat"))
        if ref:
            entry["categories_ref"] = ref
    return entry


def walk_shapes(shapes):
    """Every shape the slide draws, in document order, with groups opened out.

    A chart or table inside a group is absent from `slide.shapes` entirely, so
    collecting only the top level reports the group's text and drops the numbers
    behind it. A hidden shape is left out, a hidden group taking its members with
    it, so check-deck never audits a figure the slide does not show.
    """
    for shape in shapes:
        if is_hidden(shape):
            continue
        if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
            yield from walk_shapes(shape.shapes)
        else:
            yield shape


def text_of(shape) -> list[str]:
    out = []
    if is_hidden(shape):  # nothing a hidden box holds is on the slide to read
        return out
    if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
        for child in shape.shapes:
            out.extend(text_of(child))
        return out
    if getattr(shape, "has_table", False):
        # Table text belongs in the prose too, so --text-only returns the whole slide.
        for row in shape.table.rows:
            out.append(" | ".join(cell.text.strip() for cell in row.cells))
        return out
    if shape.has_text_frame:
        for paragraph in shape.text_frame.paragraphs:
            line = paragraph.text.strip()
            if line:
                out.append(line)
    return out


def notes_text(slide) -> str:
    """The speaker notes on this slide, empty when the notes part carries none.

    A notes part with no body placeholder is still a notes part: the text a
    reader typed into the notes pane lives in that placeholder, so without one
    there are no notes to read, and `notes_text_frame` is None rather than an
    empty frame. Reading `.text` straight off it is what stopped both scripts
    dead on a deck whose notes pages hold ordinary boxes instead.
    """
    if not slide.has_notes_slide:
        return ""
    frame = slide.notes_slide.notes_text_frame
    return frame.text.strip() if frame is not None else ""


def title_of(slide, slide_height):
    """The title placeholder, or the largest text in the top 30 percent of the slide.

    A deck built with pptxgenjs has no title placeholder at all, so falling back
    to position is the difference between reporting a title and reporting None
    for every slide in the deck. The fallback is the same rule check.py applies
    for title_drift, down to how a run with no size of its own is sized and to
    passing over a hidden box however large its text, so the two scripts never
    disagree about which box is the title.
    """
    placeholder = slide.shapes.title
    if placeholder is not None and not is_hidden(placeholder) and placeholder.text.strip():
        return placeholder.text, "placeholder"
    best, best_pt, topmost, top_y = None, 0.0, None, None
    for shape in slide.shapes:
        if is_hidden(shape):
            continue
        if not shape.has_text_frame or not shape.text_frame.text.strip() or shape.top is None:
            continue
        if top_y is None or shape.top < top_y:
            topmost, top_y = shape, shape.top
        if shape.top > slide_height * 0.30:
            continue
        sizes = [
            p for para in shape.text_frame.paragraphs
            for p in (run_points(r, para) for r in para.runs) if p
        ]
        size = max(sizes) if sizes else ASSUMED_PT
        if size > best_pt:
            best, best_pt = shape, size
    if best is not None:
        return best.text_frame.paragraphs[0].text.strip(), "largest_in_top_band"
    if topmost is not None:
        return topmost.text_frame.paragraphs[0].text.strip(), "topmost_text"
    return None, None


def fail(message: str) -> None:
    print(json.dumps({"status": "error", "message": message}))
    sys.exit(1)


def extract(path: Path, only_slide: int | None, text_only: bool) -> dict:
    prs = Presentation(str(path))
    if only_slide is not None and not 1 <= only_slide <= len(prs.slides):
        fail(f"--slide {only_slide} selects no slide; this deck has {len(prs.slides)}, numbered from 1")
    report = {
        "file": str(path),
        "slide_size_in": [inches(prs.slide_width), inches(prs.slide_height)],
        "slides": [],
    }
    for index, slide in enumerate(prs.slides, start=1):
        if only_slide is not None and index != only_slide:
            continue
        entry: dict = {"index": index, "layout": slide.slide_layout.name}
        entry["title"], entry["title_from"] = title_of(slide, prs.slide_height)
        entry["text"] = [line for shape in slide.shapes for line in text_of(shape)]
        entry["notes"] = notes_text(slide)
        if not text_only:
            entry["shapes"] = [describe_shape(shape) for shape in slide.shapes]
            flat = list(walk_shapes(slide.shapes))
            tables = [describe_table(s) for s in flat if getattr(s, "has_table", False)]
            charts = [describe_chart(s) for s in flat if getattr(s, "has_chart", False)]
            if tables:
                entry["tables"] = tables
            if charts:
                entry["charts"] = charts
        report["slides"].append(entry)
    return report


def parse_args(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token TAKES_VALUE does not name.

    A misspelt --slide used to drop out of the list unread, so the whole deck
    came back and the caller read slide 1 as the slide it had asked for.
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
    if len(args) > 1:
        fail(f"one deck at a time, and {args[1]} is a second; {USAGE}")
    only_slide = flags.get("--slide")
    if only_slide is not None:
        if not str(only_slide).isdigit():
            fail(f"--slide takes a slide number, not {only_slide}; {USAGE}")
        only_slide = int(only_slide)
    path = Path(args[0]).expanduser().resolve()
    if not path.exists():
        fail(f"no such file: {path}")
    print(json.dumps(extract(path, only_slide, "--text-only" in flags), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1:])
