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
numbers in place instead of deleting the chart and adding a new one.

`--slide N` counts from 1 and is an error when the deck has no such slide, rather
than a success carrying an empty `slides` list.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.exc import InvalidXmlError

EMU_PER_IN = 914400


def inches(value):
    return None if value is None else round(value / EMU_PER_IN, 3)


def rows_height_emu(shape) -> int:
    total = 0
    for row in shape.table.rows:
        try:
            total += row.height
        except InvalidXmlError:  # a row that declares no height, which no writer emits
            continue
    return total


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


def describe_shape(shape) -> dict:
    entry = {
        "name": shape.name,
        "kind": kind_of(shape),
        "left": inches(shape.left),
        "top": inches(shape.top),
        "width": inches(shape.width),
        "height": inches(shape.height),
    }
    if getattr(shape, "has_table", False):
        # PowerPoint draws a table at the sum of its row heights and ignores the
        # frame height, so `height` is the extent on the slide and the two parts
        # are reported beside it when they disagree.
        rows_height = rows_height_emu(shape)
        entry["height"] = inches(max(shape.height or 0, rows_height))
        entry["frame_height"] = inches(shape.height)
        entry["rows_height"] = inches(rows_height)
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
                size = run.font.size or paragraph.font.size
                if size is not None:
                    sizes.add(round(size.pt, 1))
                if run.font.name:
                    fonts.add(run.font.name)
        if sizes:
            entry["font_sizes_pt"] = sorted(sizes)
        if fonts:
            entry["fonts"] = sorted(fonts)
    if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
        entry["members"] = [describe_shape(child) for child in shape.shapes]
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


def describe_series(series) -> dict:
    x_values = cached_numbers(series, "xVal")
    if not x_values:
        return {"name": series.name, "values": list(series.values)}
    entry = {"name": series.name, "x_values": x_values, "y_values": list(series.values)}
    sizes = cached_numbers(series, "bubbleSize")
    if sizes:
        entry["bubble_sizes"] = sizes
    return entry


def describe_chart(shape) -> dict:
    chart = shape.chart
    entry = {"shape": shape.name, "type": str(chart.chart_type), "series": []}
    try:
        entry["categories"] = [str(c) for c in chart.plots[0].categories]
    except (IndexError, ValueError):
        entry["categories"] = []
    for series in chart.series:
        entry["series"].append(describe_series(series))
    return entry


def text_of(shape) -> list[str]:
    out = []
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


def title_of(slide, slide_height):
    """The title placeholder, or the largest text in the top 30 percent of the slide.

    A deck built with pptxgenjs has no title placeholder at all, so falling back
    to position is the difference between reporting a title and reporting None
    for every slide in the deck. The fallback is the same rule check.py applies
    for title_drift, so the two scripts never disagree about which box is the title.
    """
    placeholder = slide.shapes.title
    if placeholder is not None and placeholder.text.strip():
        return placeholder.text, "placeholder"
    best, best_pt, topmost, top_y = None, 0.0, None, None
    for shape in slide.shapes:
        if not shape.has_text_frame or not shape.text_frame.text.strip() or shape.top is None:
            continue
        if top_y is None or shape.top < top_y:
            topmost, top_y = shape, shape.top
        if shape.top > slide_height * 0.30:
            continue
        sizes = [r.font.size.pt for para in shape.text_frame.paragraphs for r in para.runs if r.font.size]
        size = max(sizes) if sizes else 18.0
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
        entry["notes"] = (
            slide.notes_slide.notes_text_frame.text.strip() if slide.has_notes_slide else ""
        )
        if not text_only:
            entry["shapes"] = [describe_shape(shape) for shape in slide.shapes]
            tables = [describe_table(s) for s in slide.shapes if getattr(s, "has_table", False)]
            charts = [describe_chart(s) for s in slide.shapes if getattr(s, "has_chart", False)]
            if tables:
                entry["tables"] = tables
            if charts:
                entry["charts"] = charts
        report["slides"].append(entry)
    return report


def positional(argv: list[str], value_flags: set[str]) -> list[str]:
    """Arguments that are neither a flag nor a flag's value."""
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
        fail("usage: extract.py <deck.pptx> [--slide N] [--text-only]")
    path = Path(args[0]).expanduser().resolve()
    if not path.exists():
        fail(f"no such file: {path}")
    print(json.dumps(extract(path, only_slide, "--text-only" in argv), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1:])
