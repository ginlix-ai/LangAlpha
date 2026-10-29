#!/usr/bin/env python3
"""Audit a deck against the rules in SKILL.md, whoever built it.

Usage:
    python check.py <deck.pptx> [--strict] [--require-notes] [--slide N]

Checks, in order of importance:

    package         the zip is intact, every slide part is reachable, no orphan media
    bounds          no shape crosses a slide edge
    overlap         no pair of shapes covers enough of each other to hide content
    occluded        no filled shape is painted over text that sits behind it
    text_overflow   no text box holds more text than its height can show
    placeholder     no "Click to add", "Lorem", "TODO" or "[INSERT" left behind
    font_size       body text at 10pt or above, table text at 14pt or above
    fonts           at most two families, all of them metric-safe
    title           the title sits in the same place on every content slide
    notes           speaker notes present on every content slide (--require-notes)
    charts          numeric charts are native chart parts, not pictures of charts

This reads the saved file, so it judges a deck the same way whether pptxgenjs
wrote it, python-pptx edited it, or a human sent it over, and it sees what a
placeholder inherits from its layout or master, its position, its point size
and the theme font included. The boxes it compares are the slide's own shape
tree: a logo or a footer strip that lives on the layout or the master is not
among them. A shape the file marks hidden is left out of every pass, since
PowerPoint draws none of it, and named once under `hidden_shapes` so the reader
knows it is in the file.

Every size here is the size PowerPoint draws. Text autofitted to its box keeps the
size its author typed on each run and records the shrink beside it, so a run stating
32pt in a body scaled to 62.5% is read, reported and judged as 20pt, and the finding
names both sizes so the author can still find the 32pt they typed.

Overflow is an estimate: a proportional font at N points averages close to N/2
points per character, so the line count follows from the box width and the
character count. It is deliberately loose, and it reports lines rather than
pixels, so treat a finding as "go look at the render" rather than as a
measurement. Geometry checks reach inside a group, since a group paints nothing
of its own: every member is measured where the group's scale puts it and takes
the group's place in the paint order. A rotated group is no exception, its angle
becoming a turn about the group's centre that every member takes on top of any
turn of its own, a flipped group no exception either, its mirror deciding which
side of the group each member lands on, and a group inside a group composing
them. A table is measured by the sum of its row heights (what PowerPoint draws)
rather than by the frame height stored in the file, and a rotated shape by the
box its corners sweep out rather than the rectangle stored in the file. Text
inside a group is still read for the content checks and measured for overflow in
the box the group's own scale gives it rather than the box stored on the child:
resizing a group rescales that box and leaves the point size alone, so an 18pt
run inside a group at half scale still renders at 18pt.

Findings carry a level: "fail" blocks delivery, "warn" is a judgement call,
"info" is context. With --strict the exit code is 1 when any fail exists.

`--slide N` counts from 1 and is an error when the deck has no such slide, rather
than a clean pass that audited nothing. A flag outside the three above is an
error too, rather than a token dropped in silence that leaves --strict off.
"""

from __future__ import annotations

import json
import math
import re
import sys
import zipfile
from pathlib import Path

from lxml import etree
from pptx import Presentation
from pptx.enum.dml import MSO_FILL
from pptx.enum.shapes import MSO_SHAPE_TYPE, PP_PLACEHOLDER
from pptx.enum.text import MSO_AUTO_SIZE
from pptx.exc import InvalidXmlError
from pptx.opc.constants import RELATIONSHIP_TYPE as RT

EMU_PER_IN = 914400
A_NS = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
P_NS = "{http://schemas.openxmlformats.org/presentationml/2006/main}"
EXAMPLES = 25
USAGE = "usage: check.py <deck.pptx> [--strict] [--require-notes] [--slide N]"
TAKES_VALUE = {"--strict": False, "--require-notes": False, "--slide": True}

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
AUTOFIT_UNIT = 100000.0  # normAutofit writes a percentage as thousandths of one
SLIDE_SPACE = (0.0, 0.0, 1.0, 1.0)  # shift x, shift y, scale x, scale y, in slide inches
NO_SPIN = (0.0, 0.0, 0.0)  # degrees turned, then the slide-inch shift the turn leaves behind

TITLE_PLACEHOLDERS = (PP_PLACEHOLDER.TITLE, PP_PLACEHOLDER.CENTER_TITLE, PP_PLACEHOLDER.VERTICAL_TITLE)
CHROME_PLACEHOLDERS = (PP_PLACEHOLDER.DATE, PP_PLACEHOLDER.FOOTER,
                       PP_PLACEHOLDER.SLIDE_NUMBER, PP_PLACEHOLDER.HEADER)
MASTER_TEXT_STYLE = {"title": "titleStyle", "body": "bodyStyle", "other": "otherStyle"}
MASTER_PLACEHOLDER = {"title": TITLE_PLACEHOLDERS,
                      "body": (PP_PLACEHOLDER.BODY, PP_PLACEHOLDER.VERTICAL_BODY)}
THEME_FACE = {"+mj-lt": "major", "+mn-lt": "minor"}  # what a style writes instead of a family name
HIDDEN = {"1", "true"}  # how the file spells a shape nobody is meant to see
FLIPPED = {"1", "true"}  # and how it spells a transform mirrored about its own box

PLACEHOLDER_TEXT = re.compile(r"click to add|lorem|\bTODO\b|\[INSERT", re.I)
CHART_LIKE_NAME = re.compile(r"chart|graph|plot|\bfig(ure)?\b", re.I)
CONNECTOR_GEOM = {"line", "straightConnector1", "bentConnector2", "bentConnector3", "curvedConnector3"}
COLOUR_TAGS = {f"{A_NS}{tag}" for tag in
               ("srgbClr", "schemeClr", "prstClr", "sysClr", "scrgbClr", "hslClr")}
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


def rotation_of(shape) -> float:
    """Degrees this shape is spun through, from 0 to under 360, 0 for one that cannot say."""
    try:
        return float(getattr(shape, "rotation", 0.0) or 0.0) % 360
    except (AttributeError, ValueError):
        return 0.0


def turn(x: float, y: float, angle: float) -> tuple[float, float]:
    """(x, y) spun about the origin by `angle`, clockwise, the way DrawingML counts it."""
    if angle == 0:
        return x, y
    rad = math.radians(angle)
    cos_a, sin_a = math.cos(rad), math.sin(rad)
    return x * cos_a - y * sin_a, x * sin_a + y * cos_a


def swept(centre_x: float, centre_y: float, width: float, height: float,
          angle: float) -> tuple[float, float, float, float]:
    """The axis-aligned box a `width` by `height` rectangle covers once spun `angle`."""
    if angle == 0:
        return centre_x - width / 2, centre_y - height / 2, width, height
    cos_a = abs(math.cos(math.radians(angle)))
    sin_a = abs(math.sin(math.radians(angle)))
    spun_w = width * cos_a + height * sin_a
    spun_h = width * sin_a + height * cos_a
    return centre_x - spun_w / 2, centre_y - spun_h / 2, spun_w, spun_h


def rendered_geometry(shape) -> tuple[float, float, float, float] | None:
    """The axis-aligned box the shape actually paints, rotation included.

    PowerPoint spins the stored rectangle around its centre, so a rotated shape
    can cross an edge, or cover a neighbour, that the stored rectangle clears.
    Text still wraps inside the unrotated box, so only bounds and overlap read
    this one.
    """
    box = geometry(shape)
    if box is None:
        return None
    angle = rotation_of(shape)
    if angle == 0:
        return box
    left, top, width, height = box
    return swept(left + width / 2, top + height / 2, width, height, angle)


def nothing_left(node) -> bool:
    """Whether every colour under this fill element is turned all the way down.

    Transparency rides on the colour rather than on the fill, in an `a:alpha`
    child counted in thousandths of a percent, so a colour that states none is
    opaque. A gradient is clear only when every one of its stops is.
    """
    stops = node.findall(f"{A_NS}gsLst/{A_NS}gs")
    if stops:
        return all(nothing_left(stop) for stop in stops)
    for child in node:
        if child.tag not in COLOUR_TAGS:
            continue
        alpha = child.find(f"{A_NS}alpha")
        if alpha is None:
            return False
        raw = (alpha.get("val") or "").strip()
        try:
            return (float(raw[:-1]) if raw.endswith("%") else float(raw)) == 0
        except ValueError:
            return False
    return False


def clear_fill(shape) -> bool:
    """Whether the fill this shape states is 100% transparent, so it paints nothing.

    PowerPoint's transparency slider writes an alpha on the colour, and at 100%
    the rectangle is a guide the author left behind rather than a card over the
    words. Anything short of that still tints what sits underneath, and grading a
    wash is guesswork, so only a fill with nothing left of it is let through.
    """
    props = shape.element.find(f"{P_NS}spPr")
    if props is None:
        return False
    for tag in ("solidFill", "gradFill"):
        node = props.find(f"{A_NS}{tag}")
        if node is not None:
            return nothing_left(node)
    return False


def paints_over(shape) -> bool:
    """Whether this shape lays down pixels of its own over whatever is behind it.

    Only a fill the file states counts, and only while something is left of it:
    a colour the author turned down to 100% transparency covers nothing. A shape
    with no fill element inherits one, and the style's `fillRef` is what it
    inherits: idx 0 is no fill, and any other index picks a theme fill that
    paints, which is what python-pptx writes for a rectangle nobody coloured. A
    group has no fill to read because only its members paint, so it is left alone.
    """
    try:
        fill = shape.fill.type
    except (AttributeError, NotImplementedError, ValueError):
        # A picture, a chart or a table is a rectangle of pixels either way.
        return shape.shape_type != MSO_SHAPE_TYPE.GROUP
    if fill == MSO_FILL.BACKGROUND:
        return False
    if fill is not None:
        return not clear_fill(shape)
    ref = shape.element.find(f"{P_NS}style/{A_NS}fillRef")
    return ref is not None and ref.get("idx") not in (None, "0")


def is_connector(shape) -> bool:
    if shape.shape_type == MSO_SHAPE_TYPE.LINE:
        return True
    geom = shape.element.find(f".//{A_NS}prstGeom")
    return geom is not None and geom.get("prst") in CONNECTOR_GEOM


def alt_text(shape) -> str:
    node = shape.element.find(f".//{P_NS}cNvPr")
    return (node.get("descr") or "") if node is not None else ""


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


def hidden_shapes(shapes, prefix: str = ""):
    """Every shape the file hides, a hidden group standing in for all it holds."""
    for shape in shapes:
        name = prefix + shape.name
        if is_hidden(shape):
            yield name
        elif shape.shape_type == MSO_SHAPE_TYPE.GROUP:
            yield from hidden_shapes(shape.shapes, f"{name}/")


def child_space(group, outer: tuple) -> tuple:
    """`outer`, extended by the map from this group's child space onto the slide.

    Resizing a group rewrites its ext and leaves chExt, and every child, alone:
    a child's stored width is the width it had before the resize, and only the
    ratio of the two says how wide the slide draws it. A flip mirrors every
    member about the group's own box, which is that scale turned negative and
    measured from the far edge: it decides which side of the group a member
    lands on, so reading the group as a move and a stretch alone audits each
    member at the position the slide gives its opposite number.
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
        shift_x = emu_in(int(off.get("x")) + edge_x - int(ch_off.get("x")) * scale_x)
        shift_y = emu_in(int(off.get("y")) + edge_y - int(ch_off.get("y")) * scale_y)
    except (AttributeError, TypeError, ValueError, ZeroDivisionError):
        return outer
    dx, dy, sx, sy = outer
    return dx + shift_x * sx, dy + shift_y * sy, scale_x * sx, scale_y * sy


def placed(box, transform: tuple) -> tuple[float, float, float, float] | None:
    """`box` in slide inches, read through the transform of every group holding it.

    A mirrored group carries a negative scale, which swaps the box's two edges
    rather than giving it a negative width, so the near edge is whichever of the
    pair the mirror leaves on the left.
    """
    if box is None:
        return None
    dx, dy, sx, sy = transform
    left, top, width, height = box
    near_x, far_x = dx + left * sx, dx + (left + width) * sx
    near_y, far_y = dy + top * sy, dy + (top + height) * sy
    return min(near_x, far_x), min(near_y, far_y), abs(width * sx), abs(height * sy)


def walk(shapes, transform: tuple = SLIDE_SPACE):
    """Every shape it renders, group members included, each boxed in slide inches."""
    for shape in shapes:
        if is_hidden(shape):
            continue
        yield shape, placed(geometry(shape), transform)
        if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
            yield from walk(shape.shapes, child_space(shape, transform))


def group_spin(group, transform: tuple, spin: tuple) -> tuple:
    """`spin` with this group's own turn folded in, for measuring what is inside it.

    A group's angle spins its members about the group's centre, which no member
    records, so the turn has to be carried down to them. A turn about a centre is
    a turn about the origin followed by a shift, and two of those compose into
    one of the same shape, which is what lets a rotated group inside a rotated
    group resolve without a matrix.
    """
    angle = rotation_of(group)
    box = placed(geometry(group), transform)
    if angle == 0 or box is None:
        return spin
    left, top, width, height = box
    centre_x, centre_y = left + width / 2, top + height / 2
    turned_x, turned_y = turn(centre_x, centre_y, angle)
    outer_angle, outer_x, outer_y = spin
    shift_x, shift_y = turn(centre_x - turned_x, centre_y - turned_y, outer_angle)
    return angle + outer_angle, shift_x + outer_x, shift_y + outer_y


def painted_box(shape, transform: tuple, spin: tuple) -> tuple[float, float, float, float] | None:
    """Where this shape lands on the slide, its own rotation and its groups' composed."""
    angle, shift_x, shift_y = spin
    if angle == 0:
        return placed(rendered_geometry(shape), transform)
    box = placed(geometry(shape), transform)
    if box is None:
        return None
    left, top, width, height = box
    centre_x, centre_y = turn(left + width / 2, top + height / 2, angle)
    return swept(centre_x + shift_x, centre_y + shift_y, width, height, angle + rotation_of(shape))


def paint_order(shapes, transform: tuple = SLIDE_SPACE, spin: tuple = NO_SPIN, prefix: str = ""):
    """Every shape that paints, in paint order, with its box in slide inches.

    A group paints nothing of its own, so it stands aside for its members: each
    is measured where the group's transform puts it and takes the group's slot in
    the order, members painting among themselves in their own order. Measuring
    the container instead hides everything inside it from the overlap and
    occlusion rules, which read text and fill off the shape they are handed: a
    group answers "no text, no fill" whatever it holds, so a card dropped over
    grouped text covered nothing and a grouped line of text collided with
    nothing. A rotated group stands aside too: its angle is carried down as the
    turn its members take about the group's centre, on top of the turn each was
    given of its own. A hidden shape paints nothing and a hidden group hides
    every member, so neither reaches the order at all.
    """
    for shape in shapes:
        if is_hidden(shape):
            continue
        name = prefix + shape.name
        members = shape.shapes if shape.shape_type == MSO_SHAPE_TYPE.GROUP else ()
        if len(members):
            yield from paint_order(members, child_space(shape, transform),
                                   group_spin(shape, transform, spin), f"{name}/")
        else:
            yield shape, name, painted_box(shape, transform, spin)


def shape_text(shape) -> str:
    if shape.has_text_frame:
        return shape.text_frame.text
    if getattr(shape, "has_table", False):
        return "\n".join(c.text for row in shape.table.rows for c in row.cells)
    return ""


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


def level_face(style, level: int) -> str | None:
    """The latin face a list style or a master text style sets for this outline level."""
    if style is None:
        return None
    latin = style.find(f"{A_NS}lvl{level + 1}pPr/{A_NS}defRPr/{A_NS}latin")
    return latin.get("typeface") if latin is not None else None


def level_size(style, level: int) -> float | None:
    """The point size a list style or a master text style sets for this outline level."""
    if style is None:
        return None
    props = style.find(f"{A_NS}lvl{level + 1}pPr/{A_NS}defRPr")
    raw = props.get("sz") if props is not None else None
    try:
        return int(raw) / 100 if raw else None
    except ValueError:  # a size that is not a number is a size nobody can honour
        return None


def text_body(paragraph):
    """The text body this paragraph lives in, a shape's or a table cell's alike."""
    return paragraph._p.getparent()


def body_list_style(paragraph):
    """The list style on the body this paragraph lives in, shape or table cell alike."""
    body = text_body(paragraph)
    return None if body is None else body.find(f"{A_NS}lstStyle")


def autofit_percent(raw, default: float) -> float:
    """One normAutofit percentage as a fraction: `62500` and `"62.5%"` both read 0.625.

    A shrink outside (0, 1] is one no renderer would apply, so it reads as no shrink
    at all rather than as a size the deck then gets judged by.
    """
    if raw is None:
        return default
    text = str(raw).strip()
    try:
        value = float(text[:-1]) / 100 if text.endswith("%") else float(text) / AUTOFIT_UNIT
    except ValueError:
        return default
    return value if 0.0 < value <= 1.0 else default


def autofit(body) -> tuple[float, float]:
    """The font scale and line-spacing reduction an autofitted body was shrunk by.

    PowerPoint shrinks text to fit its box by recording the shrink on `a:normAutofit`
    and drawing the smaller text, leaving every run's own `sz` at the size its author
    typed: a size read off the run is the size before the shrink. A bare
    `<a:normAutofit/>` records none and means none, which is what pptxgenjs writes on
    every text box and what PowerPoint leaves at full size.
    """
    fit = None if body is None else body.find(f"{A_NS}bodyPr/{A_NS}normAutofit")
    if fit is None:
        return 1.0, 0.0
    return autofit_percent(fit.get("fontScale"), 1.0), autofit_percent(fit.get("lnSpcReduction"), 0.0)


def size_note(points: float, paragraph) -> str:
    """How a size reads in a finding, naming the autofit whenever one shrank it.

    The author typed 32pt and will go looking for 32pt, so the size they can search
    for has to sit beside the size the slide actually shows.
    """
    scale, _ = autofit(text_body(paragraph))
    if scale == 1.0:
        return f"{points:.0f}pt"
    return f"{points / scale:.0f}pt scaled to {points:.0f}pt by autofit"


def shape_list_style(shape):
    return shape.element.find(f"{P_NS}txBody/{A_NS}lstStyle")


def is_title(shape) -> bool:
    return (getattr(shape, "is_placeholder", False)
            and shape.placeholder_format.type in TITLE_PLACEHOLDERS)


def placeholder_kind(shape) -> str:
    """Which of the master's three text styles dresses this placeholder.

    A date, footer or slide-number placeholder takes the otherStyle; handing it
    the body style reads a 32pt bullet size onto a 12pt footer. Everything else
    that is not a title is body, an `obj` content placeholder included, which is
    what a slide writes when it states no type at all.
    """
    kind = shape.placeholder_format.type
    if kind in TITLE_PLACEHOLDERS:
        return "title"
    return "other" if kind in CHROME_PLACEHOLDERS else "body"


def inherited_styles(shape, slide, kind: str):
    """The defaults a placeholder inherits, layout first, then master.

    The layout placeholder is the one sharing this placeholder's idx; a slide
    carrying an idx its layout never defined falls back to the one sharing its
    type, rather than skipping the layout and reading the master's size onto
    text the layout had already resized.
    """
    try:
        layout = slide.slide_layout
        master = layout.slide_master
    except AttributeError:  # a slide whose layout part the package does not carry
        return
    idx = shape.placeholder_format.idx
    ph_type = shape.placeholder_format.type
    on_layout = next((ph for ph in layout.placeholders if ph.placeholder_format.idx == idx), None)
    if on_layout is None:
        on_layout = next((ph for ph in layout.placeholders if ph.placeholder_format.type == ph_type), None)
    if on_layout is not None:
        yield shape_list_style(on_layout)
    for ph in master.placeholders:
        if ph.placeholder_format.type in MASTER_PLACEHOLDER.get(kind, (ph_type,)):
            yield shape_list_style(ph)
            break
    yield master.element.find(f"{P_NS}txStyles/{P_NS}{MASTER_TEXT_STYLE[kind]}")


def stated_points(run, paragraph, shape, slide) -> float | None:
    """The size this run states or inherits, before any autofit shrank it.

    A placeholder in a deck PowerPoint wrote normally states no size anywhere in
    the slide, so stopping at the run and the paragraph reads "unknown" on text
    the master has already set to 8pt. The chain is the one the family walks.
    """
    for source in (run.font.size, paragraph.font.size):
        if source is not None:
            return source.pt
    level = paragraph.level or 0
    points = level_size(body_list_style(paragraph), level)
    if points is None and getattr(shape, "is_placeholder", False):
        for style in inherited_styles(shape, slide, placeholder_kind(shape)):
            points = level_size(style, level)
            if points is not None:
                break
    return points


def run_points(run, paragraph, shape, slide) -> float | None:
    """The size this run is drawn at: the one it states or inherits, shrunk by autofit.

    Autofit is invisible in the run, so a body shrunk to 62.5% reads 32pt off a run
    PowerPoint draws at 20pt, and the minimum-size and overflow checks would both be
    judging a size nobody sees.
    """
    points = stated_points(run, paragraph, shape, slide)
    scale, _ = autofit(text_body(paragraph))
    return None if points is None else points * scale


def run_face(run, paragraph, shape, slide, theme: dict) -> str | None:
    """The family this run renders in, its own or the one it inherits.

    PowerPoint resolves a missing family down a chain and so does this: the
    paragraph's defaults, the body's list style, then, for a placeholder, the
    same defaults on its layout and master, and last the theme, whose major face
    dresses a title and whose minor face everything else.
    """
    title = is_title(shape)
    level = paragraph.level or 0
    face = run.font.name or paragraph.font.name or level_face(body_list_style(paragraph), level)
    if face is None and getattr(shape, "is_placeholder", False):
        for style in inherited_styles(shape, slide, placeholder_kind(shape)):
            face = level_face(style, level)
            if face:
                break
    if face is None:
        face = theme.get("major" if title else "minor")
    return theme.get(THEME_FACE[face]) if face in THEME_FACE else face


def estimate_lines(shape, slide, width_in: float) -> tuple[float, int]:
    """Points of text height needed, and the line count behind that number."""
    text_frame = shape.text_frame
    inset_l = emu_in(text_frame.margin_left) if text_frame.margin_left is not None else 0.1
    inset_r = emu_in(text_frame.margin_right) if text_frame.margin_right is not None else 0.1
    usable_pt = max(1.0, (width_in - inset_l - inset_r) * 72)
    # Autofit tightens the leading as well as the text, and run_points has already
    # taken the scale off the sizes, so both sides of the estimate are what is drawn.
    scale, reduction = autofit(text_frame._txBody)
    line_spacing = LINE_SPACING * (1.0 - reduction)
    needed_pt, lines = 0.0, 0
    for paragraph in text_frame.paragraphs:
        sizes = [p for p in (run_points(r, paragraph, shape, slide) for r in paragraph.runs) if p]
        size_pt = max(sizes) if sizes else ASSUMED_PT * scale
        bold = any(r.font.bold for r in paragraph.runs)
        ratio = BOLD_WIDTH_RATIO if bold else GLYPH_WIDTH_RATIO
        chars_per_line = max(1, int(usable_pt / (size_pt * ratio)))
        # A soft line break (<a:br/>) lives inside a paragraph and reaches us as
        # "\v"; each side of it starts a new line and wraps on its own.
        segments = paragraph.text.split("\v")
        count = sum(max(1, -(-len(seg) // chars_per_line)) for seg in segments)
        if text_frame.word_wrap is False:
            count = len(segments)
        lines += count
        needed_pt += count * size_pt * line_spacing
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
        if is_hidden(shape):
            continue
        if shape.is_placeholder and shape.placeholder_format.type in (
            PP_PLACEHOLDER.TITLE, PP_PLACEHOLDER.CENTER_TITLE
        ):
            return shape
    best, best_pt = None, 0.0
    for shape in slide.shapes:
        box = geometry(shape)
        if is_hidden(shape) or not box or not shape.has_text_frame or not shape.text_frame.text.strip():
            continue
        if box[1] > slide_height * 0.30:
            continue
        sizes = [
            p for para in shape.text_frame.paragraphs
            for p in (run_points(r, para, shape, slide) for r in para.runs) if p
        ]
        size = max(sizes) if sizes else ASSUMED_PT
        if size > best_pt:
            best, best_pt = shape, size
    return best


def title_position(content_titles: list[tuple[int, float, float]]) -> tuple[float, float]:
    """The position the deck's own titles agree on: the centre of the largest cluster.

    Counting exact matches makes every sub-tolerance jitter a position of its
    own, so every count is one and the reference is whichever position iteration
    reaches first, a lone stray included; clustering inside the same tolerance
    the check reports on keeps the reference with the crowd.
    """
    best: list[tuple[float, float]] = []
    for _, cx, cy in content_titles:
        near = [(x, y) for _, x, y in content_titles
                if abs(x - cx) <= TITLE_DRIFT_IN and abs(y - cy) <= TITLE_DRIFT_IN]
        if len(near) > len(best):
            best = near
    return sum(x for x, _ in best) / len(best), sum(y for _, y in best) / len(best)


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
        "runs_with_unknown_size": 0,
    })
    if stats["aspect"] and abs(stats["aspect"] - 16 / 9) > 0.02:
        finding("aspect", "warn",
                "not 16:9; every screen built this decade is 16:9, so confirm this is what the audience uses"
                ).add(f"{slide_w:.2f} x {slide_h:.2f} in, ratio {stats['aspect']:.3f}")

    package_theme = read_theme_fonts(path)
    theme_by_master: dict[str, dict] = {}
    try:
        for master in prs.slide_masters:
            theme_by_master[str(master.part.partname)] = master_theme_fonts(master, package_theme)
    except (AttributeError, KeyError):  # a package that does not carry its masters
        pass

    def slide_theme(slide) -> dict:
        """The faces this slide's own master hands it."""
        try:
            master = slide.slide_layout.slide_master
            key = str(master.part.partname)
        except (AttributeError, KeyError):
            return package_theme
        if key not in theme_by_master:
            theme_by_master[key] = master_theme_fonts(master, package_theme)
        return theme_by_master[key]

    # Arial and arial are one family, so the key is casefolded; the value is the
    # spelling the deck used first, which is the one worth reporting back.
    fonts_used: dict[str, str] = {}
    titles: list[tuple[int, float, float]] = []

    for index, slide in enumerate(prs.slides, start=1):
        if only_slide is not None and index != only_slide:
            continue
        where = f"slide {index}"
        theme_fonts = slide_theme(slide)
        boxes: list[tuple[str, tuple[float, float, float, float], bool, bool]] = []

        for name in hidden_shapes(slide.shapes):
            finding("hidden_shapes", "info",
                    "shape the file marks hidden; PowerPoint draws none of it, so no rule was applied to it"
                    ).add(where, name)

        for shape, name, box in paint_order(slide.shapes):
            if box is None:
                finding("geometry_unknown", "info",
                        "shape with no position; it inherits one from the layout and was not measured").add(where, shape.shape_type and str(shape.shape_type))
                continue
            left, top, width, height = box
            if width <= 0 or height <= 0:
                finding("zero_size", "warn", "shape with zero width or height").add(where, name)
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
                        ).add(where, f"{name}: " + ", ".join(edges))
            if not is_connector(shape):
                has_text = bool(shape_text(shape).strip())
                boxes.append((name, box, has_text, paints_over(shape)))

        # boxes is in paint order, so a is painted before b in every pair.
        for i, (name_a, box_a, text_a, _) in enumerate(boxes):
            for name_b, box_b, text_b, opaque_b in boxes[i + 1:]:
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
                # carries no text itself and the slide paints it first; an outer
                # box that carries text, and two boxes sharing one rectangle,
                # are exactly how text goes missing.
                if a_holds_b and not text_a:
                    continue
                if b_holds_a and not text_b:
                    # The same card added after the text it covers hides every
                    # word of it, and every pair of coordinates still agrees
                    # with the layout, so only the paint order gives it away.
                    if text_a and opaque_b:
                        finding("occluded", "fail",
                                "a filled shape painted over text hides it; send it behind the text or clear its fill"
                                ).add(where, f"{name_b} covers {name_a}, {aw:.2f} x {ah:.2f} in, all of it")
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

        slide_has_chart = any(getattr(s, "has_chart", False) for s, _ in walk(slide.shapes))
        for shape, box in walk(slide.shapes):
            text = shape_text(shape)
            if text and PLACEHOLDER_TEXT.search(text):
                snippet = PLACEHOLDER_TEXT.search(text).group(0)
                finding("placeholder", "fail", "template or draft text left in the deck").add(where, f"{shape.name}: {snippet}")
            if shape.shape_type == MSO_SHAPE_TYPE.PICTURE:
                stats["pictures"] += 1
                label = f"{shape.name} {alt_text(shape)}"
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
                                points = run_points(run, paragraph, shape, slide)
                                if points is None:
                                    stats["runs_with_unknown_size"] += 1
                                elif points < MIN_TABLE_PT:
                                    finding("font_size_table", "fail",
                                            f"table text below {MIN_TABLE_PT:.0f}pt is unreadable from a seat"
                                            ).add(where, f"{size_note(points, paragraph)}: {run.text[:30]}")
                                face = run_face(run, paragraph, shape, slide, theme_fonts)
                                if face and run.text.strip():
                                    fonts_used.setdefault(face.casefold(), face)
                continue
            if not shape.has_text_frame:
                continue
            frame = shape.text_frame
            if frame.text.strip():
                stats["text_shapes"] += 1
            for paragraph in frame.paragraphs:
                for run in paragraph.runs:
                    points = run_points(run, paragraph, shape, slide)
                    if points is None:
                        stats["runs_with_unknown_size"] += 1
                    elif points < MIN_BODY_PT and run.text.strip():
                        finding("font_size", "fail",
                                f"body text below {MIN_BODY_PT:.0f}pt; a footnote goes at 10pt, not smaller"
                                ).add(where, f"{size_note(points, paragraph)}: {run.text[:30]}")
                    face = run_face(run, paragraph, shape, slide, theme_fonts)
                    if face and run.text.strip():
                        fonts_used.setdefault(face.casefold(), face)

        for shape, box in walk(slide.shapes):
            if not shape.has_text_frame or not shape.text_frame.text.strip():
                continue
            if box is None:
                continue
            frame = shape.text_frame
            if frame.auto_size == MSO_AUTO_SIZE.SHAPE_TO_FIT_TEXT:
                continue
            needed_pt, lines = estimate_lines(shape, slide, box[2])
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
        if notes_text(slide):
            stats["notes_slides"] += 1
        elif require_notes and index > 1:
            finding("notes", "fail", "content slide with no speaker notes").add(where)

    # The cover legitimately places its title elsewhere, so slide 1 is what the drift check
    # exempts. Dropping the first entry instead spends the exemption on whichever slide happens
    # to carry the first recognised title, which on an image-only cover is content slide 2.
    content_titles = [entry for entry in titles if entry[0] != 1]
    if len(content_titles) >= 3:
        ref_x, ref_y = title_position(content_titles)
        for index, x, y in content_titles:
            if abs(x - ref_x) > TITLE_DRIFT_IN or abs(y - ref_y) > TITLE_DRIFT_IN:
                finding("title_drift", "warn",
                        f"title away from the deck's own title position ({ref_x:.2f}, {ref_y:.2f} in); "
                        "a title that moves between slides reads as a jitter"
                        ).add(f"slide {index}", f"{x:.2f}, {y:.2f}")

    if len(fonts_used) > MAX_FONT_FAMILIES:
        finding("fonts", "fail",
                f"more than {MAX_FONT_FAMILIES} font families; a deck reads as one document with one or two"
                ).add(", ".join(sorted(fonts_used.values())))
    unsafe = sorted(name for key, name in fonts_used.items() if key not in METRIC_SAFE)
    if unsafe:
        finding("fonts_metric", "warn",
                "font with no metric-compatible clone in the renderer; the layout you check here is not the "
                "layout the reader gets").add(", ".join(unsafe))
    if stats["runs_with_unknown_size"]:
        finding("font_size_inherited", "info",
                "text runs whose size is stated nowhere, the layout and master chain included, so the size "
                "rule could not be checked on them").add(f"{stats['runs_with_unknown_size']} run(s)")

    stats["fonts_used"] = sorted(fonts_used.values())
    stats["fonts_theme"] = next(iter(theme_by_master.values()), package_theme)
    if len(theme_by_master) > 1:
        stats["fonts_theme_by_master"] = theme_by_master
    results = [f.as_dict() for f in registry.values()]
    results.sort(key=lambda d: ({"fail": 0, "warn": 1, "info": 2}[d["level"]], d["check"]))
    fails = sum(1 for d in results if d["level"] == "fail")
    return {"status": "fail" if fails else "pass", "file": str(path), "stats": stats, "findings": results}


def parse_theme_fonts(xml: bytes) -> dict:
    """The theme's major and minor latin faces, found by namespace, not by prefix.

    Which prefix a part binds DrawingML to is the writer's to choose: a theme
    that says `dml:latin` is the same document as one that says `a:latin`, and
    matching the spelling reads no faces off it at all. Text that states no
    family of its own then contributes none, so an unsafe theme font passes.
    """
    fonts = {}
    try:
        root = etree.fromstring(xml)
    except etree.XMLSyntaxError:
        return fonts
    for key, tag in (("major", "majorFont"), ("minor", "minorFont")):
        latin = root.find(f".//{A_NS}fontScheme/{A_NS}{tag}/{A_NS}latin")
        if latin is not None and latin.get("typeface"):
            fonts[key] = latin.get("typeface")
    return fonts


def read_theme_fonts(path: Path) -> dict:
    """The package's first theme, for a deck whose masters cannot be reached."""
    try:
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if re.fullmatch(r"ppt/theme/theme\d+\.xml", n)]
            if not names:
                return {}
            xml = zf.read(sorted(names)[0])
    except Exception:
        return {}
    return parse_theme_fonts(xml)


def master_theme_fonts(master, fallback: dict) -> dict:
    """The theme this master carries, which a second master need not share.

    A deck assembled from two templates has a theme each, so one font map read
    for the package dresses every slide in the first theme's faces and the
    second master's family never reaches the metric-safety check.
    """
    try:
        xml = master.part.part_related_by(RT.THEME).blob
    except (AttributeError, KeyError):
        return fallback
    return parse_theme_fonts(xml)


def parse_args(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token TAKES_VALUE does not name.

    A misspelt --strict used to drop out of the list unread, so the deck was
    audited without the gate the flag was there to hold and a failing deck
    exited 0.
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
    report = check(path, only_slide, "--require-notes" in flags)
    print(json.dumps(report, indent=2))
    if "--strict" in flags and report["status"] == "fail":
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
