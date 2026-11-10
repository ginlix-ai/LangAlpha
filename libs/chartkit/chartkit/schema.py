"""ChartLayer v1: the overlay sandbox code publishes onto a chart.

The one definition of the shape, on both sides of the sandbox boundary: the
server validates every published line with it before the layer is stored or
streamed, and it ships into the sandbox as ``chartkit.schema``, where
``Chart`` takes its caps from it and checks a layer with ``parse_layer``
before writing it, so the agent sees a rejection in its own run. The frontend
mirrors it in ``web/src/pages/MarketView/types/chartLayer.ts``.

The caps bound what one layer can cost the database and the browser, and a
rejection's text goes back to the agent, so each message names the element,
the field and what was expected. Imports stay to pydantic and the standard
library, and the syntax to Python 3.12, the sandbox's: the server imports this
module without loading chartkit's pandas code.
"""

from __future__ import annotations

import json
import re
import types
import typing
from datetime import date, datetime
from typing import Annotated, Any, Literal, Union, get_args

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

LAYER_VERSION = 1

# Intervals a chart can exist on: the chart instance is SYMBOL:timeframe.
Timeframe = Literal["1min", "5min", "15min", "30min", "1hour", "4hour", "1day"]
TIMEFRAMES: tuple[str, ...] = get_args(Timeframe)

MAX_LAYER_BYTES = 1_500_000
MAX_PANES = 4
MAX_SERIES = 24
MAX_SERIES_POINTS = 20_000
MAX_FILLS = 24
MAX_LEVELS = 200
MAX_MARKERS = 3_000
MAX_REGIMES = 2_000
MAX_DRAWINGS = 300
MAX_ALERTS = 50
MAX_PATH_POINTS = 500
MAX_PROFILE_ROWS = 200
MAX_ANALOG_BARS = 500
MAX_CONE_BANDS = 4
MAX_FIB_LEVELS = 24
MAX_TARGETS = 6
# A lower pane's share of the chart height, and what all lower panes may take together.
PANE_MIN_HEIGHT = 0.05
PANE_MAX_HEIGHT = 0.5
PANES_MAX_TOTAL = 0.75

# String lengths, in characters.
MAX_SYMBOL = 32
MAX_TITLE = 120
MAX_LABEL = 120
MAX_SUMMARY = 1200
MAX_NOTE = 600
MAX_TEXT = 200
MAX_POINT_LABEL = 32
_COLOR = 64

LAYER_ID_PATTERN = r"^[a-z0-9-]{1,48}$"
_LAYER_ID_RE = re.compile(LAYER_ID_PATTERN)
ELEMENT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,47}$"

_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?)?$")
# Hex, or rgb()/rgba() with plain numbers: the colour lands in canvas and CSS
# on the user's page, so nothing else (url(), var(), expressions) gets through.
_COLOR_RE = re.compile(
    r"^(#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})"
    r"|rgba?\(\s*\d{1,3}%?\s*,\s*\d{1,3}%?\s*,\s*\d{1,3}%?\s*(?:,\s*(?:0|1|0?\.\d+|1\.0+|\d{1,3}%)\s*)?\))$"
)


def is_color(value: str) -> bool:
    """A colour the layer accepts as written (surrounding spaces aside)."""
    return bool(_COLOR_RE.match(value.strip()))


def parse_time(value: str) -> datetime:
    """A layer time as a naive datetime (a date reads as its midnight), for
    ordering: ``2026-10-28T09:30`` and ``2026-10-28T09:30:00`` are one moment."""
    if "T" in value:
        return datetime.fromisoformat(value)
    return datetime.combine(date.fromisoformat(value), datetime.min.time())


def _check_time(value: str) -> str:
    if not _TIME_RE.match(value):
        raise ValueError(
            f"time {value!r} must be 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM[:SS]' "
            "(exchange-local wall clock, no zone or offset)"
        )
    try:
        parse_time(value)
    except ValueError:
        raise ValueError(f"time {value!r} is not a real calendar date/time") from None
    return value


def _check_color(value: str) -> str:
    if not is_color(value):
        raise ValueError(
            f"color {value!r} must be a hex color ('#26a69a', '#26a69a80') "
            "or 'rgb(r, g, b)' / 'rgba(r, g, b, a)'"
        )
    return value.strip()


LayerTime = Annotated[str, Field(max_length=19), AfterValidator(_check_time)]
Color = Annotated[str, Field(max_length=_COLOR), AfterValidator(_check_color)]
ElementId = Annotated[str, Field(pattern=ELEMENT_ID_PATTERN)]
Note = Annotated[str, Field(max_length=MAX_NOTE)]
Title = Annotated[str, Field(min_length=1, max_length=MAX_TITLE)]
Summary = Annotated[str, Field(max_length=MAX_SUMMARY)]
Symbol = Annotated[str, Field(min_length=1, max_length=MAX_SYMBOL, pattern=r"^[^\s\x00-\x1f\x7f]+$")]
Label = Annotated[str, Field(max_length=MAX_LABEL)]
LineStyle = Literal["solid", "dashed", "dotted"]
Extend = Literal["none", "right", "left", "both"]
SeriesType = Literal["line", "area", "histogram"]
MarkerPosition = Literal["aboveBar", "belowBar", "inBar", "atPrice"]
MarkerShape = Literal["arrowUp", "arrowDown", "circle", "square"]
LabelPosition = Literal["above", "below", "left", "right"]
Side = Literal["long", "short"]
Direction = Literal["above", "below", "either"]
Width = Annotated[int, Field(ge=1, le=4)]


class _Strict(BaseModel):
    """Unknown keys are refused (a typo like ``colour`` would otherwise vanish
    silently) and so are NaN and infinities, which JSON parsing lets through
    and the chart cannot draw."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, populate_by_name=True)


class LayerPoint(_Strict):
    time: LayerTime
    price: float


class _Explained(_Strict):
    note: Note | None = None


class LayerPane(_Strict):
    id: ElementId
    title: Label | None = None
    height: Annotated[float, Field(ge=PANE_MIN_HEIGHT, le=PANE_MAX_HEIGHT)] | None = None

    @model_validator(mode="after")
    def _not_price(self) -> LayerPane:
        if self.id == "price":
            raise ValueError("'price' is the implicit main pane; declare only extra panes")
        return self


class LayerSeriesPoint(_Strict):
    time: LayerTime
    value: float
    color: Color | None = None


class LayerSeries(_Explained):
    id: ElementId
    title: Title
    pane: str = "price"
    type: SeriesType = "line"
    color: Color | None = None
    width: Width | None = None
    style: LineStyle | None = None
    data: Annotated[list[LayerSeriesPoint], Field(max_length=MAX_SERIES_POINTS)]

    @model_validator(mode="after")
    def _increasing(self) -> LayerSeries:
        prev: datetime | None = None
        for i, point in enumerate(self.data):
            at = parse_time(point.time)
            if prev is not None and at <= prev:
                raise ValueError(
                    f"data[{i}].time {point.time!r} is not after data[{i - 1}].time "
                    f"{self.data[i - 1].time!r}: point times must be strictly increasing "
                    "(sort the points and drop duplicate times)"
                )
            prev = at
        return self


class LayerFill(_Explained):
    a: str
    b: str
    color: Color


class LayerLevel(_Explained):
    price: float
    pane: str = "price"
    title: Label | None = None
    color: Color | None = None
    style: LineStyle | None = None


class LayerMarker(_Explained):
    time: LayerTime
    position: MarkerPosition = "aboveBar"
    price: float | None = None
    shape: MarkerShape = "circle"
    color: Color | None = None
    text: Label | None = None

    @model_validator(mode="after")
    def _price_when_at_price(self) -> LayerMarker:
        if self.position == "atPrice" and self.price is None:
            raise ValueError("position 'atPrice' needs a price")
        return self


class LayerRegime(_Explained):
    from_: LayerTime = Field(alias="from")
    to: LayerTime
    color: Color
    label: Label | None = None

    @model_validator(mode="after")
    def _ordered(self) -> LayerRegime:
        if parse_time(self.to) < parse_time(self.from_):
            raise ValueError(f"it ends at {self.to!r}, before it starts at {self.from_!r}")
        return self


# -- Drawings -------------------------------------------------------------------


class _Drawing(_Explained):
    id: ElementId | None = None
    label: Label | None = None
    color: Color | None = None


class SegmentDrawing(_Drawing):
    type: Literal["segment"]
    p1: LayerPoint
    p2: LayerPoint
    extend: Extend | None = None
    style: LineStyle | None = None
    width: Width | None = None


class VLineDrawing(_Drawing):
    type: Literal["vline"]
    time: LayerTime
    style: LineStyle | None = None


class ChannelDrawing(_Drawing):
    type: Literal["channel"]
    p1: LayerPoint
    p2: LayerPoint
    p3: LayerPoint
    extend: Extend | None = None
    fill: Color | None = None


class PitchforkDrawing(_Drawing):
    type: Literal["pitchfork"]
    p1: LayerPoint
    p2: LayerPoint
    p3: LayerPoint
    extend: Extend | None = None


class BoxDrawing(_Drawing):
    type: Literal["box"]
    p1: LayerPoint
    p2: LayerPoint
    fill: Color | None = None


class LabelDrawing(_Drawing):
    type: Literal["label"]
    at: LayerPoint
    text: Annotated[str, Field(min_length=1, max_length=MAX_TEXT)]
    position: LabelPosition | None = None


class ArrowDrawing(_Drawing):
    type: Literal["arrow"]
    p1: LayerPoint
    p2: LayerPoint
    width: Width | None = None


class PathDrawing(_Drawing):
    type: Literal["path"]
    points: Annotated[list[LayerPoint], Field(min_length=2, max_length=MAX_PATH_POINTS)]
    point_labels: list[Annotated[str, Field(max_length=MAX_POINT_LABEL)] | None] | None = None
    style: LineStyle | None = None
    width: Width | None = None
    closed: bool | None = None

    @model_validator(mode="after")
    def _labels_match(self) -> PathDrawing:
        if self.point_labels is not None and len(self.point_labels) != len(self.points):
            raise ValueError(
                f"point_labels has {len(self.point_labels)} entries but points has "
                f"{len(self.points)}: give one per point (None to skip a point)"
            )
        return self


class FibDrawing(_Drawing):
    type: Literal["fib"]
    p1: LayerPoint
    p2: LayerPoint
    levels: Annotated[list[float], Field(min_length=1, max_length=MAX_FIB_LEVELS)] | None = None
    extend: Extend | None = None


_BandPoints = Annotated[list[LayerPoint], Field(min_length=1, max_length=MAX_PATH_POINTS)]


class ConeBand(_Strict):
    label: Label
    upper: _BandPoints
    lower: _BandPoints
    fill: Color | None = None


class ConeDrawing(_Drawing):
    type: Literal["cone"]
    origin: LayerPoint
    bands: Annotated[list[ConeBand], Field(min_length=1, max_length=MAX_CONE_BANDS)]


class ForecastDrawing(_Drawing):
    """A claim that price reaches ``target.price`` by ``target.time``; the chart judges it from later bars."""

    type: Literal["forecast"]
    origin: LayerPoint
    target: LayerPoint
    probability: Annotated[float, Field(ge=0, le=1)] | None = None

    @model_validator(mode="after")
    def _ahead(self) -> ForecastDrawing:
        if parse_time(self.target.time) <= parse_time(self.origin.time):
            raise ValueError(
                f"target.time {self.target.time!r} must be after origin.time {self.origin.time!r}"
            )
        return self


class AnalogBar(_Strict):
    time: LayerTime
    open: float
    high: float
    low: float
    close: float

    @model_validator(mode="after")
    def _ohlc(self) -> AnalogBar:
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError(f"bar at {self.time!r} needs low <= open, close <= high")
        return self


class AnalogDrawing(_Drawing):
    """A past stretch of bars (``source_start`` to ``source_end``) replayed from ``origin``."""

    type: Literal["analog"]
    origin: LayerPoint
    source_start: LayerTime
    source_end: LayerTime
    bars: Annotated[list[AnalogBar], Field(min_length=1, max_length=MAX_ANALOG_BARS)]

    @model_validator(mode="after")
    def _ordered(self) -> AnalogDrawing:
        if parse_time(self.source_end) < parse_time(self.source_start):
            raise ValueError(f"the replayed stretch ends at {self.source_end!r}, before it starts at {self.source_start!r}")
        times = [parse_time(b.time) for b in self.bars]
        if times[0] <= parse_time(self.origin.time) or any(b <= a for a, b in zip(times, times[1:])):
            raise ValueError("bars must follow origin.time, in time order")
        return self


class PositionDrawing(_Drawing):
    type: Literal["position"]
    side: Side
    entry: LayerPoint
    stop: float
    targets: Annotated[list[float], Field(min_length=1, max_length=MAX_TARGETS)]
    until: LayerTime | None = None

    @model_validator(mode="after")
    def _sides(self) -> PositionDrawing:
        entry = self.entry.price
        if self.side == "long":
            if not self.stop < entry:
                raise ValueError(f"a long's stop {self.stop:g} must be below its entry {entry:g}")
            bad = [t for t in self.targets if not t > entry]
            if bad:
                raise ValueError(f"a long's targets must be above its entry {entry:g}; got {bad}")
        else:
            if not self.stop > entry:
                raise ValueError(f"a short's stop {self.stop:g} must be above its entry {entry:g}")
            bad = [t for t in self.targets if not t < entry]
            if bad:
                raise ValueError(f"a short's targets must be below its entry {entry:g}; got {bad}")
        if self.until is not None and parse_time(self.until) <= parse_time(self.entry.time):
            raise ValueError(f"until {self.until!r} must be after entry.time {self.entry.time!r}")
        return self


class MeasureDrawing(_Drawing):
    type: Literal["measure"]
    p1: LayerPoint
    p2: LayerPoint


class ProfileRow(_Strict):
    low: float
    high: float
    volume: Annotated[float, Field(ge=0)]
    up_volume: Annotated[float, Field(ge=0)] | None = None

    @model_validator(mode="after")
    def _row(self) -> ProfileRow:
        if not self.low < self.high:
            raise ValueError(f"low {self.low:g} must be below high {self.high:g}")
        if self.up_volume is not None and self.up_volume > self.volume:
            raise ValueError(f"up_volume {self.up_volume:g} exceeds volume {self.volume:g}")
        return self


class ProfileDrawing(_Drawing):
    type: Literal["profile"]
    from_: LayerTime = Field(alias="from")
    to: LayerTime
    rows: Annotated[list[ProfileRow], Field(min_length=1, max_length=MAX_PROFILE_ROWS)]
    poc: float | None = None
    value_area: tuple[float, float] | None = None

    @model_validator(mode="after")
    def _ranges(self) -> ProfileDrawing:
        if parse_time(self.to) < parse_time(self.from_):
            raise ValueError(f"it ends at {self.to!r}, before it starts at {self.from_!r}")
        if self.value_area is not None and self.value_area[0] > self.value_area[1]:
            raise ValueError(
                f"value_area [{self.value_area[0]:g}, {self.value_area[1]:g}] must be [low, high]"
            )
        return self


_DRAWINGS = (
    SegmentDrawing,
    VLineDrawing,
    ChannelDrawing,
    PitchforkDrawing,
    BoxDrawing,
    LabelDrawing,
    ArrowDrawing,
    PathDrawing,
    FibDrawing,
    ConeDrawing,
    ForecastDrawing,
    AnalogDrawing,
    PositionDrawing,
    MeasureDrawing,
    ProfileDrawing,
)
LayerDrawing = Annotated[Union[_DRAWINGS], Field(discriminator="type")]  # noqa: UP007
DRAWING_TYPES: tuple[str, ...] = tuple(get_args(d.model_fields["type"].annotation)[0] for d in _DRAWINGS)
DRAWING_MODELS: dict[str, type[_Drawing]] = dict(zip(DRAWING_TYPES, _DRAWINGS))
_DRAWING_TYPES = ", ".join(DRAWING_TYPES)


class AlertCondition(_Strict):
    kind: Literal["price_cross"]
    level: float
    direction: Direction = "either"


class LayerAlert(_Explained):
    id: ElementId
    title: Title
    condition: AlertCondition


class LayerSource(_Strict):
    kind: Literal["python"] = "python"


class ChartLayer(_Strict):
    version: Literal[1]
    id: Annotated[str, Field(pattern=LAYER_ID_PATTERN)]
    title: Title
    summary: Summary | None = None
    symbol: Symbol
    timeframe: Timeframe
    as_of: LayerTime | None = None
    source: LayerSource = Field(default_factory=LayerSource)
    panes: Annotated[list[LayerPane], Field(max_length=MAX_PANES)] = []
    series: Annotated[list[LayerSeries], Field(max_length=MAX_SERIES)] = []
    fills: Annotated[list[LayerFill], Field(max_length=MAX_FILLS)] = []
    levels: Annotated[list[LayerLevel], Field(max_length=MAX_LEVELS)] = []
    markers: Annotated[list[LayerMarker], Field(max_length=MAX_MARKERS)] = []
    regimes: Annotated[list[LayerRegime], Field(max_length=MAX_REGIMES)] = []
    drawings: Annotated[list[LayerDrawing], Field(max_length=MAX_DRAWINGS)] = []
    alerts: Annotated[list[LayerAlert], Field(max_length=MAX_ALERTS)] = []

    @model_validator(mode="after")
    def _references(self) -> ChartLayer:
        """Checks across elements, all reported at once: a script fixing one
        should not have to republish to learn of the next."""
        problems: list[str] = []
        problems += _duplicates("panes", [p.id for p in self.panes])
        problems += _duplicates("series", [s.id for s in self.series])
        problems += _duplicates("drawings", [d.id for d in self.drawings if d.id])
        problems += _duplicates("alerts", [a.id for a in self.alerts])
        panes = {"price", *(p.id for p in self.panes)}
        declared = ", ".join(sorted(panes))
        for kind, items in (("series", self.series), ("levels", self.levels)):
            for i, item in enumerate(items):
                if item.pane not in panes:
                    problems.append(
                        f"{kind}[{i}].pane: {item.pane!r} is not a declared pane "
                        f"(declared: {declared}); add it to panes or use 'price'"
                    )
        # A lower pane opens around its series' points and takes its scale from them;
        # a level alone there would publish and never be drawn.
        plotted = {s.pane for s in self.series if s.data}
        for i, lv in enumerate(self.levels):
            if lv.pane != "price" and lv.pane in panes and lv.pane not in plotted:
                problems.append(
                    f"levels[{i}].pane: pane {lv.pane!r} plots no series, so a level there "
                    f"has no scale to sit on; plot a series in it or put the level on 'price'"
                )
        total = sum(p.height or 0 for p in self.panes)
        if total > PANES_MAX_TOTAL:
            problems.append(
                f"panes: lower panes take {total:.2f} of the chart height; "
                f"keep their total at or under {PANES_MAX_TOTAL:g}"
            )
        series_ids = {s.id for s in self.series}
        series_pane = {s.id: s.pane for s in self.series}
        for i, fill in enumerate(self.fills):
            for side in ("a", "b"):
                ref = getattr(fill, side)
                if ref not in series_ids:
                    problems.append(
                        f"fills[{i}].{side}: {ref!r} is not a series id in this layer "
                        f"(series ids: {', '.join(sorted(series_ids)) or 'none'})"
                    )
            if fill.a == fill.b:
                problems.append(f"fills[{i}]: a and b are both {fill.a!r}; fill between two series")
            elif (
                fill.a in series_pane
                and fill.b in series_pane
                and series_pane[fill.a] != series_pane[fill.b]
            ):
                problems.append(
                    f"fills[{i}]: series {fill.a!r} is in pane {series_pane[fill.a]!r} but "
                    f"{fill.b!r} is in {series_pane[fill.b]!r}; a fill needs both in the same pane"
                )
        if problems:
            raise ValueError("\n".join(problems))
        return self


_COUNTED = ("series", "levels", "markers", "regimes", "drawings", "alerts")


def counts(layer: ChartLayer | dict[str, Any]) -> dict[str, int]:
    """How many elements of each kind a layer holds, validated or still a dict."""
    get = layer.get if isinstance(layer, dict) else lambda key: getattr(layer, key)
    return {kind: len(get(kind) or ()) for kind in _COUNTED}


def _duplicates(kind: str, ids: list[str]) -> list[str]:
    seen: set[str] = set()
    dup: list[str] = []
    for i in ids:
        if i in seen and i not in dup:
            dup.append(i)
        seen.add(i)
    return [f"{kind}: id {d!r} is used more than once; ids must be unique within {kind}" for d in dup]


# -- Parsing and errors -------------------------------------------------------

_MAX_REPORTED = 8


class LayerRejected(ValueError):
    """A published line that is not a valid layer. ``problems`` are the lines
    the agent is shown; ``layer_id`` is the line's own id when it had one."""

    def __init__(self, problems: list[str], layer_id: str | None = None) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems
        self.layer_id = layer_id


def parse_layer(line: str) -> ChartLayer:
    """One published JSON line as a validated layer; raises ``LayerRejected``."""
    size = len(line.encode("utf-8"))
    if size > MAX_LAYER_BYTES:
        raise LayerRejected(
            [
                f"the layer is {size / 1e6:.2f} MB serialized, over the "
                f"{MAX_LAYER_BYTES / 1e6:.1f} MB cap; thin the series (fewer points, "
                "a shorter window) or split it into several layers"
            ],
            _sniff_id(line),
        )
    try:
        raw = json.loads(line)
    except (ValueError, RecursionError) as e:
        # RecursionError: nesting deeper than the parser's stack, which no
        # layer needs.
        raise LayerRejected([f"the line is not valid JSON ({e})"], _sniff_id(line)) from None
    if not isinstance(raw, dict):
        raise LayerRejected([f"a layer must be a JSON object, got {type(raw).__name__}"])
    # Named in the rejection only as an id could be: agent code wrote it.
    layer_id = raw.get("id") if isinstance(raw.get("id"), str) else None
    if layer_id is not None and not _LAYER_ID_RE.fullmatch(layer_id):
        layer_id = None
    try:
        return ChartLayer.model_validate(raw)
    except ValidationError as e:
        raise LayerRejected(describe_errors(e, raw), layer_id) from None


def _sniff_id(line: str) -> str | None:
    """The line's id from its first 4 KB when it is shaped like a layer id, for a line that did not parse."""
    match = re.search(r'"id"\s*:\s*"([^"\\]{1,48})"', line[:4096])
    return match.group(1) if match and _LAYER_ID_RE.fullmatch(match.group(1)) else None


def _where(loc: tuple[Any, ...], raw: Any) -> str:
    """``drawings[3] (position 'q4-long').stop`` from a pydantic location: the
    drawing union's type tag becomes the parenthesised kind, with the
    element's own id or label when it has one, so the agent finds it in its
    own code."""
    tagged = len(loc) >= 3 and loc[0] == "drawings" and isinstance(loc[1], int)
    out = ""
    for i, part in enumerate(loc):
        if isinstance(part, int):
            out += f"[{part}]"
        elif tagged and i == 2:
            drawings = raw.get("drawings") if isinstance(raw, dict) else None
            node = drawings[loc[1]] if isinstance(drawings, list) and loc[1] < len(drawings) else {}
            name = (node.get("id") or node.get("label")) if isinstance(node, dict) else None
            if isinstance(name, str) and len(name) > 40:
                name = name[:39] + "…"
            out += f" ({part} {name!r})" if name else f" ({part})"
        else:
            out += f".{part}" if out else str(part)
    return out


_BOUND_KEYS = ("ge", "gt", "le", "lt")


def _bounds_of(tp, metadata=()) -> dict:
    """The numeric bounds a field's type declares (``Field(ge=..., le=...)``), through
    ``Annotated`` and ``X | None``."""
    out: dict = {}
    for m in metadata:
        for k in _BOUND_KEYS:
            if getattr(m, k, None) is not None:
                out[k] = getattr(m, k)
        out |= _bounds_of(None, getattr(m, "metadata", ()))
    origin = typing.get_origin(tp)
    if origin is typing.Annotated:
        base, *meta = typing.get_args(tp)
        out |= _bounds_of(base, meta)
    elif origin in (typing.Union, types.UnionType):
        for a in typing.get_args(tp):
            out |= _bounds_of(a)
    return out


def _tag_of(tp) -> str | None:
    """A tagged union member's ``type`` literal (``"forecast"``), else ``None``."""
    field = tp.model_fields.get("type") if isinstance(tp, type) and issubclass(tp, BaseModel) else None
    tags = typing.get_args(field.annotation) if field is not None else ()
    return tags[0] if len(tags) == 1 and isinstance(tags[0], str) else None


def _bounds(model, loc: tuple) -> dict:
    """The bounds of the field ``loc`` names on ``model``, or ``{}`` where the walk loses it."""
    tp, meta = model, ()
    for part in loc:
        while typing.get_origin(tp) in (typing.Union, types.UnionType, typing.Annotated):
            args = [a for a in typing.get_args(tp) if a is not type(None)]
            if typing.get_origin(tp) is not typing.Annotated and len(args) != 1:
                # A tagged union (the drawings): its location names the member by its tag.
                tp = next((a for a in args if _tag_of(a) == part), None)
                break
            tp = args[0]
        if tp is None:
            return {}
        if isinstance(part, str) and _tag_of(tp) == part:
            continue
        if isinstance(part, int):
            args = typing.get_args(tp)
            if not args:
                return {}
            tp, meta = args[0], ()
        elif isinstance(tp, type) and issubclass(tp, BaseModel):
            info = next((f for n, f in tp.model_fields.items() if part in (n, f.alias)), None)
            if info is None:
                return {}
            tp, meta = info.annotation, info.metadata
        else:
            return {}
    return _bounds_of(tp, meta)


def _range(b: dict) -> str | None:
    lo = b.get("ge", b.get("gt"))
    hi = b.get("le", b.get("lt"))
    if lo is not None and hi is not None:
        return f"from {lo:g} to {hi:g}"
    if lo is not None:
        return f"{'at least' if 'ge' in b else 'above'} {lo:g}"
    if hi is not None:
        return f"{'at most' if 'le' in b else 'below'} {hi:g}"
    return None


#: What a bounded field measures, where its range alone would not say.
_BOUNDED = {"width": "a line width in whole pixels", "height": "a fraction of the chart height"}


def plain_problem(err: dict, model: Any = None, *, got: bool = True) -> str:
    """One pydantic error in the words an agent acts on: a bound as the field's whole range
    ("must be from 1 to 4, got 9"), "Input should be" as "must be", a missing field as required.
    ``model`` is where ``err``'s location starts, for the range."""
    kind, given = err.get("type"), err.get("input")
    shown = got and isinstance(given, (int, float, str)) and not isinstance(given, bool)
    text = repr(given) if shown else ""
    tail = f", got {text if len(text) <= 60 else text[:57] + '...'}" if shown else ""
    if kind == "missing":
        return "is required"
    if kind in ("greater_than", "greater_than_equal", "less_than", "less_than_equal", "int_from_float"):
        span = _range(_bounds(model, tuple(err.get("loc", ()))) if model is not None else {}) or _range(
            {k: v for k, v in (err.get("ctx") or {}).items() if k in _BOUND_KEYS})
        names = [p for p in err.get("loc", ()) if isinstance(p, str)]
        what = _BOUNDED.get(names[-1]) if names else None
        if kind == "int_from_float":
            what = what or "a whole number"
        if what or span:
            return f"must be {' '.join(w for w in (what, span) if w)}{tail}"
    msg = str(err.get("msg", "invalid")).removeprefix("Value error, ").removeprefix("Assertion failed, ")
    if msg.startswith("Input should be "):
        msg = "must be " + msg.removeprefix("Input should be ")
    return msg + tail


#: Problems whose message is about the value given, so the rejection shows it.
_GOT_KINDS = {"greater_than", "greater_than_equal", "less_than", "less_than_equal", "int_from_float",
              "literal_error", "enum"}


def describe_errors(error: ValidationError, raw: Any) -> list[str]:
    """One line per problem, at most a handful, each naming where it is."""
    lines: list[str] = []
    for err in error.errors(include_url=False):
        kind = err.get("type")
        msg = plain_problem(err, ChartLayer, got=kind in _GOT_KINDS)
        if kind == "extra_forbidden":
            msg = "is not a field of this element; check its spelling"
        elif kind == "union_tag_invalid":
            tag = (err.get("ctx") or {}).get("tag")
            msg = f"unknown drawing type {tag!r}; expected one of {_DRAWING_TYPES}"
        elif kind == "union_tag_not_found":
            msg = f"missing 'type'; expected one of {_DRAWING_TYPES}"
        where = _where(tuple(err.get("loc", ())), raw)
        lines += [f"{where}: {part}" if where else part for part in msg.split("\n")]
    if len(lines) > _MAX_REPORTED:
        more = len(lines) - _MAX_REPORTED
        lines = [*lines[:_MAX_REPORTED], f"... and {more} more problem(s)"]
    return lines
