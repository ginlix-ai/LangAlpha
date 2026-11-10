"""Publishing the chart layers a run wrote: validate, store, stream, report.

Sandbox code publishes a layer by appending it as a JSON line to the job's
outbox (``chartkit``'s ``Chart.publish``); the job's end brings the lines back
(``Outbox``), and the lines are published where the job ends: the ExecuteCode
or Bash call (``publish_outbox``), or the watcher's settle for a handed-off
job, which stores them with the command's end (``stage_outbox``, then
``command_runs.finalize``). The valid layers are stored in one transaction,
one row each keyed by the layer's id (so republishing an id on the same chart
replaces it), and only then streamed as ``chart_layer`` events, so no event
ever shows a layer the database lost.

The text returned is appended to the tool result. It is how the agent checks
its own work: what was published where, and each element with the
coordinates it actually carries, so a level or a stop the agent misremembers
is visible against what it drew. A rejected layer says NOT published and why.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, assert_never

from chartkit.schema import (
    AnalogDrawing,
    ArrowDrawing,
    BoxDrawing,
    ChannelDrawing,
    ChartLayer,
    ConeDrawing,
    FibDrawing,
    ForecastDrawing,
    LabelDrawing,
    LayerDrawing,
    LayerRejected,
    MeasureDrawing,
    PathDrawing,
    PitchforkDrawing,
    PositionDrawing,
    ProfileDrawing,
    SegmentDrawing,
    VLineDrawing,
    parse_layer,
)
from ptc_agent.core.sandbox.jobs.protocol import OUTBOX_READ_MAX, Outbox
from src.server.database.chart_annotation import add_layers
from src.server.models.chart_layer import ChartRef, CommandOutbox, LayerRef, PublishedLayer
from src.tools.chart_annotation.stream import emit, get_writer

logger = logging.getLogger(__name__)

#: Layers one run may publish. Past it the rest are refused: each is a row
#: and a stream event, and no analysis needs more on one call.
MAX_LAYERS_PER_RUN = 20
# Elements listed per layer in the result before the rest are counted: the
# result is read into the agent's context.
_MAX_LISTED_DRAWINGS = 40
#: The most read-back text one run gets (about 6k tokens).
MAX_READ_BACK_CHARS = 24_000
_MAX_LISTED_LEVELS = 12
_MAX_LISTED_PATH_POINTS = 10
#: The largest layer an add event carries whole. Every event goes into the
#: run's Redis buffer, which a few megabyte layers would exhaust; a larger
#: one's add names it without its body, and the chart refetches it.
MAX_STREAMED_LAYER_BYTES = 32 * 1024

_UNPROCESSED = (
    "Chart layers NOT published: they could not be processed. "
    "Publish them again in a moment."
)


@dataclass(frozen=True)
class Published:
    """What a run's outbox came to: the read-back for the agent (empty when
    the run published nothing), and one entry per stored layer for the chat's
    chart-layer card."""

    text: str = ""
    layers: list[PublishedLayer] = field(default_factory=list)


@dataclass(frozen=True)
class Staged:
    """A run's outbox validated and ready to store, not yet stored: what the
    read-back says before the layers (``notes``), the layers, and their rows.
    Storing is the caller's, once, so a settle that loses its finalize stores
    nothing."""

    notes: tuple[str, ...] = ()
    layers: tuple[tuple[LayerRef, ChartLayer], ...] = ()
    bodies: tuple[tuple[LayerRef, dict[str, Any]], ...] = ()
    #: Per body, whether its add event carries it (``MAX_STREAMED_LAYER_BYTES``).
    streamed: tuple[bool, ...] = ()

    def stored(self) -> Published:
        """The read-back once the layers are stored (or there were none)."""
        described = [describe_layer(layer, ref.chart_id) for ref, layer in self.layers]
        return Published(
            _read_back([*self.notes, *described]),
            [ref.artifact(layer.title, layer.summary, layer.as_of) for ref, layer in self.layers],
        )

    def unsaved(self) -> Published:
        """The read-back when storing the layers failed."""
        if len(self.layers) == 1:
            failed = (
                f"Chart layer {self.layers[0][0].layer_id!r} NOT published: it could not be saved "
                "(storage unavailable), so nothing was drawn. Publish it again in a moment."
            )
        else:
            ids = ", ".join(repr(ref.layer_id) for ref, _ in self.layers)
            failed = (
                f"Chart layers {ids} NOT published: they could not be saved (storage "
                "unavailable), so nothing was drawn. Publish them again in a moment."
            )
        return Published(_read_back([*self.notes, failed]))

    def command_outbox(self) -> CommandOutbox | None:
        """What a handed-off command's finalize stores with its end; None
        when the run published nothing."""
        if not self.notes and not self.bodies:
            return None
        stored, unsaved = self.stored(), self.unsaved() if self.bodies else None
        return CommandOutbox(
            rows=self.bodies,
            report=stored.text or None,
            layers=stored.layers or None,
            unsaved_report=unsaved.text if unsaved else None,
        )

    def emit(self) -> None:
        """Stream the stored layers; only ever after their write commits."""
        writer = get_writer()
        for (ref, body), whole in zip(self.bodies, self.streamed, strict=True):
            emit(
                writer,
                artifact_type="chart_layer",
                artifact_id=ref.layer_id,
                payload=ref.event("add", layer=body) if whole else ref.event("add"),
            )


async def publish_outbox(outbox: Outbox, workspace_id: str | None) -> Published:
    """Validate, store and stream the layers a run published. Never raises:
    the job has ended either way, and what went wrong is told in the text."""
    if not outbox:
        return Published()
    try:
        return await _publish(outbox, workspace_id)
    except Exception:
        logger.exception("[chart_layers] publishing a run's outbox failed")
        return Published(_UNPROCESSED)


async def _publish(outbox: Outbox, workspace_id: str | None) -> Published:
    staged = await asyncio.to_thread(_stage, outbox, workspace_id)
    if not staged.bodies:
        return staged.stored()
    try:
        await add_layers(staged.bodies)
    except Exception:
        logger.exception("[chart_layers] storing %d layer(s) failed", len(staged.bodies))
        return staged.unsaved()
    staged.emit()
    return staged.stored()


async def stage_outbox(outbox: Outbox, workspace_id: str | None) -> Staged:
    """Validate the layers a run published, for a caller that stores them
    itself. Never raises, as ``publish_outbox`` does not."""
    if not outbox:
        return Staged()
    try:
        return await asyncio.to_thread(_stage, outbox, workspace_id)
    except Exception:
        logger.exception("[chart_layers] processing a run's outbox failed")
        return Staged(notes=(_UNPROCESSED,))


def _stage(outbox: Outbox, workspace_id: str | None) -> Staged:
    """Parse, validate and dump the run's layers. Run in a worker thread
    (one hop per outbox): up to 4 MB of JSON through pydantic would hold the
    event loop for a good part of a second."""
    lines = outbox.lines
    out: list[str] = []
    if outbox.truncated:
        out.append(
            f"Chart layers past the first {OUTBOX_READ_MAX // (1024 * 1024)} MB this run "
            "published were cut off and NOT published. Publish fewer or smaller layers "
            "(thin long series)."
        )
    if not workspace_id:
        out.append(
            f"{_count(len(lines), 'chart layer')} NOT published: this run has no "
            "workspace to keep them in."
        )
        return Staged(tuple(out))
    if len(lines) > MAX_LAYERS_PER_RUN:
        out.append(
            f"This run published {len(lines)} chart layers; only the first "
            f"{MAX_LAYERS_PER_RUN} were considered and the rest were NOT published."
        )
        lines = lines[:MAX_LAYERS_PER_RUN]

    parsed: list[ChartLayer] = []
    for n, line in enumerate(lines, 1):
        try:
            parsed.append(parse_layer(line))
        except LayerRejected as e:
            name = f"{e.layer_id!r}" if e.layer_id else f"#{n} of this run"
            out.append(
                f"Chart layer {name} NOT published (nothing was drawn):\n"
                + "\n".join(f"  {p}" for p in e.problems)
            )
    rows = [
        (ChartRef.of(workspace_id, layer.symbol, layer.timeframe).layer(layer.id), layer)
        for layer in parsed
    ]
    rows = _last_of_each(rows, out)
    bodies = tuple((ref, _body(layer, ref)) for ref, layer in rows)
    return Staged(
        tuple(out),
        tuple(rows),
        bodies,
        tuple(_json_bytes(body) <= MAX_STREAMED_LAYER_BYTES for _, body in bodies),
    )


def _json_bytes(body: dict[str, Any]) -> int:
    return len(json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode())


def _body(layer: ChartLayer, ref: LayerRef) -> dict[str, Any]:
    """The layer as stored and streamed, its symbol in the chart's spelling."""
    return {**layer.model_dump(mode="json", by_alias=True, exclude_none=True), "symbol": ref.symbol}


def _read_back(parts: list[str]) -> str:
    """The read-back as the agent gets it: bounded, since a handed-off job's
    rides a notice nothing trims, and free of NUL, which Postgres text refuses
    (agent strings are quoted, but a pydantic location can carry a raw key)."""
    text = "\n\n".join(parts).replace("\x00", "")
    if len(text) <= MAX_READ_BACK_CHARS:
        return text
    cut = text.rfind("\n", 0, MAX_READ_BACK_CHARS)
    return (
        text[: cut if cut > 0 else MAX_READ_BACK_CHARS]
        + f"\n… cut at {MAX_READ_BACK_CHARS:,} characters. "
        "manage_chart_layers lists the layers on a chart."
    )


def _last_of_each(
    rows: list[tuple[LayerRef, ChartLayer]], out: list[str]
) -> list[tuple[LayerRef, ChartLayer]]:
    """One layer per chart and id, the last published: the row would hold the
    last anyway, and the agent is told which stood."""
    last: dict[tuple[str, str], tuple[LayerRef, ChartLayer]] = {}
    repeated: set[tuple[str, str]] = set()
    for ref, layer in rows:
        key = (ref.chart_id, ref.layer_id)
        if key in last:
            repeated.add(key)
            del last[key]
        last[key] = (ref, layer)
    for chart_id, layer_id in sorted(repeated):
        out.append(
            f"Chart layer {layer_id!r} was published more than once on {chart_id} in "
            "this run; only the last one stands."
        )
    return list(last.values())


# -- What the agent reads back ------------------------------------------------


def _count(n: int, noun: str, plural: str | None = None) -> str:
    return f"{n} {noun}" if n == 1 else f"{n} {plural or noun + 's'}"


def _num(x: float) -> str:
    return f"{x:.6g}" if abs(x) < 1e6 else f"{x:.0f}"


def _pt(p: Any) -> str:
    return f"{_num(p.price)} @{p.time}"


def _name(d: LayerDrawing) -> str:
    name = d.id or d.label
    return f" {name!r}" if name else ""


def _drawing(d: LayerDrawing) -> str:
    """One drawing with its defining coordinates."""
    head = f"{d.type}{_name(d)}"
    match d:
        case SegmentDrawing():
            ext = f", extend {d.extend}" if d.extend not in (None, "none") else ""
            return f"{head}: {_pt(d.p1)} → {_pt(d.p2)}{ext}"
        case ArrowDrawing():
            return f"{head}: {_pt(d.p1)} → {_pt(d.p2)}"
        case VLineDrawing():
            return f"{head}: {d.time}"
        case ChannelDrawing() | PitchforkDrawing():
            return f"{head}: p1 {_pt(d.p1)}, p2 {_pt(d.p2)}, p3 {_pt(d.p3)}"
        case BoxDrawing():
            lo, hi = sorted((d.p1.price, d.p2.price))
            t1, t2 = sorted((d.p1.time, d.p2.time))
            return f"{head}: {_num(lo)}–{_num(hi)}, {t1} → {t2}"
        case LabelDrawing():
            return f"{head}: {d.text!r} at {_pt(d.at)}"
        case PathDrawing():
            labels = d.point_labels or [None] * len(d.points)
            pts = [
                f"{lab!r} {_pt(p)}" if lab else _pt(p)
                for p, lab in zip(d.points, labels, strict=True)
            ]
            if len(pts) > _MAX_LISTED_PATH_POINTS:
                pts = [*pts[:_MAX_LISTED_PATH_POINTS], f"… {len(pts) - _MAX_LISTED_PATH_POINTS} more"]
            closed = ", closed" if d.closed else ""
            return f"{head} ({len(d.points)} points{closed}): " + " → ".join(pts)
        case FibDrawing():
            levels = ", ".join(_num(v) for v in d.levels) if d.levels else "default levels"
            return f"{head}: {_pt(d.p1)} → {_pt(d.p2)} ({levels})"
        case ConeDrawing():
            bands = "; ".join(
                f"{b.label!r} {_num(b.lower[-1].price)}–{_num(b.upper[-1].price)} by {b.upper[-1].time}"
                for b in d.bands
            )
            return f"{head} from {_pt(d.origin)}: {bands}"
        case ForecastDrawing():
            move = d.target.price / d.origin.price - 1 if d.origin.price else None
            pct = f" ({move:+.1%})" if move is not None else ""
            odds = f", {d.probability:.0%}" if d.probability is not None else ""
            return f"{head}: {_pt(d.origin)} → {_num(d.target.price)} by {d.target.time}{pct}{odds}"
        case AnalogDrawing():
            last = d.bars[-1]
            move = last.close / d.origin.price - 1 if d.origin.price else None
            pct = f" ({move:+.1%})" if move is not None else ""
            return (
                f"{head}: {d.source_start} → {d.source_end} replayed from {_pt(d.origin)}, "
                f"{_count(len(d.bars), 'bar')} to {last.time}, ending {_num(last.close)}{pct}"
            )
        case PositionDrawing():
            entry = d.entry.price
            risk = abs(entry - d.stop)
            rr = ", ".join(f"{abs(t - entry) / risk:.1f}" for t in d.targets) if risk else "n/a"
            targets = ", ".join(_num(t) for t in d.targets)
            until = f" until {d.until}" if d.until else ""
            return (
                f"{head}: {d.side} entry {_pt(d.entry)} stop {_num(d.stop)} "
                f"targets {targets} (R {rr}){until}"
            )
        case MeasureDrawing():
            delta = d.p2.price - d.p1.price
            pct = f", {delta / d.p1.price:+.1%}" if d.p1.price else ""
            return f"{head}: {_pt(d.p1)} → {_pt(d.p2)} ({delta:+.6g}{pct})"
        case ProfileDrawing():
            lo = min(r.low for r in d.rows)
            hi = max(r.high for r in d.rows)
            poc = f", POC {_num(d.poc)}" if d.poc is not None else ""
            va = f", value area {_num(d.value_area[0])}–{_num(d.value_area[1])}" if d.value_area else ""
            return f"{head}: {d.from_} → {d.to}, {len(d.rows)} rows {_num(lo)}–{_num(hi)}{poc}{va}"
        case _:
            assert_never(d)


def describe_layer(layer: ChartLayer, chart_id: str) -> str:
    """What was published, element by element, as the agent reads it back."""
    as_of = f", as of {layer.as_of}" if layer.as_of else ""
    lines = [
        f"Published chart layer {layer.id} {layer.title!r} on {chart_id}{as_of}:"
    ]
    if layer.panes:
        lines.append("  panes: " + ", ".join(p.id for p in layer.panes))
    for s in layer.series:
        span = f"{s.data[0].time} → {s.data[-1].time}, last {_num(s.data[-1].value)}" if s.data else "no points"
        lines.append(
            f"  series {s.id} {s.title!r} ({s.pane}, {s.type}): {_count(len(s.data), 'point')}, {span}"
        )
    for f in layer.fills:
        lines.append(f"  fill {f.a} ↔ {f.b}")
    levels = [
        f"{_num(lv.price)}{' ' + repr(lv.title) if lv.title else ''}"
        + (f" ({lv.pane})" if lv.pane != "price" else "")
        for lv in layer.levels
    ]
    if levels:
        more = len(levels) - _MAX_LISTED_LEVELS
        shown = levels[:_MAX_LISTED_LEVELS] + ([f"… {more} more"] if more > 0 else [])
        lines.append("  levels: " + "; ".join(shown))
    if layer.markers:
        times = sorted(m.time for m in layer.markers)
        lines.append(f"  markers: {len(layer.markers)}, {times[0]} → {times[-1]}")
    if layer.regimes:
        lines.append(
            f"  regimes: {len(layer.regimes)}, "
            f"{min(r.from_ for r in layer.regimes)} → {max(r.to for r in layer.regimes)}"
        )
    for d in layer.drawings[:_MAX_LISTED_DRAWINGS]:
        lines.append(f"  {_drawing(d)}")
    if len(layer.drawings) > _MAX_LISTED_DRAWINGS:
        lines.append(f"  … {len(layer.drawings) - _MAX_LISTED_DRAWINGS} more drawings")
    for a in layer.alerts:
        c = a.condition
        lines.append(f"  alert {a.id}: price crosses {c.direction} {_num(c.level)}")
    if len(lines) == 1:
        lines.append("  (no elements)")
    return "\n".join(lines)


def layer_counts(layer: dict[str, Any]) -> str:
    """A stored layer's elements as counts, for a listing: ``2 series, 14
    drawings``. Reads the stored dict, which was validated when written."""
    parts = [
        _count(len(layer[key]), noun, plural)
        for key, noun, plural in (
            ("series", "series", "series"),
            ("levels", "level", None),
            ("markers", "marker", None),
            ("regimes", "regime", None),
            ("drawings", "drawing", None),
            ("alerts", "alert", None),
        )
        if layer.get(key)
    ]
    return ", ".join(parts) or "no elements"
