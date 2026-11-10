"""Migration 117: each chart's legacy annotations become one chart layer.

Before chart layers, the agent drew with ``draw_chart_annotation``: one
``chart_annotations`` row per drawing (``ann_<hex>``, the drawing's fields
flat in ``payload``). Nothing reads those rows any more, so each chart
(``workspace_id`` + ``SYMBOL:timeframe``, the table's key) gets its drawings
back as one layer titled "Earlier annotations" (row and layer id
``earlier-annotations``, the payload the layer itself, as a published layer is
stored), oldest first, and the annotation rows are deleted. A chart with more drawings
than a layer holds gets more than one layer.

Each annotation becomes the nearest layer element, its words kept as the
element's label or text, and anything longer than the layer allows in its
note: ``price_line`` a level, ``marker`` a marker, ``trendline`` a segment,
``vertical_line`` a vline, ``rectangle`` a box, ``text`` a label, ``event``
a label whose note is the event's detail, ``fib_retracement`` a fib. A time
lands where the old chart drew it: the instant read in UTC, written without a
zone (the chart's wall clock, as the old renderer read it), on a daily chart
the nearest date. A colour the layer cannot take (a CSS name it does not know)
falls back to the chart's own. An annotation that cannot be drawn (no readable
time, no price, nothing to say) is dropped, as the old renderer skipped it.
A payload that is not an annotation at all (not a JSON object) is dropped
the same way. Every layer is validated against ``chartkit.schema.ChartLayer``
before it is written; chartkit is loaded only when there are annotations to
convert. A chart's symbol is first respelled as the server stores it (BRK.B
is BRK-B), so its layer lands on the chart the server reads; a chart no layer
can name even then loses its annotations, logged chart by chart.

The conversion (``convert_legacy``) can run again: it reads only ``ann_``
rows, numbers a chart's layers past the ``earlier-annotations`` ones it
already has, and deletes a chart's annotation rows only once its layers are
written. During a rolling deploy a worker of the previous build can still
write annotation rows after this commits; the server's reads skip them (a
layer id has no ``_``), and a later migration sweeps them by calling
``convert_legacy`` again.

Rollback: ``alembic downgrade 116``; it changes nothing, and it is lossy for
the legacy drawings: their annotation rows are not restored, and the previous
build draws nothing from layer rows, so its charts show none of them. Drawings
it makes meanwhile are converted when this migration runs again.

Revision ID: 117
Revises: 116
"""

from __future__ import annotations

import functools
import itertools
import json
import logging
import math
from collections.abc import Callable, Collection, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import Any

from alembic import op
from sqlalchemy import text

revision = "117"
down_revision = "116"
branch_labels = None
depends_on = None

LAYER_ID = "earlier-annotations"
LAYER_TITLE = "Earlier annotations"
_SUMMARY = "Annotations drawn on this chart before chart layers, kept as they were drawn."
#: Every legacy annotation's row id began with this; a layer's is its own id.
_LEGACY_PREFIX = "ann_"

# The old renderer's event badge colour, which set events apart from the
# technical drawings; every other kind fell back to the chart's colours.
_EVENT_COLOR = "#E9954A"

# CSS colour names the old tool accepted and an agent plausibly wrote; a
# layer takes only hex and rgb(a).
_NAMED_COLORS = {
    "black": "#000000", "white": "#ffffff", "gray": "#808080", "grey": "#808080",
    "silver": "#c0c0c0", "red": "#ff0000", "darkred": "#8b0000", "crimson": "#dc143c",
    "maroon": "#800000", "tomato": "#ff6347", "coral": "#ff7f50", "salmon": "#fa8072",
    "orange": "#ffa500", "darkorange": "#ff8c00", "gold": "#ffd700", "yellow": "#ffff00",
    "olive": "#808000", "lime": "#00ff00", "limegreen": "#32cd32", "green": "#008000",
    "darkgreen": "#006400", "seagreen": "#2e8b57", "teal": "#008080", "cyan": "#00ffff",
    "aqua": "#00ffff", "turquoise": "#40e0d0", "skyblue": "#87ceeb", "steelblue": "#4682b4",
    "dodgerblue": "#1e90ff", "royalblue": "#4169e1", "blue": "#0000ff", "navy": "#000080",
    "indigo": "#4b0082", "purple": "#800080", "violet": "#ee82ee", "magenta": "#ff00ff",
    "fuchsia": "#ff00ff", "orchid": "#da70d6", "pink": "#ffc0cb", "hotpink": "#ff69b4",
    "brown": "#a52a2a", "chocolate": "#d2691e", "tan": "#d2b48c",
}

#: The lists a layer holds the converted annotations in.
_LISTS = ("levels", "markers", "drawings")


@functools.cache
def _schema() -> ModuleType:
    """``chartkit.schema``, loaded on first use: a run with nothing to convert,
    and every other migration, never imports it."""
    import chartkit.schema

    return chartkit.schema


@functools.cache
def _validators() -> dict[str, Any]:
    from pydantic import TypeAdapter

    ck = _schema()
    return {
        "levels": TypeAdapter(ck.LayerLevel),
        "markers": TypeAdapter(ck.LayerMarker),
        "drawings": TypeAdapter(ck.LayerDrawing),
    }


# -- One annotation -------------------------------------------------------------


def layer_time(value: Any, timeframe: str) -> str | None:
    """A legacy ISO 8601 time as a layer time, where the old chart drew it."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        at = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if at.tzinfo is not None:
        at = at.astimezone(UTC).replace(tzinfo=None)
    if timeframe == "1day":
        # The old renderer put a time on the nearest bar.
        return (at + timedelta(hours=12)).date().isoformat()
    return at.replace(microsecond=0).isoformat(timespec="seconds")


def _price(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value) if math.isfinite(value) else None


def _point(value: Any, timeframe: str) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    time, price = layer_time(value.get("time"), timeframe), _price(value.get("price"))
    return None if time is None or price is None else {"time": time, "price": price}


def _color(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    if _schema().is_color(value):
        return value.strip()
    return _NAMED_COLORS.get(value.strip().lower())


def _words(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return " ".join(value.replace("\x00", "").split()) or None


def _fit(value: str | None, limit: int) -> tuple[str | None, str | None]:
    """Words cut to *limit*, and the whole of them as a note when cut."""
    if value is None or len(value) <= limit:
        return value, None
    return value[: limit - 1].rstrip() + "…", value[: _schema().MAX_NOTE]


def _style(value: Any) -> str | None:
    return value if value in ("solid", "dashed", "dotted") else None


def _drop_none(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


Element = tuple[str, dict[str, Any]]


def _price_line(ann: dict[str, Any], tf: str, color: str | None, label: str | None, note: str | None) -> Element | None:
    price = _price(ann.get("price"))
    if price is None:
        return None
    return "levels", {"price": price, "title": label, "color": color,
                      "style": _style(ann.get("style")), "note": note}


def _marker(ann: dict[str, Any], tf: str, color: str | None, label: str | None, note: str | None) -> Element | None:
    time = layer_time(ann.get("time"), tf)
    if time is None:
        return None
    words, note = _fit(_words(ann.get("text")), _schema().MAX_LABEL)
    position, shape = ann.get("position"), ann.get("shape")
    return "markers", {
        "time": time,
        "position": position if position in ("aboveBar", "belowBar", "inBar") else None,
        "shape": shape if shape in ("arrowUp", "arrowDown", "circle", "square") else None,
        "color": color, "text": words, "note": note,
    }


def _two_points(drawing: str) -> Callable[..., Element | None]:
    def convert(ann: dict[str, Any], tf: str, color: str | None, label: str | None, note: str | None) -> Element | None:
        p1, p2 = _point(ann.get("point1"), tf), _point(ann.get("point2"), tf)
        if p1 is None or p2 is None:
            return None
        return "drawings", {"type": drawing, "p1": p1, "p2": p2, "label": label,
                            "color": color, "note": note}

    return convert


def _vertical_line(ann: dict[str, Any], tf: str, color: str | None, label: str | None, note: str | None) -> Element | None:
    time = layer_time(ann.get("time"), tf)
    if time is None:
        return None
    return "drawings", {"type": "vline", "time": time, "label": label, "color": color,
                        "style": _style(ann.get("style")), "note": note}


def _text(ann: dict[str, Any], tf: str, color: str | None, label: str | None, note: str | None) -> Element | None:
    words, note = _fit(_words(ann.get("text")), _schema().MAX_TEXT)
    return _label(ann, tf, words, color, note)


def _event(ann: dict[str, Any], tf: str, color: str | None, label: str | None, note: str | None) -> Element | None:
    max_text = _schema().MAX_TEXT
    detail = _words(ann.get("detail"))
    words = _fit(_words(ann.get("title")), max_text)[0] or _fit(detail, max_text)[0]
    note = detail[: _schema().MAX_NOTE] if detail else None
    return _label(ann, tf, words, color or _EVENT_COLOR, note)


def _label(ann: dict[str, Any], tf: str, words: str | None, color: str | None, note: str | None) -> Element | None:
    at = _point(ann, tf)
    if at is None or not words:
        return None
    return "drawings", {"type": "label", "at": at, "text": words, "color": color, "note": note}


#: Each legacy kind's converter: ``(annotation, timeframe, colour, label,
#: label's note)`` to ``(list, element)``, or None when it cannot be drawn.
_CONVERTERS: dict[str, Callable[..., Element | None]] = {
    "price_line": _price_line,
    "marker": _marker,
    "trendline": _two_points("segment"),
    "rectangle": _two_points("box"),
    "fib_retracement": _two_points("fib"),
    "vertical_line": _vertical_line,
    "text": _text,
    "event": _event,
}


def annotation_element(ann: dict[str, Any], timeframe: str) -> Element | None:
    """The layer element nearest one legacy annotation, as ``(list, element)``
    where list is ``levels``, ``markers`` or ``drawings``; None when it cannot
    be drawn. The element is validated."""
    convert = _CONVERTERS.get(ann.get("type"))  # type: ignore[arg-type]
    if convert is None:
        return None
    label, note = _fit(_words(ann.get("label")), _schema().MAX_LABEL)
    out = convert(ann, timeframe, _color(ann.get("color")), label, note)
    if out is None:
        return None
    where, element = out
    try:
        valid = _validators()[where].validate_python(_drop_none(element))
    except ValueError:
        return None
    return where, valid.model_dump(mode="json", by_alias=True, exclude_none=True)


# -- One chart ----------------------------------------------------------------


class _Pack:
    """The elements one layer takes, within its caps."""

    def __init__(self) -> None:
        self.lists: dict[str, list[dict[str, Any]]] = {where: [] for where in _LISTS}
        self.size = 0

    def fits(self, where: str, size: int) -> bool:
        ck = _schema()
        caps = {"levels": ck.MAX_LEVELS, "markers": ck.MAX_MARKERS, "drawings": ck.MAX_DRAWINGS}
        # Room under the byte cap for the layer's own fields.
        budget = ck.MAX_LAYER_BYTES - 4_096
        return len(self.lists[where]) < caps[where] and self.size + size <= budget

    def add(self, where: str, element: dict[str, Any], size: int) -> None:
        self.lists[where].append(element)
        self.size += size

    def __len__(self) -> int:
        return sum(len(items) for items in self.lists.values())


def _layer_names(taken: Collection[str]) -> Iterator[tuple[str, str]]:
    """Each next layer's ``(id, title)``, past the ids the chart has."""
    for n in itertools.count(1):
        layer_id = LAYER_ID if n == 1 else f"{LAYER_ID}-{n}"
        if layer_id not in taken:
            yield layer_id, LAYER_TITLE if n == 1 else f"{LAYER_TITLE} ({n})"


def chart_layers(
    symbol: str, timeframe: str, annotations: Iterable[Any], *, taken: Collection[str] = ()
) -> tuple[list[dict[str, Any]], int]:
    """One chart's legacy annotations (oldest first) as validated layers, each
    the payload its row stores, and how many annotations were dropped. Ids
    in ``taken``, the chart's own, are skipped. A chart no layer can name (a
    symbol or timeframe the layer refuses) drops them all."""
    packs = [_Pack()]
    dropped = 0
    for ann in annotations:
        converted = annotation_element(ann, timeframe) if isinstance(ann, dict) else None
        if converted is None:
            dropped += 1
            continue
        where, element = converted
        size = len(json.dumps(element)) + 1
        if not packs[-1].fits(where, size):
            packs.append(_Pack())
        packs[-1].add(where, element, size)
    layers = []
    for pack, (layer_id, title) in zip((p for p in packs if p), _layer_names(taken)):
        body = {
            "version": 1,
            "id": layer_id,
            "title": title,
            "summary": _SUMMARY, "symbol": symbol, "timeframe": timeframe,
            **pack.lists,
        }
        try:
            layer = _schema().ChartLayer.model_validate(body)
        except ValueError:
            return [], dropped + sum(len(p) for p in packs)
        layers.append(layer.model_dump(mode="json", by_alias=True, exclude_none=True))
    return layers, dropped


# -- The table ----------------------------------------------------------------


def stored_annotation(payload: Any) -> dict[str, Any] | None:
    """A row's payload as an annotation; None when it is not a JSON object
    (a jsonb array or scalar, or text that is not JSON), which cannot be drawn."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return None
    return payload if isinstance(payload, dict) else None


def stored_symbol(symbol: str) -> str:
    """The spelling the server stores and reads a chart by
    (``normalize_symbol``), or the symbol as it was if that fails."""
    try:
        from market_protocol import display_spelling

        return display_spelling(symbol) or symbol
    except Exception:  # noqa: BLE001 - the layer's validation judges it then
        return symbol


def chart_refusal(symbol: str, timeframe: str) -> str | None:
    """Why no layer can name this chart (its symbol or timeframe), or None."""
    try:
        _schema().ChartLayer.model_validate(
            {"version": 1, "id": LAYER_ID, "title": LAYER_TITLE, "symbol": symbol, "timeframe": timeframe}
        )
    except ValueError as e:
        return " ".join(str(e).split())
    return None


def upgrade() -> None:
    # An upgrade runs every pending revision in one transaction, so 116's
    # ALTER would hold command_runs, which every handed-off command writes,
    # until the conversion commits. Commit what came before first; the
    # conversion then runs in a transaction of its own.
    with op.get_context().autocommit_block():
        pass
    convert_legacy(op.get_bind())


def convert_legacy(bind: Any) -> None:
    """Turn every legacy annotation row into layers, in the caller's
    transaction. Safe to run again (see the module docstring)."""
    bind.execute(text("SET LOCAL lock_timeout = '5s'"))
    # Writers wait until commit, so no annotation lands between the read and
    # the delete; readers carry on.
    bind.execute(text("LOCK TABLE chart_annotations IN SHARE ROW EXCLUSIVE MODE"))
    rows = bind.execute(
        text(
            """
            SELECT workspace_id, chart_id, symbol, timeframe, annotation_id, payload, created_at
            FROM chart_annotations
            WHERE starts_with(annotation_id, :legacy)
            ORDER BY workspace_id, chart_id, created_at, annotation_id
            """
        ),
        {"legacy": _LEGACY_PREFIX},
    ).fetchall()
    log = logging.getLogger("alembic.runtime.migration")
    if not rows:
        log.info("117: no legacy chart annotations")
        return

    # Grouped by the chart the server reads, so two spellings of one ticker
    # become one chart.
    charts: dict[tuple[str, str, str], list[Any]] = {}
    for row in rows:
        symbol = stored_symbol(row.symbol)
        charts.setdefault((str(row.workspace_id), symbol, row.timeframe), []).append(row)
    taken = _taken_ids(bind, [(ws, f"{symbol}:{tf}") for ws, symbol, tf in charts])

    layer_count = 0
    dropped = 0
    converted: list[Any] = []
    for (ws, symbol, timeframe), chart_rows in charts.items():
        chart_id = f"{symbol}:{timeframe}"
        chart_rows.sort(key=lambda r: (r.created_at, r.annotation_id))
        refusal = chart_refusal(symbol, timeframe)
        if refusal is not None:
            log.warning(
                f"117: dropped {len(chart_rows)} annotation(s) of chart workspace_id={ws} "
                f"chart_id={chart_rows[0].chart_id!r}: no layer can name it ({refusal})"
            )
            dropped += len(chart_rows)
            converted.extend(chart_rows)
            continue
        payloads = [stored_annotation(r.payload) for r in chart_rows]
        layers, lost = chart_layers(symbol, timeframe, payloads, taken=taken.get((ws, chart_id), ()))
        if not layers:
            log.warning(
                f"117: dropped the {len(chart_rows)} annotation(s) of chart workspace_id={ws} "
                f"chart_id={chart_id!r}: none could be drawn"
            )
        dropped += lost
        written = 0
        first = chart_rows[0].created_at
        for n, layer in enumerate(layers):
            # A chart that already has a layer by the id keeps it; the ids
            # were picked past the ones it has, so none should collide.
            result = bind.execute(
                text(
                    """
                    INSERT INTO chart_annotations
                        (workspace_id, chart_id, symbol, timeframe, annotation_id, payload, created_at, updated_at)
                    VALUES (CAST(:ws AS uuid), :chart, :symbol, :timeframe, :id, CAST(:payload AS jsonb), :at, :at)
                    ON CONFLICT (workspace_id, chart_id, annotation_id) DO NOTHING
                    """
                ),
                {
                    "ws": ws,
                    "chart": chart_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "id": layer["id"],
                    "payload": json.dumps(layer),
                    # Oldest first, as the chart lists its layers.
                    "at": first + timedelta(microseconds=n),
                },
            )
            written += result.rowcount
        layer_count += written
        if written == len(layers):
            converted.extend(chart_rows)
        else:
            log.warning(
                f"117: chart workspace_id={ws} chart_id={chart_id!r} kept its "
                f"{len(chart_rows)} annotation row(s): {len(layers) - written} of its layers "
                "collided with an existing one"
            )
    if converted:
        bind.execute(
            text(
                """
                DELETE FROM chart_annotations c
                USING unnest(CAST(:ws AS uuid[]), CAST(:chart AS text[]), CAST(:id AS text[])) AS r(ws, chart, id)
                WHERE c.workspace_id = r.ws AND c.chart_id = r.chart AND c.annotation_id = r.id
                """
            ),
            {
                "ws": [str(r.workspace_id) for r in converted],
                "chart": [r.chart_id for r in converted],
                "id": [r.annotation_id for r in converted],
            },
        )
    log.info(
        f"117: {len(rows)} legacy annotation(s) on {len(charts)} chart(s) became "
        f"{layer_count} layer(s); {dropped} could not be drawn and were dropped"
    )


def _taken_ids(bind: Any, charts: list[tuple[str, str]]) -> dict[tuple[str, str], set[str]]:
    """The ``earlier-annotations`` layer ids each chart already has: from an
    earlier run of the conversion, or an agent that chose one."""
    found = bind.execute(
        text(
            """
            SELECT c.workspace_id, c.chart_id, c.annotation_id
            FROM chart_annotations c
            JOIN unnest(CAST(:ws AS uuid[]), CAST(:chart AS text[])) AS r(ws, chart)
              ON c.workspace_id = r.ws AND c.chart_id = r.chart
            WHERE starts_with(c.annotation_id, :layer_id)
            """
        ),
        {"ws": [ws for ws, _ in charts], "chart": [chart for _, chart in charts], "layer_id": LAYER_ID},
    ).fetchall()
    taken: dict[tuple[str, str], set[str]] = {}
    for row in found:
        taken.setdefault((str(row.workspace_id), row.chart_id), set()).add(row.annotation_id)
    return taken


def downgrade() -> None:
    """Nothing to undo that can be undone: the annotation rows are gone, and
    the previous build draws nothing from the layers that replaced them."""
