"""A chart layer's address, in the one shape every place that names it uses.

A chart instance is ``(workspace_id, chart_id)`` with ``chart_id =
"{SYMBOL}:{timeframe}"``, and a layer on it is named by its own id. The row,
the live event, the REST list, the tool result's artifact and a command's
report all carry these same fields, built here, so they never drift apart.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from typing import Any, Literal, TypedDict

from src.server.models.user import normalize_symbol

ChartOp = Literal["add", "remove", "clear"]


class PublishedLayer(TypedDict):
    """One published layer as a tool result or a command's report lists it,
    for the chat's chart-layer card: its address and what the card shows."""

    workspace_id: str
    chart_id: str
    symbol: str
    timeframe: str
    layer_id: str
    title: str
    summary: str | None
    as_of: str | None


@dataclass(frozen=True)
class ChartRef:
    workspace_id: str
    chart_id: str
    symbol: str
    timeframe: str

    @classmethod
    def of(cls, workspace_id: str, symbol: str, timeframe: str) -> ChartRef:
        """The chart, its ticker in the one spelling rows store, so
        ``600519.SS`` and ``600519.SH`` are one chart."""
        symbol, timeframe = normalize_symbol(symbol), timeframe.strip()
        return cls(workspace_id, f"{symbol}:{timeframe}", symbol, timeframe)

    def layer(self, layer_id: str) -> LayerRef:
        return LayerRef(**asdict(self), layer_id=layer_id)

    def event(self, op: ChartOp, **extra: Any) -> dict[str, Any]:
        """A ``chart_layer`` stream event's payload."""
        return {"op": op, **asdict(self), **extra}


@dataclass(frozen=True)
class LayerRef(ChartRef):
    layer_id: str

    def artifact(self, title: str, summary: str | None, as_of: str | None) -> PublishedLayer:
        return PublishedLayer(**asdict(self), title=title, summary=summary, as_of=as_of)

    def stored(self, layer: dict[str, Any]) -> dict[str, Any]:
        """A stored layer as the REST list returns it."""
        return {**asdict(self), "layer": layer}


@dataclass(frozen=True)
class CommandOutbox:
    """The chart layers a handed-off command published, as the finalize that
    ends the command stores them, in its own transaction: so they are stored
    once, by the one finalize that wins, and never by a settle that lost.
    ``report`` and ``layers`` are the row's read-back and card entries when
    the rows are stored; ``unsaved_report`` its read-back when they cannot be."""

    rows: Sequence[tuple[LayerRef, Mapping[str, Any]]] = ()
    report: str | None = None
    layers: Sequence[PublishedLayer] | None = None
    unsaved_report: str | None = None


def make_chart_id(symbol: str, timeframe: str) -> str:
    return ChartRef.of("", symbol, timeframe).chart_id


#: The keys of a ``chart_layer`` event a subagent's lane forwards: every one
#: but the layer body, which can run to megabytes and would fill the task's
#: buffer. A chart refetches a layer an add names without its body.
CHART_LAYER_FORWARDED_KEYS: tuple[str, ...] = (
    "op",
    *(f.name for f in fields(LayerRef)),
    "ids",
)
