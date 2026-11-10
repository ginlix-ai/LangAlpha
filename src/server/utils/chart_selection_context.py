"""
Chart selection context utilities for chat endpoint.

Parses ChartSelectionContext items from additional_context and builds a single
``<system-reminder>`` block describing each region / price level the user
selected on the chart. Selections are structured-only (bounds + OHLCV bars +
the draw-back hint); they also persist a compact summary into query metadata so
the chat can re-render the selection card on history replay.
"""

import logging
from datetime import datetime
from typing import Any, List, Optional

from src.server.models.additional_context import ChartSelectionContext, SelectionBar

logger = logging.getLogger(__name__)

# Cap the OHLCV rows rendered into the prompt. The model already bounds the
# transported bars; this keeps the table itself compact when the selection is
# wide. Showing fewer rows than held flips the truncation note on.
_MAX_TABLE_ROWS = 100

_OHLCV_HEADER = (
    "| time | open | high | low | close | volume |\n"
    "|---|---|---|---|---|---|"
)


def parse_chart_selection_contexts(
    additional_context: Optional[List[Any]],
) -> List[ChartSelectionContext]:
    """Extract ChartSelectionContext items from additional_context list."""
    if not additional_context:
        return []

    contexts: List[ChartSelectionContext] = []

    for ctx in additional_context:
        if isinstance(ctx, dict):
            if ctx.get("type") == "chart_selection":
                contexts.append(ChartSelectionContext.model_validate(ctx))
        elif isinstance(ctx, ChartSelectionContext):
            contexts.append(ctx)

    return contexts


_CHART_SELECTION_PREAMBLE = (
    "The user selected the following on the price chart and sent it with this "
    "message. Each <chart-selection> block is a region (time×price box) or a "
    "single price level they highlighted, with the chart's SYMBOL:timeframe, the "
    "bounds, and the OHLCV bars inside it. Analyze the bounded area — lean on the "
    "supplied bars and/or your market-data tools — and, when it helps, draw your "
    "conclusion back onto the SAME chart as a chart layer."
)


def _fmt(value: Any) -> str:
    """Render a numeric cell as a plain string, leaving missing values blank."""
    if value is None:
        return ""
    return str(value)


def _fmt_price(value: Optional[float]) -> str:
    """Render a price, dropping pixel-interpolation FP noise.

    Selection bounds come from client pixel→price interpolation, so a clean
    195.10 can arrive as 195.10000000000002. ``%g`` is magnitude-aware: it strips
    that noise without a fixed decimal count, so sub-dollar and large prices both
    keep their real precision (and unlike volume, prices never need %g's
    scientific fallback at realistic magnitudes).
    """
    if value is None:
        return ""
    return f"{value:.12g}"


def _chart_time(value: Optional[str], timeframe: str) -> str:
    """A selection time as chartkit reads it: the exchange's wall clock, no zone.

    The web encodes chart times as the venue's wall clock stamped UTC (``Z``),
    so the zone is false: kept, chartkit would convert it to exchange time and
    draw a daily box a day early, an intraday one hours off. Text that is not
    an ISO time passes through as sent.
    """
    if not value:
        return ""
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return moment.strftime("%Y-%m-%d" if timeframe == "1day" else "%Y-%m-%d %H:%M")


def _render_bars_table(bars: List[SelectionBar], timeframe: str) -> tuple[str, str]:
    """Render an OHLCV markdown table plus a truncation note.

    Returns ``(table, note)`` where ``note`` is empty when nothing was dropped.
    """
    total = len(bars)
    shown = bars[:_MAX_TABLE_ROWS]
    rows = [
        "| {time} | {open} | {high} | {low} | {close} | {volume} |".format(
            time=_chart_time(b.time, timeframe),
            open=_fmt_price(b.open),
            high=_fmt_price(b.high),
            low=_fmt_price(b.low),
            close=_fmt_price(b.close),
            volume=_fmt(b.volume),
        )
        for b in shown
    ]
    table = _OHLCV_HEADER + "\n" + "\n".join(rows) if rows else ""
    note = ""
    if len(shown) < total:
        note = f"(truncated, {len(shown)} of {total} bars shown)"
    return table, note


def _render_selection(sel: ChartSelectionContext) -> str:
    """Render a single ``<chart-selection>`` block."""
    symbol = sel.symbol.upper()
    chart_id = f"{symbol}:{sel.timeframe}"
    lines = [
        f"<chart-selection chart_id='{chart_id}' selection_type='{sel.selection_type}'>",
        f"Symbol: {symbol} · Timeframe: {sel.timeframe}",
    ]
    if sel.label:
        lines.append(f"User note: {sel.label}")

    if sel.selection_type == "region":
        start = _chart_time(sel.time_start, sel.timeframe)
        end = _chart_time(sel.time_end, sel.timeframe)
        lines.append(f"Time range: {start} → {end}")
        lines.append(f"Price range: {_fmt_price(sel.price_low)} – {_fmt_price(sel.price_high)}")
        draw_back = (
            f'To draw this back, build on bars("{symbol}", "{sel.timeframe}") and add '
            f'c.box(("{start}", {_fmt_price(sel.price_high)}), '
            f'("{end}", {_fmt_price(sel.price_low)})).'
        )
    else:
        lines.append(f"Price level: {_fmt_price(sel.price_low)}")
        draw_back = (
            f'To draw this back, build on bars("{symbol}", "{sel.timeframe}") and add '
            f"c.level({_fmt_price(sel.price_low)})."
        )

    table, note = _render_bars_table(sel.bars, sel.timeframe)
    if table:
        lines.append("")
        lines.append("Bars (OHLCV):")
        lines.append(table)
        if note or sel.bars_truncated:
            lines.append(note or "(truncated — earlier bars omitted to fit)")

    lines.append("")
    lines.append(draw_back)
    lines.append("</chart-selection>")
    return "\n".join(lines)


def build_chart_selection_reminder(
    selections: List[ChartSelectionContext],
) -> Optional[str]:
    """Build a system-reminder block from chart selection contexts.

    Concatenates each selection's ``<chart-selection>`` block into one
    ``<system-reminder>`` envelope, prefixed by an explainer so the agent knows
    the user drew these on the chart. Returns ``None`` when there is nothing to
    inject so the caller can skip the append step entirely.
    """
    if not selections:
        return None

    body = "\n\n".join(_render_selection(s) for s in selections)
    return (
        "\n\n<system-reminder>\n"
        f"{_CHART_SELECTION_PREAMBLE}\n\n"
        f"{body}\n"
        "</system-reminder>"
    )


def serialize_chart_selections_for_metadata(
    selections: List[ChartSelectionContext],
) -> List[dict]:
    """Serialize selections for ``query_metadata['chart_selections']``.

    Emits the camelCase snapshot the frontend selection card reads directly on
    history replay. Mirrors the live ``ChartSelectionSnapshot``: card-face
    fields (identity + note) plus the agent-context detail (time bounds + the
    OHLCV bars the agent received) so the card's preview renders identically on
    replay.
    """
    return [
        {
            "selectionType": s.selection_type,
            "symbol": s.symbol,
            "timeframe": s.timeframe,
            "priceLow": s.price_low,
            "priceHigh": s.price_high,
            **({"comment": s.label} if s.label else {}),
            **({"timeStart": s.time_start} if s.time_start else {}),
            **({"timeEnd": s.time_end} if s.time_end else {}),
            "bars": [b.model_dump() for b in s.bars],
            "barsTruncated": s.bars_truncated,
        }
        for s in selections
    ]
