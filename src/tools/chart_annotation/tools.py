"""LangChain tools for drawing and managing chart annotations.

These tools belong to the ``chart-annotation`` skill. A chart instance is
identified by ``chart_id = "{SYMBOL}:{timeframe}"`` and scoped to the agent's
workspace; drawing again with the same symbol + timeframe edits that chart.
Annotations persist in Postgres (durable, no TTL).
"""

import logging
import uuid
from typing import Any

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from pydantic import TypeAdapter, ValidationError

from src.server.database.chart_annotation import (
    add_and_list_annotations,
    clear_chart,
    list_annotations,
    make_chart_id,
    remove_annotations,
)
from src.tools.chart_annotation.schemas import (
    Annotation,
    DrawChartAnnotationArgs,
    ManageChartAnnotationsArgs,
    Timeframe,
)

logger = logging.getLogger(__name__)

# Validates raw-dict annotation payloads against the discriminated union.
_ANNOTATION_ADAPTER = TypeAdapter(Annotation)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _get_workspace_id(config: RunnableConfig) -> str:
    configurable = config.get("configurable", {})
    workspace_id = configurable.get("workspace_id")
    if not workspace_id:
        raise ValueError(
            "workspace_id not found in config. Chart annotations require a workspace."
        )
    return workspace_id


def _make_annotation_id() -> str:
    return f"ann_{uuid.uuid4().hex}"


def _normalize_annotation(annotation: Any) -> dict[str, Any] | None:
    """Turn the incoming annotation arg into a validated plain dict.

    The ``@tool`` decorator may hand us a Pydantic instance (if args_schema
    validated ahead of call) or a raw dict (if the LLM passed nested JSON).
    Raw dicts are re-validated through the annotation union so this path
    enforces the same shape + length caps as the args_schema path; an invalid
    payload returns ``None``.
    """
    if hasattr(annotation, "model_dump"):
        return annotation.model_dump()
    if isinstance(annotation, dict):
        try:
            return _ANNOTATION_ADAPTER.validate_python(annotation).model_dump()
        except ValidationError:
            return None
    return None


def _summarize(stored: dict[str, Any]) -> str:
    """Human-readable one-liner for the tool's content return."""
    kind = stored.get("type", "annotation")
    symbol = stored.get("symbol", "")
    timeframe = stored.get("timeframe", "")
    label = stored.get("label") or stored.get("text") or ""

    if kind == "price_line":
        base = f"Drew price line at ${stored.get('price')}"
    elif kind == "trendline":
        p1 = stored.get("point1", {})
        p2 = stored.get("point2", {})
        base = (
            f"Drew trendline from ({p1.get('time')}, ${p1.get('price')}) "
            f"to ({p2.get('time')}, ${p2.get('price')})"
        )
    elif kind == "marker":
        base = f"Placed {stored.get('shape', 'marker')} at {stored.get('time')}"
    elif kind == "vertical_line":
        base = f"Drew vertical line at {stored.get('time')}"
    elif kind == "rectangle":
        p1 = stored.get("point1", {})
        p2 = stored.get("point2", {})
        base = (
            f"Drew zone from ({p1.get('time')}, ${p1.get('price')}) "
            f"to ({p2.get('time')}, ${p2.get('price')})"
        )
    elif kind == "text":
        base = f"Added text at ({stored.get('time')}, ${stored.get('price')})"
    elif kind == "event":
        base = f"Marked event '{stored.get('title')}' at {stored.get('time')} (${stored.get('price')})"
    elif kind == "fib_retracement":
        p1 = stored.get("point1", {})
        p2 = stored.get("point2", {})
        base = (
            f"Drew Fibonacci retracement from ({p1.get('time')}, ${p1.get('price')}) "
            f"to ({p2.get('time')}, ${p2.get('price')})"
        )
    else:
        base = f"Drew {kind}"

    if label:
        base += f" — {label}"
    if symbol:
        base += f" on {symbol}"
        if timeframe:
            base += f" ({timeframe})"
    return base


def _emit(writer, artifact_type: str, artifact_id: str, payload: dict[str, Any]) -> None:
    """Best-effort stream writer call — never raises."""
    if writer is None:
        return
    try:
        writer(
            {
                "artifact_type": artifact_type,
                "artifact_id": artifact_id,
                "payload": payload,
            }
        )
    except Exception:
        logger.warning(
            "[chart_annotation] stream writer failed", exc_info=True
        )


def _get_writer():
    """Return the current LangGraph stream writer, or None when unavailable."""
    try:
        from langgraph.config import get_stream_writer

        return get_stream_writer()
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Tools
# --------------------------------------------------------------------------- #


@tool(
    "draw_chart_annotation",
    args_schema=DrawChartAnnotationArgs,
    response_format="content_and_artifact",
)
async def draw_chart_annotation(
    symbol: str,
    annotation: Any,
    config: RunnableConfig,
    timeframe: Timeframe = "1day",
) -> tuple[str, dict]:
    """Draw on the user's stock chart.

    Pick the variant that matches the intent:

    - ``price_line``: a horizontal level across the whole chart. Use for
      support, resistance, a target, a stop, or any flat price callout
      (e.g. "resistance at 205", "target 250").
    - ``trendline``: a line between two (time, price) anchors. Use for
      channels, pattern boundaries, or connecting two highs/lows across
      dates (e.g. "connect the Oct high and the Dec low").
    - ``marker``: an icon at a single bar. Use for event callouts or
      entry/exit signals (e.g. "earnings beat on Nov 14", "entry").
    - ``vertical_line``: a vertical line at one date across the whole
      chart. Use to mark an event in time (earnings, a split, FOMC).
    - ``rectangle``: a box over a time + price region (two opposite
      corners). Use for supply/demand zones or consolidation ranges.
    - ``text``: a free-floating text label anchored at (time, price).
      Use for a callout that isn't tied to a marker or level.
    - ``event``: a news/event badge anchored at (time, price) with a short
      ``title`` and a few-sentence ``detail`` revealed on hover/click. Use
      when a callout needs more context than a one-line label (e.g. an
      earnings report, an acquisition, an analyst action).
    - ``fib_retracement``: Fibonacci levels between a swing high and low
      (two anchors). Use to map retracement targets of a move.

    The chart is identified by ``SYMBOL:timeframe``: pass the same symbol +
    timeframe to add to (edit) that chart, or a different ticker/timeframe to
    start a new one. Annotations on a chart accumulate.
    """
    try:
        workspace_id = _get_workspace_id(config)
    except ValueError as exc:
        return f"Error: {exc}", {}

    payload = _normalize_annotation(annotation)
    if payload is None:
        return (
            "Error: invalid annotation payload — it did not match any known "
            "annotation type. Pass a valid annotation object (see the tool schema).",
            {},
        )

    symbol_upper = symbol.upper()
    chart_id = make_chart_id(symbol_upper, timeframe)
    annotation_id = _make_annotation_id()
    stored = {
        "annotation_id": annotation_id,
        "symbol": symbol_upper,
        "timeframe": timeframe,
        "chart_id": chart_id,
        **payload,
    }

    # Fail-closed: persist first. If the DB write fails, we return an error
    # tuple and do NOT emit an SSE event — the user sees a clear failure in
    # chat and no ghost drawing appears on the chart.
    # Persist the write and read back the full set in one connection checkout
    # (autocommit → the read sees the just-written row). Fail closed on the
    # write so no ghost drawing appears in chat or on the chart. The returned
    # set is the chart instance's full current annotations so the inline-card
    # artifact renders the cumulative chart, not just this one shape.
    try:
        all_items = await add_and_list_annotations(
            workspace_id, chart_id, symbol_upper, timeframe, stored
        )
    except Exception:
        logger.exception("[chart_annotation] persistence failed")
        return (
            "Could not save annotation (persistence unavailable). "
            "The drawing was NOT applied. Please try again in a moment.",
            {},
        )
    if not all_items:
        all_items = [stored]

    _emit(
        _get_writer(),
        artifact_type="chart_annotation",
        artifact_id=annotation_id,
        payload={
            "op": "add",
            "workspace_id": workspace_id,
            "chart_id": chart_id,
            "symbol": symbol_upper,
            "timeframe": timeframe,
            "annotation_id": annotation_id,
            "annotation": stored,
        },
    )

    result_artifact = {
        "type": "chart_annotation",
        "op": "add",
        "chart_id": chart_id,
        "symbol": symbol_upper,
        "timeframe": timeframe,
        "workspace_id": workspace_id,
        "annotation_id": annotation_id,
        "annotations": all_items,
    }
    return _summarize(stored), result_artifact


@tool(
    "manage_chart_annotations",
    args_schema=ManageChartAnnotationsArgs,
    response_format="content_and_artifact",
)
async def manage_chart_annotations(
    symbol: str,
    action: str,
    config: RunnableConfig,
    timeframe: Timeframe = "1day",
    ids: list[str] | None = None,
) -> tuple[str, dict]:
    """Inspect or delete annotations on a chart (``SYMBOL:timeframe``).

    Actions:

    - ``list``: return all annotations currently on the chart. Do not pass
      ``ids``. Use this first if you need ids for a later remove call.
    - ``remove``: delete specific annotations by id. ``ids`` is required
      and must be non-empty. Use ``clear_all`` instead if you want to
      wipe everything.
    - ``clear_all``: delete every annotation on the chart. Do not pass
      ``ids`` — the tool will reject the call. Use ``remove`` for
      partial deletion.
    """
    try:
        workspace_id = _get_workspace_id(config)
    except ValueError as exc:
        return f"Error: {exc}", {}

    symbol_upper = symbol.upper()
    chart_id = make_chart_id(symbol_upper, timeframe)

    if action == "list":
        if ids:
            return "Error: 'list' action does not accept 'ids'.", {}
        try:
            items = await list_annotations(workspace_id, chart_id)
        except Exception:
            logger.exception("[chart_annotation] list failed")
            return (
                "Could not list annotations (persistence unavailable). "
                "Please try again in a moment.",
                {},
            )
        return (
            f"{len(items)} annotation(s) on {chart_id}.",
            {"chart_id": chart_id, "symbol": symbol_upper, "timeframe": timeframe, "annotations": items},
        )

    if action == "remove":
        if not ids:
            return (
                "Error: 'remove' requires a non-empty 'ids' list. "
                "Call manage_chart_annotations(action='list', symbol=..., "
                "timeframe=...) first to discover ids, or use "
                "action='clear_all' to wipe the whole chart.",
                {},
            )
        try:
            removed = await remove_annotations(workspace_id, chart_id, ids)
        except Exception:
            logger.exception("[chart_annotation] remove failed")
            return (
                "Could not remove annotations (persistence unavailable). "
                "Nothing was deleted. Please try again in a moment.",
                {},
            )
        _emit(
            _get_writer(),
            artifact_type="chart_annotation",
            artifact_id=f"remove:{chart_id}:{','.join(ids)}",
            payload={"op": "remove", "workspace_id": workspace_id, "chart_id": chart_id, "symbol": symbol_upper, "timeframe": timeframe, "ids": ids},
        )
        return (
            f"Removed {removed} annotation(s) from {chart_id}.",
            {"chart_id": chart_id, "symbol": symbol_upper, "timeframe": timeframe, "removed": removed, "ids": ids},
        )

    if action == "clear_all":
        if ids:
            return (
                "Error: 'clear_all' does not accept 'ids'. "
                "Use action='remove' with a specific id list for partial deletion.",
                {},
            )
        try:
            cleared = await clear_chart(workspace_id, chart_id)
        except Exception:
            logger.exception("[chart_annotation] clear failed")
            return (
                "Could not clear annotations (persistence unavailable). "
                "Nothing was deleted. Please try again in a moment.",
                {},
            )
        _emit(
            _get_writer(),
            artifact_type="chart_annotation",
            artifact_id=f"clear:{chart_id}",
            payload={"op": "clear", "workspace_id": workspace_id, "chart_id": chart_id, "symbol": symbol_upper, "timeframe": timeframe},
        )
        return (
            f"Cleared {cleared} annotation(s) from {chart_id}.",
            {"chart_id": chart_id, "symbol": symbol_upper, "timeframe": timeframe, "cleared": cleared},
        )

    return (
        f"Error: unknown action '{action}'. Use 'list', 'remove', or 'clear_all'.",
        {},
    )
