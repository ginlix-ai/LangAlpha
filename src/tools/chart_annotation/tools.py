"""``manage_chart_layers``: the agent's way to inspect and delete the chart
layers its code published. A chart is ``chart_id = "{SYMBOL}:{timeframe}"``,
scoped to the agent's workspace; layers persist in Postgres (no TTL).
"""

import logging
from dataclasses import asdict

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from chartkit.schema import plain_problem
from pydantic import ValidationError

from src.server.database.chart_annotation import clear_chart, list_layers, remove_layers
from src.server.models.chart_layer import ChartRef
from src.tools.chart_annotation.layers import layer_counts
from src.tools.chart_annotation.schemas import ChartLayerAction, ManageChartLayersArgs, Timeframe
from src.tools.chart_annotation.stream import emit, get_writer

logger = logging.getLogger(__name__)


def _persistence_error(what: str, *, deleting: bool = True) -> tuple[str, dict]:
    """What a store failure answers: ``what`` failed, and nothing was deleted."""
    nothing = " Nothing was deleted." if deleting else ""
    return f"Could not {what} (persistence unavailable).{nothing} Try again in a moment.", {}


def _invalid_args(e: ValidationError) -> str:
    """Arguments the schema refused, as the agent reads them: each problem's
    own words, under the argument it names."""
    problems = (
        f"{'.'.join(map(str, err['loc']))}: {plain_problem(err)}" if err["loc"]
        else plain_problem(err, got=False)
        for err in e.errors(include_url=False)
    )
    return "Error: " + "; ".join(problems)


@tool(
    "manage_chart_layers",
    args_schema=ManageChartLayersArgs,
    response_format="content_and_artifact",
)
async def manage_chart_layers(
    symbol: str,
    action: ChartLayerAction,
    config: RunnableConfig,
    timeframe: Timeframe = "1day",
    ids: list[str] | None = None,
) -> tuple[str, dict]:
    """List or delete the chart layers on a chart (``SYMBOL:timeframe``). A layer
    is drawn, and changed, by publishing it from code (the chart-annotation skill).

    Returns:
        For ``list``, each layer's id, title and element counts, not its contents.
    """
    workspace_id = (config.get("configurable") or {}).get("workspace_id")
    if not workspace_id:
        return "Error: chart layers need a workspace, and this thread has none.", {}
    # The spelling the rows and the chart id use (BRK.B is BRK-B), so the
    # event's symbol and its chart_id name one chart.
    chart = ChartRef.of(workspace_id, symbol, timeframe)
    fields = asdict(chart)

    match action:
        case "list":
            try:
                stored = await list_layers(workspace_id, chart.chart_id)
            except Exception:
                logger.exception("[chart_layers] list failed")
                return _persistence_error("list layers", deleting=False)
            # Layers are listed by id, title and size, never whole: one can run
            # to megabytes, and the artifact is kept in the thread's history.
            layers = [
                {
                    "layer_id": row["layer_id"],
                    "title": row["layer"]["title"],
                    "elements": layer_counts(row["layer"]),
                }
                for row in stored
            ]
            text = f"{len(layers)} layer(s) on {chart.chart_id}."
            if layers:
                text += "\n" + "\n".join(
                    f"- {lyr['layer_id']} {lyr['title']!r}: {lyr['elements']}" for lyr in layers
                )
            return text, {**fields, "layers": layers}

        case "remove":
            assert ids  # the args schema requires them
            try:
                removed = await remove_layers(workspace_id, chart.chart_id, ids)
            except Exception:
                logger.exception("[chart_layers] remove failed")
                return _persistence_error("remove layers")
            emit(
                get_writer(),
                artifact_type="chart_layer",
                artifact_id=f"remove:{chart.chart_id}:{','.join(ids)}",
                payload=chart.event("remove", ids=ids),
            )
            text = f"Removed {len(removed)} layer(s) from {chart.chart_id}."
            if missed := [i for i in dict.fromkeys(ids) if i not in removed]:
                names = ", ".join(repr(i) for i in missed)
                text += f" No layer {names} on this chart; action='list' shows the ids."
            return text, {**fields, "removed": len(removed), "ids": ids}

        case "clear_all":
            try:
                cleared = await clear_chart(workspace_id, chart.chart_id)
            except Exception:
                logger.exception("[chart_layers] clear failed")
                return _persistence_error("clear the chart")
            emit(
                get_writer(),
                artifact_type="chart_layer",
                artifact_id=f"clear:{chart.chart_id}",
                payload=chart.event("clear"),
            )
            return (
                f"Cleared {cleared} layer(s) from {chart.chart_id}.",
                {**fields, "cleared": cleared},
            )


# Set after the decorator, which takes no such argument: an args refusal
# answers the call in the tool's own words, as its other errors do.
manage_chart_layers.handle_validation_error = _invalid_args
