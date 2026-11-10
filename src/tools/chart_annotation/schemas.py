"""Pydantic schema for the chart-layer tool's arguments."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, model_validator

# The timeframes a chart can exist on (charts are SYMBOL:timeframe), defined
# once with the chart layer contract chartkit ships into the sandbox.
from chartkit.schema import Timeframe

ChartLayerAction = Literal["list", "remove", "clear_all"]


class ManageChartLayersArgs(BaseModel):
    """Arguments for ``manage_chart_layers``."""

    symbol: str = Field(max_length=32, description="Ticker of the chart (e.g. 'NVDA').")
    timeframe: Timeframe = Field(
        default="1day",
        description="Chart interval; with the symbol it names the chart (SYMBOL:timeframe).",
    )
    action: ChartLayerAction = Field(
        description="'list' the layers, 'remove' the ids given, or 'clear_all'."
    )
    ids: list[Annotated[str, Field(min_length=1, max_length=64)]] | None = Field(
        default=None,
        max_length=100,
        description="Layer ids to delete; 'remove' only.",
    )

    @model_validator(mode="after")
    def _ids_fit_the_action(self) -> Self:
        match self.action:
            case "remove" if not self.ids:
                raise ValueError(
                    "'remove' needs the layer ids to delete. "
                    "action='list' shows them; action='clear_all' deletes every layer."
                )
            case "list" if self.ids:
                raise ValueError("'list' takes no 'ids'.")
            case "clear_all" if self.ids:
                raise ValueError("'clear_all' takes no 'ids'; action='remove' deletes some layers.")
        return self
