"""Pydantic schemas for chart annotation tool arguments.

The annotation union uses a discriminated union keyed by ``type`` so the LLM
sees a clean ``oneOf`` in the JSON schema rather than a flat bag of
mostly-None fields.
"""

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


# Bounds keep LLM-generated strings small enough that a runaway agent can't
# bloat storage. All fields are cosmetic — the chart has no use for oversized
# strings, so capping here is lossless.
_MAX_TIME_LEN = 40   # ISO8601 with timezone comfortably fits
_MAX_LABEL_LEN = 200
_MAX_COLOR_LEN = 64  # #rrggbb, rgba(...), or CSS name
_MAX_DETAIL_LEN = 600  # a few sentences of event context, revealed on hover/click

# Intervals the market-data API (and the inline chart card) can fetch. The
# chart instance is identified by SYMBOL:timeframe, so this is also the set of
# timeframes a chart can exist on.
Timeframe = Literal[
    "1min", "5min", "15min", "30min", "1hour", "4hour", "1day"
]


class _AnnotationBase(BaseModel):
    """Base for annotation models. Rejects NaN/Infinity floats — Pydantic
    allows them by default but they break JSONB serialization and fail the draw.
    """

    model_config = ConfigDict(allow_inf_nan=False)


class TimePricePoint(_AnnotationBase):
    """A single (time, price) anchor on the chart."""

    time: str = Field(
        max_length=_MAX_TIME_LEN,
        description="ISO8601 datetime aligned to a bar on the chart (e.g. '2024-10-16T00:00:00Z')",
    )
    price: float = Field(description="Price (y-axis value) at the given time")


class PriceLineAnnotation(_AnnotationBase):
    """Horizontal price level — support/resistance/target."""

    type: Literal["price_line"]
    price: float = Field(description="Price level (y-axis value)")
    label: str | None = Field(
        default=None,
        max_length=_MAX_LABEL_LEN,
        description="Short label shown with the line, e.g. 'Resistance 205'",
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color (hex, rgb, or named). Default: theme-aware.",
    )
    style: Literal["solid", "dashed", "dotted"] = "solid"


class TrendlineAnnotation(_AnnotationBase):
    """Trendline connecting two (time, price) anchors."""

    type: Literal["trendline"]
    point1: TimePricePoint
    point2: TimePricePoint
    label: str | None = Field(
        default=None,
        max_length=_MAX_LABEL_LEN,
        description="Short label shown near the line, e.g. 'Channel top'",
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color",
    )


class MarkerAnnotation(_AnnotationBase):
    """Event callout at a specific bar."""

    type: Literal["marker"]
    time: str = Field(
        max_length=_MAX_TIME_LEN,
        description="ISO8601 datetime of the bar to mark",
    )
    shape: Literal["arrowUp", "arrowDown", "circle", "square"]
    position: Literal["aboveBar", "belowBar", "inBar"] = "aboveBar"
    text: str | None = Field(
        default=None,
        max_length=_MAX_LABEL_LEN,
        description="Short label shown on/near the marker (e.g. 'Earnings beat')",
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color",
    )


class VerticalLineAnnotation(_AnnotationBase):
    """Vertical line marking a moment in time across the whole chart."""

    type: Literal["vertical_line"]
    time: str = Field(
        max_length=_MAX_TIME_LEN,
        description="ISO8601 datetime of the bar to mark (e.g. earnings or FOMC date)",
    )
    label: str | None = Field(
        default=None,
        max_length=_MAX_LABEL_LEN,
        description="Short label shown at the top of the line, e.g. 'Earnings'",
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color",
    )
    style: Literal["solid", "dashed", "dotted"] = "dashed"


class RectangleAnnotation(_AnnotationBase):
    """Rectangular zone spanning a time range and a price range.

    Use for supply/demand zones, consolidation ranges, or any box that
    calls out a region of the chart. ``point1`` and ``point2`` are two
    opposite corners — order does not matter.
    """

    type: Literal["rectangle"]
    point1: TimePricePoint
    point2: TimePricePoint
    label: str | None = Field(
        default=None,
        max_length=_MAX_LABEL_LEN,
        description="Short label shown in the box, e.g. 'Demand zone'",
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color for the border and (translucent) fill",
    )


class TextAnnotation(_AnnotationBase):
    """Free-floating text label anchored at a (time, price) point."""

    type: Literal["text"]
    time: str = Field(
        max_length=_MAX_TIME_LEN,
        description="ISO8601 datetime anchoring the text horizontally",
    )
    price: float = Field(description="Price (y-axis value) anchoring the text vertically")
    text: str = Field(
        max_length=_MAX_LABEL_LEN,
        description="The text to display on the chart",
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color",
    )


class EventAnnotation(_AnnotationBase):
    """News/event callout anchored at a (time, price) point.

    Renders an always-visible title badge on the chart; the longer ``detail``
    is revealed on hover (desktop) or tap (mobile). Use for earnings, M&A,
    analyst actions, product launches, or any significant news tied to a bar
    that warrants more than a one-line label.
    """

    type: Literal["event"]
    time: str = Field(
        max_length=_MAX_TIME_LEN,
        description="ISO8601 datetime of the event, aligned to a bar (e.g. '2024-11-14T00:00:00Z')",
    )
    price: float = Field(
        description=(
            "Price (y-axis value) the badge anchors to — usually the bar's "
            "close or the level the news affected"
        ),
    )
    title: str = Field(
        max_length=_MAX_LABEL_LEN,
        description="Short headline shown on the badge, e.g. 'Q3 earnings beat'",
    )
    detail: str = Field(
        max_length=_MAX_DETAIL_LEN,
        description=(
            "A few sentences shown on hover/click — the context behind the "
            "event (what happened and why it matters)"
        ),
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color for the badge accent. Default: theme-aware.",
    )


class FibRetracementAnnotation(_AnnotationBase):
    """Fibonacci retracement between a swing high and a swing low.

    Standard levels (0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0) are drawn as
    horizontal lines spanning the two anchor times. ``point1`` and
    ``point2`` are the two ends of the move (e.g. swing low → swing high).
    """

    type: Literal["fib_retracement"]
    point1: TimePricePoint
    point2: TimePricePoint
    label: str | None = Field(
        default=None,
        max_length=_MAX_LABEL_LEN,
        description="Short label shown with the retracement, e.g. 'Oct–Dec move'",
    )
    color: str | None = Field(
        default=None,
        max_length=_MAX_COLOR_LEN,
        description="CSS color",
    )


# Discriminated union — the LLM sees `oneOf` keyed by `type`.
Annotation = Annotated[
    Union[
        PriceLineAnnotation,
        TrendlineAnnotation,
        MarkerAnnotation,
        VerticalLineAnnotation,
        RectangleAnnotation,
        TextAnnotation,
        EventAnnotation,
        FibRetracementAnnotation,
    ],
    Field(discriminator="type"),
]


class DrawChartAnnotationArgs(BaseModel):
    """Arguments for ``draw_chart_annotation``."""

    symbol: str = Field(
        max_length=32,
        description="Ticker symbol of the chart (e.g. 'NVDA').",
    )
    timeframe: Timeframe = Field(
        default="1day",
        description=(
            "Chart interval. Together with the symbol this is the chart's "
            "identity (chart_id = 'SYMBOL:timeframe'): drawing again with the "
            "same symbol + timeframe edits THAT chart, while a different ticker "
            "or timeframe starts a new one. Match the interval the user is "
            "viewing (default daily)."
        ),
    )
    annotation: Annotation = Field(
        description=(
            "Annotation payload. The 'type' field discriminates the variant: "
            "'price_line', 'trendline', 'marker', 'vertical_line', "
            "'rectangle', 'text', 'event', or 'fib_retracement'."
        )
    )


class ManageChartAnnotationsArgs(BaseModel):
    """Arguments for ``manage_chart_annotations``."""

    symbol: str = Field(
        max_length=32, description="Ticker symbol (scopes the operation)"
    )
    timeframe: Timeframe = Field(
        default="1day",
        description=(
            "Chart interval. With the symbol it selects the chart instance "
            "(chart_id = 'SYMBOL:timeframe') to inspect or modify."
        ),
    )
    action: Literal["list", "remove", "clear_all"] = Field(
        description=(
            "What to do: 'list' returns current annotations for the chart, "
            "'remove' deletes specific ids (requires ids), "
            "'clear_all' deletes every annotation on the chart (must not pass ids)."
        )
    )
    ids: list[Annotated[str, Field(max_length=64)]] | None = Field(
        default=None,
        max_length=100,
        description=(
            "Annotation ids to delete. Required when action='remove'. "
            "Must be omitted/None for action='list' or action='clear_all'."
        ),
    )
