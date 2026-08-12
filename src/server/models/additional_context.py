"""
Additional context models for workflow execution.

Supports flexible context types that can be passed along with user queries.
Contexts are fetched, formatted, and appended to user messages before processing.
"""

from datetime import datetime
from typing import Annotated, Any, Literal, Optional, List, Union
from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    model_validator,
)

from src.tools.chart_annotation.schemas import Timeframe


class AdditionalContextBase(BaseModel):
    """Base model for additional context with type discrimination."""

    type: str = Field(..., description="Type of context (e.g., 'skills')")
    id: Optional[str] = Field(None, description="Resource identifier for fetching context")


class SkillContext(AdditionalContextBase):
    """Context requesting skill instructions to be loaded for the agent."""

    type: Literal["skills"] = "skills"
    name: str = Field(..., description="Skill name (e.g., 'user-profile')")
    instruction: Optional[str] = Field(
        None,
        description="Additional instruction for the skill (e.g., 'Help the user with first time onboarding')"
    )


class MultimodalContext(AdditionalContextBase):
    """Context providing an image, PDF, or arbitrary file attachment."""

    type: Literal["image", "pdf", "file"] = "image"
    data: str = Field(..., description="Base64 data URL (data:<mime>;base64,...)")
    description: Optional[str] = Field(None, description="Filename or caption for the attachment")


class DirectiveContext(AdditionalContextBase):
    """Context injecting a directive inline with the user message via XML tags."""

    type: Literal["directive"] = "directive"
    content: str = Field(..., description="Directive text to inject inline with user message")


class WidgetContext(AdditionalContextBase):
    """Context attached from a dashboard widget snapshot.

    Carries pre-rendered ``<widget-context>...</widget-context>`` markdown that
    is concatenated into a single ``<system-reminder>`` and appended to the last
    user message. Image bytes for chart-type widgets ride the existing
    ``MultimodalContext(type='image')`` channel — this model does not transport
    image data.
    """

    type: Literal["widget"] = "widget"
    widget_type: str = Field(..., description="Widget definition id (e.g., 'markets.chart')")
    widget_id: str = Field(..., description="Widget instance id (uuid) — stable across reflows")
    label: str = Field(..., description="Human-readable label for the snapshot (chip title)")
    text: str = Field(..., description="Pre-rendered <widget-context>...</widget-context> markdown")
    data: dict[str, Any] = Field(default_factory=dict, description="Structured raw payload for replay")
    captured_at: Optional[datetime] = Field(None, description="When the snapshot was taken (client clock)")
    description: Optional[str] = Field(None, description="Optional caption / freshness note")


# Defensive cap on transported bars — a region selection should never need more
# than this many candles for the agent to reason about. Anything larger is
# sliced and flagged so a runaway client can't bloat the prompt or storage.
_MAX_SELECTION_BARS = 500


class SelectionBar(BaseModel):
    """A single OHLCV candle inside a chart selection.

    ``extra="ignore"`` silently drops unknown keys; ``allow_inf_nan=False``
    rejects NaN/Inf so serialized metadata stays JSON-safe.
    """

    time: str = Field(..., max_length=40, description="ISO8601 bar timestamp")
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None

    model_config = ConfigDict(allow_inf_nan=False, extra="ignore")


class ChartSelectionContext(AdditionalContextBase):
    """Context for a region or price level the user selected on the chart.

    Carries only the structured selection (bounds + OHLCV bars + optional note);
    there is no cropped screenshot — the exact bars are the agent's signal.
    """

    type: Literal["chart_selection"] = "chart_selection"
    symbol: str = Field(..., max_length=32, description="Ticker symbol of the chart (e.g. 'NVDA')")
    timeframe: Timeframe = Field(..., description="Chart interval; with symbol it is the chart id")
    selection_type: Literal["region", "price_level"] = Field(
        ..., description="'region' = time×price box, 'price_level' = a single price"
    )
    time_start: Optional[str] = Field(None, max_length=40, description="ISO8601 region start (region only)")
    time_end: Optional[str] = Field(None, max_length=40, description="ISO8601 region end (region only)")
    price_low: float = Field(..., description="Lower price bound (the level itself for price_level)")
    price_high: float = Field(..., description="Upper price bound")
    bars: list[SelectionBar] = Field(default_factory=list, description="OHLCV bars inside the selection")
    bars_truncated: bool = Field(False, description="True when bars were dropped to fit")
    label: Optional[str] = Field(
        None, max_length=500, description="Optional user-supplied note for this selection"
    )

    model_config = ConfigDict(allow_inf_nan=False)

    @model_validator(mode="after")
    def _validate_geometry(self) -> "ChartSelectionContext":
        """Require region bounds, normalize the price range, and cap bars.

        A price_level is a single price, so any stray high is collapsed onto
        price_low — the render and draw-back hint read only price_low, so the
        stored/replayed range must not disagree with it.
        """
        if self.selection_type == "region" and not (self.time_start and self.time_end):
            raise ValueError("region selection requires both time_start and time_end")
        if self.price_low > self.price_high:
            self.price_low, self.price_high = self.price_high, self.price_low
        if self.selection_type == "price_level":
            self.price_high = self.price_low
        if len(self.bars) > _MAX_SELECTION_BARS:
            self.bars = self.bars[:_MAX_SELECTION_BARS]
            self.bars_truncated = True
        return self


AdditionalContext = Annotated[
    Union[
        Annotated[SkillContext, Tag("skills")],
        Annotated[MultimodalContext, Tag("image")],
        Annotated[MultimodalContext, Tag("pdf")],
        Annotated[MultimodalContext, Tag("file")],
        Annotated[DirectiveContext, Tag("directive")],
        Annotated[WidgetContext, Tag("widget")],
        Annotated[ChartSelectionContext, Tag("chart_selection")],
    ],
    Discriminator(lambda v: v.get("type") if isinstance(v, dict) else getattr(v, "type", None)),
]


def format_additional_contexts(contexts: List[AdditionalContextBase]) -> str:
    """Join multiple context strings into a single markdown section with a separator."""
    if not contexts:
        return ""

    return "\n\n---\n\n" + "\n\n".join(contexts)
