"""The charts a user is looking at as they send, a request's ``chart_views``.

Never rendered into the prompt: the turn's sandbox jobs read them as
``PTC_CHART_VIEWS`` (``src/server/utils/chart_view_context.py``), so a chart
snapshot the agent takes in that turn looks like what the user sees. They only
shape the turn's snapshots, so an invalid one is dropped rather than failing
the send.
"""

import logging
import re
from typing import Annotated, Any, Literal, Optional

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, ValidationError, field_validator

from src.tools.chart_annotation.schemas import Timeframe

logger = logging.getLogger(__name__)

# Exchange-local wall clock, no zone: the chart's own time convention.
_VIEW_TIME = r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?)?$"
_LOCALE = re.compile(r"[a-z]{2,3}(-[A-Za-z0-9]{2,8})?")


class ChartView(BaseModel):
    """One chart on screen: symbol, interval, theme, language, visible range and size."""

    symbol: str = Field(..., min_length=1, max_length=32)
    timeframe: Timeframe
    theme: Literal["dark", "light"]
    from_: str = Field(..., alias="from", pattern=_VIEW_TIME)
    to: str = Field(..., pattern=_VIEW_TIME)
    # Bounded so a crafted request cannot bloat every job's launch; chartkit
    # clamps to what it renders.
    width: int = Field(..., gt=0, le=20_000)
    height: int = Field(..., gt=0, le=20_000)
    # The reader's language, for a snapshot's labels and number formats. A
    # client that sends none, or one that is not a language tag, leaves it to
    # the snapshot's default rather than losing the view.
    locale: Optional[str] = None

    @field_validator("locale", mode="before")
    @classmethod
    def _language_tag(cls, v: Any) -> Optional[str]:
        return v if isinstance(v, str) and _LOCALE.fullmatch(v) else None

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_by_alias=True,
        validate_by_name=True,
        serialize_by_alias=True,
    )


#: The most views a request is read for. A screen shows a few charts; the
#: rest of a longer list is dropped before any is validated or logged, so a
#: crafted one costs nothing.
MAX_CHART_VIEWS = 32


def _valid_views(items: Any) -> Any:
    """Keep the views that validate, logging the rest; anything but a list
    (or none) is no views."""
    if items is None:
        return None
    if not isinstance(items, list):
        logger.warning("Dropped chart views that were not a list: %s", type(items).__name__)
        return []
    if len(items) > MAX_CHART_VIEWS:
        logger.warning("Dropped %d chart views past the first %d", len(items) - MAX_CHART_VIEWS, MAX_CHART_VIEWS)
        items = items[:MAX_CHART_VIEWS]
    kept: list[ChartView] = []
    for item in items:
        try:
            kept.append(ChartView.model_validate(item))
        except ValidationError as e:
            logger.warning(
                "Dropped an invalid chart view: %s",
                "; ".join(
                    f"{'.'.join(map(str, err['loc'])) or 'item'}: {err['msg']}"
                    for err in e.errors(include_input=False)
                ),
            )
    return kept


#: A request's chart views; an invalid one is dropped, never a 422.
ChartViews = Optional[Annotated[list[ChartView], BeforeValidator(_valid_views)]]
