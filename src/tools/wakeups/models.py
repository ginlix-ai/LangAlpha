"""The argument schema `schedule_wakeup` shows the model.

The model sees the strict schema (``extra="forbid"``, no per-argument
descriptions on the top-level arguments), but the tool validates the raw call
itself so a bad entry is refused in the drafted wording rather than by a
generic pydantic error. These models therefore exist to be rendered, and
``check_trigger`` in ``schedule`` is what decides.
"""

from __future__ import annotations

from typing import Literal, Optional, TypedDict, Union

from pydantic import BaseModel, ConfigDict, Field, create_model

from src.tools.wakeups.wording import (
    PRICE_PCT_FROM,
    TRIGGER_FIELDS,
    TRIGGER_MODEL_DOC,
)


class PriceTarget(BaseModel):
    """Set exactly one of above, below, pct_up, pct_down."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(description="Ticker, spelled as you would watch it.")
    above: Optional[float] = Field(
        default=None, description="Alert when the price rises to this level."
    )
    below: Optional[float] = Field(
        default=None, description="Alert when the price falls to this level."
    )
    pct_up: Optional[float] = Field(
        default=None, description="Alert on a rise of this many percent (3 means 3%)."
    )
    pct_down: Optional[float] = Field(
        default=None, description="Alert on a fall of this many percent (3 means 3%)."
    )
    pct_from: Optional[Literal["previous_close", "now"]] = Field(
        default=None, description=PRICE_PCT_FROM
    )


_FIELD_TYPES = {
    "label": Optional[str],
    "at": Optional[str],
    "cron": Optional[str],
    "until": Optional[str],
    "timezone": Optional[str],
    "price": Optional[PriceTarget],
    "order": Optional[str],
}
# A new wording key must come with a type, or the schema silently drops it.
assert list(TRIGGER_FIELDS) == list(_FIELD_TYPES), list(TRIGGER_FIELDS)

LABEL_MAX = 40

WakeupTrigger = create_model(
    "WakeupTrigger",
    __config__=ConfigDict(extra="forbid"),
    __doc__=TRIGGER_MODEL_DOC,
    **{
        key: (
            _FIELD_TYPES[key],
            Field(
                default=None,
                description=TRIGGER_FIELDS[key],
                **({"max_length": LABEL_MAX} if key == "label" else {}),
            ),
        )
        for key in TRIGGER_FIELDS
    },
)

TRIGGER_KEYS: tuple[str, ...] = tuple(WakeupTrigger.model_fields)


class ScheduleWakeupArgs(BaseModel):
    """Arguments of `schedule_wakeup`; every one optional, none described."""

    triggers: Optional[list[WakeupTrigger]] = None  # type: ignore[valid-type]
    cancel: Optional[Union[list[str], str]] = None
    quiet: Optional[str] = None


class WakeupArtifact(TypedDict):
    """What a `schedule_wakeup` result carries beside its text: whether it
    ended the turn quietly, and whether any call this turn set or cancelled
    a wakeup, which tells a quiet check that is news apart from one that is
    not."""

    quiet: bool
    changed: bool
