"""A thread's wakeups as the web reads them: the Status panel and the tray chip.

``state`` is derived rather than stored. Storage knows whether a wakeup can
still fire and how far its notices got; the panel needs to know what the user
can do about it, which also turns on where an unread notice sits in the
outbox (``held`` for one waiting on the next turn, ``held_usage`` for one
waiting on usage).
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.server.database.wakeups.contract import Kind as WakeupKind
from src.server.database.wakeups.contract import Outcome as CancelOutcomeName

WakeupState = Literal[
    "armed", "held", "held_usage", "fired", "finished", "lapsed", "expired", "cancelled"
]


class WakeupPriceOut(BaseModel):
    symbol: str
    kind: Literal["above", "below", "pct_up", "pct_down"]
    value: float
    # The agent's vocabulary, not storage's: "now" is stored as "armed".
    pct_from: Optional[Literal["previous_close", "now"]] = None
    armed_price: Optional[float] = None
    fired_price: Optional[float] = None


class WakeupOrderOut(BaseModel):
    vendor_order_id: str
    summary: str
    # How the order stood when the wakeup fired; None while it still watches.
    outcome: Optional[
        Literal[
            "filled",
            "partial",
            "partial_cancelled",
            "cancelled",
            "rejected",
            "failed",
            "partial_expired",
            "expired",
            "confirmed",
            "handed_off",
        ]
    ] = None
    filled_qty: Optional[float] = None
    qty: Optional[float] = None
    avg_fill_price: Optional[float] = None
    # The currency the order is priced in, when it names one.
    currency: Optional[str] = None


class WakeupOut(BaseModel):
    wakeup_id: str
    short_id: Optional[str] = None
    label: Optional[str] = None
    kind: WakeupKind
    state: WakeupState
    timezone: Optional[str] = None
    cron: Optional[str] = None
    at: Optional[datetime] = None
    until: Optional[datetime] = None
    next_fire_at: Optional[datetime] = None
    fires_done: int = 0
    fires_total: int = 1
    ends_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    fired_at: Optional[datetime] = None
    price: Optional[WakeupPriceOut] = None
    order: Optional[WakeupOrderOut] = None
    cancellable: bool
    # A price target set before wakeups existed; it fires or lapses on its own.
    legacy: bool = False
    created_at: datetime


class WakeupsResponse(BaseModel):
    thread_id: str
    user_timezone: Optional[str] = None
    wakeups: List[WakeupOut] = Field(default_factory=list)


class WakeupCancelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    wakeup_ids: Optional[List[str]] = Field(default=None, min_length=1, max_length=50)
    all: Optional[bool] = None

    @model_validator(mode="after")
    def _exactly_one(self) -> "WakeupCancelRequest":
        if (self.wakeup_ids is not None) == (self.all is True):
            raise ValueError('give exactly one of "wakeup_ids" or "all": true')
        if self.all is not None and self.all is not True:
            raise ValueError('"all" can only be true')
        return self


class WakeupCancelResult(BaseModel):
    wakeup_id: str
    outcome: CancelOutcomeName


class WakeupCancelResponse(BaseModel):
    results: List[WakeupCancelResult]
    # None when the cancel committed but the listing after it could not be read.
    wakeups: Optional[List[WakeupOut]] = None
