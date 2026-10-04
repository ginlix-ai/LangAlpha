"""
Request and response models for Automations API.

Defines Pydantic models for creating, updating, listing, and viewing
automations and their execution history.
"""

import re
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from croniter import croniter
from market_protocol import display_spelling
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.server.utils.error_sanitization import validation_error_text
from src.utils.timezone_utils import zone_or_none


# =============================================================================
# Price Trigger Models
# =============================================================================


class MarketType(str, Enum):
    """Market type for price-triggered automations."""
    STOCK = "stock"
    INDEX = "index"


class PriceConditionType(str, Enum):
    """Supported price condition types (extensible)."""
    PRICE_ABOVE = "price_above"
    PRICE_BELOW = "price_below"
    PCT_CHANGE_ABOVE = "pct_change_above"
    PCT_CHANGE_BELOW = "pct_change_below"


class PriceCondition(BaseModel):
    """A single price condition to evaluate."""
    type: PriceConditionType
    value: float = Field(..., gt=0, description="Threshold: dollar amount or percentage")
    reference: Literal["previous_close", "day_open"] = Field(
        default="previous_close",
        description="Reference price for percentage conditions",
    )


class RetriggerMode(str, Enum):
    """How the automation re-arms after triggering."""
    ONE_SHOT = "one_shot"
    RECURRING = "recurring"


class RetriggerConfig(BaseModel):
    """Retrigger behavior configuration."""
    mode: RetriggerMode = RetriggerMode.ONE_SHOT
    cooldown_seconds: Optional[int] = Field(
        default=None,
        description="Cooldown period in seconds for 'recurring' mode. "
        "None = default to next trading day. Minimum 4 hours (14400s) when set.",
    )

    @field_validator("mode", mode="before")
    @classmethod
    def normalize_cooldown_mode(cls, v):
        """Backward compat: 'cooldown' -> 'recurring'."""
        if v == "cooldown":
            return "recurring"
        return v

    @model_validator(mode="after")
    def validate_cooldown(self):
        if self.mode == RetriggerMode.ONE_SHOT:
            self.cooldown_seconds = None
        elif self.mode == RetriggerMode.RECURRING and self.cooldown_seconds is not None:
            if self.cooldown_seconds < 14400:
                raise ValueError("cooldown_seconds must be >= 14400 (4 hours) when set")
        return self


# Display-style aliases → canonical bare symbols
_DISPLAY_ALIASES: dict[str, str] = {"GSPC": "SPX", "IXIC": "COMP"}

# Canonical bare symbols that are indices (for auto-detection)
_INDEX_SYMBOLS: set[str] = {"SPX", "DJI", "COMP", "NDX", "RUT", "VIX"}


class PriceTriggerConfig(BaseModel):
    """Configuration stored in trigger_config for price-triggered automations."""
    symbol: str = Field(..., min_length=1, max_length=10, description="Ticker symbol (bare, e.g. 'AAPL' or 'SPX')")
    market: MarketType = Field(default=MarketType.STOCK, description="Market type: 'stock' or 'index'")
    conditions: List[PriceCondition] = Field(
        ..., min_length=1,
        description="Price conditions to evaluate (AND logic for multiple)",
    )
    retrigger: RetriggerConfig = Field(default_factory=RetriggerConfig)

    @field_validator("symbol")
    @classmethod
    def validate_bare_symbol(cls, v: str) -> str:
        """Reject prefixed symbols and normalize display aliases.

        Spelled first, so a padded or full-width entry (" ^GSPC", "＾GSPC",
        "GSPC ") meets the same checks and aliases as its plain form.
        """
        spelled = display_spelling(v)
        for prefix in ("I:", "^"):
            if spelled.startswith(prefix):
                bare = spelled[len(prefix):]
                raise ValueError(f"Use bare symbol (e.g. '{bare}', not '{spelled}')")
        return _DISPLAY_ALIASES.get(spelled, spelled)

    @model_validator(mode="after")
    def infer_market_from_symbol(self):
        """Auto-set market=INDEX when symbol is a known index."""
        if self.symbol in _INDEX_SYMBOLS:
            self.market = MarketType.INDEX
        return self


# =============================================================================
# Delivery Config
# =============================================================================


class DeliveryConfig(BaseModel):
    """Delivery configuration — which methods to use for result delivery."""
    methods: List[str] = Field(
        default_factory=list,
        description="Delivery methods to enable: 'slack', etc."
    )


class DeliveryDefaultUpdate(BaseModel):
    """A workspace's default chat on one app, where an entry naming only the
    app delivers. A null address clears it."""

    workspace_id: UUID
    platform: str = Field(..., min_length=1, max_length=32)
    address: Optional[str] = Field(None, max_length=500)


def parse_delivery(value: Any) -> Dict[str, List[str]]:
    """Delivery as the agent writes it, a list or a comma-separated string, as
    the stored ``delivery_config``."""
    if isinstance(value, str):
        return {"methods": [m.strip() for m in value.split(",") if m.strip()]}
    # Blanks drop as in the string form: the old tool stored "slack," as ["slack", ""].
    if isinstance(value, list) and all(isinstance(m, str) for m in value):
        return {"methods": [m.strip() for m in value if m.strip()]}
    raise ValueError('must be a list of delivery methods, e.g. ["slack"], or [] for none')


# =============================================================================
# Thread, clock and run time
# =============================================================================


def thread_fields(value: Any, current_thread_id: Optional[str]) -> Dict[str, Any]:
    """Where runs post, as the agent names it, as the row's two thread fields.

    "persistent" is the automation's own thread, which its first run creates,
    so it clears any pinned conversation; "current" pins the conversation the
    agent is writing from.
    """
    if value == "new":
        return {"thread_strategy": "new", "conversation_thread_id": None}
    if value == "persistent":
        return {"thread_strategy": "continue", "conversation_thread_id": None}
    if value == "current":
        if not current_thread_id:
            raise ValueError('"current" needs a conversation thread, and this run has none')
        return {"thread_strategy": "continue", "conversation_thread_id": str(current_thread_id)}
    if isinstance(value, str):
        try:
            return {"thread_strategy": "continue", "conversation_thread_id": str(UUID(value))}
        except ValueError:
            pass
    raise ValueError('must be "new", "persistent", "current" or a thread id')


def in_own_thread(row: Dict[str, Any]) -> bool:
    """Whether the row's runs continue in the automation's own thread, the
    one "persistent" names. Its first run pins the thread it creates, so a
    pin alone does not tell it from a pinned conversation; ``owns_thread``
    does."""
    if row.get("thread_strategy") != "continue":
        return False
    return not row.get("conversation_thread_id") or bool(row.get("owns_thread"))


# Zones that read as a region's clock but hold one offset all year, so a
# 9:00 "EST" automation runs at 10:00 New York time all summer.
_FIXED_OFFSET_ZONES = {
    "EST": "America/New_York",
    "MST": "America/Denver (or America/Phoenix, which keeps MST all year)",
}


def region_zone(name: str) -> str:
    """Refuse a fixed-offset zone the agent writes, which it picks for a
    region by mistake. A zone the user chose, on the page or in their
    profile, is theirs to keep."""
    if name in _FIXED_OFFSET_ZONES:
        raise ValueError(
            f"{name!r} is a fixed offset that ignores daylight saving; use a region zone "
            f"such as {_FIXED_OFFSET_ZONES[name]}"
        )
    return name


def on_clock(value: datetime, tz_name: Optional[str]) -> str:
    """A time on an automation's clock, to the second, with its offset."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(zone_or_none(tz_name or "UTC") or UTC).isoformat(timespec="seconds")


_DATE_ONLY = re.compile(r"\d{4}-?\d{2}-?\d{2}")


def run_time(text: str) -> datetime:
    """A one-time run's ISO time as the agent writes it, naive when it names
    no offset: the caller reads that on the clock it means. A bare date is
    refused, since it would run at midnight."""
    if _DATE_ONLY.fullmatch(text.strip()):
        raise ValueError(
            f"{text!r} has no time of day, which would run at midnight; write one, "
            f"e.g. {text.strip()}T09:00:00"
        )
    try:
        return datetime.fromisoformat(text.strip())
    except ValueError:
        raise ValueError(f"not an ISO datetime: {text!r}") from None


def future_run(when: datetime, tz_name: Optional[str]) -> datetime:
    """Refuse a one-time run the agent sets in the past, which the scheduler
    would fire the moment it is saved. ``tz_name`` is the clock the refusal
    shows both times on."""
    now = datetime.now(UTC)
    if (when if when.tzinfo else when.replace(tzinfo=UTC)) >= now:
        return when
    where = "on this automation's clock" if tz_name else "in UTC"
    raise ValueError(
        f"{on_clock(when, tz_name)} has already passed (it is {on_clock(now, tz_name)} {where}); "
        "set a future time"
    )


# =============================================================================
# Request Models
# =============================================================================


TriggerType = Literal["cron", "once", "price"]

# The schedule field each kind of trigger reads, and reads alone.
SCHEDULE_FIELD: Dict[str, str] = {
    "cron": "cron_expression",
    "once": "next_run_at",
    "price": "trigger_config",
}

# A crypto, currency or futures symbol is refused: the monitor has no feed
# for any of them, so it would be saved as a stock that never quotes. Bare
# bases stay allowed: BTC and ETH are also US-listed fund tickers.
_PAIR_QUOTES = ("USD", "USDT", "USDC", "EUR", "GBP", "JPY")
_CRYPTO_BASES = ("BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LTC", "BNB", "AVAX", "DOT", "LINK", "SHIB")


def _is_pair_symbol(symbol: str) -> bool:
    s = symbol.upper()
    if s.startswith(("X:", "C:")) or s.endswith(("=X", "=F")):
        return True
    if s.partition("-")[2] in _PAIR_QUOTES:
        return True
    return any(s == base + quote for base in _CRYPTO_BASES for quote in _PAIR_QUOTES)


# Each field's name and cycle: a step up to the cycle is a steady interval
# (*/60 minutes is hourly); a longer one matches once per cycle.
_CRON_FIELDS = (("minute", 60), ("hour", 24), ("day-of-month", 31), ("month", 12), ("day-of-week", 7))


def _refuse_misread_cron(expression: str) -> None:
    """Refuse cron croniter accepts but reads differently than it looks.

    A sixth field is seconds to croniter and the first field to Quartz, so
    ``0 0 9 * * 5`` meant as Fridays at 9:00 runs on the 9th at 00:00:05. A
    step longer than its field's cycle (``*/90`` minutes) matches once per
    cycle, not every 90 minutes.
    """
    if expression.startswith("@"):
        return
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(
            f"'{expression}' has {len(fields)} fields; use five: minute hour day-of-month month "
            "day-of-week (seconds aren't supported)"
        )
    for value, (label, cycle) in zip(fields, _CRON_FIELDS):
        for part in value.split(","):
            step = part.partition("/")[2]
            if step.isdigit() and int(step) > cycle:
                raise ValueError(
                    f"'{expression}': a /{step} step is longer than the {label} field's cycle of "
                    f"{cycle}, so it matches once per cycle. Cron steps only within one field; "
                    "list the times instead"
                )


class _ScheduleFields(BaseModel):
    """The schedule fields, which a create and an update check the same way."""

    timezone: Optional[str] = Field(
        default=None, max_length=100, description="IANA timezone (e.g., 'America/New_York')"
    )
    # Widths match the VARCHAR columns, so an overlong value is a schema error
    # naming the field rather than a database error on save.
    cron_expression: Optional[str] = Field(
        None, max_length=100, description="Cron expression (required for trigger_type='cron')"
    )
    trigger_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Trigger parameters. Required for 'price' type (PriceTriggerConfig schema).",
    )
    next_run_at: Optional[datetime] = Field(
        None,
        description="Scheduled time for one-time triggers; one without an offset is UTC",
    )

    @field_validator("cron_expression")
    @classmethod
    def _cron_parses(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: '{v}'")
        if v is not None:
            _refuse_misread_cron(v)
        return v

    @field_validator("trigger_config")
    @classmethod
    def _price_monitor_can_read(
        cls, v: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        # The monitor skips a config it cannot parse without a word, so one
        # stored unchecked leaves an alert that reads as watching and never
        # fires. The config is stored as sent, the monitor parses it again;
        # only the symbol is stored as the monitor reads it (600519.SH, SPX).
        if v is not None:
            try:
                config = PriceTriggerConfig(**v)
            except ValidationError as e:
                raise ValueError(f"Invalid price trigger config: {validation_error_text(e)}")
            if _is_pair_symbol(config.symbol):
                raise ValueError(
                    f"Invalid price trigger config: '{config.symbol}' is a crypto, currency or futures "
                    "symbol; price alerts cannot watch those"
                )
            v = {**v, "symbol": config.symbol}
        return v

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and zone_or_none(v) is None:
            raise ValueError(f"unknown IANA timezone {v!r}")
        return v

    @field_validator("next_run_at")
    @classmethod
    def _utc_when_naive(cls, v: Optional[datetime]) -> Optional[datetime]:
        return v.replace(tzinfo=UTC) if v is not None and v.tzinfo is None else v

    def _refuse_other_kinds(self, kind: str) -> None:
        # Refused rather than stored: a next_run_at on a price row would have
        # the scheduler claim and fire it.
        foreign = [
            field for other, field in SCHEDULE_FIELD.items()
            if other != kind and getattr(self, field) is not None
        ]
        if foreign:
            raise ValueError(
                f"{', '.join(foreign)} doesn't apply to a '{kind}' automation"
            )


class AutomationCreate(_ScheduleFields):
    """Request model for creating an automation."""

    name: str = Field(..., min_length=1, max_length=255, description="Display name for the automation")
    description: Optional[str] = Field(None, description="Optional description")

    # Trigger
    trigger_type: TriggerType = Field(
        ..., description="'cron' for recurring, 'once' for one-time, 'price' for price-triggered"
    )
    timezone: str = Field(
        default="UTC", max_length=100, description="IANA timezone (e.g., 'America/New_York')"
    )

    # Agent config
    agent_mode: Literal["ptc", "flash"] = Field(
        default="flash", description="Agent mode for execution"
    )
    instruction: str = Field(
        ..., min_length=1, description="The prompt/instruction for the agent"
    )
    workspace_id: Optional[UUID] = Field(
        None, description="Workspace ID (required for 'ptc' mode)"
    )
    llm_model: Optional[str] = Field(
        None, max_length=100, description="LLM model name override"
    )
    additional_context: Optional[List[Dict[str, Any]]] = Field(
        None, description="Additional context items (skills, images, etc.)"
    )

    # Thread strategy
    thread_strategy: Literal["new", "continue"] = Field(
        default="new",
        description="'new' creates a fresh thread each run, 'continue' reuses a pinned thread",
    )
    conversation_thread_id: Optional[UUID] = Field(
        None, description="Pinned thread ID for 'continue' strategy"
    )

    # Lifecycle
    max_failures: int = Field(
        default=3, ge=1, le=100,
        description="Auto-disable after this many consecutive failures",
    )

    # Future extensibility
    delivery_config: Optional[DeliveryConfig] = Field(
        default=None,
        description="Delivery configuration: { methods: ['slack', ...] }",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="Arbitrary metadata"
    )

    @model_validator(mode="after")
    def _schedule_fits_the_kind(self) -> "AutomationCreate":
        field = SCHEDULE_FIELD[self.trigger_type]
        if getattr(self, field) is None:
            raise ValueError(f"{field} is required for trigger_type='{self.trigger_type}'")
        self._refuse_other_kinds(self.trigger_type)
        return self


class AutomationUpdate(_ScheduleFields):
    """Request model for partial update of an automation.

    The kind of trigger is fixed at create, so the schedule fields are checked
    against the stored kind, which the handler passes as the validation
    context's ``trigger_type`` once it has read the row. ``trigger_type`` here
    is accepted only to be refused when it differs from the stored one.
    """

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None

    trigger_type: Optional[TriggerType] = None

    # Agent config
    agent_mode: Optional[Literal["ptc", "flash"]] = None
    instruction: Optional[str] = Field(None, min_length=1)
    workspace_id: Optional[UUID] = None
    llm_model: Optional[str] = Field(None, max_length=100)
    additional_context: Optional[List[Dict[str, Any]]] = None

    # Thread strategy
    thread_strategy: Optional[Literal["new", "continue"]] = None
    conversation_thread_id: Optional[UUID] = None

    # Lifecycle
    max_failures: Optional[int] = Field(None, ge=1, le=100)

    # Future
    delivery_config: Optional[DeliveryConfig] = Field(
        default=None,
        description="Delivery configuration: { methods: ['slack', ...] }",
    )
    metadata: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _keeps_the_stored_kind(self, info: ValidationInfo) -> "AutomationUpdate":
        kind = (info.context or {}).get("trigger_type")
        if kind is None:
            return self
        if self.trigger_type not in (None, kind):
            raise ValueError(
                f"trigger_type can't change from '{kind}' to "
                f"'{self.trigger_type}'; create a new automation instead"
            )
        self._refuse_other_kinds(kind)
        return self


# =============================================================================
# Response Models
# =============================================================================


ExecutionStatus = Literal[
    "pending", "waiting", "running", "completed", "failed", "timeout", "skipped"
]
# Why a ``skipped`` firing did not run to its end: someone skipped it or
# stopped its run, its thread stayed busy (or an earlier firing was already
# waiting), or the server stopped while it waited.
SkipReason = Literal["user", "thread_busy", "interrupted"]
# Why a ``failed`` firing failed, when it was not the automation's own doing:
# a usage limit refused it or paused its run, the provider rejected the
# user's own key, the service failed it (``server_error``), or the server cut
# its run off (``interrupted``).
FailureReason = Literal["usage_limit", "provider_auth", "server_error", "interrupted"]


class AutomationExecutionResponse(BaseModel):
    """One execution: a history row, and an automation's newest run."""

    automation_execution_id: UUID
    automation_id: UUID
    # Open strings on the way out, whose known values are the Literals above:
    # a build rolled back under rows a newer one wrote must still answer, and
    # this row rides on every automation in the list.
    status: str
    conversation_thread_id: Optional[UUID] = None
    scheduled_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    skip_reason: Optional[str] = None
    failure_reason: Optional[str] = None
    server_id: Optional[str] = None
    delivery_result: Optional[List[Dict[str, Any]]] = None
    created_at: datetime
    # When the user took this failed run out of Needs attention.
    dismissed_at: Optional[datetime] = None
    excerpt: Optional[str] = None

    model_config = {"from_attributes": True}


class AutomationResponse(BaseModel):
    """Response model for a single automation."""

    automation_id: UUID
    user_id: str
    name: str
    description: Optional[str] = None

    trigger_type: str
    cron_expression: Optional[str] = None
    timezone: str
    trigger_config: Optional[Dict[str, Any]] = None

    next_run_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None

    agent_mode: str
    instruction: str
    workspace_id: Optional[UUID] = None
    llm_model: Optional[str] = None
    additional_context: Optional[List[Dict[str, Any]]] = None

    thread_strategy: str
    conversation_thread_id: Optional[UUID] = None

    status: str
    max_failures: int
    failure_count: int
    # Why the server switched a disabled automation off: 'provider_auth' (the
    # provider rejected the user's own key) or 'max_failures'.
    disable_reason: Optional[str] = None

    delivery_config: Optional[DeliveryConfig] = None
    metadata: Optional[Dict[str, Any]] = None

    created_at: datetime
    updated_at: datetime

    last_execution: Optional[AutomationExecutionResponse] = None

    model_config = {"from_attributes": True}


class AutomationsListResponse(BaseModel):
    """Response model for listing automations."""

    automations: List[AutomationResponse]
    total: int


class AutomationRunResponse(AutomationExecutionResponse):
    """An execution with its automation's identity."""

    automation_name: str
    agent_mode: str
    trigger_type: str
    workspace_id: Optional[UUID] = None


class AutomationRunsListResponse(BaseModel):
    """A page of runs: the user-wide feed, or one automation's history."""

    executions: List[AutomationRunResponse]
    has_more: bool
