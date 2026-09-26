"""
Request and response models for Automations API.

Defines Pydantic models for creating, updating, listing, and viewing
automations and their execution history.
"""

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from croniter import croniter
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)


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
        """Reject prefixed symbols and normalize display aliases."""
        if v.startswith("I:"):
            bare = v[2:]
            raise ValueError(f"Use bare symbol (e.g. '{bare}', not '{v}')")
        if v.startswith("^"):
            bare = v[1:]
            raise ValueError(f"Use bare symbol (e.g. '{bare}', not '{v}')")
        upper = v.upper()
        return _DISPLAY_ALIASES.get(upper, upper)

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


# =============================================================================
# Request Models
# =============================================================================


TriggerType = Literal["cron", "once", "price"]

# The schedule field each kind of trigger reads, and reads alone.
_SCHEDULE_FIELD: Dict[str, str] = {
    "cron": "cron_expression",
    "once": "next_run_at",
    "price": "trigger_config",
}


def error_sentences(e: ValidationError) -> str:
    """Each refusal as its field and the validator's own sentence, without the
    framing, input dump and link that a ValidationError's str() adds."""
    parts = []
    for err in e.errors(include_url=False):
        msg = err["msg"].removeprefix("Value error, ")
        field = ".".join(str(p) for p in err["loc"])
        parts.append(f"{field}: {msg}" if field else msg)
    return "; ".join(parts)


class _ScheduleFields(BaseModel):
    """The schedule fields, which a create and an update check the same way."""

    cron_expression: Optional[str] = Field(
        None, description="Cron expression (required for trigger_type='cron')"
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
        return v

    @field_validator("trigger_config")
    @classmethod
    def _price_monitor_can_read(
        cls, v: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        # The monitor skips a config it cannot parse without a word, so one
        # stored unchecked leaves an alert that reads as watching and never
        # fires. The config is stored as sent: the monitor parses it again.
        if v is not None:
            try:
                PriceTriggerConfig(**v)
            except ValidationError as e:
                raise ValueError(f"Invalid price trigger config: {error_sentences(e)}")
        return v

    @field_validator("next_run_at")
    @classmethod
    def _utc_when_naive(cls, v: Optional[datetime]) -> Optional[datetime]:
        return v.replace(tzinfo=UTC) if v is not None and v.tzinfo is None else v

    def _refuse_other_kinds(self, kind: str) -> None:
        # Refused rather than stored: a next_run_at on a price row would have
        # the scheduler claim and fire it.
        foreign = [
            field for other, field in _SCHEDULE_FIELD.items()
            if other != kind and getattr(self, field) is not None
        ]
        if foreign:
            raise ValueError(
                f"{', '.join(foreign)} doesn't apply to a '{kind}' automation"
            )


class AutomationCreate(_ScheduleFields):
    """Request model for creating an automation."""

    name: str = Field(..., max_length=255, description="Display name for the automation")
    description: Optional[str] = Field(None, description="Optional description")

    # Trigger
    trigger_type: TriggerType = Field(
        ..., description="'cron' for recurring, 'once' for one-time, 'price' for price-triggered"
    )
    timezone: str = Field(
        default="UTC", description="IANA timezone (e.g., 'America/New_York')"
    )

    # Agent config
    agent_mode: Literal["ptc", "flash"] = Field(
        default="flash", description="Agent mode for execution"
    )
    instruction: str = Field(
        ..., description="The prompt/instruction for the agent"
    )
    workspace_id: Optional[UUID] = Field(
        None, description="Workspace ID (required for 'ptc' mode)"
    )
    llm_model: Optional[str] = Field(
        None, description="LLM model name override"
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
        field = _SCHEDULE_FIELD[self.trigger_type]
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

    name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None

    trigger_type: Optional[TriggerType] = None
    timezone: Optional[str] = None

    # Agent config
    agent_mode: Optional[Literal["ptc", "flash"]] = None
    instruction: Optional[str] = None
    workspace_id: Optional[UUID] = None
    llm_model: Optional[str] = None
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
# A ``failed`` firing the user has to act on: a usage limit refused it or
# paused its run, or the provider rejected the user's own key.
FailureReason = Literal["usage_limit", "provider_auth"]


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
