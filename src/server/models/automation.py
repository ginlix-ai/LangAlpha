"""
Request and response models for Automations API.

Defines Pydantic models for creating, updating, listing, and viewing
automations and their execution history.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


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


class AutomationCreate(BaseModel):
    """Request model for creating an automation."""

    name: str = Field(..., max_length=255, description="Display name for the automation")
    description: Optional[str] = Field(None, description="Optional description")

    # Trigger
    trigger_type: Literal["cron", "once", "price"] = Field(
        ..., description="'cron' for recurring, 'once' for one-time, 'price' for price-triggered"
    )
    cron_expression: Optional[str] = Field(
        None, description="Cron expression (required for trigger_type='cron')"
    )
    timezone: str = Field(
        default="UTC", description="IANA timezone (e.g., 'America/New_York')"
    )
    trigger_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Trigger parameters. Required for 'price' type (PriceTriggerConfig schema).",
    )

    # Scheduling for one-time triggers
    next_run_at: Optional[datetime] = Field(
        None, description="Scheduled time for one-time triggers (UTC)"
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

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_if_needed(cls, v, info):
        # Actual cron validation done in handler (requires croniter import)
        return v


class AutomationUpdate(BaseModel):
    """Request model for partial update of an automation."""

    name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None

    # Trigger
    cron_expression: Optional[str] = None
    timezone: Optional[str] = None
    trigger_config: Optional[Dict[str, Any]] = None
    next_run_at: Optional[datetime] = None

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


# =============================================================================
# Response Models
# =============================================================================


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

    delivery_config: Optional[DeliveryConfig] = None
    metadata: Optional[Dict[str, Any]] = None

    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class AutomationsListResponse(BaseModel):
    """Response model for listing automations."""

    automations: List[AutomationResponse]
    total: int


class AutomationExecutionResponse(BaseModel):
    """Response model for a single automation execution."""

    automation_execution_id: UUID
    automation_id: UUID
    status: str
    conversation_thread_id: Optional[UUID] = None
    scheduled_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    server_id: Optional[str] = None
    delivery_result: Optional[List[Dict[str, Any]]] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class AutomationExecutionsListResponse(BaseModel):
    """Response model for listing automation executions."""

    executions: List[AutomationExecutionResponse]
    total: int
