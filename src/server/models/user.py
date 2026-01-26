"""
Pydantic models for User, UserPreferences, Watchlist, and Portfolio.

These models support the user onboarding flow and investment preferences.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# =============================================================================
# Symbol Normalization
# =============================================================================


def normalize_symbol(symbol: str) -> str:
    """
    Normalize instrument symbol to standard format.

    Rules:
    - US/International stocks, ETFs, indexes: UPPERCASE (AAPL, GOOGL, BARC.L)
    - Crypto pairs: UPPERCASE (BTC, ETH, BTC-USD)
    - Forex: UPPERCASE (EURUSD)

    Args:
        symbol: Raw symbol input

    Returns:
        Normalized symbol (uppercase, stripped)
    """
    return symbol.strip().upper()


# =============================================================================
# Enums
# =============================================================================


class RiskTolerance(str, Enum):
    """Risk tolerance levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    LONG_TERM_FOCUS = "long_term_focus"


class CompanyInterest(str, Enum):
    """Company interest types."""

    GROWTH = "growth"
    STABLE = "stable"
    VALUE = "value"
    ESG = "esg"


class HoldingPeriod(str, Enum):
    """Investment holding period preferences."""

    SHORT_TERM = "short_term"
    MID_TERM = "mid_term"
    LONG_TERM = "long_term"
    FLEXIBLE = "flexible"


class AnalysisFocus(str, Enum):
    """Analysis focus areas."""

    GROWTH = "growth"
    VALUATION = "valuation"
    MOAT = "moat"
    RISK = "risk"


class OutputStyle(str, Enum):
    """Agent output style preferences."""

    SUMMARY = "summary"
    DATA = "data"
    DEEP_DIVE = "deep_dive"
    QUICK = "quick"


class InstrumentType(str, Enum):
    """Supported instrument types for watchlist and portfolio."""

    STOCK = "stock"
    ETF = "etf"
    INDEX = "index"
    CRYPTO = "crypto"
    FUTURE = "future"
    COMMODITY = "commodity"
    CURRENCY = "currency"


# =============================================================================
# JSONB Schema Models (for validation)
# =============================================================================


class RiskPreference(BaseModel):
    """Risk preference settings stored in JSONB."""

    risk_tolerance: Optional[RiskTolerance] = Field(
        None, description="Risk tolerance level"
    )

    class Config:
        extra = "allow"  # Allow additional fields (notes, etc.)


class InvestmentPreference(BaseModel):
    """Investment preference settings stored in JSONB."""

    company_interest: Optional[CompanyInterest] = Field(
        None, description="Type of companies interested in"
    )
    holding_period: Optional[HoldingPeriod] = Field(
        None, description="Preferred holding period"
    )
    analysis_focus: Optional[AnalysisFocus] = Field(
        None, description="Primary analysis focus area"
    )

    class Config:
        extra = "allow"  # Allow additional fields (avoid_sectors, focus_sectors, notes, etc.)


class AgentPreference(BaseModel):
    """AI agent behavior preferences stored in JSONB."""

    output_style: Optional[OutputStyle] = Field(
        None, description="Preferred output style"
    )

    class Config:
        extra = "allow"  # Allow additional fields (notes, instruction, etc.)


class OtherPreference(BaseModel):
    """Miscellaneous preferences stored in JSONB."""

    # Future fields: theme, email_notifications, marketing_opt_in, feature_flags
    pass

    class Config:
        extra = "allow"  # Allow additional fields for flexibility


class AlertSettings(BaseModel):
    """Alert settings for watchlist items."""

    price_above: Optional[float] = Field(None, description="Alert when price goes above")
    price_below: Optional[float] = Field(None, description="Alert when price goes below")
    percent_change: Optional[float] = Field(
        None, description="Alert on percent change threshold"
    )
    news_alerts: Optional[bool] = Field(None, description="Enable news alerts")

    class Config:
        extra = "allow"


# =============================================================================
# User Models
# =============================================================================


class UserBase(BaseModel):
    """Base user fields."""

    email: Optional[str] = Field(None, max_length=255, description="User email")
    name: Optional[str] = Field(None, max_length=255, description="User display name")
    avatar_url: Optional[str] = Field(None, description="URL to user avatar")
    timezone: Optional[str] = Field(
        None, max_length=100, description="User timezone (e.g., 'America/New_York')"
    )
    locale: Optional[str] = Field(
        None, max_length=20, description="User locale (e.g., 'en-US')"
    )


class UserCreate(UserBase):
    """Request model for creating a new user."""

    user_id: str = Field(
        ..., max_length=255, description="External auth ID (e.g., Clerk, Auth0)"
    )


class UserUpdate(UserBase):
    """Request model for updating user profile."""

    onboarding_completed: Optional[bool] = Field(
        None, description="Whether onboarding is completed"
    )


class UserResponse(UserBase):
    """Response model for user details."""

    user_id: str = Field(description="User ID")
    onboarding_completed: bool = Field(
        default=False, description="Whether onboarding is completed"
    )
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")
    last_login_at: Optional[datetime] = Field(None, description="Last login timestamp")

    class Config:
        from_attributes = True


# =============================================================================
# User Preferences Models
# =============================================================================


class UserPreferencesBase(BaseModel):
    """Base preferences fields."""

    risk_preference: Optional[RiskPreference] = Field(
        default_factory=RiskPreference, description="Risk tolerance settings"
    )
    investment_preference: Optional[InvestmentPreference] = Field(
        default_factory=InvestmentPreference, description="Investment style settings"
    )
    agent_preference: Optional[AgentPreference] = Field(
        default_factory=AgentPreference, description="AI agent behavior settings"
    )
    other_preference: Optional[OtherPreference] = Field(
        default_factory=OtherPreference, description="Miscellaneous preferences"
    )


class UserPreferencesCreate(UserPreferencesBase):
    """Request model for creating user preferences."""

    pass


class UserPreferencesUpdate(UserPreferencesBase):
    """Request model for updating user preferences."""

    pass


class UserPreferencesResponse(UserPreferencesBase):
    """Response model for user preferences."""

    preference_id: UUID = Field(description="Preference record ID")
    user_id: str = Field(description="User ID")
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")

    class Config:
        from_attributes = True


# =============================================================================
# Watchlist Models
# =============================================================================


# -----------------------------------------------------------------------------
# Watchlist (list metadata) Models
# -----------------------------------------------------------------------------


class WatchlistBase(BaseModel):
    """Base watchlist fields."""

    name: str = Field(..., max_length=100, description="Watchlist name")
    description: Optional[str] = Field(None, description="Watchlist description")
    is_default: bool = Field(default=False, description="Whether this is the default watchlist")
    display_order: int = Field(default=0, description="Display order for sorting")


class WatchlistCreate(WatchlistBase):
    """Request model for creating a watchlist."""

    pass


class WatchlistUpdate(BaseModel):
    """Request model for updating watchlist metadata."""

    name: Optional[str] = Field(None, max_length=100, description="Watchlist name")
    description: Optional[str] = Field(None, description="Watchlist description")
    display_order: Optional[int] = Field(None, description="Display order for sorting")


class WatchlistResponse(WatchlistBase):
    """Response model for watchlist metadata."""

    watchlist_id: UUID = Field(description="Watchlist ID")
    user_id: str = Field(description="User ID")
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")

    class Config:
        from_attributes = True


# -----------------------------------------------------------------------------
# Watchlist Item Models
# -----------------------------------------------------------------------------


class WatchlistItemBase(BaseModel):
    """Base watchlist item fields."""

    symbol: str = Field(..., max_length=50, description="Instrument symbol")
    instrument_type: InstrumentType = Field(..., description="Type of instrument")
    exchange: Optional[str] = Field(
        None, max_length=50, description="Exchange (e.g., 'NASDAQ')"
    )
    name: Optional[str] = Field(None, max_length=255, description="Full instrument name")
    notes: Optional[str] = Field(None, description="User notes")
    alert_settings: Optional[AlertSettings] = Field(
        default_factory=AlertSettings, description="Price alert settings"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Additional metadata"
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol_field(cls, v: Any) -> str:
        """Normalize symbol to uppercase."""
        if isinstance(v, str):
            return normalize_symbol(v)
        return v  # Let Pydantic handle type validation


class WatchlistItemCreate(WatchlistItemBase):
    """Request model for adding item to watchlist."""

    pass


class WatchlistItemUpdate(BaseModel):
    """Request model for updating watchlist item."""

    name: Optional[str] = Field(None, max_length=255, description="Full instrument name")
    notes: Optional[str] = Field(None, description="User notes")
    alert_settings: Optional[AlertSettings] = Field(
        None, description="Price alert settings"
    )
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class WatchlistItemResponse(WatchlistItemBase):
    """Response model for watchlist item."""

    item_id: UUID = Field(description="Watchlist item ID")
    watchlist_id: UUID = Field(description="Parent watchlist ID")
    user_id: str = Field(description="User ID")
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")

    class Config:
        from_attributes = True


# -----------------------------------------------------------------------------
# Aggregated Response Models
# -----------------------------------------------------------------------------


class WatchlistItemsResponse(BaseModel):
    """Response model for watchlist items list (backward compatibility)."""

    items: List[WatchlistItemResponse] = Field(
        default_factory=list, description="Watchlist items"
    )
    total: int = Field(0, description="Total number of items")


class WatchlistWithItemsResponse(WatchlistResponse):
    """Response model for watchlist with its items."""

    items: List[WatchlistItemResponse] = Field(
        default_factory=list, description="Watchlist items"
    )
    total: int = Field(0, description="Total number of items")


class WatchlistsResponse(BaseModel):
    """Response model for list of watchlists."""

    watchlists: List[WatchlistResponse] = Field(
        default_factory=list, description="User's watchlists"
    )
    total: int = Field(0, description="Total number of watchlists")


# =============================================================================
# Portfolio Models
# =============================================================================


class PortfolioHoldingBase(BaseModel):
    """Base portfolio holding fields."""

    symbol: str = Field(..., max_length=50, description="Instrument symbol")
    instrument_type: InstrumentType = Field(..., description="Type of instrument")
    exchange: Optional[str] = Field(
        None, max_length=50, description="Exchange (e.g., 'NASDAQ')"
    )
    name: Optional[str] = Field(None, max_length=255, description="Full instrument name")
    quantity: Decimal = Field(..., description="Number of units held")
    average_cost: Optional[Decimal] = Field(None, description="Average cost per unit")
    currency: str = Field(default="USD", max_length=10, description="Currency")
    account_name: Optional[str] = Field(
        None, max_length=100, description="Account name (e.g., 'Robinhood')"
    )
    notes: Optional[str] = Field(None, description="User notes")
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Additional metadata"
    )
    first_purchased_at: Optional[datetime] = Field(
        None, description="First purchase date"
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol_field(cls, v: Any) -> str:
        """Normalize symbol to uppercase."""
        if isinstance(v, str):
            return normalize_symbol(v)
        return v  # Let Pydantic handle type validation


class PortfolioHoldingCreate(PortfolioHoldingBase):
    """Request model for adding holding to portfolio."""

    pass


class PortfolioHoldingUpdate(BaseModel):
    """Request model for updating portfolio holding."""

    name: Optional[str] = Field(None, max_length=255, description="Full instrument name")
    quantity: Optional[Decimal] = Field(None, description="Number of units held")
    average_cost: Optional[Decimal] = Field(None, description="Average cost per unit")
    currency: Optional[str] = Field(None, max_length=10, description="Currency")
    notes: Optional[str] = Field(None, description="User notes")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    first_purchased_at: Optional[datetime] = Field(
        None, description="First purchase date"
    )


class PortfolioHoldingResponse(PortfolioHoldingBase):
    """Response model for portfolio holding."""

    holding_id: UUID = Field(description="Holding entry ID")
    user_id: str = Field(description="User ID")
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")

    class Config:
        from_attributes = True


class PortfolioResponse(BaseModel):
    """Response model for user's full portfolio."""

    holdings: List[PortfolioHoldingResponse] = Field(
        default_factory=list, description="Portfolio holdings"
    )
    total: int = Field(0, description="Total number of holdings")


# =============================================================================
# Combined Response Models (for /me endpoint)
# =============================================================================


class UserWithPreferencesResponse(BaseModel):
    """Combined response for GET /users/me endpoint."""

    user: UserResponse = Field(description="User profile")
    preferences: Optional[UserPreferencesResponse] = Field(
        None, description="User preferences (may be null if not set)"
    )
