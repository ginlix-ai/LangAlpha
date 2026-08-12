"""
Utility functions for market data tools.

Provides helper functions for formatting, market session detection, and FMP client access.
"""

from typing import Optional, Tuple
from datetime import datetime, time
import math
import pytz
import logging

logger = logging.getLogger(__name__)


def get_market_session() -> Tuple[str, datetime]:
    """
    Determine current US market session based on Eastern Time.

    Returns:
        Tuple of (session_name, current_et_time)
        session_name: "PRE_MARKET", "REGULAR_HOURS", "AFTER_HOURS", or "CLOSED"
    """
    # Get current time in US Eastern Time
    et_tz = pytz.timezone("US/Eastern")
    now_et = datetime.now(et_tz)

    # Check if it's a weekday (Monday=0, Sunday=6)
    if now_et.weekday() >= 5:  # Saturday or Sunday
        return "CLOSED", now_et

    # Get current time
    current_time = now_et.time()

    # Market hours in ET
    pre_market_open = time(4, 0)   # 4:00 AM
    market_open = time(9, 30)      # 9:30 AM
    market_close = time(16, 0)     # 4:00 PM
    after_hours_close = time(20, 0)  # 8:00 PM

    if market_open <= current_time < market_close:
        return "REGULAR_HOURS", now_et
    elif market_close <= current_time < after_hours_close:
        return "AFTER_HOURS", now_et
    elif pre_market_open <= current_time < market_open:
        return "PRE_MARKET", now_et
    else:
        return "CLOSED", now_et


def format_number(value: Optional[float], suffix: bool = True) -> str:
    """
    Format large numbers with B/M/T suffixes or as currency.

    Args:
        value: Number to format
        suffix: Whether to add B/M/T suffix for large numbers

    Returns:
        Formatted string (e.g., "$3.68T", "$247.92")
    """
    if value is None:
        return "N/A"

    if suffix and abs(value) >= 1e12:
        return f"${value / 1e12:.2f}T"
    elif suffix and abs(value) >= 1e9:
        return f"${value / 1e9:.2f}B"
    elif suffix and abs(value) >= 1e6:
        return f"${value / 1e6:.2f}M"
    elif suffix:
        return f"${value:,.2f}"
    else:
        return f"{value:,.2f}"


def finite_or_none(value) -> Optional[float]:
    """Return value if it's a finite number, else None (NaN/Inf/non-numeric)."""
    return value if isinstance(value, (int, float)) and math.isfinite(value) else None


def format_percentage(value: Optional[float]) -> str:
    """
    Format decimal as percentage with sign.

    Args:
        value: Decimal value (e.g., 0.0523 for 5.23%)

    Returns:
        Formatted percentage string (e.g., "+5.23%", "-2.15%")
    """
    if value is None:
        return "N/A"
    if isinstance(value, (int, float)):
        # Non-finite (NaN/Inf) renders as "+nan%" otherwise — treat as missing.
        return f"{value:+.2f}%" if math.isfinite(value) else "N/A"
    return str(value)


def get_rating_label(score: int) -> str:
    """
    Convert numeric score to letter grade.

    Args:
        score: Numeric score (typically 0-5)

    Returns:
        Letter grade (A+, A, A-, B+, B, B-, C, D)
    """
    if score >= 4.5:
        return "A+"
    elif score >= 4:
        return "A"
    elif score >= 3.5:
        return "A-"
    elif score >= 3:
        return "B+"
    elif score >= 2.5:
        return "B"
    elif score >= 2:
        return "B-"
    elif score >= 1.5:
        return "C"
    else:
        return "D"
