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

# US session boundaries in Eastern Time. The runtime-context market clock reads
# these too, so the tool output and the agent's runtime context cannot disagree.
US_PRE_MARKET_OPEN = time(4, 0)    # 4:00 AM ET
US_MARKET_OPEN = time(9, 30)       # 9:30 AM ET
US_MARKET_CLOSE = time(16, 0)      # 4:00 PM ET
US_AFTER_HOURS_CLOSE = time(20, 0)  # 8:00 PM ET


def get_market_session(now: Optional[datetime] = None) -> Tuple[str, datetime]:
    """
    Determine the US market session at ``now`` (default: the current moment) in Eastern Time.

    Returns:
        Tuple of (session_name, et_time)
        session_name: "PRE_MARKET", "REGULAR_HOURS", "AFTER_HOURS", or "CLOSED"
    """
    et_tz = pytz.timezone("US/Eastern")
    now_et = datetime.now(et_tz) if now is None else now.astimezone(et_tz)

    # Check if it's a weekday (Monday=0, Sunday=6)
    if now_et.weekday() >= 5:  # Saturday or Sunday
        return "CLOSED", now_et

    # Get current time
    current_time = now_et.time()

    if US_MARKET_OPEN <= current_time < US_MARKET_CLOSE:
        return "REGULAR_HOURS", now_et
    elif US_MARKET_CLOSE <= current_time < US_AFTER_HOURS_CLOSE:
        return "AFTER_HOURS", now_et
    elif US_PRE_MARKET_OPEN <= current_time < US_MARKET_OPEN:
        return "PRE_MARKET", now_et
    else:
        return "CLOSED", now_et


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
