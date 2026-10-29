"""Timezone utilities: parsing a zone name, and label extraction and formatting."""

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def zone_or_none(name: Optional[str]) -> Optional[ZoneInfo]:
    """The zone an IANA name names, or None when it names none.

    Zone names arrive unvalidated from profiles, requests and stored rows, and
    ``ZoneInfo`` refuses a bad one three ways: an unknown name, a malformed key
    (empty, absolute, ``..``) as ValueError, and a directory or overlong name
    as OSError. Each caller decides its own fallback from None.
    """
    if not name or not isinstance(name, str):
        return None
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        return None


def get_timezone_label(dt: Optional[datetime]) -> str:
    """
    Extract timezone abbreviation from a datetime object.

    Uses strftime('%Z') to get DST-aware abbreviations like:
    - EST/EDT for America/New_York
    - CST for Asia/Shanghai
    - UTC for UTC timezone

    Args:
        dt: Timezone-aware datetime object (or None)

    Returns:
        Timezone abbreviation string (e.g., "EST", "EDT", "CST", "UTC")
        Returns "UTC" if dt is None or naive (no timezone info)

    Example:
        >>> from datetime import datetime
        >>> from zoneinfo import ZoneInfo
        >>> dt_winter = datetime(2025, 1, 15, tzinfo=ZoneInfo("America/New_York"))
        >>> get_timezone_label(dt_winter)
        'EST'
        >>> dt_summer = datetime(2025, 7, 15, tzinfo=ZoneInfo("America/New_York"))
        >>> get_timezone_label(dt_summer)
        'EDT'
    """
    if dt is None or dt.tzinfo is None:
        # Naive datetime or None - default to UTC
        return "UTC"

    # Extract timezone abbreviation (DST-aware)
    return dt.strftime('%Z')
