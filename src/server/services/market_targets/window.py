"""How long a market-watch target stays armed.

A target lasts through the first full regular session of its venue that
opens after it is set and closes at least ``MIN_LIFETIME`` later, and lapses
at that session's close. A flat 24 hours would let a Friday-afternoon target
lapse on Saturday, and wake the agent to re-arm it while nothing trades; on
the venue calendar it carries over a weekend or holiday to the next session
in which it can actually hit. The floor keeps a target set before the open
from lapsing that same afternoon.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Union

from market_protocol.calendars import get_calendar

from src.config.settings import get_market_watch_target_warning_minutes
from src.server.models.automation import MarketType
from src.server.services.price_monitor import watched_listing

logger = logging.getLogger(__name__)

# When the venue calendar cannot answer, a day still outlasts any session.
FALLBACK_LIFETIME = timedelta(hours=24)
MIN_LIFETIME = timedelta(hours=12)

# Longer than any venue's gap between sessions (Shanghai's longest is 17
# days), so a walk started inside a holiday always reaches the next session.
_WALK_DAYS = 21


def _aware(at: datetime) -> datetime:
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


def session_expiry(
    symbol: str, market: Union[MarketType, str], now: datetime
) -> Optional[datetime]:
    """Close of the first full regular session that opens after *now* and
    closes at least ``MIN_LIFETIME`` later, or None.

    The listing is read the way the price monitor reads it, so the calendar
    is the one the target is watched on. A session already under way at
    *now* does not count: a target set at 15:50 would otherwise lapse ten
    minutes later.
    """
    now = _aware(now)
    try:
        ref = watched_listing(symbol, MarketType(market))
        if ref is None:
            return None
        cal = get_calendar(ref.calendar_id)
        now_ms = int(now.timestamp() * 1000)
        start = now.astimezone(cal.tz).date()
        for i in range(_WALK_DAYS):
            day = start + timedelta(days=i)
            open_ms = cal.session_open_ms(day)
            if open_ms is None or open_ms <= now_ms:
                continue
            close_ms = cal.session_close_ms(day)
            if close_ms is None or close_ms <= open_ms:
                continue
            if close_ms - now_ms < MIN_LIFETIME.total_seconds() * 1000:
                continue
            return datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc)
    except Exception:
        logger.warning(
            "[MarketTargets] no session calendar for %s, using the fallback window",
            symbol,
            exc_info=True,
        )
    return None


def target_window(
    symbol: str,
    market: Union[MarketType, str],
    now: datetime,
    *,
    warning_minutes: Optional[int] = None,
) -> Tuple[datetime, datetime]:
    """``(expires_at, warn_at)`` for a target armed at *now*, both UTC.

    The warning is clamped to *now* so a window shorter than the warning
    period still gets its notice instead of one dated in the past.
    """
    now = _aware(now).astimezone(timezone.utc)
    expires_at = session_expiry(symbol, market, now) or now + FALLBACK_LIFETIME
    minutes = (
        warning_minutes
        if warning_minutes is not None
        else get_market_watch_target_warning_minutes()
    )
    warn_at = max(expires_at - timedelta(minutes=minutes), now)
    return expires_at, warn_at
