"""Calendar endpoints — economic releases and earnings announcements."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.data_client.fmp.fmp_client import FMPClient
from src.server.models.calendar import (
    EarningsCalendarResponse,
    EarningsEvent,
    EconomicCalendarResponse,
    EconomicEvent,
)
from src.server.services.cache.earnings_cache_service import EarningsCacheService
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/calendar", tags=["Calendar"])

_earnings_cache = EarningsCacheService()


def _default_dates(
    from_date: Optional[str], to_date: Optional[str]
) -> tuple[str, str]:
    """Fill in missing dates with today → today+7."""
    today = date.today()
    if not from_date:
        from_date = today.isoformat()
    if not to_date:
        to_date = (today + timedelta(days=7)).isoformat()
    return from_date, to_date


@router.get("/economic", response_model=EconomicCalendarResponse)
async def get_economic_calendar(
    user_id: CurrentUserId,
    from_date: Optional[str] = Query(
        None, alias="from", description="Start date (YYYY-MM-DD). Defaults to today."
    ),
    to_date: Optional[str] = Query(
        None, alias="to", description="End date (YYYY-MM-DD). Defaults to today+7."
    ),
) -> EconomicCalendarResponse:
    """Get upcoming and past economic data releases (GDP, CPI, etc.)."""
    from_date, to_date = _default_dates(from_date, to_date)

    try:
        fmp_client = FMPClient()
    except (ValueError, ImportError):
        # FMP unavailable — no fallback for economic calendar
        return EconomicCalendarResponse(data=[], count=0)

    try:
        try:
            raw = await fmp_client.get_economic_calendar(
                from_date=from_date, to_date=to_date
            )
            events = [EconomicEvent(**item) for item in (raw or [])]
            return EconomicCalendarResponse(data=events, count=len(events))
        finally:
            await fmp_client.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching economic calendar: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/earnings", response_model=EarningsCalendarResponse)
async def get_earnings_calendar(
    user_id: CurrentUserId,
    from_date: Optional[str] = Query(
        None, alias="from", description="Start date (YYYY-MM-DD). Defaults to today."
    ),
    to_date: Optional[str] = Query(
        None, alias="to", description="End date (YYYY-MM-DD). Defaults to today+7."
    ),
) -> EarningsCalendarResponse:
    """Get upcoming and past earnings announcements with EPS and revenue data."""
    from_date, to_date = _default_dates(from_date, to_date)

    # Check cache
    cached = await _earnings_cache.get(from_date, to_date)
    if cached is not None:
        events = [EarningsEvent(**item) for item in cached]
        return EarningsCalendarResponse(data=events, count=len(events))

    try:
        fmp_client = FMPClient()
    except (ValueError, ImportError):
        # FMP unavailable — no fallback for earnings calendar
        return EarningsCalendarResponse(data=[], count=0)

    try:
        try:
            raw = await fmp_client.get_earnings_calendar_by_date(
                from_date=from_date, to_date=to_date
            )
            items = raw or []
            events = [EarningsEvent(**item) for item in items]

            # Populate cache
            await _earnings_cache.set(items, from_date, to_date)

            return EarningsCalendarResponse(data=events, count=len(events))
        finally:
            await fmp_client.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching earnings calendar: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
