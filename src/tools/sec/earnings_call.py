"""
Async earnings call transcript integration for SEC filings.

Matches the transcript of the earnings call that reports a filing's period, using
FMP's transcript dates. Call dates change as calls are scheduled and held, so they
are read on every call; a published transcript does not change once out, so it is
kept in Redis for every worker, and one not published yet is never kept.
"""

from datetime import date
from typing import List, NamedTuple, Optional

from src.data_client.fmp import FMPClient
from src.data_client.fmp.fmp_client import CallDate
from src.utils.cache.redis_cache import PERSIST, get_cache_client

from .metering import fmp_event_hooks, note_cache
from .types import FilingType

# Per form: the most days a call may follow the period end, and the fiscal
# quarters whose call reports that form's period.
CALL_POLICY = {
    FilingType.FORM_10K: (180, {4}),
    FilingType.FORM_10Q: (90, {1, 2, 3}),
}


class EarningsCall(NamedTuple):
    transcript: str
    fiscal_year: int
    quarter: int
    call_date: date


def _select_call(
    dates: List[CallDate],
    filing_type: FilingType,
    period_end: date,
) -> Optional[CallDate]:
    """
    Pick the FMP call that reports the filing's period, or None.

    The call reporting a period is the first one held after it ends whose
    quarter fits the form, within the form's CALL_POLICY lag. A call
    outranked by the fiscal label of a later call in that window reports an
    earlier period (a late filer's slipped call) and is skipped. FMP can
    list one call twice under two fiscal labels; the lower fiscal year wins.
    """
    max_lag, quarters = CALL_POLICY[filing_type]
    calls = [c for c in dates if c.quarter in quarters and 0 < (c.date - period_end).days <= max_lag]
    reporting = [
        c for c in calls
        if not any(o.date > c.date and (o.fiscal_year, o.quarter) > (c.fiscal_year, c.quarter) for o in calls)
    ]
    return min(reporting, key=lambda c: (c.date, c.fiscal_year, c.quarter), default=None)


async def _transcript(client: FMPClient, symbol: str, call: CallDate) -> str:
    # The call date is in the key because FMP can move a fiscal label to another call.
    key = f"sec:fmp:transcript:{symbol.upper()}:{call.fiscal_year}:{call.quarter}:{call.date.isoformat()}"
    cache = get_cache_client()
    cached = await cache.get(key)
    note_cache("fmp_transcript", "miss" if cached is None else "hit")
    if cached is not None:
        return cached
    data = await client.get_earnings_call_transcript(symbol, call.fiscal_year, call.quarter)
    content = (data[0].get("content") if data else None) or ""
    if not content.strip() or content.strip().startswith("No data available"):
        content = ""
    if content:
        await cache.set(key, content, ttl=PERSIST)
    return content


async def fetch_matching_earnings_call(
    symbol: str,
    filing_type: FilingType,
    period_end: str,
) -> Optional[EarningsCall]:
    """
    The earnings call for the SEC filing's period.

    FMP errors propagate. None means FMP lists no call for the period (see
    _select_call); an empty transcript means FMP has not published one for
    the selected call.
    """
    period = date.fromisoformat(period_end)
    async with FMPClient(event_hooks=fmp_event_hooks()) as client:
        call = _select_call(await client.get_earnings_call_dates(symbol), filing_type, period)
        if call is None:
            return None
        content = await _transcript(client, symbol, call)
    return EarningsCall(content, call.fiscal_year, call.quarter, call.date)
