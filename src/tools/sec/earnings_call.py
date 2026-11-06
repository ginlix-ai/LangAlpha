"""
Async earnings call transcript integration for SEC filings.

Provides async functionality to fetch earnings call transcripts that match
SEC filing periods using FMP's transcript dates API.
"""

from datetime import date
from typing import List, NamedTuple, Optional

from src.data_client.fmp import FMPClient
from src.data_client.fmp.fmp_client import CallDate

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


def format_earnings_call_section(
    transcript: str,
    fiscal_year: int,
    quarter: int,
    call_date: date,
    filing_date: date,
) -> str:
    """
    Format earnings call transcript as markdown section.

    Args:
        transcript: Raw transcript content
        fiscal_year: Fiscal year (e.g., 2026)
        quarter: Fiscal quarter (1-4)
        call_date: Date of the earnings call
        filing_date: Date of the SEC filing

    Returns:
        Formatted markdown section
    """
    days_diff = abs((filing_date - call_date).days)

    return f"""

---

## Earnings Call Transcript

**Matched Call:** Q{quarter} FY{fiscal_year} (call date: {call_date}, {days_diff} days from filing)
**Alignment:** SEC filing and earnings call cover the same fiscal period

{transcript}
"""


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


async def fetch_matching_earnings_call(
    symbol: str,
    filing_type: FilingType,
    period_end: str,
) -> Optional[EarningsCall]:
    """
    Async fetch the transcript of the earnings call for the SEC filing's period.

    FMP errors propagate. None means FMP lists no call for the period (see
    _select_call) or has no transcript for it.

    Args:
        symbol: Stock ticker symbol
        filing_type: FilingType.FORM_10K or FilingType.FORM_10Q
        period_end: The filing's period of report, "YYYY-MM-DD"
    """
    period = date.fromisoformat(period_end)
    async with FMPClient() as client:
        call = _select_call(await client.get_earnings_call_dates(symbol), filing_type, period)
        if call is None:
            return None
        transcript_data = await client.get_earnings_call_transcript(symbol, call.fiscal_year, call.quarter)

    content = transcript_data[0].get("content", "") if transcript_data else ""
    if not content or "No data available" in content:
        return None
    return EarningsCall(content, call.fiscal_year, call.quarter, call.date)
