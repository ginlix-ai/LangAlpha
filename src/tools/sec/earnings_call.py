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


def format_earnings_call_section(call: Optional[EarningsCall], period_end: str) -> str:
    """
    Format the matched call's transcript as a markdown section, or the notice
    that no call matched the filing's period or that its transcript is not
    published.

    Args:
        call: The matched earnings call, or None
        period_end: The filing's period of report, "YYYY-MM-DD"

    Returns:
        Formatted markdown section
    """
    if call and call.transcript:
        return f"""

---

## Earnings Call Transcript

**Matched Call:** Q{call.quarter} FY{call.fiscal_year}, held {call.call_date}, the call reporting the period ending {period_end}
**Alignment:** SEC filing and earnings call cover the same fiscal period

{call.transcript}
"""
    if call is None:
        notice = f"no earnings call transcript could be matched to the period ending {period_end}."
    else:
        notice = f"the earnings call held {call.call_date} for the period ending {period_end} has no published transcript yet."
    return f"""

---

## Earnings Call Transcript

**Not included:** {notice}
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
    _select_call); an empty transcript means FMP has not published one for
    the selected call.

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

    content = (transcript_data[0].get("content") if transcript_data else None) or ""
    if not content.strip() or content.strip().startswith("No data available"):
        content = ""
    return EarningsCall(content, call.fiscal_year, call.quarter, call.date)
