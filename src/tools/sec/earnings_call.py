"""
Async earnings call transcript integration for SEC filings.

Provides async functionality to fetch earnings call transcripts that match
SEC filing periods using FMP's transcript dates API.
"""

import logging
from datetime import date, datetime
from typing import Optional, Tuple

from src.data_client.fmp import FMPClient

logger = logging.getLogger(__name__)


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


async def fetch_matching_earnings_call(
    symbol: str,
    filing_date: date,
) -> Optional[Tuple[str, int, int, date]]:
    """
    Async fetch earnings call transcript matching the SEC filing period.

    Uses native async FMP client for non-blocking API calls.

    Strategy:
    1. Get all transcript dates from FMP
    2. Find the call closest to filing_date (within 30 days)
    3. Fetch and return that transcript with fiscal period info

    Args:
        symbol: Stock ticker symbol
        filing_date: Date the SEC filing was submitted

    Returns:
        Tuple of (transcript_content, fiscal_year, fiscal_quarter, call_date) or None
    """
    try:
        async with FMPClient() as client:
            dates = await client.get_earnings_call_dates(symbol)

            if not dates:
                logger.debug(f"No earnings call dates found for {symbol}")
                return None

            # Find closest call to filing_date
            best_match = None
            min_diff = float('inf')

            for entry in dates:
                if len(entry) < 3:
                    continue
                quarter, fiscal_year, call_datetime = entry[0], entry[1], entry[2]
                try:
                    call_date = datetime.strptime(call_datetime[:10], "%Y-%m-%d").date()
                    diff = abs((filing_date - call_date).days)

                    if diff < min_diff and diff <= 30:  # Within 30 days
                        min_diff = diff
                        best_match = (quarter, fiscal_year, call_date)
                except (ValueError, TypeError):
                    continue

            if not best_match:
                logger.debug(f"No earnings call found within 30 days of filing date {filing_date}")
                return None

            quarter, fiscal_year, call_date = best_match
            logger.debug(f"Found matching earnings call: Q{quarter} FY{fiscal_year} (call date: {call_date})")

            # fetch_earnings_transcript is sync, but since we're in async context,
            # we need to fetch the transcript using the async FMP client directly
            transcript_data = await client.get_earnings_call_transcript(symbol, fiscal_year, quarter)

            if transcript_data and len(transcript_data) > 0:
                content = transcript_data[0].get('content', '')
                if content and "No data available" not in content:
                    return (content, fiscal_year, quarter, call_date)

            return None

    except Exception as e:
        logger.warning(f"Failed to fetch earnings call for {symbol}: {e}")
        return None
