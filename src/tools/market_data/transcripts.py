# pyright: ignore
"""Earnings call transcripts (FMP)."""

from datetime import datetime, timezone
import logging



from ._shared import _fmp_request

logger = logging.getLogger(__name__)


async def fetch_earnings_transcript(symbol: str, year: int, quarter: int) -> str:
    """
    Fetch earnings call transcript.

    Retrieves the full transcript of a company's earnings call, formatted for
    easy reading and analysis of management's communication about financial
    performance, future plans, and strategy.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")
        year: Fiscal year (e.g., 2020) - REQUIRED
        quarter: Fiscal quarter (1, 2, 3, or 4) - REQUIRED

    Returns:
        Formatted string with earnings call transcript
    """
    try:
        output_lines = []

        # Fetch transcript data (FMP-specific, not in generic protocol)
        transcript_data = await _fmp_request(
            "get_earnings_call_transcript", symbol=symbol, year=year, quarter=quarter
        )

        if not transcript_data or len(transcript_data) == 0:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            return f"""## Earnings Transcript: {symbol} Q{quarter} {year}
**Retrieved:** {timestamp}
**Status:** No data available

No earnings transcript found for {symbol} Q{quarter} {year}"""

        transcript = transcript_data[0]

        # Extract metadata
        company_symbol = transcript.get("symbol", symbol)
        period = transcript.get("period", "N/A")
        fiscal_year = transcript.get("year", "N/A")
        call_date = transcript.get("date", "N/A")
        content = transcript.get("content", "")

        # Add file-ready header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        output_lines.append(f"## Earnings Transcript: {symbol} Q{quarter} {year}")
        output_lines.append(f"**Retrieved:** {timestamp}")
        output_lines.append(f"**Fiscal Period:** {period} {fiscal_year}")
        output_lines.append(f"**Call Date:** {call_date}")
        output_lines.append("")

        # Header section
        output_lines.append(f"Earnings Call Transcript: {company_symbol}")
        output_lines.append("═" * 70)
        output_lines.append(f"Fiscal Period: {period} {fiscal_year}")
        output_lines.append(f"Call Date: {call_date}")
        output_lines.append("═" * 70)
        output_lines.append("")

        # Add transcript content
        if content:
            # Split content into lines for better formatting
            content_lines = content.split("\n")

            # If content is very long, provide full transcript
            # (LLMs can handle large context, and users want full analysis capability)
            output_lines.append("Transcript Content:")
            output_lines.append("")
            output_lines.append("```text")
            output_lines.extend(content_lines)
            output_lines.append("```")
            output_lines.append("")

            # Add transcript stats
            word_count = len(content.split())
            char_count = len(content)
            output_lines.append("Transcript Statistics:")
            output_lines.append(f"├─ Words: {word_count:,}")
            output_lines.append(f"└─ Characters: {char_count:,}")
        else:
            output_lines.append("Note: Transcript content is empty or not available.")

        result = "\n".join(output_lines)
        logger.debug(
            f"Retrieved earnings transcript for {symbol} {period} {fiscal_year}"
        )
        return result

    except Exception as e:
        logger.error(f"Error retrieving earnings transcript for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return f"""## Earnings Transcript: {symbol} Q{quarter} {year}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving earnings transcript: {str(e)}"""


# ─── Stock Screener ──────────────────────────────────────────────────
