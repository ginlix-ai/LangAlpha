"""
LangChain tool for SEC filing extraction.

Provides a unified interface to fetch and parse SEC filings (10-K, 10-Q).
Uses edgartools for direct SEC EDGAR access with structured section extraction.
Falls back to regex-based extraction if edgartools fails.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional, Tuple

from langchain_core.tools import tool

from .types import (
    FilingType,
    DEFAULT_10K_SECTIONS,
    DEFAULT_10Q_SECTIONS,
)
from .parsers.base import ParsingFailedError
from .earnings_call import (
    fetch_matching_earnings_call,
    format_earnings_call_section,
)
from .eight_k import (
    fetch_8k_filings,
    format_8k_filings,
    find_recent_8k_filings,
    format_8k_reminder,
    DEFAULT_8K_DAYS,
)

logger = logging.getLogger(__name__)

# Thread pool for running blocking edgartools calls
_executor = ThreadPoolExecutor(max_workers=3)


def _get_sec_filing_edgartools_blocking(
    symbol: str,
    filing_type: FilingType,
    sections: Optional[List[str]] = None,
    include_financials: bool = True,
    output_format: str = "markdown",
) -> Tuple[str, Dict[str, Any]]:
    """
    Get SEC filing using edgartools (blocking - runs in thread pool).

    Returns:
        Tuple of (markdown_content, metadata) when output_format="markdown"
    """
    from .parsers.edgartools_parser import EdgarToolsParser

    parser = EdgarToolsParser()
    result = parser.parse_filing(
        symbol=symbol,
        filing_type=filing_type,
        sections=sections,
        include_financials=include_financials,
        output_format=output_format,
    )

    # Parser returns {"content": str, "metadata": dict} for markdown format
    if isinstance(result, dict) and "content" in result and "metadata" in result:
        return result["content"], result["metadata"]

    # Fallback for dict format or unexpected return
    return str(result), {}


def _fetch_sec_filing_blocking(
    symbol: str,
    filing_type: FilingType,
    sections: Optional[List[str]],
    include_financials: bool,
    output_format: str,
) -> Tuple[Any, Optional[str], Dict[str, Any]]:
    """
    Blocking helper to fetch SEC filing content (runs in thread pool).

    Returns:
        Tuple of (filing_content, filing_date_str, metadata) or (error_dict, None, {})
    """
    logger.debug(f"Fetching {filing_type.value} filing for {symbol} using edgartools")

    try:
        content, metadata = _get_sec_filing_edgartools_blocking(
            symbol=symbol,
            filing_type=filing_type,
            sections=sections,
            include_financials=include_financials,
            output_format=output_format,
        )

        # Extract filing date from metadata
        filing_date_str = metadata.get("filing_date")

        return content, filing_date_str, metadata

    except ParsingFailedError as e:
        logger.warning(f"edgartools failed: {e}")
        return {
            "error": str(e),
            "symbol": symbol,
            "filing_type": filing_type.value,
        }, None, {}

    except Exception as e:
        logger.warning(f"edgartools error: {e}")
        return {
            "error": f"Unexpected error: {e}",
            "symbol": symbol,
            "filing_type": filing_type.value,
        }, None, {}


async def _fetch_sec_filing(
    symbol: str,
    filing_type: FilingType,
    sections: Optional[List[str]],
    include_financials: bool,
    output_format: str,
) -> Tuple[Any, Optional[str], Dict[str, Any]]:
    """
    Async fetch SEC filing content using thread pool for blocking calls.

    Returns:
        Tuple of (filing_content, filing_date_str, metadata) or (error_dict, None, {})
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor,
        _fetch_sec_filing_blocking,
        symbol,
        filing_type,
        sections,
        include_financials,
        output_format,
    )


async def get_sec_filing_async(
    symbol: str,
    filing_type: str = "10-K",
    sections: Optional[List[str]] = None,
    use_defaults: bool = True,
    include_financials: bool = True,
    include_earnings_call: bool = True,
    output_format: str = "markdown",
) -> Tuple[str, Dict[str, Any]]:
    """
    Async implementation of SEC filing extraction with parallel fetching.

    Returns:
        Tuple of (content_str, artifact_dict)
    """
    # Validate filing type
    try:
        ftype = FilingType(filing_type)
    except ValueError:
        error_msg = f"Invalid filing type: {filing_type}. Use '10-K', '10-Q', or '8-K'."
        return error_msg, {}

    # Handle 8-K separately - fetch all filings from last 90 days
    if ftype == FilingType.FORM_8K:
        return await _fetch_8k_filings(symbol)

    # Determine sections to extract for 10-K/10-Q
    if sections is None and use_defaults:
        if ftype == FilingType.FORM_10K:
            sections = DEFAULT_10K_SECTIONS
        else:
            sections = DEFAULT_10Q_SECTIONS

    # Step 1: Fetch SEC filing (sequential - need filing_date first)
    result, filing_date_str, metadata = await _fetch_sec_filing(
        symbol=symbol,
        filing_type=ftype,
        sections=sections,
        include_financials=include_financials,
        output_format=output_format,
    )

    # Check for errors
    if isinstance(result, dict) and "error" in result:
        return str(result), {}

    # Build artifact from metadata
    artifact = {
        "type": "sec_filing",
        **metadata,
    }

    # If no filing date or not markdown, return as-is
    if not filing_date_str or not isinstance(result, str):
        return str(result), artifact

    # Parse filing date for parallel fetches
    try:
        filing_date_obj = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(f"Failed to parse filing date: {filing_date_str}")
        return result, artifact

    # Step 2: Fetch earnings call and nearby 8-Ks IN PARALLEL
    tasks = []

    if include_earnings_call:
        tasks.append(
            asyncio.create_task(
                fetch_matching_earnings_call(symbol, filing_date_obj),
                name="earnings_call"
            )
        )

    # Always fetch recent 8-K filings (last 90 days) for 10-K/10-Q
    tasks.append(
        asyncio.create_task(
            find_recent_8k_filings(symbol, max_days=DEFAULT_8K_DAYS),
            name="recent_8k"
        )
    )

    if not tasks:
        return result, artifact

    # Wait for all parallel tasks
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Step 3: Assemble final output
    output = result

    for i, task in enumerate(tasks):
        task_result = results[i]

        if isinstance(task_result, Exception):
            logger.warning(f"Task {task.get_name()} failed: {task_result}")
            continue

        if task.get_name() == "earnings_call" and task_result:
            try:
                transcript, fiscal_year, quarter, call_date = task_result
                earnings_section = format_earnings_call_section(
                    transcript, fiscal_year, quarter, call_date, filing_date_obj
                )
                output += earnings_section
                artifact["has_earnings_call"] = True
            except Exception as e:
                logger.warning(f"Failed to format earnings call: {e}")

        elif task.get_name() == "recent_8k" and task_result:
            try:
                reminder_section = format_8k_reminder(
                    task_result, filing_type, max_days=DEFAULT_8K_DAYS
                )
                output += reminder_section
                artifact["recent_8k_count"] = len(task_result)
            except Exception as e:
                logger.warning(f"Failed to format 8-K reminder: {e}")

    return output, artifact


async def _fetch_8k_filings(symbol: str) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch all 8-K filings from the last 90 days for a symbol.

    Returns:
        Tuple of (markdown_content, artifact_dict)
    """
    filings = await fetch_8k_filings(symbol, max_days=DEFAULT_8K_DAYS)
    content = format_8k_filings(symbol, filings, max_days=DEFAULT_8K_DAYS)

    # Build 8-K artifact
    artifact = {
        "type": "sec_filing",
        "symbol": symbol.upper(),
        "filing_type": "8-K",
        "filing_count": len(filings),
        "days_range": DEFAULT_8K_DAYS,
        "filings": [
            {
                "filing_date": str(f["filing_date"]),
                "items": f["items"],
                "items_desc": [item_info["description"] for item_info in f.get("items_with_desc", [])],
                "source_url": f["source_url"],
                "has_press_release": f.get("has_press_release", False),
            }
            for f in filings
        ],
    }

    return content, artifact


@tool(response_format="content_and_artifact")
async def get_sec_filing(
    symbol: Annotated[str, "Stock ticker symbol (e.g., 'AAPL', 'MSFT', 'NVDA')"],
    filing_type: Annotated[str, "Type of SEC filing: '10-K' (annual), '10-Q' (quarterly), or '8-K' (event-driven)"] = "10-K",
    include_financials: Annotated[bool, "Include financial statements and key metrics (10-K/10-Q only)"] = True,
    include_earnings_call: Annotated[bool, "Include matching earnings call transcript (10-K/10-Q only)"] = True,
) -> Tuple[str, Dict[str, Any]]:
    """Fetch an SEC filing (10-K, 10-Q or 8-K) with its financial statements,
    essential sections and the matching earnings call.

    US SEC filers only — a non-US listing ("0700.HK", "600519.SS") has no
    10-K, 10-Q or 8-K to fetch.

    Args:
        symbol: Stock ticker, e.g. "AAPL".
        filing_type: "10-K" for full-year depth (audited, filed ~60 days after
            fiscal year end); "10-Q" for the most recent numbers (filed ~45 days
            after quarter end); "8-K" for every event-driven filing of the last
            90 days.
        include_financials: Include statements and key metrics. Default True.
        include_earnings_call: Include the transcript for the same fiscal period,
            matched to the filing date. Default True.

    Returns:
        A long markdown report carrying the filing's official SEC EDGAR source
        URLs. Expect it to consume real context; for headline fundamentals alone
        use get_company_overview.

    You MUST cite those source URLs inline for every claim you take from the
    filing, so the user can verify each figure against SEC.gov instead of taking
    it on trust.
    """
    return await get_sec_filing_async(
        symbol=symbol,
        filing_type=filing_type,
        sections=None,
        use_defaults=True,
        include_financials=include_financials,
        include_earnings_call=include_earnings_call,
        output_format="markdown",
    )
