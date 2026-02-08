"""Bocha search tool for LangChain integration.

Bocha is a search API service optimized for Chinese language queries
and Chinese market content.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from langchain_core.tools import tool

from src.tools.search_services.bocha import BochaAPI

logger = logging.getLogger(__name__)

# Module-level configuration
_api_wrapper: Optional[BochaAPI] = None
_max_results: int = 10
_default_time_range: Optional[str] = None
_verbose: bool = True


def _get_api_wrapper() -> BochaAPI:
    """Get or create the API wrapper instance."""
    global _api_wrapper
    if _api_wrapper is None:
        _api_wrapper = BochaAPI()
    return _api_wrapper


def configure(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    verbose: bool = True,
) -> None:
    """Configure the Bocha search tool settings.

    Args:
        max_results: Maximum number of search results to return.
        default_time_range: Default time range filter (d/w/m/y or day/week/month/year).
            Used as fallback if LLM doesn't specify time_range in query.
        verbose: Control verbosity of search results.
            True (default): Include images in results.
            False: Exclude images, return webpage results only (lightweight for planning).
    """
    global _max_results, _default_time_range, _verbose
    _max_results = max_results
    _default_time_range = default_time_range
    _verbose = verbose


def _translate_time_to_freshness(
    time_range: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> Optional[str]:
    """Translate Tavily-style time parameters to Bocha's freshness format.

    Priority: Date range (start_date/end_date) > time_range > None (noLimit)

    Args:
        time_range: Relative time filter (day/week/month/year or d/w/m/y)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Bocha freshness parameter value or None for no filtering
    """
    # Priority 1: Explicit date range
    if start_date or end_date:
        if start_date and end_date:
            # Date range: "YYYY-MM-DD..YYYY-MM-DD"
            return f"{start_date}..{end_date}"
        elif start_date:
            # From specific date to present: just use the date
            return start_date
        else:
            # end_date only - not typically meaningful for search, but support it
            logger.warning(f"end_date provided without start_date: {end_date}. Using as specific date.")
            return end_date

    # Priority 2: Relative time range
    if time_range:
        # Map Tavily-style time_range to Bocha freshness values
        time_range_map = {
            "day": "oneDay",
            "d": "oneDay",
            "week": "oneWeek",
            "w": "oneWeek",
            "month": "oneMonth",
            "m": "oneMonth",
            "year": "oneYear",
            "y": "oneYear",
        }

        if time_range in time_range_map:
            return time_range_map[time_range]
        else:
            logger.warning(f"Unknown time_range value: {time_range}. Using noLimit.")
            return "noLimit"

    # Priority 3: No time filtering (default)
    return "noLimit"


def _validate_date_format(date_str: Optional[str]) -> Optional[str]:
    """Validate date format is YYYY-MM-DD and represents a valid date."""
    if date_str is None:
        return None

    # Check format
    if not date_str or len(date_str) != 10:
        raise ValueError("Date must be in YYYY-MM-DD format (e.g., '2025-01-15')")

    parts = date_str.split('-')
    if len(parts) != 3:
        raise ValueError("Date must be in YYYY-MM-DD format (e.g., '2025-01-15')")

    try:
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        raise ValueError("Date must be in YYYY-MM-DD format with valid integers")

    # Validate it's a real date
    try:
        datetime(year, month, day)
    except ValueError as e:
        raise ValueError(f"Invalid date: {e}")

    return date_str


def _filter_artifact_for_frontend(raw_data: Dict[str, Any], verbose: bool) -> Dict[str, Any]:
    """Filter artifact to remove content duplication for frontend display.

    Removes: content, summary, snippet, site_name from results
    Keeps: query, response_time, total_results, results[].{title, url, favicon, publish_time, id, snippet}, images

    Args:
        raw_data: Full raw data with all fields
        verbose: Whether to include images

    Returns:
        Filtered artifact with UI-only metadata
    """
    filtered = {
        "query": raw_data.get("query", ""),
        "response_time": raw_data.get("response_time", 0),
        "total_results": raw_data.get("total_results", 0),
        "results": [],
        "images": []
    }

    # Filter webpage results - keep UI metadata only
    for result in raw_data.get("results", []):
        if result.get("type") == "webpage":
            filtered["results"].append({
                "title": result.get("title", ""),
                "url": result.get("url", ""),
                "favicon": result.get("favicon", ""),
                "publish_time": result.get("publish_time", ""),
                "id": result.get("id", ""),
                "snippet": result.get("snippet", "")  # Brief excerpt for UI preview
                # NO content, summary, site_name (avoid duplication)
            })

    # Filter image results - only include when verbose=True
    if verbose:
        for result in raw_data.get("results", []):
            if result.get("type") == "image":
                filtered["images"].append({
                    "image_url": result.get("image_url", ""),
                    "thumbnail_url": result.get("thumbnail_url", ""),
                    "source_url": result.get("source_url", "")
                })

    # Add conversation metadata if available
    if "conversation_id" in raw_data:
        filtered["conversation_id"] = raw_data["conversation_id"]
    if "log_id" in raw_data:
        filtered["log_id"] = raw_data["log_id"]

    return filtered


@tool(response_format="content_and_artifact")
async def web_search(
    query: str,
    time_range: Optional[Literal["day", "week", "month", "year", "d", "w", "m", "y"]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
    """Search the web for current information, news, and facts.

    Use when you need to:
    - Find recent news or current events
    - Look up facts, statistics, or real-time data
    - Research topics beyond your knowledge cutoff
    - Verify or update information

    Args:
        query: Search query to look up (supports Chinese and English)
        time_range: Filter results by relative time from now.
            'day'/'d' = past 24 hours, 'week'/'w' = past week,
            'month'/'m' = past month, 'year'/'y' = past year.
            Ignored if start_date or end_date is provided.
        start_date: Start date for filtering results (YYYY-MM-DD format).
            Returns results from this date onwards.
        end_date: End date for filtering results (YYYY-MM-DD format).
            Returns results up to this date.
    """
    try:
        # Validate date formats
        _validate_date_format(start_date)
        _validate_date_format(end_date)

        # Apply default time_range if LLM didn't specify one
        effective_time_range = time_range or _default_time_range
        if effective_time_range != time_range:
            logger.debug(
                f"Using default time_range: {effective_time_range} "
                f"(LLM: {time_range}, default: {_default_time_range})"
            )

        # Translate time parameters to Bocha's freshness format
        freshness = _translate_time_to_freshness(effective_time_range, start_date, end_date)

        # Execute search via BochaAPI
        api = _get_api_wrapper()
        api_results, metadata = await api.web_search(
            query=query,
            count=_max_results,
            freshness=freshness,
            answer=False  # Don't request LLM answer
        )

        # Build content for LLM (focused, no UI metadata)
        content: List[Dict[str, Any]] = []

        for item in api_results:
            if item.get("type") == "webpage":
                content.append({
                    "type": "page",
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "content": item.get("summary", ""),  # Primary content for LLM
                    "publish_time": item.get("publish_time", ""),
                    "site_name": item.get("site_name", "")  # Source credibility
                })
            elif item.get("type") == "image" and _verbose:
                # Only include images when verbose=True
                content.append({
                    "type": "image",
                    "image_url": item.get("content_url", ""),
                    "image_description": ""  # Bocha doesn't provide descriptions
                })

        # Build raw data structure with all fields (for artifact filtering)
        raw_data = {
            "query": metadata.get("query", query),
            "response_time": metadata.get("response_time", 0),
            "total_results": metadata.get("total_results", 0),
            "results": []
        }

        # Add all fields to raw data
        for item in api_results:
            if item.get("type") == "webpage":
                raw_data["results"].append({
                    "type": "webpage",
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "favicon": item.get("site_icon", ""),  # Map site_icon to favicon
                    "publish_time": item.get("publish_time", ""),
                    "id": item.get("id", ""),
                    "snippet": item.get("snippet", ""),
                    # Include content fields for filtering (will be removed)
                    "content": item.get("summary", ""),
                    "summary": item.get("summary", ""),
                    "site_name": item.get("site_name", "")
                })
            elif item.get("type") == "image":
                raw_data["results"].append({
                    "type": "image",
                    "image_url": item.get("content_url", ""),
                    "thumbnail_url": item.get("thumbnail_url", ""),
                    "source_url": item.get("host_page_url", "")
                })

        # Add conversation metadata
        if "conversation_id" in metadata:
            raw_data["conversation_id"] = metadata["conversation_id"]
        if "log_id" in metadata:
            raw_data["log_id"] = metadata["log_id"]

        # Filter artifact to remove content duplication
        artifact = _filter_artifact_for_frontend(raw_data, _verbose)

        logger.info(f"Bocha AI search completed: {len(content)} items for query '{query[:50]}...'")
        logger.debug(f"Content structure: {len([c for c in content if c.get('type')=='page'])} pages, "
                    f"{len([c for c in content if c.get('type')=='image'])} images")

        return content, artifact

    except Exception as e:
        logger.error(f"Bocha AI search failed: {e}", exc_info=True)
        error_msg = f"Bocha search error: {str(e)}"
        return error_msg, {"error": str(e), "query": query}


# Backwards compatibility alias
BochaSearchTool = web_search
