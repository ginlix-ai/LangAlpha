"""Serper search tool for LangChain integration."""

import logging
from typing import Any, Literal, Optional, Union

import httpx
from langchain_core.tools import tool

from .serper import SerperAPI

logger = logging.getLogger(__name__)

# Module-level configuration
_api_wrapper: Optional[SerperAPI] = None
_max_results: int = 10
_default_time_range: Optional[str] = None
_default_gl: str = "us"
_default_hl: str = "en"


def _get_api_wrapper() -> SerperAPI:
    """Get or create the API wrapper instance."""
    global _api_wrapper
    if _api_wrapper is None:
        _api_wrapper = SerperAPI()
    return _api_wrapper


def _normalize_time_range(time_range: Optional[str]) -> Optional[str]:
    """Normalize time range to single letter format."""
    if not time_range:
        return _default_time_range

    time_map = {
        "hour": "h",
        "day": "d",
        "week": "w",
        "month": "m",
        "year": "y",
    }
    normalized = time_map.get(time_range.lower(), time_range.lower())

    if normalized in ("h", "d", "w", "m", "y"):
        return normalized

    logger.warning(f"Invalid time_range '{time_range}', ignoring")
    return _default_time_range


def configure(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    default_gl: str = "us",
    default_hl: str = "en",
) -> None:
    """Configure the Serper search tool settings."""
    global _max_results, _default_time_range, _default_gl, _default_hl
    _max_results = max_results
    _default_time_range = default_time_range
    _default_gl = default_gl
    _default_hl = default_hl


@tool(response_format="content_and_artifact")
async def web_search(
    query: str,
    search_type: Optional[Literal["general", "news"]] = "general",
    time_range: Optional[Literal["h", "d", "w", "m", "y"]] = None,
    geographic_location: Optional[str] = None,
    language: Optional[str] = None,
) -> tuple[Union[list[dict[str, Any]], str], dict[str, Any]]:
    """Search the web for current information, news, and facts.

    Use when you need to:
    - Find recent news or current events
    - Look up facts, statistics, or real-time data
    - Research topics beyond your knowledge cutoff
    - Verify or update information

    Args:
        query: Search query to execute
        search_type: 'general' (default) or 'news' for news articles only
        time_range: Filter by recency - 'h' (hour), 'd' (day), 'w' (week), 'm' (month), 'y' (year)
        geographic_location: Country code (e.g., 'us', 'cn', 'uk')
        language: Language code (e.g., 'en', 'zh-cn')
    """
    try:
        api = _get_api_wrapper()
        serper_type = "news" if search_type == "news" else "search"
        effective_time_range = _normalize_time_range(time_range)
        gl = geographic_location or _default_gl
        hl = language or _default_hl

        logger.debug(
            f"Executing Serper search: query='{query}', "
            f"type={serper_type}, time_range={effective_time_range}, gl={gl}, hl={hl}"
        )

        detailed_results, metadata = await api.web_search(
            query=query,
            search_type=serper_type,
            num=_max_results,
            time_range=effective_time_range,
            gl=gl,
            hl=hl,
        )

        logger.debug(f"Serper search completed: {len(detailed_results)} results returned")
        return detailed_results, metadata

    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else "unknown"
        body = e.response.text if e.response is not None else "no response body"
        logger.error(f"Serper API HTTP {status}: {body}")
        error_message = f"Search failed (HTTP {status}): {body}"
        return error_message, {"error": error_message, "query": query}
    except httpx.HTTPError as e:
        logger.error(f"Serper search failed: {e}", exc_info=True)
        error_message = f"Search failed: {str(e)}"
        return error_message, {"error": str(e), "query": query}


# Alias for backwards compatibility
SerperSearchTool = web_search
