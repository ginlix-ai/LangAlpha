
import logging
import os
from typing import List, Optional

from pydantic import BaseModel, Field

from src.config import SearchEngine, SELECTED_SEARCH_ENGINE
from src.tools.search_services.tavily.tavily_search_tool import (
    TavilySearchTool,
)
from src.tools.search_services.bocha import BochaSearchTool
from src.tools.search_services.serper import SerperSearchTool

from src.tools.decorators import create_logged_tool

logger = logging.getLogger(__name__)


# Create logged versions of the search tools with explicit schemas
LoggedTavilySearch = create_logged_tool(TavilySearchTool)
LoggedBochaSearch = create_logged_tool(BochaSearchTool)
LoggedSerperSearch = create_logged_tool(SerperSearchTool)


# Get the selected search tool
def get_web_search_tool(max_search_results: int,
                        time_range: Optional[str] = None,
                        verbose: bool = True):
    """Get web search tool with verbosity and time range control.

    Args:
        max_search_results: Maximum number of results to return.
        time_range: Default time range filter (d/w/m/y or day/week/month/year).
            Used as fallback if LLM doesn't specify time_range in query.
            LLM can still override by specifying a different time_range.
        verbose: Control verbosity of search results.
            - True (default): Include images, raw_content (for research agents)
            - False: Text-only results (lightweight for planning agents)
    """
    if SELECTED_SEARCH_ENGINE == SearchEngine.TAVILY.value:
        logger.debug(f"Tavily search configuration loaded: default_time_range={time_range}")

        return LoggedTavilySearch(
            name="web_search",
            max_results=max_search_results,
            include_domains=[],
            exclude_domains=[],
            verbose=verbose,
            default_time_range=time_range,
        )
    elif SELECTED_SEARCH_ENGINE == SearchEngine.BOCHA.value:
        return LoggedBochaSearch(
            name="web_search",
            max_results=max_search_results,
            verbose=verbose,
            default_time_range=time_range,
        )
    elif SELECTED_SEARCH_ENGINE == SearchEngine.SERPER.value:
        return LoggedSerperSearch(
            name="web_search",
            max_results=max_search_results,
            verbose=verbose,
            default_time_range=time_range,
        )
    else:
        raise ValueError(f"Unsupported search engine: {SELECTED_SEARCH_ENGINE}")