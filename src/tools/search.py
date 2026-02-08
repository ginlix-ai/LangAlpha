import logging
from typing import Optional

from src.config import SearchEngine, SELECTED_SEARCH_ENGINE
from src.tools.decorators import create_logged_tool

logger = logging.getLogger(__name__)


def get_web_search_tool(
    max_search_results: int,
    time_range: Optional[str] = None,
    verbose: bool = True,
):
    """Get web search tool with verbosity and time range control.

    Args:
        max_search_results: Maximum number of results to return.
        time_range: Default time range filter (d/w/m/y or day/week/month/year).
            Used as fallback if LLM doesn't specify time_range in query.
            LLM can still override by specifying a different time_range.
        verbose: Control verbosity of search results.
            True (default): Include images in results.
            False: Exclude images (lightweight for planning).
    """
    if SELECTED_SEARCH_ENGINE == SearchEngine.SERPER.value:
        from src.tools.search_services.serper import configure as configure_serper
        from src.tools.search_services.serper import web_search as serper_web_search

        configure_serper(
            max_results=max_search_results,
            default_time_range=time_range,
        )
        return create_logged_tool(serper_web_search, name="WebSearch")

    elif SELECTED_SEARCH_ENGINE == SearchEngine.TAVILY.value:
        from src.tools.search_services.tavily import configure as configure_tavily
        from src.tools.search_services.tavily import web_search as tavily_web_search

        configure_tavily(
            max_results=max_search_results,
            default_time_range=time_range,
            verbose=verbose,
        )
        return create_logged_tool(tavily_web_search, name="WebSearch")

    elif SELECTED_SEARCH_ENGINE == SearchEngine.BOCHA.value:
        from src.tools.search_services.bocha import configure as configure_bocha
        from src.tools.search_services.bocha import web_search as bocha_web_search

        configure_bocha(
            max_results=max_search_results,
            default_time_range=time_range,
            verbose=verbose,
        )
        return create_logged_tool(bocha_web_search, name="WebSearch")

    else:
        raise ValueError(
            f"Unsupported search engine: {SELECTED_SEARCH_ENGINE}. "
            f"Supported engines: {[e.value for e in SearchEngine]}"
        )
