"""Serper search tool for LangChain integration."""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from langchain_core.callbacks import AsyncCallbackManagerForToolRun
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from .serper import SerperAPI

logger = logging.getLogger(__name__)


class SerperSearchInput(BaseModel):
    """Input schema for Serper search tool."""

    query: str = Field(description="Search query to execute")
    time_range: Optional[str] = Field(
        default=None,
        description=(
            "Time range filter for search results. Options:\n"
            "- 'd' or 'day': Past 24 hours\n"
            "- 'w' or 'week': Past 7 days\n"
            "- 'm' or 'month': Past 30 days\n"
            "- 'y' or 'year': Past 12 months\n"
            "If not specified, searches all time."
        ),
    )
    geographic_location: Optional[str] = Field(
        default="us",
        description="Geographic location code (e.g., 'us', 'cn', 'uk')",
    )
    language: Optional[str] = Field(
        default="en",
        description="Language code (e.g., 'en', 'zh-cn', 'es')",
    )


class SerperSearchTool(BaseTool):
    """LangChain-compatible Serper search tool using Google Search.

    This tool provides access to Google Search results via Serper.dev API.
    Serper offers fresh, high-quality search results with low latency.

    Best for:
    - Current events and news
    - General web search
    - Factual information lookup
    - Recent developments and updates
    """

    name: str = "web_search"
    description: str = (
        "Search the web using Google via Serper.dev. "
        "Use this tool to find current information, news, facts, and general knowledge. "
        "Supports time range filtering (day/week/month/year) and geographic/language preferences. "
        "Returns high-quality, relevant search results."
    )
    args_schema: type[BaseModel] = SerperSearchInput
    response_format: str = "content_and_artifact"

    # Configuration
    api_wrapper: SerperAPI = Field(default_factory=SerperAPI)
    max_results: int = Field(default=10, description="Maximum number of results to return")
    verbose: bool = Field(default=True, description="Include metadata in response")
    default_time_range: Optional[str] = Field(
        default=None,
        description="Default time range if not specified by LLM",
    )
    default_gl: str = Field(default="us", description="Default geographic location")
    default_hl: str = Field(default="en", description="Default language")

    def _determine_time_range(
        self,
        time_range: Optional[str] = None,
    ) -> Optional[str]:
        """Determine effective time range using priority order.

        Priority (highest to lowest):
        1. LLM-provided time_range parameter
        2. Tool's default_time_range
        3. None (no filtering)

        Args:
            time_range: Time range from LLM

        Returns:
            Effective time range to use, or None for no filtering
        """
        # Priority 1: LLM-provided time_range
        if time_range:
            # Normalize to single letter
            time_map = {
                "day": "d",
                "week": "w",
                "month": "m",
                "year": "y",
            }
            return time_map.get(time_range.lower(), time_range.lower())

        # Priority 2: default_time_range
        if self.default_time_range:
            time_map = {
                "day": "d",
                "week": "w",
                "month": "m",
                "year": "y",
            }
            return time_map.get(
                self.default_time_range.lower(),
                self.default_time_range.lower(),
            )

        # Priority 3: No filtering
        return None

    async def _arun(
        self,
        query: str,
        time_range: Optional[str] = None,
        geographic_location: Optional[str] = None,
        language: Optional[str] = None,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
        """Execute Serper search asynchronously.

        Args:
            query: Search query
            time_range: Time range filter (d, w, m, y)
            geographic_location: Geographic location code
            language: Language code
            run_manager: Callback manager

        Returns:
            Tuple of (detailed_results, metadata):
                - detailed_results: List of search results for LLM
                - metadata: Search metadata for UI
        """
        try:
            # Determine effective time range
            effective_time_range = self._determine_time_range(time_range)

            # Use defaults if not specified
            gl = geographic_location or self.default_gl
            hl = language or self.default_hl

            logger.info(
                f"Executing Serper search: query='{query}', "
                f"time_range={effective_time_range}, gl={gl}, hl={hl}"
            )

            # Execute search
            detailed_results, metadata = await self.api_wrapper.web_search(
                query=query,
                num=self.max_results,
                time_range=effective_time_range,
                gl=gl,
                hl=hl,
            )

            logger.info(
                f"Serper search completed: {len(detailed_results)} results returned"
            )

            # Return both content (for LLM) and artifact (for UI)
            return detailed_results, metadata

        except Exception as e:
            logger.error(f"Serper search failed: {e}", exc_info=True)
            error_message = f"Search failed: {str(e)}"
            return error_message, {"error": str(e), "query": query}

    def _run(
        self,
        query: str,
        time_range: Optional[str] = None,
        geographic_location: Optional[str] = None,
        language: Optional[str] = None,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
        """Synchronous version - not supported.

        Use _arun() instead for async execution.
        """
        raise NotImplementedError("SerperSearchTool only supports async execution (_arun)")
