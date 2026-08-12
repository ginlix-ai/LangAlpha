"""Serper provider: Google Search results via serper.dev.

Official docs: https://serper.dev/docs — auth via ``X-API-KEY`` (SERPER_API_KEY).
"""

import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlparse

import httpx
from langchain_core.tools import tool

from src.tools.web.providers._shared import lazy, normalize_time_range, request_json, result_card

logger = logging.getLogger(__name__)


def _favicon_url(url: str) -> str:
    """Build a Google favicon service URL from a page URL."""
    try:
        domain = urlparse(url).netloc.removeprefix("www.")
        return f"https://www.google.com/s2/favicons?domain={domain}&sz=32" if domain else ""
    except Exception:
        return ""


class SerperAPI:
    """Serper.dev API client for Google Search."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize Serper API client.

        Args:
            api_key: Serper API key. If not provided, reads from SERPER_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("SERPER_API_KEY")
        if not self.api_key:
            raise ValueError("SERPER_API_KEY not found in environment variables")

        self.base_url = "https://google.serper.dev"

    async def _make_request(
        self,
        query: str,
        search_type: Literal["search", "news"] = "search",
        num: int = 10,
        time_range: Optional[str] = None,
        gl: str = "us",
        hl: str = "en",
    ) -> dict:
        """Make HTTP request to Serper API.

        Args:
            query: Search query
            search_type: Type of search - "search" for general, "news" for news
            num: Number of results (max 100)
            time_range: Time range filter (d, w, m, y)
            gl: Geographic location code (e.g., 'us', 'cn')
            hl: Language code (e.g., 'en', 'zh-cn')

        Returns:
            Raw API response as dict

        Raises:
            httpx.HTTPError: If API request fails
        """
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }

        payload = {
            "q": query,
            "num": min(num, 100),  # Serper max is 100
            "gl": gl,
            "hl": hl,
        }

        # Add time range if specified
        if time_range:
            # Serper uses 'tbs' parameter for time filtering
            time_map = {
                "h": "qdr:h",  # Past hour
                "d": "qdr:d",  # Past day
                "w": "qdr:w",  # Past week
                "m": "qdr:m",  # Past month
                "y": "qdr:y",  # Past year
            }
            if time_range.lower() in time_map:
                payload["tbs"] = time_map[time_range.lower()]

        endpoint = f"{self.base_url}/{search_type}"

        return await request_json(
            "POST", endpoint, provider="Serper",
            headers=headers, json_body=payload, timeout=30.0,
        )

    def _format_news_results(
        self,
        raw_response: dict,
        query: str,
        response_time: float,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Format news search results.

        Args:
            raw_response: Raw API response
            query: Original search query
            response_time: Time taken for the search

        Returns:
            Tuple of (detailed_results, metadata)
        """
        detailed_results = []
        news_results = raw_response.get("news", [])

        for article in news_results:
            detailed_results.append({
                "type": "news",
                "title": article.get("title", ""),
                "url": article.get("link", ""),
                "source": article.get("source", ""),
                "date": article.get("date", ""),
                "content": article.get("snippet", ""),
                "image_url": article.get("imageUrl", ""),
            })

        metadata = {
            "type": "web_search",
            "query": query,
            "search_type": "news",
            "search_engine": "serper",
            "response_time": round(response_time, 2),
            "total_results": len(detailed_results),
            "results": [
                {
                    "title": r["title"],
                    "url": r["url"],
                    "favicon": _favicon_url(r["url"]),
                    "source": r["source"],
                    "date": r["date"],
                    "snippet": r["content"],
                    "image_url": r["image_url"],
                }
                for r in detailed_results
            ],
        }

        return detailed_results, metadata

    def _format_general_results(
        self,
        raw_response: dict,
        query: str,
        response_time: float,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Format general search results.

        Args:
            raw_response: Raw API response
            query: Original search query
            response_time: Time taken for the search

        Returns:
            Tuple of (detailed_results, metadata)
        """
        detailed_results = []

        # Add knowledge graph as first item if present
        knowledge_graph = raw_response.get("knowledgeGraph")
        if knowledge_graph:
            detailed_results.append({
                "type": "knowledge_graph",
                "title": knowledge_graph.get("title", ""),
                "entity_type": knowledge_graph.get("type", ""),
                "website": knowledge_graph.get("website", ""),
                "description": knowledge_graph.get("description", ""),
                "description_source": knowledge_graph.get("descriptionSource", ""),
                "description_link": knowledge_graph.get("descriptionLink", ""),
                "image_url": knowledge_graph.get("imageUrl", ""),
                "attributes": knowledge_graph.get("attributes", {}),
            })

        # Extract organic results with sitelinks
        organic_results = raw_response.get("organic", [])
        for result in organic_results:
            page_result = {
                "type": "page",
                "title": result.get("title", ""),
                "url": result.get("link", ""),
                "content": result.get("snippet", ""),
                "position": result.get("position", 0),
                "date": result.get("date", ""),
            }

            sitelinks = result.get("sitelinks", [])
            if sitelinks:
                page_result["sitelinks"] = [
                    {"title": sl.get("title", ""), "url": sl.get("link", "")}
                    for sl in sitelinks
                ]

            detailed_results.append(page_result)

        # Add top stories (news) if present in general search
        top_stories = raw_response.get("topStories", [])
        for story in top_stories:
            detailed_results.append({
                "type": "news",
                "title": story.get("title", ""),
                "url": story.get("link", ""),
                "source": story.get("source", ""),
                "date": story.get("date", ""),
                "image_url": story.get("imageUrl", ""),
            })

        # Add people also ask (FAQ) results if present
        people_also_ask = raw_response.get("peopleAlsoAsk", [])
        for paa in people_also_ask:
            detailed_results.append({
                "type": "people_also_ask",
                "question": paa.get("question", ""),
                "snippet": paa.get("snippet", ""),
                "url": paa.get("link", ""),
            })

        # Add related searches at the end if present
        related_searches = raw_response.get("relatedSearches", [])
        if related_searches:
            detailed_results.append({
                "type": "related_searches",
                "queries": [rs.get("query", "") for rs in related_searches],
            })

        metadata = {
            "type": "web_search",
            "query": query,
            "search_type": "general",
            "search_engine": "serper",
            "response_time": round(response_time, 2),
            "total_results": len(detailed_results),
            "answer_box": raw_response.get("answerBox"),
            "knowledge_graph": knowledge_graph,
            "top_stories": [
                {
                    "title": s.get("title", ""),
                    "url": s.get("link", ""),
                    "source": s.get("source", ""),
                    "date": s.get("date", ""),
                    "image_url": s.get("imageUrl", ""),
                }
                for s in top_stories
            ],
            "related_searches": [
                rs.get("query", "") for rs in related_searches
            ],
            "results": [
                result_card(
                    title=r.get("title", ""),
                    url=r.get("link", ""),
                    favicon=_favicon_url(r.get("link", "")),
                    snippet=r.get("snippet", ""),
                )
                for r in organic_results
            ],
        }

        return detailed_results, metadata

    async def web_search(
        self,
        query: str,
        search_type: Literal["search", "news"] = "search",
        num: int = 10,
        time_range: Optional[str] = None,
        gl: str = "us",
        hl: str = "en",
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Execute web search and return formatted results.

        Args:
            query: Search query
            search_type: Type of search - "search" for general, "news" for news
            num: Number of results
            time_range: Time range filter (d, w, m, y)
            gl: Geographic location code
            hl: Language code

        Returns:
            Tuple of (detailed_results, metadata):
                - detailed_results: List of search result dicts for LLM
                - metadata: Search metadata for UI/frontend
        """
        start_time = time.time()
        raw_response = await self._make_request(
            query=query,
            search_type=search_type,
            num=num,
            time_range=time_range,
            gl=gl,
            hl=hl,
        )
        response_time = time.time() - start_time

        if search_type == "news":
            return self._format_news_results(raw_response, query, response_time)
        return self._format_general_results(raw_response, query, response_time)


def build_web_search_tool(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    verbose: bool = True,
    default_gl: str = "us",
    default_hl: str = "en",
):
    """Build a per-request Serper web_search tool.

    Each call returns a fresh tool whose settings live in closure scope, so
    concurrent requests with different settings can't race. The API wrapper is
    created lazily inside the tool call so a missing SERPER_API_KEY surfaces
    as a per-call error, not a build crash. ``verbose`` is accepted for the
    uniform builder interface; Serper results have no image variant.
    """
    _get_api_wrapper = lazy(SerperAPI)

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
            effective_time_range = normalize_time_range(
                time_range, default_time_range, provider="Serper"
            )
            gl = geographic_location or default_gl
            hl = language or default_hl

            logger.debug(
                f"Executing Serper search: query='{query}', "
                f"type={serper_type}, time_range={effective_time_range}, gl={gl}, hl={hl}"
            )

            detailed_results, metadata = await api.web_search(
                query=query,
                search_type=serper_type,
                num=max_results,
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

    return web_search
