"""Bocha provider: AI Search optimized for Chinese-language queries and
Chinese market content.

Auth via ``Authorization: Bearer`` (BOCHA_API_KEY).
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import httpx
from langchain_core.tools import tool

from src.tools.utils.validation_utils import validate_date_format
from src.tools.web.providers._shared import lazy, normalize_time_range, request_json

logger = logging.getLogger(__name__)

# Canonical time-range letter → Bocha freshness value. Bocha has no sub-day
# bucket, so an hour range maps to its finest granularity ("oneDay").
_FRESHNESS_BY_RANGE = {
    "h": "oneDay",
    "d": "oneDay",
    "w": "oneWeek",
    "m": "oneMonth",
    "y": "oneYear",
}


def _translate_time_to_freshness(
    time_range: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> Optional[str]:
    """Translate Tavily-style time parameters to Bocha's freshness format.

    Priority: date range (start_date/end_date) > time_range > None (noLimit).
    Relative ranges are canonicalized via the shared normalizer; an unknown
    value falls back to "noLimit".
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

    # Priority 2: Relative time range → Bocha freshness bucket
    if time_range:
        canonical = normalize_time_range(time_range, None, provider="Bocha")
        if canonical is None:
            return "noLimit"
        if canonical == "h":
            logger.debug("Bocha: hour range mapped to finest granularity 'oneDay'")
        return _FRESHNESS_BY_RANGE[canonical]

    # Priority 3: No time filtering (default)
    return "noLimit"


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
        "type": "web_search",
        "query": raw_data.get("query", ""),
        "search_engine": "bocha",
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


class BochaAPI:
    """Bocha API wrapper class providing web search functionality."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Bocha API client.

        Reads API key from BOCHA_API_KEY environment variable if not provided.

        Args:
            api_key: Optional API key. If not provided, reads from environment.
        """
        self.api_key = api_key or os.getenv('BOCHA_API_KEY')
        if not self.api_key:
            raise ValueError("BOCHA_API_KEY not found in environment variables")
        self.base_url = "https://api.bochaai.com/v1"
        self.ai_search_endpoint = f"{self.base_url}/ai-search"
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        logger.debug("Bocha API client initialized (using AI Search endpoint).")

    async def _make_request(self, query: str, count: int = 10, freshness: Optional[str] = None, answer: bool = False) -> dict:
        """
        Send AI Search request to Bocha API (POST).

        Args:
            query: Search query
            count: Number of results to return
            freshness: Time range filter parameter
            answer: Whether to request LLM-generated answer (default False)

        Returns:
            API response as JSON dictionary
        """
        payload_dict = {
            "query": query,
            "count": count,
            "answer": answer,
            "stream": False
        }

        # Add freshness parameter if provided and not "noLimit"
        if freshness and freshness != "noLimit":
            payload_dict["freshness"] = freshness

        logger.debug(f"Bocha AI Search POST request URL: {self.ai_search_endpoint}")
        logger.debug(f"Bocha AI Search POST request body: {payload_dict}")

        try:
            json_response = await request_json(
                "POST", self.ai_search_endpoint, provider="Bocha",
                headers=self.headers, json_body=payload_dict, timeout=30.0,
            )

            if json_response.get("code") != 200:
                error_msg = f"Bocha API returned business error: Code={json_response.get('code')}, Msg={json_response.get('msg')}"
                logger.error(error_msg)
                return {"error": error_msg}

            logger.debug("Bocha AI Search request successful.")
            return json_response

        except httpx.HTTPStatusError as e:
            logger.error(f"Bocha API HTTP error: {e.response.status_code} - {e.response.text}", exc_info=True)
            return {"error": f"HTTP error {e.response.status_code}: {e.response.text}"}
        except httpx.TimeoutException as e:
            logger.error(f"Bocha API request timeout: {e}", exc_info=True)
            return {"error": f"Request timeout: {e}"}
        except httpx.RequestError as e:
            logger.error(f"Bocha API request failed: {e}", exc_info=True)
            return {"error": str(e)}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Bocha API response JSON: {e}", exc_info=True)
            return {"error": f"Unable to parse API response: {e}"}

    async def web_search(self, query: str, count: int = 10, freshness: Optional[str] = None, answer: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Execute AI Search and return detailed results (with artifact support).

        Args:
            query: Search query
            count: Number of results to return
            freshness: Time range filter parameter
                - "noLimit": No time limit (default)
                - "oneDay": Within one day
                - "oneWeek": Within one week
                - "oneMonth": Within one month
                - "oneYear": Within one year
                - "YYYY-MM-DD..YYYY-MM-DD": Date range
                - "YYYY-MM-DD": Specific date
            answer: Whether to request LLM-generated answer (default False)

        Returns:
            Tuple[List[Dict[str, Any]], Dict[str, Any]]:
            - First element: Detailed results list with all fields (for building content and artifact)
            - Second element: Raw response metadata (query, response_time, conversation_id, etc.)
        """
        logger.info(f"Executing Bocha AI search: {query[:50]}... (freshness={freshness}, answer={answer})")

        # Track response time
        start_time = time.time()
        data = await self._make_request(query, count, freshness, answer)
        response_time = time.time() - start_time

        detailed_results_list = []
        image_results_list = []

        if "error" in data:
            logger.warning(f"Bocha AI search returned error: {data['error']}")
            return detailed_results_list, {
                "query": query,
                "response_time": response_time,
                "error": data["error"]
            }

        try:
            messages = data.get("messages", [])

            # Parse webpage results from messages[0]
            webpage_msg = next((m for m in messages if m.get("content_type") == "webpage"), None)
            if webpage_msg:
                webpage_content = json.loads(webpage_msg["content"])
                results = webpage_content.get("value", [])

                for item in results:
                    detailed_results_list.append({
                        "type": "webpage",
                        "title": item.get('name', ''),
                        "url": item.get('url', ''),
                        "summary": item.get('summary', ''),
                        "snippet": item.get('snippet', ''),
                        "site_name": item.get('siteName', ''),
                        "site_icon": item.get('siteIcon', ''),
                        "publish_time": item.get('datePublished', ''),
                        "id": item.get('id', '')
                    })

            # Parse image results from messages[1]
            image_msg = next((m for m in messages if m.get("content_type") == "image"), None)
            if image_msg:
                try:
                    image_content = json.loads(image_msg["content"])
                    images = image_content.get("value", [])

                    for img in images:
                        image_results_list.append({
                            "type": "image",
                            "content_url": img.get('contentUrl', ''),
                            "thumbnail_url": img.get('thumbnailUrl', ''),
                            "host_page_url": img.get('hostPageUrl', ''),
                            "host_page_display_url": img.get('hostPageDisplayUrl', ''),
                            "width": img.get('width', 0),
                            "height": img.get('height', 0)
                        })
                except (json.JSONDecodeError, KeyError) as e:
                    logger.debug(f"Failed to parse image results: {e}")

            # Combine webpage and image results
            all_results = detailed_results_list + image_results_list

            result_count = len(all_results)
            logger.info(f"Bocha AI search successful, returned {len(detailed_results_list)} webpages and {len(image_results_list)} images.")

            # Return results and metadata
            metadata = {
                "query": query,
                "response_time": round(response_time, 2),
                "total_results": result_count,
                "conversation_id": data.get("conversation_id", ""),
                "log_id": data.get("log_id", "")
            }

            return all_results, metadata

        except (AttributeError, KeyError, json.JSONDecodeError) as e:
            logger.error(f"Error parsing Bocha AI Search response structure: {e}", exc_info=True)
            logger.debug(f"Raw messages field: {data.get('messages')}")
            return detailed_results_list, {
                "query": query,
                "response_time": response_time,
                "error": str(e)
            }


def build_web_search_tool(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    verbose: bool = True,
):
    """Build a per-request Bocha web_search tool.

    Each call returns a fresh tool whose settings live in closure scope, so
    concurrent requests with different settings can't race. The API wrapper is
    created lazily inside the tool call so a missing BOCHA_API_KEY surfaces as
    a per-call error, not a build crash.

    Args:
        max_results: Maximum number of search results to return.
        default_time_range: Default time range filter (d/w/m/y or day/week/month/year).
            Used as fallback if LLM doesn't specify time_range in query.
        verbose: Control verbosity of search results.
            True (default): Include images in results.
            False: Exclude images, return webpage results only (lightweight for planning).
    """
    _get_api_wrapper = lazy(BochaAPI)

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
            validate_date_format(start_date)
            validate_date_format(end_date)

            # Apply default time_range if LLM didn't specify one
            effective_time_range = time_range or default_time_range
            if effective_time_range != time_range:
                logger.debug(
                    f"Using default time_range: {effective_time_range} "
                    f"(LLM: {time_range}, default: {default_time_range})"
                )

            # Translate time parameters to Bocha's freshness format
            freshness = _translate_time_to_freshness(effective_time_range, start_date, end_date)

            # Execute search via BochaAPI
            api = _get_api_wrapper()
            api_results, metadata = await api.web_search(
                query=query,
                count=max_results,
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
                elif item.get("type") == "image" and verbose:
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
            artifact = _filter_artifact_for_frontend(raw_data, verbose)

            logger.info(f"Bocha AI search completed: {len(content)} items for query '{query[:50]}...'")
            logger.debug(f"Content structure: {len([c for c in content if c.get('type')=='page'])} pages, "
                        f"{len([c for c in content if c.get('type')=='image'])} images")

            return content, artifact

        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"Bocha AI search failed: {e}", exc_info=True)
            error_message = f"Search failed: {str(e)}"
            return error_message, {"error": str(e), "query": query}

    return web_search
