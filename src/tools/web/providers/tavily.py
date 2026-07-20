"""Tavily provider: web search (official SDK), fetch (/extract, batch ≤ 20),
and deep research (/research, async poll).

Search goes through AsyncTavilyClient; fetch returns raw page markdown only —
no objective support, so extraction happens through the llm policy. All read
TAVILY_API_KEY.
"""

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import httpx
from langchain_core.tools import tool
from tavily import AsyncTavilyClient

from src.tools.utils.validation_utils import validate_date_format, validate_image_url
from src.tools.web.providers._shared import (
    SNIPPET_MAX,
    assemble_results,
    clip_error,
    empty_content_error,
    error_from_httpx,
    error_response,
    is_empty_markdown,
    lazy,
    missing_key_error,
    poll_until_terminal,
    report_or_empty,
    request_json,
)
from src.tools.web.types import (
    FetchRequest,
    FetchResponse,
    FetchResult,
    ResearchCitation,
    ResearchRequest,
    ResearchResult,
    WebError,
    WebErrorType,
    WebToolError,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.tavily.com"
_TIMEOUT = 60.0


def _filter_artifact_for_frontend(raw_results: Dict) -> Dict:
    """Remove duplicated content fields from artifact.

    The artifact is sent to frontend alongside cleaned_results.
    Since cleaned_results already contains content/raw_content/score,
    we remove these fields from the artifact to avoid duplication —
    keeping a truncated ``snippet`` so result cards match the other
    providers' artifact contract (title/url/favicon/snippet).

    Args:
        raw_results: Complete API response from Tavily

    Returns:
        Filtered artifact with content fields removed from results array
    """
    filtered = raw_results.copy()
    filtered["type"] = "web_search"
    filtered["search_engine"] = "tavily"

    if "results" in filtered:
        filtered["results"] = [
            {
                **{
                    k: v
                    for k, v in result.items()
                    if k not in ("content", "raw_content", "score")
                },
                "snippet": (result.get("content") or "")[:SNIPPET_MAX],
            }
            for result in filtered["results"]
        ]

    return filtered


class TavilySearchWrapper:
    """Tavily search API wrapper using official AsyncTavilyClient SDK."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        country: Optional[str] = None,
    ):
        self._api_key = api_key or os.environ.get("TAVILY_API_KEY", "")
        if not self._api_key:
            raise ValueError("TAVILY_API_KEY not provided or found in environment")
        self._client = AsyncTavilyClient(api_key=self._api_key)
        self._country = country

    async def raw_results(
        self,
        query: str,
        max_results: Optional[int] = 5,
        search_depth: Optional[str] = "advanced",
        include_domains: Optional[List[str]] = None,
        exclude_domains: Optional[List[str]] = None,
        include_answer: Optional[bool] = False,
        include_raw_content: Optional[bool] = False,
        include_images: Optional[bool] = False,
        include_image_descriptions: Optional[bool] = False,
        include_favicon: Optional[bool] = False,
        time_range: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        topic: Optional[str] = None,
        country: Optional[str] = None,
    ) -> Dict:
        """Get results from the Tavily Search API"""
        # Determine country: method param > instance config
        effective_country = country or self._country

        # Build search kwargs
        kwargs = {
            "query": query,
            "max_results": max_results,
            "search_depth": search_depth,
            "include_domains": include_domains or [],
            "exclude_domains": exclude_domains or [],
            "include_answer": include_answer,
            "include_raw_content": include_raw_content,
            "include_images": include_images,
            "include_image_descriptions": include_image_descriptions,
        }

        # Optional parameters
        if include_favicon:
            kwargs["include_favicon"] = include_favicon
        if time_range:
            kwargs["time_range"] = time_range
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        if topic:
            kwargs["topic"] = topic

        # Country only valid for topic="general" (or no topic)
        if effective_country:
            if topic is None or topic == "general":
                kwargs["country"] = effective_country
            else:
                logger.warning(
                    f"country='{effective_country}' ignored: only valid for topic='general', "
                    f"but topic='{topic}' was specified"
                )

        return await self._client.search(**kwargs)

    async def clean_results_with_images(
        self, raw_results: Dict[str, List[Dict]]
    ) -> List[Dict]:
        """Clean results from Tavily Search API with async image validation.

        Uses lenient validation (HEAD request) to filter out inaccessible images
        for frontend display. This is different from strict validation used for
        OpenAI Vision API.

        Args:
            raw_results: Raw results from Tavily API

        Returns:
            List of cleaned results with only accessible images
        """
        results = raw_results.get("results") or []
        clean_results = []

        # Process page results (no validation needed). Tolerate missing fields:
        # one malformed row must not discard the whole (already billed) search.
        for result in results:
            clean_result = {
                "type": "page",
                "title": result.get("title") or "",
                "url": result.get("url") or "",
                "content": result.get("content") or "",
                "score": result.get("score") or 0.0,
            }
            if raw_content := result.get("raw_content"):
                clean_result["raw_content"] = raw_content
            clean_results.append(clean_result)

        # Process images with concurrent lenient validation
        images = raw_results.get("images", [])
        if not images:
            return clean_results

        logger.debug(f"Validating {len(images)} image(s) from Tavily (lenient mode for frontend)")

        async def validate_single_image(image) -> Optional[Dict]:
            """Helper to validate a single image and return cleaned result.

            Handles both string URLs and dict format for robustness:
            - String format: When include_image_descriptions=False (URL only)
            - Dict format: When include_image_descriptions=True (URL + description)
            """
            # Handle both string URLs and dict format
            if isinstance(image, str):
                url = image
                description = ""
            elif isinstance(image, dict):
                url = image.get("url", "")
                description = image.get("description", "")
            else:
                logger.warning(f"Unexpected image format: {type(image).__name__}, skipping")
                return None

            if not url:
                return None

            # Use lenient validation (HEAD request) with longer timeout for frontend display
            validated_url = await validate_image_url(url, timeout=10, strict=False)

            if validated_url:
                return {
                    "type": "image",
                    "image_url": validated_url,  # Use validated URL (possibly upgraded to HTTPS)
                    "image_description": description,
                }
            else:
                logger.debug(f"Skipping inaccessible image URL: {url}")
                return None

        # Run all validations concurrently
        validation_results = await asyncio.gather(
            *[validate_single_image(img) for img in images],
            return_exceptions=True
        )

        # Filter out None values and exceptions
        valid_images = []
        for result in validation_results:
            if result and not isinstance(result, Exception):
                valid_images.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Image validation error: {result}")

        # Add validated images to results
        clean_results.extend(valid_images)

        logger.debug(f"Image validation: {len(valid_images)}/{len(images)} images accessible")

        return clean_results


class TavilyFetchAdapter:
    """FetchAdapter over Tavily POST /extract."""

    name = "tavily"

    async def fetch(self, req: FetchRequest, native_params: Dict[str, Any]) -> FetchResponse:
        api_key = os.getenv("TAVILY_API_KEY")
        if not api_key:
            err = missing_key_error("TAVILY_API_KEY not found in environment variables")
            return error_response(req.urls, err, self.name)

        payload = {
            "urls": req.urls,
            "extract_depth": native_params.get("extract_depth", "basic"),
            "format": "markdown",
        }
        try:
            data = await request_json(
                "POST",
                f"{_BASE_URL}/extract",
                provider="Tavily",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json_body=payload,
                timeout=_TIMEOUT,
            )
        except httpx.HTTPError as e:
            return error_response(req.urls, error_from_httpx(e), self.name)

        by_url: Dict[str, FetchResult] = {}
        for item in data.get("results", []):
            url = item.get("url", "")
            markdown = item.get("raw_content") or ""
            if is_empty_markdown(markdown):
                by_url[url] = FetchResult(url=url, error=empty_content_error())
            else:
                by_url[url] = FetchResult(url=url, markdown=markdown)
        for failed in data.get("failed_results", []):
            url = failed.get("url", "")
            if url not in by_url:
                by_url[url] = FetchResult(
                    url=url,
                    error=WebError(
                        type=WebErrorType.PROVIDER_ERROR,
                        message=clip_error(str(failed.get("error") or "extract failed")),
                    ),
                )

        return assemble_results(req.urls, by_url, self.name)


class TavilyResearchAdapter:
    """ResearchAdapter over Tavily POST /research (async; model from
    native_params). Polls GET /research/{request_id} until terminal."""

    name = "tavily"

    async def research(
        self, req: ResearchRequest, native_params: Dict[str, Any]
    ) -> ResearchResult:
        api_key = os.getenv("TAVILY_API_KEY")
        if not api_key:
            raise WebToolError(
                missing_key_error("TAVILY_API_KEY not found in environment variables")
            )
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload: Dict[str, Any] = {
            "input": req.query,
            "model": native_params.get("model", "mini"),
        }
        if req.output_schema:
            payload["output_schema"] = req.output_schema

        start_time = time.time()
        try:
            data = await request_json(
                "POST",
                f"{_BASE_URL}/research",
                provider="Tavily",
                headers=headers,
                json_body=payload,
                timeout=_TIMEOUT,
            )
            request_id = data.get("request_id")
            if not request_id:
                raise WebToolError(
                    WebError(
                        type=WebErrorType.PROVIDER_ERROR,
                        message=f"Tavily research returned no request_id: "
                        f"{clip_error(str(data))}",
                    )
                )

            async def refresh() -> Tuple[str, Dict[str, Any]]:
                poll = await request_json(
                    "GET",
                    f"{_BASE_URL}/research/{request_id}",
                    provider="Tavily",
                    headers=headers,
                    timeout=_TIMEOUT,
                )
                return poll.get("status", ""), poll

            status, data = await poll_until_terminal(
                refresh,
                status=data.get("status", "pending"),
                data=data,
                max_wait_seconds=req.max_wait_seconds,
                poll_interval=req.poll_interval,
                provider=f"Tavily research {request_id}",
            )
        except httpx.HTTPError as e:
            raise WebToolError(error_from_httpx(e))
        response_time = time.time() - start_time

        if status != "completed":
            raise WebToolError(
                WebError(
                    type=WebErrorType.PROVIDER_ERROR,
                    message=f"Tavily research {request_id} ended '{status}': "
                    f"{clip_error(str(data.get('error') or ''))}",
                )
            )

        content = data.get("output") or data.get("content") or data.get("answer")
        report = report_or_empty(content, "Tavily")

        citations = [
            ResearchCitation(
                url=src.get("url", ""),
                title=src.get("title"),
            )
            for src in data.get("sources") or data.get("citations") or []
            if isinstance(src, dict) and src.get("url")
        ]

        return ResearchResult(
            provider=self.name,
            report=report,
            citations=citations,
            request_id=request_id,
            model=data.get("model") or payload["model"],
            response_time=round(response_time, 2),
            usage=data.get("usage"),
        )


def build_web_search_tool(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    verbose: bool = True,
    search_depth: str = "basic",
    include_domains: Optional[List[str]] = None,
    exclude_domains: Optional[List[str]] = None,
    include_answer: bool = False,
    include_favicon: bool = True,
    country: Optional[str] = None,
):
    """Build a per-request Tavily web_search tool.

    Each call returns a fresh tool whose settings live in closure scope, so
    concurrent requests with different settings (e.g. per-user search depth)
    can't race. The API wrapper is created lazily inside the tool call so a
    missing TAVILY_API_KEY surfaces as a per-call error, not a build crash.

    Args:
        max_results: Maximum number of search results to return.
        default_time_range: Default time range filter (d/w/m/y or day/week/month/year).
            Used as fallback if LLM doesn't specify time_range in query.
        verbose: Control verbosity of search results.
            True (default): Include images (raw_content always disabled).
            False: Text-only results without images (lightweight for planning).
        search_depth: Tavily search depth - "ultra-fast", "fast", "basic"
            (default), or "advanced".
        include_domains: List of domains to include in search.
        exclude_domains: List of domains to exclude from search.
        include_answer: Whether to include Tavily's answer in results.
        include_favicon: Whether to include favicon URLs in artifact.
        country: Country for localized results (lowercase, e.g., "united states").
            Only valid for topic="general". Examples: "china", "japan", "germany".
    """
    include_domains = include_domains or []
    exclude_domains = exclude_domains or []
    _get_api_wrapper = lazy(lambda: TavilySearchWrapper(country=country))

    @tool(response_format="content_and_artifact")
    async def web_search(
        query: str,
        time_range: Optional[Literal["day", "week", "month", "year", "d", "w", "m", "y"]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        topic: Optional[Literal["general", "news", "finance"]] = "general",
    ) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
        """Search the web for current information, news, and facts.

        Use when you need to:
        - Find recent news or current events
        - Look up facts, statistics, or real-time data
        - Research topics beyond your knowledge cutoff
        - Verify or update information

        Args:
            query: Search query to look up
            time_range: Filter by recency - 'day'/'d' (24h), 'week'/'w' (7d),
                'month'/'m' (30d), 'year'/'y' (365d).
                Ignored if start_date or end_date is provided.
            start_date: Start date for results (YYYY-MM-DD format).
                Takes priority over time_range.
            end_date: End date for results (YYYY-MM-DD format).
                Takes priority over time_range.
            topic: Search topic - 'general' (default), 'news', or 'finance'
        """
        try:
            # Validate date formats
            validate_date_format(start_date)
            validate_date_format(end_date)

            # Prioritization logic: dates > LLM time_range > default_time_range
            effective_time_range = None
            effective_start_date = None
            effective_end_date = None

            if start_date or end_date:
                # Use dates if provided (highest priority)
                effective_start_date = start_date
                effective_end_date = end_date
                logger.debug(
                    f"Using date range: start_date={start_date}, end_date={end_date} "
                    f"(ignoring time_range={time_range}, default={default_time_range})"
                )
            else:
                # Use LLM-provided time_range, or fall back to default
                effective_time_range = time_range or default_time_range
                logger.debug(
                    f"Using time_range: {effective_time_range} "
                    f"(LLM: {time_range}, default: {default_time_range})"
                )

            # Verbosity control: determine what to include based on verbose
            # Always disable raw_content to reduce response size
            include_raw_content = False
            if verbose:
                include_images = True
                include_image_descriptions = True
                logger.debug("Verbose mode: including images (raw_content disabled)")
            else:
                include_images = False
                include_image_descriptions = False
                logger.debug("Lightweight mode: text-only results")

            api = _get_api_wrapper()
            raw_results = await api.raw_results(
                query,
                max_results,
                search_depth,
                include_domains,
                exclude_domains,
                include_answer,
                include_raw_content,
                include_images,
                include_image_descriptions,
                include_favicon=include_favicon,
                time_range=effective_time_range,
                start_date=effective_start_date,
                end_date=effective_end_date,
                topic=topic,
            )

            cleaned_results = await api.clean_results_with_images(raw_results)
            logger.debug(f"Tavily search completed: {len(cleaned_results)} results")

            # Filter artifact to remove duplicated content fields
            filtered_artifact = _filter_artifact_for_frontend(raw_results)

            return cleaned_results, filtered_artifact

        except Exception as e:  # broad: the Tavily SDK raises its own hierarchy
            logger.error(f"Tavily search failed: {e}", exc_info=True)
            error_message = f"Search failed: {str(e)}"
            return error_message, {"error": str(e), "query": query}

    return web_search
