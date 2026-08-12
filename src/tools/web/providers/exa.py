"""Exa provider: semantic web search (/search) and batch fetch (/contents).

API docs: https://docs.exa.ai — auth via ``x-api-key`` (EXA_API_KEY).
"""

import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import httpx
from langchain_core.tools import tool

from src.tools.web.providers._shared import (
    SNIPPET_MAX,
    assemble_results,
    empty_content_error,
    error_from_httpx,
    error_response,
    lazy,
    missing_key_error,
    report_or_empty,
    request_json,
    result_card,
    time_range_to_start_date,
)
from src.tools.web.types import (
    FetchRequest,
    FetchResponse,
    FetchResult,
    ResearchCitation,
    ResearchRequest,
    ResearchResult,
    SearchResult,
    WebError,
    WebErrorType,
    WebToolError,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.exa.ai"
_TIMEOUT = 30.0
# Deep search runs the provider's agentic loop synchronously (deep-reasoning
# takes up to ~50s per docs); give the single HTTP call generous headroom.
_RESEARCH_TIMEOUT = 180.0

# Exa /contents statuses[].error.tag → normalized taxonomy.
_CONTENTS_ERROR_TAGS = {
    "CRAWL_NOT_FOUND": WebErrorType.NOT_FOUND,
    "CRAWL_TIMEOUT": WebErrorType.TIMEOUT,
    "CRAWL_LIVECRAWL_TIMEOUT": WebErrorType.TIMEOUT,
    "SOURCE_NOT_AVAILABLE": WebErrorType.FORBIDDEN,
    "CRAWL_UNKNOWN_ERROR": WebErrorType.PROVIDER_ERROR,
}

class ExaAPI:
    """Raw-httpx Exa client. Key is read lazily so a missing EXA_API_KEY
    surfaces as a per-call error, not a build crash."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("EXA_API_KEY")
        if not self.api_key:
            raise ValueError("EXA_API_KEY not found in environment variables")
        self.headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}

    async def _post(
        self, path: str, payload: Dict[str, Any], timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        return await request_json(
            "POST", f"{_BASE_URL}{path}", provider="Exa",
            headers=self.headers, json_body=payload, timeout=timeout or _TIMEOUT,
        )

    async def search(
        self,
        query: str,
        search_type: str = "auto",
        num_results: int = 10,
        category: Optional[str] = None,
        start_published_date: Optional[str] = None,
        end_published_date: Optional[str] = None,
        include_domains: Optional[List[str]] = None,
    ) -> Tuple[List[SearchResult], Dict[str, Any]]:
        """POST /search with relevance-ranked highlights as result content."""
        payload: Dict[str, Any] = {
            "query": query,
            "type": search_type,
            "numResults": num_results,
            "contents": {
                "highlights": {"numSentences": 3, "highlightsPerUrl": 3},
            },
        }
        if category:
            payload["category"] = category
        if start_published_date:
            payload["startPublishedDate"] = start_published_date
        if end_published_date:
            payload["endPublishedDate"] = end_published_date
        if include_domains:
            payload["includeDomains"] = include_domains

        start_time = time.time()
        data = await self._post("/search", payload)
        response_time = time.time() - start_time

        results = [
            SearchResult(
                title=item.get("title") or "",
                url=item.get("url", ""),
                excerpts=tuple(item.get("highlights") or []),
                content="\n\n".join(item.get("highlights") or []) or (item.get("text") or "")[:1500],
                published_date=item.get("publishedDate"),
                author=item.get("author"),
                favicon=item.get("favicon"),
                score=item.get("score"),
            )
            for item in data.get("results", [])
        ]

        metadata = {
            "type": "web_search",
            "query": query,
            "search_type": data.get("resolvedSearchType") or search_type,
            "search_engine": "exa",
            "response_time": round(response_time, 2),
            "total_results": len(results),
            "results": [
                result_card(
                    title=r.title,
                    url=r.url,
                    favicon=r.favicon,
                    snippet=(r.excerpts[0] if r.excerpts else r.content)[:SNIPPET_MAX],
                )
                for r in results
            ],
            "cost_dollars": data.get("costDollars"),
        }
        return results, metadata

    async def contents(
        self,
        urls: List[str],
        objective: Optional[str] = None,
        mode: str = "excerpts",
        max_age_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """POST /contents — batch page content with per-URL error statuses."""
        payload: Dict[str, Any] = {"urls": urls}
        if mode in ("full", "both"):
            payload["text"] = True
        if objective:
            payload["highlights"] = {
                "numSentences": 5,
                "highlightsPerUrl": 5,
                "query": objective,
            }
            payload["summary"] = {"query": objective}
        elif mode == "excerpts":
            payload["highlights"] = {"numSentences": 5, "highlightsPerUrl": 5}
        if max_age_seconds == 0:
            payload["livecrawl"] = "always"
        elif max_age_seconds == -1:
            payload["livecrawl"] = "never"
        return await self._post("/contents", payload)

    async def research(
        self,
        query: str,
        search_type: str = "deep",
        output_schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """POST /search with a deep type — a synchronous research run.

        Exa only synthesizes (``output.content`` + ``output.grounding``) when
        an outputSchema is present — without one, deep search returns plain
        results. A default single-field report schema fills that gap.
        """
        payload: Dict[str, Any] = {
            "query": query,
            "type": search_type,
            "outputSchema": output_schema or _DEFAULT_REPORT_SCHEMA,
        }
        return await self._post("/search", payload, timeout=_RESEARCH_TIMEOUT)


# Applied when the caller passes no output_schema: a single markdown report
# field, unwrapped back to a plain string by the adapter.
_DEFAULT_REPORT_SCHEMA = {
    "type": "object",
    "properties": {
        "report": {
            "type": "string",
            "description": "Comprehensive markdown report answering the query, "
            "with specific figures, dates, and sources.",
        }
    },
    "required": ["report"],
}


class ExaResearchAdapter:
    """ResearchAdapter over Exa deep search (sync; type from native_params)."""

    name = "exa"

    async def research(
        self, req: ResearchRequest, native_params: Dict[str, Any]
    ) -> ResearchResult:
        try:
            api = ExaAPI()
        except ValueError as e:
            raise WebToolError(missing_key_error(str(e)))

        start_time = time.time()
        try:
            data = await api.research(
                query=req.query,
                search_type=native_params.get("search_type", "deep"),
                output_schema=req.output_schema,
            )
        except httpx.HTTPError as e:
            raise WebToolError(error_from_httpx(e))
        response_time = time.time() - start_time

        output = data.get("output") or {}
        content = output.get("content")
        # Without a caller schema Exa returns a {"report": ...} object; pull the
        # field out before the shared string/JSON unwrap.
        if req.output_schema is None and isinstance(content, dict):
            content = content.get("report") or ""
        report = report_or_empty(content, "Exa")

        # grounding[] carries per-field citations with confidence; fall back
        # to the plain results[] source list when it's absent.
        citations = []
        for grounding_item in output.get("grounding") or []:
            confidence = grounding_item.get("confidence")
            for cite in grounding_item.get("citations") or []:
                if cite.get("url"):
                    citations.append(
                        ResearchCitation(
                            url=cite["url"],
                            title=cite.get("title"),
                            confidence=confidence,
                        )
                    )
        if not citations:
            citations = [
                ResearchCitation(
                    url=item.get("url", ""),
                    title=item.get("title"),
                    excerpts=tuple(item.get("highlights") or []),
                )
                for item in data.get("results", [])
                if item.get("url")
            ]

        return ResearchResult(
            provider=self.name,
            report=report,
            citations=citations,
            request_id=data.get("requestId"),
            model=data.get("resolvedSearchType") or native_params.get("search_type"),
            response_time=round(response_time, 2),
            usage={
                "cost_dollars": data.get("costDollars"),
                "num_searches": data.get("numSearches"),
            },
        )


class ExaFetchAdapter:
    """FetchAdapter over Exa /contents (batch ≤ 100)."""

    name = "exa"

    async def fetch(self, req: FetchRequest, native_params: Dict[str, Any]) -> FetchResponse:
        try:
            api = ExaAPI()
        except ValueError as e:
            return error_response(req.urls, missing_key_error(str(e)), self.name)
        try:
            data = await api.contents(
                urls=req.urls,
                objective=req.objective,
                mode=req.mode,
                max_age_seconds=req.max_age_seconds,
            )
        except httpx.HTTPError as e:
            return error_response(req.urls, error_from_httpx(e), self.name)

        by_url: Dict[str, FetchResult] = {}
        for item in data.get("results", []):
            url = item.get("id") or item.get("url", "")
            if not (
                (item.get("text") or "").strip()
                or item.get("highlights")
                or item.get("summary")
            ):
                by_url[url] = FetchResult(url=url, error=empty_content_error())
                continue
            by_url[url] = FetchResult(
                url=url,
                final_url=item.get("url"),
                title=item.get("title"),
                markdown=item.get("text"),
                excerpts=tuple(item.get("highlights") or []),
                summary=item.get("summary"),
                published_date=item.get("publishedDate"),
                author=item.get("author"),
            )
        for status in data.get("statuses", []):
            url = status.get("id", "")
            if status.get("status") == "error" and url not in by_url:
                tag = (status.get("error") or {}).get("tag", "")
                etype = _CONTENTS_ERROR_TAGS.get(tag, WebErrorType.PROVIDER_ERROR)
                by_url[url] = FetchResult(
                    url=url,
                    error=WebError(
                        type=etype,
                        message=f"Exa contents error: {tag or 'unknown'}",
                        http_status=(status.get("error") or {}).get("httpStatusCode"),
                    ),
                )

        return assemble_results(req.urls, by_url, self.name)


def build_web_search_tool(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    verbose: bool = True,
    search_type: str = "auto",
):
    """Build a per-request Exa web_search tool.

    Fresh tool per call: settings live in closure scope so concurrent
    requests with different settings can't race. ``search_type`` comes from
    the manifest level's native_params. ``verbose`` is accepted for the
    uniform builder interface; Exa results have no image variant.
    """
    _get_api_wrapper = lazy(ExaAPI)

    @tool(response_format="content_and_artifact")
    async def web_search(
        query: str,
        category: Optional[
            Literal["company", "research paper", "financial report", "news", "people"]
        ] = None,
        start_published_date: Optional[str] = None,
        end_published_date: Optional[str] = None,
        include_domains: Optional[List[str]] = None,
    ) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
        """Search the web with Exa's meaning-based (semantic) search engine.

        Queries work best phrased as natural-language descriptions of what
        you want to find, not just keywords. Use when you need to:
        - Find recent news, filings, or research on a topic
        - Discover companies, papers, or people matching a description
        - Research topics beyond your knowledge cutoff

        Args:
            query: What you want to find, as a natural-language description
            category: Restrict results to one content type: 'company',
                'research paper', 'financial report', 'news', or 'people'
            start_published_date: Only results published on/after this date (YYYY-MM-DD)
            end_published_date: Only results published on/before this date (YYYY-MM-DD)
            include_domains: Restrict results to these domains (e.g. ['sec.gov'])
        """
        try:
            api = _get_api_wrapper()
            start = start_published_date or time_range_to_start_date(
                default_time_range, fmt="%Y-%m-%dT%H:%M:%S.000Z", provider="Exa"
            )
            if start and len(start) == 10:  # bare YYYY-MM-DD → ISO datetime
                start = f"{start}T00:00:00.000Z"
            end = end_published_date
            if end and len(end) == 10:
                end = f"{end}T23:59:59.999Z"

            logger.debug(
                f"Executing Exa search: query='{query}', type={search_type}, "
                f"category={category}, start={start}, end={end}"
            )
            results, metadata = await api.search(
                query=query,
                search_type=search_type,
                num_results=max_results,
                category=category,
                start_published_date=start,
                end_published_date=end,
                include_domains=include_domains,
            )
            return [r.as_dict() for r in results], metadata

        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            body = e.response.text if e.response is not None else "no response body"
            logger.error(f"Exa API HTTP {status}: {body}")
            error_message = f"Search failed (HTTP {status}): {body}"
            return error_message, {"error": error_message, "query": query}
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"Exa search failed: {e}", exc_info=True)
            error_message = f"Search failed: {str(e)}"
            return error_message, {"error": str(e), "query": query}

    return web_search
