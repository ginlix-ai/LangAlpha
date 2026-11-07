"""DeepSeek provider: web search (/v1/tools/search) and page fetch (/v1/tools/fetch).

API docs: https://api-docs.deepseek.com/guides/search/ — bearer auth with the
same key as DeepSeek's models (DEEPSEEK_API_KEY). Both endpoints bill only a
call that returns something: an empty search or a failed fetch is free.
"""

import asyncio
import logging
import os
import re
import time
from dataclasses import replace
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from langchain_core.tools import tool

from src.tools.web.providers._shared import (
    SNIPPET_MAX,
    empty_content_error,
    error_from_httpx,
    error_response,
    is_empty_markdown,
    lazy,
    missing_key_error,
    request_json,
    result_card,
    search_failed,
    search_refusal,
    time_range_to_start_date,
)
from src.tools.web.types import (
    FetchRequest,
    FetchResponse,
    FetchResult,
    SearchResult,
    WebError,
    WebErrorType,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.deepseek.com/v1/tools"
_SEARCH_TIMEOUT = 30.0
_FETCH_TIMEOUT = 60.0

# The API refuses a query or question longer than this many characters.
_QUERY_MAX = 128
# The API caps count here.
_COUNT_MAX = 50

_DATE_OPERATOR = re.compile(r"(?:^|\s)(?:after|before):", re.IGNORECASE)

# /fetch is one URL per call, so its 4xx statuses are about that URL, not our
# account: 400 is a URL it will not take, 404 a page it got nothing usable
# from (dead, blocked by the site, an unsupported type), 504 the site being
# slow. The in-house crawler may still get them, so each stays retryable, and
# none counts against the breaker. 401/402/5xx keep the shared mapping.
_FETCH_STATUS_OVERRIDES = {
    400: WebErrorType.UNSUPPORTED_URL,
    404: WebErrorType.EMPTY,
    504: WebErrorType.TIMEOUT,
}


def _fetch_error(e: httpx.HTTPError) -> WebError:
    """Classify a failed /fetch call.

    A fetch 429 is DeepSeek's per-domain limit as often as the account's, and
    its body does not say which, so it never counts against the breaker: one
    busy site must not shut DeepSeek off for every URL. The URL still falls
    back in-house, and an account limit clears within the second.
    """
    error = error_from_httpx(e, status_overrides=_FETCH_STATUS_OVERRIDES)
    if error.http_status == 429:
        return replace(error, provider_fault=False)
    return error


def _with_default_date(query: str, default_time_range: Optional[str]) -> str:
    """Add the deployment's default range as an ``after:`` operator.

    The API has no date field, so the range rides in the query. A query that
    names its own date keeps it, and one the operator would push past the
    length cap goes unfiltered rather than refused.
    """
    if _DATE_OPERATOR.search(query):
        return query
    start = time_range_to_start_date(default_time_range, fmt="%Y-%m-%d", provider="DeepSeek")
    with_date = f"{query} after:{start}" if start else query
    return with_date if len(with_date) <= _QUERY_MAX else query


class DeepSeekAPI:
    """Raw-httpx DeepSeek tools client. Key is read lazily so a missing
    DEEPSEEK_API_KEY surfaces as a per-call error, not a build crash."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY not found in environment variables")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def _post(self, path: str, payload: Dict[str, Any], timeout: float) -> Dict[str, Any]:
        return await request_json(
            "POST", f"{_BASE_URL}{path}", provider="DeepSeek",
            headers=self.headers, json_body=payload, timeout=timeout,
        )

    async def search(
        self, query: str, count: int = 10, question: Optional[str] = None
    ) -> Tuple[List[SearchResult], Dict[str, Any]]:
        """POST /search; each hit's model-facing ``summary`` is its content."""
        payload: Dict[str, Any] = {"query": query, "count": max(1, min(count, _COUNT_MAX))}
        if question:
            payload["ori_question"] = question

        start_time = time.time()
        data = await self._post("/search", payload, _SEARCH_TIMEOUT)
        response_time = time.time() - start_time

        items = data.get("results") or []
        results = [
            SearchResult(
                title=item.get("title") or "",
                url=item.get("url") or "",
                content=item.get("summary") or item.get("snippet") or "",
                published_date=item.get("date"),
                score=item.get("rerank_score"),
            )
            for item in items
        ]

        metadata = {
            "type": "web_search",
            "query": query,
            "search_engine": "deepseek",
            "response_time": round(response_time, 2),
            "total_results": len(results),
            # The cards show DeepSeek's display snippet; the model reads the summary.
            "results": [
                result_card(
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    favicon=item.get("site_icon"),
                    snippet=(item.get("snippet") or item.get("summary") or "")[:SNIPPET_MAX],
                )
                for item in items
            ],
        }
        return results, metadata

    async def fetch(self, url: str) -> Dict[str, Any]:
        """POST /fetch — one page as markdown."""
        return await self._post("/fetch", {"url": url}, _FETCH_TIMEOUT)


class DeepSeekFetchAdapter:
    """FetchAdapter over DeepSeek /fetch (one URL per call)."""

    name = "deepseek"

    async def _fetch_one(self, api: DeepSeekAPI, url: str) -> FetchResult:
        try:
            data = await api.fetch(url)
        except httpx.HTTPError as e:
            return FetchResult(url=url, error=_fetch_error(e))
        markdown = data.get("content") or ""
        if is_empty_markdown(markdown):
            return FetchResult(url=url, error=empty_content_error())
        return FetchResult(url=url, title=data.get("title") or None, markdown=markdown)

    async def fetch(self, req: FetchRequest, native_params: Dict[str, Any]) -> FetchResponse:
        try:
            api = DeepSeekAPI()
        except ValueError as e:
            return error_response(req.urls, missing_key_error(str(e)), self.name)
        results: List[FetchResult] = await asyncio.gather(
            *(self._fetch_one(api, u) for u in req.urls)
        )
        return FetchResponse(results=list(results), provider=self.name)


def build_web_search_tool(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    verbose: bool = True,
):
    """Build a per-request DeepSeek web_search tool.

    Fresh tool per call: settings live in closure scope so concurrent
    requests with different settings can't race. ``verbose`` is accepted for
    the uniform builder interface; DeepSeek results have no image variant.
    """
    _get_api_wrapper = lazy(DeepSeekAPI)

    @tool(response_format="content_and_artifact")
    async def web_search(
        query: str,
        question: Optional[str] = None,
    ) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
        """Search the web with DeepSeek's search engine.

        Use when you need to:
        - Find recent news, filings, or facts on a topic
        - Research topics beyond your knowledge cutoff

        query and question are each refused past 128 characters.

        Args:
            query: Keywords. Accepts operators, which combine: site:sec.gov
                (several joined with OR), -site:x.com, "exact phrase", -word,
                lang:en,zh, after:2025-01-01, before:2025-06-30
            question: The full question the search serves; improves ranking

        Returns:
            Ranked results, each with ~700 characters of its page's best text.
        """
        if not query.strip() or len(query) > _QUERY_MAX or len(question or "") > _QUERY_MAX:
            # The API refuses these, and that is the agent's mistake, not a failed search.
            return search_refusal(
                ValueError(f"query must be 1 to {_QUERY_MAX} characters, question at most {_QUERY_MAX}"),
                query,
            )

        try:
            api = _get_api_wrapper()
            effective_query = _with_default_date(query, default_time_range)
            logger.debug(f"Executing DeepSeek search: query='{effective_query}'")
            results, metadata = await api.search(
                query=effective_query, count=max_results, question=question
            )
            return [r.as_dict() for r in results], metadata

        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"DeepSeek search failed: {e}", exc_info=True)
            return search_failed(e, query, quote_http_body=True)

    return web_search
