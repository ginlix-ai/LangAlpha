"""Parallel provider: objective-guided web search (/v1/search), batch
extract (/v1/extract), and deep research via the Task API (/v1/tasks/runs).

API docs: https://docs.parallel.ai — auth via ``x-api-key`` (PARALLEL_API_KEY).
Search/extract return relevance-ranked markdown excerpts per URL.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from langchain_core.tools import tool

from src.tools.web.providers._shared import (
    SNIPPET_MAX,
    assemble_results,
    clip_error,
    empty_content_error,
    error_from_httpx,
    error_response,
    lazy,
    missing_key_error,
    poll_until_terminal,
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

_BASE_URL = "https://api.parallel.ai"
_TIMEOUT = 60.0

# Cap on total excerpt characters for a search call, spread across results.
_SEARCH_MAX_CHARS_TOTAL = 12000

# /v1/extract errors[].error_type → normalized taxonomy. HTTP-status-bearing
# errors are refined by _refine_extract_error below.
_EXTRACT_ERROR_TYPES = {
    "fetch_error": WebErrorType.PROVIDER_ERROR,
    "timeout": WebErrorType.TIMEOUT,
    "unsupported_url": WebErrorType.UNSUPPORTED_URL,
}


def _refine_extract_error(error_type: str, http_status: Optional[int]) -> WebErrorType:
    if http_status == 404:
        return WebErrorType.NOT_FOUND
    if http_status in (401, 402, 403):
        return WebErrorType.PAYWALL
    if http_status == 429:
        return WebErrorType.RATE_LIMITED
    return _EXTRACT_ERROR_TYPES.get(error_type, WebErrorType.PROVIDER_ERROR)


class ParallelAPI:
    """Raw-httpx Parallel client. Key is read lazily so a missing
    PARALLEL_API_KEY surfaces as a per-call error, not a build crash."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("PARALLEL_API_KEY")
        if not self.api_key:
            raise ValueError("PARALLEL_API_KEY not found in environment variables")
        self.headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}

    async def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await request_json(
            "POST", f"{_BASE_URL}{path}", provider="Parallel",
            headers=self.headers, json_body=payload, timeout=_TIMEOUT,
        )

    async def search(
        self,
        search_queries: List[str],
        objective: Optional[str] = None,
        mode: Optional[str] = None,
        max_results: int = 10,
        include_domains: Optional[List[str]] = None,
        exclude_domains: Optional[List[str]] = None,
        after_date: Optional[str] = None,
    ) -> Tuple[List[SearchResult], Dict[str, Any]]:
        """POST /v1/search — results ordered by decreasing relevance."""
        advanced: Dict[str, Any] = {"max_results": max_results}
        source_policy: Dict[str, Any] = {}
        if include_domains:
            source_policy["include_domains"] = include_domains
        if exclude_domains:
            source_policy["exclude_domains"] = exclude_domains
        if after_date:
            source_policy["after_date"] = after_date
        if source_policy:
            advanced["source_policy"] = source_policy

        payload: Dict[str, Any] = {
            "search_queries": search_queries,
            "max_chars_total": _SEARCH_MAX_CHARS_TOTAL,
            "advanced_settings": advanced,
        }
        if objective:
            payload["objective"] = objective
        if mode:
            payload["mode"] = mode

        start_time = time.time()
        data = await self._post("/v1/search", payload)
        response_time = time.time() - start_time

        results = [
            SearchResult(
                title=item.get("title") or "",
                url=item.get("url", ""),
                excerpts=tuple(item.get("excerpts") or []),
                published_date=item.get("publish_date"),
            )
            for item in data.get("results", [])
        ]

        metadata = {
            "type": "web_search",
            "query": objective or "; ".join(search_queries),
            "search_type": mode or "default",
            "search_engine": "parallel",
            "response_time": round(response_time, 2),
            "total_results": len(results),
            "results": [
                result_card(
                    title=r.title,
                    url=r.url,
                    snippet=(r.excerpts[0] if r.excerpts else "")[:SNIPPET_MAX],
                )
                for r in results
            ],
            "usage": data.get("usage"),
            "search_id": data.get("search_id"),
        }
        return results, metadata

    async def extract(
        self,
        urls: List[str],
        objective: Optional[str] = None,
        mode: str = "excerpts",
        max_age_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """POST /v1/extract — batch ≤ 20 URLs, per-URL errors[]."""
        advanced: Dict[str, Any] = {}
        if mode in ("full", "both"):
            advanced["full_content"] = True
        # Parallel's fetch_policy floor is 600s; 0 means "force fresh" which
        # maps to the minimum allowed cache age.
        if max_age_seconds is not None and max_age_seconds >= 0:
            advanced["fetch_policy"] = {"max_age_seconds": max(max_age_seconds, 600)}
        payload: Dict[str, Any] = {"urls": urls}
        if objective:
            payload["objective"] = objective
        if advanced:
            payload["advanced_settings"] = advanced
        return await self._post("/v1/extract", payload)

    async def _get(self, path: str) -> Dict[str, Any]:
        return await request_json(
            "GET", f"{_BASE_URL}{path}", provider="Parallel",
            headers=self.headers, timeout=_TIMEOUT,
        )

    async def create_task_run(
        self,
        input_text: str,
        processor: str,
        output_schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """POST /v1/tasks/runs — start an async research run, returns run_id.

        Without a caller schema the task_spec requests plain text so
        ``output.content`` comes back as a markdown string, not an
        auto-wrapped JSON object.
        """
        if output_schema:
            spec: Dict[str, Any] = {"type": "json", "json_schema": output_schema}
        else:
            spec = {
                "type": "text",
                "description": "Comprehensive markdown report answering the "
                "input, with specific figures, dates, and sources.",
            }
        payload: Dict[str, Any] = {
            "input": input_text,
            "processor": processor,
            "task_spec": {"output_schema": spec},
        }
        return await self._post("/v1/tasks/runs", payload)

    async def get_task_run(self, run_id: str) -> Dict[str, Any]:
        """GET /v1/tasks/runs/{id} — run status."""
        return await self._get(f"/v1/tasks/runs/{run_id}")

    async def get_task_result(self, run_id: str) -> Dict[str, Any]:
        """GET /v1/tasks/runs/{id}/result — output + basis for a completed run."""
        return await self._get(f"/v1/tasks/runs/{run_id}/result")


class ParallelResearchAdapter:
    """ResearchAdapter over the Parallel Task API (async; processor from
    native_params). Polls the run until terminal, then reads /result."""

    name = "parallel"

    async def research(
        self, req: ResearchRequest, native_params: Dict[str, Any]
    ) -> ResearchResult:
        try:
            api = ParallelAPI()
        except ValueError as e:
            raise WebToolError(missing_key_error(str(e)))

        processor = native_params.get("processor", "base")
        start_time = time.time()
        try:
            run = await api.create_task_run(
                input_text=req.query,
                processor=processor,
                output_schema=req.output_schema,
            )
            run_id = run.get("run_id")
            if not run_id:
                raise WebToolError(
                    WebError(
                        type=WebErrorType.PROVIDER_ERROR,
                        message=f"Parallel task run returned no run_id: {clip_error(str(run))}",
                    )
                )

            async def refresh() -> Tuple[str, Dict[str, Any]]:
                r = await api.get_task_run(run_id)
                return r.get("status", ""), r

            status, run = await poll_until_terminal(
                refresh,
                status=run.get("status", "queued"),
                data=run,
                max_wait_seconds=req.max_wait_seconds,
                poll_interval=req.poll_interval,
                provider=f"Parallel task run {run_id}",
            )

            if status != "completed":
                raise WebToolError(
                    WebError(
                        type=WebErrorType.PROVIDER_ERROR,
                        message=f"Parallel task run {run_id} ended '{status}': "
                        f"{clip_error(str(run.get('error') or ''))}",
                    )
                )
            data = await api.get_task_result(run_id)
        except httpx.HTTPError as e:
            raise WebToolError(error_from_httpx(e))
        response_time = time.time() - start_time

        output = data.get("output") or {}
        content = output.get("content")
        # Auto-generated task specs wrap text in {"output": "..."}; unwrap.
        if isinstance(content, dict) and len(content) == 1 and isinstance(content.get("output"), str):
            content = content["output"]
        report = report_or_empty(content, "Parallel")

        # basis[] carries per-field citations with excerpts and confidence.
        citations = []
        for basis_item in output.get("basis") or []:
            confidence = basis_item.get("confidence")
            for cite in basis_item.get("citations") or []:
                if cite.get("url"):
                    citations.append(
                        ResearchCitation(
                            url=cite["url"],
                            title=cite.get("title"),
                            excerpts=tuple(cite.get("excerpts") or []),
                            confidence=confidence,
                        )
                    )

        run_info = data.get("run") or run
        return ResearchResult(
            provider=self.name,
            report=report,
            citations=citations,
            request_id=run_info.get("run_id"),
            model=run_info.get("processor") or processor,
            response_time=round(response_time, 2),
            usage={"processor": run_info.get("processor") or processor},
        )


class ParallelFetchAdapter:
    """FetchAdapter over Parallel /v1/extract (batch ≤ 20)."""

    name = "parallel"

    async def fetch(self, req: FetchRequest, native_params: Dict[str, Any]) -> FetchResponse:
        try:
            api = ParallelAPI()
        except ValueError as e:
            return error_response(req.urls, missing_key_error(str(e)), self.name)
        try:
            data = await api.extract(
                urls=req.urls,
                objective=req.objective,
                mode=req.mode,
                max_age_seconds=req.max_age_seconds,
            )
        except httpx.HTTPError as e:
            return error_response(req.urls, error_from_httpx(e), self.name)

        by_url: Dict[str, FetchResult] = {}
        for item in data.get("results", []):
            url = item.get("url", "")
            if not ((item.get("full_content") or "").strip() or item.get("excerpts")):
                by_url[url] = FetchResult(url=url, error=empty_content_error())
                continue
            by_url[url] = FetchResult(
                url=url,
                title=item.get("title"),
                markdown=item.get("full_content"),
                excerpts=tuple(item.get("excerpts") or []),
                published_date=item.get("publish_date"),
            )
        for err_item in data.get("errors", []):
            url = err_item.get("url", "")
            if url not in by_url:
                error_type = err_item.get("error_type", "")
                http_status = err_item.get("http_status_code")
                by_url[url] = FetchResult(
                    url=url,
                    error=WebError(
                        type=_refine_extract_error(error_type, http_status),
                        message=f"Parallel extract error: {error_type or 'unknown'}",
                        http_status=http_status,
                    ),
                )

        return assemble_results(req.urls, by_url, self.name)


def build_web_search_tool(
    max_results: int = 10,
    default_time_range: Optional[str] = None,
    verbose: bool = True,
    mode: Optional[str] = None,
):
    """Build a per-request Parallel web_search tool.

    Fresh tool per call: settings live in closure scope so concurrent
    requests with different settings can't race. ``mode`` (turbo/basic/
    advanced) comes from the manifest level's native_params. ``verbose`` is
    accepted for the uniform builder interface; no image variant exists.
    """
    _get_api_wrapper = lazy(ParallelAPI)

    @tool(response_format="content_and_artifact")
    async def web_search(
        objective: str,
        search_queries: Optional[List[str]] = None,
        include_domains: Optional[List[str]] = None,
        after_date: Optional[str] = None,
    ) -> Tuple[Union[List[Dict[str, Any]], str], Dict[str, Any]]:
        """Search the web with Parallel's objective-guided search engine.

        Results come back as relevance-ranked markdown excerpts per URL —
        often enough signal to answer without fetching pages. Use when you
        need to:
        - Find recent news, facts, or filings on a topic
        - Answer a question from current web content
        - Research topics beyond your knowledge cutoff

        Args:
            objective: Natural-language description of the question or goal
                driving the search. Make it self-contained with enough context.
            search_queries: Optional 2-3 concise keyword queries (3-6 words
                each) to supplement the objective
            include_domains: Restrict results to these domains (e.g. ['sec.gov'])
            after_date: Only content published on/after this date (YYYY-MM-DD)
        """
        try:
            api = _get_api_wrapper()
            effective_after = after_date or time_range_to_start_date(
                default_time_range, fmt="%Y-%m-%d", provider="Parallel"
            )
            queries = search_queries or [objective[:200]]

            logger.debug(
                f"Executing Parallel search: objective='{objective[:80]}', "
                f"queries={queries}, mode={mode}, after={effective_after}"
            )
            results, metadata = await api.search(
                search_queries=queries,
                objective=objective,
                mode=mode,
                max_results=max_results,
                include_domains=include_domains,
                after_date=effective_after,
            )
            return [r.as_dict() for r in results], metadata

        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            body = e.response.text if e.response is not None else "no response body"
            logger.error(f"Parallel API HTTP {status}: {body}")
            error_message = f"Search failed (HTTP {status}): {body}"
            return error_message, {"error": error_message, "query": objective}
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"Parallel search failed: {e}", exc_info=True)
            error_message = f"Search failed: {str(e)}"
            return error_message, {"error": str(e), "query": objective}

    return web_search
