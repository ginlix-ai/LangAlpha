"""Firecrawl provider: single-page scrape (/v2/scrape) fetch adapter and
site-crawl adapter (/v2/crawl async jobs + /v2/map URL discovery).

API docs: https://docs.firecrawl.dev — Bearer auth (FIRECRAWL_API_KEY).
Set FIRECRAWL_BASE_URL to target a self-hosted instance. The ``proxy``
native param ("auto" | "stealth") comes from the manifest level — the
"stealth" level is the router's anti-bot escalation target.
"""

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from src.tools.web.providers._shared import (
    clip_error,
    empty_content_error,
    error_from_httpx,
    error_response,
    is_empty_markdown,
    missing_key_error,
    request_json,
)
from src.tools.web.types import (
    CrawlJob,
    CrawlPage,
    CrawlRequest,
    CrawlState,
    CrawlStatus,
    FetchRequest,
    FetchResponse,
    FetchResult,
    MapRequest,
    UrlInfo,
    WebError,
    WebErrorType,
    WebToolError,
)

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://api.firecrawl.dev"
_TIMEOUT = 90.0  # stealth proxy scrapes can be slow
_MAX_STATUS_PAGES = 20  # crawl-status pagination hop cap (crawls are ≤100 pages)
# Firecrawl's maxAge defaults to 2 DAYS when omitted; bound provider-cache
# staleness to WebFetch's own Redis cache window instead.
_DEFAULT_MAX_AGE_SECONDS = 900


# Firecrawl returns 408 for scrape-engine timeouts.
_STATUS_OVERRIDES = {408: WebErrorType.TIMEOUT}


def _target_status_error(status: int) -> Optional[WebError]:
    """Map the *scraped page's* HTTP status (data.metadata.statusCode)."""
    if status in (404, 410):
        return WebError(type=WebErrorType.NOT_FOUND, message="Page not found", http_status=status)
    if status in (401, 402, 403):
        # Target rejected the scrape — retryable so the chain can escalate
        # to a stealth level or another provider.
        return WebError(
            type=WebErrorType.ANTI_BOT, message="Target blocked the scrape", http_status=status
        )
    if status == 429:
        return WebError(
            type=WebErrorType.RATE_LIMITED, message="Target rate-limited the scrape",
            http_status=status, provider_fault=False,
        )
    if status >= 500:
        return WebError(
            type=WebErrorType.PROVIDER_ERROR, message="Target server error",
            http_status=status, provider_fault=False,
        )
    return None


class FirecrawlAPI:
    """Raw-httpx Firecrawl client. Key is read lazily so a missing
    FIRECRAWL_API_KEY surfaces as a per-call error, not a build crash."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FIRECRAWL_API_KEY")
        if not self.api_key:
            raise ValueError("FIRECRAWL_API_KEY not found in environment variables")
        self.base_url = os.getenv("FIRECRAWL_BASE_URL", _DEFAULT_BASE_URL).rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def scrape(
        self,
        url: str,
        proxy: Optional[str] = None,
        max_age_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """POST /v2/scrape — one page as markdown."""
        payload: Dict[str, Any] = {
            "url": url,
            "formats": ["markdown"],
            "onlyMainContent": True,
        }
        if proxy:
            payload["proxy"] = proxy
        if max_age_seconds is None:
            max_age_seconds = _DEFAULT_MAX_AGE_SECONDS
        if max_age_seconds >= 0:
            payload["maxAge"] = max_age_seconds * 1000  # Firecrawl maxAge is ms
        return await self._request("POST", f"{self.base_url}/v2/scrape", json=payload)

    async def start_crawl(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v2/crawl — start an async crawl job; returns {id, url}."""
        return await self._request("POST", f"{self.base_url}/v2/crawl", json=payload)

    async def crawl_status(
        self, crawl_id: str, skip: int = 0, page_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """GET /v2/crawl/{id} — job status + a page window of results.

        ``page_url`` continues pagination from a response's ``next`` cursor
        (an absolute URL); otherwise ``skip`` positions the first window.
        """
        url = page_url or f"{self.base_url}/v2/crawl/{crawl_id}"
        params = {"skip": skip} if page_url is None and skip else None
        return await self._request("GET", url, params=params)

    async def cancel_crawl(self, crawl_id: str) -> Dict[str, Any]:
        """DELETE /v2/crawl/{id} — cancel a running job."""
        return await self._request("DELETE", f"{self.base_url}/v2/crawl/{crawl_id}")

    async def map_site(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v2/map — discover site URLs; returns {links: [{url, ...}]}."""
        return await self._request("POST", f"{self.base_url}/v2/map", json=payload)

    async def _request(self, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:
        return await request_json(
            method, url, provider="Firecrawl", headers=self.headers,
            json_body=kwargs.get("json"), params=kwargs.get("params"), timeout=_TIMEOUT,
        )


class FirecrawlFetchAdapter:
    """FetchAdapter over Firecrawl /v2/scrape (one URL per call)."""

    name = "firecrawl"

    async def _fetch_one(
        self, api: FirecrawlAPI, url: str, req: FetchRequest, proxy: Optional[str]
    ) -> FetchResult:
        try:
            data = await api.scrape(url, proxy=proxy, max_age_seconds=req.max_age_seconds)
        except httpx.HTTPError as e:
            return FetchResult(url=url, error=error_from_httpx(e, status_overrides=_STATUS_OVERRIDES))

        if not data.get("success", False):
            message = str(data.get("error") or "Firecrawl returned success=false")
            return FetchResult(
                url=url,
                error=WebError(type=WebErrorType.PROVIDER_ERROR, message=clip_error(message)),
            )

        page = data.get("data") or {}
        metadata = page.get("metadata") or {}
        target_status = metadata.get("statusCode")
        if isinstance(target_status, int):
            err = _target_status_error(target_status)
            if err is not None:
                return FetchResult(url=url, error=err)

        markdown = page.get("markdown") or ""
        if is_empty_markdown(markdown):
            return FetchResult(url=url, error=empty_content_error())
        return FetchResult(
            url=url,
            final_url=metadata.get("sourceURL") or url,
            title=metadata.get("title"),
            markdown=markdown,
        )

    async def fetch(self, req: FetchRequest, native_params: Dict[str, Any]) -> FetchResponse:
        try:
            api = FirecrawlAPI()
        except ValueError as e:
            return error_response(req.urls, missing_key_error(str(e)), self.name)
        proxy = native_params.get("proxy")
        results: List[FetchResult] = await asyncio.gather(
            *(self._fetch_one(api, u, req, proxy) for u in req.urls)
        )
        return FetchResponse(results=list(results), provider=self.name)


def _crawl_call_error(e: httpx.HTTPError) -> WebToolError:
    """Wrap an httpx failure as a call-level WebToolError (crawl status overrides)."""
    return WebToolError(error_from_httpx(e, status_overrides=_STATUS_OVERRIDES))


def _page_from_item(item: Dict[str, Any]) -> CrawlPage:
    """Normalize one crawl data[] item; per-page failures become CrawlPage.error."""
    metadata = item.get("metadata") or {}
    url = metadata.get("url") or metadata.get("sourceURL") or ""
    title = metadata.get("title")
    if isinstance(title, list):  # metadata.title may be string|array
        title = title[0] if title else None
    if isinstance(title, str):  # live pages embed stray whitespace/newlines
        title = title.strip() or None

    page_error = metadata.get("error")
    if page_error:
        return CrawlPage(
            url=url,
            title=title,
            error=WebError(type=WebErrorType.PROVIDER_ERROR, message=clip_error(str(page_error))),
        )
    target_status = metadata.get("statusCode")
    if isinstance(target_status, int):
        err = _target_status_error(target_status)
        if err is not None:
            return CrawlPage(url=url, title=title, error=err)

    markdown = item.get("markdown") or ""
    if is_empty_markdown(markdown):
        return CrawlPage(url=url, title=title, error=empty_content_error())
    return CrawlPage(url=url, title=title, markdown=markdown)


_CRAWL_STATES = {
    "scraping": CrawlState.RUNNING,
    "completed": CrawlState.COMPLETED,
    "failed": CrawlState.FAILED,
    "cancelled": CrawlState.CANCELLED,
}


class FirecrawlCrawlAdapter:
    """CrawlAdapter over Firecrawl /v2/crawl (async job) + /v2/map."""

    name = "firecrawl"

    async def start_crawl(self, req: CrawlRequest, native_params: Dict[str, Any]) -> CrawlJob:
        try:
            api = FirecrawlAPI()
        except ValueError as e:
            raise WebToolError(missing_key_error(str(e)))
        payload: Dict[str, Any] = {
            "url": req.url,
            "limit": req.limit,
            "scrapeOptions": {"formats": [{"type": "markdown"}], "onlyMainContent": True},
        }
        if req.max_depth is not None:
            payload["maxDiscoveryDepth"] = req.max_depth
        if req.include_paths:
            payload["includePaths"] = req.include_paths
        if req.exclude_paths:
            payload["excludePaths"] = req.exclude_paths
        if req.query:
            # Natural-language steer; explicit params above override anything
            # the prompt would generate (Firecrawl semantics).
            payload["prompt"] = req.query
        payload.update(native_params)

        try:
            data = await api.start_crawl(payload)
        except httpx.HTTPError as e:
            raise _crawl_call_error(e)
        job_id = data.get("id")
        if not data.get("success", False) or not job_id:
            message = str(data.get("error") or "Firecrawl did not return a crawl id")
            raise WebToolError(WebError(type=WebErrorType.PROVIDER_ERROR, message=clip_error(message)))
        return CrawlJob(id=job_id, provider=self.name)

    async def crawl_status(self, job_id: str, skip: int = 0) -> CrawlStatus:
        """Poll job progress; returns pages past ``skip``, following pagination."""
        try:
            api = FirecrawlAPI()
        except ValueError as e:
            raise WebToolError(missing_key_error(str(e)))
        pages: List[CrawlPage] = []
        try:
            data = await api.crawl_status(job_id, skip=skip)
            pages.extend(_page_from_item(item) for item in data.get("data") or [])
            # `next` is an absolute URL; present while more result pages exist.
            # Hop-bounded: a cursor that never clears must not pin the poll loop
            # past the crawl deadline (which is only checked between polls).
            next_url = data.get("next")
            hops = 0
            while next_url and hops < _MAX_STATUS_PAGES:
                chunk = await api.crawl_status(job_id, page_url=next_url)
                pages.extend(_page_from_item(item) for item in chunk.get("data") or [])
                next_url = chunk.get("next")
                hops += 1
            if next_url:
                logger.warning(
                    "Firecrawl crawl %s: status pagination truncated after %d pages",
                    job_id,
                    _MAX_STATUS_PAGES,
                )
        except httpx.HTTPError as e:
            raise _crawl_call_error(e)

        state = _CRAWL_STATES.get(data.get("status"), CrawlState.RUNNING)
        error = None
        if state is CrawlState.FAILED:
            error = WebError(
                type=WebErrorType.PROVIDER_ERROR,
                message=str(data.get("error") or "Crawl job failed"),
            )
        return CrawlStatus(
            state=state,
            total=data.get("total") or 0,
            completed=data.get("completed") or 0,
            pages=pages,
            error=error,
        )

    async def cancel_crawl(self, job_id: str) -> None:
        """Best-effort cancel — a terminal/expired job 404s; never raises."""
        try:
            api = FirecrawlAPI()
            await api.cancel_crawl(job_id)
        except (ValueError, httpx.HTTPError) as e:
            logger.warning("Firecrawl crawl %s cancel failed: %s", job_id, e)

    async def map_site(self, req: MapRequest, native_params: Dict[str, Any]) -> List[UrlInfo]:
        try:
            api = FirecrawlAPI()
        except ValueError as e:
            raise WebToolError(missing_key_error(str(e)))
        payload: Dict[str, Any] = {"url": req.url, "limit": req.limit}
        if req.query:
            payload["search"] = req.query
        payload.update(native_params)

        try:
            data = await api.map_site(payload)
        except httpx.HTTPError as e:
            raise _crawl_call_error(e)
        if not data.get("success", True):
            message = str(data.get("error") or "Firecrawl map returned success=false")
            raise WebToolError(WebError(type=WebErrorType.PROVIDER_ERROR, message=clip_error(message)))

        links: List[UrlInfo] = []
        for link in data.get("links") or []:
            if isinstance(link, str):  # tolerate legacy bare-string links
                links.append(UrlInfo(url=link))
            elif isinstance(link, dict) and link.get("url"):
                links.append(
                    UrlInfo(
                        url=link["url"],
                        title=link.get("title"),
                        description=link.get("description"),
                    )
                )
        return links
