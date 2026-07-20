"""Helpers shared by the web provider adapters and search builders.

The raw-httpx providers repeat the same moves: issue a JSON API call and map
its HTTP failure to the normalized taxonomy, poll an async research job until
terminal, unwrap a research report, normalize a time-range filter, and
assemble per-URL fetch results back into request order. Provider quirks stay
local and come in as ``overrides`` / format strings. Frontend result-card and
snippet/error-truncation shapes are shared where identical.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Tuple, TypeVar

import httpx

from src.tools.web.types import (
    FetchResponse,
    FetchResult,
    WebError,
    WebErrorType,
    WebToolError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Snippet/excerpt truncation for frontend result cards; error-message clipping.
SNIPPET_MAX = 300
ERROR_MESSAGE_MAX = 300


def clip_error(message: str) -> str:
    """Bound a provider error string before it enters a WebError message."""
    return message[:ERROR_MESSAGE_MAX]


def http_status_error(
    status: int, body: str, overrides: Optional[Mapping[int, "WebErrorType"]] = None
) -> WebError:
    """Map a provider-API HTTP failure to the taxonomy.

    401/403 mean our key was rejected, 402 that the provider account's
    budget is exhausted, 429 rate limiting; anything else is a generic
    provider error. ``overrides`` adds provider quirks (e.g. firecrawl
    408 -> timeout).

    Always ``retryable``: this is a whole-call failure against the
    *provider's* endpoint (our key/account/quota), not a verdict on the
    URL — so the router advances to the next provider. A revoked primary
    key therefore degrades to the fallbacks instead of failing the fetch.
    Per-URL forbidden/paywall statuses are built elsewhere and stay terminal.
    """
    if overrides and status in overrides:
        etype = overrides[status]
    elif status in (401, 403):
        etype = WebErrorType.FORBIDDEN
    elif status == 402:
        etype = WebErrorType.BUDGET_EXCEEDED
    elif status == 429:
        etype = WebErrorType.RATE_LIMITED
    else:
        etype = WebErrorType.PROVIDER_ERROR
    return WebError(type=etype, message=clip_error(body), http_status=status, retryable=True)


def transport_error(e: httpx.HTTPError) -> WebError:
    """Map an httpx transport failure; timeouts keep their taxonomy type."""
    if isinstance(e, httpx.TimeoutException):
        return WebError(type=WebErrorType.TIMEOUT, message=str(e) or "Request timed out")
    return WebError(type=WebErrorType.PROVIDER_ERROR, message=str(e))


def error_from_httpx(
    e: httpx.HTTPError, *, status_overrides: Optional[Mapping[int, "WebErrorType"]] = None
) -> WebError:
    """Normalize any httpx failure: status errors keep the response body and
    honor ``status_overrides``; other transport failures map by type."""
    if isinstance(e, httpx.HTTPStatusError):
        return http_status_error(e.response.status_code, e.response.text, status_overrides)
    return transport_error(e)


def missing_key_error(message: str) -> WebError:
    """Adapter built without its API key — retryable so the chain moves on."""
    return WebError(type=WebErrorType.PROVIDER_ERROR, message=message, retryable=True)


async def request_json(
    method: str,
    url: str,
    *,
    provider: str,
    headers: Optional[Mapping[str, str]] = None,
    json_body: Optional[Any] = None,
    params: Optional[Mapping[str, Any]] = None,
    timeout: float,
) -> Dict[str, Any]:
    """Issue one JSON API call and return the decoded body.

    A status >= 400 raises ``httpx.HTTPStatusError`` with the providers'
    shared ``"{Provider} API error {code}: {text}"`` message so callers can
    funnel it through ``error_from_httpx``; transport failures propagate as
    their native httpx exceptions.
    """
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(
            method, url, headers=headers, json=json_body, params=params
        )
        if response.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"{provider} API error {response.status_code}: {response.text}",
                request=response.request,
                response=response,
            )
        return response.json()


# Terminal states shared by the async research/task poll loops.
RESEARCH_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


async def poll_until_terminal(
    refresh: Callable[[], Awaitable[Tuple[str, Dict[str, Any]]]],
    *,
    status: str,
    data: Dict[str, Any],
    max_wait_seconds: float,
    poll_interval: float,
    provider: str,
) -> Tuple[str, Dict[str, Any]]:
    """Poll ``refresh`` until the run reaches a terminal state or the deadline.

    ``refresh`` returns the latest ``(status, data)``. Raises a TIMEOUT
    ``WebToolError`` once ``max_wait_seconds`` elapses; ``provider`` is the
    message prefix (e.g. ``"Tavily research <id>"``). The caller keeps any
    non-``completed`` terminal handling.
    """
    deadline = time.monotonic() + max_wait_seconds
    while status not in RESEARCH_TERMINAL_STATUSES:
        if time.monotonic() >= deadline:
            raise WebToolError(
                WebError(
                    type=WebErrorType.TIMEOUT,
                    message=f"{provider} still '{status}' after {max_wait_seconds:.0f}s",
                )
            )
        await asyncio.sleep(poll_interval)
        status, data = await refresh()
    return status, data


def report_or_empty(content: Any, provider: str) -> str:
    """Unwrap research ``content`` to a markdown report, raising EMPTY if blank.

    Strings pass through; anything else is JSON-encoded. Citation extraction
    stays per-provider; providers that need custom content unwrapping do it
    before calling here.
    """
    report = content if isinstance(content, str) else json.dumps(content or {})
    if not report or report == "{}":
        raise WebToolError(
            WebError(type=WebErrorType.EMPTY, message=f"{provider} research returned no output")
        )
    return report


# Time-range aliases → canonical single-letter form; canonical → day count.
_CANONICAL_RANGE = {
    "h": "h", "hour": "h",
    "d": "d", "day": "d",
    "w": "w", "week": "w",
    "m": "m", "month": "m",
    "y": "y", "year": "y",
}

TIME_RANGE_DAYS = {"h": 1 / 24, "d": 1, "w": 7, "m": 30, "y": 365}


def normalize_time_range(
    time_range: Optional[str], default: Optional[str] = None, *, provider: str = ""
) -> Optional[str]:
    """Canonicalize a time-range token to a single letter (h/d/w/m/y).

    Empty input returns ``default`` unchanged; an unknown non-empty value
    warns and falls back to ``default``.
    """
    if not time_range:
        return default
    canonical = _CANONICAL_RANGE.get(time_range.lower())
    if canonical is None:
        suffix = f" for {provider}" if provider else ""
        logger.warning(f"Invalid time_range '{time_range}'{suffix}, ignoring")
        return default
    return canonical


def time_range_to_days(time_range: Optional[str]) -> Optional[float]:
    """Map a time-range token (aliases accepted) to fractional days, None if unmappable."""
    canonical = _CANONICAL_RANGE.get((time_range or "").lower())
    return TIME_RANGE_DAYS.get(canonical) if canonical else None


def time_range_to_start_date(
    time_range: Optional[str], *, fmt: str, provider: str = ""
) -> Optional[str]:
    """Map a time-range token to a past start-date string via ``fmt``, None if unmappable.

    Shared arithmetic for the search providers; only the ``strftime`` format
    differs (Exa wants an ISO datetime, Parallel a bare date).
    """
    days = time_range_to_days(time_range)
    if days is None:
        if time_range:
            suffix = f" for {provider}" if provider else ""
            logger.warning(f"Invalid time_range '{time_range}'{suffix}, ignoring")
        return None
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime(fmt)


def is_empty_markdown(markdown: str) -> bool:
    """Whether scraped markdown is too short to count as real page content."""
    return len(markdown.strip()) < 10


def empty_content_error() -> WebError:
    """The EMPTY error for a page that returned no usable content."""
    return WebError(type=WebErrorType.EMPTY, message="Page returned empty content")


def result_card(
    *, title: str, url: str, favicon: Optional[str] = None, snippet: str = ""
) -> Dict[str, str]:
    """The frontend result-card dict for providers whose artifact card is
    exactly ``{title, url, favicon, snippet}``. Providers with extra keys
    (news source/date, publish_time/id) build their own."""
    return {"title": title, "url": url, "favicon": favicon or "", "snippet": snippet}


def error_response(urls: List[str], err: WebError, provider: str) -> FetchResponse:
    """One call-level error fanned out over every URL in the batch."""
    return FetchResponse(
        results=[FetchResult(url=u, error=err) for u in urls], provider=provider
    )


def assemble_results(
    urls: List[str], by_url: Dict[str, FetchResult], provider: str
) -> FetchResponse:
    """Order per-URL results by the request; URLs the provider never
    mentioned come back as EMPTY errors."""
    results = [
        by_url.get(
            u,
            FetchResult(
                url=u,
                error=WebError(type=WebErrorType.EMPTY, message="No content returned for URL"),
            ),
        )
        for u in urls
    ]
    return FetchResponse(results=results, provider=provider)


def lazy(factory: Callable[[], T]) -> Callable[[], T]:
    """Memoize a zero-arg factory. Search builders defer API-wrapper
    construction so a missing key surfaces per call, not at build time."""
    box: List[T] = []

    def get() -> T:
        if not box:
            box.append(factory())
        return box[0]

    return get
