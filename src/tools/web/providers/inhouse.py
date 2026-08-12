"""In-house fetch adapter over SafeCrawlerWrapper.

The zero-key default and terminal chain entry: OSS deployments work with no
vendor keys, and it keeps the YouTube-transcript / X-post / PDF extractors
no third party replicates. Returns raw page markdown (llm extraction policy).
"""

import asyncio
import logging
from typing import Any, Dict, List

from src.tools.web.types import (
    FetchRequest,
    FetchResponse,
    FetchResult,
    WebError,
    WebErrorType,
)

logger = logging.getLogger(__name__)

# SafeCrawlerWrapper CrawlResult.error_type → (normalized type, provider_fault).
#
# provider_fault is stated for every outcome instead of falling back to the
# error type's default: it means "this failure is independent of the URL we
# asked for", so the chain should stop routing here entirely. A host that
# blocks, rate-limits, times out or refuses the connection is a property of
# that host — it must not open a breaker shared by every URL.
_ERROR_TYPES = {
    # Target-side.
    "blocked": (WebErrorType.FORBIDDEN, False),
    "stealth_failed": (WebErrorType.ANTI_BOT, False),
    "timeout": (WebErrorType.TIMEOUT, False),
    "connection_timeout": (WebErrorType.TIMEOUT, False),
    "rate_limited": (WebErrorType.RATE_LIMITED, False),
    "circuit_open": (WebErrorType.CIRCUIT_OPEN, False),
    "empty_content": (WebErrorType.EMPTY, False),
    "dns_error": (WebErrorType.PROVIDER_ERROR, False),
    "connection_refused": (WebErrorType.PROVIDER_ERROR, False),
    "network_error": (WebErrorType.PROVIDER_ERROR, False),
    "crawl_error": (WebErrorType.PROVIDER_ERROR, False),
    # Despite the name, its only producer is the Tier 1 unreachable
    # short-circuit (scrapling_crawler.py) — "could not resolve" and
    # "connection refused" are the target's problem, not ours.
    "infra_error": (WebErrorType.PROVIDER_ERROR, False),
    # Our own capacity/infrastructure — any other URL would fail the same way.
    "queue_full": (WebErrorType.PROVIDER_ERROR, True),
    "browser_closed": (WebErrorType.PROVIDER_ERROR, True),
}


class InhouseFetchAdapter:
    """FetchAdapter over the in-house tiered crawler (scrapling)."""

    name = "inhouse"

    async def _fetch_one(self, crawler, url: str) -> FetchResult:
        result = await crawler.crawl(url)
        if result.success:
            return FetchResult(url=url, title=result.title, markdown=result.markdown)
        kind = result.error_type or "crawl_error"
        if kind == "cancelled":
            # Not a failure at all — the caller stopped. Neither retry it on
            # the next provider nor count it against this one.
            return FetchResult(
                url=url,
                error=WebError(
                    type=WebErrorType.PROVIDER_ERROR,
                    message="Crawl was cancelled",
                    retryable=False,
                    provider_fault=False,
                    native_kind=kind,
                ),
            )
        mapped = _ERROR_TYPES.get(kind)
        if mapped is None:
            # A crawler outcome added without a mapping here. Default to
            # target-side so it cannot silently open the provider breaker.
            logger.warning("inhouse fetch: unmapped crawler error_type %r", kind)
            mapped = (WebErrorType.PROVIDER_ERROR, False)
        error_type, provider_fault = mapped
        return FetchResult(
            url=url,
            error=WebError(
                type=error_type,
                message=str(result.error or kind)[:300],
                provider_fault=provider_fault,
                native_kind=kind,
            ),
        )

    async def fetch(self, req: FetchRequest, native_params: Dict[str, Any]) -> FetchResponse:
        from src.tools.web.inhouse.safe_wrapper import get_safe_crawler

        crawler = await get_safe_crawler()
        results: List[FetchResult] = await asyncio.gather(
            *(self._fetch_one(crawler, u) for u in req.urls)
        )
        return FetchResponse(results=list(results), provider=self.name)
