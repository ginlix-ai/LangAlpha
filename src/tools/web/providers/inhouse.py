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

# SafeCrawlerWrapper CrawlResult.error_type → normalized taxonomy.
_ERROR_TYPES = {
    "blocked": WebErrorType.FORBIDDEN,
    "stealth_failed": WebErrorType.ANTI_BOT,
    "timeout": WebErrorType.TIMEOUT,
    "connection_timeout": WebErrorType.TIMEOUT,
    "rate_limited": WebErrorType.RATE_LIMITED,
    "circuit_open": WebErrorType.CIRCUIT_OPEN,
    "queue_full": WebErrorType.PROVIDER_ERROR,
    "empty_content": WebErrorType.EMPTY,
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
            error = WebError(
                type=WebErrorType.PROVIDER_ERROR,
                message="Crawl was cancelled",
                retryable=False,
            )
        else:
            error = WebError(
                type=_ERROR_TYPES.get(kind, WebErrorType.PROVIDER_ERROR),
                message=str(result.error or kind)[:300],
            )
        return FetchResult(url=url, error=error)

    async def fetch(self, req: FetchRequest, native_params: Dict[str, Any]) -> FetchResponse:
        from src.tools.web.inhouse.safe_wrapper import get_safe_crawler

        crawler = await get_safe_crawler()
        results: List[FetchResult] = await asyncio.gather(
            *(self._fetch_one(crawler, u) for u in req.urls)
        )
        return FetchResponse(results=list(results), provider=self.name)
