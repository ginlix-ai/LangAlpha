"""
Web crawling tool using SafeCrawlerWrapper.

Provides async web crawling that returns raw markdown content.
For LLM-based extraction, use web_fetch_tool instead.
"""

import logging
from typing import Annotated

from langchain_core.tools import StructuredTool

from .decorators import log_io
from .crawler.safe_wrapper import get_safe_crawler_sync

logger = logging.getLogger(__name__)


async def _crawl_impl(
    url: Annotated[str, "The url to crawl."],
) -> str:
    """
    Use this to crawl a url and get readable content in markdown format.

    Supports multiple content types with dedicated extractors:
    - PDF files: text extraction via pdfplumber
    - YouTube videos: transcript extraction with timestamps
    - X/Twitter posts: tweet text, media, and engagement stats

    For regular web pages, uses a tiered fetching strategy:
    - Tier 1: Fast HTTP fetch with TLS impersonation
    - Tier 2: Browser-based fetch for JS-rendered pages
    - Tier 3: Stealth fetch for anti-bot protected pages

    Protected by a circuit breaker for fault tolerance.
    Returns full content without truncation for comprehensive analysis.
    """
    try:
        safe_crawler = get_safe_crawler_sync()
        result = await safe_crawler.crawl(url)
        if not result.success:
            kind = result.error_type or "error"
            return f"[{kind}] Failed to crawl {url}: {result.error}"
        if not result.markdown or len(result.markdown.strip()) < 50:
            return f"[empty] Failed to crawl {url}: page returned empty or blocked content (possibly anti-bot protected or paywalled)"
        return {"url": url, "crawled_content": result.markdown}
    except BaseException as e:
        error_msg = f"Failed to crawl. Error: {repr(e)}"
        logger.error(error_msg)
        return error_msg


# Apply decorator and create async tool
_decorated_impl = log_io(_crawl_impl)

crawl_tool = StructuredTool.from_function(
    coroutine=_decorated_impl,
    name="crawl",
    description="""Use this to crawl a url and get readable content in markdown format.

Supports multiple content types with dedicated extractors:
- PDF files: text extraction via pdfplumber
- YouTube videos: transcript extraction with timestamps
- X/Twitter posts: tweet text, media, and engagement stats

For regular web pages, uses tiered fetching with automatic fallback:
- Fast HTTP fetch for static pages
- Browser-based fetch for JS-rendered pages
- Stealth fetch for anti-bot protected pages

Protected by a circuit breaker for fault tolerance.
Returns full content without truncation for comprehensive analysis.""",
)
