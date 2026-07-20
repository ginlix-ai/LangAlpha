"""Abstract crawler backend protocol."""

from dataclasses import dataclass
from typing import Literal, Optional, Protocol


FailureKind = Literal[
    "blocked",          # 401/451 — host permanently rejects us (403 falls through to Tier 2/3 because some Cloudflare configs return 403 to curl_cffi but 200 to Camoufox)
    "stealth_failed",   # Tier 3 reached but came back with no usable content
    "rate_limited",     # 429
    "infra_error",      # browser crash, DNS, conn refused — our crawler is broken
]


@dataclass
class CrawlOutput:
    """Raw output from a crawler backend."""

    title: str
    html: str
    markdown: str
    status: Optional[int] = None
    failure_kind: Optional[FailureKind] = None


class CrawlerBackend(Protocol):
    """Protocol for pluggable crawler backends."""

    async def crawl(self, url: str) -> str:
        """Crawl a URL and return markdown content."""
        ...

    async def crawl_with_metadata(self, url: str) -> CrawlOutput:
        """Crawl a URL and return full metadata."""
        ...

    async def shutdown(self) -> None:
        """Gracefully release resources."""
        ...
