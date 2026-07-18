"""Content extractor base class and registry."""

import ipaddress
import logging
import re
from abc import ABC, abstractmethod
from urllib.parse import urlparse

import httpx

from ..backend import CrawlOutput

logger = logging.getLogger(__name__)


class ExtractorError(Exception):
    """Raised when an extractor encounters an unrecoverable error."""


def _validate_url(url: str) -> None:
    """Validate URL scheme and block private/localhost targets."""
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ExtractorError(f"Unsupported URL scheme: {parsed.scheme!r}")

    hostname = parsed.hostname or ""

    # Block localhost variants
    if hostname in ("localhost", ""):
        raise ExtractorError(f"Access to localhost is not allowed: {url}")

    # Try to parse as IP address and check for private ranges
    try:
        addr = ipaddress.ip_address(hostname)
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved or addr.is_unspecified:
            raise ExtractorError(f"Access to private/reserved IP is not allowed: {url}")
    except ValueError:
        # Not a raw IP — hostname is fine, DNS resolution happens later
        pass


_EXTRACTOR_REGISTRY: dict[str, "ContentExtractor"] = {}


class ContentExtractor(ABC):
    """Base class for specialised content extractors."""

    name: str
    url_patterns: list[re.Pattern]

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LangAlpha/1.0)"},
        )

    def matches(self, url: str) -> bool:
        return any(p.search(url) for p in self.url_patterns)

    @abstractmethod
    async def extract(self, url: str) -> CrawlOutput | None: ...

    async def shutdown(self) -> None:
        await self._client.aclose()


def register_extractor(cls: type[ContentExtractor]) -> type[ContentExtractor]:
    """Class decorator — instantiate and register an extractor."""
    instance = cls()
    _EXTRACTOR_REGISTRY[instance.name] = instance
    logger.debug(f"Registered content extractor: {instance.name}")
    return cls


def get_extractor_registry() -> dict[str, ContentExtractor]:
    return _EXTRACTOR_REGISTRY
