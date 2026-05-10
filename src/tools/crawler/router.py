"""Content router — tries specialised extractors before falling back to ScraplingCrawler."""

import logging

from .backend import CrawlOutput
from .extractors import ExtractorError, get_extractor_registry
from .extractors.base import _validate_url
from .scrapling_crawler import ScraplingCrawler

logger = logging.getLogger(__name__)


class ContentRouter:
    """
    Routes URLs to specialised extractors, with ScraplingCrawler as fallback.

    Satisfies the CrawlerBackend protocol.
    """

    def __init__(
        self,
        http_concurrency: int = 20,
        browser_concurrency: int = 6,
    ) -> None:
        self._fallback = ScraplingCrawler(
            http_concurrency=http_concurrency,
            browser_concurrency=browser_concurrency,
        )

    async def crawl_with_metadata(self, url: str) -> CrawlOutput:
        # SSRF protection: block private IPs, localhost, non-http(s) schemes
        # for ALL URLs before any extractor or ScraplingCrawler processes them
        _validate_url(url)

        for extractor in get_extractor_registry().values():
            if not extractor.matches(url):
                continue

            try:
                result = await extractor.extract(url)
                if result is not None:
                    logger.debug(f"Extractor '{extractor.name}' handled {url}")
                    return result
                logger.debug(f"Extractor '{extractor.name}' returned None for {url}, falling through")
            except ExtractorError as e:
                logger.warning(f"Extractor '{extractor.name}' error for {url}: {e}")
            except Exception as e:
                logger.warning(f"Extractor '{extractor.name}' unexpected error for {url}: {e}")

        return await self._fallback.crawl_with_metadata(url)

    async def crawl(self, url: str) -> str:
        output = await self.crawl_with_metadata(url)
        return output.markdown

    async def shutdown(self) -> None:
        for extractor in get_extractor_registry().values():
            try:
                await extractor.shutdown()
            except Exception as e:
                logger.debug(f"Error shutting down extractor '{extractor.name}': {e}")
        await self._fallback.shutdown()
