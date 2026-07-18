"""Unit tests for crawler backend selection."""

import pytest

from src.tools.web.inhouse.safe_wrapper import SafeCrawlerWrapper


class TestBackendSelection:
    """Tests for backend selection in SafeCrawlerWrapper."""

    def test_default_backend_is_scrapling(self):
        wrapper = SafeCrawlerWrapper()
        assert wrapper._backend == "scrapling"

    def test_explicit_scrapling_backend(self):
        wrapper = SafeCrawlerWrapper(backend="scrapling")
        assert wrapper._backend == "scrapling"

    @pytest.mark.asyncio
    async def test_get_crawler_scrapling(self):
        wrapper = SafeCrawlerWrapper(backend="scrapling")
        crawler = await wrapper._get_crawler()
        from src.tools.web.inhouse.scrapling_crawler import ScraplingCrawler
        assert isinstance(crawler, ScraplingCrawler)

    def test_unknown_backend_raises(self):
        with pytest.raises(ValueError, match="Unknown crawler backend"):
            SafeCrawlerWrapper(backend="nonexistent")

    @pytest.mark.asyncio
    async def test_crawler_is_cached(self):
        wrapper = SafeCrawlerWrapper(backend="scrapling")
        crawler1 = await wrapper._get_crawler()
        crawler2 = await wrapper._get_crawler()
        assert crawler1 is crawler2

    @pytest.mark.asyncio
    async def test_router_backend(self):
        wrapper = SafeCrawlerWrapper(backend="router")
        crawler = await wrapper._get_crawler()
        from src.tools.web.inhouse.content_router import ContentRouter
        assert isinstance(crawler, ContentRouter)

    @pytest.mark.asyncio
    async def test_router_backend_cached(self):
        wrapper = SafeCrawlerWrapper(backend="router")
        crawler1 = await wrapper._get_crawler()
        crawler2 = await wrapper._get_crawler()
        assert crawler1 is crawler2
