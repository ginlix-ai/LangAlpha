"""Integration tests for Scrapling crawler backend — hits real websites.

Tests the full stack: ScraplingCrawler → SafeCrawlerWrapper → crawl_tool.

Run with:
    uv run pytest tests/integration/tools/test_scrapling_integration.py -m integration -v
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# A simple, fast, reliable target for Tier 1 HTTP fetch
_TEST_URL = "https://example.com"
_TEST_URL_HTTPBIN = "https://httpbin.org/html"


# ---------------------------------------------------------------------------
# ScraplingCrawler direct tests
# ---------------------------------------------------------------------------


class TestScraplingCrawlerLive:
    """Test ScraplingCrawler against real URLs (Tier 1 HTTP fetch)."""

    async def test_tier1_fetch_example_com(self):
        from src.tools.crawler.scrapling_crawler import ScraplingCrawler

        crawler = ScraplingCrawler()
        output = await crawler.crawl_with_metadata(_TEST_URL)

        assert output.markdown, "Should return non-empty markdown"
        assert len(output.markdown) > 50, "Markdown should have substantial content"
        assert output.html, "Should return raw HTML"
        assert "<html" in output.html.lower() or "<body" in output.html.lower()
        # example.com has a known title
        assert "example" in output.title.lower(), f"Expected 'example' in title, got: {output.title}"

    async def test_tier1_returns_markdown_string(self):
        from src.tools.crawler.scrapling_crawler import ScraplingCrawler

        crawler = ScraplingCrawler()
        markdown = await crawler.crawl(_TEST_URL)

        assert isinstance(markdown, str)
        assert len(markdown) > 50
        # example.com contains "Example Domain" in its content
        assert "example" in markdown.lower()

    async def test_tier1_httpbin_html(self):
        from src.tools.crawler.scrapling_crawler import ScraplingCrawler

        crawler = ScraplingCrawler()
        output = await crawler.crawl_with_metadata(_TEST_URL_HTTPBIN)

        assert output.markdown, "Should return non-empty markdown"
        assert "herman melville" in output.markdown.lower(), (
            "httpbin.org/html contains Moby Dick excerpt"
        )


# ---------------------------------------------------------------------------
# SafeCrawlerWrapper integration tests
# ---------------------------------------------------------------------------


class TestSafeCrawlerWrapperLive:
    """Test SafeCrawlerWrapper with Scrapling backend against real URLs."""

    async def test_crawl_success(self):
        from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling")
        result = await wrapper.crawl(_TEST_URL)

        assert result.success, f"Crawl failed: {result.error}"
        assert result.markdown, "Should return markdown content"
        assert "example" in result.markdown.lower()
        assert result.title, "Should extract page title"
        assert result.error is None

    async def test_crawl_returns_title(self):
        from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling")
        result = await wrapper.crawl(_TEST_URL)

        assert result.success
        assert result.title
        assert "example" in result.title.lower()

    async def test_crawl_invalid_url_returns_error(self):
        from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling", default_timeout=10.0)
        result = await wrapper.crawl("https://this-domain-does-not-exist-12345.com")

        assert not result.success
        assert result.error
        # When all three tiers fail with network errors, ScraplingCrawler rolls
        # the exception path into failure_kind="infra_error" → error_type="infra_error".
        # If the single-tier exception path fires, it classifies via string match.
        assert result.error_type in (
            "infra_error", "dns_error", "network_error", "connection_timeout",
            "crawl_error", "timeout", "empty_content",
        )

    async def test_circuit_breaker_starts_healthy(self):
        from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling")
        assert wrapper.is_healthy()

        status = wrapper.get_status()
        assert status["infra_circuit_state"] == "closed"
        assert status["infra_failure_count"] == 0
        assert status["host_breaker_count"] == 0
        assert status["blocked_host_count"] == 0

    async def test_concurrent_crawls(self):
        """Test multiple concurrent crawls don't interfere."""
        import asyncio
        from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling", http_concurrency=5)
        urls = [_TEST_URL, _TEST_URL_HTTPBIN]

        results = await asyncio.gather(*[wrapper.crawl(url) for url in urls])

        for result in results:
            assert result.success, f"Crawl failed: {result.error}"
            assert result.markdown


# ---------------------------------------------------------------------------
# crawl_tool end-to-end (through SafeCrawlerWrapper)
# ---------------------------------------------------------------------------


class TestCrawlToolLive:
    """Test crawl_tool end-to-end with Scrapling backend."""

    async def test_crawl_tool_returns_content(self):
        """Test the crawl tool function directly (bypassing StructuredTool wrapper)."""
        from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper
        import src.tools.crawler.safe_wrapper as wrapper_mod

        # Reset singleton to ensure Scrapling backend
        old = wrapper_mod._safe_wrapper
        wrapper_mod._safe_wrapper = SafeCrawlerWrapper(backend="scrapling")
        try:
            from src.tools.crawl import _crawl_impl
            result = await _crawl_impl(_TEST_URL)

            assert isinstance(result, dict), f"Expected dict, got {type(result)}: {result}"
            assert result["url"] == _TEST_URL
            assert result["crawled_content"]
            assert "example" in result["crawled_content"].lower()
        finally:
            wrapper_mod._safe_wrapper = old

    async def test_crawl_tool_bad_url(self):
        from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper
        import src.tools.crawler.safe_wrapper as wrapper_mod

        old = wrapper_mod._safe_wrapper
        wrapper_mod._safe_wrapper = SafeCrawlerWrapper(backend="scrapling", default_timeout=10.0)
        try:
            from src.tools.crawl import _crawl_impl
            result = await _crawl_impl("https://this-domain-does-not-exist-12345.com")

            assert isinstance(result, str), "Should return error string for failed crawl"
            assert "failed" in result.lower() or "error" in result.lower()
        finally:
            wrapper_mod._safe_wrapper = old
