"""Integration tests for Scrapling crawler backend — hits real websites.

Tests the stack: ScraplingCrawler → SafeCrawlerWrapper.

Run with:
    uv run pytest tests/integration/tools/test_scrapling_integration.py -m integration -v
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# A simple, fast, reliable target for Tier 1 HTTP fetch
_TEST_URL = "https://example.com"
_TEST_URL_HTTPBIN = "https://httpbin.org/html"

# Some targets (e.g. httpbin.org) push the crawler past the HTTP-only Tier 1
# into the patchright/Camoufox browser tiers. Those browsers are installed
# inside sandboxes at runtime, not in CI — skip such tests when the binary is
# absent rather than failing on an environment gap.
_BROWSER_MISSING_MARKERS = ("executable doesn't exist", "playwright install")


def _skip_if_no_browser(text: str | None) -> None:
    if text and any(marker in text.lower() for marker in _BROWSER_MISSING_MARKERS):
        pytest.skip(
            "scrapling browser tier (patchright/Camoufox) not installed in this environment"
        )


# ---------------------------------------------------------------------------
# ScraplingCrawler direct tests
# ---------------------------------------------------------------------------


class TestScraplingCrawlerLive:
    """Test ScraplingCrawler against real URLs (Tier 1 HTTP fetch)."""

    async def test_tier1_fetch_example_com(self):
        from src.tools.web.inhouse.scrapling_crawler import ScraplingCrawler

        crawler = ScraplingCrawler()
        output = await crawler.crawl_with_metadata(_TEST_URL)

        assert output.markdown, "Should return non-empty markdown"
        assert len(output.markdown) > 50, "Markdown should have substantial content"
        assert output.html, "Should return raw HTML"
        assert "<html" in output.html.lower() or "<body" in output.html.lower()
        # example.com has a known title
        assert "example" in output.title.lower(), f"Expected 'example' in title, got: {output.title}"

    async def test_tier1_returns_markdown_string(self):
        from src.tools.web.inhouse.scrapling_crawler import ScraplingCrawler

        crawler = ScraplingCrawler()
        markdown = await crawler.crawl(_TEST_URL)

        assert isinstance(markdown, str)
        assert len(markdown) > 50
        # example.com contains "Example Domain" in its content
        assert "example" in markdown.lower()

    async def test_tier1_httpbin_html(self):
        from src.tools.web.inhouse.scrapling_crawler import ScraplingCrawler

        crawler = ScraplingCrawler()
        try:
            output = await crawler.crawl_with_metadata(_TEST_URL_HTTPBIN)
        except Exception as e:
            # httpbin.org commonly forces escalation into the browser tiers;
            # skip if those browsers aren't installed instead of hard-failing.
            _skip_if_no_browser(str(e))
            raise

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
        from src.tools.web.inhouse.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling")
        result = await wrapper.crawl(_TEST_URL)

        assert result.success, f"Crawl failed: {result.error}"
        assert result.markdown, "Should return markdown content"
        assert "example" in result.markdown.lower()
        assert result.title, "Should extract page title"
        assert result.error is None

    async def test_crawl_returns_title(self):
        from src.tools.web.inhouse.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling")
        result = await wrapper.crawl(_TEST_URL)

        assert result.success
        assert result.title
        assert "example" in result.title.lower()

    async def test_crawl_invalid_url_returns_error(self):
        from src.tools.web.inhouse.safe_wrapper import SafeCrawlerWrapper

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
        from src.tools.web.breaker import CircuitState
        from src.tools.web.inhouse.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling")
        assert wrapper.is_healthy()
        assert wrapper._infra_breaker.state == CircuitState.CLOSED
        assert wrapper._infra_breaker.failure_count == 0
        assert not wrapper._host_breakers
        assert not wrapper._blocked_hosts

    async def test_concurrent_crawls(self):
        """Test multiple concurrent crawls don't interfere."""
        import asyncio
        from src.tools.web.inhouse.safe_wrapper import SafeCrawlerWrapper

        wrapper = SafeCrawlerWrapper(backend="scrapling", http_concurrency=5)
        urls = [_TEST_URL, _TEST_URL_HTTPBIN]

        results = await asyncio.gather(*[wrapper.crawl(url) for url in urls])

        for result in results:
            if not result.success:
                # httpbin.org may escalate to a browser tier; skip if absent.
                _skip_if_no_browser(result.error)
            assert result.success, f"Crawl failed: {result.error}"
            assert result.markdown


# The legacy single-page crawl_tool was retired with the site-crawl tools
# (its role is WebFetch's raw path); SafeCrawlerWrapper coverage above stands.
