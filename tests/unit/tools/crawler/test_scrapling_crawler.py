"""Unit tests for Scrapling crawler backend with tier classification."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncio

import pytest

from src.tools.crawler.backend import CrawlOutput
from src.tools.crawler.scrapling_crawler import (
    ScraplingCrawler,
    _extract_title,
    _html_to_markdown,
    _needs_browser,
    _needs_stealth,
)


class TestNeedsBrowser:
    """Tests for Tier 1 -> Tier 2 escalation detection."""

    def test_4xx_status(self):
        assert _needs_browser("<html>Access Denied</html>", 403) is True
        assert _needs_browser("<html>Not Found</html>", 404) is True

    def test_5xx_status(self):
        assert _needs_browser("<html>Server Error</html>", 500) is True

    def test_empty_body(self):
        assert _needs_browser("", 200) is True
        assert _needs_browser("   ", 200) is True

    def test_short_body(self):
        assert _needs_browser("<html><body>tiny</body></html>", 200) is True

    def test_cloudflare_signal(self):
        html = "<html><body>Just a moment... Checking your browser</body></html>" + "x" * 200
        assert _needs_browser(html, 200) is True

    def test_normal_page(self):
        html = "<html><body>" + "<p>Real content here.</p>" * 20 + "</body></html>"
        assert _needs_browser(html, 200) is False

    def test_case_insensitive(self):
        html = "<html><body>ACCESS DENIED" + "x" * 200 + "</body></html>"
        assert _needs_browser(html, 200) is True


class TestNeedsStealth:
    """Tests for Tier 2 -> Tier 3 escalation. Returns 'cloudflare' / 'blocked' / None."""

    def test_403_returns_blocked(self):
        # 403 with no CF signals → bare bot block.
        assert _needs_stealth("<html>Blocked</html>", 403) == "blocked"

    def test_401_returns_blocked(self):
        assert _needs_stealth("<html>Unauthorized</html>", 401) == "blocked"

    def test_cloudflare_with_ray_id(self):
        html = "<html>Cloudflare challenge Ray ID: abc123</html>"
        assert _needs_stealth(html, 200) == "cloudflare"

    def test_cloudflare_just_a_moment(self):
        html = "<html>Cloudflare Just a moment...</html>"
        assert _needs_stealth(html, 200) == "cloudflare"

    def test_403_with_cloudflare_signals_returns_cloudflare(self):
        # CF signals win over plain status — solver might help.
        html = "<html>Cloudflare Just a moment... Ray ID: foo</html>"
        assert _needs_stealth(html, 403) == "cloudflare"

    def test_normal_page_returns_none(self):
        html = "<html><body>Normal page content</body></html>"
        assert _needs_stealth(html, 200) is None

    def test_cloudflare_without_challenge_returns_none(self):
        # Cloudflare-hosted but not a challenge page.
        html = "<html>Powered by Cloudflare</html>"
        assert _needs_stealth(html, 200) is None

    def test_datadome_challenge_returns_cloudflare(self):
        # Generic JS challenge classified as cloudflare so solver runs.
        html = '<html><body><p>Please enable JS and disable any ad blocker</p></body></html>'
        assert _needs_stealth(html, 200) == "cloudflare"

    def test_enable_js_on_large_page_not_stealth(self):
        # Large page that mentions enable-javascript is not a challenge.
        html = "<html><body>" + "x" * 3000 + "enable javascript" + "</body></html>"
        assert _needs_stealth(html, 200) is None


class TestHtmlToMarkdown:
    def test_basic_conversion(self):
        md = _html_to_markdown("<h1>Title</h1><p>Paragraph text.</p>")
        assert "Title" in md
        assert "Paragraph text." in md

    def test_links_preserved(self):
        md = _html_to_markdown('<a href="https://example.com">Link</a>')
        assert "https://example.com" in md
        assert "Link" in md

    def test_empty_html(self):
        assert _html_to_markdown("").strip() == ""


class TestCrawlOutput:
    def test_create(self):
        output = CrawlOutput(title="Test", html="<p>Hi</p>", markdown="Hi")
        assert output.title == "Test"
        assert output.status is None
        assert output.failure_kind is None

    def test_with_failure_kind(self):
        output = CrawlOutput(
            title="", html="", markdown="", status=401, failure_kind="blocked"
        )
        assert output.failure_kind == "blocked"
        assert output.status == 401


# ---------------------------------------------------------------------------
# Helpers for tier dispatch tests
# ---------------------------------------------------------------------------


def _make_page_mock(title_text: str = "Test Page"):
    title_node = MagicMock()
    title_node.get.return_value = title_text
    page = MagicMock()
    page.css.return_value = title_node
    return page


_GOOD_HTML = "<html><head><title>Test Page</title></head><body>" + "<p>Content</p>" * 20 + "</body></html>"
_BLOCKED_HTML_LIGHT = "<html><body>Just a moment... Cloudflare Ray ID: abc</body></html>"
_STEALTH_HTML = "<html><body>Cloudflare challenge Ray ID: xyz Just a moment</body></html>"
_BARE_401_HTML = "<html><body>401 Unauthorized</body></html>"


class TestTierDispatch:
    """Tier 1 → Tier 2 → Tier 3 escalation paths."""

    @pytest.mark.asyncio
    async def test_tier1_succeeds(self):
        crawler = ScraplingCrawler()
        page = _make_page_mock("Tier1 Title")

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page, _GOOD_HTML, 200)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        t2.assert_not_awaited()
        t3.assert_not_awaited()
        assert result.title == "Tier1 Title"
        assert result.status == 200
        assert result.failure_kind is None

    @pytest.mark.asyncio
    async def test_tier1_401_skips_browsers(self):
        """Hard block at Tier 1 → return blocked immediately. No browser spawn."""
        crawler = ScraplingCrawler()
        page = _make_page_mock()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page, _BARE_401_HTML, 401)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://reuters.com/markets")

        t2.assert_not_awaited()
        t3.assert_not_awaited()
        assert result.failure_kind == "blocked"
        assert result.status == 401
        assert result.markdown == ""

    @pytest.mark.asyncio
    async def test_tier1_451_skips_browsers(self):
        """Legal block (451) is also terminal at Tier 1."""
        crawler = ScraplingCrawler()
        page = _make_page_mock()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page, "Unavailable for legal reasons", 451)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        t2.assert_not_awaited()
        t3.assert_not_awaited()
        assert result.failure_kind == "blocked"
        assert result.status == 451

    @pytest.mark.asyncio
    async def test_tier1_dns_failure_skips_browsers(self):
        """DNS resolution failure at Tier 1 → return infra_error. No browser spawn."""
        crawler = ScraplingCrawler()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock,
                         side_effect=RuntimeError("Could not resolve host: nonexistent.invalid")),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://nonexistent.invalid/page")

        t2.assert_not_awaited()
        t3.assert_not_awaited()
        assert result.failure_kind == "infra_error"

    @pytest.mark.asyncio
    async def test_tier1_connection_refused_skips_browsers(self):
        """Connection-refused at Tier 1 → return infra_error. No browser spawn."""
        crawler = ScraplingCrawler()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock,
                         side_effect=RuntimeError("Failed to connect: Connection refused")),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://refused.example/page")

        t2.assert_not_awaited()
        t3.assert_not_awaited()
        assert result.failure_kind == "infra_error"

    @pytest.mark.asyncio
    async def test_tier1_generic_error_still_escalates(self):
        """Non-DNS Tier 1 errors still escalate (regression guard for N3)."""
        crawler = ScraplingCrawler()
        page_t2 = _make_page_mock("Tier2 Title")

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock,
                         side_effect=RuntimeError("SSL handshake timeout")),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock,
                         return_value=(page_t2, _GOOD_HTML, 200)) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        t2.assert_awaited_once()
        t3.assert_not_awaited()
        assert result.failure_kind is None
        assert result.status == 200

    @pytest.mark.asyncio
    async def test_tier1_403_still_escalates(self):
        """403 is ambiguous — still try Tier 2 (some CF configs accept browsers)."""
        crawler = ScraplingCrawler()
        page_t1 = _make_page_mock()
        page_t2 = _make_page_mock("Tier2 Title")

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page_t1, "Forbidden", 403)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, return_value=(page_t2, _GOOD_HTML, 200)) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        t2.assert_awaited_once()
        t3.assert_not_awaited()
        assert result.status == 200
        assert result.failure_kind is None

    @pytest.mark.asyncio
    async def test_tier1_429_returns_rate_limited(self):
        crawler = ScraplingCrawler()
        page = _make_page_mock()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page, "Too Many Requests", 429)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock) as t2,
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        t2.assert_not_awaited()
        t3.assert_not_awaited()
        assert result.failure_kind == "rate_limited"
        assert result.status == 429

    @pytest.mark.asyncio
    async def test_tier1_insufficient_escalates_to_tier2(self):
        crawler = ScraplingCrawler()
        page_t1 = _make_page_mock()
        page_t2 = _make_page_mock("Tier2 Title")

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page_t1, _BLOCKED_HTML_LIGHT, 200)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, return_value=(page_t2, _GOOD_HTML, 200)),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        t3.assert_not_awaited()
        assert result.title == "Tier2 Title"
        assert result.status == 200

    @pytest.mark.asyncio
    async def test_tier1_import_error_skips_to_tier2(self):
        crawler = ScraplingCrawler()
        page_t2 = _make_page_mock("Tier2 Title")

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, side_effect=ImportError("No module")),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, return_value=(page_t2, _GOOD_HTML, 200)),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock) as t3,
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        t3.assert_not_awaited()
        assert result.title == "Tier2 Title"


class TestSolveCloudflareDecision:
    """Whether Tier 3 invokes scrapling's CF solver depends on Tier 2's reason."""

    @pytest.mark.asyncio
    async def test_cf_reason_invokes_solver(self):
        crawler = ScraplingCrawler()
        page_t1 = _make_page_mock()
        page_t2 = _make_page_mock()
        page_t3 = _make_page_mock("Done")

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page_t1, _BLOCKED_HTML_LIGHT, 200)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, return_value=(page_t2, _STEALTH_HTML, 200)),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock, return_value=(page_t3, _GOOD_HTML, 200)) as t3,
        ):
            await crawler.crawl_with_metadata("https://example.com")

        # Tier 3 should be called with solve_cloudflare=True because Tier 2 saw a CF challenge.
        t3.assert_awaited_once()
        call_kwargs = t3.await_args.kwargs
        assert call_kwargs.get("solve_cloudflare") is True

    @pytest.mark.asyncio
    async def test_blocked_reason_disables_solver(self):
        """Tier 2 returns bare 401 — Tier 3 should skip the CF solver."""
        crawler = ScraplingCrawler()
        page_t1 = _make_page_mock()
        page_t2 = _make_page_mock()
        page_t3 = _make_page_mock("Done")

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page_t1, _BLOCKED_HTML_LIGHT, 200)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, return_value=(page_t2, _BARE_401_HTML, 401)),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock, return_value=(page_t3, _GOOD_HTML, 200)) as t3,
        ):
            await crawler.crawl_with_metadata("https://example.com")

        t3.assert_awaited_once()
        call_kwargs = t3.await_args.kwargs
        assert call_kwargs.get("solve_cloudflare") is False


class TestTier3Outcomes:
    @pytest.mark.asyncio
    async def test_tier3_still_blocked_sets_failure_kind(self):
        crawler = ScraplingCrawler()
        page = _make_page_mock()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page, _BLOCKED_HTML_LIGHT, 200)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, return_value=(page, _BARE_401_HTML, 401)),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock, return_value=(page, _BARE_401_HTML, 401)),
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        assert result.failure_kind == "blocked"
        assert result.status == 401

    @pytest.mark.asyncio
    async def test_tier3_still_cloudflare_sets_stealth_failed(self):
        crawler = ScraplingCrawler()
        page = _make_page_mock()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, return_value=(page, _BLOCKED_HTML_LIGHT, 200)),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, return_value=(page, _STEALTH_HTML, 200)),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock, return_value=(page, _STEALTH_HTML, 200)),
        ):
            result = await crawler.crawl_with_metadata("https://example.com")

        assert result.failure_kind == "stealth_failed"

    @pytest.mark.asyncio
    async def test_all_tiers_fail_propagates_exception(self):
        """Tier-3 exception now re-raises so the wrapper's _classify_exception
        can distinguish DNS/connection failures (host-only) from genuine infra
        failures (browser_closed → trips global infra breaker). Crawler-level
        blanket 'infra_error' was the bug that re-created host-isolation gaps."""
        crawler = ScraplingCrawler()

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock, side_effect=RuntimeError("t1")),
            patch.object(crawler, "_tier2_fetch", new_callable=AsyncMock, side_effect=RuntimeError("t2")),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock,
                         side_effect=RuntimeError("net::ERR_NAME_NOT_RESOLVED at example.com")),
        ):
            with pytest.raises(RuntimeError, match="ERR_NAME_NOT_RESOLVED"):
                await crawler.crawl_with_metadata("https://example.com")


class TestStageSemaphores:
    """The Tier-1 semaphore must be released before any browser-tier wait."""

    @pytest.mark.asyncio
    async def test_browser_sem_does_not_block_tier1(self):
        """Saturating the browser semaphore must not block Tier-1 calls."""
        crawler = ScraplingCrawler(http_concurrency=4, browser_concurrency=2)

        # Acquire all browser slots so Tier 2/3 would block.
        await crawler._browser_sem.acquire()
        await crawler._browser_sem.acquire()
        assert crawler._browser_sem._value == 0

        # Tier 1 should still proceed normally.
        page = _make_page_mock("OK")
        with patch.object(
            crawler, "_tier1_fetch", new_callable=AsyncMock,
            return_value=(page, _GOOD_HTML, 200),
        ):
            result = await asyncio.wait_for(
                crawler.crawl_with_metadata("https://example.com"), timeout=1.0
            )

        assert result.failure_kind is None

        # Cleanup.
        crawler._browser_sem.release()
        crawler._browser_sem.release()

    @pytest.mark.asyncio
    async def test_http_sem_released_during_browser_wait(self):
        """Tier-1 returning insufficient must release http_sem before Tier-2 acquires browser_sem."""
        crawler = ScraplingCrawler(http_concurrency=1, browser_concurrency=1)
        page_t1 = _make_page_mock()
        page_t2 = _make_page_mock("Tier2")

        async def slow_t2(url):
            # If http_sem were still held, this concurrent Tier-1 call would deadlock.
            return (page_t2, _GOOD_HTML, 200)

        with (
            patch.object(crawler, "_tier1_fetch", new_callable=AsyncMock,
                         return_value=(page_t1, _BLOCKED_HTML_LIGHT, 200)),
            patch.object(crawler, "_tier2_fetch", side_effect=slow_t2),
            patch.object(crawler, "_tier3_fetch", new_callable=AsyncMock),
        ):
            result = await asyncio.wait_for(
                crawler.crawl_with_metadata("https://example.com"), timeout=1.0
            )

        # http_sem fully released before Tier 2 ran.
        assert crawler._http_sem._value == 1
        assert result.title == "Tier2"


class TestExtractTitle:
    def test_with_title_element(self):
        page = _make_page_mock("My Page Title")
        assert _extract_title(page) == "My Page Title"

    def test_without_title_element(self):
        title_node = MagicMock()
        title_node.get.return_value = None
        page = MagicMock()
        page.css.return_value = title_node
        assert _extract_title(page) == ""

    def test_exception_returns_empty(self):
        page = MagicMock()
        page.css.side_effect = AttributeError("no css")
        assert _extract_title(page) == ""
