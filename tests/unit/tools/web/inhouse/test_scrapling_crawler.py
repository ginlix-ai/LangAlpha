"""Unit tests for Scrapling crawler backend with tier classification."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import asyncio

import pytest

from src.tools.web.inhouse.backend import CrawlOutput
from src.tools.web.inhouse.scrapling_crawler import (
    _MAX_FULL_PAGE_CHARS,
    ScraplingCrawler,
    _extract_title,
    _financial_figures,
    _html_to_markdown,
    _needs_browser,
    _needs_stealth,
    _try_full_page,
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


def _convert_result(content):
    """Mimic html_to_markdown.convert()'s return object (has a .content attr)."""
    result = MagicMock()
    result.content = content
    return result


@contextmanager
def _mock_converters(traf_return, full_return=None, full_raises=False):
    """Patch trafilatura.extract and html_to_markdown.convert at the lib boundary.

    Lets each test drive the trafilatura-vs-full-page decision deterministically
    without touching the network.
    """
    traf_mock = MagicMock(return_value=traf_return)
    if full_raises:
        full_mock = MagicMock(side_effect=RuntimeError("convert boom"))
    else:
        full_mock = MagicMock(return_value=_convert_result(full_return))
    with (
        patch("src.tools.web.inhouse.scrapling_crawler.trafilatura.extract", traf_mock),
        patch("src.tools.web.inhouse.scrapling_crawler.html_to_markdown.convert", full_mock),
    ):
        yield


class TestFinancialFigures:
    def test_extracts_dollars_percents_and_bignums(self):
        figs = _financial_figures("up 85% to $81.62 billion and index at 66,588.12")
        assert figs == {"85%", "$81.62billion", "66,588.12"}

    def test_empty_text_has_no_figures(self):
        assert _financial_figures("purely qualitative prose, no numbers") == set()


class TestHtmlToMarkdownDecision:
    """trafilatura-vs-full-page selection in _html_to_markdown.

    Calibration recap (live 10-page sample): clean extractions keep 88-100% of
    figures; broken ones keep <=28% (CNBC card/liveblog) or collapse to a stub
    (<2% of page, SEC/Fed listings). Thresholds: stub < 10% of a >5k page;
    figure retention < 65% over a >=8-figure page.
    """

    # --- figure-retention guard (full page < 5k so the stub guard stays out) ---

    def test_clean_extraction_is_kept(self):
        """High figure retention => trust trafilatura's clean output."""
        figs = [f"${n}1 billion" for n in range(1, 11)]  # 10 distinct figures
        full = "Body: " + ", ".join(figs) + ". Footer nav."
        extracted = "Article: " + ", ".join(figs[:8]) + "."  # keeps 8/10 = 80%
        with _mock_converters(traf_return=extracted, full_return=full):
            assert _html_to_markdown("<html>x</html>") == extracted

    def test_figure_loss_falls_back_to_full(self):
        """CNBC-style body drop (kept <65%) => prefer the full page."""
        figs = [f"${n}1 billion" for n in range(1, 11)]
        full = "Body: " + ", ".join(figs) + ". Net income $42 billion."
        extracted = "Lead card: " + ", ".join(figs[:2]) + "."  # keeps 2/10 = 20%
        with _mock_converters(traf_return=extracted, full_return=full):
            result = _html_to_markdown("<html>x</html>")
        assert result == full
        assert "$42 billion" in result  # the dropped body figure is recovered

    def test_low_figure_page_is_not_flipped(self):
        """Below the 8-figure sample gate the ratio is noise — keep trafilatura."""
        full = "Policy stays accommodative. Rates 1% to 2%. Possibly 3%."  # 3 figures
        extracted = "Summary: policy unchanged."  # keeps 0, but gate not met
        with _mock_converters(traf_return=extracted, full_return=full):
            assert _html_to_markdown("<html>x</html>") == extracted

    # --- stub guard (figure-poor listing pages) ---

    def test_listing_stub_falls_back_to_full(self):
        """Tiny extraction of a large figure-poor page => listing stub => full."""
        full = "# Press Releases\n" + "\n".join(
            f"[SEC Announces New Members Item {i}](/news/{i})" for i in range(300)
        )
        extracted = "# Press Releases\nOfficial announcements highlighting actions."
        assert len(extracted) < 0.10 * len(full) and len(full) > 5000
        with _mock_converters(traf_return=extracted, full_return=full):
            result = _html_to_markdown("<html>x</html>")
        assert result == full
        assert "SEC Announces New Members" in result  # dropped headlines recovered

    def test_substantial_extraction_of_large_page_is_kept(self):
        """A healthy fraction of a large page is a real article, not a stub."""
        full = "Intro. " + "Real article paragraph with detail. " * 400  # ~14k
        extracted = "Real article paragraph with detail. " * 200  # ~37% of full
        assert len(extracted) > 0.10 * len(full)
        with _mock_converters(traf_return=extracted, full_return=full):
            assert _html_to_markdown("<html>x</html>") == extracted

    # --- empty / error fallbacks ---

    def test_empty_extraction_uses_full(self):
        with _mock_converters(traf_return=None, full_return="# Article\n$5 billion."):
            assert _html_to_markdown("<html>x</html>") == "# Article\n$5 billion."

    def test_whitespace_extraction_uses_full(self):
        with _mock_converters(traf_return="   \n  ", full_return="# Article\nbody."):
            assert _html_to_markdown("<html>x</html>") == "# Article\nbody."

    def test_empty_extraction_and_full_failure_uses_plain_text(self):
        html = "<html><body><p>Hello world figure</p><style>.x{color:red}</style></body></html>"
        with _mock_converters(traf_return=None, full_raises=True):
            result = _html_to_markdown(html)
        assert "Hello world figure" in result
        assert "color:red" not in result  # <style> contents stripped

    def test_no_full_baseline_keeps_trafilatura(self):
        """full-page conversion raised but trafilatura succeeded => keep extraction."""
        with _mock_converters(traf_return="Good article $9 billion.", full_raises=True):
            assert _html_to_markdown("<html>x</html>") == "Good article $9 billion."

    def test_oversized_full_page_fallback_is_capped(self):
        """A full-page fallback above the ceiling is truncated to protect context."""
        full = "$1 billion. " * (_MAX_FULL_PAGE_CHARS // 5)  # ~960k chars, over cap
        extracted = "Tiny stub."  # < 10% of a >5k page => stub guard returns full
        with _mock_converters(traf_return=extracted, full_return=full):
            result = _html_to_markdown("<html>x</html>")
        assert len(result) <= _MAX_FULL_PAGE_CHARS + 100  # cap + short marker
        assert result.startswith("$1 billion.")
        assert result.rstrip().endswith("~100K tokens ...]")

    def test_full_page_under_cap_is_not_truncated(self):
        """A full page within the ceiling passes through untouched."""
        full = "# Press Releases\n" + "\n".join(
            f"[Item {i}](/n/{i})" for i in range(300)
        )  # large stub-trigger, but well under the cap
        assert len(full) < _MAX_FULL_PAGE_CHARS
        extracted = "# Press Releases\nIntro blurb."
        with _mock_converters(traf_return=extracted, full_return=full):
            result = _html_to_markdown("<html>x</html>")
        assert result == full  # no truncation marker appended


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


class TestFullPageNoHeadMetaLeak:
    """The full-page fallback must not dump <head> meta tags as frontmatter noise."""

    def test_head_meta_not_emitted(self):
        html = (
            '<html><head>'
            '<meta name="description" content="seo blurb">'
            '<meta property="og:title" content="OG Title">'
            "</head><body><h1>Real Heading</h1><p>Real body content.</p></body></html>"
        )
        out = _try_full_page(html)
        assert "Real Heading" in out and "Real body content." in out
        assert not out.lstrip().startswith("---")
        assert "meta-description" not in out and "og:title" not in out
