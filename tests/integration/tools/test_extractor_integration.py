"""Integration tests for content extractors (hits real websites).

Run with:
    uv run pytest tests/integration/tools/test_extractor_integration.py -m integration -v
"""

from __future__ import annotations

import pytest

from src.tools.web.inhouse.backend import CrawlOutput

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# PDF extractor — live fetch
# ---------------------------------------------------------------------------


class TestPdfExtractorLive:
    """Test PdfExtractor against a real public PDF."""

    async def test_fetch_public_pdf(self):
        from src.tools.web.inhouse.extractors.pdf import PdfExtractor

        extractor = PdfExtractor()
        try:
            result = await extractor.extract(
                "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"
            )

            assert result is not None, "Expected CrawlOutput, got None"
            assert isinstance(result, CrawlOutput)
            assert result.markdown, "Expected non-empty markdown"
            assert len(result.markdown.strip()) > 10, "Markdown should have meaningful content"
            assert result.title, "Expected a title extracted from URL"
        finally:
            await extractor.shutdown()


# ---------------------------------------------------------------------------
# YouTube extractor — live fetch
# ---------------------------------------------------------------------------


class TestYouTubeExtractorLive:
    """Test YouTubeExtractor against a known video with captions."""

    async def test_fetch_transcript(self):
        from src.tools.web.inhouse.extractors.youtube import YouTubeExtractor

        extractor = YouTubeExtractor()
        try:
            result = await extractor.extract(
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            )

            assert result is not None, "Expected CrawlOutput, got None"
            assert isinstance(result, CrawlOutput)
            assert result.title, "Expected a video title"
            assert result.markdown, "Expected non-empty markdown"
            # This video has well-known captions; verify something is present
            assert len(result.markdown) > 100, "Markdown should contain substantial content"
        finally:
            await extractor.shutdown()


# ---------------------------------------------------------------------------
# Twitter extractor — live fetch
# ---------------------------------------------------------------------------


class TestTwitterExtractorLive:
    """Test TwitterExtractor against a known public tweet via FixTweet."""

    async def test_fetch_tweet(self):
        from src.tools.web.inhouse.extractors.twitter import TwitterExtractor

        extractor = TwitterExtractor()
        try:
            # Use a well-known tweet that is unlikely to be deleted
            # Jack Dorsey's first tweet
            result = await extractor.extract(
                "https://x.com/jack/status/20"
            )

            # FixTweet API may be rate-limited or down; be resilient
            if result is not None:
                assert isinstance(result, CrawlOutput)
                assert result.markdown, "Expected non-empty markdown"
                assert "View on X" in result.markdown
        finally:
            await extractor.shutdown()


# ---------------------------------------------------------------------------
# ContentRouter — live dispatch
# ---------------------------------------------------------------------------


class TestRouterLive:
    """Test ContentRouter dispatching to extractors and fallback."""

    async def test_pdf_through_router(self):
        from src.tools.web.inhouse.content_router import ContentRouter

        router = ContentRouter()
        try:
            result = await router.crawl_with_metadata(
                "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"
            )

            assert isinstance(result, CrawlOutput)
            assert result.markdown, "Expected non-empty markdown from PDF"
            assert result.title, "Expected a title"
        finally:
            await router.shutdown()

    async def test_html_falls_through(self):
        """HTML URL with no matching extractor falls through to ScraplingCrawler."""
        from src.tools.web.inhouse.content_router import ContentRouter

        router = ContentRouter()
        try:
            result = await router.crawl_with_metadata("https://example.com")

            assert isinstance(result, CrawlOutput)
            assert result.markdown, "Expected non-empty markdown"
            assert "example" in result.markdown.lower()
        finally:
            await router.shutdown()
