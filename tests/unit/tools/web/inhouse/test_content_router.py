"""Unit tests for ContentRouter."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.web.inhouse.backend import CrawlOutput
from src.tools.web.inhouse.extractors.base import ExtractorError
from src.tools.web.inhouse.content_router import ContentRouter


class TestContentRouter:
    """Verify ContentRouter dispatches to extractors and falls through to ScraplingCrawler."""

    @pytest.mark.asyncio
    async def test_dispatches_to_matching_extractor(self):
        """Matching extractor is called and its result is returned."""
        mock_extractor = MagicMock()
        mock_extractor.name = "test_ext"
        mock_extractor.matches.return_value = True
        mock_extractor.extract = AsyncMock(
            return_value=CrawlOutput(title="Extracted", html="", markdown="extracted content")
        )

        mock_registry = {"test_ext": mock_extractor}

        with patch(
            "src.tools.web.inhouse.content_router.get_extractor_registry", return_value=mock_registry
        ), patch("src.tools.web.inhouse.content_router.ScraplingCrawler"):
            from src.tools.web.inhouse.content_router import ContentRouter

            router = ContentRouter()
            result = await router.crawl_with_metadata("https://example.com/doc.pdf")

        assert result.title == "Extracted"
        assert result.markdown == "extracted content"
        mock_extractor.extract.assert_awaited_once_with("https://example.com/doc.pdf")

    @pytest.mark.asyncio
    async def test_extractor_returns_none_falls_through(self):
        """Extractor returns None -> ScraplingCrawler is used as fallback."""
        mock_extractor = MagicMock()
        mock_extractor.name = "test_ext"
        mock_extractor.matches.return_value = True
        mock_extractor.extract = AsyncMock(return_value=None)

        mock_registry = {"test_ext": mock_extractor}
        fallback_output = CrawlOutput(title="Fallback", html="<p>hi</p>", markdown="fallback md")

        mock_scrapling_cls = MagicMock()
        mock_scrapling_instance = AsyncMock()
        mock_scrapling_instance.crawl_with_metadata = AsyncMock(return_value=fallback_output)
        mock_scrapling_cls.return_value = mock_scrapling_instance

        with patch(
            "src.tools.web.inhouse.content_router.get_extractor_registry", return_value=mock_registry
        ), patch("src.tools.web.inhouse.content_router.ScraplingCrawler", mock_scrapling_cls):
            from src.tools.web.inhouse.content_router import ContentRouter

            router = ContentRouter()
            result = await router.crawl_with_metadata("https://example.com/page")

        assert result.title == "Fallback"
        assert result.markdown == "fallback md"
        mock_scrapling_instance.crawl_with_metadata.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_extractor_error_falls_through(self):
        """ExtractorError from extractor -> falls through to ScraplingCrawler (not re-raised)."""
        mock_extractor = MagicMock()
        mock_extractor.name = "broken_ext"
        mock_extractor.matches.return_value = True
        mock_extractor.extract = AsyncMock(side_effect=ExtractorError("SSRF blocked"))

        mock_registry = {"broken_ext": mock_extractor}
        fallback_output = CrawlOutput(title="Fallback", html="", markdown="ok")

        mock_scrapling_cls = MagicMock()
        mock_scrapling_instance = AsyncMock()
        mock_scrapling_instance.crawl_with_metadata = AsyncMock(return_value=fallback_output)
        mock_scrapling_cls.return_value = mock_scrapling_instance

        with patch(
            "src.tools.web.inhouse.content_router.get_extractor_registry", return_value=mock_registry
        ), patch("src.tools.web.inhouse.content_router.ScraplingCrawler", mock_scrapling_cls):
            from src.tools.web.inhouse.content_router import ContentRouter

            router = ContentRouter()
            result = await router.crawl_with_metadata("https://example.com/page")

        assert result.markdown == "ok"
        mock_scrapling_instance.crawl_with_metadata.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_general_exception_falls_through(self):
        """Unexpected ValueError from extractor -> falls through to ScraplingCrawler."""
        mock_extractor = MagicMock()
        mock_extractor.name = "flaky_ext"
        mock_extractor.matches.return_value = True
        mock_extractor.extract = AsyncMock(side_effect=ValueError("unexpected"))

        mock_registry = {"flaky_ext": mock_extractor}
        fallback_output = CrawlOutput(title="Fallback", html="", markdown="ok")

        mock_scrapling_cls = MagicMock()
        mock_scrapling_instance = AsyncMock()
        mock_scrapling_instance.crawl_with_metadata = AsyncMock(return_value=fallback_output)
        mock_scrapling_cls.return_value = mock_scrapling_instance

        with patch(
            "src.tools.web.inhouse.content_router.get_extractor_registry", return_value=mock_registry
        ), patch("src.tools.web.inhouse.content_router.ScraplingCrawler", mock_scrapling_cls):
            from src.tools.web.inhouse.content_router import ContentRouter

            router = ContentRouter()
            result = await router.crawl_with_metadata("https://example.com/page")

        assert result.markdown == "ok"

    @pytest.mark.asyncio
    async def test_no_match_uses_scrapling(self):
        """No extractor matches the URL -> ScraplingCrawler.crawl_with_metadata is called."""
        mock_extractor = MagicMock()
        mock_extractor.name = "pdf"
        mock_extractor.matches.return_value = False

        mock_registry = {"pdf": mock_extractor}
        fallback_output = CrawlOutput(title="HTML Page", html="<h1>Hi</h1>", markdown="# Hi")

        mock_scrapling_cls = MagicMock()
        mock_scrapling_instance = AsyncMock()
        mock_scrapling_instance.crawl_with_metadata = AsyncMock(return_value=fallback_output)
        mock_scrapling_cls.return_value = mock_scrapling_instance

        with patch(
            "src.tools.web.inhouse.content_router.get_extractor_registry", return_value=mock_registry
        ), patch("src.tools.web.inhouse.content_router.ScraplingCrawler", mock_scrapling_cls):
            from src.tools.web.inhouse.content_router import ContentRouter

            router = ContentRouter()
            result = await router.crawl_with_metadata("https://example.com/index.html")

        assert result.title == "HTML Page"
        mock_extractor.extract.assert_not_called()
        mock_scrapling_instance.crawl_with_metadata.assert_awaited_once_with(
            "https://example.com/index.html"
        )

    @pytest.mark.asyncio
    async def test_crawl_returns_markdown(self):
        """router.crawl() returns the markdown string from CrawlOutput."""
        mock_extractor = MagicMock()
        mock_extractor.name = "test"
        mock_extractor.matches.return_value = False

        mock_registry = {"test": mock_extractor}
        fallback_output = CrawlOutput(title="Page", html="<p>text</p>", markdown="page text")

        mock_scrapling_cls = MagicMock()
        mock_scrapling_instance = AsyncMock()
        mock_scrapling_instance.crawl_with_metadata = AsyncMock(return_value=fallback_output)
        mock_scrapling_cls.return_value = mock_scrapling_instance

        with patch(
            "src.tools.web.inhouse.content_router.get_extractor_registry", return_value=mock_registry
        ), patch("src.tools.web.inhouse.content_router.ScraplingCrawler", mock_scrapling_cls):
            from src.tools.web.inhouse.content_router import ContentRouter

            router = ContentRouter()
            result = await router.crawl("https://example.com")

        assert result == "page text"

    @pytest.mark.asyncio
    async def test_shutdown_closes_all(self):
        """router.shutdown() calls shutdown on all extractors and ScraplingCrawler."""
        ext1 = MagicMock()
        ext1.name = "pdf"
        ext1.shutdown = AsyncMock()
        ext2 = MagicMock()
        ext2.name = "youtube"
        ext2.shutdown = AsyncMock()

        mock_registry = {"pdf": ext1, "youtube": ext2}

        mock_scrapling_cls = MagicMock()
        mock_scrapling_instance = AsyncMock()
        mock_scrapling_instance.shutdown = AsyncMock()
        mock_scrapling_cls.return_value = mock_scrapling_instance

        with patch(
            "src.tools.web.inhouse.content_router.get_extractor_registry", return_value=mock_registry
        ), patch("src.tools.web.inhouse.content_router.ScraplingCrawler", mock_scrapling_cls):
            from src.tools.web.inhouse.content_router import ContentRouter

            router = ContentRouter()
            await router.shutdown()

        ext1.shutdown.assert_awaited_once()
        ext2.shutdown.assert_awaited_once()
        mock_scrapling_instance.shutdown.assert_awaited_once()


class TestRouterSSRFProtection:
    """Verify ContentRouter blocks SSRF URLs before any processing."""

    @pytest.mark.asyncio
    async def test_private_ip_blocked(self):
        router = ContentRouter()
        with pytest.raises(ExtractorError, match="not allowed"):
            await router.crawl_with_metadata("http://192.168.1.1/admin")

    @pytest.mark.asyncio
    async def test_localhost_blocked(self):
        router = ContentRouter()
        with pytest.raises(ExtractorError, match="not allowed"):
            await router.crawl_with_metadata("http://localhost:8080/secret")

    @pytest.mark.asyncio
    async def test_non_http_scheme_blocked(self):
        router = ContentRouter()
        with pytest.raises(ExtractorError, match="Unsupported URL scheme"):
            await router.crawl_with_metadata("file:///etc/passwd")

    @pytest.mark.asyncio
    async def test_valid_url_not_blocked(self):
        """Valid public URLs pass SSRF check (extractor/fallback still processes)."""
        mock_scrapling = AsyncMock()
        mock_scrapling.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="ok", html="", markdown="ok")
        )
        with patch.object(ContentRouter, "__init__", lambda self: None):
            router = ContentRouter()
            router._fallback = mock_scrapling
            with patch(
                "src.tools.web.inhouse.content_router.get_extractor_registry", return_value={}
            ):
                result = await router.crawl_with_metadata("https://example.com")
        assert result.markdown == "ok"
