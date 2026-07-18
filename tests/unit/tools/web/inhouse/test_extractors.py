"""Unit tests for content extractors."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.tools.web.inhouse.backend import CrawlOutput
from src.tools.web.inhouse.extractors.base import (
    ContentExtractor,
    ExtractorError,
    _validate_url,
    get_extractor_registry,
)
from src.tools.web.inhouse.extractors.pdf import PdfExtractor
from src.tools.web.inhouse.extractors.twitter import TwitterExtractor, _parse_tweet_url
from src.tools.web.inhouse.extractors.youtube import YouTubeExtractor, _parse_video_id


# ---------------------------------------------------------------------------
# PDF extractor — URL pattern matching
# ---------------------------------------------------------------------------


class TestPdfExtractorPatterns:
    """Verify PdfExtractor.matches() against various URL shapes."""

    def setup_method(self):
        self.extractor = PdfExtractor()

    def test_matches_pdf_url(self):
        assert self.extractor.matches("https://example.com/report.pdf")

    def test_matches_pdf_with_query(self):
        assert self.extractor.matches("https://example.com/report.pdf?token=xxx")

    def test_matches_pdf_with_hash(self):
        assert self.extractor.matches("https://example.com/report.pdf#page=1")

    def test_no_match_html(self):
        assert not self.extractor.matches("https://example.com/index.html")

    def test_no_match_pdf_in_path(self):
        assert not self.extractor.matches("https://example.com/pdf/something")

    def test_case_insensitive(self):
        assert self.extractor.matches("https://example.com/report.PDF")


# ---------------------------------------------------------------------------
# YouTube extractor — URL pattern matching
# ---------------------------------------------------------------------------


class TestYouTubeExtractorPatterns:
    """Verify YouTubeExtractor.matches() against various URL shapes."""

    def setup_method(self):
        self.extractor = YouTubeExtractor()

    def test_matches_watch(self):
        assert self.extractor.matches("https://youtube.com/watch?v=dQw4w9WgXcQ")

    def test_matches_short_url(self):
        assert self.extractor.matches("https://youtu.be/dQw4w9WgXcQ")

    def test_matches_shorts(self):
        assert self.extractor.matches("https://youtube.com/shorts/dQw4w9WgXcQ")

    def test_matches_live(self):
        assert self.extractor.matches("https://youtube.com/live/dQw4w9WgXcQ")

    def test_no_match_channel(self):
        assert not self.extractor.matches("https://youtube.com/channel/UCxxxx")

    def test_no_match_other(self):
        assert not self.extractor.matches("https://vimeo.com/12345")


# ---------------------------------------------------------------------------
# YouTube — video ID parsing
# ---------------------------------------------------------------------------


class TestYouTubeVideoIdParsing:
    """Verify _parse_video_id extracts the correct ID from various URL formats."""

    def test_watch_url(self):
        assert _parse_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_short_url(self):
        assert _parse_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_shorts_url(self):
        assert _parse_video_id("https://youtube.com/shorts/dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_live_url(self):
        assert _parse_video_id("https://youtube.com/live/dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_with_extra_params(self):
        url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PLxxx&t=123"
        assert _parse_video_id(url) == "dQw4w9WgXcQ"

    def test_invalid_url(self):
        assert _parse_video_id("https://vimeo.com/12345") is None


# ---------------------------------------------------------------------------
# Twitter extractor — URL pattern matching
# ---------------------------------------------------------------------------


class TestTwitterExtractorPatterns:
    """Verify TwitterExtractor.matches() against various URL shapes."""

    def setup_method(self):
        self.extractor = TwitterExtractor()

    def test_matches_x_com(self):
        assert self.extractor.matches("https://x.com/user/status/123456789")

    def test_matches_twitter_com(self):
        assert self.extractor.matches("https://twitter.com/user/status/123456789")

    def test_no_match_profile(self):
        assert not self.extractor.matches("https://x.com/user")

    def test_no_match_other(self):
        assert not self.extractor.matches("https://mastodon.social/status/123")


# ---------------------------------------------------------------------------
# Twitter — URL parsing
# ---------------------------------------------------------------------------


class TestTwitterUrlParsing:
    """Verify _parse_tweet_url extracts (username, tweet_id)."""

    def test_x_com(self):
        assert _parse_tweet_url("https://x.com/elonmusk/status/123") == ("elonmusk", "123")

    def test_twitter_com(self):
        assert _parse_tweet_url("https://twitter.com/user/status/456") == ("user", "456")

    def test_with_query(self):
        assert _parse_tweet_url("https://x.com/user/status/123?s=20") == ("user", "123")

    def test_invalid(self):
        assert _parse_tweet_url("https://mastodon.social/@user/123") is None


# ---------------------------------------------------------------------------
# Extractor registry
# ---------------------------------------------------------------------------


class TestExtractorRegistry:
    """Verify that all built-in extractors are registered on import."""

    def test_extractors_registered(self):
        registry = get_extractor_registry()
        assert "pdf" in registry
        assert "youtube" in registry
        assert "twitter" in registry

    def test_get_extractor_registry(self):
        registry = get_extractor_registry()
        assert isinstance(registry, dict)
        assert len(registry) >= 3

    def test_registry_instances(self):
        registry = get_extractor_registry()
        for name, extractor in registry.items():
            assert isinstance(extractor, ContentExtractor), (
                f"Expected ContentExtractor instance for '{name}', got {type(extractor)}"
            )


# ---------------------------------------------------------------------------
# SSRF validation (_validate_url)
# ---------------------------------------------------------------------------


class TestSSRFValidation:
    """Verify _validate_url blocks private IPs, localhost, and bad schemes."""

    def test_valid_http(self):
        _validate_url("http://example.com")  # should not raise

    def test_valid_https(self):
        _validate_url("https://example.com")  # should not raise

    def test_private_10(self):
        with pytest.raises(ExtractorError, match="private"):
            _validate_url("http://10.0.0.1/file.pdf")

    def test_private_172(self):
        with pytest.raises(ExtractorError, match="private"):
            _validate_url("http://172.16.0.1/file.pdf")

    def test_private_192(self):
        with pytest.raises(ExtractorError, match="private"):
            _validate_url("http://192.168.1.1/file.pdf")

    def test_link_local(self):
        with pytest.raises(ExtractorError, match="private"):
            _validate_url("http://169.254.169.254/latest")

    def test_localhost(self):
        with pytest.raises(ExtractorError, match="localhost"):
            _validate_url("http://localhost/file")

    def test_localhost_ip(self):
        with pytest.raises(ExtractorError, match="private"):
            _validate_url("http://127.0.0.1/file")

    def test_ipv6_loopback(self):
        with pytest.raises(ExtractorError, match="private"):
            _validate_url("http://[::1]/file")

    def test_unspecified_address_blocked(self):
        with pytest.raises(ExtractorError, match="private"):
            _validate_url("http://0.0.0.0/internal")

    def test_file_scheme(self):
        with pytest.raises(ExtractorError, match="Unsupported URL scheme"):
            _validate_url("file:///etc/passwd")

    def test_ftp_scheme(self):
        with pytest.raises(ExtractorError, match="Unsupported URL scheme"):
            _validate_url("ftp://example.com/file")


# ---------------------------------------------------------------------------
# PDF extract — mock-based
# ---------------------------------------------------------------------------


class TestPdfExtract:
    """Test PdfExtractor.extract() with mocked HTTP and PDF parsers."""

    def setup_method(self):
        self.extractor = PdfExtractor()

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Mock successful download + pdfplumber parse -> CrawlOutput."""
        fake_pdf_bytes = b"%PDF-fake-content"

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "content-type": "application/pdf",
            "content-length": str(len(fake_pdf_bytes)),
        }
        mock_response.raise_for_status = MagicMock()

        # Simulate aiter_bytes yielding one chunk
        async def aiter_bytes(chunk_size=65536):
            yield fake_pdf_bytes

        mock_response.aiter_bytes = aiter_bytes

        async def mock_stream(method, url):
            yield mock_response

        with patch.object(
            self.extractor._client, "stream", side_effect=lambda *a, **kw: _async_cm(mock_response)
        ), patch.object(
            PdfExtractor, "_pdfplumber_extract", return_value="Page 1 text content"
        ):
            result = await self.extractor.extract("https://example.com/report.pdf")

        assert result is not None
        assert isinstance(result, CrawlOutput)
        assert result.markdown == "Page 1 text content"
        assert result.title == "report"

    @pytest.mark.asyncio
    async def test_content_type_mismatch(self):
        """HTML Content-Type -> returns None."""
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.raise_for_status = MagicMock()

        async def aiter_bytes(chunk_size=65536):
            yield b"<html>not a PDF</html>"

        mock_response.aiter_bytes = aiter_bytes

        with patch.object(
            self.extractor._client, "stream", side_effect=lambda *a, **kw: _async_cm(mock_response)
        ):
            result = await self.extractor.extract("https://example.com/report.pdf")

        assert result is None

    @pytest.mark.asyncio
    async def test_content_length_too_large(self):
        """Content-Length > 20MB -> returns None."""
        big_size = 25 * 1024 * 1024

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "content-type": "application/pdf",
            "content-length": str(big_size),
        }
        mock_response.raise_for_status = MagicMock()

        async def aiter_bytes(chunk_size=65536):
            # Should never be called because Content-Length check happens first
            yield b""  # pragma: no cover

        mock_response.aiter_bytes = aiter_bytes

        with patch.object(
            self.extractor._client, "stream", side_effect=lambda *a, **kw: _async_cm(mock_response)
        ):
            result = await self.extractor.extract("https://example.com/large.pdf")

        assert result is None

    @pytest.mark.asyncio
    async def test_pdfplumber_fails_pypdf_fallback(self):
        """pdfplumber raises -> pypdf fallback succeeds -> CrawlOutput."""
        fake_pdf_bytes = b"%PDF-fake"

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "content-type": "application/pdf",
            "content-length": str(len(fake_pdf_bytes)),
        }
        mock_response.raise_for_status = MagicMock()

        async def aiter_bytes(chunk_size=65536):
            yield fake_pdf_bytes

        mock_response.aiter_bytes = aiter_bytes

        with patch.object(
            self.extractor._client, "stream", side_effect=lambda *a, **kw: _async_cm(mock_response)
        ), patch.object(
            PdfExtractor, "_pdfplumber_extract", side_effect=RuntimeError("corrupt PDF")
        ), patch.object(
            PdfExtractor, "_pypdf_extract", return_value="Fallback text"
        ):
            result = await self.extractor.extract("https://example.com/report.pdf")

        assert result is not None
        assert result.markdown == "Fallback text"

    @pytest.mark.asyncio
    async def test_both_parsers_fail(self):
        """Both pdfplumber and pypdf fail -> returns None."""
        fake_pdf_bytes = b"%PDF-fake"

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "content-type": "application/pdf",
            "content-length": str(len(fake_pdf_bytes)),
        }
        mock_response.raise_for_status = MagicMock()

        async def aiter_bytes(chunk_size=65536):
            yield fake_pdf_bytes

        mock_response.aiter_bytes = aiter_bytes

        with patch.object(
            self.extractor._client, "stream", side_effect=lambda *a, **kw: _async_cm(mock_response)
        ), patch.object(
            PdfExtractor, "_pdfplumber_extract", side_effect=RuntimeError("parse error")
        ), patch.object(
            PdfExtractor, "_pypdf_extract", side_effect=RuntimeError("fallback error")
        ):
            result = await self.extractor.extract("https://example.com/report.pdf")

        assert result is None

    @pytest.mark.asyncio
    async def test_network_error(self):
        """httpx raises ConnectError -> raises ExtractorError."""
        with patch.object(
            self.extractor._client,
            "stream",
            side_effect=lambda *a, **kw: _async_cm_raises(httpx.ConnectError("connection refused")),
        ):
            with pytest.raises(ExtractorError, match="Failed to download PDF"):
                await self.extractor.extract("https://example.com/report.pdf")


# ---------------------------------------------------------------------------
# YouTube extract — mock-based
# ---------------------------------------------------------------------------


class TestYouTubeExtract:
    """Test YouTubeExtractor.extract() with mocked HTTP + transcript API."""

    def setup_method(self):
        self.extractor = YouTubeExtractor()

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Mock oEmbed + transcript -> CrawlOutput with transcript."""
        oembed_response = MagicMock()
        oembed_response.status_code = 200
        oembed_response.raise_for_status = MagicMock()
        oembed_response.json.return_value = {
            "title": "Never Gonna Give You Up",
            "author_name": "Rick Astley",
        }

        # Mock transcript API returning snippet objects
        mock_transcript = [
            {"start": 0.0, "text": "We're no strangers to love"},
            {"start": 5.0, "text": "You know the rules and so do I"},
        ]

        mock_yta = MagicMock()
        mock_yta.fetch.return_value = mock_transcript

        with patch.object(
            self.extractor._client, "get", return_value=oembed_response
        ), patch(
            "youtube_transcript_api.YouTubeTranscriptApi", mock_yta,
        ):
            result = await self.extractor.extract(
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            )

        assert result is not None
        assert isinstance(result, CrawlOutput)
        assert "Never Gonna Give You Up" in result.title
        assert "Rick Astley" in result.title
        assert "Transcript" in result.markdown
        assert "strangers" in result.markdown

    @pytest.mark.asyncio
    async def test_no_transcript(self):
        """Transcript API fails -> CrawlOutput with metadata only, no transcript."""
        oembed_response = MagicMock()
        oembed_response.status_code = 200
        oembed_response.raise_for_status = MagicMock()
        oembed_response.json.return_value = {
            "title": "Some Video",
            "author_name": "Author",
        }

        mock_yta = MagicMock()
        mock_yta.fetch.side_effect = Exception("NoTranscriptFound")

        with patch.object(
            self.extractor._client, "get", return_value=oembed_response
        ), patch(
            "youtube_transcript_api.YouTubeTranscriptApi", mock_yta,
        ):
            result = await self.extractor.extract(
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            )

        assert result is not None
        assert "Some Video" in result.title
        assert "No transcript available" in result.markdown

    @pytest.mark.asyncio
    async def test_oembed_fails(self):
        """oEmbed returns 404 -> still works with default title."""
        oembed_response = MagicMock()
        oembed_response.status_code = 404
        oembed_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=MagicMock(), response=oembed_response
        )

        mock_transcript = [{"start": 0.0, "text": "Hello world"}]
        mock_yta = MagicMock()
        mock_yta.fetch.return_value = mock_transcript

        with patch.object(
            self.extractor._client, "get", return_value=oembed_response
        ), patch(
            "youtube_transcript_api.YouTubeTranscriptApi", mock_yta,
        ):
            result = await self.extractor.extract(
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            )

        assert result is not None
        # Falls back to default title
        assert "YouTube Video" in result.title
        assert "Hello world" in result.markdown

    @pytest.mark.asyncio
    async def test_invalid_video_id(self):
        """URL with no parseable video ID -> returns None."""
        result = await self.extractor.extract("https://youtube.com/channel/UCxxxx")

        assert result is None


# ---------------------------------------------------------------------------
# Twitter extract — mock-based
# ---------------------------------------------------------------------------


class TestTwitterExtract:
    """Test TwitterExtractor.extract() with mocked FixTweet API."""

    def setup_method(self):
        self.extractor = TwitterExtractor()

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Mock FixTweet JSON -> CrawlOutput with formatted tweet."""
        api_response = MagicMock()
        api_response.status_code = 200
        api_response.json.return_value = {
            "tweet": {
                "author": {"screen_name": "testuser", "name": "Test User"},
                "text": "Hello, world! This is a test tweet.",
                "created_at": "2024-01-15T12:00:00Z",
                "likes": 42,
                "retweets": 7,
                "replies": 3,
                "media": {},
            }
        }

        with patch.object(self.extractor._client, "get", return_value=api_response):
            result = await self.extractor.extract(
                "https://x.com/testuser/status/123456789"
            )

        assert result is not None
        assert isinstance(result, CrawlOutput)
        assert "@testuser" in result.markdown
        assert "Hello, world!" in result.markdown
        assert "42" in result.markdown  # likes
        assert "View on X" in result.markdown

    @pytest.mark.asyncio
    async def test_api_down(self):
        """FixTweet returns 500 -> returns None."""
        api_response = MagicMock()
        api_response.status_code = 500

        with patch.object(self.extractor._client, "get", return_value=api_response):
            result = await self.extractor.extract(
                "https://x.com/testuser/status/123456789"
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_tweet_with_media(self):
        """FixTweet response with images -> markdown includes image URLs."""
        api_response = MagicMock()
        api_response.status_code = 200
        api_response.json.return_value = {
            "tweet": {
                "author": {"screen_name": "photouser", "name": "Photo User"},
                "text": "Check out this photo!",
                "created_at": "2024-01-15T12:00:00Z",
                "likes": 100,
                "retweets": 20,
                "replies": 5,
                "media": {
                    "photos": [
                        {"url": "https://pbs.twimg.com/media/photo1.jpg"},
                        {"url": "https://pbs.twimg.com/media/photo2.jpg"},
                    ]
                },
            }
        }

        with patch.object(self.extractor._client, "get", return_value=api_response):
            result = await self.extractor.extract(
                "https://x.com/photouser/status/987654321"
            )

        assert result is not None
        assert "photo1.jpg" in result.markdown
        assert "photo2.jpg" in result.markdown

    @pytest.mark.asyncio
    async def test_invalid_url(self):
        """Bad tweet URL (no parseable status) -> returns None."""
        result = await self.extractor.extract("https://x.com/user")
        assert result is None


# ---------------------------------------------------------------------------
# Helpers — async context manager stubs for httpx.stream()
# ---------------------------------------------------------------------------


class _async_cm:
    """Async context manager wrapping a mock response for httpx.stream()."""

    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc):
        return False


class _async_cm_raises:
    """Async context manager that raises on __aenter__ (simulates network error)."""

    def __init__(self, exc):
        self._exc = exc

    async def __aenter__(self):
        raise self._exc

    async def __aexit__(self, *exc):
        return False
