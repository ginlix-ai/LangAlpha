"""PDF content extractor using pdfplumber with pypdf fallback."""

import asyncio
import logging
import re
from io import BytesIO
from urllib.parse import unquote, urlparse

from ..backend import CrawlOutput
from .base import ExtractorError, ContentExtractor, _validate_url, register_extractor

logger = logging.getLogger(__name__)

_MAX_PDF_SIZE = 20 * 1024 * 1024  # 20 MB


@register_extractor
class PdfExtractor(ContentExtractor):
    name = "pdf"
    url_patterns = [re.compile(r"\.pdf($|\?|#)", re.IGNORECASE)]

    async def extract(self, url: str) -> CrawlOutput | None:
        _validate_url(url)

        # Stream download with size guard
        content = await self._download(url)
        if content is None:
            return None

        # Extract text
        text = await self._extract_text(content)
        if text is None:
            return None

        title = self._title_from_url(url)
        return CrawlOutput(title=title, html="", markdown=text)

    async def _download(self, url: str) -> bytes | None:
        """Download PDF with size guard. Returns None if too large or wrong type."""
        try:
            async with self._client.stream("GET", url) as resp:
                resp.raise_for_status()

                # Check Content-Type
                ct = resp.headers.get("content-type", "")
                if ct and "application/pdf" not in ct:
                    return None

                # Check Content-Length if available
                cl = resp.headers.get("content-length")
                if cl and int(cl) > _MAX_PDF_SIZE:
                    logger.warning(f"PDF too large ({cl} bytes): {url}")
                    return None

                # Read in chunks up to limit
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    total += len(chunk)
                    if total > _MAX_PDF_SIZE:
                        logger.warning(f"PDF exceeded {_MAX_PDF_SIZE} bytes during streaming: {url}")
                        return None
                    chunks.append(chunk)

                return b"".join(chunks)
        except ExtractorError:
            raise
        except Exception as e:
            raise ExtractorError(f"Failed to download PDF: {e}") from e

    async def _extract_text(self, content: bytes) -> str | None:
        """Extract text via pdfplumber, falling back to pypdf."""
        try:
            text = await asyncio.wait_for(
                asyncio.to_thread(self._pdfplumber_extract, content),
                timeout=30,
            )
            if text and text.strip():
                return text
        except asyncio.TimeoutError:
            logger.warning("pdfplumber extraction timed out (30s)")
        except Exception as e:
            logger.debug(f"pdfplumber failed: {e}, trying pypdf fallback")

        # Fallback: pypdf
        try:
            text = await asyncio.wait_for(
                asyncio.to_thread(self._pypdf_extract, content),
                timeout=30,
            )
            if text and text.strip():
                return text
        except asyncio.TimeoutError:
            logger.warning("pypdf extraction timed out (30s)")
        except Exception as e:
            logger.debug(f"pypdf fallback also failed: {e}")

        return None

    @staticmethod
    def _pdfplumber_extract(content: bytes) -> str:
        import pdfplumber

        pages: list[str] = []
        with pdfplumber.open(BytesIO(content)) as pdf:
            for i, page in enumerate(pdf.pages, 1):
                text = page.extract_text() or ""
                page.close()
                if text.strip():
                    pages.append(f"--- Page {i} ---\n\n{text}")
        return "\n\n".join(pages)

    @staticmethod
    def _pypdf_extract(content: bytes) -> str:
        import pypdf

        reader = pypdf.PdfReader(BytesIO(content))
        pages: list[str] = []
        for i, page in enumerate(reader.pages, 1):
            text = page.extract_text() or ""
            if text.strip():
                pages.append(f"--- Page {i} ---\n\n{text}")
        return "\n\n".join(pages)

    @staticmethod
    def _title_from_url(url: str) -> str:
        path = urlparse(url).path
        filename = path.rsplit("/", 1)[-1] if "/" in path else path
        name = re.sub(r"\.pdf$", "", unquote(filename), flags=re.IGNORECASE)
        return name or "PDF Document"
