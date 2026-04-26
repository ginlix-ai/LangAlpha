"""PDF text extraction on raw bytes — pdfplumber primary, pypdf fallback.

Mirrors the internals of ``src/tools/crawler/extractors/pdf.py`` but takes
``bytes`` directly instead of a URL. The crawler's ``PdfExtractor`` downloads
the PDF over HTTP — wrong entry point for memo uploads where the bytes are
already in hand.
"""

from __future__ import annotations

import asyncio
import logging
from io import BytesIO

from ptc_agent.agent.memo.schema import (
    MEMO_CONTENT_MIN_CHARS,
    MEMO_MAX_CONTENT_BYTES,
)

logger = logging.getLogger(__name__)

_EXTRACTION_TIMEOUT_S = 30

# Bound the per-PDF page count to keep extraction memory predictable. A 1000-page
# financial filing decompressed and held in memory comfortably crosses 1 GB on
# pdfplumber. 500 pages covers any realistic memo input; documents that exceed
# this almost always belong in a different workflow (e.g. summarized externally
# or chunked for retrieval) and should not blow up the FastAPI worker.
_MAX_PDF_PAGES = 500

# Cap on concurrent PDF extractions across the worker. pdfplumber holds the
# entire parsed page tree in memory until the ``with`` block exits — peak RSS
# per extraction can hit ~1 GB on the documented 500-page cap. Two concurrent
# extractions therefore bound peak memory at ~2 GB regardless of how many
# uploads land at once. Additional uploads queue on this semaphore.
_EXTRACTION_CONCURRENCY = 2
_EXTRACTION_SEM = asyncio.Semaphore(_EXTRACTION_CONCURRENCY)


class MemoPdfExtractionError(Exception):
    """Raised when a PDF yields less than ``MEMO_CONTENT_MIN_CHARS`` of text.

    Typical cause: the PDF has no text layer (scanned document). Surface as
    HTTP 422 at the router so the user knows to run OCR first.
    """


class MemoPdfTooManyPagesError(MemoPdfExtractionError):
    """Raised when a PDF exceeds the configured page-count cap."""


def _pdfplumber_extract(content: bytes) -> str:
    import pdfplumber

    pages: list[str] = []
    accumulated_bytes = 0
    with pdfplumber.open(BytesIO(content)) as pdf:
        if len(pdf.pages) > _MAX_PDF_PAGES:
            raise MemoPdfTooManyPagesError(
                f"PDF has {len(pdf.pages)} pages; "
                f"max is {_MAX_PDF_PAGES}. Split the document or summarize first."
            )
        for i, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            page.close()
            if text.strip():
                pages.append(f"--- Page {i} ---\n\n{text}")
                # Short-circuit when accumulated text already exceeds the
                # store-side cap. Defends against PDFs whose page count is
                # below the cap but whose individual pages emit megabytes
                # of text via embedded fonts or repeated streams.
                accumulated_bytes += len(text.encode("utf-8"))
                if accumulated_bytes > MEMO_MAX_CONTENT_BYTES:
                    break
    return "\n\n".join(pages)


def _pypdf_extract(content: bytes) -> str:
    import pypdf

    reader = pypdf.PdfReader(BytesIO(content))
    if len(reader.pages) > _MAX_PDF_PAGES:
        raise MemoPdfTooManyPagesError(
            f"PDF has {len(reader.pages)} pages; "
            f"max is {_MAX_PDF_PAGES}. Split the document or summarize first."
        )
    pages: list[str] = []
    accumulated_bytes = 0
    for i, page in enumerate(reader.pages, 1):
        text = page.extract_text() or ""
        if text.strip():
            pages.append(f"--- Page {i} ---\n\n{text}")
            accumulated_bytes += len(text.encode("utf-8"))
            if accumulated_bytes > MEMO_MAX_CONTENT_BYTES:
                break
    return "\n\n".join(pages)


async def extract_pdf_text(content: bytes) -> str:
    """Extract readable text from PDF bytes.

    Tries pdfplumber first, falls back to pypdf. If neither yields at least
    ``MEMO_CONTENT_MIN_CHARS`` of non-whitespace text, raises
    ``MemoPdfExtractionError`` so the router can return 422 with a clear
    message.

    Concurrent extractions are bounded by ``_EXTRACTION_SEM`` so the FastAPI
    worker's RSS stays bounded even under a burst of PDF uploads.
    """
    async with _EXTRACTION_SEM:
        # Primary: pdfplumber
        try:
            text = await asyncio.wait_for(
                asyncio.to_thread(_pdfplumber_extract, content),
                timeout=_EXTRACTION_TIMEOUT_S,
            )
            if _is_usable(text):
                return text
        except asyncio.TimeoutError:
            logger.warning(
                "memo pdf pdfplumber timed out (%ss)", _EXTRACTION_TIMEOUT_S,
            )
        except MemoPdfTooManyPagesError:
            # Page-count cap is not a fall-through: the same PDF will trip pypdf
            # too. Surface the specific error so the router returns the page-cap
            # message to the user instead of the generic "couldn't extract" one.
            raise
        except Exception as exc:
            logger.debug(
                "memo pdf pdfplumber failed: %s; falling back to pypdf", exc,
            )

        # Fallback: pypdf
        try:
            text = await asyncio.wait_for(
                asyncio.to_thread(_pypdf_extract, content),
                timeout=_EXTRACTION_TIMEOUT_S,
            )
            if _is_usable(text):
                return text
        except asyncio.TimeoutError:
            logger.warning("memo pdf pypdf timed out (%ss)", _EXTRACTION_TIMEOUT_S)
        except MemoPdfTooManyPagesError:
            raise
        except Exception as exc:
            logger.debug("memo pdf pypdf failed: %s", exc)

    raise MemoPdfExtractionError(
        "Could not extract readable text from this PDF (it may be a scan). "
        "Try OCR first."
    )


def _is_usable(text: str | None) -> bool:
    return bool(text) and len(text.strip()) >= MEMO_CONTENT_MIN_CHARS
