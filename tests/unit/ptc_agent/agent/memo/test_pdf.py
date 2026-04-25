"""Unit tests for PDF text extraction with pdfplumber/pypdf fallback."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ptc_agent.agent.memo.pdf import (
    MemoPdfExtractionError,
    extract_pdf_text,
)


class TestExtractPdfText:
    @pytest.mark.asyncio
    async def test_pdfplumber_success_returns_text(self):
        with patch(
            "ptc_agent.agent.memo.pdf._pdfplumber_extract",
            return_value="This is a PDF with plenty of readable content spanning multiple lines.",
        ) as mock_plumber, patch(
            "ptc_agent.agent.memo.pdf._pypdf_extract"
        ) as mock_pypdf:
            text = await extract_pdf_text(b"%PDF-1.4 fake")
            assert "readable content" in text
            mock_plumber.assert_called_once()
            mock_pypdf.assert_not_called()

    @pytest.mark.asyncio
    async def test_pypdf_fallback_when_pdfplumber_returns_short(self):
        # pdfplumber returns < MEMO_CONTENT_MIN_CHARS (50) — should try pypdf.
        with patch(
            "ptc_agent.agent.memo.pdf._pdfplumber_extract", return_value="tiny"
        ), patch(
            "ptc_agent.agent.memo.pdf._pypdf_extract",
            return_value="x" * 100,
        ) as mock_pypdf:
            text = await extract_pdf_text(b"%PDF-1.4 fake")
            assert len(text.strip()) >= 50
            mock_pypdf.assert_called_once()

    @pytest.mark.asyncio
    async def test_pypdf_fallback_when_pdfplumber_raises(self):
        with patch(
            "ptc_agent.agent.memo.pdf._pdfplumber_extract",
            side_effect=RuntimeError("pdfplumber blew up"),
        ), patch(
            "ptc_agent.agent.memo.pdf._pypdf_extract",
            return_value="x" * 100,
        ) as mock_pypdf:
            text = await extract_pdf_text(b"%PDF-1.4 fake")
            assert text
            mock_pypdf.assert_called_once()

    @pytest.mark.asyncio
    async def test_both_empty_raises_extraction_error(self):
        with patch(
            "ptc_agent.agent.memo.pdf._pdfplumber_extract", return_value=""
        ), patch(
            "ptc_agent.agent.memo.pdf._pypdf_extract", return_value=""
        ):
            with pytest.raises(MemoPdfExtractionError) as exc:
                await extract_pdf_text(b"%PDF-1.4 scan")
            assert "readable text" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_both_below_minimum_raises(self):
        with patch(
            "ptc_agent.agent.memo.pdf._pdfplumber_extract", return_value="tiny"
        ), patch(
            "ptc_agent.agent.memo.pdf._pypdf_extract", return_value="also tiny"
        ):
            with pytest.raises(MemoPdfExtractionError):
                await extract_pdf_text(b"%PDF-1.4 scan")

    @pytest.mark.asyncio
    async def test_both_raise_raises_extraction_error(self):
        with patch(
            "ptc_agent.agent.memo.pdf._pdfplumber_extract",
            side_effect=RuntimeError("plumber"),
        ), patch(
            "ptc_agent.agent.memo.pdf._pypdf_extract",
            side_effect=RuntimeError("pypdf"),
        ):
            with pytest.raises(MemoPdfExtractionError):
                await extract_pdf_text(b"%PDF-1.4 broken")
