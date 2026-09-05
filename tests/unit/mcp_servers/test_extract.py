"""Contract tests for the shared HTML→markdown extraction (``mcp_servers/_extract.py``).

The trafilatura-vs-full-page decision is calibrated on live pages and is shared
verbatim by the scrape MCP server and the in-process crawler, so it is pinned
here rather than in either caller's suite.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from mcp_servers._extract import (
    _MAX_FULL_PAGE_CHARS,
    _financial_figures,
    _try_full_page,
    to_markdown,
)


def _silent(_message: str) -> None:
    pass


class TestHtmlToMarkdown:
    def test_basic_conversion(self):
        md = to_markdown("<h1>Title</h1><p>Paragraph text.</p>")
        assert "Title" in md
        assert "Paragraph text." in md

    def test_links_preserved(self):
        md = to_markdown('<a href="https://example.com">Link</a>')
        assert "https://example.com" in md
        assert "Link" in md

    def test_empty_html(self):
        assert to_markdown("").strip() == ""


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
        patch("trafilatura.extract", traf_mock),
        patch("html_to_markdown.convert", full_mock),
    ):
        yield


class TestFinancialFigures:
    def test_extracts_dollars_percents_and_bignums(self):
        figs = _financial_figures("up 85% to $81.62 billion and index at 66,588.12")
        assert figs == {"85%", "$81.62billion", "66,588.12"}

    def test_empty_text_has_no_figures(self):
        assert _financial_figures("purely qualitative prose, no numbers") == set()


class TestHtmlToMarkdownDecision:
    """trafilatura-vs-full-page selection in to_markdown.

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
            assert to_markdown("<html>x</html>") == extracted

    def test_figure_loss_falls_back_to_full(self):
        """CNBC-style body drop (kept <65%) => prefer the full page."""
        figs = [f"${n}1 billion" for n in range(1, 11)]
        full = "Body: " + ", ".join(figs) + ". Net income $42 billion."
        extracted = "Lead card: " + ", ".join(figs[:2]) + "."  # keeps 2/10 = 20%
        with _mock_converters(traf_return=extracted, full_return=full):
            result = to_markdown("<html>x</html>")
        assert result == full
        assert "$42 billion" in result  # the dropped body figure is recovered

    def test_low_figure_page_is_not_flipped(self):
        """Below the 8-figure sample gate the ratio is noise — keep trafilatura."""
        full = "Policy stays accommodative. Rates 1% to 2%. Possibly 3%."  # 3 figures
        extracted = "Summary: policy unchanged."  # keeps 0, but gate not met
        with _mock_converters(traf_return=extracted, full_return=full):
            assert to_markdown("<html>x</html>") == extracted

    # --- stub guard (figure-poor listing pages) ---

    def test_listing_stub_falls_back_to_full(self):
        """Tiny extraction of a large figure-poor page => listing stub => full."""
        full = "# Press Releases\n" + "\n".join(
            f"[SEC Announces New Members Item {i}](/news/{i})" for i in range(300)
        )
        extracted = "# Press Releases\nOfficial announcements highlighting actions."
        assert len(extracted) < 0.10 * len(full) and len(full) > 5000
        with _mock_converters(traf_return=extracted, full_return=full):
            result = to_markdown("<html>x</html>")
        assert result == full
        assert "SEC Announces New Members" in result  # dropped headlines recovered

    def test_substantial_extraction_of_large_page_is_kept(self):
        """A healthy fraction of a large page is a real article, not a stub."""
        full = "Intro. " + "Real article paragraph with detail. " * 400  # ~14k
        extracted = "Real article paragraph with detail. " * 200  # ~37% of full
        assert len(extracted) > 0.10 * len(full)
        with _mock_converters(traf_return=extracted, full_return=full):
            assert to_markdown("<html>x</html>") == extracted

    # --- empty / error fallbacks ---

    def test_empty_extraction_uses_full(self):
        with _mock_converters(traf_return=None, full_return="# Article\n$5 billion."):
            assert to_markdown("<html>x</html>") == "# Article\n$5 billion."

    def test_whitespace_extraction_uses_full(self):
        with _mock_converters(traf_return="   \n  ", full_return="# Article\nbody."):
            assert to_markdown("<html>x</html>") == "# Article\nbody."

    def test_empty_extraction_and_full_failure_uses_plain_text(self):
        html = "<html><body><p>Hello world figure</p><style>.x{color:red}</style></body></html>"
        with _mock_converters(traf_return=None, full_raises=True):
            result = to_markdown(html)
        assert "Hello world figure" in result
        assert "color:red" not in result  # <style> contents stripped

    def test_no_full_baseline_keeps_trafilatura(self):
        """full-page conversion raised but trafilatura succeeded => keep extraction."""
        with _mock_converters(traf_return="Good article $9 billion.", full_raises=True):
            assert to_markdown("<html>x</html>") == "Good article $9 billion."

    def test_oversized_full_page_fallback_is_capped(self):
        """A full-page fallback above the ceiling is truncated to protect context."""
        full = "$1 billion. " * (_MAX_FULL_PAGE_CHARS // 5)  # ~960k chars, over cap
        extracted = "Tiny stub."  # < 10% of a >5k page => stub guard returns full
        with _mock_converters(traf_return=extracted, full_return=full):
            result = to_markdown("<html>x</html>")
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
            result = to_markdown("<html>x</html>")
        assert result == full  # no truncation marker appended



class TestFullPageNoHeadMetaLeak:
    """The full-page fallback must not dump <head> meta tags as frontmatter noise."""

    def test_head_meta_not_emitted(self):
        html = (
            '<html><head>'
            '<meta name="description" content="seo blurb">'
            '<meta property="og:title" content="OG Title">'
            "</head><body><h1>Real Heading</h1><p>Real body content.</p></body></html>"
        )
        out = _try_full_page(html, _silent)
        assert "Real Heading" in out and "Real body content." in out
        assert not out.lstrip().startswith("---")
        assert "meta-description" not in out and "og:title" not in out
