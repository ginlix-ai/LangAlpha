"""Unit tests for RegexParser (SEC filing regex-based section extraction)."""

from unittest.mock import patch

import pytest

from src.tools.sec.parsers.base import ParsingFailedError
from src.tools.sec.parsers.regex_parser import RegexParser
from src.tools.sec.types import FilingType, SECSection


# ---------------------------------------------------------------------------
# Helpers — sample filing content builders
# ---------------------------------------------------------------------------


def _build_10k_html(sections: dict[str, str] | None = None) -> str:
    """Build a minimal 10-K HTML document with the given sections embedded.

    Each section has enough padding to pass the min_content_length=1000 check.
    """
    if sections is None:
        sections = {
            "item_1": "Item 1. Business\n" + "Business description. " * 200,
            "item_1a": "Item 1A. Risk Factors\n" + "Risk factor details. " * 200,
            "item_7": "Item 7. Management's Discussion\n" + "MD&A discussion. " * 200,
            "item_8": "Item 8. Financial Statements\n" + "Financial data. " * 200,
        }

    body_parts = []
    for text in sections.values():
        body_parts.append(f"<p>{text}</p>")

    # Add surrounding markers
    body = (
        "<p>UNITED STATES</p>"
        "<p>SECURITIES AND EXCHANGE COMMISSION</p>"
        + "".join(body_parts)
        + "<p>Item 9. Changes in and Disagreements with Accountants</p>"
    )
    return f"<html><body>{body}</body></html>"


def _build_10q_html(sections: dict[str, str] | None = None) -> str:
    """Build a minimal 10-Q HTML document."""
    if sections is None:
        sections = {
            "part1_item2": "Item 2. Management's Discussion\n" + "Quarterly MD&A. " * 200,
            "part2_item1a": "Item 1A. Risk Factors\n" + "Updated risk factors. " * 200,
        }

    body_parts = []
    for text in sections.values():
        body_parts.append(f"<p>{text}</p>")

    body = (
        "<p>UNITED STATES</p>"
        "<p>SECURITIES AND EXCHANGE COMMISSION</p>"
        + "".join(body_parts)
        + "<p>Item 2. Unregistered Sales of Equity Securities</p>"
    )
    return f"<html><body>{body}</body></html>"


def _make_markdown_content(sections: dict[str, str]) -> str:
    """Build raw markdown text (already converted from HTML) with section headers."""
    parts = ["UNITED STATES\nSECURITIES AND EXCHANGE COMMISSION\n\n"]
    for text in sections.values():
        parts.append(text + "\n\n")
    return "".join(parts)


# ---------------------------------------------------------------------------
# name property
# ---------------------------------------------------------------------------


class TestName:
    """Tests for the name property."""

    def test_name_returns_regex(self):
        parser = RegexParser()
        assert parser.name == "regex"


# ---------------------------------------------------------------------------
# supports_filing_type
# ---------------------------------------------------------------------------


class TestSupportsFilingType:
    """Tests for filing type support checks."""

    def setup_method(self):
        self.parser = RegexParser()

    def test_supports_10k(self):
        assert self.parser.supports_filing_type(FilingType.FORM_10K) is True

    def test_supports_10q(self):
        assert self.parser.supports_filing_type(FilingType.FORM_10Q) is True

    def test_does_not_support_8k(self):
        assert self.parser.supports_filing_type(FilingType.FORM_8K) is False


# ---------------------------------------------------------------------------
# _clean_xbrl_content
# ---------------------------------------------------------------------------


class TestCleanXbrlContent:
    """Tests for XBRL metadata stripping."""

    def setup_method(self):
        self.parser = RegexParser()

    def test_finds_united_states_marker(self):
        xbrl_prefix = "ix:nonfraction xmlns blah blah " * 50
        content = xbrl_prefix + "UNITED STATES\nSECURITIES AND EXCHANGE COMMISSION\nActual content"
        result = self.parser._clean_xbrl_content(content)
        assert result.startswith("UNITED STATES")
        assert "ix:nonfraction" not in result

    def test_finds_sec_commission_marker(self):
        """Falls through to second marker when first is not present."""
        xbrl_prefix = "xbrli:context id='abc' " * 50
        content = xbrl_prefix + "SECURITIES AND EXCHANGE COMMISSION\nWashington"
        result = self.parser._clean_xbrl_content(content)
        assert result.startswith("SECURITIES AND EXCHANGE COMMISSION")

    def test_no_marker_returns_as_is(self):
        content = "Some random document without any SEC markers at all."
        result = self.parser._clean_xbrl_content(content)
        assert result == content

    def test_marker_at_position_zero_ignored(self):
        """Marker at pos=0 is not > 0, so it is skipped (no XBRL to strip)."""
        content = "UNITED STATES\nSECURITIES AND EXCHANGE COMMISSION\nContent"
        result = self.parser._clean_xbrl_content(content)
        # pos == 0 means the condition `pos > 0` is False, falls through
        # Second marker "SECURITIES AND EXCHANGE COMMISSION" is at pos > 0
        assert "SECURITIES AND EXCHANGE COMMISSION" in result

    def test_marker_beyond_20k_ignored(self):
        """Marker past 20K chars should be ignored."""
        padding = "x" * 25000
        content = padding + "UNITED STATES\nActual content"
        result = self.parser._clean_xbrl_content(content)
        # Should return as-is since marker is beyond 20K
        assert result == content


# ---------------------------------------------------------------------------
# _extract_section
# ---------------------------------------------------------------------------


class TestExtractSection:
    """Tests for regex-based section extraction."""

    def setup_method(self):
        self.parser = RegexParser()

    def test_happy_path_extracts_between_patterns(self):
        """Content between start and end patterns is extracted."""
        section_body = "Detailed business description. " * 100  # well over 1000 chars
        content = (
            "Preamble text.\n"
            "Item 1. Business\n"
            + section_body
            + "\nItem 1A. Risk Factors\n"
            "Risk factor content."
        )
        result = self.parser._extract_section(
            content,
            r"Item 1\.\s+Business",
            r"Item 1A\.",
            "item_1",
        )
        assert result is not None
        assert "Item 1. Business" in result
        assert "Detailed business description" in result
        # Should not include the end pattern's section content
        assert "Risk factor content" not in result

    def test_start_pattern_not_found_returns_none(self):
        content = "This document has no matching section headers at all."
        result = self.parser._extract_section(
            content,
            r"Item 99\.\s+Nonexistent",
            r"Item 100\.",
            "item_99",
        )
        assert result is None

    def test_no_end_pattern_takes_up_to_50k(self):
        """When end pattern is missing, extracts up to 50K chars."""
        section_body = "A" * 60000
        content = "Item 7. Management's Discussion\n" + section_body
        result = self.parser._extract_section(
            content,
            r"Item 7\.\s+Management.s Discussion",
            r"Item 7A\.",  # not present in content
            "item_7",
        )
        assert result is not None
        assert len(result) == 50000

    def test_toc_entry_skipped(self):
        """Short content (< min_content_length) is treated as TOC entry and skipped."""
        # First match: short TOC entry
        toc_entry = "Item 1. Business...............23\n"
        # Second match: actual section content
        real_content = "Item 1. Business\n" + "Real business details here. " * 100
        content = toc_entry + "\n" + real_content + "\nItem 1A. Risk Factors"

        result = self.parser._extract_section(
            content,
            r"Item 1\.\s+Business",
            r"Item 1A\.",
            "item_1",
            min_content_length=500,
        )
        assert result is not None
        assert "Real business details" in result

    def test_max_attempts_exhausted_returns_none(self):
        """After 10 short matches, returns None."""
        # Create 15 short matches - all below min_content_length
        entries = []
        for _ in range(15):
            entries.append("Item 1. Business\nShort.\n")
        content = "".join(entries) + "Item 1A. Risk Factors"

        result = self.parser._extract_section(
            content,
            r"Item 1\.\s+Business",
            r"Item 1A\.",
            "item_1",
            min_content_length=5000,
        )
        assert result is None

    def test_case_insensitive_matching(self):
        """Patterns match case-insensitively."""
        body = "Details about the business. " * 100
        content = "item 1. business\n" + body + "\nitem 1a. risk factors"
        result = self.parser._extract_section(
            content,
            r"Item 1\.\s+Business",
            r"Item 1A\.",
            "item_1",
        )
        assert result is not None
        assert "Details about the business" in result

    def test_regex_error_returns_none(self):
        """Invalid regex pattern is caught and returns None."""
        result = self.parser._extract_section(
            "Some content",
            r"[invalid regex",  # unclosed bracket
            r"end",
            "bad_section",
        )
        assert result is None


# ---------------------------------------------------------------------------
# parse — integration tests
# ---------------------------------------------------------------------------


class TestParse:
    """Tests for the full parse() pipeline."""

    def setup_method(self):
        self.parser = RegexParser()

    def test_10k_with_sections_filter(self):
        """Parse a 10-K with specific sections requested."""
        # Build markdown content that looks like a converted 10-K
        item1_body = "Business overview and description. " * 100
        item1a_body = "Risks related to our business. " * 100
        item7_body = "Management discusses financial condition. " * 100

        markdown = (
            "UNITED STATES\n"
            "SECURITIES AND EXCHANGE COMMISSION\n\n"
            "Item 1. Business\n" + item1_body + "\n"
            "Item 1A. Risk Factors\n" + item1a_body + "\n"
            "Item 1B. Unresolved Staff Comments\n"
            "No unresolved comments.\n"
            "Item 1C. Cybersecurity\n"
            "Cyber info.\n"
            "Item 2. Properties\n"
            "Property info.\n"
            "Item 7. Management's Discussion\n" + item7_body + "\n"
            "Item 7A. Quantitative and Qualitative\n"
            "Market risk details.\n"
            "Item 8. Financial Statements\n"
        )
        html = f"<html><body><pre>{markdown}</pre></body></html>"

        result = self.parser.parse(
            html,
            FilingType.FORM_10K,
            sections=["item_1", "item_1a"],
        )

        assert "item_1" in result
        assert "item_1a" in result
        assert len(result) == 2
        assert isinstance(result["item_1"], SECSection)
        assert result["item_1"].length > 0
        assert "Business overview" in result["item_1"].content

    def test_10q_parse(self):
        """Parse a 10-Q filing."""
        part1_item2_body = "Quarterly management discussion. " * 100
        part2_item1a_body = "Updated risk factor information. " * 100

        markdown = (
            "UNITED STATES\n"
            "SECURITIES AND EXCHANGE COMMISSION\n\n"
            "PART I\n"
            "Item 1. Financial Statements\n"
            "Financial tables here.\n"
            "Item 2. Management's Discussion\n" + part1_item2_body + "\n"
            "Item 3. Quantitative and Qualitative\n"
            "Market risk.\n"
            "Item 4. Controls and Procedures\n"
            "Controls info.\n"
            "PART II\n"
            "Item 1. Legal Proceedings\n"
            "Legal info here for part two content. " * 60 + "\n"
            "Item 1A. Risk Factors\n" + part2_item1a_body + "\n"
            "Item 2. Unregistered Sales of Equity Securities\n"
        )
        html = f"<html><body><pre>{markdown}</pre></body></html>"

        result = self.parser.parse(
            html,
            FilingType.FORM_10Q,
            sections=["part1_item2", "part2_item1a"],
        )

        assert "part1_item2" in result
        assert "part2_item1a" in result
        assert "Quarterly management discussion" in result["part1_item2"].content
        assert "Updated risk factor" in result["part2_item1a"].content

    def test_sections_none_extracts_all_patterns(self):
        """When sections=None, all matching patterns are attempted."""
        item1_body = "Business overview content. " * 100
        item1a_body = "Risk factors details. " * 100

        markdown = (
            "UNITED STATES\n\n"
            "Item 1. Business\n" + item1_body + "\n"
            "Item 1A. Risk Factors\n" + item1a_body + "\n"
            "Item 1B. Unresolved Staff Comments\n"
        )
        html = f"<html><body><pre>{markdown}</pre></body></html>"

        result = self.parser.parse(html, FilingType.FORM_10K, sections=None)

        # Should find at least item_1 and item_1a
        assert "item_1" in result
        assert "item_1a" in result

    def test_no_sections_found_raises_parsing_failed(self):
        """When no sections can be extracted, raises ParsingFailedError."""
        html = "<html><body><p>This document has no recognizable SEC sections.</p></body></html>"

        with pytest.raises(ParsingFailedError, match="No sections could be extracted"):
            self.parser.parse(html, FilingType.FORM_10K)

    def test_partial_extraction(self):
        """Only some sections are found -- returns what was found."""
        item7_body = "Management discusses the financial condition. " * 100

        markdown = (
            "UNITED STATES\n\n"
            "Item 7. Management's Discussion\n" + item7_body + "\n"
            "Item 7A. Quantitative and Qualitative\n"
        )
        html = f"<html><body><pre>{markdown}</pre></body></html>"

        result = self.parser.parse(
            html,
            FilingType.FORM_10K,
            sections=["item_1", "item_7"],
        )

        # item_1 was requested but not present in the document
        assert "item_1" not in result
        # item_7 was found
        assert "item_7" in result
        assert "Management discusses" in result["item_7"].content

    def test_section_has_correct_title(self):
        """Extracted sections have human-readable titles from the section mappings."""
        item1_body = "Business content here. " * 100

        markdown = (
            "UNITED STATES\n\n"
            "Item 1. Business\n" + item1_body + "\n"
            "Item 1A. Risk Factors\n"
        )
        html = f"<html><body><pre>{markdown}</pre></body></html>"

        result = self.parser.parse(
            html, FilingType.FORM_10K, sections=["item_1"]
        )

        assert result["item_1"].title == "Item 1"

    def test_section_length_matches_content(self):
        """SECSection.length equals len(content)."""
        item1_body = "Business info. " * 100

        markdown = (
            "UNITED STATES\n\n"
            "Item 1. Business\n" + item1_body + "\n"
            "Item 1A. Risk Factors\n"
        )
        html = f"<html><body><pre>{markdown}</pre></body></html>"

        result = self.parser.parse(
            html, FilingType.FORM_10K, sections=["item_1"]
        )

        section = result["item_1"]
        assert section.length == len(section.content)


# ---------------------------------------------------------------------------
# _html_to_markdown
# ---------------------------------------------------------------------------


class TestHtmlToMarkdown:
    """Tests for HTML-to-markdown conversion."""

    def setup_method(self):
        self.parser = RegexParser()

    def test_happy_path(self):
        html = "<h1>Annual Report</h1><p>Company overview paragraph.</p>"
        result = self.parser._html_to_markdown(html)
        assert "Annual Report" in result
        assert "Company overview paragraph" in result

    def test_link_text_preserved(self):
        """PLAIN output keeps anchor text (URLs are dropped — fine for section regexes)."""
        html = '<p>See <a href="https://sec.gov/filing">the filing</a>.</p>'
        result = self.parser._html_to_markdown(html)
        assert "the filing" in result

    def test_conversion_exception_falls_back(self):
        """When html-to-markdown conversion raises, falls back to _simple_html_to_text."""
        html = "<p>Fallback content here</p>"

        with patch(
            "src.tools.sec.parsers.regex_parser.html_to_markdown.convert",
            side_effect=RuntimeError("conversion broken"),
        ):
            result = self.parser._html_to_markdown(html)

        assert "Fallback content here" in result

    def test_tables_flattened_not_pipe_syntax(self):
        """Table cells render as plain text (no pipe-table syntax) so section regexes match."""
        html = "<table><tr><td>Item 15.</td><td>Exhibits</td></tr></table>"
        result = self.parser._html_to_markdown(html)

        assert "Item 15." in result
        assert "Exhibits" in result
        assert "|" not in result  # not a markdown pipe-table


# ---------------------------------------------------------------------------
# _simple_html_to_text
# ---------------------------------------------------------------------------


class TestSimpleHtmlToText:
    """Tests for the fallback HTML-to-text extractor."""

    def setup_method(self):
        self.parser = RegexParser()

    def test_normal_text_preserved(self):
        html = "<html><body><p>Hello world</p><p>Second paragraph</p></body></html>"
        result = self.parser._simple_html_to_text(html)
        assert "Hello world" in result
        assert "Second paragraph" in result

    def test_script_tags_filtered(self):
        html = (
            "<html><body>"
            "<script>var x = 1;</script>"
            "<p>Visible text</p>"
            "</body></html>"
        )
        result = self.parser._simple_html_to_text(html)
        assert "var x = 1" not in result
        assert "Visible text" in result

    def test_style_tags_filtered(self):
        html = (
            "<html><body>"
            "<style>.cls { color: red; }</style>"
            "<p>Styled text</p>"
            "</body></html>"
        )
        result = self.parser._simple_html_to_text(html)
        assert "color: red" not in result
        assert "Styled text" in result

    def test_ix_header_tags_filtered(self):
        """iXBRL header tags should be filtered out."""
        html = (
            "<html><body>"
            "<ix:header>XBRL metadata stuff</ix:header>"
            "<p>Real content</p>"
            "</body></html>"
        )
        result = self.parser._simple_html_to_text(html)
        assert "XBRL metadata" not in result
        assert "Real content" in result

    def test_ix_hidden_tags_filtered(self):
        """iXBRL hidden tags should be filtered out."""
        html = (
            "<html><body>"
            "<ix:hidden>Hidden XBRL data</ix:hidden>"
            "<p>Visible content</p>"
            "</body></html>"
        )
        result = self.parser._simple_html_to_text(html)
        assert "Hidden XBRL data" not in result
        assert "Visible content" in result

    def test_nested_skip_tags(self):
        """Nested script inside style should still be filtered."""
        html = (
            "<html><body>"
            "<style><script>nested bad</script>.cls{}</style>"
            "<p>Good text</p>"
            "</body></html>"
        )
        result = self.parser._simple_html_to_text(html)
        assert "nested bad" not in result
        assert "Good text" in result

    def test_empty_html(self):
        result = self.parser._simple_html_to_text("<html><body></body></html>")
        assert result.strip() == ""

    def test_html_parser_exception_returns_raw_html(self):
        """When HTMLParser.feed() raises, returns the raw html."""
        html = "<p>Some content</p>"

        from html.parser import HTMLParser as RealHTMLParser

        def broken_feed(self, data):
            raise Exception("Parser exploded")

        with patch.object(RealHTMLParser, "feed", broken_feed):
            result = self.parser._simple_html_to_text(html)

        assert result == html

    def test_whitespace_only_content_not_included(self):
        """Whitespace-only text nodes are stripped and excluded."""
        html = "<html><body>  <p>  </p>  <p>Real</p>  </body></html>"
        result = self.parser._simple_html_to_text(html)
        lines = [line for line in result.split("\n") if line.strip()]
        assert len(lines) == 1
        assert "Real" in lines[0]
