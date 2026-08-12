"""
SEC filing parser using regex extraction.

Converts HTML to plain text using html-to-markdown's PLAIN output, then extracts
sections using regex patterns. Works as a fallback when edgartools parser fails.
"""

import re
import logging
from typing import Dict, Optional, Tuple

import html_to_markdown

from .base import BaseSECParser, ParsingFailedError
from ..types import SECSection, FilingType

logger = logging.getLogger(__name__)


# Regex patterns for 10-K sections
FORM_10K_PATTERNS: Dict[str, Tuple[str, str]] = {
    "item_1": (r"Item 1\.\s+Business", r"Item 1A\."),
    "item_1a": (r"Item 1A\.\s+Risk Factors", r"Item 1B\."),
    "item_1b": (r"Item 1B\.\s+Unresolved", r"Item 1C\."),
    "item_1c": (r"Item 1C\.\s+Cybersecurity", r"Item 2\."),
    "item_2": (r"Item 2\.\s+Properties", r"Item 3\."),
    "item_3": (r"Item 3\.\s+Legal", r"Item 4\."),
    "item_4": (r"Item 4\.\s+Mine Safety", r"PART II"),
    "item_5": (r"PART II.*?Item 5\.\s+Market", r"Item 6\."),
    "item_6": (r"Item 6\.\s+\[?Reserved", r"Item 7\."),
    "item_7": (r"Item 7\.\s+Management.s Discussion", r"Item 7A\."),
    "item_7a": (r"Item 7A\.\s+Quantitative", r"Item 8\."),
    "item_8": (r"Item 8\.\s+Financial Statements", r"Item 9\."),
    "item_9": (r"Item 9\.\s+Changes in", r"Item 9A\."),
    "item_9a": (r"Item 9A\.\s+Controls", r"Item 9B\."),
    "item_9b": (r"Item 9B\.\s+Other Information", r"Item 9C\."),
    "item_9c": (r"Item 9C\.\s+Disclosure", r"PART III"),
    "item_10": (r"PART III.*?Item 10\.\s+Directors", r"Item 11\."),
    "item_11": (r"Item 11\.\s+Executive Compensation", r"Item 12\."),
    "item_12": (r"Item 12\.?\s+Security Ownership", r"Item 13\."),
    "item_13": (r"Item 13\.?\s+Certain Relationships", r"Item 14\."),
    "item_14": (r"Item 14\.\s+Principal Accountant", r"PART IV"),
    "item_15": (r"PART IV.*?Item 15\.\s+Exhibit", r"Item 16\."),
    "item_16": (r"Item 16\.\s+Form 10-K Summary", r"SIGNATURES"),
}

# Regex patterns for 10-Q sections
FORM_10Q_PATTERNS: Dict[str, Tuple[str, str]] = {
    "part1_item1": (r"Item 1\.\s+Financial Statements", r"Item 2\."),
    "part1_item2": (r"Item 2\.\s+Management.s Discussion", r"Item 3\."),
    "part1_item3": (r"Item 3\.\s+Quantitative", r"Item 4\."),
    "part1_item4": (r"Item 4\.\s+Controls", r"PART II"),
    "part2_item1": (r"PART II.*?Item 1\.\s+Legal", r"Item 1A\."),
    "part2_item1a": (r"Item 1A\.\s+Risk Factors", r"Item 2\."),
    "part2_item2": (r"Item 2\.\s+Unregistered", r"Item 3\."),
    "part2_item3": (r"Item 3\.\s+Defaults", r"Item 4\."),
    "part2_item4": (r"Item 4\.\s+Mine Safety", r"Item 5\."),
    "part2_item5": (r"Item 5\.\s+Other Information", r"Item 6\."),
    "part2_item6": (r"Item 6\.\s+Exhibits", r"SIGNATURES"),
}


class RegexParser(BaseSECParser):
    """
    Parser using html-to-markdown for HTML-to-text conversion and regex extraction.

    This parser is a reliable fallback that works for both 10-K and 10-Q filings.
    """

    @property
    def name(self) -> str:
        return "regex"

    def supports_filing_type(self, filing_type: FilingType) -> bool:
        """Supports both 10-K and 10-Q."""
        return filing_type in (FilingType.FORM_10K, FilingType.FORM_10Q)

    def _clean_xbrl_content(self, markdown: str) -> str:
        """
        Remove XBRL metadata from the beginning of the content.

        SEC filings use inline XBRL (iXBRL) which embeds structured data
        in the first ~10-15K chars of the document.
        """
        # Look for the standard SEC filing header
        markers = ["UNITED STATES", "SECURITIES AND EXCHANGE COMMISSION"]

        for marker in markers:
            pos = markdown.find(marker)
            if pos > 0 and pos < 20000:  # Should be in first 20K chars
                logger.debug(f"Found content start marker '{marker}' at position {pos}")
                return markdown[pos:]

        # If no marker found, return as-is
        return markdown

    def _extract_section(
        self,
        content: str,
        start_pattern: str,
        end_pattern: str,
        section_key: str,
        min_content_length: int = 1000,
    ) -> Optional[str]:
        """
        Extract a section using regex patterns.

        Skips table of contents entries by requiring minimum content length.
        TOC entries typically have < 500 chars between section headers, while
        actual content sections have thousands of characters.

        Args:
            content: Full markdown content
            start_pattern: Regex pattern for section start
            end_pattern: Regex pattern for section end
            section_key: Key for logging
            min_content_length: Minimum chars to consider valid (skip TOC entries)

        Returns:
            Extracted section text, or None if not found
        """
        try:
            # Find all matches and try each one (to skip TOC entries)
            search_start = 0
            max_attempts = 10  # Prevent infinite loops

            for attempt in range(max_attempts):
                # Find start from current position
                start_match = re.search(
                    start_pattern,
                    content[search_start:],
                    re.IGNORECASE | re.DOTALL,
                )
                if not start_match:
                    return None

                start_pos = search_start + start_match.start()
                remaining = content[start_pos:]

                # Find end (skip first 100 chars to avoid matching start pattern)
                end_match = re.search(end_pattern, remaining[100:], re.IGNORECASE)
                if end_match:
                    end_pos = 100 + end_match.start()
                    section_text = remaining[:end_pos].strip()
                else:
                    # Take up to 50K chars if no end marker found
                    section_text = remaining[:50000].strip()

                # Check if this is actual content (not TOC entry)
                if len(section_text) >= min_content_length:
                    logger.debug(
                        f"Found {section_key} on attempt {attempt + 1} "
                        f"at pos {start_pos} with {len(section_text):,} chars"
                    )
                    return section_text

                # Too short - likely TOC entry, try next match
                logger.debug(
                    f"Skipping TOC entry for {section_key} at pos {start_pos} "
                    f"({len(section_text)} chars < {min_content_length})"
                )
                search_start = start_pos + len(start_match.group())

            # No valid match found after all attempts
            return None

        except Exception as e:
            logger.debug(f"Failed to extract {section_key}: {e}")
            return None

    def parse(
        self,
        html: str,
        filing_type: FilingType,
        sections: Optional[list[str]] = None,
    ) -> Dict[str, SECSection]:
        """
        Parse SEC filing using regex extraction.

        Args:
            html: Raw HTML content of the filing
            filing_type: Type of filing (10-K or 10-Q)
            sections: Optional list of sections to extract

        Returns:
            Dictionary of section key -> SECSection
        """
        # Convert HTML to markdown
        markdown = self._html_to_markdown(html)

        # Clean XBRL metadata
        clean_content = self._clean_xbrl_content(markdown)

        # Get patterns based on filing type
        if filing_type == FilingType.FORM_10K:
            patterns = FORM_10K_PATTERNS
        else:
            patterns = FORM_10Q_PATTERNS

        # Determine which sections to extract
        if sections:
            target_patterns = {k: v for k, v in patterns.items() if k in sections}
        else:
            target_patterns = patterns

        # Extract sections
        result: Dict[str, SECSection] = {}

        for key, (start_pattern, end_pattern) in target_patterns.items():
            text = self._extract_section(
                clean_content,
                start_pattern,
                end_pattern,
                key,
            )
            if text:
                # Generate title from key
                title = self._key_to_title(key, filing_type)
                result[key] = SECSection.from_content(
                    title=title,
                    content=text,
                )
                logger.debug(f"Extracted {key}: {len(text):,} chars")

        if not result:
            raise ParsingFailedError(
                f"No sections could be extracted from {filing_type.value}"
            )

        logger.debug(
            f"regex extracted {len(result)} sections from {filing_type.value}"
        )
        return result

    def _html_to_markdown(self, html: str) -> str:
        """
        Convert HTML to plain text using html-to-markdown's PLAIN output.

        PLAIN flattens tables to tab/newline-separated text (no ``|`` pipe-tables
        that would split ``Item N.`` headers) and drops inline markdown such as
        ``**bold**``, so the section regexes match cleanly. Falls back to a stdlib
        text extractor on failure.

        Args:
            html: Raw HTML content

        Returns:
            Plain-text content
        """
        try:
            return html_to_markdown.convert(
                html, html_to_markdown.ConversionOptions(output_format="plain")
            ).content
        except Exception as e:
            logger.debug(f"html-to-markdown conversion failed: {e}")

        # Fallback: Simple HTML text extraction
        return self._simple_html_to_text(html)

    def _simple_html_to_text(self, html: str) -> str:
        """
        Simple HTML to text extraction as fallback.

        Args:
            html: Raw HTML content

        Returns:
            Plain text content
        """
        from html.parser import HTMLParser

        class TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self.text_parts = []
                self.skip_tags = {"script", "style", "ix:header", "ix:hidden"}
                self.skip_depth = 0

            def handle_starttag(self, tag, attrs):
                if tag.lower() in self.skip_tags:
                    self.skip_depth += 1

            def handle_endtag(self, tag):
                if tag.lower() in self.skip_tags and self.skip_depth > 0:
                    self.skip_depth -= 1

            def handle_data(self, data):
                if self.skip_depth == 0:
                    text = data.strip()
                    if text:
                        self.text_parts.append(text)

        try:
            parser = TextExtractor()
            parser.feed(html)
            return "\n".join(parser.text_parts)
        except Exception as e:
            logger.warning(f"HTML parsing failed: {e}")
            return html

    def _key_to_title(self, key: str, filing_type: FilingType) -> str:
        """Convert section key to human-readable title."""
        from ..types import FORM_10K_SECTIONS, FORM_10Q_SECTIONS

        if filing_type == FilingType.FORM_10K:
            return FORM_10K_SECTIONS.get(key, key)
        else:
            return FORM_10Q_SECTIONS.get(key, key)
