"""EDGAR engine: structured data from SEC filings, built on edgartools."""
from .api import get_sections
from .sections import FilingSections, Section, Span, Target, SECTIONS_VERSION, extract_sections, sections_for_filing

__version__ = "0.1.0"

__all__ = ["FilingSections", "SECTIONS_VERSION", "Section", "Span", "Target", "extract_sections", "get_sections",
           "sections_for_filing"]
