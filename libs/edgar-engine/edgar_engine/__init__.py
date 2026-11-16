"""EDGAR engine: structured data from SEC filings, built on edgartools.

The public API is what this package exports, plus edgar_engine.filings (filing selection), edgar_engine.amendments
(amendment_scope, merge) and edgar_engine.cache; each declares it in __all__. Everything else, and every module
of edgar_engine.sections except model, is internal.
"""
from .api import get_sections
from .sections import SECTIONS_VERSION, extract_sections, sections_for_filing
from .sections.model import (DocumentInfo, FilingSections, FilingStatus, Scope, Section, Span, Status, Target, TargetKind,
                             View)

__version__ = "0.1.0"

__all__ = ["DocumentInfo", "FilingSections", "FilingStatus", "SECTIONS_VERSION", "Scope", "Section", "Span", "Status",
           "Target", "TargetKind", "View", "extract_sections", "get_sections", "sections_for_filing"]
