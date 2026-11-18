"""EDGAR engine: structured data from SEC filings, built on edgartools.

The public API is what this package exports, plus edgar_engine.filings (filing selection),
edgar_engine.amendments (amended_sections, amendment_scope, merge, read, restates_statements), edgar_engine.cache,
edgar_engine.documents, edgar_engine.forms (item_order, item_title, mdna_item) and edgar_engine.submission; each
declares it in __all__.
Everything else, and every module of edgar_engine.sections except model, is internal.
"""
from .api import get_sections
from .documents import SubmissionDocument, submission_documents
from .sections import SECTIONS_VERSION, extract_sections, sections_for_filing
from .sections.model import (DocumentInfo, FilingSections, FilingStatus, Scope, Section, Span, Status, Target, TargetKind,
                             View)

__version__ = "0.1.0"

__all__ = ["DocumentInfo", "FilingSections", "FilingStatus", "SECTIONS_VERSION", "Scope", "Section", "Span", "Status",
           "SubmissionDocument", "Target", "TargetKind", "View", "extract_sections", "get_sections", "sections_for_filing",
           "submission_documents"]
