"""EDGAR engine: structured data from SEC filings, built on edgartools.

The public API is what this package exports, plus edgar_engine.filings (filing selection),
edgar_engine.amendments (amended_sections, amendment_scope, merge, read, restates_statements), edgar_engine.cache,
edgar_engine.documents, edgar_engine.forms (item_order, item_title, mdna_item), edgar_engine.statements (Financials,
from_submission, read), edgar_engine.submission (fetching, reading and parsing a filing's .txt), edgar_engine.ownership
(Forms 3, 4 and 5, and Form 144), edgar_engine.thirteenf (Form 13F) and edgar_engine.nport (Form N-PORT); each
declares it in __all__.
Everything else, and every module of edgar_engine.sections except model, is internal.

edgar_engine.periods (13F deadlines and the period expected on a day) is public too. The exports load on first use,
so it and edgar_engine.calendar import without edgartools.
"""
from importlib import import_module

__version__ = "0.1.0"

_EXPORTS = {
    **dict.fromkeys(("Form144Page", "ThirteenFReports", "get_13f_holdings", "get_13f_reports", "get_form144_notices",
                     "get_insider_filings", "get_sections"), ".api"),
    **dict.fromkeys(("SubmissionDocument", "submission_documents"), ".documents"),
    **dict.fromkeys(("SECTIONS_VERSION", "extract_sections", "sections_for_filing"), ".sections"),
    **dict.fromkeys(("DocumentInfo", "FilingSections", "FilingStatus", "Scope", "Section", "Span", "Status", "Target",
                     "TargetKind", "View"), ".sections.model"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name], __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
