"""A filing's XBRL statements and key metrics, read with edgartools from the filing or from its full submission text.

The submission path builds the XBRL from the documents the .txt carries, so a caller holding the text (a pipeline
that already fetched it) reads statements with no further request.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .cache import StatementsCache
    from .filings import FilingRef

__all__ = ["STATEMENTS_VERSION", "Financials", "from_submission", "read"]

logger = logging.getLogger(__name__)

#: Bumped when what a stored Financials holds changes; the edgartools version is part of the cache key too
#: (StatementsCache), since its rendering decides the statements' text. 4: XBRL whose instance document is missing is
#: no longer kept as a filing without statements (version 3 kept it, so no reader trusts a version 3 entry).
STATEMENTS_VERSION = 4
_STATEMENTS = (("income statement", "income_statement"), ("balance sheet", "balance_sheet"),
              ("cash flow statement", "cash_flow_statement"))


@dataclass(frozen=True)
class Financials:
    """Key metrics in the reporting currency (an ISO code, None when the filing does not state one) and the three
    statements the filing has as markdown, under the names in held. unread: the parts that could not be read this
    time, and failed: none could; neither is ever cached."""
    metrics: dict = field(default_factory=dict)
    statements: str | None = None
    currency: str | None = None
    failed: bool = False
    unread: tuple[str, ...] = ()
    held: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        """Every part read, so the result can be kept."""
        return not self.failed and not self.unread

    @classmethod
    def from_dict(cls, payload: dict) -> Financials:
        return cls(**{**payload, "unread": tuple(payload.get("unread", ())), "held": tuple(payload.get("held", ()))})


def edgartools_version() -> str:
    return version("edgartools")


def _plain(value: Any) -> Any:
    try:
        return float(value) if not isinstance(value, (str, bool, type(None))) else value
    except (TypeError, ValueError):
        return str(value)


def _currency(fin: Any) -> str | None:
    """The ISO code of the currency most of the filing's monetary XBRL facts are in; None when no fact names one, or
    two are used equally, rather than a guess. Counted by fact, not by unit definition, since a dollar filer also
    defines the currencies of a few disclosures (Tesla's yuan)."""
    xb = getattr(fin, "xb", None)
    units = getattr(xb, "units", None) or {}
    measures = {uid: str(u.get("measure", "")).strip() for uid, u in units.items() if u.get("type") == "simple"}
    currency = {uid: m.removeprefix("iso4217:") for uid, m in measures.items() if m.startswith("iso4217:")}
    facts = (getattr(xb, "_facts", None) or {}).values()
    codes = Counter(currency[f.unit_ref] for f in facts if getattr(f, "unit_ref", None) in currency).most_common(2)
    if not codes or (len(codes) == 2 and codes[0][1] == codes[1][1]):
        return None
    return codes[0][0]


def _read(filing: Any) -> tuple[dict, dict[str, str], str | None, list[str]]:
    """(key metrics, each statement that rendered as markdown by its name, the reporting currency, the parts that
    failed to read), or ({}, {}, None, []) for a filing with no XBRL. XBRL without facts (its instance document is
    missing, as in an inline-XBRL submission's text from before late 2020) reads every part as unread, so it is never
    kept as a filing without statements. A statement that fails to render is left out whole, never kept partial.
    filing is an edgartools Filing or anything with its form and attachments."""
    from edgar.financials import Financials as Extracted

    fin = Extracted.extract(filing)
    if fin is None:
        return {}, {}, None, []
    xb = getattr(fin, "xb", None)
    if xb is not None and not getattr(xb, "_facts", None):
        return {}, {}, None, [name for name, _ in _STATEMENTS] + ["key metrics"]
    parts, unread = {}, []
    for name, method in _STATEMENTS:
        try:
            stmt = getattr(fin, method)()
            if not stmt:
                continue
            rendered = stmt.render()
            if rendered is None:
                continue
            text = rendered.to_markdown() if hasattr(rendered, "to_markdown") else str(rendered)
            if text:
                parts[name] = text
        except Exception:
            unread.append(name)
            logger.debug("edgar_engine: %s failed to render", method, exc_info=True)
    try:
        metrics = {k: _plain(v) for k, v in (fin.get_financial_metrics() or {}).items() if v is not None}
    except Exception:
        unread.append("key metrics")
        logger.debug("edgar_engine: metrics failed", exc_info=True)
        metrics = {}
    return metrics, parts, _currency(fin), unread


def _financials(filing: Any, accession: str) -> Financials:
    try:
        metrics, parts, currency, unread = _read(filing)
    except Exception:
        logger.warning("edgar_engine: statements of %s unavailable", accession, exc_info=True)
        return Financials(failed=True)
    if unread and not (metrics or parts):
        return Financials(failed=True)
    return Financials(metrics, "\n\n".join(parts.values()) or None, currency, unread=tuple(unread), held=tuple(parts))


def from_submission(text: str, ref: FilingRef) -> Financials:
    """read's statements and key metrics from the filing's full submission text, with no request: the edgartools
    Filing is built from ref with the text as its parsed submission, so the XBRL and the header (the period of report
    its dates are checked against, the filer's SIC) both come from the text. The one exception is edgartools' own:
    with its local storage and network fallback turned on (EDGAR_USE_LOCAL_DATA), it fetches an instance document the
    text lacks from the filing's index page."""
    from edgar.sgml.sgml_common import FilingSGML

    from .filings import get_filing

    filing = get_filing(ref)
    filing._sgml = FilingSGML.from_text(text)
    return _financials(filing, ref.accession)


def read(ref: FilingRef, cache: StatementsCache | None = None) -> Financials:
    """A filing's statements and key metrics, through cache when given; only a complete result is kept."""
    from .filings import get_filing

    if cache is not None and (hit := cache.get(ref.accession)) is not None:
        return hit
    fin = _financials(get_filing(ref), ref.accession)
    if cache is not None and fin.complete:
        cache.put(ref.accession, fin)
    return fin
