"""XBRL statements and key metrics of a filing, read with edgartools."""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)

STATEMENTS = (("income statement", "income_statement"), ("balance sheet", "balance_sheet"),
               ("cash flow statement", "cash_flow_statement"))


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
    currency = {uid: str(u.get("measure", "")).removeprefix("iso4217:") for uid, u in units.items()
                if u.get("type") == "simple" and str(u.get("measure", "")).startswith("iso4217:")}
    facts = (getattr(xb, "_facts", None) or {}).values()
    codes = Counter(currency[f.unit_ref] for f in facts if getattr(f, "unit_ref", None) in currency).most_common(2)
    if not codes or (len(codes) == 2 and codes[0][1] == codes[1][1]):
        return None
    return codes[0][0]


def read_financials(filing: Any) -> tuple[dict, dict[str, str], str | None, list[str]]:
    """(key metrics, each statement that rendered as markdown by its name, the reporting currency, the parts that
    failed to read), or ({}, {}, None, []) for a filing with no XBRL. A statement that fails to render is left out
    whole, never kept partial."""
    from edgar.financials import Financials

    fin = Financials.extract(filing)
    if fin is None:
        return {}, {}, None, []
    parts, unread = {}, []
    for name, method in STATEMENTS:
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
            logger.debug("sec_filing: %s failed to render", method, exc_info=True)
    try:
        metrics = {k: _plain(v) for k, v in (fin.get_financial_metrics() or {}).items() if v is not None}
    except Exception:
        unread.append("key metrics")
        logger.debug("sec_filing: metrics failed", exc_info=True)
        metrics = {}
    return metrics, parts, _currency(fin), unread
