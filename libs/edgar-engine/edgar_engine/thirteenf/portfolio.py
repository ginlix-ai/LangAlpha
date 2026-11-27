"""One filer's 13F portfolio for a period, compared with the period before.

A report lists its holdings line by line, and one security can sit on several lines split by investment discretion or
other manager. A position here is one (CUSIP, share or principal type, put or call): its lines' shares and values
added up. Comparing the positions with the period before gives each one's change, and a position held then and gone
now comes back as a sold_out row with 0 shares.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

NEW, INCREASED, DECREASED, UNCHANGED, SOLD_OUT = "new", "increased", "decreased", "unchanged", "sold_out"
# A position's value status is its worst line's: one line whose value was excluded leaves the sum unknown.
_STATUS_ORDER = ("as_filed", "rescaled", "excluded")
_KEY = ("cusip", "sh_prn", "put_call")


def _key(line: Mapping[str, Any]) -> tuple[str | None, ...]:
    """A missing part (None, NaN as a data set row carries it, or blank) is None, so it joins with its like."""
    return tuple(None if pd.isna(v) or (isinstance(v, str) and not v.strip()) else v
                 for v in (line.get(k) for k in _KEY))


def _amount(v: Any) -> float | None:
    """A share count or value, None when unknown (None, or NaN as a data set row carries it)."""
    return None if v is None or pd.isna(v) else float(v)


def _status(a: str | None, b: str | None) -> str | None:
    rank = {s: i for i, s in enumerate(_STATUS_ORDER)}
    return max((s for s in (a, b) if s is not None), key=lambda s: rank.get(s, -1), default=None)


def positions(lines: Iterable[Mapping[str, Any]]) -> dict[tuple, dict[str, Any]]:
    """Lines added up by (cusip, sh_prn, put_call), in the order each key first appears.

    Each line carries cusip, sh_prn, put_call, shares and value (each None or NaN when unknown), and optionally
    value_status, issuer, title_class, ticker and accession. A position's shares or value is None when any of its
    lines' is; its accession joins its lines' distinct accessions with ";", sorted.
    """
    out: dict[tuple, dict[str, Any]] = {}
    for line in lines:
        k = _key(line)
        p = out.get(k)
        shares, value = _amount(line.get("shares")), _amount(line.get("value"))
        if p is None:
            out[k] = {**dict(zip(_KEY, k)), "issuer": line.get("issuer"),
                      "title_class": line.get("title_class"), "ticker": line.get("ticker"),
                      "shares": shares, "value": value, "value_status": line.get("value_status"),
                      "accession": {line["accession"]} if line.get("accession") else set()}
            continue
        p["shares"] = None if p["shares"] is None or shares is None else p["shares"] + shares
        p["value"] = None if p["value"] is None or value is None else p["value"] + value
        p["value_status"] = _status(p["value_status"], line.get("value_status"))
        if line.get("accession"):
            p["accession"].add(line["accession"])
        for n in ("issuer", "title_class", "ticker"):
            if p[n] is None:
                p[n] = line.get(n)
    for p in out.values():
        p["accession"] = ";".join(sorted(p["accession"])) or None
    return out


def _order(row: Mapping[str, Any]) -> tuple:
    group = 2 if row["change"] == SOLD_OUT else 1 if row["value"] is None else 0
    return (group, -(row["value"] or 0.0), *((row[k] or "") for k in _KEY))


def portfolio(lines: Iterable[Mapping[str, Any]],
              prior_lines: Iterable[Mapping[str, Any]] | None, *,
              omits_confidential: bool = False) -> list[dict[str, Any]]:
    """The period's positions (see positions) with weight, change and shares_prior, plus a sold_out row (shares 0,
    value 0) for each position of the period before that is gone.

    prior_lines are the filer's lines for the period before, [] when it reported none then; None when that period
    cannot be read, which leaves change and shares_prior None. omits_confidential is True when either period's
    reports leave holdings out under confidential treatment (their confidential_omitted): a position missing from
    one of them may still be held, so change and shares_prior are None and no sold_out rows come back, as when
    prior_lines is None. change is new, increased, decreased, unchanged or
    sold_out, on shares, and None too for a position held in both periods whose shares are unknown in either;
    shares_prior is 0 for a new position. weight is the value over the period's total value of
    positions with a known value, None when its own value is unknown.

    Ordered largest value first, then positions with an unknown value, then sold_out rows, each group by cusip,
    sh_prn and put_call, so the order is total.
    """
    now = positions(lines)
    prior = None if prior_lines is None or omits_confidential else positions(prior_lines)
    total = sum(p["value"] for p in now.values() if p["value"] is not None)
    rows = []
    for k, p in now.items():
        if prior is None:
            before, change = None, None
        elif k not in prior:
            before, change = 0.0, NEW
        else:
            before = prior[k]["shares"]
            change = (None if before is None or p["shares"] is None else INCREASED if p["shares"] > before
                      else DECREASED if p["shares"] < before else UNCHANGED)
        weight = round(p["value"] / total, 6) if p["value"] is not None and total > 0 else None
        rows.append({**p, "weight": weight, "change": change, "shares_prior": before})
    for k, p in (prior or {}).items():
        if k not in now:
            rows.append({**p, "shares": 0.0, "value": 0.0, "value_status": "as_filed", "accession": None,
                         "weight": 0.0, "change": SOLD_OUT, "shares_prior": p["shares"]})
    return sorted(rows, key=_order)
