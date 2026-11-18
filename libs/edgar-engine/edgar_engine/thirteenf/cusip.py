"""CUSIP to the exchange ticker of the same security, from edgartools' bundled CUSIP snapshot.

A CUSIP whose check digit fails is a typo or a pseudo-CUSIP (option series printed as their own "CUSIP"), and maps
to nothing rather than to a near neighbour. The snapshot writes class tickers without a separator (BRKB); a ticker
the SEC's company list does not hold is rewritten to the one listed ticker that differs only by its class separator
(BRK-B), and kept as is when there is none or more than one.
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable

_CUSIP = re.compile(r"^[0-9A-Z*@#]{8}[0-9]$")


def check_digit_ok(cusip: str) -> bool:
    """The CUSIP modulus-10 double-add-double check on the first eight characters."""
    if not _CUSIP.match(cusip):
        return False
    total = 0
    for i, ch in enumerate(cusip[:8]):
        v = int(ch) if ch.isdigit() else ord(ch) - 55 if ch.isalpha() else {"*": 36, "@": 37, "#": 38}[ch]
        v = v * 2 if i % 2 else v
        total += v // 10 + v % 10
    return str((10 - total % 10) % 10) == cusip[8]


@lru_cache(maxsize=1)
def _snapshot() -> dict[str, str]:
    from edgar.reference.tickers import cusip_ticker_mapping

    df = cusip_ticker_mapping(allow_duplicate_cusips=False).reset_index()
    return {str(c).strip().upper(): str(t).strip().upper() for c, t in zip(df.Cusip, df.Ticker) if c and t}


@lru_cache(maxsize=1)
def _listed() -> tuple[frozenset[str], dict[str, str]]:
    from edgar.reference.tickers import get_company_tickers

    tickers = {str(t).strip().upper() for t in get_company_tickers(clean_name=False).ticker if t}
    by_collapsed: dict[str, set[str]] = {}
    for t in tickers:
        c = re.sub(r"[^A-Z0-9]", "", t)
        if c != t:
            by_collapsed.setdefault(c, set()).add(t)
    return frozenset(tickers), {c: next(iter(s)) for c, s in by_collapsed.items() if len(s) == 1}


def class_ticker(ticker: str) -> str:
    listed, separated = _listed()
    return ticker if ticker in listed else separated.get(ticker, ticker)


def ticker_for_cusip(cusip: str | None) -> str | None:
    if not cusip:
        return None
    c = cusip.strip().upper()
    if not check_digit_ok(c):
        return None
    t = _snapshot().get(c)
    return class_ticker(t) if t else None


def tickers_for_cusips(cusips: Iterable[str | None]) -> dict[str, str | None]:
    return {c: ticker_for_cusip(c) for c in set(cusips) if c}
