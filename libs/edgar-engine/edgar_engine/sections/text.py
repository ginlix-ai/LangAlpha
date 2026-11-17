"""Line text the layouts share: normalize, and the vocabulary patterns more than one module reads.

Patterns that look alike but differ in anchoring or breadth are kept apart and named for the difference:
merging them would move item ends. STATEMENTS_MARKER ends an item at the statements' first page and so
also takes a bare balance sheet title; STATEMENTS_OPENER is where a stub's statements target starts, and
takes any title after an index or auditor's report. EXHIBIT_INDEX is the index a pointer names; a 40-F
also heads its list with a bare "Exhibits" (EXHIBIT_LIST).
"""
from __future__ import annotations

import re

_DASH = re.compile(r"[\u2010-\u2015\u2212]")
_LEAD = re.compile(r"^[\s|*\u2022\u00b7\u25cf\u25aa>]+")
_SPACED = re.compile(r"^((?:[A-Za-z] ){2,}[A-Za-z])(?=[\s\d.:\-]|$)")
_SPACED_WORDS = {"ITEM", "ITEMS", "PART", "SIGNATURE", "SIGNATURES"}
_WS = re.compile(r"\s+")
_NUMERAL = re.compile(r"[IVX]+", re.I)

SIGNATURES = re.compile(r"SIGNATURES?(?:\s*\(S\))?(?:\s+PAGES?)?\s*[.:]?", re.I)
SIGNATURE_PARAGRAPH = re.compile(r"Pursuant to the requirements of\b", re.I)
SENTENCE = re.compile(r"\w\W*\s+\S.*[.;]$")
CONTENTS = re.compile(r"\b(?:TABLE OF CONTENTS|CONTENTS|INDEX)\b", re.I)
STATEMENTS_MARKER = re.compile(r"(?:REPORTS? OF INDEPENDENT (?:REGISTERED PUBLIC ACCOUNTING FIRMS?|AUDITORS?|ACCOUNTANTS?|"
                               r"CERTIFIED PUBLIC ACCOUNTANTS?)|INDEX TO (?:THE )?(?:CONSOLIDATED )?FINANCIAL STATEMENTS\b.*|"
                               r"REPORT AND (?:CONSOLIDATED )?FINANCIAL STATEMENTS|CONSOLIDATED BALANCE SHEETS?)\s*[.:]?", re.I)
STATEMENTS_OPENER = re.compile(r"(?:INDEX TO (?:THE )?(?:CONSOLIDATED )?FINANCIAL STATEMENTS\b.*|REPORTS? OF INDEPENDENT "
                               r"(?:REGISTERED PUBLIC ACCOUNTING FIRMS?|AUDITORS?|ACCOUNTANTS?|CERTIFIED PUBLIC ACCOUNTANTS?)\b.*)",
                               re.I)
EXHIBIT_INDEX = re.compile(r"(?:EXHIBIT INDEX|INDEX TO EXHIBITS|EXHIBITS INDEX)\s*[.:]?", re.I)
EXHIBIT_LIST = re.compile(r"(?:EXHIBIT INDEX|INDEX TO EXHIBITS|EXHIBITS?|LIST OF EXHIBITS|EXHIBIT LIST)\s*[.:]?", re.I)


def normalize(line: str) -> str:
    """One line as the rules read it: dashes and quotes plain, bullets and leading bars dropped, whitespace
    single, and a spaced-out keyword ("I T E M") closed up."""
    s = _DASH.sub("-", line.replace("\xa0", " "))
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    s = _WS.sub(" ", _LEAD.sub("", s)).strip()
    m = _SPACED.match(s)
    if m:
        letters = m.group(1).replace(" ", "")
        word = letters[:4] if letters.upper()[:4] in ("PART", "ITEM") and _NUMERAL.fullmatch(letters[4:]) else letters
        if word.upper() in _SPACED_WORDS:
            s = " ".join(filter(None, (word, letters[len(word):]))) + s[m.end():]
    return s
