"""13F managers by name: the words a manager's name is compared on."""
from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import Any

# Legal-form words a manager's name ends with, which a caller may leave out ("Berkshire Hathaway").
_LEGAL_FORMS = frozenset({
    "INC", "INCORPORATED", "LLC", "LP", "LLP", "LTD", "LIMITED", "CORP", "CORPORATION", "CO", "COMPANY", "PLC",
    "NA", "SA", "AG", "NV", "SE", "GMBH", "PC",
})
# EDGAR's conformed names may end in a state or kind tag: "BERKSHIRE HATHAWAY INC /DE/", "XYZ CAPITAL /ADV".
_NAME_TAG = re.compile(r"\s+/[A-Z0-9 ]{1,6}/?\s*$")


def name_tokens(name: Any) -> tuple[str, ...]:
    """A manager's name as comparable words: upper case, "&" as AND, punctuation, a trailing EDGAR tag, a leading
    "THE" and trailing legal-form words dropped (one word is always kept, so "CO" alone stays a name)."""
    text = _NAME_TAG.sub("", name.upper() if isinstance(name, str) else "").replace("&", " AND ")
    words = re.sub(r"[^A-Z0-9 ]+", " ", text.replace(".", "").replace("'", "").replace("’", "")).split()
    while len(words) > 1 and words[-1] in _LEGAL_FORMS:
        words.pop()
    if len(words) > 1 and words[0] == "THE":
        words.pop(0)
    return tuple(words)


def name_matches(query: Any, names: list[Any]) -> list[int]:
    """The indexes of *names* a manager's name (*query*) means: those equal to it word for word once legal forms
    are dropped, else those holding every word of it; none for a query with no words."""
    return name_matches_tokens(name_tokens(query), [name_tokens(n) for n in names])


def name_matches_tokens(query: Sequence[str], names: Iterable[Sequence[str]]) -> list[int]:
    """name_matches on words name_tokens already made, the query's and each name's, so a caller that holds many
    names makes their words once and compares every query by the same rule."""
    want = tuple(query)
    if not want:
        return []
    tokens = [tuple(t) for t in names]
    exact = [i for i, t in enumerate(tokens) if t == want]
    return exact or [i for i, t in enumerate(tokens) if set(want) <= set(t)]
