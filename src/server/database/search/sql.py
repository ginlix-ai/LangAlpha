"""The SQL a search spells the same way everywhere: a query's terms as
matches, and keyset paging."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from src.server.utils.pg_sanitize import like_escape, normalize_uuid

# A term this short and Latin is a word's start, as the palette's own matcher
# reads it: "nv" finds NVDA, not "conventions". Postgres's ``\m`` and Python's
# ``(?<!\w)`` agree on what a word character is (letters, digits, ``_``).
_SHORT_LATIN = re.compile(r"[A-Za-z0-9]{1,2}")


def is_word_start(term: str) -> bool:
    return _SHORT_LATIN.fullmatch(term) is not None


def like_pattern(text: str) -> str:
    """``%text%`` with the text's own ``\\``, ``%`` and ``_`` matching
    literally, for an ``ILIKE ... ESCAPE '\\'``."""
    return f"%{like_escape(text)}%"


class Terms:
    """A query's terms as SQL matches them, every one required: an escaped
    ``ILIKE`` anywhere, or for a word-start term a case-blind regex anchored
    at a word's start. ``params`` bind once; ``on`` spells the match against
    one column."""

    def __init__(self, terms: Sequence[str]) -> None:
        self.params: dict[str, str] = {}
        self._matches: list[str] = []
        for i, term in enumerate(terms):
            name = f"term{i}"
            if is_word_start(term):
                self.params[name] = rf"\m{term}"
                self._matches.append(f"~* %({name})s")
            else:
                self.params[name] = like_pattern(term)
                self._matches.append(f"ILIKE %({name})s ESCAPE '\\'")
        #: The query's words as typed, an ILIKE pattern, when it has several.
        self.phrase = like_pattern(" ".join(terms)) if len(terms) > 1 else None

    def on(self, column: str) -> str:
        return " AND ".join(f"{column} {m}" for m in self._matches) or "TRUE"


class BadKey(ValueError):
    """A key that is not the values of the page's sort."""


_CASTS: dict[type, str] = {datetime: "timestamptz", UUID: "uuid", int: "int", bool: "boolean"}
# What the ``int`` cast takes; a key past it would fail the cast as a 500.
_INT_MIN, _INT_MAX = -(2**31), 2**31 - 1


@dataclass(frozen=True)
class Keyset:
    """A page's sort key, every column descending: each an expression and
    the type of its value. A key is the last row's values, as the row read
    them or as a cursor carried them through JSON."""

    columns: tuple[tuple[str, type], ...]

    @property
    def order_by(self) -> str:
        return ", ".join(f"{expr} DESC" for expr, _ in self.columns)

    def after(self, key: Sequence[Any] | None, params: dict[str, Any]) -> str:
        """The condition that starts the page after ``key``; empty for the
        first page. ``BadKey`` for a key that is not these columns' values."""
        if key is None:
            return ""
        if len(key) != len(self.columns):
            raise BadKey("key does not match the page's sort")
        values = []
        for i, ((_, kind), value) in enumerate(zip(self.columns, key)):
            params[f"after{i}"] = _value(kind, value)
            values.append(f"%(after{i})s::{_CASTS[kind]}")
        exprs = ", ".join(expr for expr, _ in self.columns)
        lead = self.columns[0][0]
        # The row comparison implies the bound on its leading column, spelled
        # so an index on that column can start the scan there.
        return f"AND {lead} <= {values[0]} AND ({exprs}) < ({', '.join(values)})"


def _value(kind: type, value: Any) -> Any:
    if kind is bool and isinstance(value, bool):
        return value
    if (
        kind is int
        and isinstance(value, int)
        and not isinstance(value, bool)
        and _INT_MIN <= value <= _INT_MAX
    ):
        return value
    if kind is UUID and isinstance(value, str | UUID):
        if (uuid := normalize_uuid(value)) is not None:
            return uuid
    if kind is datetime:
        at = value
        if isinstance(value, str):
            try:
                at = datetime.fromisoformat(value)
            except ValueError:
                at = None
        if isinstance(at, datetime) and at.tzinfo is not None:
            return at
    raise BadKey(f"not a {kind.__name__} key value")
