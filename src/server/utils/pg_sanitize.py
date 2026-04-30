"""Sanitize values bound to Postgres TEXT/JSONB columns.

Postgres rejects NUL (`\\x00`) in TEXT/VARCHAR and the `\\u0000` escape in JSONB
text content (psycopg surfaces these as `cannot contain NUL` /
`UntranslatableCharacter`). This module is the single shared helper for
stripping those bytes at the persistence boundary.

Use `strip_pg_nul_str` for plain TEXT binds. Use `SafeJson` as a drop-in
replacement for `psycopg.types.json.Json` when binding JSONB.
"""

from __future__ import annotations

import json
from typing import Any

from psycopg.types.json import Json


def strip_pg_nul_str(value: str | None) -> str | None:
    """Strip NUL bytes from a string before it's bound to a TEXT/VARCHAR column."""
    if not value or "\x00" not in value:
        return value
    return value.replace("\x00", "")


def _safe_dumps(value: Any) -> str:
    """JSON-serialize for psycopg JSONB bind, stripping any `\\u0000` escape.

    Piggybacks on the dumps psycopg already performs at bind time. The strip is a
    single C-level `str.replace` on the serialized text — no extra Python-level
    walks of the value tree.
    """
    s = json.dumps(value, ensure_ascii=False)
    if "\\u0000" not in s:
        return s
    return s.replace("\\u0000", "")


class SafeJson(Json):
    """Drop-in replacement for `psycopg.types.json.Json` that strips `\\u0000`.

    psycopg already calls `dumps()` once per `Json` bind. Overriding `dumps`
    here adds zero extra traversal — only one extra C-level scan on the
    serialized JSON for the escape sequence.
    """

    def __init__(self, value: Any):
        super().__init__(value, dumps=_safe_dumps)
