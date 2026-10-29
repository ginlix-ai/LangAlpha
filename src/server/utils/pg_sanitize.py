"""Sanitize values bound to Postgres TEXT/JSONB columns.

Postgres rejects NUL (`\\x00`) in TEXT/VARCHAR and the `\\u0000` escape in JSONB
text content (psycopg surfaces these as `cannot contain NUL` /
`UntranslatableCharacter`). This module is the single shared helper for
stripping those bytes at the persistence boundary.

Use `strip_pg_nul_str` for plain TEXT binds. Use `SafeJson` as a drop-in
replacement for `psycopg.types.json.Json` when binding JSONB.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import uuid
from typing import Any

from psycopg.types.json import Json, Jsonb

# A `\u0000` escape starts at an odd-numbered backslash of its run. After an even
# run the backslash before `u0000` is itself escaped, so the six characters are
# text ("\u0000" written out in a JSON file, code or a repr) and must survive.
_NUL_ESCAPE = re.compile(r"(?<!\\)((?:\\\\)*)\\u0000")


def strip_pg_nul_str(value: str | None) -> str | None:
    """Strip NUL bytes from a string before it's bound to a TEXT/VARCHAR column."""
    if not value or "\x00" not in value:
        return value
    return value.replace("\x00", "")


def normalize_uuid(value: object) -> str | None:
    """Canonical UUID string for binding to a Postgres ``uuid`` column, or None.

    Postgres' ``uuid`` type rejects forms that Python's ``uuid.UUID`` accepts
    (notably the ``urn:uuid:`` prefix), so binding the raw input risks
    ``InvalidTextRepresentation`` (22P02), which API handlers surface as a 500.
    Re-stringifying the parsed value yields the canonical 36-char hyphenated form
    Postgres always accepts; a value that isn't a UUID returns None so callers
    can short-circuit to "not found".
    """
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError):
        return None


def _drop_non_finite(value: Any) -> Any:
    """Recursively replace non-finite floats (NaN/Inf) with None."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _drop_non_finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_drop_non_finite(v) for v in value]
    return value


def finite_json_dumps(value: Any, **kwargs: Any) -> str:
    """json.dumps that emits valid JSON even when the tree contains NaN/Inf.

    Fast path: single dumps with allow_nan=False; only on the non-finite
    ValueError does it pay one tree walk to null out non-finite floats. Other
    ValueErrors (circular reference) re-raise — _drop_non_finite would recurse
    forever on them.
    """
    try:
        return json.dumps(value, allow_nan=False, **kwargs)
    except ValueError as e:
        if "Out of range float" not in str(e):
            raise
        return json.dumps(_drop_non_finite(value), allow_nan=False, **kwargs)


def _safe_dumps(value: Any) -> str:
    """JSON-serialize for psycopg JSONB bind, stripping any `\\u0000` escape.

    Piggybacks on the dumps psycopg already performs at bind time. The strip is a
    single C-level pass over the serialized text — no extra Python-level walks
    of the value tree. It matches whole escapes only: a plain replace also cut
    the escaped backslash in front of a literal `\\u0000` in half, and the
    dangling backslash made Postgres reject the row. Non-finite floats (NaN/Inf)
    are nulled out for the same reason.
    """
    s = finite_json_dumps(value, ensure_ascii=False)
    if "\\u0000" not in s:
        return s
    return _NUL_ESCAPE.sub(r"\1", s)


# Items per slice: a few milliseconds of encoding between event-loop yields.
_JSON_ARRAY_SLICE = 2000


async def safe_jsonb_array(items: list[Any]) -> Jsonb:
    """``SafeJson`` encoding of a long list, a slice at a time, bound as jsonb.

    The C encoder holds the GIL for a whole ``json.dumps``, so the loop yields
    between slices. The split is here, not in smaller writes, because a
    subagent archive write must carry each task's events whole (it replaces
    those agents' rows), and one write can be the entire archive: every task
    settled before collection starts, or what a turn collector hands its orphan
    continuation. Each slice becomes UTF-8 as it is dumped and psycopg sends
    the joined bytes as they are, so no str of the whole array is ever built:
    one character past Latin-1 anywhere would widen all of it.
    """
    chunks = [b"["]
    for start in range(0, len(items), _JSON_ARRAY_SLICE):
        if start:
            await asyncio.sleep(0)
            chunks.append(b", ")
        chunks.append(
            _safe_dumps(items[start : start + _JSON_ARRAY_SLICE])[1:-1].encode()
        )
    chunks.append(b"]")
    return Jsonb(b"".join(chunks), dumps=_as_is)


def _as_is(data: bytes) -> bytes:
    return data


class SafeJson(Json):
    """Drop-in replacement for `psycopg.types.json.Json` that strips `\\u0000`.

    psycopg already calls `dumps()` once per `Json` bind. Overriding `dumps`
    here adds zero extra traversal — only one extra scan on the serialized JSON
    for the escape sequence.
    """

    def __init__(self, value: Any):
        super().__init__(value, dumps=_safe_dumps)
