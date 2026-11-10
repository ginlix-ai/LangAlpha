"""The pure half of search: terms, snippets, match ranges and page cursors.

Every offset handed to a client is in UTF-16 code units, the unit a
JavaScript string is indexed in, so the palette can slice a title or a
snippet by them as they come; an emoji counts two.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, get_args

from src.server.database.search.sql import is_word_start

MAX_TERMS = 8
SNIPPET_CHARS = 160
# How far a cut may move to land on a space rather than inside a word.
_WORD_SLACK = 20
# Characters of context kept before the first match, at most.
_LEAD = 50
# Raw characters either side of the first match that a snippet is cut from:
# room for its lead and width once markup is cleaned away, and all the
# cleanup's patterns ever read, so a 32K answer costs what a short one does.
_WINDOW = 1000

Kind = Literal["threads", "turns", "files"]


def split_terms(q: str) -> list[str]:
    """The query's whitespace-separated terms, each once whatever its case,
    in order, at most ``MAX_TERMS``."""
    seen: set[str] = set()
    terms: list[str] = []
    for term in q.split():
        key = term.casefold()
        if key not in seen:
            seen.add(key)
            terms.append(term)
    return terms[:MAX_TERMS]


def _term_regex(term: str) -> str:
    """The term as the search matched it: anywhere, or at a word's start."""
    body = re.escape(term)
    return rf"(?<!\w){body}" if is_word_start(term) else body


# Bounded on both sides: an unclosed ``[a](`` repeated would otherwise make
# each match scan to the end of the text, quadratic over a long one.
_IMAGE = re.compile(r"!\[([^\]\n]{0,512})\]\([^)\n]{0,2048}\)")
_LINK = re.compile(r"\[([^\]\n]{1,512})\]\([^)\n]{0,2048}\)")
_HEADING = re.compile(r"^[ \t]{0,3}#{1,6}[ \t]+", re.MULTILINE)
_SPACE = re.compile(r"\s+")


def clean_markdown(text: str) -> str:
    """``text`` as a line of prose: links and images by their text, without
    bold markers, heading marks or backticks, its whitespace collapsed."""
    text = _IMAGE.sub(r"\1", text)
    text = _LINK.sub(r"\1", text)
    text = _HEADING.sub("", text)
    text = text.replace("**", "").replace("`", "")
    return _SPACE.sub(" ", text).strip()


def _term_spans(text: str, terms: list[str]) -> list[tuple[int, int]]:
    """Every occurrence of every term, case-insensitively, merged where they
    overlap, in code points."""
    spans = sorted(
        m.span()
        for term in terms
        if term
        for m in re.finditer(_term_regex(term), text, re.IGNORECASE)
    )
    merged: list[tuple[int, int]] = []
    for start, end in spans:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _utf16(text: str, index: int) -> int:
    return len(text[:index].encode("utf-16-le")) // 2


def match_ranges(text: str, terms: list[str]) -> list[list[int]]:
    """``[start, end)`` of each match of the terms in ``text``, in UTF-16
    code units."""
    return [
        [_utf16(text, start), _utf16(text, end)]
        for start, end in _term_spans(text, terms)
    ]


# Windows a snippet tries, each after the last, for a match the cleaned
# text still shows, before it shows the first one's raw markdown.
_MAX_WINDOWS = 8


def _first_match(text: str, terms: list[str], start: int = 0) -> int | None:
    """Where the first match of any term at or after ``start`` begins."""
    starts = [
        m.start()
        for term in terms
        if term
        and (m := re.compile(_term_regex(term), re.IGNORECASE).search(text, start))
    ]
    return min(starts, default=None)


def _window(text: str, at: int) -> tuple[str, bool, bool]:
    """The ``_WINDOW`` characters either side of ``at``, and whether text
    was cut before and after them."""
    lo, hi = max(0, at - _WINDOW), min(len(text), at + _WINDOW)
    return text[lo:hi], lo > 0, hi < len(text)


def snippet(text: str, terms: list[str], width: int = SNIPPET_CHARS) -> tuple[str, list[list[int]]]:
    """About ``width`` characters of ``text`` around its first match, cut at
    spaces where one is near, with ``…`` where text was cut away, and the
    matches in it. The text is cleaned of markdown first, so the ranges are
    into what is shown. Only the ``_WINDOW`` characters either side of a
    match are read past finding it: a window whose match only the markup
    held (a link's address) gives way to the next match past it, and when
    none of ``_MAX_WINDOWS`` shows one, the first is shown uncleaned."""
    at = _first_match(text, terms)
    first = _window(text, at or 0)
    window, cut_before, cut_after = first
    shown = clean_markdown(window)
    spans = _term_spans(shown, terms)
    tried = 1
    while not spans and at is not None and tried < _MAX_WINDOWS:
        at = _first_match(text, terms, min(len(text), at + _WINDOW))
        if at is None:
            break
        tried += 1
        window, cut_before, cut_after = _window(text, at)
        shown = clean_markdown(window)
        spans = _term_spans(shown, terms)
    if not spans:
        window, cut_before, cut_after = first
        shown = clean_markdown(window)
        raw = _SPACE.sub(" ", window).strip()
        if raw_spans := _term_spans(raw, terms):
            shown, spans = raw, raw_spans
    pos, end_of_match = spans[0] if spans else (0, 0)

    if len(shown) <= width:
        start, end = 0, len(shown)
    else:
        lead = max(0, min(_LEAD, (width - (end_of_match - pos)) // 3))
        start = max(0, pos - lead)
        end = min(len(shown), start + width)
        start = max(0, min(start, end - width))
        if start > 0:
            space = shown.find(" ", start, min(pos, start + _WORD_SLACK))
            if space != -1:
                start = space + 1
        if end < len(shown):
            space = shown.rfind(" ", max(end_of_match, end - _WORD_SLACK), end)
            if space != -1:
                end = space

    cut = shown[start:end]
    out = (
        ("…" if start > 0 or cut_before else "")
        + cut
        + ("…" if end < len(shown) or cut_after else "")
    )
    return out, match_ranges(out, terms)


@dataclass(frozen=True)
class Cursor:
    """Where a page ended: its kind and the last row's sort key, in JSON
    values (a time as ISO 8601). Opaque to a client."""

    kind: Kind
    key: tuple[Any, ...]


def encode_cursor(cursor: Cursor) -> str:
    raw = json.dumps(
        {"k": cursor.kind, "key": cursor.key},
        separators=(",", ":"),
        default=_json_value,
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _json_value(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} is not a key value")


def decode_cursor(value: str) -> Cursor:
    """The cursor a page handed out; ValueError for anything else. Its key's
    values are checked by the read it pages."""
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        payload = json.loads(raw)
    except (binascii.Error, ValueError, UnicodeDecodeError) as e:
        raise ValueError("malformed cursor") from e
    if not isinstance(payload, dict):
        raise ValueError("malformed cursor")
    kind, key = payload.get("k"), payload.get("key")
    if kind not in get_args(Kind) or not isinstance(key, list):
        raise ValueError("malformed cursor")
    return Cursor(kind, tuple(key))
