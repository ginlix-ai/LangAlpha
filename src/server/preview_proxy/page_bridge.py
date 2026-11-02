"""The page bridge: the script the server puts first in the head of an HTML
document it frames for the app, which speaks to the viewer around the frame
(``bridge/core.js`` says what it does).

A document gets the parts its view needs, inside one function. Every bridged
document gets ``core.js`` and ``requests.js`` (``view``): a published version,
and a live file the panel or a share reads. The owner's live view of a file
adds the ``comments-*.js`` parts (``comments``). A live file is bridged only
when its URL asks (``?inject=``, ``parse_inject``); a PDF render or a download reads the
bytes as they are.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

__all__ = ["Bridge", "Inject", "bridge_for", "parse_inject", "with_bridge"]

Inject = Literal["view", "comments"]

_PARTS: dict[Inject, tuple[str, ...]] = {
    "view": ("core", "requests"),
    "comments": (
        "core",
        "requests",
        "comments-text",
        "comments-areas",
        "comments-anchors",
        "comments-mode",
    ),
}
# One function around the parts, so nothing they declare is the document's.
# Opened outside a frame, a document has no viewer to speak to. The hello goes
# last, once every part is listening.
_OPEN = "(function () {\n'use strict';\nif (!window.parent || window.parent === window) return;\n"
_CLOSE = "\nhello();\n})();"

# The frame's scheme follows the app's theme, so a document that paints no
# ground of its own shows the app's, not a white sheet under a dark app.
# Ours goes first and in HTML the first such meta wins, so a document that
# declares its own scheme gets none of ours.
_COLOR_SCHEME = b'<meta name="color-scheme" content="light dark">'
_META = re.compile(rb"<meta\b", re.IGNORECASE)
_NAMES_COLOR_SCHEME = re.compile(
    rb"\bname\s*=\s*[\"']?color-scheme\b", re.IGNORECASE
)
# The viewer's scrollbar, matching the app's (web/src/styles/tokens.css): the
# thumb inset from the frame's edge and short of both ends. The fallback colour
# covers the moment before the first theme push. A document with WebKit
# scrollbar rules of its own gets none of it: the two would merge rule by rule,
# and a 6px scrollbar less this 3px inset on each side paints no thumb at all.
# `scrollbar-width` and `scrollbar-color` cannot merge that way, so a document
# that sets only those still gets it, and a browser without them shows it.
_EMBED_SCROLLBARS = (
    b"<style>"
    b"::-webkit-scrollbar{width:12px;height:12px}"
    b"::-webkit-scrollbar-track{background:transparent;margin:4px}"
    b"::-webkit-scrollbar-thumb{"
    b"background-color:var(--color-border-elevated,rgba(128,128,128,.45));"
    b"background-clip:padding-box;border:3px solid transparent;border-radius:6px}"
    b"::-webkit-scrollbar-thumb:hover{"
    b"background-color:var(--color-text-tertiary,rgba(128,128,128,.7))}"
    b"</style>"
)
_STYLES_OWN_SCROLLBAR = re.compile(rb"-webkit-scrollbar", re.IGNORECASE)

_HEAD = re.compile(rb"<head\b", re.IGNORECASE)
_HTML = re.compile(rb"<html\b", re.IGNORECASE)
_DOCTYPE = re.compile(rb"<!doctype\b", re.IGNORECASE)
# Where a document's content begins, or an element whose contents no parser
# takes as the document's tags; a tag after it is text, or inert.
_CONTENT = re.compile(
    rb"<(?:script|style|body|template|title|noscript|textarea|xmp|iframe"
    rb"|noembed|noframes)\b",
    re.IGNORECASE,
)
_COMMENT_OPEN = b"<!--"
# Browsers close one on the misspelled form too, and on ``<!-->`` at once.
_COMMENT_CLOSE = re.compile(rb"--!?>")
_BOM = b"\xef\xbb\xbf"


@dataclass(frozen=True)
class Bridge:
    script: bytes
    # In the ETag of every document it is spliced into, so a changed bridge
    # is a changed document. Covers all ``with_bridge`` may add.
    digest: str


def _part(name: str) -> str:
    source = (Path(__file__).parent / "bridge" / f"{name}.js").read_text("utf-8")
    if not source.isascii() or "</script" in source.lower():
        # Spliced as bytes into documents of any ASCII-compatible encoding, and
        # a closing tag in it would end the script early.
        raise RuntimeError(f"bridge/{name}.js must be ASCII without a closing script tag")
    return source


def _assemble(inject: Inject) -> Bridge:
    source = _OPEN + "\n".join(_part(name) for name in _PARTS[inject]) + _CLOSE
    script = f"<script>{source}</script>".encode("ascii")
    digest = hashlib.sha256(_COLOR_SCHEME + _EMBED_SCROLLBARS + script).hexdigest()[:12]
    return Bridge(script=script, digest=digest)


_BRIDGES: dict[Inject, Bridge] = {inject: _assemble(inject) for inject in _PARTS}


def bridge_for(inject: Inject) -> Bridge:
    return _BRIDGES[inject]


def parse_inject(value: str | None) -> Inject | None:
    """The bridge ``?inject=`` asks for, or None for the bytes as they are.

    ``theme`` is what the read-only view was called before it carried the
    bridge, and addresses saved then still say it.
    """
    if value in ("view", "theme"):
        return "view"
    if value == "comments":
        return "comments"
    return None


def _declares_color_scheme(document: bytes) -> bool:
    """Whether a meta tag names the color scheme. A scan rather than one
    pattern: this runs on anyone's published bytes, and a ``[^>]*`` tried at
    each of a run of unclosed tags is quadratic in the document."""
    at = 0
    while (meta := _META.search(document, at)) is not None:
        close = document.find(b">", meta.end())
        if close < 0:
            return False
        if _NAMES_COLOR_SCHEME.search(document, meta.end(), close):
            return True
        at = close + 1
    return False


def _outside_comments(
    pattern: re.Pattern[bytes], document: bytes, end: int
) -> re.Match[bytes] | None:
    """The first match of *pattern* ahead of *end* that no comment holds. One
    pass, on the same grounds as :func:`_declares_color_scheme`."""
    at = 0
    while True:
        opened = document.find(_COMMENT_OPEN, at, end)
        match = pattern.search(document, at, opened if opened >= 0 else end)
        if match is not None or opened < 0:
            return match
        closed = _COMMENT_CLOSE.search(document, opened + 2, end)
        if closed is None:
            return None
        at = closed.end()


def with_bridge(document: bytes, bridge: Bridge) -> bytes:
    """*document* with the bridge as the first thing in its head.

    A document with no head tag gets it after its html tag or doctype, where
    the parser opens the head itself: put first, it would push the doctype
    out of place and the document into quirks mode. Only tags ahead of the
    first script, style, body or inert element and outside comments count, so
    a ``<head>`` written inside a script's string, a template or a comment is
    never taken for the document's: the bridge would land where nothing runs
    it.

    Up to the 25 MiB a version holds, so callers on the event loop hand it to
    a thread.
    """
    scrollbars = b"" if _STYLES_OWN_SCROLLBAR.search(document) else _EMBED_SCROLLBARS
    scheme = b"" if _declares_color_scheme(document) else _COLOR_SCHEME
    tag = scheme + scrollbars + bridge.script
    content = _outside_comments(_CONTENT, document, len(document))
    end = content.start() if content is not None else len(document)
    for pattern in (_HEAD, _HTML, _DOCTYPE):
        match = _outside_comments(pattern, document, end)
        close = document.find(b">", match.end(), end) if match is not None else -1
        if close >= 0:
            return document[: close + 1] + tag + document[close + 1 :]
    if document.startswith(_BOM):
        return _BOM + tag + document[len(_BOM) :]
    return tag + document
