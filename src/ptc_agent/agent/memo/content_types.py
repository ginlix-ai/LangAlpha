"""MIME type validation for memo uploads."""

from __future__ import annotations

import os

from ptc_agent.agent.memo.schema import ACCEPTED_MIME_TYPES

# Browser/OS combos sometimes report empty or generic mime types for valid
# memo files (Safari drag-and-drop, file-picker via OS file association, some
# Windows configurations). Map known accepted extensions back to their proper
# mime so the upload doesn't 415 on a file the frontend already validated.
_EXTENSION_MIME_FALLBACK: dict[str, str] = {
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".txt": "text/plain",
    ".csv": "text/csv",
    ".json": "application/json",
    ".pdf": "application/pdf",
}

# Mime types we treat as "the browser didn't tell us" — fall back to extension.
_AMBIGUOUS_MIMES: frozenset[str] = frozenset({
    "",
    "application/octet-stream",
    "application/x-download",
})


def resolve_mime_type(reported_mime: str | None, filename: str | None) -> str:
    """Return a canonical mime type, falling back to the filename extension.

    When the multipart upload reports an ambiguous mime (empty or
    octet-stream), look at the filename extension. Resolves the Safari /
    drag-and-drop case where valid memo files would otherwise 415.
    """
    mime = (reported_mime or "").lower()
    if mime in ACCEPTED_MIME_TYPES:
        return mime
    if mime in _AMBIGUOUS_MIMES and filename:
        ext = os.path.splitext(filename)[1].lower()
        fallback = _EXTENSION_MIME_FALLBACK.get(ext)
        if fallback in ACCEPTED_MIME_TYPES:
            return fallback
    return mime  # caller decides whether to reject


def is_pdf(mime_type: str) -> bool:
    return mime_type == "application/pdf"


def is_text(mime_type: str) -> bool:
    """True for formats whose raw bytes are stored as content directly (not PDFs)."""
    return mime_type in ACCEPTED_MIME_TYPES and not is_pdf(mime_type)


def is_accepted(mime_type: str) -> bool:
    return mime_type in ACCEPTED_MIME_TYPES
