"""Canonical Content-Type resolution.

One pinned extension→type map, consulted at both upload and serve time, so a
file gets the *same* Content-Type wherever it is handed out. The stdlib
`mimetypes` tables vary by platform and base image — e.g. `python:3.12-slim`
returns None for `.woff2` and `text/xml` for `.xml`, differing from macOS — and
some served types (`.md`, `.yaml`) resolve inconsistently or not at all. Pinning
removes that variance; `resolve_content_type` never returns None, so a caller
can always set an explicit type rather than omitting the header (an omitted or
generic type breaks inline rendering behind a `nosniff` policy).
"""

from __future__ import annotations

import mimetypes
from pathlib import Path

# Pinned types. Superset of every extension the workspace-file serve path and
# the object-storage uploaders previously mapped locally, plus text types the
# stdlib tables miss or resolve inconsistently (`.md`, `.yaml`). Text types
# carry an explicit charset; binary types do not.
CONTENT_TYPES: dict[str, str] = {
    # Documents / markup
    ".html": "text/html; charset=utf-8",
    ".htm": "text/html; charset=utf-8",
    ".txt": "text/plain; charset=utf-8",
    ".md": "text/markdown; charset=utf-8",
    ".markdown": "text/markdown; charset=utf-8",
    ".csv": "text/csv; charset=utf-8",
    ".xml": "application/xml; charset=utf-8",
    ".pdf": "application/pdf",
    # Data / config
    ".json": "application/json; charset=utf-8",
    ".map": "application/json; charset=utf-8",
    ".ipynb": "application/json; charset=utf-8",
    ".yaml": "application/yaml; charset=utf-8",
    ".yml": "application/yaml; charset=utf-8",
    # Web assets
    ".js": "text/javascript; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".wasm": "application/wasm",
    # Images
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
    ".avif": "image/avif",
    ".bmp": "image/bmp",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    # Fonts
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
}


def resolve_content_type(name: str, *, default: str = "application/octet-stream") -> str:
    """Resolve a Content-Type from a filename/key: pinned map, then stdlib, then default.

    Never returns None — callers should always send an explicit Content-Type.
    """
    suffix = Path(name).suffix.lower()
    if suffix in CONTENT_TYPES:
        return CONTENT_TYPES[suffix]
    return mimetypes.guess_type(name)[0] or default
