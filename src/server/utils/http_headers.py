"""HTTP header builders that survive non-ASCII inputs.

Starlette encodes header values as latin-1 and 500s on anything else, so
filenames with CJK / emoji / accented characters must be encoded per RFC 6266
before they reach the response layer.
"""

from __future__ import annotations

import re
from urllib.parse import quote

# Printable ASCII minus quote, backslash, and the separators that break
# Starlette's latin-1 header encoder or downstream parsers. Anything else is
# replaced with an underscore in the ASCII fallback; the original (unicode)
# name flows through the ``filename*`` parameter per RFC 6266.
_ASCII_FILENAME_RE = re.compile(r"[^\w.\-+@~ ]", re.ASCII)


def content_disposition(
    filename: str,
    *,
    disposition: str = "attachment",
    fallback: str = "download",
) -> str:
    """Build an RFC 6266-compliant Content-Disposition header value.

    Pure-ASCII clients see ``filename="..."``; modern clients prefer the
    ``filename*=UTF-8''<percent-encoded>`` parameter and recover the original
    unicode name (e.g. CJK or emoji).

    Without this we'd hit Starlette's latin-1 header encoder and 500 the
    download endpoint for every file whose name contains a non-latin-1
    character — and we'd accept whatever quote/CR/LF the caller injected via
    upload metadata.
    """
    cleaned = filename.replace("\r", "").replace("\n", "").replace('"', "")
    ascii_fallback = _ASCII_FILENAME_RE.sub("_", cleaned)
    if not ascii_fallback or ascii_fallback.isspace():
        ascii_fallback = fallback
    encoded = quote(cleaned, safe="")
    return (
        f'{disposition}; filename="{ascii_fallback}"; '
        f"filename*=UTF-8''{encoded}"
    )
