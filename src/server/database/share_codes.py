"""The code behind every ``/s/`` and ``/a/`` link.

Chat tokens and item links draw from one space of 62^12 codes. Nothing
guards one table against the other: a collision is not expected in the
lifetime of a deployment, and a guard would cost every mint a retry loop.
"""

from __future__ import annotations

import re
import secrets
import string
from urllib.parse import urlencode

from src.config.env import PUBLIC_APP_URL

SHARE_CODE_LENGTH = 12
_ALPHABET = string.digits + string.ascii_letters
_SHARE_CODE_RE = re.compile(rf"[0-9A-Za-z]{{{SHARE_CODE_LENGTH}}}")


def mint_share_code() -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(SHARE_CODE_LENGTH))


def share_path(code: str) -> str:
    """The item's page, relative to the web app."""
    return f"/a/{code}"


def share_url(code: str, path: str | None = None) -> str:
    """An app's page as an absolute URL, for text a person reads outside the app.

    The page is always named, the app's root as ``/``: a URL without one opens
    the link's own entry, which a later call on the port moves, and a message
    should keep opening the page it announced.
    """
    query = urlencode({"path": path or "/"}, safe="/")
    return f"{PUBLIC_APP_URL}{share_path(code)}?{query}"


def is_share_code(value: str) -> bool:
    """Whether a token has the shape of a link code, so a lookup is worth making.

    Legacy chat tokens are 22 characters of base64url and never match, so
    the link table is only consulted for tokens that could be in it.
    """
    return _SHARE_CODE_RE.fullmatch(value) is not None
