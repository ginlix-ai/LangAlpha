"""Filename → memo key slug with collision suffixes.

The store's key validator (``validate_store_key``) only accepts
``[A-Za-z0-9\\-_.@+~]`` per segment. User uploads arrive with spaces, parens,
unicode, etc. This module turns ``"Q1 2026 (Thesis).md"`` into
``"q1-2026-thesis.md"`` and resolves collisions with ``-2``, ``-3``, …
"""

from __future__ import annotations

import os
import re
import secrets
import unicodedata
from collections.abc import Iterable

_ALLOWED_CHAR_RE = re.compile(r"[^a-z0-9\-_.@+~]")
_MULTI_DASH_RE = re.compile(r"-{2,}")

# Maximum basename length for the slug (before extension). Keeps keys readable
# and under filesystem limits when anyone mirrors to a local FS later.
_MAX_BASE_LEN = 120

# Linear-probe cap on collision suffix search. Past this we fall through to a
# random hex suffix — capped low (50) so a pathological 999-base-collision
# user can't pin the namespace lock for ~30 minutes during slug allocation.
_MAX_COLLISION_SUFFIX = 50

# Hex randomness appended after the linear cap is exhausted. 4 hex chars =
# 2^16 buckets — enough that two random suffixes collide with probability
# < 0.001 even at 1000 prior memos.
_RANDOM_SUFFIX_LEN = 4


def _strip_accents(text: str) -> str:
    """NFKD decompose and drop combining marks — maps é → e, ñ → n, etc."""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _base_slug(name: str) -> str:
    """Transform a filename (sans extension) into the slug form."""
    ascii_name = _strip_accents(name).lower()
    # Collapse whitespace and most separators to '-'.
    ascii_name = re.sub(r"[\s_]+", "-", ascii_name)
    # Strip anything that's not in the allowed set.
    ascii_name = _ALLOWED_CHAR_RE.sub("-", ascii_name)
    # Collapse dash runs and trim.
    ascii_name = _MULTI_DASH_RE.sub("-", ascii_name)
    ascii_name = ascii_name.strip("-.")
    if not ascii_name:
        # Purely-unicode or empty input — fall back to a stable placeholder.
        ascii_name = "memo"
    return ascii_name[:_MAX_BASE_LEN]


def slugify_filename(
    original_filename: str, existing_keys: Iterable[str] = (),
) -> str:
    """Turn an original filename into a collision-free memo key.

    Preserves the (lowercased) extension. Collisions append ``-2``, ``-3``, …
    to the base until a free key is found.

    Args:
        original_filename: The user-supplied filename (may contain spaces,
            unicode, parens, etc.). May be empty.
        existing_keys: Iterable of keys already in the namespace. Deduped
            internally.

    Returns:
        A key string that passes ``validate_store_key``.
    """
    base_slug, suffix = slug_components(original_filename)

    existing = set(existing_keys)
    candidate = f"{base_slug}{suffix}"
    if candidate not in existing:
        return candidate
    for n in range(2, _MAX_COLLISION_SUFFIX + 1):
        candidate = f"{base_slug}-{n}{suffix}"
        if candidate not in existing:
            return candidate
    # Linear cap exhausted — fall back to random hex. Bounded retries so a
    # truly adversarial caller can't loop forever; ``random_collision_slug``
    # has a wide enough range that the first try is virtually always free.
    for _ in range(8):
        candidate = random_collision_slug(base_slug, suffix)
        if candidate not in existing:
            return candidate
    return random_collision_slug(base_slug, suffix)


def slug_components(original_filename: str) -> tuple[str, str]:
    """Return the slugified ``(base, suffix)`` for a filename.

    Exposed so the upload path can probe the store one key at a time
    (``aget(namespace, candidate)``) instead of paginating the entire
    namespace just to feed ``existing_keys``. ``suffix`` is the extension
    with leading dot, or empty when the filename has none.
    """
    base, ext = os.path.splitext(original_filename or "")
    base_slug = _base_slug(base)
    ext_slug = _base_slug(ext.lstrip(".")) if ext else ""
    suffix = f".{ext_slug}" if ext_slug else ""
    return base_slug, suffix


def candidate_slug(base: str, suffix: str, n: int) -> str:
    """Build the n-th collision candidate. ``n=1`` returns the bare slug."""
    if n <= 1:
        return f"{base}{suffix}"
    return f"{base}-{n}{suffix}"


def random_collision_slug(base: str, suffix: str) -> str:
    """Build a random-suffix candidate for use after the linear cap is exhausted."""
    return f"{base}-{secrets.token_hex(_RANDOM_SUFFIX_LEN // 2)}{suffix}"


MAX_COLLISION_SUFFIX = _MAX_COLLISION_SUFFIX
