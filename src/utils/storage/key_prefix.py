"""The key namespace this process owns inside a shared bucket.

Stored keys are content-addressed (``blobs/{user_id}/{sha256}``), so two
environments pointed at one bucket write identical keys for identical bytes,
and each one's garbage collector judges them by its own database alone. One
of them deleting an object the other still references is then only a matter
of time. A prefix gives each environment its own namespace. It is applied
where a key reaches the provider, so everything above this layer, the
database included, keeps prefix-free keys, and an unset prefix changes
nothing.

Set it before a deployment first writes, and never change it afterwards. The
database records that a digest is stored, not where, so a changed prefix
points every read at a key that holds nothing while backups skip the digests
they believe are already stored. Moving an existing deployment means copying
its objects under the new prefix first.
"""

import os


def _load_key_prefix() -> str:
    raw = os.getenv("STORAGE_KEY_PREFIX", "").strip()
    if not raw:
        return ""
    # A leading slash makes an empty first segment, and ".." reads as a
    # parent to anything that later treats the key as a path.
    if raw.startswith("/") or ".." in raw.split("/"):
        raise ValueError(
            f"STORAGE_KEY_PREFIX {raw!r} must be a relative path such as 'staging/'"
        )
    return raw if raw.endswith("/") else f"{raw}/"


KEY_PREFIX = _load_key_prefix()


def full_key(key: str) -> str:
    """The key as the provider stores it."""
    return f"{KEY_PREFIX}{key}"
