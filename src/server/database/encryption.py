"""
Shared encryption key helper for pgcrypto-based encryption at rest.

Used by api_keys.py and oauth_tokens.py for pgp_sym_encrypt/decrypt.
"""

import os


def get_encryption_key() -> str:
    """Return the symmetric encryption key for data stored at rest."""
    key = os.getenv("BYOK_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError(
            "BYOK_ENCRYPTION_KEY environment variable is not set. "
            "Required for encrypting sensitive data at rest."
        )
    return key


def encryption_configured() -> bool:
    """True when the at-rest encryption key is set — i.e. encrypted stores
    (vault secrets, BYOK keys, OAuth tokens) can hold data at all."""
    return bool(os.getenv("BYOK_ENCRYPTION_KEY"))
