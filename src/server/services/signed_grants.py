"""One codec for the server's signed path grants.

A grant is ``<version>.<claim>....<exp>.<sig>``: the claims and an expiry,
signed with a key a migration minted in Postgres. Each kind of grant has its
own key and version, so no grant can be read as another kind. Deliberately not
a JWT: there are no claims to negotiate and no algorithm to be told.

The signature covers the version, every claim and the expiry joined by NUL,
which no claim can hold, so moving a part between fields changes it.
"""

from __future__ import annotations

import base64
import hmac
import re
import time
from collections.abc import Sequence
from hashlib import sha256

from src.server.database.server_keys import get_server_key

__all__ = ["COUNT", "GrantCodec"]

# Expiries are minted as plain decimal seconds, so anything else is forged.
COUNT = re.compile(r"0|[1-9][0-9]{0,9}")


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


class GrantCodec:
    """Mints and verifies one kind of grant.

    ``claim_patterns`` has one entry per claim: a pattern the claim must
    match whole, or None for any non-empty text. ``error`` is what every
    refusal raises; the caller answers all of them with the same 404, so the
    reasons only ever reach a log.
    """

    def __init__(
        self,
        *,
        key_name: str,
        version: str,
        claim_patterns: Sequence[re.Pattern[str] | None],
        error: type[Exception],
        label: str,
    ) -> None:
        self.key_name = key_name
        self.version = version
        self.claim_patterns = tuple(claim_patterns)
        self.error = error
        self.label = label
        # The one process-level cache a request path may consult: the key is
        # written once by a migration and never changes, so every worker reads
        # the same bytes and a stale copy cannot exist.
        self._key: bytes | None = None

    async def _signing_key(self) -> bytes:
        if self._key is None:
            self._key = await get_server_key(self.key_name)
        return self._key

    def signature(self, key: bytes, claims: Sequence[str], expires_at: int) -> str:
        message = "\x00".join((self.version, *claims, str(expires_at)))
        return _b64(hmac.new(key, message.encode("utf-8"), sha256).digest())

    def claims_token(self, token: str | None) -> bool:
        """Whether a token says it is this kind, so it is checked as one."""
        return (token or "").startswith(f"{self.version}.")

    async def mint(
        self, claims: Sequence[str], *, ttl: int, now: int | None = None
    ) -> tuple[str, int]:
        """The token and its expiry."""
        expires_at = int(now if now is not None else time.time()) + int(ttl)
        signature = self.signature(await self._signing_key(), claims, expires_at)
        return ".".join((self.version, *claims, str(expires_at), signature)), expires_at

    async def verify(
        self, token: str | None, *, now: int | None = None
    ) -> tuple[tuple[str, ...], int]:
        """The claims and expiry of a live grant, or ``error``. The shape is
        checked before the key is read, so garbage costs no lookup."""
        parts = (token or "").split(".")
        if len(parts) != len(self.claim_patterns) + 3 or parts[0] != self.version:
            raise self.error(f"malformed {self.label}")
        claims, expires, signature = parts[1:-2], parts[-2], parts[-1]
        if not signature or COUNT.fullmatch(expires) is None:
            raise self.error(f"malformed {self.label}")
        for claim, pattern in zip(claims, self.claim_patterns):
            fits = bool(claim) if pattern is None else bool(pattern.fullmatch(claim))
            if not fits:
                raise self.error(f"malformed {self.label}")
        expires_at = int(expires)
        if expires_at < int(now if now is not None else time.time()):
            raise self.error(f"{self.label} expired")
        expected = self.signature(await self._signing_key(), claims, expires_at)
        # Bytes, because the signature comes from the URL: compare_digest
        # raises on a non-ASCII str, which would turn a forged grant into a 500.
        if not hmac.compare_digest(expected.encode(), signature.encode()):
            raise self.error(f"{self.label} signature does not match")
        return tuple(claims), expires_at
