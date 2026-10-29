"""The credential an owner's iframe carries to fetch workspace files by path.

A grant names one workspace and an expiry, signed with the key migration 049
minted in Postgres. It replaces the bare workspace UUID as the thing a URL
proves: a grant expires and is only ever minted for the owner, so a pasted
report URL stops working instead of reading the workspace forever.

Shape is ``v1.<workspace>.<exp>.<sig>``, deliberately not a JWT: there are no
claims to negotiate and no algorithm to be told.
"""

from __future__ import annotations

import base64
import hmac
import time
from dataclasses import dataclass
from hashlib import sha256

from src.server.database.server_keys import get_server_key

__all__ = [
    "DEFAULT_TTL_SECONDS",
    "FileGrant",
    "FileGrantError",
    "grant_prefix",
    "mint_file_grant",
    "seconds_left",
    "verify_file_grant",
]

# Long enough for a chat left open all day; the web app re-mints well before
# it runs out. A grant found in an access log is worthless a day later.
DEFAULT_TTL_SECONDS = 12 * 3600

_KEY_NAME = "file_grant"
_VERSION = "v1"

# The one process-level cache a request path may consult: the key is written
# once by the migration and never changes, so every worker reads the same
# bytes and a stale copy cannot exist.
_key: bytes | None = None


class FileGrantError(Exception):
    """The presented grant does not open any workspace."""


def seconds_left(epoch: int, *, now: float | None = None) -> int:
    """The wire shape of every credential lifetime the web reads: ``expires_in``.

    A lifetime rather than a timestamp, because the browser schedules renewal
    on its own clock. It adds this to the moment the answer arrived, so one
    clock measures both ends and a skewed one cannot put renewal past expiry.
    """
    return max(int(epoch - (time.time() if now is None else now)), 0)


@dataclass(frozen=True)
class FileGrant:
    token: str
    workspace_id: str
    expires_at: int


async def _signing_key() -> bytes:
    global _key
    if _key is None:
        _key = await get_server_key(_KEY_NAME)
    return _key


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _signature(key: bytes, workspace_id: str, expires_at: int) -> str:
    message = "\x00".join((_VERSION, workspace_id, str(expires_at))).encode("utf-8")
    return _b64(hmac.new(key, message, sha256).digest())


async def mint_file_grant(
    workspace_id: str, *, ttl: int = DEFAULT_TTL_SECONDS, now: int | None = None
) -> FileGrant:
    expires_at = int(now if now is not None else time.time()) + int(ttl)
    signature = _signature(await _signing_key(), workspace_id, expires_at)
    return FileGrant(
        token=f"{_VERSION}.{workspace_id}.{expires_at}.{signature}",
        workspace_id=workspace_id,
        expires_at=expires_at,
    )


async def verify_file_grant(token: str, *, now: int | None = None) -> str:
    """The workspace a live grant opens, or ``FileGrantError``.

    The caller answers every failure with the same 404 a missing file gets,
    so the reasons below only ever reach a log.
    """
    parts = (token or "").split(".")
    if len(parts) != 4 or parts[0] != _VERSION or not all(parts):
        raise FileGrantError("malformed file grant")
    _, workspace_id, expires, signature = parts
    try:
        expires_at = int(expires)
    except ValueError:
        raise FileGrantError("malformed file grant expiry") from None
    if expires_at < int(now if now is not None else time.time()):
        raise FileGrantError("file grant expired")
    expected = _signature(await _signing_key(), workspace_id, expires_at)
    # Bytes, because the signature comes from the URL: compare_digest raises
    # on a non-ASCII str, which would turn a forged grant into a 500.
    if not hmac.compare_digest(expected.encode(), signature.encode()):
        raise FileGrantError("file grant signature does not match")
    return workspace_id


def grant_prefix(grant: FileGrant) -> str:
    """The route prefix a client joins a percent-encoded path onto."""
    return f"/api/v1/wsfiles/g/{grant.token}/"
