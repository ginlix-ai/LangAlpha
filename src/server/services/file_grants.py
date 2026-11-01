"""The credential an owner's iframe carries to fetch workspace files by path.

A grant names one workspace and an expiry, signed with the key migration 049
minted in Postgres. It replaces the bare workspace UUID as the thing a URL
proves: a grant expires and is only ever minted for the owner, so a pasted
report URL stops working instead of reading the workspace forever.

Shape is ``v1.<workspace>.<exp>.<sig>`` (``services/signed_grants``).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from src.server.services.signed_grants import GrantCodec

__all__ = [
    "DEFAULT_TTL_SECONDS",
    "FILE_GRANT_ROUTE",
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

#: The route a grant opens a path under: ``<route><token>/<path>``.
FILE_GRANT_ROUTE = "/api/v1/wsfiles/g/"


class FileGrantError(Exception):
    """The presented grant does not open any workspace."""


_CODEC = GrantCodec(
    key_name="file_grant",
    version="v1",
    claim_patterns=(None,),
    error=FileGrantError,
    label="file grant",
)


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


async def mint_file_grant(
    workspace_id: str, *, ttl: int = DEFAULT_TTL_SECONDS, now: int | None = None
) -> FileGrant:
    token, expires_at = await _CODEC.mint((workspace_id,), ttl=ttl, now=now)
    return FileGrant(token=token, workspace_id=workspace_id, expires_at=expires_at)


async def verify_file_grant(token: str, *, now: int | None = None) -> str:
    """The workspace a live grant opens, or ``FileGrantError``.

    The caller answers every failure with the same 404 a missing file gets,
    so the reasons only ever reach a log.
    """
    (workspace_id,), _ = await _CODEC.verify(token, now=now)
    return workspace_id


def grant_prefix(grant: FileGrant | str) -> str:
    """The route prefix a client joins a percent-encoded path onto, for a
    grant or its token as a request path carried it."""
    token = grant if isinstance(grant, str) else grant.token
    return f"{FILE_GRANT_ROUTE}{token}/"
