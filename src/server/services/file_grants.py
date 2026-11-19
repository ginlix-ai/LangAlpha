"""The credential an owner's iframe carries to fetch workspace files by path.

A grant names one workspace and an expiry, signed with the key migration 049
minted in Postgres. It replaces the bare workspace UUID as the thing a URL
proves: a grant expires and is only ever minted for the owner, so a pasted
report URL stops working instead of reading the workspace forever.

Shape is ``v1.<workspace>.<exp>.<sig>`` (``services/signed_grants``).

A shared workspace's files are reached by several people, and a page any of
their turns wrote can carry its own address out, so a member's grant also
names the member: ``w1.<workspace>.<viewer>.<exp>.<sig>``, an hour long, and
judged again against the viewer's access on every request it opens, so it
stops when they leave the workspace or the organization.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from src.server.services.member_grants import VIEWER
from src.server.services.signed_grants import GrantCodec

__all__ = [
    "DEFAULT_TTL_SECONDS",
    "FILE_GRANT_ROUTE",
    "FileGrant",
    "FileGrantError",
    "SHARED_TTL_SECONDS",
    "grant_prefix",
    "mint_file_grant",
    "mint_shared_file_grant",
    "narrowed_file_grant",
    "seconds_left",
    "verify_file_grant",
    "verify_grant_viewer",
]

# Long enough for a chat left open all day; the web app re-mints well before
# it runs out. A grant found in an access log is worthless a day later.
DEFAULT_TTL_SECONDS = 12 * 3600
#: A member's grant: renewed by the page well before it runs out.
SHARED_TTL_SECONDS = 3600

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
_SHARED = GrantCodec(
    key_name="shared_file_grant",
    version="w1",
    claim_patterns=(None, VIEWER),
    error=FileGrantError,
    label="shared file grant",
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
    #: The member a shared workspace's grant is bound to; None for an owner's.
    viewer_id: str | None = None


async def mint_file_grant(
    workspace_id: str, *, ttl: int = DEFAULT_TTL_SECONDS, now: int | None = None
) -> FileGrant:
    token, expires_at = await _CODEC.mint((workspace_id,), ttl=ttl, now=now)
    return FileGrant(token=token, workspace_id=workspace_id, expires_at=expires_at)


async def mint_shared_file_grant(
    workspace_id: str,
    viewer_id: str,
    *,
    ttl: int = SHARED_TTL_SECONDS,
    now: int | None = None,
) -> FileGrant:
    """A member's grant to a shared workspace, bound to them."""
    token, expires_at = await _SHARED.mint((workspace_id, viewer_id), ttl=ttl, now=now)
    return FileGrant(token, workspace_id, expires_at, viewer_id)


async def _verified(token: str, *, now: int | None) -> FileGrant:
    if _SHARED.claims_token(token):
        (workspace_id, viewer_id), expires_at = await _SHARED.verify(token, now=now)
        return FileGrant(token, workspace_id, expires_at, viewer_id)
    (workspace_id,), expires_at = await _CODEC.verify(token, now=now)
    return FileGrant(token, workspace_id, expires_at)


async def narrowed_file_grant(
    token: str, *, ttl: int, now: int | None = None
) -> FileGrant:
    """A grant to the workspace a live ``token`` opens, for the same viewer,
    that lasts ``ttl`` seconds at most and never past ``token`` itself, or
    ``FileGrantError``: for an address that may be copied out of the request,
    as a rendered document can print its own."""
    grant = await _verified(token, now=now)
    current = int(now if now is not None else time.time())
    ttl = min(ttl, grant.expires_at - current)
    if grant.viewer_id is not None:
        return await mint_shared_file_grant(
            grant.workspace_id, grant.viewer_id, ttl=ttl, now=current
        )
    return await mint_file_grant(grant.workspace_id, ttl=ttl, now=current)


async def verify_file_grant(token: str, *, now: int | None = None) -> str:
    """The workspace a live grant opens, or ``FileGrantError``.

    The caller answers every failure with the same 404 a missing file gets,
    so the reasons only ever reach a log.
    """
    return (await _verified(token, now=now)).workspace_id


async def verify_grant_viewer(
    token: str, *, now: int | None = None
) -> tuple[str, str | None]:
    """:func:`verify_file_grant` and the member the grant is bound to (None
    for an owner's), whose access the caller checks before serving."""
    grant = await _verified(token, now=now)
    return grant.workspace_id, grant.viewer_id


def grant_prefix(grant: FileGrant | str) -> str:
    """The route prefix a client joins a percent-encoded path onto, for a
    grant or its token as a request path carried it."""
    token = grant if isinstance(grant, str) else grant.token
    return f"{FILE_GRANT_ROUTE}{token}/"
