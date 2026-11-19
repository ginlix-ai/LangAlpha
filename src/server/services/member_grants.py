"""The grant a member's page carries to a members-only link's bearerless routes.

A members-only link is opened by its metadata route, which knows the viewer
from their session. Everything the page then loads (a thread's replay and
files, a file's frames) is fetched with no bearer, as a visitor's is, so the
metadata route hands a member a grant bound to the link and to them, and
every later request is judged again from it: the link still opens to members
and the viewer still is one (``share_audience.admits_member``).

A thread grant stands where the link's token stands in every path:
``t1.<token>.<viewer>.<exp>.<sig>``. A file grant stands where a visitor's
artifact grant stands: ``f1.<code>.<artifact>.<revision>.<seq>.<viewer>.<exp>.<sig>``.
Each has its own key (migration 138), so neither reads as the other or as any
older grant.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.server.services.signed_grants import COUNT, GrantCodec

#: An hour, renewed by the page before it runs out; a removed member's grant
#: is refused at once anyway, since membership is read on every request.
TTL_SECONDS = 3600


class MemberGrantError(Exception):
    """The presented grant opens nothing."""


# Thread tokens: link codes (12) and the base64url tokens from before them (22).
_THREAD_TOKEN = re.compile(r"[0-9A-Za-z_-]{12,64}")
_SHARE_CODE = re.compile(r"[0-9A-Za-z]{12}")
_ARTIFACT_ID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
#: A user id as it can stand in a grant: no dots, which separate its parts.
VIEWER = re.compile(r"[0-9A-Za-z_-]{1,255}")

_THREAD = GrantCodec(
    key_name="member_share_grant",
    version="t1",
    claim_patterns=(_THREAD_TOKEN, VIEWER),
    error=MemberGrantError,
    label="member thread grant",
)
_FILE = GrantCodec(
    key_name="artifact_member_grant",
    version="f1",
    claim_patterns=(_SHARE_CODE, _ARTIFACT_ID, COUNT, COUNT, VIEWER),
    error=MemberGrantError,
    label="member file grant",
)


def can_hold(viewer_id: str) -> bool:
    """Whether a user id fits in a grant; one that does not is never minted
    one, and so is answered as a non-member."""
    return VIEWER.fullmatch(viewer_id) is not None


@dataclass(frozen=True)
class ThreadGrant:
    token: str
    share_token: str
    viewer_id: str
    expires_at: int


def is_thread_grant(value: str) -> bool:
    return _THREAD.claims_token(value)


async def mint_thread_grant(share_token: str, viewer_id: str) -> ThreadGrant:
    token, expires_at = await _THREAD.mint((share_token, viewer_id), ttl=TTL_SECONDS)
    return ThreadGrant(token, share_token, viewer_id, expires_at)


async def verify_thread_grant(value: str) -> ThreadGrant:
    (share_token, viewer_id), expires_at = await _THREAD.verify(value)
    return ThreadGrant(value, share_token, viewer_id, expires_at)


@dataclass(frozen=True)
class FileGrant:
    token: str
    code: str
    artifact_id: str
    revision: int
    seq: int
    viewer_id: str
    expires_at: int


def is_file_grant(value: str) -> bool:
    return _FILE.claims_token(value)


async def mint_file_grant(
    code: str, artifact_id: str, *, revision: int, seq: int, viewer_id: str
) -> FileGrant:
    token, expires_at = await _FILE.mint(
        (code, artifact_id, str(revision), str(seq), viewer_id), ttl=TTL_SECONDS
    )
    return FileGrant(token, code, artifact_id, revision, seq, viewer_id, expires_at)


async def verify_file_grant(value: str) -> FileGrant:
    (code, artifact_id, revision, seq, viewer_id), expires_at = await _FILE.verify(
        value
    )
    return FileGrant(
        value, code, artifact_id, int(revision), int(seq), viewer_id, expires_at
    )
