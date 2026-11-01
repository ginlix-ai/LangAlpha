"""The credential a frame carries to fetch a published artifact version's files.

A grant names one artifact and an expiry, signed with the key migration 101
minted in Postgres, apart from the workspace file grant's so neither can be
read as the other. It rides in the path rather than a cookie: the frame is
cross-site to the app, so a cookie set there would be third party, which
Safari drops. The artifact id, not the grant, names the frame's origin, so
its storage survives a new grant.

Shape is ``v1.<artifact>.<exp>.<sig>``, the workspace file grant's
(``services/signed_grants``).

A visitor who opens the artifact's link gets a grant of another shape,
``s1.<code>.<artifact>.<revision>.<seq>.<exp>.<sig>``, under a key of its
own. The owner's grant opens every version and answers to nothing but its
signature; a visitor's opens one version, and only while the link it was
minted from still says so, which the server reads from the link's row on
every fetch (``database/artifacts.get_shared_version_file``). The separate
key is what keeps the two from ever being read as each other.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.server.services.signed_grants import COUNT, GrantCodec

__all__ = [
    "ARTIFACT_GRANT_ROUTE",
    "DEFAULT_TTL_SECONDS",
    "ArtifactGrant",
    "ArtifactGrantError",
    "ArtifactShareGrant",
    "grant_prefix",
    "is_share_grant",
    "mint_artifact_grant",
    "mint_artifact_share_grant",
    "verify_artifact_grant",
    "verify_artifact_share_grant",
]

# The viewer re-mints well before it runs out, as it does a file grant.
DEFAULT_TTL_SECONDS = 12 * 3600

#: The API-origin route a grant opens versions under: ``<route><token>/v/...``.
ARTIFACT_GRANT_ROUTE = "/api/v1/artifacts/g/"

_SHARE_CODE = re.compile(r"[0-9A-Za-z]{12}")
_ARTIFACT_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class ArtifactGrantError(Exception):
    """The presented grant does not open any artifact."""


_OWNER = GrantCodec(
    key_name="artifact_grant",
    version="v1",
    claim_patterns=(None,),
    error=ArtifactGrantError,
    label="artifact grant",
)
_VISITOR = GrantCodec(
    key_name="artifact_share_grant",
    version="s1",
    claim_patterns=(_SHARE_CODE, _ARTIFACT_ID, COUNT, COUNT),
    error=ArtifactGrantError,
    label="artifact share grant",
)


@dataclass(frozen=True)
class ArtifactGrant:
    token: str
    artifact_id: str
    expires_at: int


@dataclass(frozen=True)
class ArtifactShareGrant:
    """What a visitor's grant claims: the link it came from, at the revision
    it was read at, and the one version that link resolved to."""

    token: str
    code: str
    artifact_id: str
    revision: int
    seq: int
    expires_at: int


async def mint_artifact_grant(
    artifact_id: str, *, ttl: int = DEFAULT_TTL_SECONDS, now: int | None = None
) -> ArtifactGrant:
    token, expires_at = await _OWNER.mint((artifact_id,), ttl=ttl, now=now)
    return ArtifactGrant(token=token, artifact_id=artifact_id, expires_at=expires_at)


async def verify_artifact_grant(token: str, *, now: int | None = None) -> str:
    """The artifact a live grant opens, or ``ArtifactGrantError``.

    The caller answers every failure with the same 404 a missing file gets,
    so the reasons only ever reach a log.
    """
    (artifact_id,), _ = await _OWNER.verify(token, now=now)
    return artifact_id


def is_share_grant(token: str) -> bool:
    """Whether a token claims to be a visitor's grant, so it is checked as one."""
    return _VISITOR.claims_token(token)


async def mint_artifact_share_grant(
    code: str,
    artifact_id: str,
    *,
    revision: int,
    seq: int,
    ttl: int = DEFAULT_TTL_SECONDS,
    now: int | None = None,
) -> ArtifactShareGrant:
    """A visitor's grant for version ``seq``, as the link stood at ``revision``."""
    token, expires_at = await _VISITOR.mint(
        (code, artifact_id, str(revision), str(seq)), ttl=ttl, now=now
    )
    return ArtifactShareGrant(
        token=token,
        code=code,
        artifact_id=artifact_id,
        revision=revision,
        seq=seq,
        expires_at=expires_at,
    )


async def verify_artifact_share_grant(
    token: str, *, now: int | None = None
) -> ArtifactShareGrant:
    """What a live visitor's grant claims, or ``ArtifactGrantError``.

    Only that the server minted it and it has not expired. Whether the link
    still says what the grant claims is the share row's to answer, read on
    every fetch: a signature cannot be taken back, a row can.
    """
    (code, artifact_id, revision, seq), expires_at = await _VISITOR.verify(
        token, now=now
    )
    return ArtifactShareGrant(
        token=token,
        code=code,
        artifact_id=artifact_id,
        revision=int(revision),
        seq=int(seq),
        expires_at=expires_at,
    )


def grant_prefix(grant: ArtifactGrant | ArtifactShareGrant | str) -> str:
    """The API-origin prefix a viewer joins ``v/<seq>/<path>`` onto, for a
    deployment with no preview base: of a grant, or its token as a request
    path carried it."""
    token = grant if isinstance(grant, str) else grant.token
    return f"{ARTIFACT_GRANT_ROUTE}{token}/"
