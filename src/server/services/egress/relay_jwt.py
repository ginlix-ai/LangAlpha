"""Relay JWT: the only credential a sandbox holds.

Minted host-side at turn-start, written into the sandbox at 0600, and accepted
exclusively by the egress relay route — deliberately NOT the app's user auth
(OSS mode authenticates without a bearer, so reusing that dependency would
leave the relay open). The JWT authenticates the sandbox; authorization is a
per-request grant lookup, so revocation never waits on expiry.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

import jwt

__all__ = [
    "MintedJwt",
    "RelayClaims",
    "RelayJwtError",
    "mint_relay_jwt",
    "validate_relay_jwt",
]

ISSUER = "langalpha"
AUDIENCE = "langalpha-egress-relay"
ALGORITHM = "HS256"  # fixed single-entry allowlist — never taken from the header
# Minted at session acquisition and re-minted only when the acquire-time check
# finds the token under the threshold — so the credential life a turn can rely
# on is the THRESHOLD (a warm acquire keeps anything above it), and the
# threshold must cover the per-turn cap (config.yaml workflow_timeout=21600)
# with margin or a long turn loses egress mid-run. TTL minus threshold is only
# the remint cadence. Long tokens are free here: revocation is a per-request
# grant lookup, never expiry.
DEFAULT_TTL_SECONDS = 8 * 60 * 60
LEEWAY_SECONDS = 30
REMINT_THRESHOLD_SECONDS = 21600 + 30 * 60  # turn cap + bringup/skew margin

_REQUIRED_CLAIMS = ["iss", "aud", "sub", "workspace_id", "sandbox_id", "iat", "nbf", "exp", "jti"]


class RelayJwtError(Exception):
    """The presented token failed validation (never says why to the caller)."""


@dataclass(frozen=True)
class RelayClaims:
    user_id: str
    workspace_id: str
    sandbox_id: str
    jti: str
    expires_at: int


@dataclass(frozen=True)
class MintedJwt:
    """A freshly minted token with the expiry it was encoded with — callers
    schedule the remint off ``expires_at`` rather than recomputing it."""

    token: str
    expires_at: int


def mint_relay_jwt(
    secret: str,
    *,
    user_id: str,
    workspace_id: str,
    sandbox_id: str,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> MintedJwt:
    now = int(time.time())
    expires_at = now + ttl_seconds
    token = jwt.encode(
        {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": user_id,
            "workspace_id": workspace_id,
            "sandbox_id": sandbox_id,
            "iat": now,
            "nbf": now,
            "exp": expires_at,
            "jti": uuid.uuid4().hex,
        },
        secret,
        algorithm=ALGORITHM,
    )
    return MintedJwt(token=token, expires_at=expires_at)


def validate_relay_jwt(secret: str, token: str) -> RelayClaims:
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=[ALGORITHM],
            issuer=ISSUER,
            audience=AUDIENCE,
            leeway=LEEWAY_SECONDS,
            options={"require": _REQUIRED_CLAIMS},
        )
    except jwt.PyJWTError as exc:
        raise RelayJwtError("invalid relay token") from exc
    for claim in ("sub", "workspace_id", "sandbox_id", "jti"):
        if not isinstance(payload.get(claim), str) or not payload[claim]:
            raise RelayJwtError("invalid relay token")
    return RelayClaims(
        user_id=payload["sub"],
        workspace_id=payload["workspace_id"],
        sandbox_id=payload["sandbox_id"],
        jti=payload["jti"],
        expires_at=int(payload["exp"]),
    )


def needs_remint(claims_expires_at: int, *, now: float | None = None) -> bool:
    return (claims_expires_at - (now if now is not None else time.time())) < REMINT_THRESHOLD_SECONDS
