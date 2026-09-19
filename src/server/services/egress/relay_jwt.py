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
    "identity_claim",
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

_REQUIRED_CLAIMS = ["iss", "aud", "sub", "workspace_id", "iat", "nbf", "exp", "jti"]

# The two machine-identity claims. Either, both, or neither may be present: a
# token carries `computer_id` once its session knows the computer, `sandbox_id`
# for the pre-computer shape, and neither for a host caller with no machine at
# all (Flash order reads, a session whose sandbox is not yet provisioned).
#
# Neither is authorized against -- authorization is the per-request grant lookup
# keyed by `workspace_id` -- so a token that names no machine is not a weaker
# credential, only a less informative audit record. What IS refused is an empty
# string: the sandbox-less callers used to mint `sandbox_id=""` and the
# validator rejected it, so they issued a credential they could not use.
_IDENTITY_CLAIMS = ("sandbox_id", "computer_id")


class RelayJwtError(Exception):
    """The presented token failed validation (never says why to the caller)."""


# Who is presenting the token. The relay applies one more refusal to a
# sandbox than to the host: a tool bound directly to the model is not callable
# from code, or the middleware gating the direct call could be walked around
# by hand-writing the JSON-RPC in ``ExecuteCode``. A token with no claim is
# read as the sandbox, so every credential minted before the claim existed
# keeps the stricter reading.
CALLER_SANDBOX = "sandbox"
CALLER_HOST = "host"
_CALLERS = frozenset({CALLER_SANDBOX, CALLER_HOST})


@dataclass(frozen=True)
class RelayClaims:
    user_id: str
    workspace_id: str
    sandbox_id: str | None
    jti: str
    expires_at: int
    caller: str = CALLER_SANDBOX
    computer_id: str | None = None

    @property
    def identity(self) -> str | None:
        """Which machine presented the token, preferring the computer id.

        For audit only. A host caller with no machine has neither claim, which
        is why this can be None.
        """
        return self.computer_id or self.sandbox_id


@dataclass(frozen=True)
class MintedJwt:
    """A freshly minted token with the expiry it was encoded with — callers
    schedule the remint off ``expires_at`` rather than recomputing it."""

    token: str
    expires_at: int


def identity_claim(value: object) -> str | None:
    """Normalize a machine identity into a claim: a non-empty string, or absent.

    Lenient on purpose, because the value comes from whatever the call site has
    in hand -- a sandbox that may not be provisioned yet, a session that may not
    name a computer. "No identity" is a valid claim shape; the empty string is
    the one the validator refuses, so it can no longer be minted.
    """
    return value if isinstance(value, str) and value else None


def mint_relay_jwt(
    secret: str,
    *,
    user_id: str,
    workspace_id: str,
    sandbox_id: str | None = None,
    computer_id: str | None = None,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    caller: str = CALLER_SANDBOX,
) -> MintedJwt:
    if caller not in _CALLERS:
        raise ValueError(f"unknown relay caller {caller!r}")
    now = int(time.time())
    expires_at = now + ttl_seconds
    payload = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": user_id,
        "workspace_id": workspace_id,
        "caller": caller,
        "iat": now,
        "nbf": now,
        "exp": expires_at,
        "jti": uuid.uuid4().hex,
    }
    for name, value in (("sandbox_id", sandbox_id), ("computer_id", computer_id)):
        claim = identity_claim(value)
        if claim is not None:
            payload[name] = claim
    token = jwt.encode(payload, secret, algorithm=ALGORITHM)
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
    for claim in ("sub", "workspace_id", "jti"):
        if not isinstance(payload.get(claim), str) or not payload[claim]:
            raise RelayJwtError("invalid relay token")
    identities: dict[str, str | None] = {}
    for claim in _IDENTITY_CLAIMS:
        value = payload.get(claim)
        if value is None:
            identities[claim] = None
        elif isinstance(value, str) and value:
            identities[claim] = value
        else:
            raise RelayJwtError("invalid relay token")
    caller = payload.get("caller", CALLER_SANDBOX)
    if caller not in _CALLERS:
        raise RelayJwtError("invalid relay token")
    return RelayClaims(
        user_id=payload["sub"],
        workspace_id=payload["workspace_id"],
        sandbox_id=identities["sandbox_id"],
        jti=payload["jti"],
        expires_at=int(payload["exp"]),
        caller=caller,
        computer_id=identities["computer_id"],
    )


def needs_remint(claims_expires_at: int, *, now: float | None = None) -> bool:
    return (claims_expires_at - (now if now is not None else time.time())) < REMINT_THRESHOLD_SECONDS
