"""Access tokens that outside apps get from an OAuth issuer.

Off unless ``OAUTH_RESOURCE_ISSUER``, ``OAUTH_RESOURCE_JWKS_URL`` and
``OAUTH_RESOURCE_AUDIENCE`` are all set. ``get_current_user_id`` never accepts
these tokens: only a route that opts in through
``src.server.dependencies.principal`` does, until routes declare the scopes
they need.

With ``AUTH_SERVICE_URL`` set, every token also passes the platform's consent
check, which pins a consent (``sid``) to the first subject it saw and knows
when the user disconnected the app. The issuer's own tokens stay valid until
they expire, so this check is what ends an app's access promptly.
"""

import hashlib
import json
import logging
from dataclasses import dataclass

import jwt
from fastapi import HTTPException

from src.config.env import (
    AUTH_SERVICE_URL,
    OAUTH_RESOURCE_AUDIENCES,
    OAUTH_RESOURCE_ISSUER,
    OAUTH_RESOURCE_JWKS_URL,
)
from src.server.auth.jwt_bearer import _SigningKeys, auth_unavailable
from src.server.dependencies.usage_limits import get_http_client, service_headers
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

ALGORITHM = "RS256"
LEEWAY_SECONDS = 30
CONSENT_CACHE_TTL = 30
# Long enough that a retry lands after the platform restarts in place.
CONSENT_RETRY_AFTER = 15
_REQUIRED_CLAIMS = ["iss", "aud", "sub", "exp", "iat"]
# RFC 6750: tells the app to get a new token rather than resend this one.
_INVALID_TOKEN = {"WWW-Authenticate": 'Bearer error="invalid_token"'}


@dataclass(frozen=True)
class OAuthPrincipal:
    user_id: str
    client_id: str | None
    sid: str | None
    audience: str
    scopes: tuple[str, ...]


def _jwks_url() -> str:
    if not OAUTH_RESOURCE_JWKS_URL:
        raise RuntimeError("OAUTH_RESOURCE_JWKS_URL environment variable is not set")
    return OAUTH_RESOURCE_JWKS_URL


_signing_keys = _SigningKeys(_jwks_url, label="OAuth JWKS")


def oauth_resource_enabled() -> bool:
    return bool(OAUTH_RESOURCE_ISSUER and OAUTH_RESOURCE_JWKS_URL and OAUTH_RESOURCE_AUDIENCES)


def consent_check_enabled() -> bool:
    return bool(AUTH_SERVICE_URL)


async def warm_oauth_resource_keys() -> None:
    """Load the issuer's signing keys at startup so the first app doesn't wait."""
    if oauth_resource_enabled():
        await _signing_keys.refresh()
    elif OAUTH_RESOURCE_ISSUER or OAUTH_RESOURCE_JWKS_URL or OAUTH_RESOURCE_AUDIENCES:
        logger.warning(
            "Outside-app tokens are off: set all of OAUTH_RESOURCE_ISSUER, "
            "OAUTH_RESOURCE_JWKS_URL and OAUTH_RESOURCE_AUDIENCE to accept them"
        )


def from_resource_issuer(token: str) -> bool:
    """Whether ``token`` names the configured issuer, read without verifying it.

    Only chooses the verifier; ``verify_access_token`` checks everything.
    """
    if not oauth_resource_enabled():
        return False
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
    except jwt.InvalidTokenError:
        return False
    return claims.get("iss") == OAUTH_RESOURCE_ISSUER


def _unauthorized(message: str, **extra: str) -> HTTPException:
    detail = {"message": message, **extra} if extra else message
    return HTTPException(status_code=401, detail=detail, headers=_INVALID_TOKEN)


def _str_claim(claims: dict, name: str) -> str | None:
    value = claims.get(name)
    return value if isinstance(value, str) and value else None


async def _verified_claims(token: str) -> dict:
    try:
        header = jwt.get_unverified_header(token)
    except jwt.InvalidTokenError:
        raise _unauthorized("Invalid token")
    kid = header.get("kid")
    # Refused before the key lookup, so a token in another algorithm cannot
    # spend an unknown-kid refetch.
    if header.get("alg") != ALGORITHM or not kid:
        raise _unauthorized("Invalid token")
    signing_key = await _signing_keys.get(kid)
    if signing_key is None:
        if not _signing_keys.usable:
            raise auth_unavailable(int(_SigningKeys.UNKNOWN_KID_COOLDOWN))
        raise _unauthorized("Invalid token")
    try:
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=[ALGORITHM],
            issuer=OAUTH_RESOURCE_ISSUER,
            audience=list(OAUTH_RESOURCE_AUDIENCES),
            leeway=LEEWAY_SECONDS,
            options={"require": _REQUIRED_CLAIMS},
        )
    except jwt.ExpiredSignatureError:
        raise _unauthorized("Token expired")
    except jwt.InvalidTokenError:
        raise _unauthorized("Invalid token")
    if _str_claim(claims, "sub") is None:
        raise _unauthorized("Invalid token")
    return claims


def _matched_audience(aud: str | list[str]) -> str:
    claimed = [aud] if isinstance(aud, str) else aud
    return next(a for a in OAUTH_RESOURCE_AUDIENCES if a in claimed)


async def verify_access_token(token: str) -> OAuthPrincipal:
    """Verify an outside app's access token; 401 when refused, 503 when it
    cannot be checked right now."""
    claims = await _verified_claims(token)
    user_id = claims["sub"]
    client_id = _str_claim(claims, "client_id")
    sid = _str_claim(claims, "sid")
    audience = _matched_audience(claims["aud"])

    if not consent_check_enabled():
        scope = claims.get("scope")
        scopes = tuple(scope.split()) if isinstance(scope, str) else ()
        return OAuthPrincipal(user_id, client_id, sid, audience, scopes)

    if sid is None:
        raise _unauthorized("Token missing sid claim")
    decision = await _consent_decision(user_id, sid, client_id, audience)
    if not decision["active"]:
        raise _unauthorized(
            "This app's access is no longer active",
            reason=decision.get("reason") or "inactive",
        )
    # The token's own scope holds only what the issuer can express; what the
    # app may do is what the user's plan grants, which only the platform knows.
    return OAuthPrincipal(user_id, client_id, sid, audience, tuple(decision["scopes"]))


# ---------------------------------------------------------------------------
# The platform's consent check
# ---------------------------------------------------------------------------


def consent_cache_key(sid: str, sub: str) -> str:
    # Hashed as a pair, so no two claim values can meet across a separator.
    digest = hashlib.sha256(json.dumps([sid, sub]).encode()).hexdigest()
    return f"oauth_consent:{digest}"


def _is_decision(value: object) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("active"), bool)
        and isinstance(value.get("scopes"), list)
    )


async def _consent_decision(
    sub: str, sid: str, client_id: str | None, audience: str
) -> dict:
    """The platform's decision for this consent, shared by every worker for
    ``CONSENT_CACHE_TTL``. A check that could not be made is never cached."""
    cache = get_cache_client()
    key = consent_cache_key(sid, sub)
    cached = await cache.get(key)
    if _is_decision(cached):
        return cached
    decision = await _ask_platform(sub, sid, client_id, audience)
    await cache.set(key, decision, ttl=CONSENT_CACHE_TTL)
    return decision


def _consent_unavailable() -> HTTPException:
    return auth_unavailable(CONSENT_RETRY_AFTER)


async def _ask_platform(
    sub: str, sid: str, client_id: str | None, audience: str
) -> dict:
    """Fails closed: without an answer the app is refused, never let through."""
    url = f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/connect/verify"
    body = {"sub": sub, "sid": sid, "client_id": client_id, "aud": audience}
    try:
        client = await get_http_client()
        resp = await client.post(url, json=body, headers=service_headers())
    except Exception as exc:
        logger.warning("Consent check unreachable: %s", exc)
        raise _consent_unavailable()
    if resp.status_code != 200:
        logger.warning(
            "Consent check returned %d: %s", resp.status_code, resp.text[:200]
        )
        raise _consent_unavailable()
    try:
        answer = resp.json()
    except ValueError as exc:
        logger.warning("Consent check returned an unreadable 200: %s", exc)
        raise _consent_unavailable()
    if not isinstance(answer, dict) or not isinstance(answer.get("active"), bool):
        logger.warning("Consent check returned no decision: %.200r", answer)
        raise _consent_unavailable()

    if not answer["active"]:
        reason = answer.get("reason")
        return {
            "active": False,
            "reason": reason if isinstance(reason, str) and reason else "inactive",
            "scopes": [],
        }
    if answer.get("user_id") not in (None, sub):
        return {"active": False, "reason": "subject_mismatch", "scopes": []}
    scopes = answer.get("scopes")
    return {
        "active": True,
        "reason": None,
        "scopes": [s for s in scopes if isinstance(s, str)] if isinstance(scopes, list) else [],
    }
