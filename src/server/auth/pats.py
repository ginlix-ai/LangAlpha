"""Personal access tokens (``la_pat_…``), which the platform issues and checks.

A PAT is how a script or CI job with no browser signs in. The platform holds
the keys and answers, per call, whether one is live and what it may do now:
its scopes are re-read against the owner's current plan, so a downgrade
narrows a key already issued. Answers are shared by every worker for
``CACHE_TTL``, so a revoked key stops working within that.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

from fastapi import HTTPException

from src.config.env import AUTH_SERVICE_URL
from src.server.auth.jwt_bearer import auth_unavailable
from src.server.utils.http_client import get_http_client
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

PREFIX = "la_pat_"
CACHE_TTL = 30
_RETRY_AFTER = 15
_INVALID_TOKEN = {"WWW-Authenticate": 'Bearer error="invalid_token"'}


@dataclass(frozen=True)
class PatPrincipal:
    user_id: str
    scopes: tuple[str, ...]


def is_pat(token: str) -> bool:
    return token.startswith(PREFIX)


def _cache_key(token: str) -> str:
    return f"pat_validation:{hashlib.sha256(token.encode()).hexdigest()}"


def _refused(message: str) -> HTTPException:
    return HTTPException(status_code=401, detail=message, headers=_INVALID_TOKEN)


async def verify_pat(token: str) -> PatPrincipal:
    """The key's owner and live scopes; 401 when refused, 503 when the
    platform cannot answer (fails closed)."""
    if not AUTH_SERVICE_URL:
        raise _refused("This server does not accept personal access tokens")
    cache = get_cache_client()
    key = _cache_key(token)
    answer = await cache.get(key)
    if not _is_answer(answer):
        answer = await _ask_platform(token)
        await cache.set(key, answer, ttl=CACHE_TTL)
    if not answer["valid"] or not answer.get("user_id"):
        raise _refused("Invalid or expired personal access token")
    return PatPrincipal(answer["user_id"], tuple(answer["scopes"]))


def _is_answer(value: object) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("valid"), bool)
        and isinstance(value.get("scopes"), list)
    )


async def _ask_platform(token: str) -> dict:
    # No service headers: /validate checks a service token before the key,
    # and would answer for the service instead.
    url = f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/validate"
    try:
        client = await get_http_client()
        resp = await client.post(url, headers={"X-Api-Key": token})
    except Exception as exc:
        logger.warning("PAT check unreachable: %s", type(exc).__name__)
        raise auth_unavailable(_RETRY_AFTER)
    if resp.status_code != 200:
        logger.warning("PAT check returned %d", resp.status_code)
        raise auth_unavailable(_RETRY_AFTER)
    try:
        body = resp.json()
    except ValueError:
        logger.warning("PAT check returned an unreadable 200")
        raise auth_unavailable(_RETRY_AFTER)
    if not isinstance(body, dict) or not isinstance(body.get("valid"), bool):
        raise auth_unavailable(_RETRY_AFTER)
    scopes = body.get("scopes")
    user_id = body.get("user_id")
    return {
        "valid": body["valid"],
        "user_id": user_id if isinstance(user_id, str) else None,
        "scopes": [s for s in scopes if isinstance(s, str)] if isinstance(scopes, list) else [],
    }
