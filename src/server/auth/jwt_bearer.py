"""
Supabase JWT verification.

Decodes asymmetric JWTs (RS256/ES256) using JWKS public keys fetched from the
Supabase project endpoint. Returns the user UUID from the `sub` claim and
optionally the ``auth_provider`` from ``app_metadata.provider``.

When ``SUPABASE_URL`` is **not set**, authentication is bypassed and all
requests are attributed to a default local-dev identity.  This lets
contributors run the stack locally without a Supabase project.
"""

import asyncio
import contextvars
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import jwt
from jwt import PyJWK, PyJWKClient
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.config.settings import HOST_MODE, LOCAL_DEV_USER_ID, SUPABASE_URL

logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class AuthInfo:
    """Decoded JWT fields needed by the auth-sync flow."""
    user_id: str
    auth_provider: str | None = None


class _SigningKeys:
    """Per-worker cache of the JWKS signing keys that never fetches on the loop.

    Every authenticated request verifies against these keys, so a blocking
    fetch on expiry froze every stream on the worker for the round trip. Fetches
    run in a thread and concurrent callers share one. A set older than
    ``REFRESH_AFTER`` is refetched before it verifies, so a rotated-out key is
    dropped as promptly as before; after a failed refetch the cached set is
    served while retries run in the background, until ``MAX_STALE``, so an
    outage is neither a sign-out nor a stall. An unknown ``kid`` (key
    rotation) waits for a refetch, at most one per cooldown on its own clock, so
    forged ``kid``s cannot drive a fetch storm and routine refreshes cannot
    starve a real rotation.
    """

    REFRESH_AFTER = 300.0
    MAX_STALE = 3600.0
    UNKNOWN_KID_COOLDOWN = 30.0
    FETCH_TIMEOUT = 10.0

    def __init__(self, jwks_url: Callable[[], str], *, label: str = "JWKS") -> None:
        self._jwks_url = jwks_url
        self._label = label
        self._client: PyJWKClient | None = None
        self._keys: dict[str, PyJWK] = {}
        self._fetched_at = 0.0
        self._last_attempt = float("-inf")
        self._last_kid_fetch = float("-inf")
        self._last_failed = False
        self._inflight: asyncio.Task | None = None

    def _client_for(self) -> PyJWKClient:
        if self._client is None:
            self._client = PyJWKClient(
                self._jwks_url(), cache_jwk_set=False, timeout=self.FETCH_TIMEOUT
            )
        return self._client

    def _fetch(self) -> dict[str, PyJWK]:
        keys = self._client_for().get_signing_keys(refresh=True)
        return {k.key_id: k for k in keys}

    async def _refresh(self) -> None:
        try:
            self._keys = await asyncio.to_thread(self._fetch)
            self._fetched_at = time.monotonic()
            self._last_failed = False
        except Exception as exc:
            self._last_failed = True
            logger.warning(f"[{self._label}] Refresh failed, serving the cached set: {exc}")

    def _start_refresh(self) -> asyncio.Task:
        """One fetch at a time; concurrent callers share it."""
        if self._inflight is None or self._inflight.done():
            self._last_attempt = time.monotonic()
            # A fresh context: the task must not pin the request that started it.
            self._inflight = asyncio.get_running_loop().create_task(
                self._refresh(), context=contextvars.Context()
            )
        return self._inflight

    async def refresh(self) -> None:
        await asyncio.shield(self._start_refresh())

    @property
    def usable(self) -> bool:
        return bool(self._keys) and time.monotonic() - self._fetched_at <= self.MAX_STALE

    async def get(self, kid: str) -> PyJWK | None:
        """The signing key for ``kid``, or None if unknown or the set is unusable."""
        now = time.monotonic()
        refreshing = self._inflight is not None and not self._inflight.done()
        if not self.usable or kid not in self._keys:
            if refreshing or now - self._last_kid_fetch >= self.UNKNOWN_KID_COOLDOWN:
                if not refreshing:
                    self._last_kid_fetch = now
                await self.refresh()
        elif now - self._fetched_at > self.REFRESH_AFTER and (
            refreshing or now - self._last_attempt >= self.UNKNOWN_KID_COOLDOWN
        ):
            # Once a refresh has failed, retry in the background: holding
            # requests on a fetch that may hang to its timeout buys nothing.
            if self._last_failed:
                self._start_refresh()
            else:
                await self.refresh()
        return self._keys.get(kid) if self.usable else None


def _supabase_jwks_url() -> str:
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL environment variable is not set")
    return f"{SUPABASE_URL.rstrip('/')}/auth/v1/.well-known/jwks.json"


_signing_keys = _SigningKeys(_supabase_jwks_url)


def auth_unavailable(retry_after: int) -> HTTPException:
    """The 503 for a credential that cannot be checked right now.

    Structured like the credit gate's 503, so clients show an outage rather
    than reading the status as a quota denial.
    """
    return HTTPException(
        status_code=503,
        detail={
            "message": "Service temporarily unavailable. Please try again shortly.",
            "type": "service_unavailable",
            "retry_after": retry_after,
        },
        headers={"Retry-After": str(retry_after)},
    )


async def warm_jwks() -> None:
    """Load the signing keys at startup so the first request doesn't wait."""
    if HOST_MODE != "oss" and SUPABASE_URL:
        await _signing_keys.refresh()


async def _decode_token(token: str) -> AuthInfo:
    """Decode a Supabase JWT and return user UUID + auth provider."""
    try:
        kid = jwt.get_unverified_header(token).get("kid")
        if not kid:
            raise HTTPException(status_code=401, detail="Invalid token")
        signing_key = await _signing_keys.get(kid)
        if signing_key is None:
            if not _signing_keys.usable:
                # The next fetch waits out the cooldown, so retry after it.
                raise auth_unavailable(int(_SigningKeys.UNKNOWN_KID_COOLDOWN))
            raise HTTPException(status_code=401, detail="Invalid token")
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256", "ES256"],
            audience="authenticated",
        )
        user_id: str = payload.get("sub", "")
        if not user_id:
            raise HTTPException(status_code=401, detail="Token missing sub claim")
        auth_provider = payload.get("app_metadata", {}).get("provider")
        return AuthInfo(user_id=user_id, auth_provider=auth_provider)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def verify_jwt_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> str:
    """FastAPI dependency — extracts Bearer token via HTTPBearer and verifies it.

    Returns the Supabase user UUID (``sub`` claim) which is used directly
    as ``user_id`` across all database tables.

    When Supabase auth is disabled (``SUPABASE_URL`` unset), returns a
    static local-dev user ID without requiring a token.
    """
    if HOST_MODE == "oss":
        return LOCAL_DEV_USER_ID
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return (await _decode_token(credentials.credentials)).user_id


async def get_current_auth_info(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> AuthInfo:
    """FastAPI dependency — returns both ``user_id`` and ``auth_provider``.

    Used by the auth-sync endpoint to persist the provider on first login.
    """
    if HOST_MODE == "oss":
        return AuthInfo(user_id=LOCAL_DEV_USER_ID)
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return await _decode_token(credentials.credentials)
