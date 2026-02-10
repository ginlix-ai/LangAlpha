"""
Supabase JWT verification.

Decodes asymmetric JWTs (RS256/ES256) using JWKS public keys fetched from the
Supabase project endpoint. Returns the user UUID from the `sub` claim.

When ``SUPABASE_URL`` is **not set**, authentication is bypassed and all
requests are attributed to a default local-dev identity.  This lets
contributors run the stack locally without a Supabase project.
"""

import os

import jwt
from jwt import PyJWKClient
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_AUTH_ENABLED = bool(os.getenv("SUPABASE_URL", ""))
LOCAL_DEV_USER_ID = os.getenv("AUTH_USER_ID", "local-dev-user")

_bearer_scheme = HTTPBearer(auto_error=False)

_jwks_client: PyJWKClient | None = None


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        supabase_url = os.getenv("SUPABASE_URL", "")
        if not supabase_url:
            raise RuntimeError("SUPABASE_URL environment variable is not set")
        jwks_url = f"{supabase_url.rstrip('/')}/auth/v1/.well-known/jwks.json"
        _jwks_client = PyJWKClient(jwks_url, cache_jwk_set=True, lifespan=300)
    return _jwks_client


def _decode_token(token: str) -> str:
    """Decode a Supabase JWT using JWKS public key and return the user UUID."""
    try:
        client = _get_jwks_client()
        signing_key = client.get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256", "ES256"],
            audience="authenticated",
        )
        user_id: str = payload.get("sub", "")
        if not user_id:
            raise HTTPException(status_code=401, detail="Token missing sub claim")
        return user_id
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
    if not _AUTH_ENABLED:
        return LOCAL_DEV_USER_ID
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return _decode_token(credentials.credentials)
