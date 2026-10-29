"""RS256 keys and tokens for the auth tests.

Two signers, as in production: the login provider signs first-party sessions
and an OAuth issuer signs the tokens outside apps hold. Each has its own key,
and ``StaticKeys`` stands in for a JWKS cache holding a fixed set.
"""

from __future__ import annotations

import time

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt import PyJWK
from jwt.algorithms import RSAAlgorithm

ISSUER = "https://issuer.example.test"
JWKS_URL = f"{ISSUER}/oauth2/jwks"
AUDIENCE = "https://api.example.test"
OTHER_AUDIENCE = "https://mcp.example.test"
SESSION_ISSUER = "https://login.example.test/auth/v1"

USER_ID = "user-unit-1"
CLIENT_ID = "https://app.example.test/client.json"
SID = "consent-unit-1"

APP_KID = "app-key"
SESSION_KID = "session-key"

APP_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
SESSION_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def public_jwk(private_key, kid: str) -> PyJWK:
    jwk = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    return PyJWK({**jwk, "kid": kid, "alg": "RS256", "use": "sig"})


class StaticKeys:
    """A JWKS cache that never fetches."""

    def __init__(self, *keys: PyJWK, usable: bool = True) -> None:
        self._keys = {k.key_id: k for k in keys}
        self.usable = usable

    async def get(self, kid: str) -> PyJWK | None:
        return self._keys.get(kid) if self.usable else None


def _claims(base: dict, overrides: dict) -> dict:
    """``base`` with ``overrides`` applied; an override of None drops the claim."""
    claims = {**base, **overrides}
    return {k: v for k, v in claims.items() if v is not None}


def app_claims(**overrides) -> dict:
    now = int(time.time())
    return _claims(
        {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": USER_ID,
            "client_id": CLIENT_ID,
            "sid": SID,
            "scope": "openid profile email offline_access",
            "jti": "jti-unit-1",
            "iat": now,
            "exp": now + 300,
        },
        overrides,
    )


def app_token(*, key=APP_KEY, kid: str = APP_KID, algorithm: str = "RS256", **overrides) -> str:
    return jwt.encode(app_claims(**overrides), key, algorithm=algorithm, headers={"kid": kid})


def session_token(**overrides) -> str:
    now = int(time.time())
    claims = _claims(
        {
            "iss": SESSION_ISSUER,
            "aud": "authenticated",
            "sub": USER_ID,
            "role": "authenticated",
            "app_metadata": {"provider": "email"},
            "iat": now,
            "exp": now + 3600,
        },
        overrides,
    )
    return jwt.encode(claims, SESSION_KEY, algorithm="RS256", headers={"kid": SESSION_KID})
