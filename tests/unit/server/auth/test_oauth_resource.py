"""The verifier for outside apps' access tokens.

A security boundary, so the arms that matter are the refusals: a token is
accepted only when it is RS256-signed by the issuer's key, names the exact
issuer and one of the configured audiences, and carries a subject and its
validity window. With the platform's consent check on, it also needs a
consent the platform still calls active, and an unanswered check refuses.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import jwt
import pytest
from fastapi import HTTPException

from src.server.auth import oauth_resource
from tests.unit.server.auth.tokens import (
    APP_KEY,
    APP_KID,
    AUDIENCE,
    CLIENT_ID,
    ISSUER,
    JWKS_URL,
    OTHER_AUDIENCE,
    SESSION_KEY,
    SID,
    USER_ID,
    StaticKeys,
    app_claims,
    app_token,
    public_jwk,
    session_token,
)

PLATFORM = "http://platform.test"
SERVICE_HEADERS = {"X-Service-Token": "svc-unit"}
ACTIVE = {"active": True, "user_id": USER_ID, "scopes": ["data:read", "agent:run"]}


@pytest.fixture
def configured():
    keys = StaticKeys(public_jwk(APP_KEY, APP_KID))
    with (
        patch.object(oauth_resource, "OAUTH_RESOURCE_ISSUER", ISSUER),
        patch.object(oauth_resource, "OAUTH_RESOURCE_JWKS_URL", JWKS_URL),
        patch.object(oauth_resource, "OAUTH_RESOURCE_AUDIENCES", (AUDIENCE, OTHER_AUDIENCE)),
        patch.object(oauth_resource, "AUTH_SERVICE_URL", ""),
        patch.object(oauth_resource, "_signing_keys", keys),
    ):
        yield keys


class FakeCache:
    """The Redis cache client's get/set, JSON round trip included."""

    def __init__(self) -> None:
        self.store: dict[str, object] = {}
        self.ttls: dict[str, int | None] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value, ttl: int | None = None) -> bool:
        self.store[key] = json.loads(json.dumps(value))
        self.ttls[key] = ttl
        return True


@pytest.fixture
def platform(configured):
    cache = FakeCache()
    client = MagicMock()
    client.post = AsyncMock(return_value=httpx.Response(200, json=ACTIVE))
    with (
        patch.object(oauth_resource, "AUTH_SERVICE_URL", PLATFORM),
        patch.object(oauth_resource, "get_cache_client", return_value=cache),
        patch.object(oauth_resource, "get_http_client", AsyncMock(return_value=client)),
        patch.object(oauth_resource, "service_headers", return_value=SERVICE_HEADERS),
    ):
        yield SimpleNamespace(cache=cache, client=client)


async def _refused(token: str, status: int = 401) -> HTTPException:
    with pytest.raises(HTTPException) as exc:
        await oauth_resource.verify_access_token(token)
    assert exc.value.status_code == status
    return exc.value


# ---------------------------------------------------------------------------
# The token itself (consent check off)
# ---------------------------------------------------------------------------


class TestTokenChecks:
    @pytest.mark.asyncio
    async def test_a_valid_token_names_its_user_app_and_scopes(self, configured):
        principal = await oauth_resource.verify_access_token(app_token())

        assert principal.user_id == USER_ID
        assert principal.client_id == CLIENT_ID
        assert principal.sid == SID
        assert principal.audience == AUDIENCE
        assert principal.scopes == ("openid", "profile", "email", "offline_access")

    @pytest.mark.asyncio
    async def test_an_audience_list_needs_only_one_configured_audience(self, configured):
        token = app_token(aud=["https://elsewhere.example.test", OTHER_AUDIENCE])
        principal = await oauth_resource.verify_access_token(token)
        assert principal.audience == OTHER_AUDIENCE

    @pytest.mark.asyncio
    async def test_another_issuer_is_refused(self, configured):
        await _refused(app_token(iss="https://issuer.example.test/other"))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "aud", ["https://elsewhere.example.test", ["https://elsewhere.example.test"]]
    )
    async def test_an_audience_outside_the_list_is_refused(self, configured, aud):
        await _refused(app_token(aud=aud))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "token",
        [
            # The issuer's own key under another RSA algorithm.
            pytest.param(lambda: app_token(algorithm="RS384"), id="rs384"),
            pytest.param(
                lambda: app_token(key="unit-hmac-secret-" + "0" * 48, algorithm="HS256"),
                id="hs256",
            ),
            pytest.param(lambda: app_token(key=None, algorithm="none"), id="none"),
            # Signed by a key that is not the issuer's, under the issuer's kid.
            pytest.param(lambda: app_token(key=SESSION_KEY), id="foreign-key"),
            pytest.param(lambda: app_token(kid="unknown-kid"), id="unknown-kid"),
        ],
    )
    async def test_anything_but_rs256_from_the_issuers_key_is_refused(self, configured, token):
        await _refused(token())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("claim", ["sub", "exp", "iat", "aud", "iss"])
    async def test_a_missing_required_claim_is_refused(self, configured, claim):
        await _refused(app_token(**{claim: None}))

    @pytest.mark.asyncio
    async def test_an_empty_subject_is_refused(self, configured):
        await _refused(app_token(sub=""))

    @pytest.mark.asyncio
    async def test_an_expired_token_is_refused_past_the_leeway(self, configured):
        expired = app_token(exp=app_claims()["iat"] - oauth_resource.LEEWAY_SECONDS - 5)
        err = await _refused(expired)
        assert err.detail == "Token expired"
        assert err.headers["WWW-Authenticate"].startswith("Bearer")

    @pytest.mark.asyncio
    async def test_keys_that_cannot_be_fetched_are_an_outage_not_a_refusal(self, configured):
        configured.usable = False
        err = await _refused(app_token(), status=503)
        assert "Retry-After" in err.headers


class TestRouting:
    def test_only_the_configured_issuer_takes_the_outside_app_path(self, configured):
        assert oauth_resource.from_resource_issuer(app_token())
        assert not oauth_resource.from_resource_issuer(session_token())
        assert not oauth_resource.from_resource_issuer("not-a-jwt")

    def test_nothing_takes_it_while_the_verifier_is_off(self, configured):
        with patch.object(oauth_resource, "OAUTH_RESOURCE_AUDIENCES", ()):
            assert not oauth_resource.from_resource_issuer(app_token())


# ---------------------------------------------------------------------------
# The platform's consent check
# ---------------------------------------------------------------------------


class TestConsentCheck:
    @pytest.mark.asyncio
    async def test_an_active_consent_carries_the_scopes_the_platform_grants(self, platform):
        principal = await oauth_resource.verify_access_token(app_token())

        assert principal.user_id == USER_ID
        assert principal.scopes == ("data:read", "agent:run")
        platform.client.post.assert_awaited_once_with(
            f"{PLATFORM}/api/auth/connect/verify",
            json={"sub": USER_ID, "sid": SID, "client_id": CLIENT_ID, "aud": AUDIENCE},
            headers=SERVICE_HEADERS,
        )
        key = oauth_resource.consent_cache_key(SID, USER_ID)
        assert platform.cache.store[key]["active"] is True
        assert platform.cache.ttls[key] == oauth_resource.CONSENT_CACHE_TTL == 30

    @pytest.mark.asyncio
    async def test_a_token_without_a_consent_id_is_refused_before_asking(self, platform):
        await _refused(app_token(sid=None))
        platform.client.post.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_inactive_consent_is_refused_with_its_reason(self, platform):
        platform.client.post.return_value = httpx.Response(
            200, json={"active": False, "reason": "consent_revoked"}
        )
        err = await _refused(app_token())
        assert err.detail["reason"] == "consent_revoked"
        assert err.headers["WWW-Authenticate"] == 'Bearer error="invalid_token"'

    @pytest.mark.asyncio
    async def test_an_active_answer_for_another_user_is_refused(self, platform):
        platform.client.post.return_value = httpx.Response(
            200, json={**ACTIVE, "user_id": "someone-else"}
        )
        err = await _refused(app_token())
        assert err.detail["reason"] == "subject_mismatch"

    @pytest.mark.asyncio
    async def test_a_cached_decision_skips_the_platform(self, platform):
        await oauth_resource.verify_access_token(app_token())
        await oauth_resource.verify_access_token(app_token(jti="jti-unit-2"))
        platform.client.post.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_cached_refusal_also_skips_the_platform(self, platform):
        key = oauth_resource.consent_cache_key(SID, USER_ID)
        platform.cache.store[key] = {"active": False, "reason": "consent_revoked", "scopes": []}

        err = await _refused(app_token())
        assert err.detail["reason"] == "consent_revoked"
        platform.client.post.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "answer",
        [
            pytest.param(httpx.ConnectError("refused"), id="unreachable"),
            pytest.param(httpx.Response(500, text="boom"), id="5xx"),
            pytest.param(httpx.Response(401, json={"detail": "bad token"}), id="service-token-refused"),
            pytest.param(httpx.Response(200, text="<html>"), id="unreadable-200"),
            pytest.param(httpx.Response(200, json={"user_id": USER_ID}), id="no-decision"),
        ],
    )
    async def test_an_unanswered_check_fails_closed_and_is_not_cached(self, platform, answer):
        if isinstance(answer, Exception):
            platform.client.post.side_effect = answer
        else:
            platform.client.post.return_value = answer

        err = await _refused(app_token(), status=503)
        assert err.headers["Retry-After"] == str(oauth_resource.CONSENT_RETRY_AFTER)
        assert platform.cache.store == {}

    def test_the_cache_key_separates_its_two_claims(self):
        assert oauth_resource.consent_cache_key("a:b", "c") != oauth_resource.consent_cache_key(
            "a", "b:c"
        )


def test_the_issuer_signature_is_what_the_verifier_checks():
    """The fixture keys are real RS256: a token decodes with the issuer's
    public key and not with the session key's."""
    token = app_token()
    jwt.decode(token, APP_KEY.public_key(), algorithms=["RS256"], audience=AUDIENCE)
    with pytest.raises(jwt.InvalidSignatureError):
        jwt.decode(token, SESSION_KEY.public_key(), algorithms=["RS256"], audience=AUDIENCE)
