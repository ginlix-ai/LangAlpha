"""``GET /api/v1/auth/principal`` and the boundary around it.

The principal route is the one place an outside app's token is accepted. The
same token must keep failing on every ordinary route, which authenticates
through ``get_current_user_id``, until routes declare the scopes they need.
Real verification runs here: no auth dependency is overridden.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest_asyncio
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.server.auth import jwt_bearer, oauth_resource
from src.server.utils import api
from tests.unit.server.auth.tokens import (
    APP_KEY,
    APP_KID,
    AUDIENCE,
    CLIENT_ID,
    ISSUER,
    JWKS_URL,
    SESSION_KEY,
    SESSION_KID,
    USER_ID,
    StaticKeys,
    app_token,
    public_jwk,
    session_token,
)


@pytest_asyncio.fixture
async def client():
    from src.server.app.users import router

    app = FastAPI()
    app.include_router(router)
    with (
        patch.object(api, "HOST_MODE", "platform"),
        patch.object(api, "_SERVICE_TOKEN", ""),
        patch.object(jwt_bearer, "_signing_keys", StaticKeys(public_jwk(SESSION_KEY, SESSION_KID))),
        patch.object(oauth_resource, "_signing_keys", StaticKeys(public_jwk(APP_KEY, APP_KID))),
        patch.object(oauth_resource, "OAUTH_RESOURCE_ISSUER", ISSUER),
        patch.object(oauth_resource, "OAUTH_RESOURCE_JWKS_URL", JWKS_URL),
        patch.object(oauth_resource, "OAUTH_RESOURCE_AUDIENCES", (AUDIENCE,)),
        patch.object(oauth_resource, "AUTH_SERVICE_URL", ""),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_a_session_is_a_first_party_principal(client):
    resp = await client.get("/api/v1/auth/principal", headers=_bearer(session_token()))

    assert resp.status_code == 200
    assert resp.json() == {"user_id": USER_ID, "auth": "session", "client_id": None, "scopes": []}


@pytest.mark.asyncio
async def test_an_outside_app_is_named_with_what_it_may_do(client):
    resp = await client.get("/api/v1/auth/principal", headers=_bearer(app_token(scope="data:read")))

    assert resp.status_code == 200
    assert resp.json() == {
        "user_id": USER_ID,
        "auth": "oauth",
        "client_id": CLIENT_ID,
        "scopes": ["data:read"],
    }


@pytest.mark.asyncio
async def test_an_outside_app_still_fails_on_an_ordinary_route(client):
    resp = await client.get("/api/v1/users/me", headers=_bearer(app_token()))
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_an_outside_app_refused_by_the_verifier_is_not_retried_as_a_session(client):
    resp = await client.get(
        "/api/v1/auth/principal", headers=_bearer(app_token(aud="https://elsewhere.example.test"))
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_no_credential_is_refused(client):
    resp = await client.get("/api/v1/auth/principal")
    assert resp.status_code == 401
