"""``_decode_token``: the first-party session check every route relies on."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from src.server.auth import jwt_bearer
from tests.unit.server.auth.tokens import (
    SESSION_KEY,
    SESSION_KID,
    USER_ID,
    StaticKeys,
    public_jwk,
    session_token,
)


@pytest.fixture(autouse=True)
def session_keys():
    keys = StaticKeys(public_jwk(SESSION_KEY, SESSION_KID))
    with patch.object(jwt_bearer, "_signing_keys", keys):
        yield keys


@pytest.mark.asyncio
async def test_a_session_names_its_user():
    info = await jwt_bearer._decode_token(session_token())
    assert info == jwt_bearer.AuthInfo(user_id=USER_ID, auth_provider="email")


@pytest.mark.asyncio
async def test_keys_that_cannot_be_fetched_are_an_outage(session_keys):
    session_keys.usable = False
    with pytest.raises(HTTPException) as exc:
        await jwt_bearer._decode_token(session_token())
    assert exc.value.status_code == 503
    assert exc.value.headers["Retry-After"] == str(int(jwt_bearer._SigningKeys.UNKNOWN_KID_COOLDOWN))
