"""Claude's PKCE sign-in: a random state, never the verifier, and the setup-token
variant asked for and checked the way ``claude setup-token`` asks."""

import base64
import hashlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from src.server.services import claude_oauth


def _params(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}


@pytest.mark.parametrize(
    ("start", "host", "scope"),
    [
        (claude_oauth.generate_authorize_url, "https://claude.ai/oauth/authorize", claude_oauth.CLAUDE_SCOPES),
        (claude_oauth.authorize_setup_token, "https://claude.com/cai/oauth/authorize", "user:inference"),
    ],
)
def test_a_sign_in_shows_a_random_state_and_keeps_the_verifier_back(start, host, scope):
    url, verifier, state = start()
    params = _params(url)
    assert url.startswith(host + "?")
    assert params["state"] == state and state != verifier and verifier not in url
    assert params["scope"] == scope
    assert params["code_challenge"] == base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    assert list(params) == [
        "code", "client_id", "response_type", "redirect_uri", "scope", "code_challenge", "code_challenge_method", "state",
    ]


GRANT = {
    "access_token": "sk-ant-oat01-minted",
    "refresh_token": "rt-dropped",
    "refresh_token_expires_in": 99,
    "expires_in": 31_536_000,
    "scope": "user:inference",
    "token_uuid": "tok-uuid",
    "account": {"email_address": "user@example.com", "uuid": "acct"},
}


@pytest.mark.asyncio
async def test_a_setup_token_is_asked_for_a_year_with_no_scope_and_its_refresh_token_dropped():
    request = AsyncMock(return_value=GRANT)
    with patch.object(claude_oauth, "_token_request", request):
        minted = await claude_oauth.mint_setup_token("code", "state", "verifier")
    body = request.await_args.args[0]
    assert body["expires_in"] == 31_536_000 and "scope" not in body
    assert (body["code"], body["state"], body["code_verifier"]) == ("code", "state", "verifier")
    assert (minted.access_token, minted.token_uuid, minted.email) == ("sk-ant-oat01-minted", "tok-uuid", "user@example.com")
    assert abs(minted.expires_at - (datetime.now(timezone.utc) + timedelta(days=365))) < timedelta(minutes=1)
    assert "rt-dropped" not in repr(minted) and "sk-ant" not in repr(minted)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"scope": "user:inference user:profile"}, {"expires_in": 3600}, {"expires_in": None},
    {"access_token": None}, {"access_token": ""}, {"account": "user@example.com"},
])
async def test_a_grant_that_is_not_a_setup_token_is_refused(change):
    with patch.object(claude_oauth, "_token_request", AsyncMock(return_value={**GRANT, **change})):
        with pytest.raises(claude_oauth.UnexpectedGrant):
            await claude_oauth.mint_setup_token("code", "state", "verifier")
