"""The Grok subscription contract: how long a token is trusted, what a refresh
answer and refusal mean, and how a model call's 403 is read.

Upstream answers are the ones auth.x.ai gave live probes (2026-10-24): a
device code of 30 minutes at a 5 second interval, ``authorization_pending``
until approved, and 400 ``invalid_grant`` for an unknown device code or
refresh token. The 403 readings follow Hermes Agent's.
"""

import base64
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.server.services import grok_oauth, oauth_health
from src.server.services.grok_oauth import (
    access_token_expiry,
    classify_device_poll,
    is_xai_endpoint,
    permanent_grok_refresh_failure,
    refresh_buffer,
    stale_token_refusal,
    token_set,
)
from src.server.services.oauth_health import valid_token
from src.server.services.oauth_protocol import (
    DevicePollFailed,
    DeviceUpstreamUnavailable,
    OAuthPlanNoApiAccess,
    TokenSet,
)
from src.server.services.oauth_providers import SUBSCRIPTIONS
from src.server.services.oauth_refresh_wait import REFRESH_BUFFER

_REQ = httpx.Request("POST", "https://auth.x.ai/oauth2/token")
_NOW = datetime.now(timezone.utc)


def _jwt(**claims) -> str:
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"eyJhbGciOiJub25lIn0.{body}.sig"


def _resp(status: int, body=None, headers=None) -> httpx.Response:
    return httpx.Response(status, json=body or {}, headers=headers, request=_REQ)


def _status_error(status: int, body=None) -> httpx.HTTPStatusError:
    return httpx.HTTPStatusError(str(status), request=_REQ, response=_resp(status, body))


def _held(access_token: str) -> TokenSet:
    return TokenSet(access_token=access_token, refresh_token="rt", expires_at=_NOW)


# --- Endpoints ------------------------------------------------------------------


@pytest.mark.parametrize(
    "url, ok",
    [
        ("https://auth.x.ai/oauth2/token", True),
        ("https://accounts.x.ai/oauth2/device?user_code=X", True),
        ("https://x.ai/", True),
        ("http://auth.x.ai/oauth2/token", False),
        ("https://auth.x.ai.evil.com/", False),
        ("https://evilx.ai/", False),
        ("https://cli-chat-proxy.grok.com/v1", False),
    ],
)
def test_only_https_on_x_ai_is_trusted(url, ok):
    assert is_xai_endpoint(url) is ok


# --- Device poll ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "status, code, expired_locally, expected",
    [
        (400, "authorization_pending", False, "pending"),
        (400, "slow_down", False, "pending"),
        (400, "access_denied", False, "denied"),
        (400, "authorization_denied", False, "denied"),
        (400, "expired_token", False, "expired"),
        # a spent code past its lifetime
        (400, "invalid_grant", True, "expired"),
    ],
)
def test_device_poll_statuses(status, code, expired_locally, expected):
    assert classify_device_poll(status, code, expired_locally=expired_locally).status == expected


def test_device_poll_unknown_code_is_terminal_before_expiry():
    with pytest.raises(DevicePollFailed) as raised:
        classify_device_poll(400, "invalid_grant", expired_locally=False)
    assert raised.value.code == "invalid_grant"


@pytest.mark.parametrize("status", [429, 500, 503])
def test_device_poll_outage_is_retryable(status):
    with pytest.raises(DeviceUpstreamUnavailable):
        classify_device_poll(status, None, expired_locally=False)


# --- Token lifetime ---------------------------------------------------------------------


def test_expiry_prefers_expires_in_then_the_access_tokens_exp_then_an_hour():
    exp = int((_NOW + timedelta(hours=6)).timestamp())
    at = _jwt(exp=exp)
    assert access_token_expiry({"access_token": at, "expires_in": 900}, _NOW) == _NOW + timedelta(seconds=900)
    assert access_token_expiry({"access_token": at}, _NOW) == datetime.fromtimestamp(exp, tz=timezone.utc)
    assert access_token_expiry({"access_token": "opaque"}, _NOW) == _NOW + timedelta(hours=1)


def test_expiry_ignores_the_id_tokens_exp():
    later = int((_NOW + timedelta(days=30)).timestamp())
    data = {"access_token": "opaque", "id_token": _jwt(exp=later)}
    assert access_token_expiry(data, _NOW) == _NOW + timedelta(hours=1)


@pytest.mark.parametrize(
    "lifetime, expected",
    [
        (timedelta(hours=6), timedelta(hours=1)),
        (timedelta(hours=4), timedelta(hours=1)),
        # An hour's skew would refresh these on every use.
        (timedelta(hours=2), timedelta(minutes=30)),
        (timedelta(minutes=60), timedelta(minutes=15)),
        (timedelta(minutes=46), timedelta(minutes=11, seconds=30)),
        (timedelta(minutes=45), timedelta(minutes=2)),
        (timedelta(minutes=15), timedelta(minutes=2)),
    ],
)
def test_refresh_buffer_follows_the_tokens_issued_lifetime(lifetime, expected):
    iat = int(_NOW.timestamp())
    tokens = _held(_jwt(iat=iat, exp=iat + int(lifetime.total_seconds())))
    assert refresh_buffer(tokens) == expected


def test_refresh_buffer_without_claims_is_the_shared_one():
    assert refresh_buffer(_held("opaque")) == REFRESH_BUFFER


# --- Token answers ---------------------------------------------------------------------


def test_identity_comes_from_the_id_token_and_no_plan_is_guessed():
    tokens = token_set({
        "access_token": "at", "refresh_token": "rt", "expires_in": 3600,
        "id_token": _jwt(sub="user-1", email="a@example.com"),
    })
    assert tokens.account_id == "user-1"
    assert tokens.email == "a@example.com"
    assert tokens.plan_type is None


def test_refresh_answer_without_a_refresh_token_keeps_the_held_one():
    tokens = token_set({"access_token": "at-2", "expires_in": 900}, previous_refresh_token="rt-1")
    assert tokens.refresh_token == "rt-1"


def test_rotated_refresh_token_replaces_the_held_one():
    tokens = token_set({"access_token": "at-2", "refresh_token": "rt-2"}, previous_refresh_token="rt-1")
    assert tokens.refresh_token == "rt-2"


def test_answer_without_any_refresh_token_is_refused():
    with pytest.raises(ValueError):
        token_set({"access_token": "at"})


# --- Refresh refusals -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_403_is_the_plan_not_the_sign_in():
    with patch.object(grok_oauth, "_post_form", AsyncMock(return_value=_resp(403, {"error": "forbidden"}))):
        with pytest.raises(OAuthPlanNoApiAccess) as raised:
            await grok_oauth.refresh_tokens("rt")
    assert raised.value.provider == "grok-oauth"


@pytest.mark.asyncio
async def test_refresh_403_from_a_cloudflare_challenge_is_transient():
    resp = _resp(403, headers={"cf-mitigated": "challenge"})
    with patch.object(grok_oauth, "_post_form", AsyncMock(return_value=resp)):
        with pytest.raises(httpx.HTTPStatusError) as raised:
            await grok_oauth.refresh_tokens("rt")
    assert permanent_grok_refresh_failure(raised.value) is None


@pytest.mark.parametrize(
    "exc, expected",
    [
        # what auth.x.ai answers an unknown refresh token
        (_status_error(400, {"error": "invalid_grant"}), "invalid_grant"),
        (_status_error(400, {"error": "invalid_request"}), "invalid_request"),
        (_status_error(400, {}), "bad_request"),
        (_status_error(401, {}), "unauthorized"),
        # our client registration, not the user's token
        (_status_error(400, {"error": "invalid_client"}), None),
        (_status_error(401, {"error": "invalid_client"}), None),
        (_status_error(503, {}), None),
        (httpx.ConnectError("down", request=_REQ), None),
    ],
)
def test_refresh_400_and_401_need_a_new_sign_in(exc, expected):
    assert permanent_grok_refresh_failure(exc) == expected


@pytest.mark.asyncio
async def test_no_api_access_flags_nothing():
    """The connection stays as it is: a new sign-in would get the same plan."""
    cache = MagicMock()
    cache.acquire_lock = AsyncMock(return_value=True)
    cache.release_lock = AsyncMock()
    row = {
        "access_token": "at", "refresh_token": "rt", "account_id": "user-1",
        "plan_type": None, "expires_at": _NOW - timedelta(minutes=1),
        "needs_reauth_at": None, "last_error_code": None,
    }
    mark = AsyncMock()
    grok = replace(
        SUBSCRIPTIONS["grok-oauth"], refresh=AsyncMock(side_effect=OAuthPlanNoApiAccess("grok-oauth"))
    )
    with (
        patch.object(oauth_health, "get_oauth_tokens", AsyncMock(return_value=row)),
        patch.object(oauth_health, "mark_oauth_needs_reauth", mark),
        patch.object(oauth_health, "SUBSCRIPTIONS", {"grok-oauth": grok}),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        with pytest.raises(OAuthPlanNoApiAccess):
            await valid_token("u1", "grok-oauth")
    mark.assert_not_awaited()
    cache.release_lock.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_api_access_refuses_the_turn_without_a_reconnect():
    from fastapi import HTTPException

    from src.server.services.llm.clients import resolve_oauth_llm_client
    from src.server.services.llm.thread_model import _runs_default_instead

    with patch.object(
        oauth_health, "valid_token", AsyncMock(side_effect=OAuthPlanNoApiAccess("grok-oauth")),
    ):
        with pytest.raises(HTTPException) as raised:
            await resolve_oauth_llm_client("u1", "grok-4.7-oauth")
    exc = raised.value
    assert exc.status_code == 400
    assert exc.detail["type"] == "oauth_plan_no_api_access"
    assert exc.detail["provider"] == "grok-oauth"
    assert "reason" not in exc.detail
    assert _runs_default_instead(exc, named=None, model="grok-4.7-oauth")


# --- A model call's refusal ------------------------------------------------------------


@pytest.mark.parametrize(
    "status, body, stale",
    [
        (401, "anything", True),
        (403, "Error code: 403 - {'error': '[WKE=unauthenticated:bad-credentials] ...'}", True),
        (403, "The OAuth2 access token could not be validated", True),
        (403, "You do not have an active Grok subscription", False),
        (403, "personal-team-blocked:spending-limit", False),
        (429, "unauthenticated", False),
    ],
)
def test_a_403_is_the_plan_unless_it_refuses_the_token(status, body, stale):
    assert stale_token_refusal(status, body) is stale


def _refused(status: int, error: str, model="grok-4.7-oauth"):
    exc = RuntimeError("model call failed")
    exc.__model_resilience__ = {
        "model": model,
        "attempted_models": [{"model": model, "status_code": status, "error": error}],
    }
    return exc


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error, expected",
    [("[WKE=unauthenticated:bad-credentials]", True), ("no active Grok subscription", False)],
)
async def test_only_a_stale_token_403_expires_the_access_token(error, expected):
    expire = AsyncMock(return_value=True)
    with patch.object(oauth_health, "expire_oauth_access_token", expire):
        calls = oauth_health.failed_model_calls(_refused(403, error).__model_resilience__, None)
        await oauth_health.note_token_refusals("u1", calls)
    if expected:
        expire.assert_awaited_once_with("u1", "grok-oauth")
    else:
        expire.assert_not_awaited()


@pytest.mark.parametrize(
    "error, hints",
    [
        ("[WKE=unauthenticated:bad-credentials]", ["reconnect_account", "try_another_model"]),
        ("no active Grok subscription", ["model_access", "try_another_model"]),
    ],
)
def test_error_frame_hints_for_a_grok_403(error, hints):
    from src.server.services.runs.stream_errors import error_event_data

    exc = _refused(403, error)
    with patch(
        "src.server.services.runs.stream_errors.failure_kind",
        return_value={"kind": "upstream", "status_code": 403, "provider_module": None},
    ):
        data = error_event_data("t1", "boom", exc=exc, credential_source="oauth")
    assert data["hints"] == hints
