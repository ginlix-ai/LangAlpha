"""The subscription-connection health contract: which refresh failures flag a
connection for sign-in, the states a stored connection moves through, the
status a client reads, and the refusal a turn on a flagged one gets.

The error bodies are the ones the token endpoints returned to live probes
(2026-10-24) with a bogus refresh token.
"""

from dataclasses import replace
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.server.models.oauth import OAuthStatus
from src.server.services import oauth_health
from src.server.services.oauth_health import (
    OAuthReauthRequired,
    OAuthRefreshUnavailable,
    valid_token,
)
from src.server.services.oauth_protocol import TokenSet, permanent_refresh_failure
from src.server.services.oauth_providers import SUBSCRIPTIONS

_REQ = httpx.Request("POST", "https://auth.example/oauth/token")


def _status_error(status: int, body) -> httpx.HTTPStatusError:
    resp = httpx.Response(status, json=body, request=_REQ)
    return httpx.HTTPStatusError(f"{status}", request=_REQ, response=resp)


def _openai(status: int, code: str) -> httpx.HTTPStatusError:
    return _status_error(status, {"error": {
        "message": "provider text", "type": "invalid_request_error",
        "param": None, "code": code,
    }})


# --- Classification ---------------------------------------------------------


@pytest.mark.parametrize(
    "exc, expected",
    [
        # auth.openai.com, unknown refresh token
        (_openai(401, "invalid_refresh_token"), "invalid_refresh_token"),
        # platform.claude.com, unknown refresh token
        (_status_error(400, {"error": "invalid_grant",
                             "error_description": "Refresh token not found or invalid"}),
         "invalid_grant"),
        # the codes the official Codex CLI treats as permanent
        (_openai(401, "refresh_token_reused"), "refresh_token_reused"),
        (_openai(400, "refresh_token_expired"), "refresh_token_expired"),
        (_openai(400, "refresh_token_invalidated"), "refresh_token_invalidated"),
        # a 401 with no readable code is still the token
        (_status_error(401, {"unexpected": True}), "unauthorized"),
    ],
)
def test_dead_refresh_tokens_are_permanent(exc, expected):
    assert permanent_refresh_failure(exc) == expected


@pytest.mark.parametrize(
    "exc",
    [
        # our client registration, not the user's token
        _openai(401, "invalid_client"),
        # platform.claude.com's rate limit answer
        _status_error(429, {"error": {"type": "rate_limit_error",
                                      "message": "Rate limited. Please try again later."}}),
        _status_error(503, {"error": "temporarily_unavailable"}),
        _openai(400, "empty_string"),
        httpx.ConnectError("boom", request=_REQ),
        httpx.ReadTimeout("slow", request=_REQ),
        KeyError("access_token"),
    ],
)
def test_everything_else_is_transient(exc):
    assert permanent_refresh_failure(exc) is None


def test_only_a_safe_code_is_read_from_a_body():
    exc = _status_error(401, {"error": {"code": "<script>alert(1)</script>"}})
    assert permanent_refresh_failure(exc) == "unauthorized"


# --- valid_token state transitions -------------------------------------------


def _row(*, expires_in: timedelta, flagged: bool = False, refresh_token="rt-1"):
    return {
        "access_token": "at-1",
        "refresh_token": refresh_token,
        "account_id": "acct",
        "email": None,
        "plan_type": "plus",
        "expires_at": datetime.now(timezone.utc) + expires_in,
        "needs_reauth_at": datetime.now(timezone.utc) if flagged else None,
        "last_error_code": "invalid_grant" if flagged else None,
    }


@pytest.fixture
def store():
    """The DB and Redis surface ``valid_token`` touches, mocked."""
    cache = MagicMock()
    cache.acquire_lock = AsyncMock(return_value=True)
    cache.release_lock = AsyncMock()
    mocks = {
        "get": AsyncMock(),
        "mark": AsyncMock(return_value=True),
        "upsert": AsyncMock(),
        "replace": AsyncMock(return_value=True),
        "wait": AsyncMock(),
        "cache": cache,
    }
    with (
        patch.object(oauth_health, "get_oauth_tokens", mocks["get"]),
        patch.object(oauth_health, "mark_oauth_needs_reauth", mocks["mark"]),
        patch.object(oauth_health, "upsert_oauth_tokens", mocks["upsert"]),
        patch.object(oauth_health, "replace_refreshed_tokens", mocks["replace"]),
        patch.object(oauth_health, "await_refreshed_tokens", mocks["wait"]),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        yield mocks


async def _call(refresh):
    codex = replace(SUBSCRIPTIONS["codex-oauth"], refresh=refresh)
    with patch.object(oauth_health, "SUBSCRIPTIONS", {"codex-oauth": codex}):
        return await valid_token("u1", "codex-oauth")


@pytest.mark.asyncio
async def test_no_connection_is_none(store):
    store["get"].return_value = None
    assert await _call(AsyncMock()) is None


@pytest.mark.asyncio
async def test_flagged_connection_refuses_without_calling_the_provider(store):
    store["get"].return_value = _row(expires_in=timedelta(hours=1), flagged=True)
    refresh = AsyncMock()
    with pytest.raises(OAuthReauthRequired) as raised:
        await _call(refresh)
    assert raised.value.code == "invalid_grant"
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_token_is_used_as_is(store):
    store["get"].return_value = _row(expires_in=timedelta(hours=1))
    refresh = AsyncMock()
    assert (await _call(refresh)).access_token == "at-1"
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_permanent_refresh_failure_flags_the_row_it_read(store):
    store["get"].return_value = _row(expires_in=timedelta(minutes=-1))
    refresh = AsyncMock(side_effect=_openai(401, "refresh_token_reused"))
    with pytest.raises(OAuthReauthRequired) as raised:
        await _call(refresh)
    assert raised.value.code == "refresh_token_reused"
    store["mark"].assert_awaited_once_with(
        "u1", "codex-oauth", refresh_token="rt-1", error_code="refresh_token_reused"
    )
    store["cache"].release_lock.assert_awaited_once()


@pytest.mark.asyncio
async def test_rejection_of_a_token_already_replaced_does_not_flag(store):
    """Another request rotated the single-use token first: the CAS misses and
    the working connection that replaced it is used."""
    store["get"].side_effect = [
        _row(expires_in=timedelta(minutes=-1)),
        _row(expires_in=timedelta(minutes=-1)),
        {**_row(expires_in=timedelta(hours=1), refresh_token="rt-2"), "access_token": "at-2"},
    ]
    store["mark"].return_value = False
    refresh = AsyncMock(side_effect=_openai(401, "refresh_token_reused"))
    assert (await _call(refresh)).access_token == "at-2"


@pytest.mark.asyncio
async def test_a_refresh_that_landed_before_the_lock_is_used_not_repeated(store):
    """Read, then another request refreshed and let the lock go: the refresh
    token read first is spent, so the row is read again under the lock."""
    store["get"].side_effect = [
        _row(expires_in=timedelta(minutes=-1)),
        {**_row(expires_in=timedelta(hours=1), refresh_token="rt-2"), "access_token": "at-2"},
    ]
    refresh = AsyncMock()
    assert (await _call(refresh)).access_token == "at-2"
    refresh.assert_not_awaited()
    store["cache"].release_lock.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_refresh_still_due_under_the_lock_spends_the_token_read_there(store):
    store["get"].side_effect = [
        _row(expires_in=timedelta(minutes=-1)),
        _row(expires_in=timedelta(minutes=-1), refresh_token="rt-2"),
    ]
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    refresh = AsyncMock(return_value=TokenSet("at-3", "rt-3", expires))
    await _call(refresh)
    refresh.assert_awaited_once_with("rt-2")


@pytest.mark.asyncio
async def test_transient_failure_on_an_expired_token_flags_nothing(store):
    store["get"].return_value = _row(expires_in=timedelta(minutes=-1))
    refresh = AsyncMock(side_effect=httpx.ConnectError("down", request=_REQ))
    with pytest.raises(OAuthRefreshUnavailable):
        await _call(refresh)
    store["mark"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refresh_that_stalls_ends_inside_the_lease_as_transient(store, monkeypatch):
    # httpx bounds each phase only; past the lock's lease a second worker
    # could spend the same single-use refresh token.
    monkeypatch.setattr(oauth_health, "REFRESH_DEADLINE_S", 0.05)
    store["get"].return_value = _row(expires_in=timedelta(minutes=-1))

    async def trickle(_token):
        await asyncio.sleep(5)

    with pytest.raises(OAuthRefreshUnavailable):
        await _call(AsyncMock(side_effect=trickle))
    store["mark"].assert_not_awaited()


@pytest.mark.asyncio
async def test_transient_failure_keeps_using_a_token_that_still_works(store):
    """Inside the refresh buffer the stored token still has minutes in it."""
    store["get"].return_value = _row(expires_in=timedelta(minutes=2))
    refresh = AsyncMock(side_effect=_status_error(503, {"error": "server_error"}))
    assert (await _call(refresh)).access_token == "at-1"
    store["mark"].assert_not_awaited()


@pytest.mark.parametrize(("expires_in", "refreshed"), [(timedelta(minutes=2), False), (timedelta(minutes=-1), True)])
@pytest.mark.asyncio
async def test_without_a_lock_the_held_token_serves_until_it_expires(store, expires_in, refreshed):
    # Redis can't say whether another worker is spending the single-use
    # refresh token, so none is spent while the held token still works.
    store["cache"].acquire_lock.return_value = None
    store["get"].return_value = _row(expires_in=expires_in)
    refresh = AsyncMock(side_effect=httpx.ConnectError("down", request=_REQ))
    if refreshed:
        with pytest.raises(OAuthRefreshUnavailable):
            await _call(refresh)
    else:
        assert (await _call(refresh)).access_token == "at-1"
    assert refresh.await_count == int(refreshed)


@pytest.mark.asyncio
async def test_refresh_keeps_the_stored_identity_its_answer_leaves_out(store):
    store["get"].return_value = _row(expires_in=timedelta(minutes=-1))
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    refresh = AsyncMock(return_value=TokenSet("at-2", "rt-2", expires, email="a@b.c"))
    tokens = await _call(refresh)
    assert (tokens.account_id, tokens.email, tokens.plan_type) == ("acct", "a@b.c", "plus")
    refresh.assert_awaited_once_with("rt-1")
    store["replace"].assert_awaited_once_with(
        "u1", "codex-oauth", spent_refresh_token="rt-1", access_token="at-2", refresh_token="rt-2",
        account_id="acct", email="a@b.c", plan_type="plus", expires_at=expires,
    )


@pytest.mark.asyncio
async def test_a_refresh_that_could_not_be_saved_still_serves_the_turn(store, monkeypatch):
    """The provider spent the old refresh token, so the old access token is no
    fallback and the failed save is not a failed refresh."""
    monkeypatch.setattr(oauth_health, "_SAVE_RETRY_DELAYS_S", (0, 0))
    store["get"].return_value = _row(expires_in=timedelta(minutes=2))
    store["replace"].side_effect = RuntimeError("db down")
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    refresh = AsyncMock(return_value=TokenSet("at-2", "rt-2", expires))
    assert (await _call(refresh)).access_token == "at-2"
    assert store["replace"].await_count == 3
    store["mark"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_save_that_fails_once_is_retried_so_the_rotated_token_is_kept(store, monkeypatch):
    # Left unsaved, the stored refresh token is the spent one, which the next
    # refresh would present and be refused for good.
    assert oauth_health.REFRESH_DEADLINE_S + oauth_health.REFRESH_SAVE_DEADLINE_S < (
        oauth_health.REFRESH_LOCK_TTL_MS / 1000
    )
    monkeypatch.setattr(oauth_health, "_SAVE_RETRY_DELAYS_S", (0, 0))
    store["get"].return_value = _row(expires_in=timedelta(minutes=2))
    store["replace"].side_effect = [RuntimeError("db blip"), True]
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    assert (await _call(AsyncMock(return_value=TokenSet("at-2", "rt-2", expires)))).access_token == "at-2"
    assert store["replace"].await_count == 2
    assert store["replace"].await_args.kwargs["refresh_token"] == "rt-2"


@pytest.mark.asyncio
async def test_a_disconnect_during_the_refresh_stays_disconnected(store):
    store["get"].side_effect = [_row(expires_in=timedelta(minutes=-1)), None]
    store["replace"].return_value = False
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    assert await _call(AsyncMock(return_value=TokenSet("at-2", "rt-2", expires))) is None
    store["upsert"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_reconnect_during_the_refresh_keeps_the_new_account(store):
    store["get"].side_effect = [
        _row(expires_in=timedelta(minutes=-1)),
        {**_row(expires_in=timedelta(hours=1), refresh_token="rt-new"), "access_token": "at-new"},
    ]
    store["replace"].return_value = False
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    assert (await _call(AsyncMock(return_value=TokenSet("at-2", "rt-2", expires)))).access_token == "at-new"


@pytest.mark.asyncio
async def test_waiter_reports_the_flag_the_lock_holder_set(store):
    store["get"].return_value = _row(expires_in=timedelta(minutes=-1))
    store["cache"].acquire_lock.return_value = False
    store["wait"].return_value = _row(expires_in=timedelta(minutes=-1), flagged=True)
    with pytest.raises(OAuthReauthRequired):
        await _call(AsyncMock())


@pytest.mark.asyncio
async def test_waiter_that_times_out_is_transient(store):
    store["get"].return_value = _row(expires_in=timedelta(minutes=-1))
    store["cache"].acquire_lock.return_value = False
    store["wait"].return_value = None
    with pytest.raises(OAuthRefreshUnavailable) as raised:
        await _call(AsyncMock())
    assert raised.value.reason == "refresh_in_progress"
    store["mark"].assert_not_awaited()


# --- Status payload ------------------------------------------------------------


def _status(row):
    return OAuthStatus.from_row(row).model_dump(mode="json")


def test_status_disconnected():
    assert _status(None) == {
        "connected": False,
        "state": "disconnected",
        "account_id": None,
        "email": None,
        "plan_type": None,
        "expires_at": None,
        "needs_reauth_at": None,
        "last_error_code": None,
    }


def test_status_connected_and_needs_reauth():
    expires = datetime(2026, 10, 25, tzinfo=timezone.utc)
    flagged_at = datetime(2026, 10, 24, tzinfo=timezone.utc)
    row = {
        "account_id": "acct", "email": "a@b.c", "plan_type": "plus",
        "expires_at": expires, "needs_reauth_at": None, "last_error_code": "stale",
    }
    ok = _status(row)
    assert ok["connected"] is True and ok["state"] == "connected"
    assert ok["expires_at"] == expires.isoformat()
    assert ok["last_error_code"] is None  # only meaningful while flagged

    bad = _status(
        {**row, "needs_reauth_at": flagged_at, "last_error_code": "invalid_grant"}
    )
    assert bad["connected"] is True  # a connection is still stored
    assert bad["state"] == "needs_reauth"
    assert bad["needs_reauth_at"] == flagged_at.isoformat()
    assert bad["last_error_code"] == "invalid_grant"


# --- The turn's refusal --------------------------------------------------------


def _model_config(provider="codex-oauth"):
    mc = MagicMock()
    mc.get_model_config.return_value = {"provider": provider}
    mc.get_provider_info.return_value = {"access_type": "oauth"}
    return mc


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["codex-oauth", "claude-oauth"])
async def test_flagged_connection_refuses_the_turn_with_a_reconnect(provider):
    from fastapi import HTTPException

    from src.server.services.llm.clients import resolve_oauth_llm_client
    from src.server.services.llm.thread_model import _runs_default_instead

    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_model_config(provider)),
        patch.object(
            oauth_health, "valid_token",
            AsyncMock(side_effect=OAuthReauthRequired(provider, "invalid_grant")),
        ),
    ):
        with pytest.raises(HTTPException) as raised:
            await resolve_oauth_llm_client("u1", "some-model")

    exc = raised.value
    assert exc.status_code == 400
    assert exc.detail["type"] == "oauth_reauth_required"
    assert exc.detail["provider"] == provider
    assert exc.detail["reason"] == "invalid_grant"
    assert exc.detail["link"]["url"]
    # A turn nobody is watching runs the account default instead.
    assert _runs_default_instead(exc, named=None, model="some-model")


@pytest.mark.asyncio
async def test_unrenewable_token_is_a_retryable_refusal():
    from fastapi import HTTPException

    from src.server.services.llm.clients import resolve_oauth_llm_client

    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_model_config()),
        patch.object(
            oauth_health, "valid_token",
            AsyncMock(side_effect=OAuthRefreshUnavailable("codex-oauth", "refresh_failed")),
        ),
    ):
        with pytest.raises(HTTPException) as raised:
            await resolve_oauth_llm_client("u1", "some-model")
    assert raised.value.status_code == 503
    assert raised.value.detail["type"] == "oauth_refresh_unavailable"


# --- A subscription token refused mid-turn ----------------------------------


def _trace(status: int, model="some-model"):
    return {"model": model, "attempted_models": [{"model": model, "status_code": status}]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider, status, expected",
    [("codex-oauth", 401, True), ("codex-oauth", 403, False), ("codex-oauth", 500, False), ("openai", 401, False)],
)
async def test_a_401_on_a_subscription_expires_the_access_token(provider, status, expected):
    expire = AsyncMock(return_value=True)
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_model_config(provider)),
        patch.object(oauth_health, "expire_oauth_access_token", expire),
    ):
        calls = oauth_health.failed_model_calls(_trace(status), None)
        await oauth_health.note_token_refusals("u1", calls)
    if expected:
        expire.assert_awaited_once_with("u1", "codex-oauth")
    else:
        expire.assert_not_awaited()


# --- A refusal a fallback answered --------------------------------------------


def _fallback(status: int, *, from_is_primary=True, model="some-model"):
    return {"type": "ui", "name": "model_fallback", "props": {
        "from_model": model, "to_model": "backup", "from_is_primary": from_is_primary,
        "error": "Unauthorized", "status_code": status,
    }}


def _handler(fallbacks, *, client_meta=None):
    from src.server.database.runs.lifecycle import ProducerRecord

    handler = MagicMock()
    handler.agent_config.credential_source = "oauth"
    handler.agent_config.llm_client.metadata = client_meta or {}
    handler.record = MagicMock(return_value=ProducerRecord(model_fallbacks=fallbacks))
    return handler


@pytest.mark.asyncio
async def test_a_refusal_a_fallback_answered_still_expires_the_access_token():
    from src.server.services.runs.executor import LocalRunExecutor

    expire = AsyncMock(return_value=True)
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_model_config()),
        patch.object(oauth_health, "expire_oauth_access_token", expire),
    ):
        await LocalRunExecutor._note_credential_refusal(
            _handler([_fallback(500), _fallback(401)]), "u1", None,
        )
    expire.assert_awaited_once_with("u1", "codex-oauth")


@pytest.mark.asyncio
async def test_a_refusal_that_is_not_the_tokens_is_not_noted():
    from src.server.services.runs.executor import LocalRunExecutor

    expire = AsyncMock(return_value=True)
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_model_config()),
        patch.object(oauth_health, "expire_oauth_access_token", expire),
    ):
        await LocalRunExecutor._note_credential_refusal(_handler([_fallback(500)]), "u1", None)
    expire.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("answered", [True, False])
async def test_a_subscription_fallback_behind_a_key_has_its_refused_token_expired(answered):
    # The primary ran on the user's key; the subscription later in the list is
    # held on its own token, which a refusal there must expire as well.
    from src.server.services.runs.executor import LocalRunExecutor

    config = MagicMock()
    config.get_model_config.side_effect = lambda m: {"provider": "codex-oauth" if m == "sub-model" else "openai"}
    handler = _handler([_fallback(429, model="key-model"), _fallback(401, from_is_primary=False, model="sub-model")]
                       if answered else [_fallback(429, model="key-model")])
    handler.agent_config.credential_source = "byok"
    exc = None
    if not answered:
        exc = RuntimeError("exhausted")
        exc.__model_resilience__ = {"model": "key-model", "attempted_models": [
            {"model": "key-model", "status_code": 429}, {"model": "sub-model", "status_code": 401},
        ]}
    expire = AsyncMock(return_value=True)
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=config),
        patch.object(oauth_health, "expire_oauth_access_token", expire),
    ):
        await LocalRunExecutor._note_credential_refusal(handler, "u1", exc)
    expire.assert_awaited_once_with("u1", "codex-oauth")


@pytest.mark.asyncio
async def test_a_custom_model_on_a_subscription_is_judged_by_its_clients_provider():
    from src.server.services.runs.executor import LocalRunExecutor

    unlisted = MagicMock()
    unlisted.get_model_config.return_value = None
    expire = AsyncMock(return_value=True)
    handler = _handler(
        [_fallback(401, model="listed-model")],
        client_meta={"manifest_model": "listed-model", "pricing_provider": "codex-oauth"},
    )
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=unlisted),
        patch.object(oauth_health, "expire_oauth_access_token", expire),
    ):
        await LocalRunExecutor._note_credential_refusal(handler, "u1", None)
    expire.assert_awaited_once_with("u1", "codex-oauth")
