"""The Claude Code routes: GET /status, whether the picker offers Claude Code
models and whether the user's saved token works; PUT and DELETE /token, saving
a token only once Anthropic serves it, and removing it."""

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from ptc_agent.core import claude_code_managed
from src.server.app import claude_code as route
from src.server.services import claude_oauth
from src.server.services.claude_code import checks, flow
from src.server.services.claude_code.saved import token_check_key
from src.server.services.oauth_signin import OAuthRefusal, PendingSignIn
from tests.conftest import create_test_app
from tests.unit.server.services.claude_code.fakes import Cache as _Cache, serving

FLOW = "src.server.services.claude_code.flow"
SAVED = "src.server.services.claude_code.saved"


@pytest_asyncio.fixture
async def client():
    app = create_test_app(route.router)
    app.add_exception_handler(
        OAuthRefusal, lambda _, exc: JSONResponse({"detail": exc.detail, "error": exc.error}, exc.status_code)
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@contextmanager
def _switches(*, runs=True, rows=True, opted_in=True):
    """The server's switches and the user's opt-in, as the flow and the store read them."""
    with (
        patch(f"{FLOW}.backend_available", return_value=runs and rows),
        patch(f"{SAVED}.backend_available", return_value=runs and rows),
        patch(f"{FLOW}.rows_available", return_value=rows),
        patch(f"{FLOW}.user_feature_enabled", new_callable=AsyncMock, return_value=opted_in),
    ):
        yield


async def _status(client, *, token="tok", answer=True, runs=True, rows=True, opted_in=True, cache=None, settle=True):
    """One read, and the check it starts run to its end unless ``settle`` is False."""
    check = AsyncMock(side_effect=answer) if isinstance(answer, Exception) else AsyncMock(return_value=answer)
    with (
        serving(cache if cache is not None else _Cache()),
        _switches(runs=runs, rows=rows, opted_in=opted_in),
        patch(f"{SAVED}.stored_row", new_callable=AsyncMock, return_value={"access_token": token} if token else None),
        patch(f"{SAVED}.get_oauth_connection", new_callable=AsyncMock, return_value={"email": None} if token else None),
        patch("ptc_agent.agent.claude_code.runtime.token_works", check),
    ):
        response = await client.get("/api/v1/claude-code/status")
        if settle:
            await asyncio.gather(*checks._checks)
    assert response.status_code == 200
    return response.json(), check


@pytest.mark.asyncio
@pytest.mark.parametrize(("rows", "opted_in"), [(False, True), (True, False)])
async def test_not_enabled_checks_nothing(client, rows, opted_in):
    body, check = await _status(client, rows=rows, opted_in=opted_in)
    assert body == {"state": "off", "email": None, "expires_at": None, "saved": True, "enabled": False}
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_where_only_own_computers_run_it_it_is_on_and_the_saved_token_off(client):
    body, check = await _status(client, runs=False)
    assert body == {"state": "off", "email": None, "expires_at": None, "saved": True, "enabled": True}
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_without_a_token_it_needs_one_and_nothing_runs(client):
    body, check = await _status(client, token=None)
    assert body == {"state": "no_token", "email": None, "expires_at": None, "saved": False, "enabled": True}
    check.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("answer", "state"), [(True, "signed_in"), (False, "rejected")])
async def test_a_read_never_waits_on_the_check_and_a_later_read_has_its_answer(client, answer, state):
    cache = _Cache()
    first, check = await _status(client, answer=answer, cache=cache)
    assert first == {"state": "checking", "email": None, "expires_at": None, "saved": True, "enabled": True}
    check.assert_awaited_once()
    later, again = await _status(client, answer=answer, cache=cache)
    assert later == {"state": state, "email": None, "expires_at": None, "saved": True, "enabled": True}
    again.assert_not_awaited()
    assert set(cache.ttls.values()) == {checks.TOKEN_CHECK_TTL_S} and not cache.locks
    assert set(cache.values.values()) == {answer}, "kept as a worker from before the words reads it"


@pytest.mark.asyncio
async def test_a_check_that_could_not_tell_reads_as_unverified_and_is_kept_briefly(client):
    cache = _Cache()
    await _status(client, answer=TimeoutError(), cache=cache)
    later, again = await _status(client, answer=TimeoutError(), cache=cache)
    assert later == {"state": "unverified", "email": None, "expires_at": None, "saved": True, "enabled": True}
    again.assert_not_awaited()
    assert set(cache.ttls.values()) == {checks.SHORT_TTL_S} and set(cache.values.values()) == {"unknown"}


@pytest.mark.asyncio
async def test_reads_during_a_check_start_no_other(client):
    cache = _Cache()
    _, first = await _status(client, cache=cache, settle=False)
    _, second = await _status(client, cache=cache, settle=False)
    await asyncio.gather(*checks._checks)
    assert first.await_count + second.await_count == 1


def _policy_installed(tmp_path, monkeypatch) -> None:
    """An organization's managed settings on the backend's own machine, as the CLI there would read them."""
    folder = tmp_path / "claude-code"
    folder.mkdir()
    (folder / "managed-settings.json").write_text("{}")
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_LINUX", str(folder))
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_MAC", str(folder))


@pytest.mark.asyncio
@pytest.mark.parametrize("token", ["tok", None])
async def test_where_a_policy_is_installed_on_the_backend_it_reads_so_and_checks_nothing(client, tmp_path, monkeypatch, token):
    _policy_installed(tmp_path, monkeypatch)
    body, check = await _status(client, token=token)
    assert body == {"state": "policy_blocked", "email": None, "expires_at": None, "saved": token is not None, "enabled": True}
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_without_redis_a_read_spends_nothing_on_the_plan(client):
    body, check = await _status(client, answer=True, cache=_Cache(redis=False))
    assert body == {"state": "unverified", "email": None, "expires_at": None, "saved": True, "enabled": True}
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_new_token_is_checked_at_once_and_no_key_holds_a_token(client):
    cache = _Cache()
    await _status(client, token="tok-1", cache=cache)
    body, check = await _status(client, token="tok-2", answer=False, cache=cache)
    assert body == {"state": "checking", "email": None, "expires_at": None, "saved": True, "enabled": True}
    check.assert_awaited_once()
    assert not any(token in key for key in cache.values for token in ("tok-1", "tok-2"))


class _Store:
    """The token store's one row for the test user, and the writes made to it."""

    def __init__(self, token=None):
        self.token, self.writes, self.email, self.expires_at = token, [], None, None

    async def read(self, user_id):
        return self.token

    async def row(self, user_id):
        return {"access_token": self.token, "email": self.email, "expires_at": self.expires_at} if self.token else None

    @asynccontextmanager
    async def change(self, user_id, provider):
        self.writes.append(("lock", provider))
        yield "conn"

    async def upsert(self, user_id, provider, *, access_token, refresh_token, account_id, expires_at, conn, email=None):
        assert conn == "conn" and refresh_token == ""
        self.writes.append(("upsert", provider))
        self.token, self.account_id, self.email, self.expires_at = access_token, account_id, email, expires_at

    async def delete(self, user_id, provider, conn):
        assert conn == "conn"
        self.writes.append(("delete", provider))
        self.token = None


async def _token_call(
    client, method, *, body=None, store=None, answer=True, opted_in=True, cache=None,
    path="/api/v1/claude-code/token", mint=None,
):
    check = AsyncMock(side_effect=answer) if isinstance(answer, Exception) else AsyncMock(return_value=answer)
    store, cache = store or _Store(), cache if cache is not None else _Cache()
    with (
        serving(cache),
        patch.object(claude_oauth, "mint_setup_token", mint or AsyncMock()),
        _switches(opted_in=opted_in),
        patch(f"{SAVED}.stored_token", new=store.read),
        patch(f"{SAVED}.stored_row", new=store.row),
        patch(f"{SAVED}.connection_change", new=store.change),
        patch(f"{SAVED}.upsert_oauth_tokens", new=store.upsert),
        patch(f"{SAVED}.delete_oauth_tokens", new=store.delete),
        patch("ptc_agent.agent.claude_code.runtime.token_works", check),
    ):
        response = await client.request(method, path, json=body)
    return response, check, store


@pytest.mark.asyncio
async def test_a_token_anthropic_serves_is_saved_over_the_last_and_the_last_one_s_answer_dropped(client):
    cache, store = _Cache(), _Store("tok-old")
    cache.values[token_check_key("tok-old")] = "works"
    response, check, _ = await _token_call(client, "PUT", body={"token": "  tok-new\n"}, store=store, cache=cache)
    assert response.status_code == 200
    body = response.json()
    saved_until = datetime.fromisoformat(body.pop("expires_at"))
    assert body == {"state": "signed_in", "email": None, "saved": True, "enabled": True}
    assert abs(saved_until - datetime.now(timezone.utc) - flow.TOKEN_LIFETIME).total_seconds() < 60
    assert check.await_args.args[0].token == "tok-new"
    assert store.token == "tok-new" and store.writes == [("lock", "claude-code"), ("upsert", "claude-code")]
    assert cache.values == {token_check_key("tok-new"): True}
    assert "tok-new" not in response.text


class _LapsedCache(_Cache):
    """A hold another change took over once this one's lapsed mid-check."""

    async def acquire_lock(self, key, token, ttl_ms):
        self.locks[key] = "another-change"
        return True


@pytest.mark.asyncio
async def test_a_save_whose_hold_lapsed_writes_nothing(client):
    response, _, store = await _token_call(client, "PUT", body={"token": "tok"}, cache=_LapsedCache())
    assert response.status_code == 409 and response.json()["error"]["code"] == "check_in_progress"
    assert store.token is None and ("upsert", "claude-code") not in store.writes


@pytest.mark.asyncio
async def test_a_token_the_deployment_can_no_longer_read_is_still_removed(client):
    store = _Store("tok")

    async def unreadable(user_id):
        raise RuntimeError("BYOK_ENCRYPTION_KEY environment variable is not set.")

    store.read = unreadable
    response, _, _ = await _token_call(client, "DELETE", store=store)
    assert response.status_code == 204 and store.token is None


@pytest.mark.asyncio
async def test_the_same_token_saved_again_keeps_when_it_lapses_and_its_account(client):
    store = _Store("tok")
    store.email, store.expires_at = "user@example.com", datetime(2027, 1, 2, tzinfo=timezone.utc)
    response, _, _ = await _token_call(client, "PUT", body={"token": "tok"}, store=store)
    assert response.status_code == 200
    assert response.json() == {"state": "signed_in", "email": "user@example.com", "expires_at": "2027-01-02T00:00:00Z", "saved": True, "enabled": True}
    assert store.expires_at == datetime(2027, 1, 2, tzinfo=timezone.utc)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "answer", "status", "code"),
    [
        ({"token": "   "}, True, 400, "token_missing"),
        ({"token": "tok bad"}, True, 400, "token_rejected"),
        ({"token": "tok"}, False, 400, "token_rejected"),
        ({"token": "tok"}, TimeoutError(), 503, "check_unavailable"),
    ],
)
async def test_a_token_not_known_to_work_is_never_saved(client, body, answer, status, code):
    response, _, store = await _token_call(client, "PUT", body=body, answer=answer)
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert store.writes == [] and store.token is None


@pytest.mark.asyncio
async def test_one_check_runs_per_user_at_a_time(client):
    cache = _Cache()
    cache.locks["claude_code:token_save:test-user-123"] = "other"
    response, check, store = await _token_call(client, "PUT", body={"token": "tok"}, cache=cache)
    assert response.status_code == 409 and response.json()["error"]["code"] == "check_in_progress"
    check.assert_not_awaited()
    assert store.writes == []


@pytest.mark.asyncio
async def test_saving_needs_claude_code_on_for_the_user(client):
    response, check, store = await _token_call(client, "PUT", body={"token": "tok"}, opted_in=False)
    assert response.status_code == 404 and response.json()["error"]["code"] == "unavailable"
    check.assert_not_awaited()
    assert store.writes == []


@pytest.mark.asyncio
async def test_removing_deletes_the_token_and_its_answer(client):
    cache, store = _Cache(), _Store("tok")
    cache.values[token_check_key("tok")] = "works"
    response, _, _ = await _token_call(client, "DELETE", store=store, cache=cache, opted_in=False)
    assert response.status_code == 204
    assert store.token is None and store.writes == [("lock", "claude-code"), ("delete", "claude-code")]
    assert cache.values == {}


@pytest.mark.asyncio
async def test_a_removals_hold_covers_one_removal_of_the_saved_token(client):
    cache = _Cache()
    response, _, _ = await _token_call(client, "DELETE", store=_Store("tok"), cache=cache)
    assert response.status_code == 204
    assert cache.lock_ttls["claude_code:token_save:test-user-123"] == (flow.MACHINE_IO_S + checks.MARGIN_S) * 1000


SIGN_IN = "/api/v1/claude-code/sign-in"
FLOW_KEY = "claude_code:sign_in:test-user-123"
MINTED = claude_oauth.SetupToken(
    access_token="sk-ant-oat01-minted", expires_at=datetime(2027, 10, 29, tzinfo=timezone.utc),
    token_uuid="tok-uuid", email="user@example.com",
)


def _flow_cache():
    cache = _Cache()
    cache.values[FLOW_KEY] = json.dumps({"verifier": "verifier", "state": "state-1"})
    return cache


def _refused(status):
    return httpx.HTTPStatusError("refused", request=MagicMock(), response=httpx.Response(status))


@pytest.mark.asyncio
async def test_a_sign_in_keeps_its_verifier_and_state_and_shows_neither_together():
    cache = _Cache()
    with serving(cache), _switches():
        response = await route.start_sign_in("test-user-123")
    flow = json.loads(cache.values[FLOW_KEY])
    assert flow["state"] in response["authorize_url"] and flow["verifier"] not in response["authorize_url"]
    assert response["authorize_url"].startswith(claude_oauth.SETUP_TOKEN_AUTHORIZE_URL)


@pytest.mark.asyncio
async def test_a_signed_in_token_is_checked_then_saved_with_its_account_and_the_sign_in_ended(client):
    cache, store = _flow_cache(), _Store()
    mint = AsyncMock(return_value=MINTED)
    response, check, _ = await _token_call(
        client, "POST", path=f"{SIGN_IN}/code", body={"code": " code#state-1 "}, cache=cache, store=store, mint=mint,
    )
    assert response.status_code == 200
    assert response.json() == {"state": "signed_in", "email": "user@example.com", "expires_at": "2027-10-29T00:00:00Z", "saved": True, "enabled": True}
    assert mint.await_args.args == ("code", "state-1", "verifier")
    assert check.await_args.args[0].token == "sk-ant-oat01-minted"
    assert (store.token, store.account_id, store.email) == ("sk-ant-oat01-minted", "tok-uuid", "user@example.com")
    assert FLOW_KEY not in cache.values and cache.values[f"{FLOW_KEY}:saved"] == "state-1"
    assert "sk-ant" not in response.text

    again, _, _ = await _token_call(client, "POST", path=f"{SIGN_IN}/code", body={"code": "code#state-1"}, cache=cache, store=store, mint=mint)
    assert again.status_code == 200 and again.json()["state"] == "signed_in"
    mint.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "mint", "answer", "status", "error", "retryable", "flow_kept"),
    [
        ("", None, True, 400, "code_missing", True, True),
        ("garbage", None, True, 400, "invalid_callback_input", True, True),
        ("code#other", None, True, 400, "state_mismatch", False, True),
        ("code#st\u00e4te", None, True, 400, "state_mismatch", False, True),
        ("code#state-1", _refused(400), True, 400, "code_rejected", False, False),
        ("code#state-1", _refused(403), True, 400, "code_rejected", False, False),
        ("code#state-1", _refused(429), True, 503, "check_unavailable", True, True),
        ("code#state-1", _refused(503), True, 503, "check_unavailable", True, True),
        ("code#state-1", httpx.ConnectError("down"), True, 503, "check_unavailable", True, True),
        ("code#state-1", claude_oauth.UnexpectedGrant("scope"), True, 502, "unexpected_grant", False, False),
        ("code#state-1", MINTED, TimeoutError(), 503, "check_unavailable", False, False),
        ("code#state-1", MINTED, False, 400, "token_rejected", False, False),
    ],
)
async def test_a_sign_in_that_does_not_end_in_a_working_token_saves_nothing(
    client, code, mint, answer, status, error, retryable, flow_kept,
):
    cache = _flow_cache()
    minting = AsyncMock(side_effect=mint) if isinstance(mint, Exception) else AsyncMock(return_value=mint)
    response, _, store = await _token_call(
        client, "POST", path=f"{SIGN_IN}/code", body={"code": code}, cache=cache, answer=answer, mint=minting,
    )
    assert response.status_code == status
    assert response.json()["error"]["code"] == error and response.json()["error"]["retryable"] is retryable
    assert store.writes == [] and (FLOW_KEY in cache.values) is flow_kept


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["paste", "sign-in"])
async def test_where_a_policy_is_installed_on_the_backend_no_code_is_spent_and_no_token_checked(
    client, tmp_path, monkeypatch, path,
):
    _policy_installed(tmp_path, monkeypatch)
    cache, mint = _flow_cache(), AsyncMock(return_value=MINTED)
    call = {"path": f"{SIGN_IN}/code", "body": {"code": "code#state-1"}} if path == "sign-in" else {"body": {"token": "tok"}}
    response, check, store = await _token_call(client, "POST" if path == "sign-in" else "PUT", cache=cache, mint=mint, **call)
    assert response.status_code == 400
    assert response.json()["error"] == {"code": "policy_blocked", "message": flow.POLICY_BLOCKED, "retryable": False}
    mint.assert_not_awaited()
    check.assert_not_awaited()
    assert store.writes == [] and FLOW_KEY in cache.values
    removed, _, _ = await _token_call(client, "DELETE", store=_Store("tok"))
    assert removed.status_code == 204, "a token saved before still goes"


@pytest.mark.parametrize("pasted", ["code#st\u00e4te", "code#st\ud800"])
def test_a_state_no_sign_in_could_have_made_is_a_mismatch(pasted):
    with pytest.raises(OAuthRefusal) as refused:
        PendingSignIn.code(pasted, "state-1")
    assert refused.value.error["code"] == "state_mismatch"


@pytest.mark.asyncio
async def test_a_sign_in_started_before_its_state_was_its_own_still_lands(client):
    cache = _Cache()
    cache.values[FLOW_KEY] = "verifier"
    mint = AsyncMock(return_value=MINTED)
    response, _, _ = await _token_call(
        client, "POST", path=f"{SIGN_IN}/code", body={"code": "code#verifier"}, cache=cache, mint=mint,
    )
    assert response.status_code == 200
    assert mint.await_args.args == ("code", "verifier", "verifier")


@pytest.mark.asyncio
async def test_a_sign_in_that_expired_says_so(client):
    response, _, _ = await _token_call(client, "POST", path=f"{SIGN_IN}/code", body={"code": "code#state-1"}, cache=_Cache())
    assert response.status_code == 400 and response.json()["error"]["code"] == "no_pending_flow"


@pytest.mark.asyncio
async def test_a_second_paste_finds_the_saved_marker_once_the_hold_is_released(client):
    cache, store = _flow_cache(), _Store()
    released_with_marker = []
    release = cache.release_lock

    async def release_lock(key, token):
        released_with_marker.append(f"{FLOW_KEY}:saved" in cache.values)
        await release(key, token)

    cache.release_lock = release_lock
    response, _, _ = await _token_call(
        client, "POST", path=f"{SIGN_IN}/code", body={"code": "code#state-1"}, cache=cache, store=store,
        mint=AsyncMock(return_value=MINTED),
    )
    assert response.status_code == 200 and released_with_marker == [True]


@pytest.mark.asyncio
async def test_removing_waits_for_a_save_in_progress(client):
    cache, store = _Cache(), _Store("tok")
    cache.locks["claude_code:token_save:test-user-123"] = "other"
    response, _, _ = await _token_call(client, "DELETE", store=store, cache=cache)
    assert response.status_code == 409 and response.json()["error"]["code"] == "check_in_progress"
    assert store.token == "tok" and store.writes == []


# -- Token hygiene --

SENTINEL = "sk-ant-oat01-SENTINEL-0123456789"


def _chain(exc):
    """An exception and every one a traceback of it prints."""
    while exc is not None:
        yield exc
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)


def _saved_paths():
    """Every way a token reaches the backend to be saved, each ending where it ends."""

    def leak(kind):
        return kind(f"failed on {SENTINEL}")

    minted = claude_oauth.SetupToken(access_token=SENTINEL, expires_at=MINTED.expires_at, token_uuid="u", email=None)
    paste = ("PUT", "/api/v1/claude-code/token", {"token": SENTINEL})
    code = ("POST", f"{SIGN_IN}/code", {"code": "code#state-1"})
    return {
        "saved": (paste, True, None),
        "check_refuses": (paste, False, None),
        "check_unreachable": (paste, leak(TimeoutError), None),
        "signed_in": (code, True, AsyncMock(return_value=minted)),
        "signed_in_check_refuses": (code, False, AsyncMock(return_value=minted)),
        "mint_fails": (code, True, AsyncMock(side_effect=leak(RuntimeError))),
        "mint_unexpected": (code, True, AsyncMock(side_effect=claude_oauth.UnexpectedGrant("scope=None"))),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("path", list(_saved_paths()))
async def test_a_saved_token_reaches_no_log_response_error_or_cache(caplog, path):
    import logging

    import structlog

    raised = []

    def refused(_, exc):
        raised.append(exc)
        return JSONResponse({"detail": exc.detail, "error": exc.error}, exc.status_code)

    app = create_test_app(route.router)
    app.add_exception_handler(OAuthRefusal, refused)
    (method, url, body), answer, mint = _saved_paths()[path]
    cache = _flow_cache()
    caplog.set_level(logging.DEBUG)
    with structlog.testing.capture_logs() as logged:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response, _, _ = await _token_call(client, method, path=url, body=body, cache=cache, answer=answer, mint=mint)
    assert response.status_code != 500, response.text
    assert SENTINEL not in response.text and SENTINEL not in caplog.text
    assert not any(SENTINEL in str(r.args) or SENTINEL in r.getMessage() for r in caplog.records)
    assert SENTINEL not in repr(logged)
    for exc in raised:
        for link in _chain(exc):
            assert SENTINEL not in str(link) and SENTINEL not in repr(link)
            assert SENTINEL not in repr(getattr(link, "error", "")) and SENTINEL not in repr(getattr(link, "detail", ""))
    assert SENTINEL not in json.dumps([cache.values, cache.locks], default=str)
