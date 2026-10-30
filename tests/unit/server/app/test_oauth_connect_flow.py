"""The connect-flow contract the setup UI builds on: the device poll's
``status`` values and every failure's stable ``{code, message}``, read here
through Codex's flow.

Upstream answers are the ones auth.openai.com gave live probes (2026-10-24):
403 ``deviceauth_authorization_pending``, 403 ``deviceauth_user_code_expired``
after the code's expiry, 404 ``deviceauth_not_found``, 400
``deviceauth_user_code_mismatch``.
"""

import asyncio
import json
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException

from src.server.app import oauth as oauth_app
from src.server.services import claude_oauth, codex_oauth
from src.server.services.codex_oauth import classify_device_poll
from src.server.services.oauth_protocol import (
    DevicePoll,
    DevicePollFailed,
    DeviceUpstreamUnavailable,
)

_REQ = httpx.Request("POST", "https://auth.example/token")


@pytest.fixture(autouse=True)
def _connection_locks(monkeypatch):
    """Postgres's per-connection advisory lock, as an asyncio lock."""
    locks = defaultdict(asyncio.Lock)

    @asynccontextmanager
    async def change(user_id, provider):
        async with locks[(user_id, provider)]:
            yield MagicMock(name="conn")

    monkeypatch.setattr(oauth_app, "connection_change", change)


# --- Reading the device token endpoint -----------------------------------------


@pytest.mark.parametrize(
    "status, code, expired_locally, expected",
    [
        (403, "deviceauth_authorization_pending", False, "pending"),
        (403, "deviceauth_authorization_pending", True, "pending"),
        # what OpenAI answers once the code's 15 minutes are up
        (403, "deviceauth_user_code_expired", False, "expired"),
        (404, "deviceauth_not_found", False, "pending"),
        (404, "deviceauth_not_found", True, "expired"),
        (403, "deviceauth_authorization_denied", False, "denied"),
        (400, "access_denied", False, "denied"),
        (400, "expired_token", False, "expired"),
        (410, "deviceauth_expired", False, "expired"),
    ],
)
def test_device_poll_statuses(status, code, expired_locally, expected):
    assert classify_device_poll(status, code, expired_locally=expired_locally).status == expected


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_device_poll_outage_is_retryable(status):
    with pytest.raises(DeviceUpstreamUnavailable):
        classify_device_poll(status, None, expired_locally=False)


def test_device_poll_unexpected_refusal_is_terminal():
    with pytest.raises(DevicePollFailed) as raised:
        classify_device_poll(400, "deviceauth_user_code_mismatch", expired_locally=False)
    assert raised.value.code == "deviceauth_user_code_mismatch"


# --- The poll endpoint ---------------------------------------------------------------


def _cache(stored=None):
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.get = AsyncMock(return_value=stored)
    cache.client.set = AsyncMock()
    cache.client.delete = AsyncMock()
    cache.acquire_lock = AsyncMock(return_value=True)
    cache.release_lock = AsyncMock()
    # What the key holds when a write lands, which a restarted sign-in changes
    # after the poll read it. The writes act only while it is what was read,
    # as the Lua scripts do.
    cache.current = stored
    cache.claim = AsyncMock(side_effect=lambda key, value: cache.current == value)
    cache.swap = AsyncMock(side_effect=lambda key, old, new: cache.current == old)
    return cache


def _pending_flow():
    return json.dumps({
        "device_auth_id": "deviceauth_x",
        "user_code": "ABCD-EFGHI",
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
    })


async def _poll(cache, poll_result=None, poll_error=None, poll=None):
    poll = poll or AsyncMock(return_value=poll_result, side_effect=poll_error)
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        return await oauth_app._device_poll(replace(codex_oauth.DEVICE_FLOW, poll=poll), "u1")


@pytest.mark.asyncio
async def test_no_pending_flow_reads_as_expired():
    assert await _poll(_cache(None)) == {
        "status": "expired", "pending": False, "code": "no_pending_flow",
    }


@pytest.mark.asyncio
async def test_pending_keeps_the_legacy_flag():
    body = await _poll(_cache(_pending_flow()), DevicePoll("pending"))
    assert body == {"status": "pending", "pending": True}


@pytest.mark.asyncio
async def test_a_poll_inside_the_interval_does_not_ask_the_provider():
    cache = _cache(_pending_flow())
    await _poll(cache, DevicePoll("pending"))
    cache.client.get.return_value = cache.swap.await_args.args[2]

    poll = AsyncMock()
    assert (await _poll(cache, poll=poll))["status"] == "pending"
    poll.assert_not_awaited()


@pytest.mark.asyncio
async def test_slow_down_widens_the_interval():
    cache = _cache(_pending_flow())
    await _poll(cache, DevicePoll("pending", code="slow_down"))
    assert json.loads(cache.swap.await_args.args[2])["interval"] == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, code", [("denied", "access_denied"), ("expired", "device_code_expired")]
)
async def test_terminal_statuses_end_the_flow(status, code):
    cache = _cache(_pending_flow())
    body = await _poll(cache, DevicePoll(status, code="upstream_text_code"))
    assert body == {"status": status, "pending": False, "code": code}
    cache.claim.assert_awaited_once_with("oauth:device:u1", _pending_flow_read(cache))


@pytest.mark.asyncio
async def test_upstream_outage_is_a_retryable_error():
    cache = _cache(_pending_flow())
    with pytest.raises(HTTPException) as raised:
        await _poll(cache, poll_error=DeviceUpstreamUnavailable("status=503"))
    assert raised.value.status_code == 502
    assert raised.value.error["code"] == "upstream_unavailable"
    assert raised.value.error["retryable"] is True
    cache.claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_poll_ends_before_its_lock_does(monkeypatch):
    # Past the lease another poll could spend the same authorization, so a
    # provider that trickles is cut off first and asked again later.
    assert oauth_app._POLL_DEADLINE_S * 1000 < oauth_app._POLL_LOCK_TTL_MS
    monkeypatch.setattr(oauth_app, "_POLL_DEADLINE_S", 0.01)
    cache = _cache(_pending_flow())

    async def trickle(*_args):
        await asyncio.sleep(1)

    with pytest.raises(HTTPException) as raised:
        await _poll(cache, poll=trickle)
    assert raised.value.error["code"] == "upstream_unavailable"
    assert raised.value.error["retryable"] is True
    cache.claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_poll_redis_cannot_lock_waits_rather_than_asking_the_provider():
    # Uncoordinated, two polls could both spend the same approval.
    cache = _cache(_pending_flow())
    cache.acquire_lock.return_value = None
    poll = AsyncMock()
    with pytest.raises(HTTPException) as raised:
        await _poll(cache, poll=poll)
    assert raised.value.status_code == 503
    assert raised.value.error["retryable"] is True
    poll.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("key, flow_key", [("codex", "oauth:device:u1"), ("claude", "oauth:claude:u1")])
async def test_disconnecting_ends_a_pending_sign_in_before_the_tokens(key, flow_key):
    # The pending save claims its flow before it writes, so with the flow gone
    # an approval arriving after the disconnect saves nothing.
    order = []
    cache = _cache(_pending_flow())
    cache.client.delete = AsyncMock(side_effect=lambda k: order.append(("flow", k)))
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch.object(oauth_app, "delete_oauth_tokens", AsyncMock(side_effect=lambda *_, **__: order.append(("tokens",)))),
        patch.object(oauth_app, "connection_changed", AsyncMock()),
    ):
        await oauth_app.oauth_disconnect(key, "u1")
    assert order == [("flow", flow_key), ("tokens",)]


@pytest.mark.asyncio
async def test_a_disconnect_waits_for_a_save_already_writing():
    # Claimed but not yet written: a disconnect landing here would delete
    # nothing, and the write would bring the connection back after it.
    order = []
    writing, release = asyncio.Event(), asyncio.Event()
    cache = _cache(_pending_flow())

    async def store(*_, **__):
        writing.set()
        await release.wait()
        order.append("stored")

    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch.object(oauth_app, "store_tokens", AsyncMock(side_effect=store)),
        patch.object(oauth_app, "delete_oauth_tokens", AsyncMock(side_effect=lambda *_, **__: order.append("deleted"))),
        patch.object(oauth_app, "connection_changed", AsyncMock()),
    ):
        save = asyncio.create_task(
            oauth_app._save(cache, "oauth:device:u1", cache.current, "u1", "codex-oauth", MagicMock())
        )
        await writing.wait()
        disconnect = asyncio.create_task(oauth_app.oauth_disconnect("codex", "u1"))
        await asyncio.sleep(0.05)
        assert order == []
        release.set()
        await asyncio.gather(save, disconnect)
    assert order == ["stored", "deleted"]


@pytest.mark.asyncio
async def test_a_save_stalled_past_its_deadline_fails_inside_the_poll_lease(monkeypatch):
    # The poll's lease must outlast the save, or a second poll could take the
    # approved flow while this one still waits on Postgres.
    assert oauth_app._POLL_DEADLINE_S + oauth_app._SAVE_DEADLINE_S < oauth_app._POLL_LOCK_TTL_MS / 1000
    monkeypatch.setattr(oauth_app, "_SAVE_DEADLINE_S", 0.05)
    cache = _cache(_pending_flow())

    async def stalled(*_, **__):
        await asyncio.sleep(5)

    with patch.object(oauth_app, "store_tokens", AsyncMock(side_effect=stalled)):
        with pytest.raises(HTTPException) as raised:
            await oauth_app._save(cache, "oauth:device:u1", cache.current, "u1", "codex-oauth", MagicMock())
    assert raised.value.error["code"] == "save_failed"


@pytest.mark.asyncio
async def test_unexpected_refusal_is_a_terminal_error():
    cache = _cache(_pending_flow())
    with pytest.raises(HTTPException) as raised:
        await _poll(cache, poll_error=DevicePollFailed(400, "deviceauth_user_code_mismatch"))
    detail = raised.value.error
    assert detail["code"] == "device_poll_failed" and detail["retryable"] is False
    cache.claim.assert_awaited_once()


@pytest.mark.asyncio
async def test_overlapping_poll_waits_instead_of_exchanging_twice():
    cache = _cache(_pending_flow())
    cache.acquire_lock.return_value = False
    poll = AsyncMock()
    body = await _poll(cache, poll=poll)
    assert body["status"] == "pending"
    poll.assert_not_awaited()


@pytest.mark.asyncio
async def test_exchange_failure_is_scrubbed():
    cache = _cache(_pending_flow())
    secret = "sk-proj-SECRETSECRETSECRET"
    resp = httpx.Response(
        400, json={"error": {"code": "invalid_grant", "message": f"bad {secret}"}},
        request=_REQ,
    )
    # The approval came back and the exchange it leads to was refused.
    exchange_error = httpx.HTTPStatusError(f"400 {secret}", request=_REQ, response=resp)
    with pytest.raises(HTTPException) as raised:
        await _poll(cache, poll_error=exchange_error)
    detail = raised.value.error
    assert detail["code"] == "exchange_failed"
    assert detail["upstream_code"] == "invalid_grant"
    assert secret not in json.dumps(detail)
    cache.claim.assert_awaited_once_with("oauth:device:u1", _pending_flow_read(cache))


def _pending_flow_read(cache):
    return cache.client.get.return_value


def _tokens():
    from src.server.services.oauth_protocol import TokenSet

    return TokenSet(
        access_token="at", refresh_token="rt", account_id="acct",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )


def _flow(flow_id):
    return json.dumps({**json.loads(_pending_flow()), "flow_id": flow_id})


def _claude_flow(state="state-1"):
    return json.dumps({"verifier": "verifier", "state": state})


def _restarted(first, second):
    # The poll read the first sign-in, then the user started again while it
    # waited on the provider.
    cache = _cache(first)
    cache.current = second
    return cache


@pytest.mark.asyncio
async def test_a_restarted_sign_in_is_not_overwritten_by_the_old_poll():
    cache = _restarted(_flow("first"), _flow("second"))
    assert (await _poll(cache, DevicePoll("pending")))["status"] == "pending"
    assert cache.swap.await_args.args[1] == _pending_flow_read(cache)


@pytest.mark.asyncio
async def test_a_restarted_sign_in_does_not_take_the_old_approval():
    # The old code was approved after the restart: its tokens may be another
    # account's, and the sign-in the user is now in must not be replaced.
    cache = _restarted(_flow("first"), _flow("second"))
    store = AsyncMock()
    with patch.object(oauth_app, "store_tokens", store):
        body = await _poll(cache, DevicePoll("success", tokens=_tokens()))
    assert body == {"status": "expired", "pending": False, "code": "no_pending_flow"}
    store.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_current_sign_in_takes_its_approval():
    cache = _cache(_flow("first"))
    store = AsyncMock()
    with (
        patch.object(oauth_app, "store_tokens", store),
        patch.object(oauth_app, "connection_changed", AsyncMock()),
    ):
        body = await _poll(cache, DevicePoll("success", tokens=_tokens()))
    assert body["status"] == "success"
    store.assert_awaited_once()
    cache.claim.assert_awaited_once_with("oauth:device:u1", _pending_flow_read(cache))


@pytest.mark.asyncio
async def test_initiate_gives_each_sign_in_its_own_id():
    from src.server.services.oauth_protocol import DeviceCode

    device = DeviceCode(
        state={"device_auth_id": "deviceauth_x"}, user_code="ABCD-EFGHI",
        verification_url="https://auth.example/device", url_has_code=False, interval=5,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )
    flow = replace(codex_oauth.DEVICE_FLOW, request=AsyncMock(return_value=device))
    cache = _cache()
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch.object(oauth_app, "throttle", AsyncMock(return_value=True)),
    ):
        await oauth_app._device_initiate(flow, "u1")
        await oauth_app._device_initiate(flow, "u1")
    ids = [json.loads(c.args[1])["flow_id"] for c in cache.client.set.await_args_list]
    assert len(set(ids)) == 2 and all(ids)


@pytest.mark.asyncio
async def test_a_stalled_code_request_ends_as_a_retryable_refusal(monkeypatch):
    async def stall():
        await asyncio.sleep(10)

    monkeypatch.setattr(oauth_app, "_INITIATE_DEADLINE_S", 0.05)
    flow = replace(codex_oauth.DEVICE_FLOW, request=stall)
    cache = _cache()
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch.object(oauth_app, "throttle", AsyncMock(return_value=True)),
    ):
        with pytest.raises(HTTPException) as raised:
            await oauth_app._device_initiate(flow, "u1")
    assert raised.value.error["code"] == "upstream_unavailable"
    assert raised.value.error["retryable"] is True
    cache.client.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_claude_callback_leaves_a_restarted_sign_in_alone():
    cache = _restarted(_claude_flow(), _claude_flow("newer-state"))
    store = AsyncMock()
    with patch.object(oauth_app, "store_tokens", store):
        with pytest.raises(HTTPException) as raised:
            await _claude(cache, "code#state-1")
    assert raised.value.error["code"] == "no_pending_flow"
    store.assert_not_awaited()


# --- Claude callback --------------------------------------------------------------


async def _claude(cache, callback_input, exchange=None):
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch.object(claude_oauth, "exchange_code", exchange or AsyncMock()),
    ):
        return await oauth_app.claude_callback(
            user_id="u1", body=oauth_app.ClaudeCallbackRequest(callback_input=callback_input)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stored, callback_input, code",
    [
        (None, "code#state-1", "no_pending_flow"),
        (_claude_flow(), "garbage", "invalid_callback_input"),
        (_claude_flow(), "code#verifier", "state_mismatch"),
    ],
)
async def test_claude_callback_failures_have_codes(stored, callback_input, code):
    with pytest.raises(HTTPException) as raised:
        await _claude(_cache(stored), callback_input)
    assert raised.value.error["code"] == code
    assert raised.value.error["message"]


async def _two_pastes(store_result):
    flow, held = {"oauth:claude:u1": _claude_flow()}, set()
    cache = _cache(_claude_flow())
    cache.client.get = AsyncMock(side_effect=lambda k: flow.get(k))
    cache.client.set = AsyncMock(side_effect=lambda k, v, **_: flow.__setitem__(k, v))

    async def claim(key, value):
        return flow.pop(key, None) == value

    async def acquire(key, token, ttl_ms):
        if key in held:
            return False
        held.add(key)
        return True

    cache.claim = AsyncMock(side_effect=claim)
    cache.acquire_lock = AsyncMock(side_effect=acquire)
    cache.release_lock = AsyncMock(side_effect=lambda key, token: held.discard(key))
    exchanging, release = asyncio.Event(), asyncio.Event()

    async def exchange(*_):
        exchanging.set()
        await release.wait()
        return MagicMock()

    exchange_mock = AsyncMock(side_effect=exchange)

    def paste():
        body = oauth_app.ClaudeCallbackRequest(callback_input="code#state-1")
        return asyncio.create_task(oauth_app.claude_callback(user_id="u1", body=body))

    # Patched once around both: patches the two tasks entered and left in
    # turn would restore each other's mocks rather than the originals.
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch.object(claude_oauth, "exchange_code", exchange_mock),
        patch.object(oauth_app, "store_tokens", AsyncMock(side_effect=store_result)),
        patch.object(oauth_app, "connection_changed", AsyncMock()),
        # A reconnect: an earlier Claude connection is already stored.
        patch.object(oauth_app, "get_oauth_connection", AsyncMock(return_value={"account_id": "account-example"})),
    ):
        first = paste()
        await exchanging.wait()
        second = paste()
        await asyncio.sleep(0.3)
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
    exchange_mock.assert_awaited_once()
    return results


@pytest.mark.asyncio
async def test_a_second_paste_of_the_same_code_waits_and_answers_as_the_first():
    # The code is single-use: exchanged twice, the provider refuses the second
    # and a sign-in that worked would be reported as failed.
    assert await _two_pastes(None) == [{"success": True}, {"success": True}]


@pytest.mark.asyncio
async def test_a_second_paste_is_not_told_a_failed_save_worked():
    # A reconnect whose save failed leaves the old connection standing; the
    # waiting paste must not read that as its own sign-in having worked.
    first, second = await _two_pastes(RuntimeError("db down"))
    assert first.error["code"] == "save_failed"
    assert isinstance(second, HTTPException) and second.error["code"] == "no_pending_flow"


@pytest.mark.asyncio
async def test_claude_exchange_outage_is_retryable():
    exchange = AsyncMock(side_effect=httpx.ConnectError("down", request=_REQ))
    with pytest.raises(HTTPException) as raised:
        await _claude(_cache(_claude_flow()), "code#state-1", exchange)
    assert raised.value.status_code == 502
    # Pages loaded before the code existed show ``detail`` as text.
    assert raised.value.detail == "Claude could not be reached. Try again."
    assert raised.value.error == {
        "code": "upstream_unavailable",
        "message": "Claude could not be reached. Try again.",
        "retryable": True,
    }


@pytest.mark.asyncio
async def test_a_claude_exchange_that_outlasts_its_deadline_is_retryable(monkeypatch):
    # httpx bounds each phase only; the whole exchange must end before the
    # callback's lease, or a second paste could spend the same code.
    monkeypatch.setattr(oauth_app, "_CALLBACK_DEADLINE_S", 0.05)

    async def trickle(*_):
        await asyncio.sleep(5)

    with pytest.raises(HTTPException) as raised:
        await _claude(_cache(_claude_flow()), "code#state-1", AsyncMock(side_effect=trickle))
    assert raised.value.error["code"] == "upstream_unavailable"
    assert raised.value.error["retryable"] is True


@pytest.mark.asyncio
async def test_redis_down_has_a_code():
    cache = _cache()
    cache.enabled = False
    with pytest.raises(HTTPException) as raised:
        await _claude(cache, "code#state-1")
    assert raised.value.status_code == 503
    assert raised.value.error["code"] == "redis_unavailable"


def test_routes_keep_the_paths_the_client_calls():
    paths = {(m, r.path) for r in oauth_app.router.routes for m in r.methods}
    assert {
        ("POST", "/api/v1/oauth/{key}/device/initiate"),
        ("POST", "/api/v1/oauth/{key}/device/poll"),
        ("POST", "/api/v1/oauth/claude/initiate"),
        ("POST", "/api/v1/oauth/claude/callback"),
        ("GET", "/api/v1/oauth/{key}/status"),
        ("DELETE", "/api/v1/oauth/{key}"),
    } <= paths
    assert set(oauth_app._DEVICE_FLOWS) == {"codex", "grok"}
    assert {oauth_app._subscription(k) for k in ("codex", "claude", "grok")} == {
        "codex-oauth", "claude-oauth", "grok-oauth",
    }
    with pytest.raises(HTTPException) as raised:
        oauth_app._subscription("gemini")
    assert raised.value.status_code == 404
