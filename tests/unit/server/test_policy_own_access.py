"""Own model access the policy denies: not used at admission, not connectable,
and nothing read or asked when no policy is configured."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from tests.unit.server.policy_standing import standing_under
from src.config.models import PolicyLayer
from src.server.services.account import Standing
from src.server.services.policy import ALLOW_ALL, merge
from tests.conftest import create_test_app

LIMITS = "src.server.dependencies.usage_limits"
POLICY = "src.server.services.policy"

DENY_ALL_OWN = merge(
    None,
    PolicyLayer.model_validate({"own_model_access": {"api_keys": False, "subscriptions": False}}),
)



def _own_access(byok=True, oauth=True):
    return (
        patch("src.server.database.api_keys.is_byok_active", new_callable=AsyncMock, return_value=byok),
        patch(
            "src.server.database.oauth_tokens.has_any_oauth_token",
            new_callable=AsyncMock,
            return_value=oauth,
        ),
    )


@pytest.mark.asyncio
async def test_denied_own_access_takes_the_admitted_path():
    """A turn on a member's own key would skip the plan check; denied, it may not."""
    from src.server.dependencies.usage_limits import chat_limit_for

    byok, oauth = _own_access()
    tier = AsyncMock(return_value=0)
    with (
        byok, oauth,
        standing_under(DENY_ALL_OWN),
        patch(f"{LIMITS}.HOST_MODE", "platform"),
        patch("src.server.services.platform_client.HOST_MODE", "platform"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://auth"),
        patch("src.server.services.platform_client.AUTH_SERVICE_URL", "http://auth"),
        patch(f"{LIMITS}._check_burst_guard", new_callable=AsyncMock, return_value={"allowed": True}),
        patch("src.server.services.membership.access_tier", tier),
    ):
        auth = await chat_limit_for("u")
    assert (auth.is_byok, auth.has_oauth, auth.own_subscriptions) == (False, False, False)
    tier.assert_awaited_once_with("u")
    assert auth.access_tier == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("host_mode", ["oss", "platform"])
async def test_nothing_configured_costs_admission_no_read_and_no_request(host_mode):
    """No deployment policy and the teams flag off: admission reads and asks
    nothing it did not before (an own-key user skips the tier read)."""
    from src.server.dependencies.usage_limits import chat_limit_for

    cache, client = MagicMock(), AsyncMock()
    byok, oauth = _own_access()
    with (
        byok, oauth,
        patch("src.server.services.account.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.membership.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.account.deployment_policy", return_value=None),
        patch(f"{LIMITS}.HOST_MODE", host_mode),
        patch("src.server.services.platform_client.HOST_MODE", host_mode),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://auth"),
        patch("src.server.services.platform_client.AUTH_SERVICE_URL", "http://auth"),
        patch("src.server.services.platform_client.get_http_client", return_value=client),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch(f"{LIMITS}._check_burst_guard", new_callable=AsyncMock, return_value={"allowed": True}),
    ):
        auth = await chat_limit_for("u")
    assert auth.is_byok is True
    assert not cache.method_calls
    client.post.assert_not_called()


@pytest.mark.asyncio
async def test_own_access_counts_only_what_the_policy_allows():
    from src.server.services.account import own_access

    byok, oauth = _own_access()
    with byok, oauth as sign_ins:
        denied = await own_access("u", policy=DENY_ALL_OWN)
        allowed = await own_access("u", policy=ALLOW_ALL)
        sign_ins.reset_mock()
        keys_only = await own_access("u", sign_ins=False, policy=ALLOW_ALL)
    assert (denied.api_keys, denied.subscriptions, denied.any) == (False, False, False)
    assert (allowed.api_keys, allowed.subscriptions, allowed.any) == (True, True, True)
    assert (keys_only.api_keys, keys_only.subscriptions) == (True, None)
    sign_ins.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_call_between_turns_does_not_count_own_keys_the_policy_denies():
    from src.server.handlers import thread_maintenance

    resolver = MagicMock()
    byok, oauth = _own_access()
    with (
        byok, oauth,
        standing_under(DENY_ALL_OWN),
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt",
            new_callable=AsyncMock, return_value=None,
        ),
        patch("src.server.services.thread_agent.thread_resolver", resolver),
    ):
        await thread_maintenance._resolver("u", "t", {}, MagicMock())
    assert resolver.call_args.kwargs["is_byok"] is False


@pytest.mark.asyncio
async def test_an_unknown_policy_refuses_admission():
    from fastapi import HTTPException

    from src.server.dependencies.usage_limits import chat_limit_for

    with (
        patch(
            "src.server.services.account.read_standing",
            new_callable=AsyncMock,
            return_value=Standing(None, None),
        ),
        pytest.raises(HTTPException) as refused,
    ):
        await chat_limit_for("u")
    assert refused.value.status_code == 503


# ---------------------------------------------------------------------------
# Connect routes
# ---------------------------------------------------------------------------


async def _client(router):
    return AsyncClient(transport=ASGITransport(app=create_test_app(router)), base_url="http://test")


@pytest.mark.asyncio
async def test_saving_an_own_key_is_refused_and_removing_one_is_not():
    from src.server.app import api_keys

    with (
        standing_under(DENY_ALL_OWN),
        patch.object(api_keys, "policy_for", new_callable=AsyncMock, return_value=DENY_ALL_OWN),
    ):
        async with await _client(api_keys.router) as c:
            refused = await c.put("/api/v1/users/me/api-keys", json={"api_keys": {"openai": "sk-12345678"}})
            turned_on = await c.put("/api/v1/users/me/api-keys", json={"byok_enabled": True})
            with (
                patch.object(api_keys, "_get_custom_providers", new_callable=AsyncMock, return_value=[]),
                patch.object(api_keys, "_get_allowed_providers", return_value={"openai"}),
                patch.object(api_keys, "key_writes") as writes,
                patch.object(api_keys, "delete_api_key", new_callable=AsyncMock),
                patch.object(api_keys, "invalidate_byok_cache", new_callable=AsyncMock),
                patch.object(api_keys, "invalidate_available_models", new_callable=AsyncMock),
                patch("src.server.services.onboarding.maybe_complete_personalization", new_callable=AsyncMock),
                patch.object(api_keys, "get_user_api_keys", new_callable=AsyncMock,
                             return_value={"byok_enabled": True, "keys": {}, "base_urls": {}}),
                patch.object(api_keys, "_format_response", return_value={"providers": []}),
            ):
                writes.return_value.__aenter__ = AsyncMock(return_value=None)
                writes.return_value.__aexit__ = AsyncMock(return_value=False)
                removed = await c.put("/api/v1/users/me/api-keys", json={"api_keys": {"openai": None}})
    assert refused.status_code == turned_on.status_code == 403
    detail = refused.json()["detail"]
    assert (detail["type"], detail["rule"]) == ("policy_denied", "own_model_access.api_keys")
    assert removed.status_code == 200
    assert removed.json()["own_keys_allowed"] is False


@pytest.mark.asyncio
async def test_keys_list_and_remove_while_the_policy_is_unknown():
    """The settings page lists keys during an outage, and a removal answers
    after it commits rather than failing then."""
    from src.server.app import api_keys

    delete = AsyncMock()
    with (
        patch(
            "src.server.services.account.read_standing",
            new_callable=AsyncMock,
            return_value=Standing(None, None),
        ),
        patch.object(api_keys, "policy_for", new_callable=AsyncMock, return_value=None),
        patch.object(api_keys, "_get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch.object(api_keys, "_get_allowed_providers", return_value={"openai"}),
        patch.object(api_keys, "delete_api_key", delete),
        patch.object(api_keys, "invalidate_byok_cache", new_callable=AsyncMock),
        patch.object(api_keys, "invalidate_available_models", new_callable=AsyncMock),
        patch.object(api_keys, "get_user_api_keys", new_callable=AsyncMock,
                     return_value={"byok_enabled": True, "keys": {}, "base_urls": {}}),
        patch.object(api_keys, "_format_response", return_value={"providers": []}),
    ):
        async with await _client(api_keys.router) as c:
            listed = await c.get("/api/v1/users/me/api-keys")
            removed = await c.delete("/api/v1/users/me/api-keys/openai")
            saved = await c.put("/api/v1/users/me/api-keys", json={"api_keys": {"openai": "sk-12345678"}})
    assert (listed.status_code, removed.status_code) == (200, 200)
    assert listed.json()["own_keys_allowed"] is None
    assert removed.json()["own_keys_allowed"] is None
    delete.assert_awaited_once()
    # A write that must refuse still refuses, before it changes anything.
    assert saved.status_code == 503


@pytest.mark.asyncio
async def test_a_model_sign_in_is_refused():
    from src.server.app import oauth

    with standing_under(DENY_ALL_OWN):
        async with await _client(oauth.router) as c:
            device = await c.post("/api/v1/oauth/codex/device/initiate")
            claude = await c.post("/api/v1/oauth/claude/initiate")
    for resp in (device, claude):
        assert resp.status_code == 403
        assert resp.json()["detail"]["rule"] == "own_model_access.subscriptions"


@pytest.mark.asyncio
async def test_a_claude_code_subscription_is_refused():
    from src.server.app import claude_code

    with (
        standing_under(DENY_ALL_OWN),
        patch.object(claude_code, "backend_on", new_callable=AsyncMock, return_value=True),
    ):
        async with await _client(claude_code.router) as c:
            resp = await c.post("/api/v1/claude-code/sign-in")
    assert resp.status_code == 403
    assert resp.json()["detail"]["rule"] == "own_model_access.subscriptions"


@pytest.mark.asyncio
async def test_a_claude_code_token_for_a_computer_is_refused():
    from src.server.app import claude_code_computers as computers

    place = AsyncMock()
    with (
        standing_under(DENY_ALL_OWN),
        patch.object(computers, "rows_on", new_callable=AsyncMock, return_value=True),
        patch.object(computers.MachineHome, "of", place),
    ):
        async with await _client(computers.router) as c:
            base = "/api/v1/claude-code/computers/c-1"
            started = await c.post(f"{base}/sign-in")
            finished = await c.post(f"{base}/sign-in/code", json={"code": "abc#def"})
            pasted = await c.put(f"{base}/token", json={"token": "sk-ant-oat01-x"})
    for resp in (started, finished, pasted):
        assert resp.status_code == 403
        assert resp.json()["detail"]["rule"] == "own_model_access.subscriptions"
    place.assert_not_awaited()


@pytest.mark.asyncio
async def test_storing_a_claude_code_token_is_refused_and_dropping_it_is_not():
    from src.server.app import claude_code
    from src.server.models.claude_code import ClaudeCodeCustody

    choose = AsyncMock(return_value=ClaudeCodeCustody(mode="local", offered=[]))
    with (
        standing_under(DENY_ALL_OWN),
        patch.object(claude_code.flow, "rows_on", new_callable=AsyncMock, return_value=True),
        patch.object(claude_code.hosted, "choose", choose),
    ):
        async with await _client(claude_code.router) as c:
            stored = await c.put("/api/v1/claude-code/custody", json={"mode": "stored"})
            local = await c.put("/api/v1/claude-code/custody", json={"mode": "local"})
    assert stored.status_code == 403
    assert local.status_code == 200
    assert stored.json()["detail"]["rule"] == "own_model_access.subscriptions"
    choose.assert_awaited_once()
    assert choose.await_args.args[1] == "local"


@pytest.mark.asyncio
async def test_connect_routes_are_open_with_no_policy():
    from src.server.app import oauth

    with (
        standing_under(ALLOW_ALL),
        patch.object(oauth, "_device_initiate", new_callable=AsyncMock, return_value={"ok": 1}),
    ):
        async with await _client(oauth.router) as c:
            resp = await c.post("/api/v1/oauth/codex/device/initiate")
    assert resp.status_code == 200
