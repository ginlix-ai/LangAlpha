"""
Tests for the API Keys router (src/server/app/api_keys.py).

Covers GET/PUT/DELETE of BYOK API keys, key masking, and
the public /api/v1/models endpoint.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SUPPORTED_PROVIDERS = ["openai", "anthropic"]
PROVIDER_INFO_MAP = {
    "openai": {"display_name": "OpenAI", "access_type": "api_key", "brand_key": "openai"},
    "anthropic": {"display_name": "Anthropic", "access_type": "api_key", "brand_key": "anthropic"},
}

DB = "src.server.app.api_keys"


@asynccontextmanager
async def _no_transaction(_user_id):
    yield None


@pytest_asyncio.fixture
async def client():
    from src.server.app.api_keys import router
    import src.server.app.api_keys as api_keys_mod

    # Reset module-level cache so our mock takes effect
    api_keys_mod._BYOK_PROVIDERS_CACHE = None

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        with (
            patch(
                f"{DB}._get_supported_providers",
                return_value=SUPPORTED_PROVIDERS,
            ),
            patch(
                f"{DB}._get_provider_info_map",
                return_value=PROVIDER_INFO_MAP,
            ),
            # The machine's own server keys are not the test's.
            patch(f"{DB}.deployment_key", return_value=None),
            # The writes are mocked one by one; their transaction needs no pool.
            patch(f"{DB}.key_writes", _no_transaction),
        ):
            yield c

    # Clean up cache
    api_keys_mod._BYOK_PROVIDERS_CACHE = None


# ---------------------------------------------------------------------------
# GET /api/v1/users/me/api-keys
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_api_keys(client):
    data = {
        "byok_enabled": True,
        "keys": {"openai": "sk-abc123def456xyz"},
        "base_urls": {},
    }
    with (
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=data,
        ),
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        resp = await client.get("/api/v1/users/me/api-keys")

    assert resp.status_code == 200
    body = resp.json()
    assert body["byok_enabled"] is True
    # Keys should be masked
    openai_prov = next(p for p in body["providers"] if p["provider"] == "openai")
    assert openai_prov["has_key"] is True
    assert openai_prov["masked_key"].startswith("sk-")
    assert "..." in openai_prov["masked_key"]


@pytest.mark.asyncio
async def test_get_api_keys_no_keys(client):
    data = {
        "byok_enabled": False,
        "keys": {},
        "base_urls": {},
    }
    with (
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=data,
        ),
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        resp = await client.get("/api/v1/users/me/api-keys")

    assert resp.status_code == 200
    body = resp.json()
    assert body["byok_enabled"] is False
    for prov in body["providers"]:
        assert prov["has_key"] is False
        assert prov["masked_key"] is None


# ---------------------------------------------------------------------------
# PUT /api/v1/users/me/api-keys
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_api_keys_set_key(client):
    updated = {
        "byok_enabled": True,
        "keys": {"openai": "sk-newkey1234567890"},
        "base_urls": {},
    }
    with (
        patch(f"{DB}.set_byok_enabled", new_callable=AsyncMock),
        patch(f"{DB}.upsert_api_key", new_callable=AsyncMock),
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=updated,
        ),
        patch(
            "src.server.services.onboarding.maybe_complete_personalization",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={
                "byok_enabled": True,
                "api_keys": {"openai": "sk-newkey1234567890"},
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["byok_enabled"] is True


@pytest.mark.asyncio
async def test_update_api_keys_delete_key(client):
    """Setting a key to null should call delete_api_key."""
    updated = {
        "byok_enabled": True,
        "keys": {},
        "base_urls": {},
    }
    with (
        patch(f"{DB}.delete_api_key", new_callable=AsyncMock) as mock_delete,
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=updated,
        ),
        patch(
            "src.server.services.onboarding.maybe_complete_personalization",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"openai": None}},
        )

    assert resp.status_code == 200
    mock_delete.assert_awaited_once_with("test-user-123", "openai", conn=None)


@pytest.mark.asyncio
async def test_a_plans_key_saved_in_one_region_removes_it_from_the_others(client):
    # A plan runs in one region; a key the client never saw (saved from another
    # tab) goes too, in the same transaction as the new one.
    with (
        patch(f"{DB}.upsert_api_key", new_callable=AsyncMock) as upsert,
        patch(f"{DB}.delete_api_key", new_callable=AsyncMock) as delete,
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(f"{DB}.get_user_api_keys", new_callable=AsyncMock, return_value={"byok_enabled": True, "keys": {}, "base_urls": {}}),
        patch("src.server.services.onboarding.maybe_complete_personalization", new_callable=AsyncMock, return_value=False),
    ):
        resp = await client.put("/api/v1/users/me/api-keys", json={"api_keys": {"moonshot-cn-coding": "sk-placeholder"}})

    assert resp.status_code == 200
    upsert.assert_awaited_once()
    delete.assert_awaited_once_with("test-user-123", "moonshot-coding", conn=None)


@pytest.mark.asyncio
async def test_update_api_keys_unsupported_provider(client):
    with patch(
        f"{DB}._get_custom_providers",
        new_callable=AsyncMock,
        return_value=[],
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={"api_keys": {"unknown_provider": "sk-1234567890abcdef"}},
        )

    assert resp.status_code == 400
    assert "Unsupported provider" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_api_keys_base_url_validation(client):
    """Base URL not starting with http(s) should be rejected."""
    resp = await client.put(
        "/api/v1/users/me/api-keys",
        json={"base_urls": {"openai": "ftp://invalid"}},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url", ["http://127.0.0.1:11434/v1", "http://localhost:1234/v1", "http://169.254.169.254/latest"]
)
async def test_platform_refuses_a_private_base_url_before_writing(client, monkeypatch, url):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    with (
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(f"{DB}.set_byok_enabled", new_callable=AsyncMock) as toggle,
        patch(f"{DB}.upsert_api_key", new_callable=AsyncMock) as upsert,
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={
                "byok_enabled": True,
                "api_keys": {"openai": "sk-newkey1234567890"},
                "base_urls": {"openai": url},
            },
        )

    assert resp.status_code == 400
    assert resp.json()["detail"] == (
        "Base URL for openai cannot be used: it points to a private or reserved address. "
        "Use a public URL."
    )
    toggle.assert_not_awaited()
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_oss_saves_a_local_base_url(client):
    with (
        patch(f"{DB}.upsert_api_key", new_callable=AsyncMock) as upsert,
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value={"byok_enabled": True, "keys": {}, "base_urls": {}},
        ),
        patch(
            "src.server.services.onboarding.maybe_complete_personalization",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={
                "api_keys": {"openai": "sk-newkey1234567890"},
                "base_urls": {"openai": "http://localhost:1234/v1"},
            },
        )

    assert resp.status_code == 200
    upsert.assert_awaited_once_with(
        "test-user-123", "openai", "sk-newkey1234567890", base_url="http://localhost:1234/v1", conn=None
    )


@pytest.mark.asyncio
async def test_platform_key_test_refuses_a_private_base_url(client, monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    with (
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=MagicMock(enabled=False),
        ),
        patch("src.server.services.llm.key_probe.check_key", new_callable=AsyncMock) as probe,
    ):
        resp = await client.post(
            "/api/v1/keys/test",
            json={"provider": "openai", "api_key": "sk-newkey1234567890", "base_url": "http://[::1]:8000/v1"},
        )

    assert resp.status_code == 400
    assert "private or reserved address" in resp.json()["detail"]
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_platform_refuses_an_unknown_provider_before_resolving_its_url(client, monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    with (
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(
            "src.server.services.llm.egress.endpoint_refusal", new_callable=AsyncMock
        ) as check,
        patch(f"{DB}.set_byok_enabled", new_callable=AsyncMock) as toggle,
    ):
        resp = await client.put(
            "/api/v1/users/me/api-keys",
            json={
                "byok_enabled": True,
                "base_urls": {
                    "openai": "https://gateway.test/v1",
                    "not-a-provider": "https://slow.test/v1",
                },
            },
        )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Unsupported provider: not-a-provider."
    check.assert_not_awaited()
    toggle.assert_not_awaited()


@pytest.mark.asyncio
async def test_platform_key_test_refused_at_connect_is_a_400(client, monkeypatch):
    """The name is public when checked and loopback when dialed."""
    import socket

    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    answers = iter(["93.184.215.14", "127.0.0.1"])

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (next(answers), port))]

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    with (
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=MagicMock(enabled=False),
        ),
    ):
        resp = await client.post(
            "/api/v1/keys/test",
            json={"provider": "openai", "api_key": "sk-newkey1234567890", "base_url": "https://rebind.test/v1"},
        )

    assert resp.status_code == 400
    assert "private or reserved address" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# DELETE /api/v1/users/me/api-keys/{provider}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_api_key(client):
    updated = {
        "byok_enabled": True,
        "keys": {},
        "base_urls": {},
    }
    with (
        patch(f"{DB}.delete_api_key", new_callable=AsyncMock) as mock_delete,
        patch(
            f"{DB}._get_custom_providers",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value=updated,
        ),
        patch(f"{DB}.invalidate_byok_cache", new_callable=AsyncMock) as invalidate,
    ):
        resp = await client.delete("/api/v1/users/me/api-keys/openai")

    assert resp.status_code == 200
    mock_delete.assert_awaited_once_with("test-user-123", "openai")
    # Or the next turn still reads the deleted key's cached BYOK status.
    invalidate.assert_awaited_once_with("test-user-123")


@pytest.mark.asyncio
async def test_remove_api_key_unsupported_provider(client):
    with (
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(
            f"{DB}.get_user_api_keys",
            new_callable=AsyncMock,
            return_value={"byok_enabled": True, "keys": {}, "base_urls": {}},
        ),
        patch(f"{DB}.delete_api_key", new_callable=AsyncMock) as mock_delete,
    ):
        resp = await client.delete(
            "/api/v1/users/me/api-keys/unknown_provider"
        )

    assert resp.status_code == 400
    assert "Unsupported provider" in resp.json()["detail"]
    mock_delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_remove_key_of_withdrawn_provider(client):
    # A key saved before its provider left the list is still the user's to delete.
    stored = {"byok_enabled": True, "keys": {"withdrawn-provider": "sk-old"}, "base_urls": {}}
    with (
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(f"{DB}.get_user_api_keys", new_callable=AsyncMock, return_value=stored),
        patch(f"{DB}.delete_api_key", new_callable=AsyncMock) as mock_delete,
        patch(f"{DB}.invalidate_byok_cache", new_callable=AsyncMock),
        patch(f"{DB}.invalidate_available_models", new_callable=AsyncMock),
    ):
        resp = await client.delete("/api/v1/users/me/api-keys/withdrawn-provider")

    assert resp.status_code == 200
    mock_delete.assert_awaited_once_with("test-user-123", "withdrawn-provider")


@pytest.mark.asyncio
async def test_a_withdrawn_providers_key_is_listed_for_removal(client):
    # Its own provider gone, or the one a custom provider was built on: listed
    # as withdrawn so Settings offers to remove it, never as a connection.
    stored = {
        "byok_enabled": True,
        "keys": {"openai": "sk-abc123def456xyz", "withdrawn-provider": "ark-old-key-91c2", "my-gw": "sk-gw-0000"},
        "base_urls": {},
    }
    live = [{"name": "my-gw", "parent_provider": "openai"}]
    with (
        patch(f"{DB}.get_user_api_keys", new_callable=AsyncMock, return_value=stored),
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=live),
    ):
        resp = await client.get("/api/v1/users/me/api-keys")

    rows = {p["provider"]: p for p in resp.json()["providers"]}
    assert rows["withdrawn-provider"]["withdrawn"] is True
    assert rows["withdrawn-provider"]["masked_key"] == "ark...91c2"
    assert not rows["openai"].get("withdrawn") and not rows["my-gw"].get("withdrawn")


@pytest.mark.asyncio
async def test_a_custom_provider_named_like_a_built_in_lists_one_row(client):
    # Saved before its name became a built-in: the key under the name is one
    # key, so Settings shows one row for it, not two under the same slug.
    stored = {"byok_enabled": True, "keys": {"openai": "sk-abc123def456xyz"}, "base_urls": {}}
    legacy = [{"name": "openai", "parent_provider": "anthropic"}]
    with (
        patch(f"{DB}.get_user_api_keys", new_callable=AsyncMock, return_value=stored),
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=legacy),
    ):
        resp = await client.get("/api/v1/users/me/api-keys")

    rows = [p for p in resp.json()["providers"] if p["provider"] == "openai"]
    assert len(rows) == 1 and rows[0]["is_custom"] is False


@pytest.mark.asyncio
async def test_the_local_server_the_deployment_default_runs_on_is_listed(client):
    # Nothing the user stored names it, but the deployment's default runs on
    # it, so its models belong in the selector; it is the server's to remove.
    stored = {"byok_enabled": True, "keys": {}, "base_urls": {}}
    ollama = {"display_name": "Ollama", "access_type": "local", "brand_key": "ollama", "base_url": "http://localhost:11434/v1"}
    with (
        patch(f"{DB}._get_supported_providers", return_value=[*SUPPORTED_PROVIDERS, "ollama"]),
        patch(f"{DB}._get_provider_info_map", return_value={**PROVIDER_INFO_MAP, "ollama": ollama}),
        patch(f"{DB}.deployment_local_provider", return_value="ollama"),
        patch(f"{DB}.get_user_api_keys", new_callable=AsyncMock, return_value=stored),
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
    ):
        resp = await client.get("/api/v1/users/me/api-keys")

    row = next(p for p in resp.json()["providers"] if p["provider"] == "ollama")
    assert row["has_key"] is True and row["masked_key"] == "(server)"
    assert row["base_url"] == "http://localhost:11434/v1"



# ---------------------------------------------------------------------------
# GET /api/v1/models (public, no auth)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_models(client):
    mock_models = {
        "openai": [{"name": "gpt-4o", "model_id": "gpt-4o"}],
    }
    mock_mc = MagicMock()
    mock_mc.get_display_name.side_effect = lambda p: p.title()
    mock_mc.get_model_metadata.return_value = {}

    mock_llm_cls = MagicMock()
    mock_llm_cls.get_model_config.return_value = mock_mc

    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

    agent_cfg = AgentConfig(
        llm=LLMConfig(name="gpt-4o", background="gpt-4o-mini"),
        security=SecurityConfig(), logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test")), mcp=MCPConfig(), filesystem=FilesystemConfig(),
    )
    with (
        patch(
            "src.llms.llm.get_configured_llm_models",
            return_value=mock_models,
        ),
        patch(
            "src.llms.llm.LLM",
            mock_llm_cls,
        ),
        patch("src.server.app.setup.agent_config", agent_cfg),
    ):
        resp = await client.get("/api/v1/models")

    assert resp.status_code == 200
    body = resp.json()
    assert "models" in body
    assert "system_defaults" in body
    assert body["system_defaults"]["default_model"] == "gpt-4o"


# ---------------------------------------------------------------------------
# POST /api/v1/providers/custom/models
# ---------------------------------------------------------------------------

ENDPOINT_BODY = {"api_format": "openai-completions", "base_url": "https://gateway.example/v1", "api_key": "sk-x"}


@pytest.mark.asyncio
async def test_endpoint_models_read_through_the_egress_check(client):
    pinned = object()
    listing = {"status": "ok", "recognized": [], "others": []}
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=MagicMock(enabled=False)),
        patch(
            "src.server.services.llm.egress.user_endpoint_transport",
            new_callable=AsyncMock, return_value=pinned,
        ) as probe,
        patch(
            "src.server.services.llm.provider_models.endpoint_models",
            new_callable=AsyncMock, return_value=listing,
        ) as read,
    ):
        resp = await client.post("/api/v1/providers/custom/models", json=ENDPOINT_BODY)

    assert resp.status_code == 200
    assert resp.json() == listing
    probe.assert_awaited_once_with("https://gateway.example/v1")
    read.assert_awaited_once_with("openai-completions", "https://gateway.example/v1", "sk-x", transport=pinned)


@pytest.mark.asyncio
async def test_endpoint_models_refuse_an_unknown_format(client):
    resp = await client.post("/api/v1/providers/custom/models", json={**ENDPOINT_BODY, "api_format": "cohere"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_endpoint_models_are_rate_limited_per_user(client):
    """Each read sends the user's key to an address they chose."""
    with (
        patch(f"{DB}.throttle", new_callable=AsyncMock, return_value=False) as throttle,
        patch("src.server.services.llm.provider_models.endpoint_models", new_callable=AsyncMock) as read,
    ):
        resp = await client.post("/api/v1/providers/custom/models", json=ENDPOINT_BODY)

    assert resp.status_code == 429
    throttle.assert_awaited_once_with("endpoint_models:test-user-123", 12, 60)
    read.assert_not_awaited()


# ---------------------------------------------------------------------------
# _get_custom_providers — which column it reads
# ---------------------------------------------------------------------------

# Every test above patches _get_custom_providers wholesale, so nothing exercised
# the read itself. It spent a release pointed at the legacy column, which
# emptied the custom-provider list for every migrated user.


@pytest.mark.asyncio
async def test_custom_providers_read_from_model_preference():
    from src.server.app.api_keys import _get_custom_providers

    stored = [{"name": "mylab", "parent_provider": "openai"}]
    with patch(
        "src.server.database.user.get_user_preferences",
        AsyncMock(return_value={"other_preference": {}, "model_preference": {"custom_providers": stored}}),
    ):
        assert await _get_custom_providers("u1") == stored


@pytest.mark.asyncio
async def test_custom_providers_falls_back_to_legacy_column():
    """A row the 026 migration has not reached still resolves."""
    from src.server.app.api_keys import _get_custom_providers

    stored = [{"name": "mylab", "parent_provider": "openai"}]
    with patch(
        "src.server.database.user.get_user_preferences",
        AsyncMock(return_value={"other_preference": {"custom_providers": stored}, "model_preference": {}}),
    ):
        assert await _get_custom_providers("u1") == stored


@pytest.mark.asyncio
async def test_custom_providers_absent_is_empty():
    from src.server.app.api_keys import _get_custom_providers

    with patch("src.server.database.user.get_user_preferences", AsyncMock(return_value=None)):
        assert await _get_custom_providers("u1") == []


# ---------------------------------------------------------------------------
# OpenRouter key test: GET /key, since /models answers 200 to any key
# ---------------------------------------------------------------------------


def _openrouter_transport(status: int, body: dict, seen: list):
    import httpx

    def handle(request):
        seen.append((request.url.path, request.headers.get("authorization")))
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handle)


@pytest.mark.asyncio
async def test_openrouter_key_test_reports_what_the_key_may_spend():
    from src.server.services.llm.key_probe import _test_openrouter_key

    seen: list = []
    body = {"data": {"label": "k", "limit": 10, "usage": 2.5, "limit_remaining": 7.5,
                     "is_free_tier": False, "limit_reset": "monthly"}}
    result = await _test_openrouter_key(
        "sk-or-test", "m", {}, None, 5.0, transport=_openrouter_transport(200, body, seen)
    )

    assert seen == [("/api/v1/key", "Bearer sk-or-test")]
    assert result["credits"] == {
        "limit": 10.0, "usage": 2.5, "remaining": 7.5,
        "is_free_tier": False, "limit_reset": "monthly",
    }


@pytest.mark.asyncio
async def test_a_gateway_under_the_openrouter_parent_is_not_asked_for_key():
    """Only OpenRouter serves ``/key``. A gateway set up with OpenRouter as its
    parent answered that route with a 401 of its own, which read as the key
    failing; it gets the OpenAI-compatible test instead."""
    import httpx

    from src.server.services.llm.key_probe import _test_openrouter_key

    seen: list = []

    def handle(request):
        seen.append(request.url.path)
        if request.url.path.endswith("/models"):
            return httpx.Response(200, json={"data": [{"id": "gw-model"}]})
        return httpx.Response(401, json={"error": {"message": "unknown route"}})

    result = await _test_openrouter_key(
        "sk-gw", "m", {"sdk": "openrouter"}, "https://gateway.example/v1", 5.0,
        transport=httpx.MockTransport(handle),
    )

    assert seen == ["/v1/models"]
    assert result["models_found"] == 1


@pytest.mark.asyncio
async def test_openrouter_key_without_a_cap_has_no_remaining():
    from src.server.services.llm.key_probe import openrouter_key_credits

    credits = openrouter_key_credits(
        {"data": {"limit": None, "usage": 0.42, "limit_remaining": None, "is_free_tier": True}}
    )
    assert credits == {
        "limit": None, "usage": 0.42, "remaining": None,
        "is_free_tier": True, "limit_reset": None,
    }


@pytest.mark.asyncio
async def test_openrouter_bad_key_fails_the_test(client):
    import httpx

    seen: list = []
    transport = _openrouter_transport(401, {"error": {"message": "User not found.", "code": 401}}, seen)
    real_client = httpx.AsyncClient

    with (
        patch(f"{DB}._get_custom_providers", new_callable=AsyncMock, return_value=[]),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=MagicMock(enabled=False),
        ),
        patch(
            "httpx.AsyncClient",
            side_effect=lambda **kw: real_client(**{**kw, "transport": transport}),
        ),
    ):
        resp = await client.post(
            "/api/v1/keys/test", json={"provider": "openrouter", "api_key": "sk-or-bogus"}
        )

    assert resp.status_code == 200
    assert resp.json()["success"] is False
    assert resp.json()["error"] == "User not found."
    assert seen and seen[0][0] == "/api/v1/key"


@pytest.mark.asyncio
async def test_key_test_has_one_deadline_across_its_requests(monkeypatch):
    # A list that answers 404 is followed by a one-token call; a server that
    # then trickles its answer must not hold the test past the deadline.
    import asyncio

    import httpx

    from src.server.services.llm import key_probe

    class _Trickle(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                await asyncio.sleep(0.05)
                yield b" "

    def handle(request):
        if request.url.path.endswith("/models"):
            return httpx.Response(404)
        return httpx.Response(200, stream=_Trickle())

    monkeypatch.setattr(key_probe, "TEST_TIMEOUT_S", 0.3)
    with pytest.raises(key_probe.ProbeTimedOut):
        await key_probe.check_key(
            "openai", {"sdk": "openai", "base_url": "https://gw.example/v1"}, "sk-test", None,
            transport=httpx.MockTransport(handle),
        )
