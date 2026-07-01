"""Unit tests for the Robinhood Agentic Trading MCP OAuth service.

Drives the HTTP-bearing functions (discovery, DCR, token exchange/refresh)
against an httpx.MockTransport — real request/response plumbing, no network —
and the get_valid_robinhood_token orchestrator with the DB + vault + Redis
seams monkeypatched.
"""

from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.server.services import robinhood_oauth as ro

MCP_URL = ro.ROBINHOOD_MCP_URL
ORIGIN = "https://agent.robinhood.com"
AUTH_EP = f"{ORIGIN}/oauth/authorize"
TOKEN_EP = f"{ORIGIN}/oauth/token"
REG_EP = f"{ORIGIN}/oauth/register"


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --- PKCE + authorize URL ---------------------------------------------------


def test_generate_pkce_pair_is_valid_s256():
    verifier, challenge = ro.generate_pkce_pair()
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    assert challenge == expected


def test_build_authorize_url_carries_pkce_and_resource():
    url = ro.build_authorize_url(
        authorization_endpoint=AUTH_EP,
        client_id="cid",
        redirect_uri="http://localhost:8000/cb",
        state="st",
        challenge="ch",
        resource=MCP_URL,
        scope="trade",
    )
    assert url.startswith(AUTH_EP + "?")
    for frag in (
        "response_type=code", "client_id=cid", "code_challenge=ch",
        "code_challenge_method=S256", "state=st", "scope=trade",
    ):
        assert frag in url
    assert "resource=https%3A%2F%2Fagent.robinhood.com%2Fmcp%2Ftrading" in url


def test_wellknown_candidates_path_aware_first():
    cands = ro._wellknown_candidates(MCP_URL, "oauth-protected-resource")
    assert cands[0] == f"{ORIGIN}/.well-known/oauth-protected-resource/mcp/trading"
    assert f"{ORIGIN}/.well-known/oauth-protected-resource" in cands


# --- Discovery --------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_via_www_authenticate_challenge():
    prm_url = f"{ORIGIN}/.well-known/oauth-protected-resource/mcp/trading"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and str(request.url) == MCP_URL:
            return httpx.Response(
                401,
                headers={"WWW-Authenticate": f'Bearer resource_metadata="{prm_url}"'},
            )
        if str(request.url) == prm_url:
            return httpx.Response(
                200,
                json={"resource": MCP_URL, "authorization_servers": [ORIGIN]},
            )
        if str(request.url) == f"{ORIGIN}/.well-known/oauth-authorization-server":
            return httpx.Response(200, json={
                "issuer": ORIGIN,
                "authorization_endpoint": AUTH_EP,
                "token_endpoint": TOKEN_EP,
                "registration_endpoint": REG_EP,
                "scopes_supported": ["trade", "read"],
            })
        return httpx.Response(404)

    async with _client(handler) as http:
        meta = await ro.discover_metadata(MCP_URL, http=http)
    assert meta["authorization_endpoint"] == AUTH_EP
    assert meta["token_endpoint"] == TOKEN_EP
    assert meta["registration_endpoint"] == REG_EP
    assert meta["resource"] == MCP_URL


@pytest.mark.asyncio
async def test_discover_falls_back_to_wellknown_when_no_challenge():
    """No 401 hint → walk the conventional well-known paths."""
    def handler(request: httpx.Request) -> httpx.Response:
        u = str(request.url)
        if request.method == "POST" and u == MCP_URL:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 0, "result": {}})
        if u == f"{ORIGIN}/.well-known/oauth-protected-resource/mcp/trading":
            return httpx.Response(200, json={"authorization_servers": [ORIGIN]})
        if u == f"{ORIGIN}/.well-known/oauth-authorization-server":
            return httpx.Response(200, json={
                "issuer": ORIGIN,
                "authorization_endpoint": AUTH_EP,
                "token_endpoint": TOKEN_EP,
            })
        return httpx.Response(404)

    async with _client(handler) as http:
        meta = await ro.discover_metadata(MCP_URL, http=http)
    assert meta["token_endpoint"] == TOKEN_EP
    assert meta["registration_endpoint"] is None


@pytest.mark.asyncio
async def test_discover_raises_when_nothing_found():
    async with _client(lambda r: httpx.Response(404)) as http:
        with pytest.raises(RuntimeError):
            await ro.discover_metadata(MCP_URL, http=http)


# --- Dynamic Client Registration --------------------------------------------


@pytest.mark.asyncio
async def test_register_client_returns_client_id():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json as _json
        seen["body"] = _json.loads(request.content)
        return httpx.Response(201, json={"client_id": "abc123"})

    async with _client(handler) as http:
        out = await ro.register_client(REG_EP, "http://localhost:8000/cb", http=http)
    assert out["client_id"] == "abc123"
    assert out["client_secret"] is None
    assert seen["body"]["redirect_uris"] == ["http://localhost:8000/cb"]
    assert seen["body"]["token_endpoint_auth_method"] == "none"
    assert set(seen["body"]["grant_types"]) == {"authorization_code", "refresh_token"}


@pytest.mark.asyncio
async def test_register_client_raises_on_error():
    async with _client(lambda r: httpx.Response(400, text="bad")) as http:
        with pytest.raises(RuntimeError):
            await ro.register_client(REG_EP, "http://localhost:8000/cb", http=http)


# --- Token exchange / refresh -----------------------------------------------


@pytest.mark.asyncio
async def test_exchange_code_posts_form_with_resource_and_verifier():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ctype"] = request.headers["content-type"]
        seen["body"] = request.content.decode()
        return httpx.Response(200, json={
            "access_token": "at", "refresh_token": "rt", "expires_in": 1800,
        })

    async with _client(handler) as http:
        out = await ro.exchange_code(
            token_endpoint=TOKEN_EP, client_id="cid", client_secret=None,
            code="thecode", redirect_uri="http://localhost:8000/cb",
            code_verifier="ver", resource=MCP_URL, http=http,
        )
    assert out == {"access_token": "at", "refresh_token": "rt",
                   "expires_in": 1800, "scope": ""}
    assert "application/x-www-form-urlencoded" in seen["ctype"]
    assert "grant_type=authorization_code" in seen["body"]
    assert "code_verifier=ver" in seen["body"]
    assert "resource=" in seen["body"]


@pytest.mark.asyncio
async def test_refresh_keeps_old_refresh_token_when_server_omits_it():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "new_at", "expires_in": 3600})

    async with _client(handler) as http:
        out = await ro.refresh_tokens(
            token_endpoint=TOKEN_EP, client_id="cid", client_secret=None,
            refresh_token="old_rt", resource=MCP_URL, http=http,
        )
    assert out["access_token"] == "new_at"
    assert out["refresh_token"] == "old_rt"  # carried over


# --- get_valid_robinhood_token orchestrator ---------------------------------


class _DisabledCache:
    enabled = False
    client = None


@pytest.fixture(autouse=True)
def _no_redis(monkeypatch):
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: _DisabledCache()
    )


@pytest.mark.asyncio
async def test_get_valid_token_returns_unexpired_without_refresh(monkeypatch):
    future = datetime.now(timezone.utc) + timedelta(hours=1)

    async def fake_get(uid, wid):
        return {"status": "connected", "access_token": "live", "refresh_token": "rt",
                "expires_at": future, "token_endpoint": TOKEN_EP, "client_id": "c",
                "resource": MCP_URL}

    called = {"refresh": False}

    async def fake_refresh(**kw):
        called["refresh"] = True
        return {}

    monkeypatch.setattr(ro, "refresh_tokens", fake_refresh)
    monkeypatch.setattr("src.server.database.robinhood_oauth.get", fake_get)

    tok = await ro.get_valid_robinhood_token("u", "w")
    assert tok == "live"
    assert called["refresh"] is False


@pytest.mark.asyncio
async def test_get_valid_token_refreshes_when_expired_and_syncs_vault(monkeypatch):
    past = datetime.now(timezone.utc) - timedelta(minutes=1)
    state: dict = {}

    async def fake_get(uid, wid):
        return {"status": "connected", "access_token": "stale", "refresh_token": "rt",
                "expires_at": past, "token_endpoint": TOKEN_EP, "client_id": "c",
                "client_secret": None, "resource": MCP_URL}

    async def fake_refresh(**kw):
        return {"access_token": "fresh_at", "refresh_token": "rt2", "expires_in": 3600}

    async def fake_set(**kw):
        state["set"] = kw

    async def fake_sync(wid, token):
        state["synced"] = token

    monkeypatch.setattr(ro, "refresh_tokens", fake_refresh)
    monkeypatch.setattr("src.server.database.robinhood_oauth.get", fake_get)
    monkeypatch.setattr("src.server.database.robinhood_oauth.set_tokens", fake_set)
    monkeypatch.setattr(ro, "_sync_vault_token", fake_sync)

    tok = await ro.get_valid_robinhood_token("u", "w")
    assert tok == "fresh_at"
    assert state["set"]["access_token"] == "fresh_at"
    assert state["set"]["refresh_token"] == "rt2"
    assert state["synced"] == "fresh_at"


@pytest.mark.asyncio
async def test_get_valid_token_none_when_not_connected(monkeypatch):
    async def fake_get(uid, wid):
        return {"status": "pending", "access_token": "", "refresh_token": ""}

    monkeypatch.setattr("src.server.database.robinhood_oauth.get", fake_get)
    assert await ro.get_valid_robinhood_token("u", "w") is None
