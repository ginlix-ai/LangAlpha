"""Unit tests for the two-phase MCP OAuth connect flow.

Phase 1 (``start_connect``) parks a state + PKCE record in Redis and hands the
browser an authorize URL; phase 2 (``complete_callback``) claims that record,
exchanges the code, and answers with a relative redirect. Three properties
carry the flow's safety, and each gets its own coverage here:

- the state record is **claimed exactly once** — the claim is get-and-delete in
  one MULTI/EXEC step, so a replayed (or concurrent) callback loses;
- the PKCE verifier parked in phase 1 is the one presented at the token
  endpoint, and the authorize URL carries its S256 challenge (recomputed here
  rather than read back from the implementation);
- every user-visible outcome lands on an allowlisted **relative** path, with
  the exact ``?mcp_error=`` / ``?mcp_connected=`` vocabulary the UI reads.

Every network seam (discovery, DCR, token exchange) and the SSRF pin is
monkeypatched: nothing here touches Redis, Postgres, or the network.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import httpx2
import pytest
from mcp.shared.auth import (
    OAuthClientInformationFull,
    OAuthClientMetadata,
    OAuthMetadata,
)

import src.server.app.mcp_servers as mcp_servers_mod
from src.server.services.mcp_oauth import connect, redirects, tokens
from src.server.services.mcp_oauth.connect import (
    STATE_TTL_SECONDS,
    McpOAuthError,
    McpServerMoved,
    StartedConnect,
    complete_callback,
    start_connect,
)
from src.server.services.mcp_oauth.http import OAuthHopBlocked
from src.server.services.mcp_oauth.redirects import (
    DEFAULT_RETURN_TO,
    callback_uri,
    sanitize_loopback_redirect,
    sanitize_return_to,
    sanitize_web_origin,
)
from src.server.database.mcp_oauth import ConnectionStatus
from src.server.utils.egress_guard import EgressBlockedError, PinnedTarget

USER_ID = "user-connect-1"
# The space is deliberate: it proves the redirect percent-encodes the name.
SERVER_NAME = "demo notes"
SERVER_NAME_Q = "demo%20notes"
SERVER_URL = "https://mcp.demo.test/mcp"
ISSUER = "https://auth.demo.test"
AUTH_HOST = "auth.demo.test"
STATE_PREFIX = "mcp:oauth:state:"
INFLIGHT_PREFIX = "mcp:oauth:inflight:"
# Two addresses on the one host a shipped brokerage answers on, because host is
# what joins a row to a vendor: the row is the user's to edit once it exists,
# and a sibling path is still that vendor.
IBKR_URL = "https://api.ibkr.com/v1/api/mcp-public"
IBKR_ALT_URL = "https://api.ibkr.com/v1/api/mcp-public/mine"
# A vendor we do not ship, which is the control for every case below rather
# than a second example of one. Neither shipped brokerage can play the part --
# both are one connection per account -- so the ordinary case has to come from
# a host the registry has never heard of.
ORDINARY_URL = "https://mcp.example.com/mcp"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakePipeline:
    """MULTI/EXEC stand-in: queued commands apply as one indivisible step.

    ``execute`` awaits once (the round trip a real client would make) and then
    applies every queued command with no further await point — which is
    precisely the property that makes the state claim single-use under
    concurrency.
    """

    def __init__(self, redis: "FakeRedis"):
        self._redis = redis
        self._queued: list[tuple[str, str, str | None]] = []

    async def __aenter__(self) -> "_FakePipeline":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    def get(self, key: str) -> "_FakePipeline":
        self._queued.append(("get", key, None))
        return self

    def set(self, key: str, value: str, *, ex=None) -> "_FakePipeline":
        self._redis.set_calls.append({"key": key, "nx": False, "ex": ex})
        self._queued.append(("set", key, value))
        return self

    def delete(self, key: str) -> "_FakePipeline":
        self._queued.append(("delete", key, None))
        return self

    async def execute(self) -> list:
        queued, self._queued = self._queued, []
        await asyncio.sleep(0)
        results: list = []
        for op, key, value in queued:
            if op == "get":
                results.append(self._redis.store.get(key))
            elif op == "set":
                self._redis.store[key] = value.encode()
                results.append(True)
            else:
                results.append(int(self._redis.store.pop(key, None) is not None))
        return results


class FakeRedis:
    """Values are bytes: the real client is built with ``decode_responses=False``."""

    def __init__(self, *, nx_always_loses: bool = False):
        self.store: dict[str, bytes] = {}
        self.set_calls: list[dict] = []
        self._nx_always_loses = nx_always_loses

    async def set(self, key, value, *, nx=False, ex=None):
        await asyncio.sleep(0)
        self.set_calls.append({"key": key, "nx": nx, "ex": ex})
        if nx and (self._nx_always_loses or key in self.store):
            return None
        self.store[key] = value.encode()
        return True

    async def get(self, key) -> bytes | None:
        await asyncio.sleep(0)
        return self.store.get(key)

    async def delete(self, key) -> int:
        await asyncio.sleep(0)
        return int(self.store.pop(key, None) is not None)

    def pipeline(self, transaction=True):
        assert transaction, "the state claim must run inside MULTI/EXEC"
        return _FakePipeline(self)

    # -- test helpers -------------------------------------------------------

    def states(self) -> dict[str, bytes]:
        """Just the parked flows — the in-flight markers outlive them on purpose."""
        return {k: v for k, v in self.store.items() if k.startswith(STATE_PREFIX)}

    def only_record(self) -> dict:
        [raw] = list(self.states().values())
        return json.loads(raw)

    def park(self, state: str, record: dict) -> None:
        self.store[f"{STATE_PREFIX}{state}"] = json.dumps(record).encode()


@asynccontextmanager
async def _fake_http_client():
    yield SimpleNamespace(name="fake-oauth-client")


def _as_metadata(**overrides) -> OAuthMetadata:
    data = {
        "issuer": ISSUER,
        "authorization_endpoint": f"{ISSUER}/authorize",
        "token_endpoint": f"{ISSUER}/token",
        "registration_endpoint": f"{ISSUER}/register",
        "scopes_supported": ["notes.read", "offline_access"],
        "code_challenge_methods_supported": ["S256"],
    }
    data.update(overrides)
    return OAuthMetadata.model_validate(data)


def _client_info(**overrides) -> OAuthClientInformationFull:
    data = {
        "client_id": "client-abc123",
        "client_name": "Langalpha",
        "redirect_uris": [callback_uri()],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    }
    data.update(overrides)
    return OAuthClientInformationFull.model_validate(data)


def _registration_response(status_code: int, body: dict | None = None):
    """Only what ``_register_client`` reads off a registration response.

    It reads the body to decide whether the server is telling us the metadata
    was the problem, which is the only refusal a smaller second request can
    answer. ``aread`` both drains and returns, as httpx's does.
    """
    raw = json.dumps(body).encode() if body is not None else b""
    return SimpleNamespace(status_code=status_code, aread=AsyncMock(return_value=raw))


def _token_payload(**overrides) -> dict:
    payload = {
        "access_token": "access-fresh",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "refresh-fresh",
        "scope": "notes.read offline_access",
    }
    payload.update(overrides)
    return payload


def _s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _query(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


async def _callback(started: StartedConnect, **kwargs) -> str:
    """Phase 2 as the initiating browser drives it.

    The real browser presents back the HttpOnly nonce cookie minted in phase 1,
    so the round trip carries ``started.browser_nonce`` by default. Tests
    exercising the CSRF guard override ``browser_nonce`` (or ``state``).
    """
    kwargs.setdefault("state", started.state)
    kwargs.setdefault("browser_nonce", started.browser_nonce)
    return await complete_callback(**kwargs)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def redis(monkeypatch) -> FakeRedis:
    fake = FakeRedis()
    cache = SimpleNamespace(enabled=True, client=fake)
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: cache
    )
    return fake


@pytest.fixture
def phase1(monkeypatch) -> SimpleNamespace:
    """Patch every phase-1 seam; the returned handle tweaks the scenario."""
    env = SimpleNamespace(
        as_metadata=_as_metadata(),
        client_info=_client_info(),
        prm=None,
        www_scope=None,
        catalog_row={"name": SERVER_NAME, "url": SERVER_URL, "transport": "http"},
        pinned=[],
        pin_error=None,
    )

    async def _discover(client, server_url):
        assert server_url == SERVER_URL
        return env.prm, env.as_metadata, ISSUER, env.www_scope

    async def _register_client(client, **kwargs):
        return connect._Registration(env.client_info)

    async def _get_catalog_server(user_id, name):
        return env.catalog_row

    async def _pin(url, *, require_https=True, allow_non_global=False):
        env.pinned.append({"url": url, "require_https": require_https})
        if env.pin_error is not None:
            raise env.pin_error
        return PinnedTarget(
            url=url, host=AUTH_HOST, ip="203.0.113.10", authority=AUTH_HOST
        )

    monkeypatch.setattr(connect, "_discover", _discover)
    monkeypatch.setattr(connect, "_register_client", _register_client)
    monkeypatch.setattr(connect, "get_catalog_server", _get_catalog_server)
    monkeypatch.setattr(connect, "oauth_http_client", _fake_http_client)
    monkeypatch.setattr("src.server.utils.egress_guard.pin_public_url", _pin)
    return env


@pytest.fixture
def phase2(monkeypatch) -> SimpleNamespace:
    """Patch every phase-2 seam; the returned handle records what was sent."""
    env = SimpleNamespace(
        requests=[],
        upserts=[],
        bumps=[],
        discoveries=[],
        applies=[],
        consented=[],
        # The connection each write was handed, and how the block wrapping them
        # ended -- together these are what "one transaction" means here.
        write_conns=[],
        transactions=[],
        # The write that installs the consent on live grants, and what it does
        # when the database will not take it.
        consent_error=None,
        running_workspaces=["ws-warm-1"],
        status_code=200,
        payload=_token_payload(),
        raises=None,
        discovery_error=None,
        # Runs inside the token exchange, for the races whose whole point is
        # that they happen while this request is in the air.
        on_request=None,
    )

    async def _pinned_request(client, method, url, *, headers=None, data=None, content=None):
        env.requests.append(
            {"method": method, "url": url, "headers": headers, "data": data}
        )
        if env.on_request is not None:
            await env.on_request(env.requests[-1])
        if env.raises is not None:
            raise env.raises
        return httpx2.Response(env.status_code, json=env.payload)

    class _Txn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            env.transactions.append("rollback" if exc_type is not None else "commit")
            return False

    class _Conn:
        def transaction(self):
            return _Txn()

    @asynccontextmanager
    async def _get_db_connection(conn=None):
        yield conn if conn is not None else _Conn()

    async def _upsert_connection(user_id, server_name, *, conn=None, **kwargs):
        env.upserts.append({"user_id": user_id, "server_name": server_name, **kwargs})
        env.write_conns.append(conn)
        return "connection-1"

    async def _bump(user_id):
        env.bumps.append(user_id)

    async def _apply_consent(connection_id, *, conn=None):
        env.consented.append(connection_id)
        env.write_conns.append(conn)
        if env.consent_error is not None:
            raise env.consent_error
        return 1

    async def _refresh_user_tool_schemas(user_id, server_name):
        env.discoveries.append((user_id, server_name))
        if env.discovery_error is not None:
            raise env.discovery_error

    async def _running_workspaces(user_id):
        return list(env.running_workspaces)

    def _schedule_proactive_apply(workspace_id, user_id):
        env.applies.append((workspace_id, user_id))

    # The token POST lives in mcp_oauth.tokens — the one place both the code
    # exchange and the refresh go through.
    monkeypatch.setattr(tokens, "pinned_request", _pinned_request)
    monkeypatch.setattr(tokens, "oauth_http_client", _fake_http_client)
    monkeypatch.setattr(connect, "upsert_connection", _upsert_connection)
    monkeypatch.setattr(connect, "get_db_connection", _get_db_connection)
    monkeypatch.setattr(connect, "bump_user_workspaces_mcp_version", _bump)
    # Not optional. Left real, every phase-2 test below reached the live pool
    # and raised -- which the callback used to swallow, so the whole file went
    # on passing while never once exercising the write it was swallowing.
    monkeypatch.setattr(connect, "apply_consent_to_active_grants", _apply_consent)
    monkeypatch.setattr(
        "src.server.services.mcp_oauth.discovery.refresh_user_tool_schemas",
        _refresh_user_tool_schemas,
    )
    monkeypatch.setattr(
        connect, "get_running_workspace_ids_for_user", _running_workspaces
    )
    monkeypatch.setattr(
        mcp_servers_mod, "_schedule_proactive_apply", _schedule_proactive_apply
    )
    return env


# ---------------------------------------------------------------------------
# Phase 1 — the parked state record
# ---------------------------------------------------------------------------


class TestStartConnect:
    @pytest.mark.asyncio
    async def test_parks_a_single_use_ttl_bounded_state_record(self, redis, phase1):
        result = await start_connect(USER_ID, SERVER_NAME)

        [call] = [c for c in redis.set_calls if c["key"].startswith(STATE_PREFIX)]
        assert call["key"] == f"{STATE_PREFIX}{result.state}"
        # nx: the state key is claimed, never overwritten. ex: it self-expires.
        assert call["nx"] is True
        assert call["ex"] == STATE_TTL_SECONDS

        record = redis.only_record()
        assert record["user_id"] == USER_ID
        assert record["server_name"] == SERVER_NAME
        assert record["server_url"] == SERVER_URL
        assert record["issuer"] == str(phase1.as_metadata.issuer)
        assert record["token_endpoint"] == str(phase1.as_metadata.token_endpoint)
        assert record["redirect_uri"] == callback_uri()
        assert record["return_to"] == DEFAULT_RETURN_TO

    @pytest.mark.asyncio
    async def test_authorize_url_carries_the_s256_challenge_of_the_parked_verifier(
        self, redis, phase1
    ):
        result = await start_connect(USER_ID, SERVER_NAME)

        params = _query(result.authorize_url)
        verifier = redis.only_record()["code_verifier"]

        assert params["code_challenge_method"] == "S256"
        # Recomputed here — never read back from the implementation.
        assert params["code_challenge"] == _s256(verifier)
        assert params["code_challenge"] != verifier
        assert params["state"] == result.state
        assert params["response_type"] == "code"
        assert params["client_id"] == "client-abc123"
        assert params["redirect_uri"] == callback_uri()
        assert result.authorize_url.startswith(f"{ISSUER}/authorize?")

    @pytest.mark.asyncio
    async def test_an_endpoints_own_query_survives_the_merge(self, redis, phase1):
        # RFC 6749 §3.1: the authorization endpoint may publish a query, and it
        # must be retained. Naive concatenation would emit a second '?'.
        phase1.as_metadata = _as_metadata(
            authorization_endpoint=f"{ISSUER}/authorize?tenant=acme&ui=dark"
        )

        result = await start_connect(USER_ID, SERVER_NAME)

        assert result.authorize_url.count("?") == 1
        params = _query(result.authorize_url)
        assert params["tenant"] == "acme"
        assert params["ui"] == "dark"
        assert params["response_type"] == "code"
        assert params["state"] == result.state

    @pytest.mark.asyncio
    async def test_our_parameters_win_a_collision_with_the_endpoints_own(
        self, redis, phase1
    ):
        # A published `state`/`redirect_uri` must not survive alongside ours —
        # duplicates make the AS's choice undefined, and the wrong one breaks
        # the callback.
        phase1.as_metadata = _as_metadata(
            authorization_endpoint=(
                f"{ISSUER}/authorize?state=stale&redirect_uri=https://evil.test"
                "&tenant=acme"
            )
        )

        result = await start_connect(USER_ID, SERVER_NAME)

        appearing = parse_qs(urlsplit(result.authorize_url).query)
        assert appearing["state"] == [result.state]
        assert appearing["redirect_uri"] == [callback_uri()]
        # A non-colliding one is untouched.
        assert appearing["tenant"] == ["acme"]

    @pytest.mark.asyncio
    async def test_offline_access_asks_for_explicit_consent(self, redis, phase1):
        # AS scopes_supported carries offline_access, so the durable-grant
        # prompt must be requested.
        params = _query((await start_connect(USER_ID, SERVER_NAME)).authorize_url)

        assert params["scope"] == "notes.read offline_access"
        assert params["prompt"] == "consent"

    @pytest.mark.asyncio
    async def test_no_consent_prompt_without_offline_access(self, redis, phase1):
        phase1.as_metadata = _as_metadata(scopes_supported=["notes.read"])

        params = _query((await start_connect(USER_ID, SERVER_NAME)).authorize_url)

        assert params["scope"] == "notes.read"
        assert "prompt" not in params

    @pytest.mark.asyncio
    async def test_authorization_endpoint_is_pinned_public_https(self, redis, phase1):
        await start_connect(USER_ID, SERVER_NAME)

        assert phase1.pinned == [
            {"url": f"{ISSUER}/authorize", "require_https": True}
        ]

    @pytest.mark.asyncio
    async def test_refuses_a_non_public_authorization_endpoint(self, redis, phase1):
        phase1.pin_error = EgressBlockedError("resolves to a non-global address")

        with pytest.raises(McpOAuthError, match="Refusing authorization endpoint"):
            await start_connect(USER_ID, SERVER_NAME)

        # Nothing is parked for a flow that never produced an authorize URL.
        assert redis.store == {}

    @pytest.mark.asyncio
    async def test_state_collision_is_refused(self, monkeypatch, phase1):
        colliding = FakeRedis(nx_always_loses=True)
        cache = SimpleNamespace(enabled=True, client=colliding)
        monkeypatch.setattr(
            "src.utils.cache.redis_cache.get_cache_client", lambda: cache
        )

        with pytest.raises(McpOAuthError, match="state collision"):
            await start_connect(USER_ID, SERVER_NAME)

    @pytest.mark.asyncio
    async def test_requires_a_known_remote_http_server(self, redis, phase1):
        phase1.catalog_row = None
        with pytest.raises(McpOAuthError, match="not found"):
            await start_connect(USER_ID, SERVER_NAME)

        phase1.catalog_row = {"name": SERVER_NAME, "transport": "stdio", "url": None}
        with pytest.raises(McpOAuthError, match="remote"):
            await start_connect(USER_ID, SERVER_NAME)

        assert redis.store == {}


# ---------------------------------------------------------------------------
# Round trip — phase 1 parks, phase 2 consumes
# ---------------------------------------------------------------------------


class TestRegistrationReuse:
    """_register_client reuses a stored DCR registration only while it fits —
    same issuer AND the registration still covers the redirect_uri we send."""

    def _existing(self, client_info: OAuthClientInformationFull) -> SimpleNamespace:
        return SimpleNamespace(
            client_info=client_info.model_dump(mode="json", exclude_none=True),
            client_secret="sec-1",
            as_metadata={"issuer": str(_as_metadata().issuer)},
        )

    def _metadata_for(
        self, redirect: str, scope: str | None = None
    ) -> OAuthClientMetadata:
        return OAuthClientMetadata.model_validate(
            {
                "client_name": "LangAlpha",
                "redirect_uris": [redirect],
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "token_endpoint_auth_method": "none",
                **({"scope": scope} if scope else {}),
            }
        )

    @pytest.mark.asyncio
    async def test_reuses_while_issuer_and_redirect_still_fit(self, monkeypatch):
        stored = _client_info()
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=self._existing(stored))
        )
        send = AsyncMock(side_effect=AssertionError("re-registered"))
        monkeypatch.setattr(connect, "pinned_send", send)

        result, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(callback_uri()),
            auth_base_url=ISSUER,
        )

        assert result.client_id == stored.client_id
        assert result.client_secret == "sec-1"
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_re_registers_when_the_redirect_moved(self, monkeypatch):
        """A SERVER_BASE_URL change leaves the stored registration carrying the
        old callback URL; reusing it would have the AS reject every authorize
        request with no in-product path back."""
        stored = _client_info(redirect_uris=["https://old.example.test/oauth/cb"])
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=self._existing(stored))
        )
        fresh = _client_info(client_id="client-fresh")
        monkeypatch.setattr(
            connect, "create_client_registration_request", lambda *a: object()
        )
        monkeypatch.setattr(
            connect, "pinned_send", AsyncMock(return_value=_registration_response(201))
        )
        monkeypatch.setattr(
            connect, "handle_registration_response", AsyncMock(return_value=fresh)
        )

        result, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(callback_uri()),
            auth_base_url=ISSUER,
        )

        assert result.client_id == "client-fresh"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "refusal",
        [None, {"error": "invalid_client_metadata"}],
        ids=["an opaque 400", "the code RFC 7591 names"],
    )
    async def test_refused_metadata_retries_with_the_interoperable_core(
        self, monkeypatch, refusal
    ):
        """An AS that refuses our optional metadata must not cost us the connect.

        Coinbase rejects `application_type` at either value and refuses every
        write scope, answering with one opaque `invalid_client_metadata` for the
        whole body. There is nothing to negotiate against, so the retry drops
        everything optional at once rather than guessing a member.

        Both refusals, because they reach the retry down different branches: the
        named code is the one the RFC defines, and a bare 400 is the fallback
        for a server that refuses our optional members without saying so.
        """
        monkeypatch.setattr(connect, "get_connection", AsyncMock(return_value=None))
        sent: list[dict] = []

        async def send(_client, request):
            sent.append(json.loads(request.content.decode()))
            if len(sent) == 1:
                return _registration_response(400, refusal)
            return _registration_response(201)

        monkeypatch.setattr(connect, "pinned_send", send)
        fresh = _client_info(client_id="client-fresh")
        monkeypatch.setattr(
            connect, "handle_registration_response", AsyncMock(return_value=fresh)
        )

        result, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(callback_uri(), scope="a:read a:write"),
            auth_base_url=ISSUER,
        )

        assert result.client_id == "client-fresh"
        assert len(sent) == 2, "asked twice, never a third time"
        # The first ask carries what the SDK builds, including the two members
        # a strict AS refuses.
        assert sent[0]["application_type"] == "native"
        assert sent[0]["scope"] == "a:read a:write"
        # The retry carries neither, and still identifies the client and where
        # it is to be sent back.
        assert "application_type" not in sent[1]
        assert "scope" not in sent[1]
        assert sent[1]["client_name"] == sent[0]["client_name"]
        assert sent[1]["redirect_uris"] == [callback_uri()]
        assert sent[1]["grant_types"] == ["authorization_code", "refresh_token"]
        assert sent[1]["response_types"] == ["code"]
        assert sent[1]["token_endpoint_auth_method"] == "none"


    @pytest.mark.asyncio
    async def test_re_registers_when_the_stored_scope_is_too_narrow(
        self, monkeypatch
    ):
        """A stored registration's scope wins over the one we just computed.

        ``effective_scope`` is ``client_info.scope or scope``, so reusing a
        registration made before we started asking for ``offline_access`` keeps
        asking without it: the connect succeeds, the token has no refresh half,
        and the connection dies at the first expiry with nothing saying why.
        """
        stored = _client_info(scope="notes.read")
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=self._existing(stored))
        )
        fresh = _client_info(client_id="client-fresh", scope="notes.read offline_access")
        monkeypatch.setattr(
            connect, "create_client_registration_request", lambda *a: object()
        )
        monkeypatch.setattr(
            connect, "pinned_send", AsyncMock(return_value=_registration_response(201))
        )
        monkeypatch.setattr(
            connect, "handle_registration_response", AsyncMock(return_value=fresh)
        )

        result, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(
                callback_uri(), scope="notes.read offline_access"
            ),
            auth_base_url=ISSUER,
        )

        assert result.client_id == "client-fresh"

    @pytest.mark.asyncio
    async def test_reuses_a_registration_that_recorded_no_scope(self, monkeypatch):
        """An empty stored scope overrides nothing, so it is fine to reuse.

        Plenty of servers echo no ``scope`` back at all. Treating that as a
        narrowing would re-register on every single connect.
        """
        stored = _client_info()
        assert stored.scope is None
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=self._existing(stored))
        )
        send = AsyncMock(side_effect=AssertionError("re-registered"))
        monkeypatch.setattr(connect, "pinned_send", send)

        result, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(
                callback_uri(), scope="notes.read offline_access"
            ),
            auth_base_url=ISSUER,
        )

        assert result.client_id == stored.client_id
        send.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status,body",
        [
            (429, {"error": "too_many_requests"}),
            (500, None),
            (400, {"error": "invalid_client_id"}),
        ],
    )
    async def test_a_refusal_that_is_not_about_our_metadata_is_not_retried(
        self, monkeypatch, status, body
    ):
        """Retrying every non-2xx made two different failures worse.

        A 429 was answered by immediately spending a second request against the
        limit that produced it. A 5xx raised after the client was already
        created registered a second one and orphaned the first. Neither is
        fixed by sending less metadata, and the real status is what the caller
        needs to see.
        """
        monkeypatch.setattr(connect, "get_connection", AsyncMock(return_value=None))
        sent: list[dict] = []

        async def send(_client, request):
            sent.append(json.loads(request.content.decode()))
            return _registration_response(status, body)

        monkeypatch.setattr(connect, "pinned_send", send)
        raised = AsyncMock(side_effect=RuntimeError(f"registration failed: {status}"))
        monkeypatch.setattr(connect, "handle_registration_response", raised)

        with pytest.raises(RuntimeError, match=str(status)):
            await connect._register_client(
                object(),
                user_id=USER_ID,
                server_name=SERVER_NAME,
                as_metadata=_as_metadata(),
                client_metadata=self._metadata_for(callback_uri()),
                auth_base_url=ISSUER,
            )

        assert len(sent) == 1, "asked once, and reported what came back"


    @pytest.mark.asyncio
    async def test_a_reused_registration_authorizes_with_the_current_scope(
        self, monkeypatch
    ):
        """The widening half of the scope bug the check above closes.

        A registration that covers more than we now ask for is reusable -- and
        ``effective_scope`` used to be ``client_info.scope or scope``, so
        reusing it sent the whole stored scope and re-requested exactly the
        permissions the current metadata had stopped asking for. Reuse the
        registration, authorize with today's scope.

        The narrowing rides beside the registration and never into it. The
        registration is persisted, so writing today's subset over its scope
        makes the next connect read the client as registered for less than it
        is, and re-register the moment we ask for anything the original client
        already covered.
        """
        stored = _client_info(scope="notes.read notes.write admin offline_access")
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=self._existing(stored))
        )
        send = AsyncMock(side_effect=AssertionError("re-registered"))
        monkeypatch.setattr(connect, "pinned_send", send)

        result, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(
                callback_uri(), scope="notes.read offline_access"
            ),
            auth_base_url=ISSUER,
        )

        assert result.client_id == stored.client_id
        send.assert_not_awaited()
        assert authorize_scope == "notes.read offline_access"
        assert "admin" not in (authorize_scope or "")
        assert result.scope == "notes.read notes.write admin offline_access"

    @pytest.mark.asyncio
    async def test_a_narrowed_connect_does_not_cost_the_next_one_a_registration(
        self, monkeypatch
    ):
        """The round trip, which is where the damage used to show up.

        Phase 1 persists whatever this returns as ``client_info``, so the run
        that asked for less is what the *following* run reads as the client's
        registered scope. With the narrowing written into the registration, the
        moment the metadata asked for ``admin`` again the coverage check failed
        and DCR ran, orphaning a client at the provider that already covered
        the request.
        """
        stored = _client_info(scope="notes.read notes.write admin offline_access")
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=self._existing(stored))
        )
        monkeypatch.setattr(
            connect, "pinned_send", AsyncMock(side_effect=AssertionError("re-registered"))
        )

        narrowed, _ = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(
                callback_uri(), scope="notes.read offline_access"
            ),
            auth_base_url=ISSUER,
        )

        # Exactly what phase 1 stores, so the next connect reads what a real
        # one would.
        persisted = SimpleNamespace(
            client_info=narrowed.model_dump(
                mode="json", exclude_none=True, exclude={"client_secret"}
            ),
            client_secret="sec-1",
            as_metadata={"issuer": str(_as_metadata().issuer)},
        )
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=persisted)
        )

        again, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(
                callback_uri(), scope="notes.read notes.write admin offline_access"
            ),
            auth_base_url=ISSUER,
        )

        assert again.client_id == stored.client_id
        assert authorize_scope == "notes.read notes.write admin offline_access"

    @pytest.mark.asyncio
    async def test_a_stored_metadata_document_is_never_reused_as_a_registration(
        self, monkeypatch
    ):
        """CIMD ids and DCR ids share one column; only one is a registration.

        A server that advertised a client metadata document once and has since
        stopped asks for DCR now. Its stored URL client_id passes the issuer,
        redirect and scope checks trivially, so it was reused -- and the
        authorize request then carried an identity the server no longer
        accepts, with nothing registered to fall back to.
        """
        stored = _client_info(client_id="https://app.example.test/client.json")
        monkeypatch.setattr(
            connect, "get_connection", AsyncMock(return_value=self._existing(stored))
        )
        monkeypatch.setattr(
            connect, "create_client_registration_request", lambda *a: object()
        )
        monkeypatch.setattr(
            connect, "pinned_send", AsyncMock(return_value=_registration_response(201))
        )
        fresh = _client_info(client_id="client-fresh")
        monkeypatch.setattr(
            connect, "handle_registration_response", AsyncMock(return_value=fresh)
        )

        result, authorize_scope = await connect._register_client(
            object(),
            user_id=USER_ID,
            server_name=SERVER_NAME,
            as_metadata=_as_metadata(),
            client_metadata=self._metadata_for(callback_uri()),
            auth_base_url=ISSUER,
        )

        assert result.client_id == "client-fresh"

    @pytest.mark.asyncio
    async def test_a_rejected_redirect_uri_is_not_retried(self, monkeypatch):
        """The smaller body carries the same redirect_uris as the larger one.

        So the retry asks the identical question, spends a second registration
        attempt, and is told the same thing. The AS named what it rejected;
        that is the answer worth surfacing.
        """
        monkeypatch.setattr(connect, "get_connection", AsyncMock(return_value=None))
        sent: list[dict] = []

        async def send(_client, request):
            sent.append(json.loads(request.content.decode()))
            return _registration_response(400, {"error": "invalid_redirect_uri"})

        monkeypatch.setattr(connect, "pinned_send", send)
        raised = AsyncMock(side_effect=RuntimeError("registration failed: 400"))
        monkeypatch.setattr(connect, "handle_registration_response", raised)

        with pytest.raises(RuntimeError, match="400"):
            await connect._register_client(
                object(),
                user_id=USER_ID,
                server_name=SERVER_NAME,
                as_metadata=_as_metadata(),
                client_metadata=self._metadata_for(callback_uri()),
                auth_base_url=ISSUER,
            )

        assert len(sent) == 1


class TestRoundTrip:
    @pytest.mark.asyncio
    async def test_parked_verifier_is_the_one_presented_at_the_token_endpoint(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)
        verifier = redis.only_record()["code_verifier"]
        challenge = _query(started.authorize_url)["code_challenge"]

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        [exchange] = phase2.requests
        assert exchange["method"] == "POST"
        assert exchange["url"] == f"{ISSUER}/token"
        assert exchange["data"]["grant_type"] == "authorization_code"
        assert exchange["data"]["code"] == "auth-code-1"
        assert exchange["data"]["client_id"] == "client-abc123"
        assert exchange["data"]["redirect_uri"] == callback_uri()
        # The pairing that makes PKCE worth anything.
        assert exchange["data"]["code_verifier"] == verifier
        assert _s256(exchange["data"]["code_verifier"]) == challenge

    @pytest.mark.asyncio
    async def test_success_persists_the_bundle_and_fans_out(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)

        await _callback(started, code="auth-code-1")

        [upsert] = phase2.upserts
        assert upsert["user_id"] == USER_ID
        assert upsert["server_name"] == SERVER_NAME
        assert upsert["server_url"] == SERVER_URL
        assert upsert["access_token"] == "access-fresh"
        assert upsert["refresh_token"] == "refresh-fresh"
        assert upsert["token_type"] == "Bearer"
        assert upsert["scope"] == "notes.read offline_access"
        expected = datetime.now(timezone.utc) + timedelta(seconds=3600)
        assert abs((upsert["expires_at"] - expected).total_seconds()) < 30
        # Sessions must re-resolve, and tools should appear immediately.
        assert phase2.bumps == [USER_ID]
        assert phase2.discoveries == [(USER_ID, SERVER_NAME)]

    @pytest.mark.asyncio
    async def test_confidential_secret_is_encrypted_not_left_in_the_blob(
        self, redis, phase1, phase2
    ):
        # A DCR confidential client's secret must reach the dedicated (encrypted)
        # client_secret column, never the plaintext client_info JSONB — which is
        # persisted verbatim. Carried out-of-band on the state record and
        # re-attached for the token exchange.
        phase1.client_info = _client_info(
            client_secret="s3cr3t-value",
            token_endpoint_auth_method="client_secret_post",
        )

        started = await start_connect(USER_ID, SERVER_NAME)

        record = redis.only_record()
        assert record["client_secret"] == "s3cr3t-value"
        assert "client_secret" not in record["client_info"]

        await _callback(started, code="auth-code-1")

        [upsert] = phase2.upserts
        assert upsert["client_secret"] == "s3cr3t-value"
        assert "client_secret" not in upsert["client_info"]
        # The secret still authenticated the token exchange (client_secret_post).
        assert phase2.requests[-1]["data"]["client_secret"] == "s3cr3t-value"

    @pytest.mark.asyncio
    async def test_post_connect_discovery_failure_still_connects(
        self, redis, phase1, phase2
    ):
        phase2.discovery_error = RuntimeError("server hung up during tools/list")
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert phase2.upserts, "the connection is stored before discovery is attempted"

    @pytest.mark.asyncio
    async def test_success_resyncs_the_users_warm_sandboxes(
        self, redis, phase1, phase2
    ):
        """The bump only makes sessions re-resolve. A warm sandbox's generated
        client embeds the relay binding, so it must be re-applied too — until it
        is, the sandbox dials the vendor directly with the headers this
        connection displaced."""
        phase2.running_workspaces = ["ws-warm-1", "ws-warm-2"]
        started = await start_connect(USER_ID, SERVER_NAME)

        await _callback(started, code="auth-code-1")

        assert phase2.applies == [("ws-warm-1", USER_ID), ("ws-warm-2", USER_ID)]

    @pytest.mark.asyncio
    async def test_discovery_failure_still_resyncs_warm_sandboxes(
        self, redis, phase1, phase2
    ):
        """The failure path is the one that needs the resync most: nothing was
        written to the user tier, so the read falls back to the pre-connect
        snapshot and no other input can carry the binding into the sandbox."""
        phase2.discovery_error = RuntimeError("needs reauth")
        started = await start_connect(USER_ID, SERVER_NAME)

        await _callback(started, code="auth-code-1")

        assert phase2.applies == [("ws-warm-1", USER_ID)]

    @pytest.mark.asyncio
    async def test_resync_failure_does_not_break_the_connect(
        self, redis, phase1, phase2, monkeypatch
    ):
        """Best-effort, like the sibling mutation paths: convergence slips to the
        next turn rather than failing a connection that is already stored."""
        async def _boom(user_id):
            raise RuntimeError("workspace lookup down")

        monkeypatch.setattr(connect, "get_running_workspace_ids_for_user", _boom)
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert phase2.upserts

    @pytest.mark.asyncio
    async def test_matching_iss_is_accepted(self, redis, phase1, phase2):
        started = await start_connect(USER_ID, SERVER_NAME)
        issuer = redis.only_record()["issuer"]

        redirect = await _callback(started, code="auth-code-1", iss=issuer)

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"


# ---------------------------------------------------------------------------
# Catalog revalidation — phase 1's check is up to STATE_TTL_SECONDS stale
# ---------------------------------------------------------------------------


class TestCatalogRevalidation:
    """The catalog row must still describe the server the user consented to.

    A connection row is never deleted, so persisting against a server that was
    deleted (or re-pointed) mid-consent leaves a live, auto-refreshing
    connection with no catalog row behind it — invisible to the UI and
    inherited by the next same-name server.
    """

    @pytest.mark.asyncio
    async def test_a_server_deleted_during_consent_is_not_resurrected(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)
        phase1.catalog_row = None

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=server_changed&server={SERVER_NAME_Q}"
        )
        assert phase2.upserts == []
        # Nothing downstream runs either — no version bump, no discovery, and
        # no sandbox resync (there is no binding to converge on).
        assert phase2.bumps == []
        assert phase2.discoveries == []
        assert phase2.applies == []

    @pytest.mark.asyncio
    async def test_a_server_repointed_during_consent_is_refused(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)
        phase1.catalog_row = {
            "name": SERVER_NAME,
            "url": "https://mcp.other.test/mcp",
            "transport": "http",
        }

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=server_changed&server={SERVER_NAME_Q}"
        )
        assert phase2.upserts == []

    @pytest.mark.asyncio
    async def test_a_cosmetically_different_url_still_connects(
        self, redis, phase1, phase2
    ):
        # The comparison is the consent canonicalizer, not raw equality: a
        # default port or trailing slash is the same consented endpoint.
        started = await start_connect(USER_ID, SERVER_NAME)
        phase1.catalog_row = {
            "name": SERVER_NAME,
            "url": "https://MCP.demo.test:443/mcp/",
            "transport": "http",
        }

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert len(phase2.upserts) == 1


# ---------------------------------------------------------------------------
# Single-use state claim
# ---------------------------------------------------------------------------


class TestSingleUseState:
    @pytest.mark.asyncio
    async def test_a_replayed_state_is_rejected(self, redis, phase1, phase2):
        started = await start_connect(USER_ID, SERVER_NAME)

        first = await _callback(started, code="auth-code-1")
        second = await _callback(started, code="auth-code-1")

        assert first == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert second == f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state"
        # The replay never reaches the token endpoint.
        assert len(phase2.requests) == 1
        assert len(phase2.upserts) == 1
        assert redis.states() == {}

    @pytest.mark.asyncio
    async def test_concurrent_callbacks_claim_the_state_once(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)

        results = await asyncio.gather(
            _callback(started, code="auth-code-1"),
            _callback(started, code="auth-code-1"),
        )

        assert sorted(results) == sorted(
            [
                f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}",
                f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state",
            ]
        )
        assert len(phase2.requests) == 1

    @pytest.mark.asyncio
    async def test_unknown_state_is_indistinguishable_from_a_used_one(
        self, redis, phase2
    ):
        redirect = await complete_callback(state="never-issued", code="auth-code-1")

        # Same answer as a replay, on the default path: no oracle for whether a
        # state ever existed, and no parked return_to to consult.
        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state"
        assert phase2.requests == []


# ---------------------------------------------------------------------------
# Concurrent connects for one server
# ---------------------------------------------------------------------------


class TestSupersededConnects:
    """Two browsing contexts, one server: only the newest flow can finish.

    Both used to, and the access the loser was granted stayed live at the vendor
    with nothing on our side pointing at it.

    Two halves close it, because a losing flow can be in either of two places
    when the newer connect starts. One has not claimed its state yet and is shut
    down by the delete; the other already has, and is shut down by the marker
    read in its own callback. Only the pair covers the whole window.
    """

    CONNECTED = f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
    RETIRED = f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state"
    # The same refusal one step later, once the record has been claimed and the
    # page can be told which server it was about.
    RETIRED_LATE = f"{RETIRED}&server={SERVER_NAME_Q}"

    @pytest.mark.asyncio
    async def test_a_second_connect_retires_the_first(self, redis, phase1, phase2):
        first = await start_connect(USER_ID, SERVER_NAME)
        second = await start_connect(USER_ID, SERVER_NAME)

        assert await _callback(first, code="code-1") == self.RETIRED
        assert await _callback(second, code="code-2") == self.CONNECTED

        # Retired here rather than at the upsert precisely so the abandoned flow
        # never reaches the token endpoint: no grant is issued, so there is none
        # to forget about.
        assert [r["data"]["code"] for r in phase2.requests] == ["code-2"]
        assert len(phase2.upserts) == 1

    @pytest.mark.asyncio
    async def test_order_of_arrival_does_not_decide_it(self, redis, phase1, phase2):
        first = await start_connect(USER_ID, SERVER_NAME)
        second = await start_connect(USER_ID, SERVER_NAME)

        # The newest flow wins even when the abandoned tab reports back last.
        assert await _callback(second, code="code-2") == self.CONNECTED
        assert await _callback(first, code="code-1") == self.RETIRED
        assert len(phase2.upserts) == 1

    @pytest.mark.asyncio
    async def test_a_marker_left_by_a_finished_flow_costs_the_next_one_nothing(
        self, redis, phase1, phase2
    ):
        first = await start_connect(USER_ID, SERVER_NAME)
        assert await _callback(first, code="code-1") == self.CONNECTED

        # The callback deliberately leaves the marker behind, so an ordinary
        # reconnect meets a stale one. It names a state that is already spent,
        # so all it can do is delete a key that is gone.
        second = await start_connect(USER_ID, SERVER_NAME)
        assert await _callback(second, code="code-2") == self.CONNECTED
        assert len(phase2.upserts) == 2

    @pytest.mark.asyncio
    async def test_a_callback_past_its_claim_is_still_refused(
        self, redis, phase1, phase2
    ):
        """The half the delete cannot reach.

        A flow whose callback already spent its state key is beyond what the
        newer connect's delete can touch -- there is nothing left to delete.
        Re-parking the record is that flow caught mid-callback: holding a
        record nothing can retire, and, without the marker read, going on to
        spend its code and upsert last over the connection the winner wrote.
        """
        first = await start_connect(USER_ID, SERVER_NAME)
        claimed = redis.only_record()
        second = await start_connect(USER_ID, SERVER_NAME)
        redis.park(first.state, claimed)

        assert await _callback(first, code="code-1") == self.RETIRED_LATE
        # Before the token endpoint, so the surplus grant is never issued.
        assert phase2.requests == []
        assert phase2.upserts == []

        assert await _callback(second, code="code-2") == self.CONNECTED
        assert len(phase2.upserts) == 1

    @pytest.mark.asyncio
    async def test_a_supersede_during_the_exchange_lands_before_the_write(
        self, redis, phase1, phase2
    ):
        """The gap the read before the exchange cannot see.

        That read happens a whole round trip to the vendor before the write, and
        a connect starting inside it cannot retire a state this flow has already
        claimed. Asked only once, the loser exchanges its code and then writes
        last, over the connection the winner just made.
        """
        first = await start_connect(USER_ID, SERVER_NAME)

        fired = []

        async def a_newer_connect_starts(_request):
            if fired:
                return
            fired.append(True)
            await start_connect(USER_ID, SERVER_NAME)

        phase2.on_request = a_newer_connect_starts

        assert await _callback(first, code="code-1") == self.RETIRED_LATE
        # The grant was issued -- nothing could have known in time -- but the
        # refusal lands before anything is written under it.
        assert any((r["data"] or {}).get("code") == "code-1" for r in phase2.requests)
        assert phase2.upserts == []

    @pytest.mark.asyncio
    async def test_a_flow_whose_marker_has_expired_still_completes(
        self, redis, phase1, phase2
    ):
        """The other side of the same read, and the one that must not fail shut.

        Marker and state record carry the same TTL, so a record that is still
        claimable with no marker beside it is not a superseded flow -- it is an
        ordinary one racing the expiry. Refusing on an absent marker would turn
        that into a connect the user cannot complete at all.
        """
        first = await start_connect(USER_ID, SERVER_NAME)
        del redis.store[f"{INFLIGHT_PREFIX}{USER_ID}:{SERVER_NAME}"]

        assert await _callback(first, code="code-1") == self.CONNECTED
        assert len(phase2.upserts) == 1

    @pytest.mark.asyncio
    async def test_the_marker_expires_with_the_flow_it_names(self, redis, phase1):
        await start_connect(USER_ID, SERVER_NAME)

        [marker] = [c for c in redis.set_calls if c["key"].startswith(INFLIGHT_PREFIX)]
        assert marker["key"] == f"{INFLIGHT_PREFIX}{USER_ID}:{SERVER_NAME}"
        assert marker["ex"] == STATE_TTL_SECONDS

    @pytest.mark.asyncio
    async def test_another_server_is_left_alone(self, redis, phase1):
        # The fake catalog answers to any name, so this is the same row under a
        # second one -- which is exactly the pair the marker keys on.
        first = await start_connect(USER_ID, SERVER_NAME)
        await start_connect(USER_ID, "other-server")

        assert f"{STATE_PREFIX}{first.state}" in redis.store

    @pytest.mark.asyncio
    async def test_another_user_is_left_alone(self, redis, phase1):
        first = await start_connect(USER_ID, SERVER_NAME)
        await start_connect("user-connect-2", SERVER_NAME)

        assert f"{STATE_PREFIX}{first.state}" in redis.store


# ---------------------------------------------------------------------------
# Exclusive vendors — one connected platform per account, across every row
# ---------------------------------------------------------------------------


class TestExclusiveVendorScope:
    """A vendor whose grant belongs to the account, not to the row that won it.

    Everything else here keys on the server name, which is the identity at every
    other tier. This vendor is the exception: it permits one connected AI
    platform per account and drops the previous grant the moment a new one
    lands, and a user can own two rows pointing at it -- the shipped row beside
    one they added, or one repointed onto the same host. Keyed by name those two
    rows never met, so both connects spent their codes and both rows went on
    reading connected over a single surviving grant.

    Two ways in, so two answers: concurrent connects are retired in flight,
    before the second code is ever spent, and a connect a week later finds the
    older row and puts it where the vendor already put it.
    """

    CONNECTED = f"{DEFAULT_RETURN_TO}?mcp_connected=ibkr"

    @staticmethod
    def _row(phase1, name: str, url: str) -> None:
        phase1.rows[name] = {"name": name, "url": url, "transport": "http"}

    @pytest.fixture(autouse=True)
    def rows(self, monkeypatch, phase1) -> SimpleNamespace:
        """Let the fake catalog answer for a row's own address.

        The shared fixture answers one row to every name, which is the right
        shape for tests about a single server and the wrong one here: the whole
        question is what two rows at two addresses do to each other.
        """
        phase1.rows = {}

        async def _discover(client, server_url):
            return phase1.prm, phase1.as_metadata, ISSUER, phase1.www_scope

        async def _get_catalog_server(user_id, name):
            return phase1.rows.get(name)

        monkeypatch.setattr(connect, "_discover", _discover)
        monkeypatch.setattr(connect, "get_catalog_server", _get_catalog_server)
        return phase1

    @pytest.fixture
    def siblings(self, monkeypatch) -> SimpleNamespace:
        """The user's other rows, and what the callback does to them."""
        env = SimpleNamespace(catalog=[], connections={}, disconnected=[])

        async def _list_catalog_servers(user_id):
            return list(env.catalog)

        async def _get_connection(user_id, name, **kwargs):
            return env.connections.get(name)

        async def _disconnect_server(user_id, name):
            env.disconnected.append(name)
            return True

        monkeypatch.setattr(connect, "list_catalog_servers", _list_catalog_servers)
        monkeypatch.setattr(connect, "get_connection", _get_connection)
        monkeypatch.setattr(
            "src.server.services.mcp_oauth.lifecycle.disconnect_server",
            _disconnect_server,
        )
        return env

    @pytest.mark.asyncio
    async def test_a_second_row_at_the_vendor_retires_the_first_connect(
        self, redis, phase1
    ):
        """The race, which used to be two races that could not see each other."""
        self._row(phase1, "ibkr", IBKR_URL)
        self._row(phase1, "my_ibkr", IBKR_ALT_URL)

        first = await start_connect(USER_ID, "ibkr", granted_capabilities=["market_data"])
        await start_connect(USER_ID, "my_ibkr", granted_capabilities=["market_data"])

        assert f"{STATE_PREFIX}{first.state}" not in redis.store
        assert f"{INFLIGHT_PREFIX}{USER_ID}:vendor:ibkr" in redis.store

    @pytest.mark.asyncio
    async def test_rows_at_an_ordinary_vendor_are_separate_connects(
        self, redis, phase1
    ):
        """The control, and the reason the scope is not simply the host.

        Two rows at one host join to one vendor exactly as the exclusive case
        does, so the host alone cannot be what decides. Widening supersession to
        every vendor would retire a connect for no reason at all -- nothing
        about a second row here costs the first one anything.
        """
        self._row(phase1, "ordinary", ORDINARY_URL)
        self._row(phase1, "my_ordinary", ORDINARY_URL + "/mine")

        first = await start_connect(USER_ID, "ordinary")
        await start_connect(USER_ID, "my_ordinary")

        assert f"{STATE_PREFIX}{first.state}" in redis.store

    @pytest.mark.asyncio
    async def test_a_connect_retires_the_row_the_vendor_just_dropped(
        self, redis, phase1, phase2, siblings
    ):
        """The sequential case, which is not a race and never met the guard above.

        The second connect is perfectly ordinary. What makes it destructive is
        the vendor, and the only thing that knows is this callback.
        """
        self._row(phase1, "ibkr", IBKR_URL)
        siblings.catalog = [
            {"name": "ibkr", "url": IBKR_URL},
            {"name": "my_ibkr", "url": IBKR_ALT_URL},
            {"name": SERVER_NAME, "url": SERVER_URL},
        ]
        siblings.connections = {
            "my_ibkr": SimpleNamespace(status=ConnectionStatus.CONNECTED),
            SERVER_NAME: SimpleNamespace(status=ConnectionStatus.CONNECTED),
        }

        started = await start_connect(USER_ID, "ibkr", granted_capabilities=["market_data"])

        assert await _callback(started, code="code-1") == self.CONNECTED
        # Its own row is not a sibling, and a server somewhere else is not this
        # vendor whatever else the user has connected.
        assert siblings.disconnected == ["my_ibkr"]

    @pytest.mark.asyncio
    async def test_a_row_already_revoked_is_left_where_it_is(
        self, redis, phase1, phase2, siblings
    ):
        """Otherwise every reconnect re-tears-down a row nothing changed.

        Disconnecting bumps each of the user's workspaces, so a no-op that is
        not one costs a re-resolve across the whole account every time the user
        repairs a connection.
        """
        self._row(phase1, "ibkr", IBKR_URL)
        siblings.catalog = [
            {"name": "ibkr", "url": IBKR_URL},
            {"name": "my_ibkr", "url": IBKR_ALT_URL},
        ]
        siblings.connections = {
            "my_ibkr": SimpleNamespace(status=ConnectionStatus.REVOKED)
        }

        started = await start_connect(USER_ID, "ibkr", granted_capabilities=["market_data"])

        assert await _callback(started, code="code-1") == self.CONNECTED
        assert siblings.disconnected == []

    @pytest.mark.asyncio
    async def test_a_sibling_that_cannot_be_retired_does_not_fail_the_connect(
        self, redis, phase1, phase2, siblings, monkeypatch
    ):
        """The grant is won and written by this point.

        Reporting a connect that worked as an error would leave the user with a
        live connection and a page telling them to try again -- and trying again
        spends another grant. The row is left overstating itself, which is
        exactly where it already was.
        """
        self._row(phase1, "ibkr", IBKR_URL)
        siblings.catalog = [{"name": "my_ibkr", "url": IBKR_ALT_URL}]

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("the catalog read went sideways")

        monkeypatch.setattr(connect, "list_catalog_servers", _boom)

        started = await start_connect(USER_ID, "ibkr", granted_capabilities=["market_data"])

        assert await _callback(started, code="code-1") == self.CONNECTED
        assert len(phase2.upserts) == 1


# ---------------------------------------------------------------------------
# The address the page asked about — a connect is refused once the row moves
# ---------------------------------------------------------------------------


class TestExpectedUrlGate:
    """A connect only starts against the server the caller answered for.

    The questions a page asks before connecting are asked of a particular
    address, and the row is the user's to edit from another tab while they
    answer. Naming the address is what makes the question a gate rather than
    a decoration.
    """

    @pytest.mark.asyncio
    async def test_a_row_that_moved_refuses_the_connect(self, redis, phase1):
        with pytest.raises(McpServerMoved):
            await start_connect(USER_ID, SERVER_NAME, expected_url=IBKR_URL)

        # Nothing was parked: the refusal comes before any state is minted, so
        # the row is left exactly where the other tab put it.
        assert not [c for c in redis.set_calls if c["key"].startswith(STATE_PREFIX)]

    @pytest.mark.asyncio
    async def test_the_address_the_page_drew_it_from_connects(self, redis, phase1):
        result = await start_connect(USER_ID, SERVER_NAME, expected_url=SERVER_URL)

        assert redis.only_record()["server_url"] == SERVER_URL
        assert result.authorize_url.startswith(f"{ISSUER}/authorize?")

    @pytest.mark.asyncio
    async def test_the_same_endpoint_written_differently_still_connects(
        self, redis, phase1
    ):
        # Compared the way the callback compares its own record against the
        # row, so a trailing slash is the same server on both ends of the flow
        # rather than a refusal on one and a match on the other.
        result = await start_connect(
            USER_ID, SERVER_NAME, expected_url=f"{SERVER_URL}/"
        )

        assert redis.only_record()["server_url"] == SERVER_URL
        assert result.authorize_url.startswith(f"{ISSUER}/authorize?")

    @pytest.mark.asyncio
    async def test_a_caller_that_names_no_address_is_let_through(self, redis, phase1):
        # An older page sends nothing and has nothing to be wrong about.
        result = await start_connect(USER_ID, SERVER_NAME)

        assert redis.only_record()["server_url"] == SERVER_URL
        assert result.authorize_url.startswith(f"{ISSUER}/authorize?")


# ---------------------------------------------------------------------------
# CSRF binding — the callback must present the browser nonce minted in phase 1
# ---------------------------------------------------------------------------


class TestCsrfBinding:
    @pytest.fixture(autouse=True)
    def deployed_callback(self, monkeypatch):
        """Pin a non-loopback callback so the binding is actually in force.

        The test env's ``SERVER_BASE_URL`` is a loopback default, which is the
        one place the nonce is deliberately not minted — leaving it would put
        every case below on the skip path and silently stop testing the control.
        """
        monkeypatch.setattr(redirects, "SERVER_BASE_URL", "https://app.example.com")

    @pytest.mark.asyncio
    async def test_matching_nonce_connects(self, redis, phase1, phase2):
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await complete_callback(
            state=started.state,
            code="auth-code-1",
            browser_nonce=started.browser_nonce,
        )

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert len(phase2.requests) == 1

    @pytest.mark.asyncio
    async def test_wrong_nonce_is_refused_and_burns_the_state(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await complete_callback(
            state=started.state, code="auth-code-1", browser_nonce="not-the-cookie"
        )

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=state_mismatch&server={SERVER_NAME_Q}"
        )
        # A forged callback never reaches the token endpoint, and the state is
        # spent — a subsequent replay (even with the right cookie) is dead.
        assert phase2.requests == []
        assert redis.states() == {}
        replay = await complete_callback(
            state=started.state,
            code="auth-code-1",
            browser_nonce=started.browser_nonce,
        )
        assert replay == f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state"

    @pytest.mark.asyncio
    async def test_absent_cookie_is_refused(self, redis, phase1, phase2):
        started = await start_connect(USER_ID, SERVER_NAME)

        # A callback landing in a browser that never held the cookie (the
        # classic login-CSRF replay) carries no nonce at all.
        redirect = await complete_callback(
            state=started.state, code="auth-code-1", browser_nonce=None
        )

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=state_mismatch&server={SERVER_NAME_Q}"
        )
        assert phase2.requests == []

    @pytest.mark.asyncio
    async def test_legacy_record_without_a_nonce_skips_the_check(
        self, redis, phase1, phase2
    ):
        """A record parked before this control shipped carries an empty nonce;
        its callback must still complete rather than fail closed on a field it
        could never have set."""
        started = await start_connect(USER_ID, SERVER_NAME)
        record = redis.only_record()
        record["browser_nonce"] = ""
        redis.store.clear()
        redis.park(started.state, record)

        redirect = await complete_callback(
            state=started.state, code="auth-code-1", browser_nonce=None
        )

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"


class TestLoopbackCallbackSkipsTheBinding:
    """A loopback callback can't receive the cookie back, so it mints no nonce.

    An AS accepts an ``http`` redirect_uri only for loopback (RFC 8252), while
    the cookie only returns if the browsed origin shares the callback's *host* —
    and a dev box routinely serves its UI from some other host. Requiring the
    cookie there rejects every connect, so the mint is skipped instead.
    """

    @pytest.mark.parametrize(
        "base,loopback",
        [
            ("http://127.0.0.1:8060", True),
            ("http://localhost:8000", True),
            ("http://wt3.localhost", True),
            ("http://[::1]:8000", True),
            ("https://app.example.com", False),
            ("https://langalpha.ai", False),
        ],
    )
    def test_host_classification(self, monkeypatch, base, loopback):
        monkeypatch.setattr(redirects, "SERVER_BASE_URL", base)
        assert redirects.callback_is_loopback() is loopback

    @pytest.mark.asyncio
    async def test_no_nonce_is_minted_and_the_callback_completes(
        self, monkeypatch, redis, phase1, phase2
    ):
        monkeypatch.setattr(redirects, "SERVER_BASE_URL", "http://127.0.0.1:8060")

        started = await start_connect(USER_ID, SERVER_NAME)

        # Empty, so the parked record takes the same skip path a pre-control
        # record takes — no dev branch in the verification logic.
        assert started.browser_nonce == ""
        assert redis.only_record()["browser_nonce"] == ""

        redirect = await complete_callback(
            state=started.state, code="auth-code-1", browser_nonce=None
        )

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert len(phase2.requests) == 1


# ---------------------------------------------------------------------------
# Callback error arms
# ---------------------------------------------------------------------------


class TestCallbackErrors:
    @pytest.mark.asyncio
    async def test_missing_state(self, redis, phase2):
        assert await complete_callback(state=None, code="x") == (
            f"{DEFAULT_RETURN_TO}?mcp_error=missing_state"
        )
        assert await complete_callback(state="", code="x") == (
            f"{DEFAULT_RETURN_TO}?mcp_error=missing_state"
        )
        assert phase2.requests == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error", "reason"),
        [
            ("access_denied", "denied"),
            ("server_error", "provider_error"),
            ("invalid_scope", "provider_error"),
        ],
    )
    async def test_authorization_server_error(
        self, redis, phase1, phase2, error, reason
    ):
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(
            started,
            code=None,
            error=error,
            error_description="user cancelled",
        )

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error={reason}&server={SERVER_NAME_Q}"
        )
        assert phase2.requests == []
        # A failed callback still burns the state.
        assert redis.states() == {}

    @pytest.mark.asyncio
    async def test_missing_code(self, redis, phase1, phase2):
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code=None)

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=missing_code&server={SERVER_NAME_Q}"
        )
        assert phase2.requests == []

    @pytest.mark.asyncio
    async def test_issuer_mismatch(self, redis, phase1, phase2):
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(
            started, code="auth-code-1", iss="https://evil.test/"
        )

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=issuer_mismatch&server={SERVER_NAME_Q}"
        )
        assert phase2.requests == []

    @pytest.mark.asyncio
    async def test_an_unplanned_failure_still_names_its_server(
        self, monkeypatch, redis, phase1, phase2
    ):
        """The one redirect that used to name nobody.

        The route above this catches whatever escapes and has nothing to put in
        the redirect: the state is spent, so it cannot look the server back up.
        A page that lands on an error naming no server refuses to attribute it
        while another connect is still out, so the row this flow switched on is
        left standing with nothing behind it, and its marker waits to be
        reported as an abandoned connect on some later visit.
        """

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("the write went sideways")

        monkeypatch.setattr(connect, "upsert_connection", _boom)
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=internal&server={SERVER_NAME_Q}"
        )

    @pytest.mark.asyncio
    async def test_token_exchange_rejected(self, redis, phase1, phase2):
        phase2.status_code = 400
        phase2.payload = {"error": "invalid_grant"}
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=token_exchange_failed"
            f"&server={SERVER_NAME_Q}"
        )
        assert phase2.upserts == []

    @pytest.mark.asyncio
    async def test_token_exchange_transport_error(self, redis, phase1, phase2):
        phase2.raises = httpx2.ConnectError("connection reset")
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=token_exchange_failed"
            f"&server={SERVER_NAME_Q}"
        )
        assert phase2.upserts == []

    @pytest.mark.asyncio
    async def test_blocked_token_endpoint(self, redis, phase1, phase2):
        phase2.raises = OAuthHopBlocked("egress to token endpoint is blocked")
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=blocked_endpoint&server={SERVER_NAME_Q}"
        )
        assert phase2.upserts == []

    @pytest.mark.asyncio
    async def test_a_pre_send_block_is_still_named_as_a_blocked_endpoint(
        self, redis, phase1, phase2
    ):
        """The shape an SSRF policy rejection actually takes.

        The guard refuses before the request is built, so every real blocked
        endpoint arrives tagged as never-sent; only a refused redirect is the
        other kind. Reporting this one as a generic exchange failure would leave
        the user's own misconfiguration unnamed.
        """
        phase2.raises = OAuthHopBlocked(
            "egress to 'token.internal.test' is blocked: "
            "resolves to a non-global address",
            request_sent=False,
        )
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=blocked_endpoint&server={SERVER_NAME_Q}"
        )
        assert phase2.upserts == []


# ---------------------------------------------------------------------------
# return_to allowlisting
# ---------------------------------------------------------------------------


class TestReturnToAllowlist:
    @pytest.mark.parametrize(
        "value",
        [
            None,
            "",
            "//evil.test/phish",
            "https://evil.test/phish",
            "http://evil.test",
            "evil.test",
            "plugins",
            "\\\\evil.test",
            # Leading slash then backslash: browsers normalize '\' to '/', so
            # '/\evil.test' becomes protocol-relative '//evil.test' — off-app.
            "/\\evil.test",
        ],
    )
    def test_off_allowlist_values_fall_back_to_the_default(self, value):
        assert sanitize_return_to(value) == DEFAULT_RETURN_TO

    @pytest.mark.parametrize(
        # "/connectors" stays honored on purpose: return_to values parked in
        # Redis before the Plugins rename must still round-trip (the SPA
        # aliases the old route).
        "value", ["/plugins", "/settings/plugins", "/plugins?tab=oauth", "/connectors"]
    )
    def test_same_app_relative_paths_are_honored(self, value):
        assert sanitize_return_to(value) == value

    @pytest.mark.asyncio
    async def test_phase1_parks_only_the_sanitized_path(self, redis, phase1):
        await start_connect(USER_ID, SERVER_NAME, return_to="https://evil.test/phish")

        assert redis.only_record()["return_to"] == DEFAULT_RETURN_TO

    @pytest.mark.asyncio
    async def test_honored_path_survives_to_the_success_redirect(
        self, redis, phase1, phase2
    ):
        started = await start_connect(
            USER_ID, SERVER_NAME, return_to="/settings/plugins"
        )

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"/settings/plugins?mcp_connected={SERVER_NAME_Q}"

    @pytest.mark.asyncio
    async def test_phase2_resanitizes_a_poisoned_record(self, redis, phase1, phase2):
        """Defense in depth: even a record whose return_to bypassed phase 1
        cannot steer the browser off-app."""
        started = await start_connect(USER_ID, SERVER_NAME)
        record = redis.only_record()
        record["return_to"] = "https://evil.test/phish"
        redis.store.clear()
        redis.park(started.state, record)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"

    @pytest.mark.asyncio
    async def test_poisoned_record_cannot_steer_the_error_redirect_either(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)
        record = redis.only_record()
        record["return_to"] = "//evil.test/phish"
        redis.store.clear()
        redis.park(started.state, record)

        redirect = await _callback(started, code=None, error="access_denied")

        assert redirect.startswith(f"{DEFAULT_RETURN_TO}?mcp_error=denied")


# ---------------------------------------------------------------------------
# web-origin capture (split-port dev: the callback's origin is the API, not
# the UI — the redirect must resolve on the origin the start request came from)
# ---------------------------------------------------------------------------


class TestWebOriginCapture:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("http://127.0.0.1:5233", "http://127.0.0.1:5233"),
            ("https://wt3.localhost", "https://wt3.localhost"),
            ("http://localhost:5173/", "http://localhost:5173"),
        ],
    )
    def test_bare_origins_are_honored(self, value, expected):
        assert sanitize_web_origin(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "",
            "null",
            "javascript:alert(1)",
            "https://evil.test/phish",
            "http://user@evil.test",
            "//evil.test",
            "ftp://files.test",
            "https://e.test?q=1",
            "https://e.test#frag",
            # Bare, well-formed, but foreign public origins — an attacker-forged
            # Origin header on the start request must not become the redirect
            # prefix, so these are dropped, not echoed back.
            "https://evil.test",
            "https://app.example.com",
        ],
    )
    def test_non_origin_values_are_dropped(self, value):
        assert sanitize_web_origin(value) == ""

    def test_the_deployments_own_origin_is_honored(self, monkeypatch):
        # A non-loopback origin is honored only when it is this deployment's own
        # base URL (a same-origin prod redirect), never an arbitrary one.
        monkeypatch.setattr(redirects, "SERVER_BASE_URL", "https://app.example.com")
        assert sanitize_web_origin("https://app.example.com") == "https://app.example.com"
        assert sanitize_web_origin("https://evil.test") == ""

    @pytest.mark.asyncio
    async def test_phase1_parks_the_sanitized_origin(self, redis, phase1):
        await start_connect(
            USER_ID, SERVER_NAME, web_origin="http://127.0.0.1:5233"
        )

        assert redis.only_record()["web_origin"] == "http://127.0.0.1:5233"

    @pytest.mark.asyncio
    async def test_success_redirect_is_absolute_on_the_captured_origin(
        self, redis, phase1, phase2
    ):
        started = await start_connect(
            USER_ID,
            SERVER_NAME,
            return_to="/plugins",
            web_origin="http://127.0.0.1:5233",
        )

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == (
            f"http://127.0.0.1:5233/plugins?mcp_connected={SERVER_NAME_Q}"
        )

    @pytest.mark.asyncio
    async def test_error_redirect_rides_the_captured_origin_too(
        self, redis, phase1, phase2
    ):
        started = await start_connect(
            USER_ID, SERVER_NAME, web_origin="https://wt3.localhost"
        )

        redirect = await _callback(started, code=None, error="access_denied")

        assert redirect.startswith(
            f"https://wt3.localhost{DEFAULT_RETURN_TO}?mcp_error=denied"
        )

    @pytest.mark.asyncio
    async def test_a_poisoned_record_origin_is_resanitized_at_phase2(
        self, redis, phase1, phase2
    ):
        """Defense in depth: a record whose origin bypassed phase 1 cannot
        turn the callback into an open redirector."""
        started = await start_connect(USER_ID, SERVER_NAME)
        record = redis.only_record()
        record["web_origin"] = "https://evil.test/phish"
        redis.store.clear()
        redis.park(started.state, record)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"


# ---------------------------------------------------------------------------
# Loopback redirect override — the native-app profile, for an AS that refuses
# a hosted callback
# ---------------------------------------------------------------------------


class TestLoopbackRedirectOverride:
    """The one caller-supplied value the callback URI can take.

    Everywhere else it comes from ``SERVER_BASE_URL`` and is underivable from
    anything on the wire. What replaces that property here is the bound: a
    loopback target reaches only the machine already running the flow.
    """

    @pytest.mark.parametrize(
        "value",
        [
            "http://127.0.0.1:8788/mcp/callback",
            "http://127.0.0.1:1024/mcp/callback",
            "http://127.9.9.9:8788/mcp/callback",
            "http://[::1]:8788/mcp/callback",
        ],
    )
    def test_accepted(self, value):
        assert sanitize_loopback_redirect(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "",
            # The whole point of the bound: a target off this machine.
            "https://evil.test/mcp/callback",
            "http://evil.test:8788/mcp/callback",
            "http://169.254.169.254:8788/x",
            # A name, not a literal. It resolves through whatever the machine's
            # resolver says, and an AS matching its allowlist by string refuses
            # it anyway.
            "http://localhost:8788/mcp/callback",
            "http://wt3.localhost:8788/mcp/callback",
            # https on loopback is not the native-app profile and no shell of
            # ours can serve it; allowing it only widens the shape accepted.
            "https://127.0.0.1:8788/mcp/callback",
            # Below 1024 needs root on a POSIX box, so it is not a desktop app
            # asking for its own port.
            "http://127.0.0.1:80/mcp/callback",
            "http://127.0.0.1/mcp/callback",
            # Userinfo, a query and a fragment each change what the AS is handed
            # versus what was checked here.
            "http://user:pw@127.0.0.1:8788/x",
            "http://127.0.0.1:8788/x?next=https://evil.test",
            "http://127.0.0.1:8788/x#f",
            # Not a URL, and a scheme the OS would hand to an installed app.
            "not a url",
            "langalpha://127.0.0.1:8788/x",
            "http://127.0.0.1:notaport/x",
            # Protocol-relative once a browser folds the backslash.
            "http://127.0.0.1:8788//evil.test",
            "http://127.0.0.1:8788/\\evil.test",
            # Any path but the shell's own. The freedom bought nothing -- the
            # shell only ever offers one -- and it let a caller name an
            # unrelated local listener, which would be handed an authorization
            # code it could log or reflect.
            "http://127.0.0.1:8788/x",
            "http://127.0.0.1:8788/",
            "http://127.0.0.1:8788/mcp/callback/",
            "http://127.0.0.1:8788/MCP/CALLBACK",
        ],
    )
    def test_refused(self, value):
        assert sanitize_loopback_redirect(value) == ""

    def test_a_mixed_case_host_cannot_differ_from_what_was_checked(self):
        # The value is bound into the state record and presented again at the
        # token exchange, so it is rebuilt rather than echoed: two spellings of
        # one host would otherwise be two different strings to the AS.
        assert (
            sanitize_loopback_redirect("HTTP://127.0.0.1:8788/mcp/callback")
            == "http://127.0.0.1:8788/mcp/callback"
        )

    @pytest.mark.asyncio
    async def test_one_value_reaches_the_authorize_url_the_dcr_and_the_record(
        self, monkeypatch, redis, phase1
    ):
        """An AS is entitled to compare all three, so they must not diverge."""
        seen = {}

        async def _register(client, **kwargs):
            seen["metadata"] = kwargs["client_metadata"]
            return connect._Registration(phase1.client_info)

        monkeypatch.setattr(connect, "_register_client", _register)
        loopback = "http://127.0.0.1:8789/mcp/callback"

        started = await start_connect(
            USER_ID, SERVER_NAME, loopback_redirect=loopback
        )

        assert _query(started.authorize_url)["redirect_uri"] == loopback
        assert [str(u) for u in seen["metadata"].redirect_uris] == [loopback]
        assert redis.only_record()["redirect_uri"] == loopback

    @pytest.mark.asyncio
    async def test_a_refused_override_falls_back_rather_than_failing(
        self, monkeypatch, redis, phase1
    ):
        # Degrading to the deployment's own callback is what makes this safe to
        # send unconditionally: a shell too old to be trusted, or a value that
        # does not pass, leaves the browser flow exactly as it was.
        monkeypatch.setattr(redirects, "SERVER_BASE_URL", "https://app.example.com")

        started = await start_connect(
            USER_ID, SERVER_NAME, loopback_redirect="https://evil.test/cb"
        )

        assert _query(started.authorize_url)["redirect_uri"] == (
            "https://app.example.com/api/v1/mcp/oauth/callback"
        )

    @pytest.mark.asyncio
    async def test_the_browser_nonce_still_binds(
        self, monkeypatch, redis, phase1, phase2
    ):
        """The trap: the nonce asks about the deployment, not about this value.

        The AS answers a listener on the user's machine, which drives that same
        browser to this deployment's callback — where the cookie is present as
        it always was. Reading the skip off ``redirect_uri`` instead would drop
        the binding on precisely the flows that can still honor it.
        """
        monkeypatch.setattr(redirects, "SERVER_BASE_URL", "https://app.example.com")

        started = await start_connect(
            USER_ID,
            SERVER_NAME,
            loopback_redirect="http://127.0.0.1:8788/mcp/callback",
        )

        assert started.browser_nonce != ""
        refused = await complete_callback(
            state=started.state, code="auth-code-1", browser_nonce=None
        )
        assert refused.startswith(f"{DEFAULT_RETURN_TO}?mcp_error=state_mismatch")

    @pytest.mark.asyncio
    async def test_phase2_presents_the_loopback_uri_at_the_token_endpoint(
        self, redis, phase1, phase2
    ):
        loopback = "http://127.0.0.1:8788/mcp/callback"

        started = await start_connect(
            USER_ID, SERVER_NAME, loopback_redirect=loopback
        )
        await _callback(started, code="auth-code-1")

        assert phase2.requests[0]["data"]["redirect_uri"] == loopback


# ---------------------------------------------------------------------------
# Issuer reconciliation
# ---------------------------------------------------------------------------


class TestIssuerReconciliation:
    """RFC 8414 §3.3 binds AS metadata to the identifier it was fetched from.

    That binding is what stops a resource server from pointing at metadata some
    other authorization server published, so it is never waived here — only
    re-checked at the identifier the metadata itself claims, and only when that
    identifier is the same host. A document may correct its own name; it may not
    hand the flow to somebody else.
    """

    ADVERTISED = "https://as.example.com/oauth2"
    ORIGIN = "https://as.example.com"

    @staticmethod
    def _served(monkeypatch, catalogue: dict[str, OAuthMetadata | None]):
        """Publish metadata per identifier, recording what was asked for."""
        asked: list[str | None] = []

        async def _fetch(client, identifier, server_url):
            asked.append(identifier)
            return catalogue.get(identifier)

        monkeypatch.setattr(connect, "_fetch_as_metadata", _fetch)
        return asked

    @pytest.mark.asyncio
    async def test_a_matching_issuer_is_left_alone(self, monkeypatch):
        meta = _as_metadata(issuer=self.ADVERTISED)
        asked = self._served(monkeypatch, {self.ADVERTISED: meta})

        identifier, resolved = await connect._resolve_as_metadata(
            None, self.ADVERTISED, "https://mcp.example.com/mcp"
        )

        assert (identifier, resolved) == (self.ADVERTISED, meta)
        assert asked == [self.ADVERTISED], "a matching issuer needs no second look"

    @pytest.mark.asyncio
    async def test_a_server_that_misnames_itself_is_taken_at_its_own_word(
        self, monkeypatch
    ):
        # The shape seen in the wild: the resource advertises the AS with a path,
        # the AS document names the bare origin, and discovery there is
        # self-consistent — the binding holds, one identifier over.
        stray = _as_metadata(issuer=self.ORIGIN)
        real = _as_metadata(issuer=self.ORIGIN)
        asked = self._served(
            monkeypatch, {self.ADVERTISED: stray, self.ORIGIN: real}
        )

        identifier, resolved = await connect._resolve_as_metadata(
            None, self.ADVERTISED, "https://mcp.example.com/mcp"
        )

        # The corrected identifier, not the advertised one: registration and the
        # stored client are both keyed on what comes back from here.
        assert identifier == self.ORIGIN
        assert resolved is real
        assert asked == [self.ADVERTISED, self.ORIGIN]

    @pytest.mark.asyncio
    async def test_it_will_not_follow_a_claim_to_another_host(self, monkeypatch):
        elsewhere = _as_metadata(issuer="https://other.example.net")
        asked = self._served(
            monkeypatch,
            {
                self.ADVERTISED: elsewhere,
                # Self-consistent, and entirely beside the point: reaching it at
                # all would be the substitution the binding exists to prevent.
                "https://other.example.net": _as_metadata(
                    issuer="https://other.example.net"
                ),
            },
        )

        with pytest.raises(connect.McpOAuthError, match="issuer mismatch"):
            await connect._resolve_as_metadata(
                None, self.ADVERTISED, "https://mcp.example.com/mcp"
            )

        assert asked == [self.ADVERTISED], "it went looking off-host"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("advertised", "claimed"),
        [
            ("https://as.example.com:443/oauth2", "https://as.example.com"),
            ("http://as.example.com:80/oauth2", "http://as.example.com"),
        ],
    )
    async def test_a_default_port_is_the_same_origin(
        self, monkeypatch, advertised, claimed
    ):
        """``https://host`` and ``https://host:443`` are one origin, not two.

        Comparing the split tuple as strings reads them as different hosts,
        refuses the correction, and leaves an AS that spells its own default
        port out unreachable. Only this direction is expressible: the claimed
        identifier comes back through ``OAuthMetadata``, which normalises a
        default port away before anything here sees it, so the explicit form can
        only ever arrive on the advertised side.
        """
        real = _as_metadata(issuer=claimed)
        asked = self._served(
            monkeypatch, {advertised: _as_metadata(issuer=claimed), claimed: real}
        )

        identifier, resolved = await connect._resolve_as_metadata(
            None, advertised, "https://mcp.example.com/mcp"
        )

        assert identifier == claimed
        assert resolved is real
        assert asked == [advertised, claimed], "the recheck never ran"

    def test_a_port_it_cannot_parse_is_not_quietly_the_same_origin(self):
        """A malformed port is a mismatch, never a match by accident.

        Asserted on the predicate rather than through a flow, because it cannot
        be reached through one: every claimed identifier is validated by
        ``OAuthMetadata`` first, which rejects this outright.
        """
        assert not connect._same_origin(
            "https://as.example.com:notaport", "https://as.example.com"
        )
        assert not connect._same_origin(
            "https://as.example.com", "https://as.example.com:notaport"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "other",
        [
            "http://as.example.com",  # scheme
            "https://as.example.com:8443",  # port
            "https://sub.as.example.com",  # host
        ],
    )
    async def test_same_origin_is_scheme_host_and_port(self, monkeypatch, other):
        self._served(
            monkeypatch,
            {self.ADVERTISED: _as_metadata(issuer=other), other: _as_metadata(issuer=other)},
        )

        with pytest.raises(connect.McpOAuthError, match="issuer mismatch"):
            await connect._resolve_as_metadata(
                None, self.ADVERTISED, "https://mcp.example.com/mcp"
            )

    @pytest.mark.asyncio
    async def test_a_claim_with_nothing_published_behind_it_is_refused(
        self, monkeypatch
    ):
        # Same origin, so it is worth a second look, but nothing answers there:
        # no second opinion, so the original mismatch stands. Named by the hop
        # that came up empty rather than by the generic issuer check, which has
        # no second document to hold this one to.
        self._served(monkeypatch, {self.ADVERTISED: _as_metadata(issuer=self.ORIGIN)})

        with pytest.raises(
            connect.McpOAuthError, match="publishes no metadata of its own"
        ):
            await connect._resolve_as_metadata(
                None, self.ADVERTISED, "https://mcp.example.com/mcp"
            )

    @pytest.mark.asyncio
    async def test_the_second_document_has_to_hold_up_on_its_own(self, monkeypatch):
        # It answered, and then named a third identifier. Following that would be
        # the same waiver one hop further out.
        self._served(
            monkeypatch,
            {
                self.ADVERTISED: _as_metadata(issuer=self.ORIGIN),
                self.ORIGIN: _as_metadata(issuer=f"{self.ORIGIN}/somewhere-else"),
            },
        )

        with pytest.raises(connect.McpOAuthError, match="issuer mismatch"):
            await connect._resolve_as_metadata(
                None, self.ADVERTISED, "https://mcp.example.com/mcp"
            )

    @pytest.mark.asyncio
    async def test_with_no_advertised_identifier_there_is_nothing_to_bind_to(
        self, monkeypatch
    ):
        # No resource metadata, so discovery fell back to the server's own URL
        # and there is no advertised identifier to check against.
        meta = _as_metadata(issuer=self.ORIGIN)
        self._served(monkeypatch, {None: meta})

        identifier, resolved = await connect._resolve_as_metadata(
            None, None, "https://mcp.example.com/mcp"
        )

        assert (identifier, resolved) == (None, meta)

    @pytest.mark.asyncio
    async def test_nothing_found_is_left_for_the_caller_to_report(self, monkeypatch):
        self._served(monkeypatch, {})

        identifier, resolved = await connect._resolve_as_metadata(
            None, self.ADVERTISED, "https://mcp.example.com/mcp"
        )

        assert (identifier, resolved) == (self.ADVERTISED, None)


# ---------------------------------------------------------------------------
# Consent — settled before the network, and installed on the live grants
# ---------------------------------------------------------------------------


class TestConsentIsSettledFirst:
    """A brokerage connect that has no selection to record is refused, and it
    is refused before anything is created at the vendor."""

    @staticmethod
    def _brokerage(phase1, monkeypatch) -> list[str]:
        phase1.catalog_row = {"name": "ibkr", "url": IBKR_URL, "transport": "http"}
        touched: list[str] = []

        async def _discover(client, server_url):
            touched.append("discover")
            return None, phase1.as_metadata, ISSUER, None

        async def _register_client(client, **kwargs):
            touched.append("register")
            return connect._Registration(phase1.client_info)

        monkeypatch.setattr(connect, "_discover", _discover)
        monkeypatch.setattr(connect, "_register_client", _register_client)
        return touched

    @pytest.mark.asyncio
    async def test_a_missing_selection_costs_the_vendor_no_registration(
        self, redis, phase1, monkeypatch
    ):
        """The refusal used to come after DCR had already created a client.

        Nothing persists that client -- the state record is what would have
        carried it, and this raises before there is one -- so every retry by an
        older page or a script posting straight at the endpoint left another
        orphan registration behind at the vendor.
        """
        touched = self._brokerage(phase1, monkeypatch)

        with pytest.raises(McpOAuthError, match="capability selection"):
            await start_connect(USER_ID, "ibkr")

        assert touched == []
        assert not redis.store

    @pytest.mark.asyncio
    async def test_a_selection_still_reaches_the_vendor(
        self, redis, phase1, monkeypatch
    ):
        """The control: moving the check earlier must not gate the happy path."""
        touched = self._brokerage(phase1, monkeypatch)

        await start_connect(USER_ID, "ibkr", granted_capabilities=["market_data"])

        assert touched == ["discover", "register"]
        assert redis.only_record()["granted_capabilities"] == ["market_data"]


class TestOlderShapedStateAtABrokerage:
    """A state record parked before the consent field existed cannot settle.

    ``None`` there is not a selection, it is a flow that never asked, and the
    resolver reads it as granting none of the vendor's curated groups. So the
    connect that finished would be a broker the agent can see and cannot use.
    """

    @staticmethod
    def _relegacy(redis, started) -> None:
        """Re-park the record in the shape a build without the field wrote."""
        record = redis.only_record()
        record.pop("granted_capabilities", None)
        redis.store.clear()
        redis.park(started.state, record)

    @pytest.mark.asyncio
    async def test_it_is_refused_before_the_code_is_spent(
        self, redis, phase1, phase2, monkeypatch
    ):
        phase1.catalog_row = {"name": "ibkr", "url": IBKR_URL, "transport": "http"}

        async def _discover(client, server_url):
            return None, phase1.as_metadata, ISSUER, None

        monkeypatch.setattr(connect, "_discover", _discover)
        started = await start_connect(
            USER_ID, "ibkr", granted_capabilities=["market_data"]
        )
        self._relegacy(redis, started)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state&server=ibkr"
        # The authorization code is still good, so the retry the user is sent
        # back to gets a clean phase 1 rather than a burnt grant.
        assert phase2.requests == []
        assert phase2.upserts == []
        assert redis.states() == {}

    @pytest.mark.asyncio
    async def test_an_ordinary_server_is_left_alone(self, redis, phase1, phase2):
        """The control. Every non-brokerage record carries ``None`` here by
        design, and always will -- nothing curates their tools, so there is no
        question the flow could have asked."""
        started = await start_connect(USER_ID, SERVER_NAME)
        self._relegacy(redis, started)

        redirect = await _callback(started, code="auth-code-1")

        assert redirect == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert phase2.upserts[0]["granted_capabilities"] is None


class TestConsentReachesTheLiveGrants:
    """The relay authorizes off the grant, not the connection.

    So a reconnect that narrows consent has to rewrite the grants there and
    then, and the two writes are one decision: a connection row recording a
    narrowing that no grant enforces is the worst of the three outcomes, and
    it is the one nothing later repairs -- the version bump has not run either,
    so a warm session short-circuits on a matching version and never
    re-resolves.
    """

    @pytest.mark.asyncio
    async def test_the_grants_are_narrowed_before_sessions_are_told_to_re_resolve(
        self, redis, phase1, phase2
    ):
        started = await start_connect(USER_ID, SERVER_NAME)

        await _callback(started, code="auth-code-1")

        assert phase2.consented == ["connection-1"]
        assert phase2.transactions == ["commit"]
        assert phase2.bumps == [USER_ID]

    @pytest.mark.asyncio
    async def test_both_writes_ride_the_same_connection(
        self, redis, phase1, phase2
    ):
        """Atomicity is the point, so assert the mechanism and not just that
        both calls happened: two handles would be two transactions."""
        started = await start_connect(USER_ID, SERVER_NAME)

        await _callback(started, code="auth-code-1")

        handles = phase2.write_conns
        assert len(handles) == 2
        assert handles[0] is not None
        assert handles[0] is handles[1]

    @pytest.mark.asyncio
    async def test_a_failed_narrowing_takes_the_connection_row_with_it(
        self, redis, phase1, phase2
    ):
        """Fail closed, and fail whole. The alternative the old code took was
        to keep the connection and revoke the grants, which reports a connect
        that half happened; unwinding both leaves the user on the previous,
        already-consented connection, which is a state they have seen."""
        phase2.consent_error = RuntimeError("connection pool exhausted")
        started = await start_connect(USER_ID, SERVER_NAME)

        redirect = await _callback(started, code="auth-code-1")

        assert phase2.transactions == ["rollback"]
        assert redirect == (
            f"{DEFAULT_RETURN_TO}?mcp_error=internal&server={SERVER_NAME_Q}"
        )
        assert phase2.bumps == []
