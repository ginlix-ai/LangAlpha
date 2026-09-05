"""End-to-end egress relay round-trip: real generated client → real relay →
real streamable-HTTP MCP server.

The sibling ``test_egress_relay.py`` exhaustively unit-tests the relay *route*,
but it hand-builds JSON-RPC bytes and answers them from an ``httpx.MockTransport``
double. This module de-mocks both of those seams so the marquee path is proven
against real code on every side:

  * **Client** — the actual sandbox client emitted by
    ``ToolFunctionGenerator.generate_mcp_client_code`` is exec'd and driven
    through its *full* ``_call_mcp_tool`` (negotiation state machine, request
    builders, SSE/JSON reply parsing, result finalization) — not a stand-in.
  * **Server** — a real ``mcp==2.0.0`` ``MCPServer`` (the SDK GA server this
    migration targets), its ``streamable_http_app`` driven inside the SDK's own
    ``session_manager.run()``. A hand-written wire double could silently encode
    the same wrong assumption as the client; a real server can't.
  * **Relay** — the real ``POST /v1/egress/{grant}`` route, real relay JWT,
    real canonicalization and both-way header allowlists. Stubbed only at the
    same documented seams the route test stubs (grant row, vendor token, SSRF
    pin, redis limiter).

Everything runs in-process over ASGITransport — no sockets, no uvicorn, no DB.
The one bridge: the generated client speaks *sync* ``httpx.Client`` while the
apps are *async* ASGI, so ``_SyncOverAsync`` marshals each request onto the test
event loop and ``_call_mcp_tool`` runs in a worker thread.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from mcp.server.mcpserver import MCPServer

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.tool_generator import ToolFunctionGenerator
from src.server.services.egress.relay_jwt import mint_relay_jwt
from src.server.services.mcp_oauth.lifecycle import AccessToken
from src.server.utils.egress_guard import PinnedTarget
from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Invented fixture data (never copied from production)
# ---------------------------------------------------------------------------

SECRET = "roundtrip-relay-secret-000000000000000000000000"
USER_ID = "usr-roundtrip-0001"
WORKSPACE_ID = "11111111-2222-3333-4444-555555555555"
OTHER_WORKSPACE_ID = "66666666-7777-8888-9999-000000000000"
SANDBOX_ID = "sbx-roundtrip-0001"
GRANT_ID = "aaaaaaaa-0000-4000-8000-000000000abc"
CONNECTION_ID = "conn-roundtrip-0001"
SERVER_NAME = "rh_srv"

# The SDK server's DNS-rebinding guard allowlists ``127.0.0.1:*`` / ``localhost:*``;
# the relay stamps this as the outbound Host from the pinned target.
VENDOR_HOST = "127.0.0.1:8931"
VENDOR_URL = "http://127.0.0.1:8931/mcp"
VENDOR_TOKEN = "vendor-access-token-roundtrip"

# A modern spec revision no SDK server advertises — stamped into the client to
# make ``server/discover`` find no mutual version and fall back to legacy.
UNSUPPORTED_MODERN = ("2099-01-01",)


def _oauth_server() -> MCPServerConfig:
    """An OAuth-connected (relay-bound) HTTP server — the only kind the relay
    exists for. ``url`` is the mutable catalog value that must never leak into
    the generated client; the real destination lives host-side in the grant,
    and the grant id reaches the sandbox only through the credential file."""
    return MCPServerConfig(
        name=SERVER_NAME,
        transport="http",
        url="https://vendor.example.com/mcp",
        source="user",
        oauth_connection_id=CONNECTION_ID,
    )


def _grant() -> dict:
    return {
        "user_id": USER_ID,
        "workspace_id": WORKSPACE_ID,
        "connection_id": CONNECTION_ID,
        "destination_url": VENDOR_URL,
        "allowed_methods": ["POST"],
        "tool_denylist": None,
        "grant_status": "active",
        "connection_status": "connected",
    }


# ---------------------------------------------------------------------------
# The vendor: a real streamable-HTTP SDK server, wrapped to record what the
# relay actually put on the wire toward it.
# ---------------------------------------------------------------------------


def _build_vendor():
    srv = MCPServer("vendor-under-test")

    @srv.tool()
    def echo(text: str) -> dict:
        """Echo the text back with a fixed marker so a round-trip is provable."""
        return {"echoed": text, "marker": "vendor-ok"}

    app = srv.streamable_http_app()
    seen: dict[str, list] = {"authorization": [], "host": []}

    async def recorder(scope, receive, send):
        if scope["type"] == "http":
            hdrs = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            seen["authorization"].append(hdrs.get("authorization"))
            seen["host"].append(hdrs.get("host"))
        await app(scope, receive, send)

    return srv, recorder, seen


# ---------------------------------------------------------------------------
# Sync→async bridge for the generated client's httpx.Client
# ---------------------------------------------------------------------------


class _SyncOverAsync:
    """The sync ``httpx.Client`` surface the generated client uses, marshalling
    every request onto the test event loop against the in-memory relay app.

    Installed as the exec'd namespace's ``httpx`` only — the process-wide httpx
    is never patched, so nothing else in the suite is affected.
    """

    def __init__(self, loop, transport):
        self._loop = loop
        self._client = AsyncClient(
            transport=transport, base_url="http://relay", follow_redirects=False
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        asyncio.run_coroutine_threadsafe(self._client.aclose(), self._loop).result()
        return False

    def post(self, url, json=None, headers=None):
        return asyncio.run_coroutine_threadsafe(
            self._client.post(url, json=json, headers=headers), self._loop
        ).result()

    def stream(self, method, url, json=None, headers=None):
        # Mirror httpx.Client.stream. The generated client reads the reply via
        # the response's sync iter_lines()/iter_bytes(); those serve from the
        # buffered body once it is read, so the bridge opens the async stream and
        # aread()s it before handing the (same) response back for sync iteration.
        return _SyncStreamCtx(self._loop, self._client, method, url, json, headers)


class _SyncStreamCtx:
    """Sync context-manager surface of ``httpx.Client.stream`` over an async client."""

    def __init__(self, loop, client, method, url, json, headers):
        self._loop = loop
        self._cm = client.stream(method, url, json=json, headers=headers)

    def __enter__(self):
        response = asyncio.run_coroutine_threadsafe(
            self._cm.__aenter__(), self._loop
        ).result()
        asyncio.run_coroutine_threadsafe(response.aread(), self._loop).result()
        return response

    def __exit__(self, *exc):
        asyncio.run_coroutine_threadsafe(
            self._cm.__aexit__(*exc), self._loop
        ).result()
        return False


class _HttpxShim:
    """A stand-in ``httpx`` module for the generated code: its ``Client`` is the
    bridge; everything else (``HTTPError`` and friends) proxies to real httpx."""

    def __init__(self, loop, transport):
        self._loop = loop
        self._transport = transport

    def Client(self, *args, **kwargs):  # noqa: N802 - mirrors httpx.Client
        return _SyncOverAsync(self._loop, self._transport)

    def __getattr__(self, name):
        return getattr(httpx, name)


# ---------------------------------------------------------------------------
# Harness: wires client ↔ relay ↔ server and drives one tool call end to end.
# ---------------------------------------------------------------------------


class _Harness:
    def __init__(self, workdir: str):
        self.grant: dict | None = _grant()
        self.grant_lookups: list[str] = []
        self.token_lookups: list[str] = []
        self.srv, recorder, self.seen = _build_vendor()
        self._recorder = recorder
        self._relay_app = create_test_app(_relay_router())
        self._workdir = workdir
        self.ns: dict = {}
        self._write_relay_creds(user_id=USER_ID, workspace_id=WORKSPACE_ID)
        self._exec_generated_client()

    # -- setup helpers ------------------------------------------------------

    def _write_relay_creds(self, *, user_id: str, workspace_id: str) -> None:
        internal = Path(self._workdir) / "_internal"
        internal.mkdir(parents=True, exist_ok=True)
        token = mint_relay_jwt(
            SECRET, user_id=user_id, workspace_id=workspace_id, sandbox_id=SANDBOX_ID
        ).token
        (internal / ".egress_relay.json").write_text(
            json.dumps(
                {
                    "relay_base_url": "http://relay",
                    "token": token,
                    "grants": {SERVER_NAME: GRANT_ID},
                }
            )
        )
        self.relay_jwt = token

    def _exec_generated_client(self) -> None:
        code = ToolFunctionGenerator().generate_mcp_client_code(
            [_oauth_server()], working_dir=self._workdir
        )
        exec(compile(code, "gen_mcp_client", "exec"), self.ns)  # noqa: S102

    # -- lifecycle ----------------------------------------------------------

    @asynccontextmanager
    async def activate(self):
        loop = asyncio.get_running_loop()
        vendor_client = AsyncClient(
            transport=ASGITransport(app=self._recorder), follow_redirects=False
        )
        self.ns["httpx"] = _HttpxShim(loop, ASGITransport(app=self._relay_app))

        async def _fetch_grant(grant_id: str):
            self.grant_lookups.append(grant_id)
            return dict(self.grant) if self.grant is not None else None

        async def _pin(url: str, **kwargs):
            # In-process seam: the vendor is an ASGI app, not a routable host, so
            # the pin is fixed to its URL (documented harness stub). authority
            # carries the non-default port, mirroring the real guard.
            return PinnedTarget(
                url=VENDOR_URL, host="127.0.0.1", ip="127.0.0.1", authority=VENDOR_HOST
            )

        async def _ensure_token(connection_id: str):
            self.token_lookups.append(connection_id)
            return AccessToken(
                access_token=VENDOR_TOKEN, token_type="Bearer", generation=1
            )

        with ExitStack() as stack:
            p = stack.enter_context
            p(patch("src.server.services.egress.relay.EGRESS_RELAY_SECRET", SECRET))
            p(patch("src.server.services.egress.relay.fetch_grant_for_relay", _fetch_grant))
            p(patch("src.server.services.egress.relay.pin_public_url", _pin))
            p(patch("src.server.services.egress.relay.get_relay_client", lambda: vendor_client))
            p(
                patch(
                    "src.server.services.egress.relay.ensure_fresh_access_token",
                    _ensure_token,
                )
            )
            # Unreachable cache → the relay limiter's documented fail-open.
            p(
                patch(
                    "src.utils.cache.redis_cache.get_cache_client",
                    lambda: SimpleNamespace(enabled=False, client=None),
                )
            )
            async with self.srv.session_manager.run():
                yield self

        await vendor_client.aclose()

    # -- driving ------------------------------------------------------------

    async def call(self, tool: str, arguments: dict):
        """Drive the real generated client's full call path in a worker thread
        (it is sync; the relay/server apps live on this loop)."""
        return await asyncio.to_thread(
            self.ns["_call_mcp_tool"], SERVER_NAME, tool, arguments
        )

    def force_legacy(self) -> None:
        """Make discover find no mutual modern version → legacy fallback."""
        self.ns["_PROTO"].clear()
        self.ns["_MODERN_VERSIONS"] = UNSUPPORTED_MODERN

    @property
    def proto(self) -> dict | None:
        return self.ns["_PROTO"].get(SERVER_NAME)


def _relay_router():
    from src.server.app.egress_relay import router

    return router


@pytest.fixture
def harness(tmp_path):
    # Returned un-activated: each test enters ``activate()`` inside its own task
    # so the SDK session manager's anyio cancel scope is entered and exited in
    # one task (pytest-asyncio may finalize an async fixture in a different task).
    return _Harness(str(tmp_path))


# ===========================================================================
# Modern negotiation (2026-07-28)
# ===========================================================================


class TestModernRoundTrip:
    @pytest.mark.asyncio
    async def test_modern_tool_call_round_trips_through_the_relay(self, harness):
        async with harness.activate() as h:
            result = await h.call("echo", {"text": "hi"})

        # The vendor's dict came back through relay → client unwrap intact.
        assert result == {"echoed": "hi", "marker": "vendor-ok"}
        # The real client negotiated the modern era with the real server.
        assert harness.proto == {
            "mode": "modern",
            "version": "2026-07-28",
            "session_id": None,
            # Captured, not discarded: a real server's identity stamp survives
            # the relay hop, which is the only place that is provable.
            "server_info": {"name": "vendor-under-test", "version": ""},
        }

    @pytest.mark.asyncio
    async def test_both_legs_physically_traverse_the_relay(self, harness):
        async with harness.activate() as h:
            await h.call("echo", {"text": "hi"})

        # discover + tools/call each hit the grant lookup and mint a vendor
        # token — proof neither leg short-circuited around the relay.
        assert harness.grant_lookups == [GRANT_ID, GRANT_ID]
        assert harness.token_lookups == [CONNECTION_ID, CONNECTION_ID]

    @pytest.mark.asyncio
    async def test_relay_jwt_never_reaches_the_vendor(self, harness):
        async with harness.activate() as h:
            await h.call("echo", {"text": "hi"})

        # The vendor only ever saw the vendor bearer and the pinned Host — the
        # relay's both-way header discipline, proven against a real server.
        assert set(harness.seen["authorization"]) == {f"Bearer {VENDOR_TOKEN}"}
        assert harness.relay_jwt not in " ".join(
            a or "" for a in harness.seen["authorization"]
        )
        assert set(harness.seen["host"]) == {VENDOR_HOST}


# ===========================================================================
# Legacy fallback (2025-11-25, SSE-framed)
# ===========================================================================


class TestLegacyRoundTrip:
    @pytest.mark.asyncio
    async def test_legacy_fallback_round_trips_and_captures_the_session(self, harness):
        # A server one revision behind: discover finds no mutual modern version,
        # so the client falls back to `initialize` — the SDK answers SSE-framed
        # with an mcp-session-id, exercising the SSE reply parser + the relay's
        # session-id echo in both directions.
        harness.force_legacy()

        async with harness.activate() as h:
            result = await h.call("echo", {"text": "leg"})

        assert result == {"echoed": "leg", "marker": "vendor-ok"}
        proto = harness.proto
        assert proto["mode"] == "legacy"
        assert proto["version"] == "2025-11-25"
        assert proto["session_id"]  # captured from the initialize response
        assert set(harness.seen["authorization"]) == {f"Bearer {VENDOR_TOKEN}"}


# ===========================================================================
# Typed relay rejection reaches the client
# ===========================================================================


class TestRelayRejection:
    @pytest.mark.asyncio
    async def test_absent_grant_surfaces_the_typed_relay_error(self, harness):
        # The relay answers a uniform 404/not_found; the client must surface the
        # machine-readable code (via X-Relay-Error) rather than a raw HTTP error.
        harness.grant = None

        async with harness.activate() as h:
            with pytest.raises(RuntimeError, match="not_found"):
                await h.call("echo", {"text": "hi"})

        # Nothing reached the vendor — the relay rejected before dialing out.
        assert harness.seen["authorization"] == []
