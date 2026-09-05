"""Unit tests for ``POST /v1/egress/{grant_id}`` — the sandbox egress relay.

The relay is the only path from a sandbox to an OAuth vendor, so its posture is
the contract under test: relay-JWT authentication (never the app's user auth), a
uniform 404 for absent/wrong-scope grants, strict JSON-RPC canonicalization,
allowlisted headers in *both* directions, one-shot 401 retry on a rotated
bundle, and a server-side destination pinned through the SSRF guard.

Every seam is patched where ``relay`` imports it, never at the definition
module — one convention, so a reader of any patch line knows which binding it
replaces:
  * ``relay.EGRESS_RELAY_SECRET`` — the OSS kill switch
  * ``relay.fetch_grant_for_relay`` — the authorization read
  * ``relay.pin_public_url`` — the SSRF guard (real one in ``TestSsrfPosture``)
  * ``relay.get_relay_client`` — the shared upstream client (httpx.MockTransport)
  * ``relay.{ensure_fresh_access_token,current_access_token,
    mark_connection_needs_reauth}`` — the vendor credential, the 401 re-read,
    and the needs_reauth report (the relay never writes connection status)
"""

from __future__ import annotations

import asyncio
import json
from contextlib import ExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from starlette.middleware.gzip import GZipMiddleware
from starlette.requests import Request

from src.server.services.egress.jsonrpc import MAX_BODY_BYTES
from src.server.services.egress.limits import RelayLimited
from src.server.services.egress.relay_jwt import mint_relay_jwt
from src.server.services.mcp_oauth.lifecycle import AccessToken
from src.server.utils.egress_guard import PinnedTarget
from src.server.utils.egress_guard import pin_public_url as real_pin_public_url
from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Invented fixtures data (never copied from production)
# ---------------------------------------------------------------------------

SECRET = "unit-test-relay-secret-0000000000"
OTHER_SECRET = "unit-test-relay-secret-1111111111"

USER_ID = "usr-egress-unit-0001"
OTHER_USER_ID = "usr-egress-unit-0002"
WORKSPACE_ID = "11111111-2222-3333-4444-555555555555"
OTHER_WORKSPACE_ID = "66666666-7777-8888-9999-000000000000"
SANDBOX_ID = "sbx-egress-unit-0001"
GRANT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
CONNECTION_ID = "ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb"

VENDOR_HOST = "mcp.vendor-under-test.example"
DESTINATION = f"https://{VENDOR_HOST}/mcp"
PINNED_IP = "198.51.100.7"  # TEST-NET-2 — stand-in for the stubbed guard only
# The real guard resolves nothing for an IP literal but demands is_global, and
# the documentation ranges (TEST-NET, RFC5737) all fail that. This one passes.
PUBLIC_IP = "93.184.216.34"

ACCESS_TOKEN = "vendor-access-token-generation-1"
ROTATED_TOKEN = "vendor-access-token-generation-2"

RELAY_PATH = f"/v1/egress/{GRANT_ID}"


def _grant(**overrides) -> dict:
    """The row shape ``fetch_grant_for_relay`` hands the relay.

    Authorization columns only — no credential. The vendor token comes from the
    OAuth lifecycle, so a drift back to a decrypting grant read shows up here.
    """
    row = {
        "user_id": USER_ID,
        "workspace_id": WORKSPACE_ID,
        "connection_id": CONNECTION_ID,
        "destination_url": DESTINATION,
        "allowed_methods": ["POST"],
        "tool_denylist": None,
        "grant_status": "active",
        "connection_status": "connected",
    }
    row.update(overrides)
    return row


def _jwt(
    *,
    secret: str = SECRET,
    user_id: str = USER_ID,
    workspace_id: str = WORKSPACE_ID,
    sandbox_id: str = SANDBOX_ID,
    ttl_seconds: int = 3600,
) -> str:
    return mint_relay_jwt(
        secret,
        user_id=user_id,
        workspace_id=workspace_id,
        sandbox_id=sandbox_id,
        ttl_seconds=ttl_seconds,
    ).token


def _rpc(method: str = "tools/call", name: str = "list_positions") -> bytes:
    frame: dict = {"jsonrpc": "2.0", "id": 7, "method": method}
    if method == "tools/call":
        frame["params"] = {"name": name, "arguments": {}}
    return json.dumps(frame).encode()


# ---------------------------------------------------------------------------
# Vendor double — records every dial, replays scripted responses
# ---------------------------------------------------------------------------


class _Stream(httpx.AsyncByteStream):
    """A vendor body that reports when the relay closes it."""

    def __init__(self, chunks: list[bytes]):
        self.chunks = list(chunks)
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def _vendor_json(status: int = 200, headers: dict | None = None, body: dict | None = None):
    """Factory for a fresh buffered vendor response (streams are one-shot)."""

    def _make() -> httpx.Response:
        return httpx.Response(
            status,
            headers={"content-type": "application/json", **(headers or {})},
            content=json.dumps(
                body if body is not None else {"jsonrpc": "2.0", "id": 7, "result": {"ok": True}}
            ).encode(),
        )

    return _make


class _SlowStream(httpx.AsyncByteStream):
    """First chunk lands at once; the next one never arrives inside the budget."""

    def __init__(self, first: bytes, stall_s: float):
        self.first = first
        self.stall_s = stall_s
        self.closed = False

    async def __aiter__(self):
        yield self.first
        await asyncio.sleep(self.stall_s)
        yield b"data: too-late\n\n"

    async def aclose(self) -> None:
        self.closed = True


def _vendor_sse(stream: httpx.AsyncByteStream, headers: dict | None = None):
    def _make() -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream", **(headers or {})},
            stream=stream,
        )

    return _make


class _Vendor:
    def __init__(self, *factories):
        self.factories = list(factories)
        self.requests: list[httpx.Request] = []
        self.client = httpx.AsyncClient(
            transport=httpx.MockTransport(self._handle), follow_redirects=False
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests) - 1, len(self.factories) - 1)
        return self.factories[index]()

    @property
    def sends(self) -> int:
        return len(self.requests)

    @property
    def last(self) -> httpx.Request:
        return self.requests[-1]


# ---------------------------------------------------------------------------
# Environment fixture — every seam, mutable per test
# ---------------------------------------------------------------------------


class RelayEnv:
    def __init__(self):
        self.grant: dict | None = _grant()
        self.grant_lookups: list[str] = []
        self.token = AccessToken(
            access_token=ACCESS_TOKEN, token_type="Bearer", generation=1
        )
        self.token_error: Exception | None = None
        # What a re-read after a vendor 401 finds stored right now.
        self.connection: AccessToken | None = None
        # (connection_id, generation) the relay reported as vendor-rejected.
        self.reauth_reports: list[tuple[str, int]] = []
        # Default-port destination → the Host authority is the bare hostname.
        self.pin = PinnedTarget(
            url=DESTINATION, host=VENDOR_HOST, ip=PINNED_IP, authority=VENDOR_HOST
        )
        self._vendors: list[_Vendor] = []
        self.vendor = self.set_vendor(_vendor_json())

    def set_vendor(self, *factories) -> _Vendor:
        vendor = _Vendor(*factories)
        self._vendors.append(vendor)
        self.vendor = vendor
        return vendor

    async def aclose(self) -> None:
        for vendor in self._vendors:
            await vendor.client.aclose()


@pytest_asyncio.fixture
async def env():
    e = RelayEnv()

    async def _fetch_grant(grant_id: str):
        e.grant_lookups.append(grant_id)
        return dict(e.grant) if e.grant is not None else None

    async def _pin(url: str, **kwargs):
        return e.pin

    async def _ensure_token(connection_id: str):
        if e.token_error is not None:
            raise e.token_error
        return e.token

    async def _current_token(connection_id: str):
        return e.connection

    async def _mark_needs_reauth(connection_id: str, *, seen_token_generation: int):
        e.reauth_reports.append((connection_id, seen_token_generation))
        return True

    with ExitStack() as stack:
        p = stack.enter_context
        p(patch("src.server.services.egress.relay.EGRESS_RELAY_SECRET", SECRET))
        p(patch("src.server.services.egress.relay.fetch_grant_for_relay", _fetch_grant))
        p(patch("src.server.services.egress.relay.pin_public_url", _pin))
        p(patch("src.server.services.egress.relay.get_relay_client", lambda: e.vendor.client))
        p(patch("src.server.services.egress.relay.ensure_fresh_access_token", _ensure_token))
        p(patch("src.server.services.egress.relay.current_access_token", _current_token))
        p(
            patch(
                "src.server.services.egress.relay.mark_connection_needs_reauth",
                _mark_needs_reauth,
            )
        )
        # Real acquire_slot with an unreachable cache → the documented fail-open.
        p(
            patch(
                "src.utils.cache.redis_cache.get_cache_client",
                lambda: SimpleNamespace(enabled=False, client=None),
            )
        )
        yield e

    await e.aclose()


def _build_app(*, gzip: bool = False):
    from src.server.app.egress_relay import router

    app = create_test_app(router)
    if gzip:
        # Mirrors setup.py: GZipMiddleware(minimum_size=1000) wraps the relay too.
        app.add_middleware(GZipMiddleware, minimum_size=1000)
    return app


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=_build_app()), base_url="http://testserver"
    ) as c:
        yield c


@pytest_asyncio.fixture
async def gzip_client():
    async with AsyncClient(
        transport=ASGITransport(app=_build_app(gzip=True)), base_url="http://testserver"
    ) as c:
        yield c


async def _post(
    client: AsyncClient,
    *,
    token: str | None = None,
    body: bytes | None = None,
    path: str = RELAY_PATH,
    headers: dict | None = None,
) -> httpx.Response:
    hdrs = {"content-type": "application/json"}
    if token is not None:
        hdrs["authorization"] = f"Bearer {token}"
    hdrs.update(headers or {})
    return await client.post(path, content=_rpc() if body is None else body, headers=hdrs)


def _error(response: httpx.Response) -> str | None:
    return response.headers.get("x-relay-error")


async def _call_route(headers: dict[str, str], *, body: bytes | None = None):
    """Drive the route callable directly, for exact control of the wire headers.

    httpx always injects its own ``accept``/``user-agent``, so an ASGI client
    cannot express "the sandbox sent no Accept" or a mid-stream disconnect.
    """
    from src.server.app.egress_relay import relay as relay_route

    payload = _rpc() if body is None else body
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": RELAY_PATH,
        "raw_path": RELAY_PATH.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "client": ("127.0.0.1", 51234),
        "server": ("testserver", 80),
    }
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": payload, "more_body": False}

    return await relay_route(GRANT_ID, Request(scope, receive))


# ===========================================================================
# 1. Relay authentication
# ===========================================================================


class TestRelayAuth:
    @pytest.mark.asyncio
    async def test_missing_bearer_is_rejected_even_though_user_auth_is_bypassed(
        self, env, client
    ):
        # create_test_app overrides get_current_user_id — a logged-in browser
        # identity must still buy nothing here. Only the relay JWT counts.
        resp = await _post(client, token=None)

        assert resp.status_code == 401
        assert _error(resp) == "relay_auth"
        assert env.grant_lookups == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "header",
        ["", "garbage", "Basic dXNlcjpwYXNz", "Bearer", "Bearer not-a-jwt", "bearer "],
    )
    async def test_garbage_authorization_headers_are_rejected(self, env, client, header):
        resp = await client.post(
            RELAY_PATH, content=_rpc(), headers={"authorization": header}
        )

        assert resp.status_code == 401
        assert _error(resp) == "relay_auth"
        assert env.grant_lookups == []

    @pytest.mark.asyncio
    async def test_jwt_signed_with_another_secret_is_rejected(self, env, client):
        resp = await _post(client, token=_jwt(secret=OTHER_SECRET))

        assert resp.status_code == 401
        assert _error(resp) == "relay_auth"
        assert env.grant_lookups == []

    @pytest.mark.asyncio
    async def test_the_bearer_scheme_match_is_case_insensitive(self, env, client):
        resp = await client.post(
            RELAY_PATH,
            content=_rpc(),
            headers={"authorization": f"BEARER {_jwt()}", "content-type": "application/json"},
        )

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_expired_jwt_is_a_relay_auth_error_not_a_404(self, env, client):
        # LEEWAY_SECONDS is 30, so the TTL has to clear that to actually expire.
        resp = await _post(client, token=_jwt(ttl_seconds=-120))

        assert resp.status_code == 401
        assert _error(resp) == "relay_auth"
        assert _error(resp) != "not_found"
        assert env.grant_lookups == []

    @pytest.mark.asyncio
    async def test_workspace_mismatch_is_a_uniform_404(self, env, client):
        # Valid signature, wrong workspace: the relay must not confirm the
        # grant exists.
        resp = await _post(client, token=_jwt(workspace_id=OTHER_WORKSPACE_ID))

        assert resp.status_code == 404
        assert _error(resp) == "not_found"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_user_mismatch_is_a_uniform_404(self, env, client):
        resp = await _post(client, token=_jwt(user_id=OTHER_USER_ID))

        assert resp.status_code == 404
        assert _error(resp) == "not_found"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_sandbox_id_is_carried_but_not_authorized_against(self, env, client):
        # Intentional (documented in prepare_relay): sandbox_id is an audit
        # claim, not an authz input — workspace↔sandbox is 1:1, so a stale
        # sandbox's JWT reaches exactly the grants its workspace owns anyway.
        # Locked so adding a sandbox binding later says so out loud.
        resp = await _post(client, token=_jwt(sandbox_id="sbx-egress-unit-9999"))

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_auth_precedes_the_body_inspection(self, env, client):
        # A body far over the cap with a bad token must answer the auth error,
        # never the body error — the cap must not be an oracle for anything.
        huge = b'{"jsonrpc":"2.0","id":1,"method":"tools/list","pad":"' + b"x" * (
            MAX_BODY_BYTES + 4096
        ) + b'"}'

        resp = await client.post(
            RELAY_PATH, content=huge, headers={"authorization": "Bearer not-a-jwt"}
        )

        assert resp.status_code == 401
        assert _error(resp) == "relay_auth"
        assert _error(resp) != "bad_request"
        assert env.grant_lookups == []

    @pytest.mark.asyncio
    async def test_relay_is_inert_without_a_configured_secret(self, env, client):
        with patch("src.server.services.egress.relay.EGRESS_RELAY_SECRET", ""):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 503
        assert _error(resp) == "relay_disabled"
        assert env.grant_lookups == []


# ===========================================================================
# 2. Grant authorization
# ===========================================================================


class TestGrantAuthorization:
    @pytest.mark.asyncio
    async def test_absent_grant_is_404(self, env, client):
        env.grant = None

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 404
        assert _error(resp) == "not_found"

    @pytest.mark.asyncio
    async def test_absent_and_wrong_scope_grants_are_indistinguishable(self, env, client):
        env.grant = None
        absent = await _post(client, token=_jwt())

        env.grant = _grant(user_id=OTHER_USER_ID)
        wrong_user = await _post(client, token=_jwt())

        env.grant = _grant(workspace_id=OTHER_WORKSPACE_ID)
        wrong_ws = await _post(client, token=_jwt())

        env.grant = _grant(grant_status="revoked")
        revoked = await _post(client, token=_jwt())

        signatures = {
            (r.status_code, _error(r), r.text)
            for r in (absent, wrong_user, wrong_ws, revoked)
        }
        assert len(signatures) == 1
        assert signatures == {(404, "not_found", "not_found")}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("connection_status", ["needs_reauth", "revoked", "pending"])
    async def test_dead_connection_status_gets_the_distinct_needs_reauth_code(
        self, env, client, connection_status
    ):
        env.grant = _grant(connection_status=connection_status)

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 401
        assert _error(resp) == "needs_reauth"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("connection_status", ["connected", "refresh_ambiguous"])
    async def test_serving_connection_statuses_pass(self, env, client, connection_status):
        env.grant = _grant(connection_status=connection_status)

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_a_tool_the_grant_denies_is_blocked(self, env, client):
        env.grant = _grant(tool_denylist=["place_order"])

        resp = await _post(client, token=_jwt(), body=_rpc(name="place_order"))

        assert resp.status_code == 403
        assert _error(resp) == "tool_blocked"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_a_tool_the_grant_does_not_deny_passes(self, env, client):
        env.grant = _grant(tool_denylist=["place_order"])

        resp = await _post(client, token=_jwt(), body=_rpc(name="list_positions"))

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_a_tool_no_group_names_passes(self, env, client):
        """The policy regulates what is curated and does not block what is not.

        A vendor publishing a tool between our releases is the case this exists
        for: it appears in no capability group, so it appears in no denial, and
        the connector goes on covering what the broker offers instead of
        refusing until a deploy catches up.
        """
        env.grant = _grant(tool_denylist=["place_order"])

        resp = await _post(client, token=_jwt(), body=_rpc(name="brand_new_tool"))

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_an_empty_denial_blocks_nothing(self, env, client):
        """Empty and NULL are the same answer now, which they were not before.

        Under the allowlist an empty list served nothing, so a derivation that
        came out empty failed shut and was obvious. Here it serves everything,
        which is why ``policy_required`` can no longer prove a policy is right.
        """
        env.grant = _grant(tool_denylist=[])

        resp = await _post(client, token=_jwt(), body=_rpc(name="place_order"))

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_a_brokerage_grant_with_no_policy_is_refused(self, env, client):
        """The fail-open hole the denylist inversion opened, closed here.

        A shipped broker always derives a denial -- an uncurated one derives the
        empty list -- so NULL is a bug, not a state: a sync that failed, a
        rollback, a migration that blanked the column. It has to be refused at
        the one point that can still see the difference, because everything
        downstream reads it as "no policy" and passes the vendor's order tools.
        """
        env.grant = _grant(
            destination_url="https://mcp.moomoo.com/mcp", tool_denylist=None
        )

        resp = await _post(client, token=_jwt(), body=_rpc(name="trading_order_place"))

        assert resp.status_code == 403
        assert _error(resp) == "policy_missing"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_a_grant_on_a_users_own_server_still_needs_no_policy(
        self, env, client
    ):
        """The half that must not move: NULL is legitimate everywhere else.

        Most OAuth MCP servers are the user's own and have no curation to
        derive from, so making NULL fatal in general would break every one of
        them. It is fatal only where a policy was supposed to exist.
        """
        env.grant = _grant(tool_denylist=None)

        resp = await _post(client, token=_jwt(), body=_rpc(name="place_order"))

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_a_missing_policy_does_not_break_discovery(self, env, client):
        """Only tools/call is gated, and the rest of the session has to survive.

        Refusing ``tools/list`` or ``initialize`` here would take the whole
        connector down rather than the tools the user declined, which is a
        louder failure than the one being prevented and a different one.
        """
        env.grant = _grant(
            destination_url="https://mcp.moomoo.com/mcp", tool_denylist=None
        )

        resp = await _post(client, token=_jwt(), body=_rpc(method="tools/list"))

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_a_variant_spelling_of_a_denied_tool_is_still_blocked(
        self, env, client
    ):
        """The denial is exact strings; the vendor decides what it matches.

        Under an allowlist a variant spelling failed shut and cost the user a
        working tool. Under a denylist the same variant sails through, so the
        comparison folds case and width on the call side only -- what is
        forwarded is still the exact bytes the sandbox sent.
        """
        env.grant = _grant(tool_denylist=["place_order"])

        resp = await _post(client, token=_jwt(), body=_rpc(name="Place_Order"))

        assert resp.status_code == 403
        assert _error(resp) == "tool_blocked"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_a_grant_without_post_in_allowed_methods_is_blocked(
        self, env, client
    ):
        # allowed_methods is an HTTP-verb policy (DB default '["POST"]').
        # The route is POST-only, so the check bites exactly when a grant is
        # deliberately narrowed away from POST — a soft kill switch.
        for narrowed in ([], ["GET"]):
            env.grant = _grant(allowed_methods=narrowed)

            resp = await _post(client, token=_jwt(), body=_rpc(method="tools/call"))

            assert resp.status_code == 403
            assert _error(resp) == "method_blocked"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_the_default_post_grant_relays(self, env, client):
        env.grant = _grant(allowed_methods=["POST"])

        resp = await _post(client, token=_jwt(), body=_rpc(method="tools/call"))

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_grant_id_spellings_collapse_to_one_canonical_key(
        self, env, client
    ):
        """Postgres' uuid cast accepts every spelling of the same grant id, but
        the limiter keys Redis by the string — an uncanonicalized id would mint
        a fresh rate bucket per spelling."""
        env.grant = _grant()
        slot_keys: list[str] = []

        @asynccontextmanager
        async def _slot(grant_id: str):
            slot_keys.append(grant_id)
            yield

        respelled = GRANT_ID.replace("-", "").upper()
        with patch("src.server.app.egress_relay.acquire_slot", _slot):
            resp = await _post(
                client, token=_jwt(), path=f"/v1/egress/{respelled}"
            )

        assert resp.status_code == 200
        assert slot_keys == [GRANT_ID]
        assert env.grant_lookups == [GRANT_ID]

    @pytest.mark.asyncio
    async def test_refresh_in_progress_is_a_503_not_a_reauth_prompt(self, env, client):
        from src.server.services.mcp_oauth.lifecycle import TokenUnavailable

        env.token_error = TokenUnavailable("refresh_in_progress")

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 503
        assert _error(resp) == "refresh_in_progress"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_unavailable_token_falls_through_to_needs_reauth(self, env, client):
        from src.server.services.mcp_oauth.lifecycle import TokenUnavailable

        env.token_error = TokenUnavailable("revoked")

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 401
        assert _error(resp) == "needs_reauth"
        assert env.vendor.sends == 0


# ===========================================================================
# 3. Body handling — strict JSON-RPC canonicalization
# ===========================================================================


class TestBodyHandling:
    @pytest.mark.asyncio
    async def test_oversized_body_is_rejected(self, env, client):
        huge = b'{"jsonrpc":"2.0","id":1,"method":"tools/list","pad":"' + b"x" * (
            MAX_BODY_BYTES + 4096
        ) + b'"}'

        resp = await _post(client, token=_jwt(), body=huge)

        assert resp.status_code == 400
        assert _error(resp) == "bad_request"
        assert "exceeds" in resp.text
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_batch_arrays_are_rejected(self, env, client):
        batch = json.dumps(
            [
                {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                 "params": {"name": "place_order"}},
            ]
        ).encode()

        resp = await _post(client, token=_jwt(), body=batch)

        assert resp.status_code == 400
        assert _error(resp) == "bad_request"
        assert "batch" in resp.text
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_duplicate_keys_are_rejected(self, env, client):
        # The classic policy-smuggle: the allowlist check reads one method,
        # a lenient vendor parser reads the other.
        dup = (
            b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
            b'"params":{"name":"list_positions"},'
            b'"params":{"name":"place_order"}}'
        )

        resp = await _post(client, token=_jwt(), body=dup)

        assert resp.status_code == 400
        assert _error(resp) == "bad_request"
        assert "duplicate key" in resp.text
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("body", "needle"),
        [
            (b"", "not valid JSON"),
            (b"not json at all", "not valid JSON"),
            (b'"a string"', "single object"),
            (b'{"id":1,"method":"tools/list"}', '"jsonrpc": "2.0"'),
            (b'{"jsonrpc":"1.0","id":1,"method":"tools/list"}', '"jsonrpc": "2.0"'),
            (b'{"jsonrpc":"2.0","id":1}', "string method"),
            (b'{"jsonrpc":"2.0","id":1,"method":42}', "string method"),
            (b'{"jsonrpc":"2.0","id":1,"method":"tools/call"}', "object params"),
            (
                b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}',
                "string params.name",
            ),
            (b'{"jsonrpc":"2.0","id":1,"method":"\xff\xfe"}', "not valid UTF-8"),
        ],
    )
    async def test_malformed_frames_are_rejected(self, env, client, body, needle):
        resp = await _post(client, token=_jwt(), body=body)

        assert resp.status_code == 400
        assert _error(resp) == "bad_request"
        assert needle in resp.text
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_policy_is_derived_from_the_body_not_from_client_headers(
        self, env, client
    ):
        # There is no header-declared method to disagree with the frame: the
        # denial reads the canonicalized body, so a header advertising an
        # innocent method buys the caller nothing.
        env.grant = _grant(tool_denylist=["place_order"])

        resp = await _post(
            client,
            token=_jwt(),
            body=_rpc(name="place_order"),
            headers={"mcp-method": "tools/list", "x-mcp-method": "tools/list"},
        )

        assert resp.status_code == 403
        assert _error(resp) == "tool_blocked"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_notification_frames_without_an_id_are_relayed(self, env, client):
        frame = b'{"jsonrpc":"2.0","method":"notifications/initialized"}'

        resp = await _post(client, token=_jwt(), body=frame)

        assert resp.status_code == 200
        assert env.vendor.last.content == frame

    @pytest.mark.asyncio
    async def test_the_forwarded_body_is_the_canonical_reserialization(self, env, client):
        # The vendor never sees the client's raw bytes — policy is enforced on
        # exactly the bytes that get forwarded.
        padded = b'{ "jsonrpc" : "2.0" ,  "id" : 7 , "method" : "tools/list" }'

        resp = await _post(client, token=_jwt(), body=padded)

        assert resp.status_code == 200
        assert env.vendor.last.content == b'{"jsonrpc":"2.0","id":7,"method":"tools/list"}'


# ===========================================================================
# 4. Header allowlists, both directions
# ===========================================================================


class TestOutboundHeaders:
    @pytest.mark.asyncio
    async def test_relay_jwt_is_never_forwarded_and_vendor_bearer_is_attached(
        self, env, client
    ):
        token = _jwt()

        resp = await _post(client, token=token)

        assert resp.status_code == 200
        sent = env.vendor.last.headers
        assert sent["authorization"] == f"Bearer {ACCESS_TOKEN}"
        assert token not in " ".join(sent.values())

    @pytest.mark.asyncio
    async def test_vendor_token_type_from_the_bundle_is_honoured(self, env, client):
        env.token = AccessToken(
            access_token=ACCESS_TOKEN, token_type="DPoP", generation=1
        )

        await _post(client, token=_jwt())

        assert env.vendor.last.headers["authorization"] == f"DPoP {ACCESS_TOKEN}"

    @pytest.mark.asyncio
    async def test_mcp_transport_headers_pass_through(self, env, client):
        await _post(
            client,
            token=_jwt(),
            headers={
                "mcp-session-id": "session-egress-unit-0001",
                "mcp-protocol-version": "2025-06-18",
                "accept": "application/json, text/event-stream",
            },
        )

        sent = env.vendor.last.headers
        assert sent["mcp-session-id"] == "session-egress-unit-0001"
        assert sent["mcp-protocol-version"] == "2025-06-18"
        assert "application/json" in sent["accept"]
        assert "text/event-stream" in sent["accept"]
        assert "application/json" in sent["content-type"]

    @pytest.mark.asyncio
    async def test_modern_negotiation_headers_pass_through(self, env, client):
        # The 2026-07-28 stateless negotiation rides Mcp-Method / Mcp-Name;
        # without them a modern server can't discover through the relay and
        # every connector silently pins to the legacy handshake.
        await _post(
            client,
            token=_jwt(),
            body=_rpc(method="server/discover"),
            headers={"mcp-method": "server/discover", "mcp-name": "probe_tool"},
        )

        sent = env.vendor.last.headers
        assert sent["mcp-method"] == "server/discover"
        assert sent["mcp-name"] == "probe_tool"

    @pytest.mark.asyncio
    async def test_a_call_reaches_the_vendor_under_the_name_the_gate_read(
        self, env, client
    ):
        # Both headers are agent-writable and the gate reads the body, so a
        # vendor routing on Mcp-Name would otherwise run a tool the policy
        # never saw. The body wins, and it is the body that was checked.
        await _post(
            client,
            token=_jwt(),
            body=_rpc(name="get_accounts"),
            headers={"mcp-method": "server/discover", "mcp-name": "place_equity_order"},
        )

        sent = env.vendor.last.headers
        assert sent["mcp-name"] == "get_accounts"
        assert sent["mcp-method"] == "tools/call"

    @pytest.mark.asyncio
    async def test_cookies_and_client_host_never_reach_the_vendor(self, env, client):
        await _post(
            client,
            token=_jwt(),
            headers={
                "cookie": "session=fake-browser-session; theme=dark",
                "x-forwarded-for": "203.0.113.9",
                "user-agent": "sandbox-generated-client/1.0",
            },
        )

        sent = env.vendor.last.headers
        assert "cookie" not in sent
        assert "x-forwarded-for" not in sent
        assert "sandbox-generated-client" not in sent.get("user-agent", "")
        # Host is set server-side from the pinned target, never echoed from
        # the sandbox's request line.
        assert sent["host"] == VENDOR_HOST
        assert sent["host"] != "testserver"

    @pytest.mark.asyncio
    async def test_defaults_apply_singly_when_the_sandbox_sends_neither(self, env):
        response = await _call_route({"authorization": f"Bearer {_jwt()}"})
        async for _ in response.body_iterator:
            pass

        sent = env.vendor.last.headers
        assert sent.get_list("content-type") == ["application/json"]
        assert sent.get_list("accept") == ["application/json, text/event-stream"]

    @pytest.mark.asyncio
    async def test_client_supplied_accept_and_content_type_are_sent_exactly_once(
        self, env, client
    ):
        # Regression: a case-preserving copy of starlette's lowercased names
        # plus a title-case setdefault once put both spellings on the wire
        # (`Content-Type: application/json, application/json` — RFC 9110
        # forbids it and strict servers/WAFs reject it). The client-supplied
        # value must win, once.
        await _post(
            client,
            token=_jwt(),
            headers={"accept": "application/json", "content-type": "application/json"},
        )

        sent = env.vendor.last.headers
        assert sent.get_list("content-type") == ["application/json"]
        assert sent.get_list("accept") == ["application/json"]

    @pytest.mark.asyncio
    async def test_only_allowlisted_names_survive_the_hop(self, env, client):
        await _post(
            client,
            token=_jwt(),
            headers={
                "x-api-key": "smuggled-key",
                "proxy-authorization": "Basic c21';",
                "forwarded": "for=203.0.113.9",
            },
        )

        sent = {k.lower() for k in env.vendor.last.headers.keys()}
        allowed = {
            "accept",
            "content-type",
            "mcp-protocol-version",
            "mcp-session-id",
            "mcp-method",
            "mcp-name",
            "authorization",
            "host",
            # httpx transport bookkeeping, added below the relay
            "content-length",
            "accept-encoding",
            "connection",
            "user-agent",
        }
        assert sent <= allowed, f"unexpected headers forwarded: {sent - allowed}"


class TestInboundHeaders:
    @pytest.mark.asyncio
    async def test_vendor_set_cookie_and_www_authenticate_are_stripped(self, env, client):
        env.set_vendor(
            _vendor_json(
                headers={
                    "set-cookie": "vendor_session=abc; HttpOnly",
                    "www-authenticate": 'Bearer realm="vendor", error="insufficient_scope"',
                    "x-vendor-internal": "leak-me",
                }
            )
        )

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 200
        assert "set-cookie" not in resp.headers
        assert "www-authenticate" not in resp.headers
        assert "x-vendor-internal" not in resp.headers
        assert resp.headers["content-type"] == "application/json"

    @pytest.mark.asyncio
    async def test_mcp_session_id_is_returned_to_the_sandbox(self, env, client):
        env.set_vendor(
            _vendor_json(
                headers={
                    "mcp-session-id": "session-egress-unit-0002",
                    "mcp-protocol-version": "2025-06-18",
                }
            )
        )

        resp = await _post(client, token=_jwt())

        assert resp.headers["mcp-session-id"] == "session-egress-unit-0002"
        assert resp.headers["mcp-protocol-version"] == "2025-06-18"

    @pytest.mark.asyncio
    async def test_vendor_retry_after_reaches_the_sandbox(self, env, client):
        # The spec mandates forwarding the vendor's backoff hint — without it
        # the generated client can only guess at a 429's retry window.
        env.set_vendor(_vendor_json(status=429, headers={"retry-after": "30"}))

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 429
        assert resp.headers["retry-after"] == "30"

    @pytest.mark.asyncio
    async def test_vendor_status_codes_pass_straight_through(self, env, client):
        env.set_vendor(_vendor_json(status=418))

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 418
        assert _error(resp) is None


# ===========================================================================
# 5. Vendor 401 disambiguation
# ===========================================================================


class TestVendor401Disambiguation:
    @pytest.mark.asyncio
    async def test_rotated_bundle_triggers_exactly_one_retry_with_the_fresh_token(
        self, env, client
    ):
        env.set_vendor(_vendor_json(status=401), _vendor_json(status=200))
        env.connection = AccessToken(
            access_token=ROTATED_TOKEN, token_type="Bearer", generation=2
        )

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 200
        assert env.vendor.sends == 2
        assert env.vendor.requests[0].headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
        assert env.vendor.requests[1].headers["authorization"] == f"Bearer {ROTATED_TOKEN}"
        # A live rotation is not a reauth event.
        assert env.reauth_reports == []

    @pytest.mark.asyncio
    async def test_retry_that_also_401s_stops_and_reports_needs_reauth(self, env, client):
        env.set_vendor(_vendor_json(status=401))
        env.connection = AccessToken(
            access_token=ROTATED_TOKEN, token_type="Bearer", generation=2
        )

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 401
        assert _error(resp) == "needs_reauth"
        assert env.vendor.sends == 2  # one retry, then stop — never a loop
        # Generation 2 is the one the vendor turned down — reporting the stale
        # generation 1 would let the CAS silently swallow the flip.
        assert env.reauth_reports == [(CONNECTION_ID, 2)]

    @pytest.mark.asyncio
    async def test_401_with_no_newer_bundle_is_needs_reauth_without_a_retry(
        self, env, client
    ):
        env.set_vendor(_vendor_json(status=401))
        env.connection = AccessToken(
            access_token=ACCESS_TOKEN, token_type="Bearer", generation=1
        )  # unchanged since our read

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 401
        assert _error(resp) == "needs_reauth"
        assert env.vendor.sends == 1
        assert env.reauth_reports == [(CONNECTION_ID, 1)]

    @pytest.mark.asyncio
    async def test_a_vanished_connection_is_still_reported_against_our_bundle(
        self, env, client
    ):
        """The re-read finding nothing must not skip the report.

        Whether that report lands is the lifecycle's CAS to decide; the relay's
        job is only to name the generation the vendor rejected.
        """
        env.set_vendor(_vendor_json(status=401))
        env.connection = None

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 401
        assert _error(resp) == "needs_reauth"
        assert env.vendor.sends == 1
        assert env.reauth_reports == [(CONNECTION_ID, 1)]

    @pytest.mark.asyncio
    async def test_vendor_403_is_not_a_reauth_signal(self, env, client):
        env.set_vendor(_vendor_json(status=403))

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 403
        assert _error(resp) is None
        assert env.vendor.sends == 1
        assert env.reauth_reports == []

    @pytest.mark.asyncio
    async def test_unreachable_vendor_is_a_502_not_a_reauth_prompt(self, env, client):
        def _boom(request: httpx.Request):
            raise httpx.ConnectError("connection refused", request=request)

        env.vendor.client._transport = httpx.MockTransport(_boom)

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "upstream_unreachable"


# ===========================================================================
# 5b. Vendor redirects
# ===========================================================================


class TestVendorRedirects:
    """A vendor 3xx has no safe passthrough: following it would carry the
    bearer to the host the vendor names, and relaying it strips Location (not
    allowlisted), leaving the sandbox to raise a bare redirect error against
    the relay's own URL. It gets its own code instead."""

    @pytest.mark.asyncio
    async def test_a_redirect_is_named_rather_than_relayed_as_a_bare_3xx(
        self, env, client
    ):
        env.set_vendor(
            _vendor_json(status=307, headers={"location": f"https://{VENDOR_HOST}/mcp/"})
        )

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "vendor_redirect"

    @pytest.mark.asyncio
    async def test_the_redirect_target_stays_host_side(self, env, client):
        # Same posture as destination_blocked: the sandbox is never told the
        # vendor's address, so the code travels and the target does not.
        env.set_vendor(
            _vendor_json(
                status=302, headers={"location": "https://elsewhere.example/mcp"}
            )
        )

        resp = await _post(client, token=_jwt())

        assert _error(resp) == "vendor_redirect"
        assert "location" not in {k.lower() for k in resp.headers}
        assert "elsewhere.example" not in resp.text
        assert VENDOR_HOST not in resp.text

    @pytest.mark.asyncio
    async def test_a_redirect_on_the_401_retry_is_caught_too(self, env, client):
        # The guard sits in the send helper, not on the first response: a
        # rotated-bundle retry is a second chance for the vendor to redirect.
        env.set_vendor(
            _vendor_json(status=401),
            _vendor_json(status=308, headers={"location": f"https://{VENDOR_HOST}/v2"}),
        )
        env.connection = AccessToken(
            access_token=ROTATED_TOKEN, token_type="Bearer", generation=2
        )

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "vendor_redirect"
        assert env.vendor.sends == 2

    @pytest.mark.asyncio
    async def test_a_304_is_not_a_redirect(self, env, client):
        # 304 is the one 3xx that answers rather than names elsewhere; the
        # passthrough contract still owns it.
        env.set_vendor(_vendor_json(status=304))

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 304
        assert _error(resp) is None

    @pytest.mark.asyncio
    async def test_a_redirect_status_without_location_is_still_refused(
        self, env, client
    ):
        # A 301 with no Location is malformed, but relaying it lands the
        # sandbox on the same bare redirect error; the status alone decides.
        env.set_vendor(_vendor_json(status=301))

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "vendor_redirect"

    @pytest.mark.asyncio
    async def test_a_300_multiple_choices_is_refused_too(self, env, client):
        # 300 may name a preferred target in Location; relayed with the header
        # stripped it is just another bare redirect error in the sandbox.
        env.set_vendor(
            _vendor_json(
                status=300, headers={"location": f"https://{VENDOR_HOST}/v2/mcp"}
            )
        )

        resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "vendor_redirect"

    @pytest.mark.asyncio
    async def test_the_log_keeps_only_the_redirect_host(self, env, client, caplog):
        # The Location value is vendor-controlled and can carry signed query
        # parameters; even the host-side log must not retain them.
        env.set_vendor(
            _vendor_json(
                status=302,
                headers={"location": "https://elsewhere.example/hop?sig=SIGNEDSECRET"},
            )
        )

        with caplog.at_level("WARNING", logger="src.server.services.egress.relay"):
            resp = await _post(client, token=_jwt())

        assert _error(resp) == "vendor_redirect"
        log_text = "\n".join(r.getMessage() for r in caplog.records)
        assert "elsewhere.example" in log_text
        assert "SIGNEDSECRET" not in log_text
        assert "/hop" not in log_text


# ===========================================================================
# 6. Streaming
# ===========================================================================


class TestStreaming:
    @pytest.mark.asyncio
    async def test_event_stream_passes_through_with_no_buffer_headers(
        self, env, gzip_client
    ):
        stream = _Stream([b"event: message\ndata: {\"a\":1}\n\n", b"data: [DONE]\n\n"])
        env.set_vendor(_vendor_sse(stream, headers={"set-cookie": "vendor=1"}))

        resp = await _post(gzip_client, token=_jwt())

        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        assert resp.headers["cache-control"] == "no-cache, no-transform"
        assert resp.headers["x-accel-buffering"] == "no"
        # GZipMiddleware auto-exempts event-stream; nothing may buffer it.
        assert "content-encoding" not in resp.headers
        assert "set-cookie" not in resp.headers
        assert resp.text == 'event: message\ndata: {"a":1}\n\ndata: [DONE]\n\n'
        assert stream.closed is True

    @pytest.mark.asyncio
    async def test_json_responses_do_not_get_the_sse_headers(self, env, client):
        resp = await _post(client, token=_jwt())

        assert resp.headers["content-type"] == "application/json"
        assert "cache-control" not in resp.headers
        assert "x-accel-buffering" not in resp.headers

    @pytest.mark.asyncio
    async def test_a_large_json_answer_survives_the_apps_gzip_middleware(
        self, env, gzip_client
    ):
        # setup.py wraps every route in GZipMiddleware(minimum_size=1000). The
        # relay forwards no vendor Content-Length, so a compressed re-encode
        # must not truncate or mismatch the frame the sandbox parses.
        payload = {"jsonrpc": "2.0", "id": 7, "result": {"rows": ["x" * 64] * 64}}
        env.set_vendor(_vendor_json(body=payload))

        resp = await _post(gzip_client, token=_jwt())

        assert resp.status_code == 200
        assert resp.headers["content-encoding"] == "gzip"
        assert resp.json() == payload

    @pytest.mark.asyncio
    async def test_client_disconnect_mid_stream_closes_the_vendor_stream(self, env):
        # Driven against the route callable so the disconnect is exact: take
        # one chunk, then close the body iterator the way starlette does when
        # the sandbox goes away mid-exchange.
        stream = _Stream([b"data: first\n\n", b"data: second\n\n", b"data: third\n\n"])
        env.set_vendor(_vendor_sse(stream))

        response = await _call_route(
            {"authorization": f"Bearer {_jwt()}", "content-type": "application/json"}
        )

        iterator = response.body_iterator
        assert await iterator.__anext__() == b"data: first\n\n"
        assert stream.closed is False

        await iterator.aclose()  # the sandbox hung up

        assert stream.closed is True

    @pytest.mark.asyncio
    async def test_wall_clock_cuts_a_stalled_stream_and_closes_the_vendor(self, env):
        # One budget covers token-to-last-byte: a vendor that goes quiet
        # mid-stream gets cut instead of pinning the concurrency slot.
        stream = _SlowStream(b"data: first\n\n", stall_s=5.0)
        env.set_vendor(_vendor_sse(stream))

        with patch("src.server.app.egress_relay.WALL_CLOCK_S", 0.25):
            response = await _call_route({"authorization": f"Bearer {_jwt()}"})
            body = b"".join([chunk async for chunk in response.body_iterator])

        assert response.status_code == 200
        assert body == b"data: first\n\n"  # the post-stall chunk never lands
        assert stream.closed is True

    @pytest.mark.asyncio
    async def test_wall_clock_on_the_dial_answers_504(self, env, client):
        async def _slow_pin(url: str, **kwargs):
            await asyncio.sleep(5.0)
            return env.pin

        with (
            patch("src.server.app.egress_relay.WALL_CLOCK_S", 0.25),
            patch("src.server.services.egress.relay.pin_public_url", _slow_pin),
        ):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 504
        assert _error(resp) == "wall_clock"
        assert env.vendor.sends == 0


# ===========================================================================
# 7. Limits
# ===========================================================================


class _RefusedSlot:
    def __init__(self, kind: str):
        self.kind = kind

    async def __aenter__(self):
        raise RelayLimited(self.kind)

    async def __aexit__(self, *exc_info):
        return False


def _slot_spy(record: dict[str, int]):
    """A stand-in for acquire_slot that records enter/exit, so a test can prove
    the slot spans the whole exchange and is never leaked on a failure path."""

    @asynccontextmanager
    async def _acquire(grant_id: str):
        record["entered"] += 1
        try:
            yield
        finally:
            record["exited"] += 1

    return _acquire


class TestLimits:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["rate", "concurrency"])
    async def test_exceeded_limits_get_distinct_codes_and_a_retry_after(
        self, env, client, kind
    ):
        with patch(
            "src.server.app.egress_relay.acquire_slot",
            lambda grant_id: _RefusedSlot(kind),
        ):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 429
        assert _error(resp) == f"limited_{kind}"
        assert resp.headers["retry-after"] == "5"
        assert kind in resp.text
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_the_slot_is_keyed_by_grant(self, env, client):
        seen: list[str] = []

        from src.server.services.egress.limits import acquire_slot as real

        def _spy(grant_id):
            seen.append(grant_id)
            return real(grant_id)

        with patch("src.server.app.egress_relay.acquire_slot", _spy):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 200
        assert seen == [GRANT_ID]

    @pytest.mark.asyncio
    async def test_limits_fail_open_when_redis_is_unavailable(self, env, client):
        # The env fixture already stubs an unreachable cache; the request must
        # still succeed — limits are plumbing, not the security boundary.
        resp = await _post(client, token=_jwt())

        assert resp.status_code == 200
        assert env.vendor.sends == 1

    @pytest.mark.asyncio
    async def test_the_slot_is_released_when_the_vendor_leg_rejects(self, env, client):
        # A leaked slot on the error path would wedge the grant at its
        # concurrency ceiling until the 120s Redis TTL expired.
        record = {"entered": 0, "exited": 0}
        env.set_vendor(_vendor_json(status=401))
        # Unchanged generation → no retry, straight to reject.
        env.connection = AccessToken(
            access_token=ACCESS_TOKEN, token_type="Bearer", generation=1
        )

        with patch("src.server.app.egress_relay.acquire_slot", _slot_spy(record)):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 401
        assert _error(resp) == "needs_reauth"
        assert record == {"entered": 1, "exited": 1}

    @pytest.mark.asyncio
    async def test_the_slot_is_released_when_the_wall_clock_fires(self, env, client):
        record = {"entered": 0, "exited": 0}

        async def _slow_pin(url: str, **kwargs):
            await asyncio.sleep(5.0)
            return env.pin

        with (
            patch("src.server.app.egress_relay.WALL_CLOCK_S", 0.25),
            patch("src.server.app.egress_relay.acquire_slot", _slot_spy(record)),
            patch("src.server.services.egress.relay.pin_public_url", _slow_pin),
        ):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 504
        assert record == {"entered": 1, "exited": 1}

    @pytest.mark.asyncio
    async def test_the_slot_is_held_for_the_whole_stream_not_just_the_dial(self, env):
        # Releasing at the dial would let N slow streams share one slot and
        # blow past the concurrency ceiling the limiter exists to enforce.
        record = {"entered": 0, "exited": 0}
        stream = _Stream([b"data: one\n\n", b"data: two\n\n"])
        env.set_vendor(_vendor_sse(stream))

        with patch("src.server.app.egress_relay.acquire_slot", _slot_spy(record)):
            response = await _call_route({"authorization": f"Bearer {_jwt()}"})

            assert await response.body_iterator.__anext__() == b"data: one\n\n"
            assert record == {"entered": 1, "exited": 0}  # still in flight

            async for _ in response.body_iterator:
                pass

        assert record == {"entered": 1, "exited": 1}
        assert stream.closed is True

    @pytest.mark.asyncio
    async def test_the_slot_is_released_when_the_sandbox_disconnects_mid_stream(
        self, env
    ):
        record = {"entered": 0, "exited": 0}
        stream = _Stream([b"data: one\n\n", b"data: two\n\n", b"data: three\n\n"])
        env.set_vendor(_vendor_sse(stream))

        with patch("src.server.app.egress_relay.acquire_slot", _slot_spy(record)):
            response = await _call_route({"authorization": f"Bearer {_jwt()}"})
            assert await response.body_iterator.__anext__() == b"data: one\n\n"
            await response.body_iterator.aclose()  # the sandbox hung up

        assert record == {"entered": 1, "exited": 1}


# ===========================================================================
# 8. SSRF posture (real egress guard)
# ===========================================================================


class TestSsrfPosture:
    @pytest.mark.asyncio
    async def test_destination_comes_from_the_stored_grant_not_the_request(
        self, env, client
    ):
        # Nothing the sandbox sends can steer the dial: no path, no query, no
        # header participates in building the vendor URL.
        await _post(
            client,
            token=_jwt(),
            headers={"x-destination": "https://attacker.example/", "host": "attacker.example"},
        )

        assert str(env.vendor.last.url) == DESTINATION
        assert env.vendor.last.method == "POST"

    @pytest.mark.asyncio
    async def test_plain_http_destinations_are_refused(self, env, client):
        env.grant = _grant(destination_url=f"http://{VENDOR_HOST}/mcp")

        with patch("src.server.services.egress.relay.pin_public_url", real_pin_public_url):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "destination_blocked"
        # The pin-failure reason names the vendor host and is a DNS-resolution
        # oracle — it stays host-side. The sandbox body carries only the code.
        assert resp.text == "destination_blocked"
        assert VENDOR_HOST not in resp.text
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "destination",
        [
            "https://127.0.0.1:9443/mcp",       # loopback
            "https://10.0.0.5/mcp",             # RFC1918
            "https://169.254.169.254/latest/",  # cloud metadata
            "https://[::1]/mcp",                # IPv6 loopback
        ],
    )
    async def test_non_global_destinations_are_refused_with_no_local_exemption(
        self, env, client, destination
    ):
        # open_upstream calls pin_public_url with require_https=True and NO
        # allow_non_global escape hatch — there is no OSS/local allowlist here.
        env.grant = _grant(destination_url=destination)

        with patch("src.server.services.egress.relay.pin_public_url", real_pin_public_url):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "destination_blocked"
        # The reason names the private/loopback host it refused to dial — a probe
        # oracle. Redacted from the sandbox body; only the code returns.
        assert resp.text == "destination_blocked"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_userinfo_in_the_destination_is_refused(self, env, client):
        env.grant = _grant(destination_url=f"https://user:pass@{VENDOR_HOST}/mcp")

        with patch("src.server.services.egress.relay.pin_public_url", real_pin_public_url):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 502
        assert _error(resp) == "destination_blocked"
        assert env.vendor.sends == 0

    @pytest.mark.asyncio
    async def test_a_public_destination_is_dialled_pinned_with_sni_and_host_restored(
        self, env, client
    ):
        # A HOSTNAME destination on a non-default port. The real guard resolves
        # it (stubbed to a fixed public IP so the test needs no DNS), then pins:
        # the dial goes to the IP, but SNI and the Host authority must both carry
        # the real name — and the Host must keep the non-default port, which a
        # bare-host header would silently drop. An IP-literal destination could
        # never prove any of this (host == ip), so a real hostname is required.
        env.grant = _grant(destination_url=f"https://{VENDOR_HOST}:8443/mcp")

        async def _resolve(host, *, port=443, allow_non_global=False):
            assert host == VENDOR_HOST
            return [PUBLIC_IP]

        with (
            patch("src.server.utils.egress_guard.resolve_public_ips", _resolve),
            patch(
                "src.server.services.egress.relay.pin_public_url",
                real_pin_public_url,
            ),
        ):
            resp = await _post(client, token=_jwt())

        assert resp.status_code == 200
        # URL rewritten to the validated IP — the hostname is gone from the netloc.
        assert str(env.vendor.last.url) == f"https://{PUBLIC_IP}:8443/mcp"
        # SNI restores the real name so certificate verification runs against it.
        assert env.vendor.last.extensions["sni_hostname"] == VENDOR_HOST
        # Host authority keeps the hostname AND the non-default port.
        assert env.vendor.last.headers["host"] == f"{VENDOR_HOST}:8443"

    @pytest.mark.asyncio
    async def test_the_shared_client_never_follows_redirects_or_trusts_env_proxies(self):
        from src.server.services.egress.relay import close_relay_client, get_relay_client

        client = get_relay_client()
        try:
            assert client.follow_redirects is False
            assert client.trust_env is False
        finally:
            await close_relay_client()

    @pytest.mark.asyncio
    async def test_idle_upstream_connections_outlive_the_gap_between_tool_bursts(self):
        # httpx defaults keepalive_expiry to 5s — shorter than the model latency
        # between two execute_code blocks, so without an explicit pool every
        # burst of MCP calls re-pays a TCP+TLS handshake to the vendor. Asserted
        # against httpx's own default rather than a pinned number, so this locks
        # the intent without becoming tuning noise.
        from src.server.services.egress.relay import close_relay_client, get_relay_client

        client = get_relay_client()
        try:
            pool = client._transport._pool
            assert pool._keepalive_expiry >= 10 * httpx.Limits().keepalive_expiry
        finally:
            await close_relay_client()
