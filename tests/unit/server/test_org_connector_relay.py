"""Organization connectors at the relay: an ``org_mcp`` grant per connector,
the account read again on every call, a shared credential asked of the auth
service for that call alone, a member's own sign-in otherwise, and every
failure a refusal rather than another credential.

The custody test plants a recognisable credential in the auth service's
answer and follows it: it reaches the vendor and nothing else."""

from __future__ import annotations

import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.server.database.egress_grants import GRANT_KIND_ORG_MCP, OrgGrantRef
from src.server.database.org_keys import org_key
from src.server.services.account_contract import OrgConnector
from src.server.services.egress import RelayError, RelayRejection

OC = "src.server.services.org_connectors"
CRED = "src.server.services.egress.credentials"

PLANTED = "planted-org-credential-7f3a9c"
AUTH_URL = "https://auth.example.test"
USER_ID = "usr-org-0001"

CRM = OrgConnector(
    id="c-crm", name="crm", url="https://crm.example.test/mcp", auth="shared",
    revision="r7",
)
WIKI = OrgConnector(
    id="c-wiki", name="wiki", url="https://wiki.example.test/mcp", auth="member",
    revision="r1",
)


def _grant(connector=CRM, **overrides) -> dict:
    row = {
        "user_id": USER_ID,
        "workspace_id": "11111111-2222-3333-4444-555555555555",
        "computer_id": None,
        "kind": GRANT_KIND_ORG_MCP,
        "connection_id": None,
        "server_name": connector.name,
        "resolver_config": {"connector_id": connector.id},
        "destination_url": connector.url,
        "allowed_methods": ["POST"],
        "tool_denylist": None,
        "tool_direct_only": None,
        "grant_status": "active",
        "connection_status": None,
    }
    row.update(overrides)
    return row


def _canonical(tool="lookup"):
    return SimpleNamespace(method="tools/call", tool_name=tool)


def _listed(value):
    return patch(f"{OC}.org_connectors_for", new=AsyncMock(return_value=value))


class _AuthService:
    """The auth service's credential endpoint, recording what it was sent."""

    def __init__(
        self, answer=None, *, status=200, raise_=None, delay=0.0, token="cred-token"
    ):
        self.token = token
        self.answer = {"headers": {"Authorization": f"Bearer {PLANTED}"}} if answer is None else answer
        self.status = status
        self.raise_ = raise_
        self.delay = delay
        self.requests: list[httpx.Request] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.raise_ is not None:
            raise self.raise_
        if isinstance(self.answer, bytes):
            return httpx.Response(self.status, content=self.answer)
        return httpx.Response(self.status, json=self.answer)

    def patches(self):
        client = httpx.AsyncClient(transport=httpx.MockTransport(self))
        return (
            patch("src.server.utils.http_client.get_http_client", AsyncMock(return_value=client)),
            patch("src.config.settings.AUTH_SERVICE_URL", AUTH_URL),
            patch(
                "src.server.services.platform_client.platform_gating_active",
                return_value=True,
            ),
            patch.dict(
                "os.environ",
                {"INTERNAL_SERVICE_TOKEN": "svc-token",
                 "ORG_CONNECTOR_CREDENTIAL_TOKEN": self.token},
            ),
        )


def _pinned(url: str = CRM.url):
    from src.server.utils.egress_guard import PinnedTarget

    host = url.split("/")[2]
    ip = "93.184.216.34"
    return PinnedTarget(url=url.replace(host, ip), host=host, ip=ip, authority=host)


async def _credential(grant, auth: _AuthService | None = None, listed=(CRM, WIKI)):
    from contextlib import ExitStack

    from src.server.services.egress.credentials import resolve_vendor_credential

    with ExitStack() as stack:
        stack.enter_context(_listed(listed))
        for p in (auth or _AuthService()).patches():
            stack.enter_context(p)
        return await resolve_vendor_credential(grant, _canonical())


def _refused(excinfo, status, code):
    assert (excinfo.value.status, excinfo.value.code) == (status, code)


# --- the shared credential --------------------------------------------------


@pytest.mark.asyncio
async def test_a_shared_call_asks_the_auth_service_for_this_call():
    auth = _AuthService()
    credential = await _credential(_grant(), auth)
    assert credential.headers == {"authorization": f"Bearer {PLANTED}"}
    # No token: the vendor's 401 is its answer, never a reason to retry.
    assert credential.token is None
    [request] = auth.requests
    assert str(request.url) == f"{AUTH_URL}/api/auth/connectors/credential"
    assert request.headers["x-user-id"] == USER_ID
    # Its own token, never the one services act for a user with.
    assert request.headers["x-connector-credential-token"] == "cred-token"
    assert "x-service-token" not in request.headers
    assert json.loads(request.content) == {
        "connector_id": "c-crm",
        "revision": "r7",
        "url": CRM.url,
        "method": "tools/call",
        "tool": "lookup",
    }
    # Asked again for the next call: nothing is kept.
    await _credential(_grant(), auth)
    assert len(auth.requests) == 2


@pytest.mark.asyncio
async def test_reserved_names_are_dropped_from_the_answer():
    auth = _AuthService({"headers": {"X-Api-Key": PLANTED, "Host": "evil.example.test",
                                     "Content-Length": "1"}})
    credential = await _credential(_grant(), auth)
    assert credential.headers == {"x-api-key": PLANTED}


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 404])
async def test_a_refusal_by_the_auth_service_refuses_the_call(status):
    with pytest.raises(RelayRejection) as refused:
        await _credential(_grant(), _AuthService({"detail": "no"}, status=status))
    _refused(refused, 403, RelayError.ORG_CONNECTOR_UNAVAILABLE)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth",
    [
        _AuthService({"detail": "boom"}, status=500),
        _AuthService(status=200, raise_=httpx.ConnectTimeout("slow")),
        _AuthService(status=200, raise_=httpx.ConnectError("down")),
        _AuthService(b"<html>not json</html>"),
        _AuthService({"nothing": True}),
        _AuthService({"headers": ["Authorization", PLANTED]}),
        _AuthService({"headers": {"Authorization": f"Bearer {PLANTED}\r\nX-Evil: 1"}}),
        _AuthService({"headers": {"Bad Name": PLANTED}}),
        _AuthService({"headers": {"Authorization": 7}}),
        # httpx cannot frame these, and its encode error would quote the value.
        _AuthService({"headers": {"Authorization": f"Bearer {PLANTED}\u00e9"}}),
        _AuthService({"headers": {"Authorization": f"Bearer {PLANTED}\t"}}),
        _AuthService({"headers": {"Authorization": f"Bearer {PLANTED}\x7f"}}),
        # Anything the request raises, its message never logged.
        _AuthService(status=200, raise_=RuntimeError(PLANTED)),
    ],
    ids=[
        "5xx", "timeout", "down", "html", "no-headers", "list", "crlf", "bad-name",
        "not-str", "non-ascii", "tab", "del", "unexpected",
    ],
)
async def test_anything_but_a_readable_answer_refuses_as_unreachable(auth, caplog):
    caplog.set_level(logging.DEBUG)
    with pytest.raises(RelayRejection) as refused:
        await _credential(_grant(), auth)
    _refused(refused, 503, RelayError.ORG_UNREACHABLE)
    assert refused.value.retry_after == 30
    assert PLANTED not in refused.value.detail
    assert PLANTED not in caplog.text


@pytest.mark.asyncio
async def test_a_slow_answer_is_cut_at_the_budget():
    with patch(f"{OC}.CREDENTIAL_TIMEOUT_S", 0.05), pytest.raises(RelayRejection) as refused:
        await _credential(_grant(), _AuthService(delay=1.0))
    _refused(refused, 503, RelayError.ORG_UNREACHABLE)


@pytest.mark.asyncio
async def test_without_its_token_no_credential_is_asked_for(caplog):
    """No fallback to the service token: the call is refused as unreachable,
    and the cause is logged once, not on every call."""
    auth = _AuthService(token="")
    with patch(f"{OC}._missing_token_logged", False):
        for _ in range(2):
            with pytest.raises(RelayRejection) as refused:
                await _credential(_grant(), auth)
            _refused(refused, 503, RelayError.ORG_UNREACHABLE)
    assert auth.requests == []
    assert caplog.text.count("ORG_CONNECTOR_CREDENTIAL_TOKEN is not set") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "token", [f"{PLANTED} x", f"{PLANTED}\u00e9", f"{PLANTED}\x01", f"{PLANTED}\x7f"],
    ids=["space", "non-ascii", "control", "del"],
)
async def test_a_token_no_header_can_carry_reads_as_unset(token, caplog):
    auth = _AuthService(token=token)
    with patch(f"{OC}._missing_token_logged", False):
        for _ in range(2):
            with pytest.raises(RelayRejection) as refused:
                await _credential(_grant(), auth)
            _refused(refused, 503, RelayError.ORG_UNREACHABLE)
    assert auth.requests == []
    assert caplog.text.count("ORG_CONNECTOR_CREDENTIAL_TOKEN has an unusable value") == 1
    assert PLANTED not in caplog.text


# --- the account, read again ------------------------------------------------


@pytest.mark.asyncio
async def test_an_unknown_account_refuses_and_asks_no_one():
    auth = _AuthService()
    with pytest.raises(RelayRejection) as refused:
        await _credential(_grant(), auth, listed=None)
    _refused(refused, 503, RelayError.ORG_UNREACHABLE)
    assert auth.requests == []


@pytest.mark.asyncio
async def test_a_connector_the_organization_no_longer_lists_is_refused():
    auth = _AuthService()
    with pytest.raises(RelayRejection) as refused:
        await _credential(_grant(), auth, listed=(WIKI,))
    _refused(refused, 403, RelayError.ORG_CONNECTOR_UNAVAILABLE)
    assert auth.requests == []


@pytest.mark.asyncio
async def test_a_grant_for_another_address_is_refused():
    auth = _AuthService()
    with pytest.raises(RelayRejection) as refused:
        await _credential(
            _grant(destination_url="https://old.example.test/mcp"), auth
        )
    _refused(refused, 403, RelayError.ORG_CONNECTOR_UNAVAILABLE)
    assert auth.requests == []


@pytest.mark.asyncio
async def test_a_grant_without_its_connector_id_is_refused():
    with pytest.raises(RelayRejection) as refused:
        await _credential(_grant(resolver_config={}))
    _refused(refused, 403, RelayError.ORG_CONNECTOR_UNAVAILABLE)


# --- a connector members sign in to -----------------------------------------


def _connection(**overrides):
    from src.server.database.mcp_oauth import ConnectionStatus

    row = dict(
        connection_id="conn-9", user_id=USER_ID, server_name=org_key(WIKI.id),
        server_url=WIKI.url, status=ConnectionStatus.CONNECTED,
    )
    row.update(overrides)
    return SimpleNamespace(**row)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection",
    [
        None,
        _connection(user_id="someone-else"),
        _connection(server_name="wiki"),
        _connection(server_url="https://elsewhere.example.test/mcp"),
        _connection(status="needs_reauth"),
        _connection(status="revoked"),
    ],
    ids=["none", "other-user", "own-server-name", "other-url", "needs-reauth", "revoked"],
)
async def test_a_member_connector_spends_only_the_members_own_connection(connection):
    from src.server.services.mcp_oauth.lifecycle import AccessToken

    mint = AsyncMock(return_value=AccessToken("tok", "Bearer", 1))
    grant = _grant(WIKI, connection_id="conn-9", connection_status="connected")
    auth = _AuthService()
    with (
        patch(f"{CRED}.get_connection_by_id", AsyncMock(return_value=connection)),
        patch(f"{CRED}.ensure_fresh_access_token", mint),
        pytest.raises(RelayRejection) as refused,
    ):
        await _credential(grant, auth)
    _refused(refused, 401, RelayError.ORG_SIGN_IN_REQUIRED)
    mint.assert_not_awaited()
    assert auth.requests == []


@pytest.mark.asyncio
async def test_a_member_connector_sends_the_members_bearer():
    from src.server.services.mcp_oauth.lifecycle import AccessToken

    token = AccessToken("member-tok", "Bearer", 3)
    grant = _grant(WIKI, connection_id="conn-9", connection_status="connected")
    with (
        patch(f"{CRED}.get_connection_by_id", AsyncMock(return_value=_connection())),
        patch(f"{CRED}.ensure_fresh_access_token", AsyncMock(return_value=token)),
    ):
        credential = await _credential(grant)
    assert credential.headers == {"authorization": "Bearer member-tok"}
    assert credential.token is token


@pytest.mark.asyncio
async def test_a_shared_grant_after_a_switch_to_member_needs_a_sign_in():
    with pytest.raises(RelayRejection) as refused:
        await _credential(_grant(WIKI))
    _refused(refused, 401, RelayError.ORG_SIGN_IN_REQUIRED)


# --- prepare_relay ----------------------------------------------------------


def _body(tool="lookup"):
    return json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
         "params": {"name": tool, "arguments": {}}}
    ).encode()


@pytest.mark.asyncio
async def test_a_shared_grant_needs_no_connection_and_no_own_connector_rule():
    from src.config.models import PolicyLayer
    from src.server.services.egress import relay
    from src.server.services.policy import merge

    no_own = merge(None, PolicyLayer.model_validate({"user_connectors": False}))
    claims = SimpleNamespace(user_id=USER_ID, caller=None, workspace_id="ws", identity=None)
    with (
        patch.object(relay, "fetch_grant_for_relay", AsyncMock(return_value=_grant())),
        patch.object(relay, "_grant_is_reachable", return_value=True),
        patch("src.server.services.account.policy_for", AsyncMock(return_value=no_own)),
        patch.object(relay, "pin_public_url", AsyncMock(return_value=_pinned())),
        _listed((CRM,)),
    ):
        auth = _AuthService()
        from contextlib import ExitStack

        with ExitStack() as stack:
            for p in auth.patches():
                stack.enter_context(p)
            prepared = await relay.prepare_relay("g", claims=claims, raw_body=_body())
    assert prepared.credential.headers == {"authorization": f"Bearer {PLANTED}"}


async def _prepare(grant, connectors, connection=None):
    from contextlib import ExitStack

    from src.server.services.egress import relay

    claims = SimpleNamespace(user_id=USER_ID, caller=None, workspace_id="ws")
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(relay, "fetch_grant_for_relay", AsyncMock(return_value=grant))
        )
        stack.enter_context(patch.object(relay, "_grant_is_reachable", return_value=True))
        stack.enter_context(
            patch.object(
                relay, "pin_public_url", AsyncMock(return_value=_pinned(grant["destination_url"]))
            )
        )
        stack.enter_context(_listed(connectors))
        stack.enter_context(
            patch(f"{CRED}.get_connection_by_id", AsyncMock(return_value=connection))
        )
        for p in _AuthService().patches():
            stack.enter_context(p)
        return await relay.prepare_relay("g", claims=claims, raw_body=_body())


@pytest.mark.asyncio
async def test_a_member_grant_whose_connection_needs_repair_asks_for_a_sign_in():
    grant = _grant(WIKI, connection_id="conn-9", connection_status="needs_reauth")
    with pytest.raises(RelayRejection) as refused:
        await _prepare(grant, (WIKI,), _connection(status="needs_reauth"))
    _refused(refused, 401, RelayError.ORG_SIGN_IN_REQUIRED)


@pytest.mark.asyncio
async def test_a_member_grant_is_served_after_a_switch_to_shared():
    """The connection it names from its member days is no longer the
    credential, so its state cannot refuse the call."""
    from dataclasses import replace

    shared = replace(WIKI, auth="shared")
    grant = _grant(WIKI, connection_id="conn-9", connection_status="needs_reauth")
    prepared = await _prepare(grant, (shared,))
    assert prepared.credential.headers == {"authorization": f"Bearer {PLANTED}"}


@pytest.mark.asyncio
async def test_a_vendor_refusing_a_members_sign_in_asks_for_that_sign_in():
    """The sandbox reads only the code's hint, and a member repairs a lapsed
    sign-in to the organization's connector, not a server of their own."""
    from src.server.services.egress import relay
    from src.server.services.egress.credentials import VendorCredential
    from src.server.services.egress.relay import PreparedRelay
    from src.server.services.mcp_oauth.lifecycle import AccessToken

    vendor = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(401))
    )
    prepared = PreparedRelay(
        claims=SimpleNamespace(workspace_id="ws"),
        grant=_grant(WIKI, connection_id="conn-9", connection_status="connected"),
        canonical=SimpleNamespace(method="tools/call", tool_name="lookup", body=b"{}"),
        credential=VendorCredential(
            headers={"authorization": "Bearer member-tok"},
            token=AccessToken("member-tok", "Bearer", 1),
        ),
        target=_pinned(WIKI.url),
        order=None,
    )
    mark = AsyncMock(return_value=True)
    with (
        patch.object(relay, "get_relay_client", return_value=vendor),
        patch.object(relay, "current_access_token", AsyncMock(return_value=None)),
        patch.object(relay, "mark_connection_needs_reauth", mark),
        pytest.raises(RelayRejection) as refused,
    ):
        await relay.open_upstream(prepared, {})
    await vendor.aclose()
    _refused(refused, 401, RelayError.ORG_SIGN_IN_REQUIRED)
    mark.assert_awaited_once_with("conn-9", seen_token_generation=1)


def test_each_call_is_attributed_without_its_credential(caplog):
    from src.server.services.egress.credentials import VendorCredential
    from src.server.services.egress.relay import PreparedRelay, log_org_frame

    caplog.set_level(logging.INFO)
    prepared = PreparedRelay(
        claims=SimpleNamespace(workspace_id="ws-1"),
        grant=_grant(),
        canonical=_canonical("lookup"),
        credential=VendorCredential(headers={"authorization": PLANTED}),
        target=_pinned(),
    )
    log_org_frame(prepared, 200)
    assert (
        "org_connector id=c-crm user=usr-org-0001 workspace=ws-1 tool=lookup status=200"
        in caplog.text
    )
    assert PLANTED not in caplog.text


# --- grants -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_every_active_connector_earns_a_grant():
    from src.server.services.egress.grant_scope import grant_refs
    from src.server.services.mcp_config import (
        Origin,
        ResolvedMCP,
        ResolvedServer,
        State,
        org_server_config,
    )

    member = org_server_config(WIKI).model_copy(update={"oauth_connection_id": "conn-9"})
    resolved = ResolvedMCP(
        entries=(
            ResolvedServer(org_server_config(CRM), Origin.ORG, State.ACTIVE),
            ResolvedServer(member, Origin.ORG, State.ACTIVE),
            ResolvedServer(
                org_server_config(
                    OrgConnector(id="c-off", name="off", url="https://off.example.test",
                                 auth="shared", revision="1")
                ),
                Origin.ORG,
                State.DISABLED,
            ),
        ),
        version=1,
    )
    assert await grant_refs(resolved, user_id=USER_ID) == [
        OrgGrantRef(server_name="crm", connection_id=None, connector_id="c-crm", destination_url=CRM.url),
        OrgGrantRef(server_name="wiki", connection_id="conn-9", connector_id="c-wiki", destination_url=WIKI.url),
    ]


class _Cursor:
    def __init__(self, rows=()):
        self.calls: list[tuple[str, object]] = []
        self._rows = list(rows)
        self.rowcount = 0

    async def execute(self, sql, params=None):
        self.calls.append((sql, params))

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


@pytest.mark.asyncio
async def test_the_grant_stores_a_reference_and_attaches_only_the_members_connection():
    from src.server.database.egress_grants import _upsert_org_grants

    cur = _Cursor(rows=[{"server_name": "wiki", "grant_id": "g-1"}])
    refs = [OrgGrantRef(server_name="wiki", connection_id="conn-9", connector_id="c-wiki", destination_url=WIKI.url)]
    assert await _upsert_org_grants(
        cur, user_id=USER_ID, workspace_id="ws", computer_id=None, refs=refs
    ) == {"wiki": "g-1"}
    sql, params = cur.calls[-1]
    assert "c.server_name = t.org_key" in sql and "c.user_id = %(user_id)s" in sql
    assert params["keys"] == [org_key("c-wiki")]
    assert params["urls"] == [WIKI.url]


# --- custody: the planted credential reaches the vendor and nothing else -----


@pytest.mark.asyncio
async def test_the_shared_credential_is_written_nowhere(tmp_path, caplog):
    """A relay call stores nothing from the credential: the grant is written
    before one is asked for, and the call's only writes are its rate counters,
    keyed by grant id, and its log line. What a probe stores is
    held to the same rule by ``test_nothing_the_probe_sent_is_kept`` in
    ``test_org_connector_discovery.py``."""
    from src.server.services.mcp_config import org_server_config
    from tests.unit.server.app import test_egress_relay_roundtrip as rt

    caplog.set_level(logging.DEBUG)
    connector = OrgConnector(
        id="c-crm", name=rt.SERVER_NAME, url=rt.VENDOR_URL, auth="shared", revision="r7"
    )
    server = org_server_config(connector)

    class _OrgHarness(rt._Harness):
        def _exec_generated_client(self):
            from pathlib import Path

            from ptc_agent.core.tool_generator import ToolFunctionGenerator

            config_path = Path(self._workdir) / ".agents/tools/mcp_client_config.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps({"workspace_id": "test", "servers": {rt.SERVER_NAME: {}}})
            )
            gen = ToolFunctionGenerator()
            self.client_config = gen.generate_client_config([server], working_dir=self._workdir)
            self.client_code = gen.generate_mcp_client_code([server], working_dir=self._workdir)
            exec(compile(self.client_code, "gen_mcp_client", "exec"), self.ns)  # noqa: S102

    harness = _OrgHarness(str(tmp_path))
    harness.grant = _grant(
        connector, user_id=rt.USER_ID, workspace_id=rt.WORKSPACE_ID,
        destination_url=rt.VENDOR_URL,
    )
    auth = _AuthService()
    from contextlib import ExitStack

    async with harness.activate() as h:
        with ExitStack() as stack:
            stack.enter_context(_listed((connector,)))
            for p in auth.patches():
                stack.enter_context(p)
            result = await h.call("echo", {"text": "hi"})
            # And a failure, whose error the agent reads.
            auth.answer = {"headers": {"Authorization": f"Bearer {PLANTED}\n"}}
            with pytest.raises(RuntimeError) as failed:
                await h.call("echo", {"text": "again"})

    # The vendor got it.
    assert f"Bearer {PLANTED}" in harness.seen["authorization"]
    # The sandbox never did: not the generated config or code, the credential
    # file, the call's result, or the error the agent reads.
    assert PLANTED not in json.dumps(harness.client_config)
    assert PLANTED not in harness.client_code
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert PLANTED not in path.read_text(errors="ignore"), path
    assert PLANTED not in json.dumps(result, default=str)
    assert PLANTED not in str(failed.value)
    assert "org_unreachable" in str(failed.value)
    # Nor any log line.
    assert PLANTED not in caplog.text


# --- the destination before the credential --------------------------------------


async def _prepared_with(grant, connectors, auth, pin):
    from contextlib import ExitStack

    from src.server.services.egress import relay

    claims = SimpleNamespace(user_id=USER_ID, caller=None, workspace_id="ws")
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(relay, "fetch_grant_for_relay", AsyncMock(return_value=grant))
        )
        stack.enter_context(patch.object(relay, "_grant_is_reachable", return_value=True))
        stack.enter_context(patch.object(relay, "pin_public_url", pin))
        stack.enter_context(_listed(connectors))
        for p in auth.patches():
            stack.enter_context(p)
        return await relay.prepare_relay("g", claims=claims, raw_body=_body())


@pytest.mark.asyncio
async def test_a_destination_the_relay_refuses_costs_no_credential_request():
    from src.server.utils.egress_guard import pin_public_url

    private = OrgConnector(
        id="c-in", name="inhouse", url="https://10.255.255.1/mcp", auth="shared",
        revision="r1",
    )
    auth = _AuthService()
    with pytest.raises(RelayRejection) as refused:
        await _prepared_with(_grant(private), (private,), auth, pin_public_url)
    _refused(refused, 502, RelayError.DESTINATION_BLOCKED)
    assert auth.requests == []


@pytest.mark.asyncio
async def test_the_address_pinned_before_the_credential_is_the_one_dialled():
    from src.server.services.egress import relay

    dialled: list[httpx.Request] = []

    def _vendor(request: httpx.Request) -> httpx.Response:
        dialled.append(request)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {}})

    pin = AsyncMock(return_value=_pinned())
    prepared = await _prepared_with(_grant(), (CRM,), _AuthService(), pin)
    vendor = httpx.AsyncClient(transport=httpx.MockTransport(_vendor))
    with patch.object(relay, "get_relay_client", return_value=vendor):
        response = await relay.open_upstream(prepared, {})
    await response.aclose()
    await vendor.aclose()
    # Resolved once, and dialled at that address with the name kept for TLS.
    pin.assert_awaited_once_with(CRM.url, require_https=True)
    [request] = dialled
    assert request.url.host == "93.184.216.34"
    assert request.extensions["sni_hostname"] == "crm.example.test"
    assert request.headers["authorization"] == f"Bearer {PLANTED}"
