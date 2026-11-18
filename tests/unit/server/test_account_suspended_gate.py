"""The request gate: a suspended account reaches no route that serves what it
holds, whichever credential it presents, and only the few that let a client
learn its state and sign out stay open. Coverage is proven over the app's
router table, so a route added without a gated resolver fails here."""

from __future__ import annotations

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import Depends, FastAPI, HTTPException, WebSocketException
from fastapi.routing import APIRoute, APIWebSocketRoute

from src.server.auth.jwt_bearer import AuthInfo, get_current_auth_info
from src.server.dependencies.principal import ReadPrincipal, get_principal
from src.server.services.account import ACCOUNT_SUSPENDED, Standing
from src.server.services.account_contract import NO_ORG, Account, Org
from src.server.utils import api
from src.server.utils.api import (
    AnyStateUserId,
    CurrentUserId,
    OptionalUserId,
    PageViewer,
    ServiceUserId,
    StampThreadAuth,
)

ACCOUNT = "src.server.services.account"
SUSPENDED = Account(org=Org("o-1", "Acme", suspended=True), features={"teams": True})
MESSAGE = (
    "This account's access to Acme has ended. Ask an admin of your "
    "organization if this is unexpected."
)


# --- coverage over the router table ---------------------------------------------


def _gated() -> set:
    from src.server.app import livefs

    return {
        api.get_current_user_id,
        api.get_service_user_id,
        api.get_optional_user_id,
        api.get_viewer,
        api.get_stamp_auth,
        get_principal,
        livefs._caller,
    }


#: Routes a suspended account still reaches, each resolving its user through
#: a resolver that does not refuse it.
OPEN_TO_A_SUSPENDED_ACCOUNT = {
    # How a client learns the account is suspended.
    ("GET", "/api/v1/users/me"),
    # The auth service's notice that the account's state changed.
    ("POST", "/api/v1/users/me/membership-changed"),
    # Signing in, and unregistering a device when signing out of it.
    ("POST", "/api/v1/auth/sync"),
    ("DELETE", "/api/v1/users/me/push-devices/{push_device_id}"),
    # A service on no user's behalf.
    ("GET", "/api/v1/activity"),
    ("GET", "/api/v1/org-library/service"),
    ("DELETE", "/api/v1/org-library/service/{item_id}"),
    ("GET", "/api/v1/shared-workspaces/service"),
    ("GET", "/api/v1/shared-workspaces/service/{workspace_id}"),
    ("GET", "/api/v1/shared-workspaces/service/teams/{team_id}"),
    ("PUT", "/api/v1/shared-workspaces/service/{workspace_id}/managers/{user_id}"),
    ("DELETE", "/api/v1/shared-workspaces/service/{workspace_id}"),
}

#: Routes no user's credential reaches, with why each needs no gate here.
NO_USER_CREDENTIAL = {
    # Public: configuration, health, icons, a public filing proxy.
    ("GET", "/api/v1/auth/cli"),
    ("GET", "/api/v1/models"),
    ("GET", "/api/v1/skills/limits"),
    ("GET", "/api/v1/sec-proxy/document"),
    ("GET", "/api/v1/mcp/brokerages/{name}/icon"),
    ("GET", "/api/v1/mcp/server-icons/{handle}"),
    ("GET", "/api/v1/market-data/logos/{symbol}"),
    ("GET", "/health"),
    ("GET", "/ws/v1/market-data/status"),
    # Authenticated in its handler, by ``authenticate_websocket``, which
    # refuses a suspended account (mounted only with market data on).
    ("WS", "/ws/v1/market-data/aggregates/{market}"),
    # Share links: the owner's public_links policy governs them.
    ("GET", "/api/v1/public/shared/{share_token}/files"),
    ("POST", "/api/v1/public/shared/{share_token}/files/resolve"),
    ("GET", "/api/v1/public/shared/{share_token}/files/read"),
    ("GET", "/api/v1/public/shared/{share_token}/files/download"),
    ("GET", "/api/v1/public/shared/{share_token}/files/serve/{path:path}"),
    ("GET", "/api/v1/public/shared/{share_token}/replay"),
    # Redirects an old preview URL to the page that resolves it for its owner.
    ("GET", "/api/v1/preview/{workspace_id}/{port}"),
    ("GET", "/api/v1/preview/{workspace_id}/{port}/{path:path}"),
    # The owner's grants, ended with the account where they are served.
    ("GET", "/api/v1/wsfiles/g/{grant}/{path:path}"),
    ("GET", "/api/v1/artifacts/g/{grant}/v/{seq}/{path:path}"),
    ("HEAD", "/api/v1/artifacts/g/{grant}/v/{seq}/{path:path}"),
    # The relay refuses every frame for a suspended account itself.
    ("POST", "/v1/egress/{grant_id}"),
    # A connector sign-in's return, bound to a state its start minted.
    ("GET", "/api/v1/mcp/oauth/callback"),
    # A machine's side of pairing; only the owner's gated route adds it.
    ("GET", "/api/v1/computers/ssh/pairings/{token}/script"),
    ("GET", "/api/v1/computers/ssh/pairings/{token}/key"),
    ("POST", "/api/v1/computers/ssh/pairings/{token}/report"),
    ("POST", "/api/v1/computers/ssh/pairings/{token}/tunnel"),
}


def _calls(dependant) -> set:
    found, stack = set(), [dependant]
    while stack:
        dep = stack.pop()
        if dep.call is not None:
            found.add(dep.call)
        stack.extend(dep.dependencies)
    return found


def _routes():
    from src.server.app.setup import app

    for route in app.routes:
        if isinstance(route, APIRoute):
            for method in route.methods:
                yield (method, route.path), _calls(route.dependant)
        elif isinstance(route, APIWebSocketRoute):
            yield ("WS", route.path), _calls(route.dependant)


def test_every_route_naming_a_user_passes_the_gate():
    gated = _gated()
    open_resolvers = {
        api.get_user_id_any_state,
        api.get_service_user_id_any_state,
        get_current_auth_info,
        api.require_service_caller,
    }
    ungated, opened = set(), set()
    for key, calls in _routes():
        if calls & gated:
            continue
        (opened if calls & open_resolvers else ungated).add(key)
    assert opened == OPEN_TO_A_SUSPENDED_ACCOUNT
    assert ungated <= NO_USER_CREDENTIAL, "classify each new route above"
    # Every entry is a route, but the ones only some deployments mount.
    assert NO_USER_CREDENTIAL - ungated <= {
        ("WS", "/ws/v1/market-data/aggregates/{market}"),
    }


def test_the_market_data_socket_authenticates_through_the_gated_helper():
    import inspect

    from src.server.app import market_data_ws

    assert "authenticate_websocket(websocket)" in inspect.getsource(market_data_ws)


def test_no_route_runs_both_kinds_of_resolver():
    """No route open to a suspended account also runs a gated resolver, which
    would refuse it after all, and none that is gated also runs an open one,
    which would read the account twice."""
    gated = _gated()
    both = [
        key for key, calls in _routes()
        if calls & gated and calls & {api.get_user_id_any_state, api.get_service_user_id_any_state}
    ]
    assert both == []


# --- each resolver ---------------------------------------------------------------


def _deployment(account, *, door=True):
    stack = ExitStack()
    stack.enter_context(patch("src.server.services.membership.teams_door_open", return_value=door))
    read = stack.enter_context(
        patch(f"{ACCOUNT}.read_standing", AsyncMock(return_value=Standing(account, account)))
    )
    stack.enter_context(patch("src.server.utils.api.HOST_MODE", "platform"))
    stack.enter_context(patch("src.server.utils.api._SERVICE_TOKEN", "svc"))
    stack.enter_context(
        patch(
            "src.server.utils.api._decode_token",
            AsyncMock(return_value=AuthInfo(user_id="u")),
        )
    )
    return stack, read


def _app() -> FastAPI:
    app = FastAPI()

    @app.get("/session")
    async def session(user_id: CurrentUserId):
        return {"user": user_id}

    @app.get("/service")
    async def service(user_id: ServiceUserId):
        return {"user": user_id}

    @app.get("/optional")
    async def optional(user_id: OptionalUserId):
        return {"user": user_id}

    @app.get("/stamp")
    async def stamp(user_id: StampThreadAuth):
        return {"user": user_id}

    @app.get("/viewer")
    async def viewer(viewer: PageViewer):
        return {"user": viewer.user_id}

    @app.get("/principal")
    async def principal(principal: ReadPrincipal):
        return {"user": principal.user_id}

    @app.get("/two")
    async def two(user_id: CurrentUserId, principal=Depends(get_principal)):
        return {"user": user_id}

    @app.get("/any")
    async def any_state(user_id: AnyStateUserId):
        return {"user": user_id}

    return app


async def _get(path, headers=None):
    transport = httpx.ASGITransport(app=_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        return await client.get(path, headers=headers or {})


SESSION = {"Authorization": "Bearer a.b.c"}
SERVICE = {"X-Service-Token": "svc", "X-User-Id": "u"}


def _refused(resp):
    assert resp.status_code == 403
    assert resp.json()["detail"] == {
        "message": MESSAGE, "type": ACCOUNT_SUSPENDED, "organization": "Acme",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "headers"),
    [
        ("/session", SESSION),
        ("/session", SERVICE),
        ("/service", SERVICE),
        ("/optional", SESSION),
        ("/optional", SERVICE),
        ("/stamp", SESSION),
        ("/stamp", SERVICE),
        ("/principal", SESSION),
        ("/principal", SERVICE),
    ],
)
async def test_every_resolver_refuses_a_suspended_account(path, headers):
    stack, _ = _deployment(SUSPENDED)
    with stack:
        _refused(await _get(path, headers))


@pytest.mark.asyncio
@pytest.mark.parametrize("account", [NO_ORG, None], ids=["no-org", "unknown"])
async def test_an_active_or_unknown_account_passes(account):
    """Unknown passes: an outage must not lock every member out of their own
    work, whose turns are refused on their own."""
    stack, _ = _deployment(account)
    with stack:
        for path in ("/session", "/principal", "/optional", "/stamp"):
            resp = await _get(path, SESSION)
            assert resp.status_code == 200, path


@pytest.mark.asyncio
async def test_the_token_kinds_are_refused_too():
    from src.server.auth.pats import PatPrincipal
    from src.server.services.cli_tokens import CliIdentity

    stack, _ = _deployment(SUSPENDED)
    with stack:
        stack.enter_context(
            patch(
                "src.server.auth.pats.verify_pat",
                AsyncMock(return_value=PatPrincipal("u", ("agent:read",))),
            )
        )
        stack.enter_context(
            patch(
                "src.server.services.cli_tokens.authenticate",
                AsyncMock(return_value=CliIdentity("c-1", "u", ("agent:read",))),
            )
        )
        stack.enter_context(
            patch("src.server.auth.oauth_resource.from_resource_issuer", return_value=True)
        )
        stack.enter_context(
            patch(
                "src.server.auth.oauth_resource.verify_access_token",
                AsyncMock(return_value=SimpleNamespace(
                    user_id="u", client_id="app", scopes=("agent:read",)
                )),
            )
        )
        _refused(await _get("/principal", {"Authorization": "Bearer la_pat_x"}))
        _refused(await _get("/principal", {"Authorization": "Bearer lac1.c-1.x"}))
        _refused(await _get("/principal", {"Authorization": "Bearer an.oauth.jwt"}))


@pytest.mark.asyncio
async def test_a_public_page_reads_a_suspended_owner_as_a_visitor():
    stack, _ = _deployment(SUSPENDED)
    with stack:
        resp = await _get("/viewer", SESSION)
    assert resp.status_code == 200 and resp.json() == {"user": None}


@pytest.mark.asyncio
async def test_the_open_resolver_lets_a_suspended_account_through():
    stack, read = _deployment(SUSPENDED)
    with stack:
        resp = await _get("/any", SESSION)
    assert resp.json() == {"user": "u"}
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_two_resolvers_read_the_account_once():
    stack, read = _deployment(NO_ORG)
    with stack:
        assert (await _get("/two", SESSION)).status_code == 200
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_with_the_flag_off_the_gate_reads_nothing():
    stack, read = _deployment(SUSPENDED, door=False)
    with stack, patch(
        "src.server.services.membership.share_membership_read"
    ) as shared:
        for path in ("/session", "/principal", "/optional", "/stamp", "/service"):
            headers = SERVICE if path == "/service" else SESSION
            assert (await _get(path, headers)).status_code == 200
    read.assert_not_awaited()
    shared.assert_not_called()


@pytest.mark.asyncio
async def test_the_file_mount_closes_with_the_account():
    from src.server.app import livefs
    from src.server.services.livefs.tokens import LivefsIdentity

    app = FastAPI()

    @app.get("/mount")
    async def mount(identity=Depends(livefs._caller)):
        return {"user": identity.user_id}

    stack, _ = _deployment(SUSPENDED)
    with stack, patch(
        "src.server.app.livefs.authenticate",
        AsyncMock(return_value=LivefsIdentity("c-1", "u", "/root")),
    ):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            resp = await client.get("/mount", headers={"Authorization": "Bearer m"})
    _refused(resp)


@pytest.mark.asyncio
async def test_a_websocket_is_closed_for_a_suspended_account():
    from src.server.auth.ws_auth import authenticate_websocket

    socket = SimpleNamespace(
        headers={"authorization": "Bearer a.b.c"}, query_params={}, close=AsyncMock()
    )
    stack, _ = _deployment(SUSPENDED)
    with stack, patch("src.server.auth.ws_auth.HOST_MODE", "platform"), patch(
        "src.server.auth.ws_auth._decode_token", AsyncMock(return_value=AuthInfo("u"))
    ), pytest.raises(WebSocketException) as closed:
        await authenticate_websocket(socket)
    assert closed.value.reason == ACCOUNT_SUSPENDED
    socket.close.assert_awaited_once()


# --- what the client sees ----------------------------------------------------------


@pytest.mark.asyncio
async def test_users_me_answers_and_says_so_while_every_other_route_refuses():
    from src.server.app.users import router

    app = FastAPI()
    app.include_router(router)
    result = {
        "user": {
            "user_id": "u", "email": "e@example.com", "name": "N",
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
        },
        "preferences": None,
    }
    stack, _ = _deployment(SUSPENDED)
    with stack:
        stack.enter_context(patch(
            "src.server.app.users.get_user_with_preferences",
            AsyncMock(return_value=result),
        ))
        stack.enter_context(patch(
            "src.server.services.membership.read_membership",
            AsyncMock(return_value={"access_tier": 1, "plan_display_name": "Pro"}),
        ))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            me = await client.get("/api/v1/users/me", headers=SESSION)
            prefs = await client.get("/api/v1/users/me/preferences", headers=SESSION)
    assert me.status_code == 200
    assert me.json()["user"]["organization"] == {
        "id": "o-1", "name": "Acme", "suspended": True, "teams": [],
    }
    _refused(prefs)


# --- what a request costs ------------------------------------------------------------


def _measured(door):
    from tests.unit.server.services.test_account_contract import _Auth, _Cache
    from tests.unit.server.services.test_account_contract import (
        _deployment as membership_deployment,
    )

    cache, auth = _Cache(), _Auth({"valid": True, "access_tier": 2})
    stack = membership_deployment(cache, auth, door=door)
    stack.enter_context(patch("src.server.utils.api.HOST_MODE", "platform"))
    stack.enter_context(
        patch(
            "src.server.utils.api._decode_token",
            AsyncMock(return_value=AuthInfo(user_id="u")),
        )
    )
    stack.enter_context(
        patch("src.server.services.account.deployment_policy", return_value=None)
    )
    return stack, cache


def _cost_app() -> FastAPI:
    from src.server.services.membership import MembershipReadScope
    from src.server.services.membership import access_tier
    from src.server.services.account import require_policy

    app = FastAPI()
    # As the app installs it (``app/setup.py``).
    app.add_middleware(MembershipReadScope)

    @app.get("/reads-nothing")
    async def reads_nothing(user_id: CurrentUserId):
        return {}

    @app.get("/reads-the-membership")
    async def reads_the_membership(user_id: CurrentUserId):
        # As a turn's admission does: the policy, then the plan.
        await require_policy(user_id)
        await access_tier(user_id)
        return {}

    return app


async def _call(path):
    transport = httpx.ASGITransport(app=_cost_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        return await client.get(path, headers=SESSION)


@pytest.mark.asyncio
async def test_with_teams_on_a_request_pays_one_cached_read_at_most():
    keys = [
        "platform_membership:v4:u",
        "platform_membership:last_known:v4:u",
        "platform_membership:unreachable:u",
        "platform_membership:epoch:u",
        "platform_membership:suspended:u",
    ]
    entry = {"access_tier": 2, "plan_display_name": "Pro"}
    for path in ("/reads-nothing", "/reads-the-membership"):
        stack, cache = _measured(door=True)
        cache.store.update({keys[0]: entry, keys[1]: entry})
        with stack:
            assert (await _call(path)).status_code == 200
        # One MGET, the one the route already paid where it read the membership.
        assert cache.calls == [("mget", keys)], path


@pytest.mark.asyncio
@pytest.mark.parametrize("door", [False, True])
async def test_with_teams_off_a_request_opens_no_read_scope_and_marks_nothing(door):
    from src.server.services import membership

    opened = MagicMock(wraps=membership.membership_read_scope)
    stack, _ = _measured(door=door)
    with stack, patch.object(membership, "membership_read_scope", opened):
        assert (await _call("/reads-nothing")).status_code == 200
    assert opened.call_count == (1 if door else 0)


@pytest.mark.asyncio
async def test_with_teams_off_admit_caller_touches_no_request_state():
    from types import SimpleNamespace

    from src.server.utils.api import admit_caller

    scope: dict = {}
    with patch("src.server.services.membership.teams_door_open", return_value=False), patch(
        "src.server.services.account.admit", AsyncMock()
    ) as admit:
        await admit_caller(SimpleNamespace(scope=scope), "u")
    assert scope == {}
    admit.assert_not_called()


@pytest.mark.asyncio
async def test_with_teams_off_the_gate_costs_nothing():
    stack, cache = _measured(door=False)
    with stack:
        assert (await _call("/reads-nothing")).status_code == 200
    assert cache.calls == []
    # A route that reads the plan reads it exactly as before: three keys, no epoch.
    stack, cache = _measured(door=False)
    with stack:
        assert (await _call("/reads-the-membership")).status_code == 200
    assert [c[0] for c in cache.calls] == ["mget", "set_many"]
    assert cache.calls[0][1] == [
        "platform_membership:u",
        "platform_membership:last_known:u",
        "platform_membership:unreachable:u",
    ]


# --- the owner's grants -----------------------------------------------------------


def _owner(account, *, door=True):
    stack = ExitStack()
    stack.enter_context(patch("src.server.services.membership.teams_door_open", return_value=door))
    read = stack.enter_context(
        patch(f"{ACCOUNT}.read_standing", AsyncMock(return_value=Standing(account, account) if door else Standing(None, None)))
    )
    return stack, read


@pytest.fixture
def _file_grant_key(monkeypatch):
    from src.server.services import file_grants
    from tests.unit.server.app.test_workspace_file_serving import _GRANT_KEY

    monkeypatch.setattr(file_grants._CODEC, "_key", _GRANT_KEY)


@pytest.mark.asyncio
async def test_an_owner_file_grant_ends_with_the_account(_file_grant_key):
    from src.server.app.workspace_files.serve import serve_workspace_file_endpoint
    from tests.unit.server.app.test_workspace_file_serving import (
        _DBWS_PATCH,
        _grant,
        _workspace,
    )

    stack, _ = _owner(SUSPENDED)
    with stack, patch(_DBWS_PATCH, AsyncMock(return_value=_workspace("stopped"))):
        with pytest.raises(HTTPException) as refused:
            await serve_workspace_file_endpoint(grant=_grant(), path="r.html")
    assert refused.value.status_code == 404


@pytest.mark.asyncio
async def test_with_the_flag_off_the_file_grant_reads_the_workspace_once_as_before(
    _file_grant_key,
):
    from src.server.app.workspace_files import serve
    from tests.unit.server.app.test_workspace_file_serving import _grant

    stack, read = _owner(SUSPENDED, door=False)
    from fastapi.responses import Response

    served = AsyncMock(return_value=Response(b"", media_type="text/plain"))
    with stack, patch.object(serve, "serve_workspace_file", served), patch.object(
        serve, "db_get_workspace", AsyncMock()
    ) as row:
        await serve.serve_workspace_file_endpoint(grant=_grant(), path="r.html", inject=None)
    row.assert_not_awaited()  # serving reads it, as before
    assert served.await_args.kwargs["workspace"] is None
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_owner_artifact_grant_ends_with_the_account(monkeypatch):
    from src.server.preview_proxy import artifacts as artifact_proxy
    from src.server.services.artifacts import grants
    from src.server.services.artifacts.grants import mint_artifact_grant
    from tests.unit.server.test_policy_public_links import ARTIFACT, STORED

    monkeypatch.setattr(grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(artifact_proxy, "get_version_file", AsyncMock(return_value=STORED))
    grant = await mint_artifact_grant(ARTIFACT)
    stack, _ = _owner(SUSPENDED)
    with stack:
        assert await artifact_proxy.load_artifact_file(grant.token, "11", "index.html") is None
    stack, _ = _owner(NO_ORG)
    with stack:
        served = await artifact_proxy.load_artifact_file(grant.token, "11", "index.html")
    assert served.status == 200


# --- what a refused request leaves behind ---------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(("account", "recorded"), [(SUSPENDED, False), (NO_ORG, True)])
async def test_a_refused_computer_call_is_recorded_nowhere(account, recorded):
    import asyncio

    from src.server.services import cli_calls
    from src.server.services.cli_tokens import CliIdentity

    app = _app()
    app.add_middleware(cli_calls.CliCallRecorder)
    record = AsyncMock()
    stack, _ = _deployment(account)
    with stack, patch.object(cli_calls.computer_cli_calls, "record", record), patch(
        "src.server.services.cli_tokens.authenticate",
        AsyncMock(return_value=CliIdentity("c-1", "u", ("agent:read",))),
    ):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            resp = await client.get("/principal", headers={"Authorization": "Bearer lac1.c-1.x"})
        await asyncio.gather(*list(cli_calls._writes))
    assert resp.status_code == (403 if account is SUSPENDED else 200)
    assert record.await_count == (1 if recorded else 0)


@pytest.mark.asyncio
async def test_a_database_error_reading_a_grants_owner_is_not_answered_as_not_found(
    _file_grant_key,
):
    from src.server.app.workspace_files import serve
    from tests.unit.server.app.test_workspace_file_serving import _grant

    stack, _ = _owner(SUSPENDED)
    with stack, patch.object(
        serve, "db_get_workspace", AsyncMock(side_effect=ConnectionError("db down"))
    ), pytest.raises(ConnectionError):
        await serve.serve_workspace_file_endpoint(grant=_grant(), path="r.html", inject=None)
