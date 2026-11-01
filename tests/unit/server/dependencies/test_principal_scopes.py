"""What each kind of caller may do on a route declared with ``require_scopes``.

``get_principal`` runs for real: only the token verifiers (a computer's, a
personal access token's) and the database lookups are replaced.
"""

from __future__ import annotations

from dataclasses import asdict
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.auth import computer_token_core
from src.server.auth.pats import PatPrincipal
from src.server.database import principal_reach as reach
from src.server.database.home_workspace import get_home_workspace_id
from src.server.dependencies.principal import (
    CALL_HEADER,
    Principal,
    get_principal,
    require_scopes,
)
from src.server.services import cli_tokens
from src.server.services.cli_tokens import CliIdentity
from src.server.services.livefs import outcomes
from src.server.utils import api
from tests.unit.server.dependencies.reach_world import (
    AUTOMATION,
    COMPUTER,
    OTHER_AUTOMATION,
    OTHER_COMPUTER,
    OTHER_THREAD,
    OTHER_WORKSPACE,
    THREAD,
    UNKNOWN,
    USER,
    WORKSPACE,
    ReachWorld,
)

CALL_ID = "call-0123456789"
SERVICE_SECRET = "service-secret-for-tests"
COMPUTER_TOKEN = f"lac1.{COMPUTER}.secret"
PAT = "la_pat_secret"
HOME = get_home_workspace_id(USER)


def _build_app() -> FastAPI:
    app = FastAPI()

    def ok(principal: Principal) -> dict:
        return {"user_id": principal.user_id, "auth": principal.auth}

    @app.get("/open")
    async def open_route(principal: Principal = Depends(get_principal)):
        return asdict(principal)

    @app.get("/c/{computer_id}")
    async def read_computer(
        computer_id: str, principal: Principal = Depends(require_scopes("agent:read"))
    ):
        return ok(principal)

    @app.post("/c/{computer_id}/stop")
    async def stop_computer(
        computer_id: str,
        principal: Principal = Depends(require_scopes("computer:write", own_computer=False)),
    ):
        return ok(principal)

    @app.get("/w/{workspace_id}")
    async def read_workspace(
        workspace_id: str, principal: Principal = Depends(require_scopes("agent:read"))
    ):
        return ok(principal)

    @app.delete("/w/{workspace_id}")
    async def delete_workspace(
        workspace_id: str, principal: Principal = Depends(require_scopes("workspace:write"))
    ):
        return ok(principal)

    @app.get("/t/{thread_id}")
    async def read_thread(
        thread_id: str, principal: Principal = Depends(require_scopes("agent:read"))
    ):
        return ok(principal)

    @app.post("/t/{thread_id}/run")
    async def run_thread(
        thread_id: str,
        principal: Principal = Depends(require_scopes("agent:run", own_thread=False)),
    ):
        return ok(principal)

    @app.get("/threads")
    async def list_threads(
        workspace_id: str | None = None,
        principal: Principal = Depends(require_scopes("agent:read")),
    ):
        return ok(principal)

    @app.get("/a/{automation_id}")
    async def read_automation(
        automation_id: str, principal: Principal = Depends(require_scopes("agent:read"))
    ):
        return ok(principal)

    @app.post("/a/{automation_id}/trigger")
    async def trigger_automation(
        automation_id: str,
        principal: Principal = Depends(require_scopes("agent:run", own_thread=False)),
    ):
        return ok(principal)

    @app.post("/c")
    async def add_computer(
        principal: Principal = Depends(require_scopes("computer:write", adds_computer=True)),
    ):
        return ok(principal)

    @app.get("/both")
    async def needs_both(
        principal: Principal = Depends(require_scopes("agent:read", "agent:run"))
    ):
        return ok(principal)

    return app


@pytest_asyncio.fixture
async def client():
    with (
        patch.object(api, "HOST_MODE", "oss"),
        patch.object(api, "_SERVICE_TOKEN", SERVICE_SECRET),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=_build_app()), base_url="http://test"
        ) as c:
            yield c


@pytest.fixture
def world(monkeypatch) -> ReachWorld:
    return ReachWorld(monkeypatch)


@pytest.fixture
def computer_token(monkeypatch):
    """Authenticates ``lac1.`` bearers as the computer, holding ``scopes``."""
    state = {"scopes": cli_tokens.SCOPES}
    authenticate = AsyncMock(
        side_effect=lambda token: CliIdentity(COMPUTER, USER, tuple(state["scopes"]))
    )
    monkeypatch.setattr(cli_tokens, "authenticate", authenticate)
    return state


@pytest.fixture
def pat(monkeypatch):
    state = {"scopes": ("agent:read", "agent:run", "workspace:write")}
    verify = AsyncMock(side_effect=lambda token: PatPrincipal(USER, tuple(state["scopes"])))
    monkeypatch.setattr("src.server.auth.pats.verify_pat", verify)
    return state


@pytest.fixture
def filed_call(monkeypatch):
    """What the command that sent the request filed about its own call."""
    filed = {"thread_id": None}

    async def call_context(computer_id, call_id):
        if filed["thread_id"] is None:
            return None
        return CallContext(workspace_id=WORKSPACE, thread_id=filed["thread_id"])

    monkeypatch.setattr(outcomes, "call_context", AsyncMock(side_effect=call_context))
    return filed


def _computer(extra: dict | None = None) -> dict:
    return {"Authorization": f"Bearer {COMPUTER_TOKEN}", **(extra or {})}


def _pat() -> dict:
    return {"Authorization": f"Bearer {PAT}"}


# -- session and service ---------------------------------------------------------


@pytest.mark.asyncio
async def test_a_session_passes_a_scoped_route_with_no_scopes(client, world):
    resp = await client.get(f"/c/{OTHER_COMPUTER}")

    assert resp.status_code == 200
    assert resp.json()["auth"] == "session"


@pytest.mark.asyncio
async def test_a_session_may_delete(client, world):
    resp = await client.delete(f"/w/{OTHER_WORKSPACE}")

    assert resp.status_code == 200
    assert world.lookups == 0


@pytest.mark.asyncio
async def test_the_servers_own_call_passes_as_the_user_it_names(client, world):
    resp = await client.get(
        f"/w/{OTHER_WORKSPACE}",
        headers={"X-Service-Token": SERVICE_SECRET, "X-User-Id": "user-named-by-service"},
    )

    assert resp.json() == {"user_id": "user-named-by-service", "auth": "service"}
    assert world.lookups == 0


@pytest.mark.asyncio
async def test_a_wrong_service_token_is_refused(client):
    resp = await client.get("/open", headers={"X-Service-Token": "not-the-secret"})

    assert resp.status_code == 401


# -- scopes ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_token_without_the_scope_gets_insufficient_scope(client, pat):
    pat["scopes"] = ("agent:run",)

    resp = await client.get(f"/c/{COMPUTER}", headers=_pat())

    assert resp.status_code == 403
    assert resp.json()["detail"]["type"] == "insufficient_scope"
    assert resp.headers["WWW-Authenticate"] == (
        'Bearer error="insufficient_scope", scope="agent:read"'
    )


@pytest.mark.asyncio
async def test_every_scope_a_route_declares_is_required(client, pat):
    pat["scopes"] = ("agent:read",)

    resp = await client.get("/both", headers=_pat())

    assert resp.status_code == 403
    assert "agent:run" in resp.json()["detail"]["message"]
    assert resp.headers["WWW-Authenticate"].endswith('scope="agent:read agent:run"')


@pytest.mark.asyncio
async def test_a_token_holding_every_scope_passes(client, pat):
    pat["scopes"] = ("agent:read", "agent:run")

    resp = await client.get("/both", headers=_pat())

    assert resp.status_code == 200
    assert resp.json() == {"user_id": USER, "auth": "pat"}


@pytest.mark.asyncio
async def test_a_computer_token_lacking_the_scope_is_refused_before_any_reach_check(
    client, world, computer_token
):
    computer_token["scopes"] = ("agent:run",)

    resp = await client.get(f"/w/{WORKSPACE}", headers=_computer())

    assert resp.status_code == 403
    assert resp.json()["detail"]["type"] == "insufficient_scope"
    assert world.lookups == 0


# -- a personal access token -----------------------------------------------------


@pytest.mark.asyncio
async def test_reach_applies_only_to_a_computer_token_so_a_pat_reaches_any_computer(
    client, world, pat
):
    resp = await client.get(f"/c/{OTHER_COMPUTER}", headers=_pat())
    deleted = await client.delete(f"/w/{OTHER_WORKSPACE}", headers=_pat())

    assert resp.status_code == 200
    assert deleted.status_code == 200
    assert world.lookups == 0


# -- a computer's token: what it may reach --------------------------------------


@pytest.mark.asyncio
async def test_a_computer_token_reads_its_own_computer(client, computer_token):
    resp = await client.get(f"/c/{COMPUTER}", headers=_computer())

    assert resp.status_code == 200
    assert resp.json() == {"user_id": USER, "auth": "computer"}


@pytest.mark.asyncio
async def test_a_computer_token_cannot_reach_another_computer(client, computer_token):
    resp = await client.get(f"/c/{OTHER_COMPUTER}", headers=_computer())

    assert resp.status_code == 403
    assert resp.json()["detail"]["type"] == "forbidden_for_token"


@pytest.mark.asyncio
async def test_a_computer_token_cannot_stop_its_own_computer(client, computer_token):
    resp = await client.post(f"/c/{COMPUTER}/stop", headers=_computer())

    assert resp.status_code == 403
    assert "itself" in resp.json()["detail"]["message"]


@pytest.mark.asyncio
async def test_a_computer_token_cannot_stop_another_computer_either(client, computer_token):
    resp = await client.post(f"/c/{OTHER_COMPUTER}/stop", headers=_computer())

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_a_session_may_stop_a_computer(client):
    resp = await client.post(f"/c/{COMPUTER}/stop")

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_a_computer_token_cannot_delete(client, world, computer_token):
    resp = await client.delete(f"/w/{WORKSPACE}", headers=_computer())

    assert resp.status_code == 403
    assert "cannot delete" in resp.json()["detail"]["message"]


@pytest.mark.asyncio
async def test_a_computer_token_reads_a_workspace_on_its_computer(
    client, world, computer_token
):
    resp = await client.get(f"/w/{WORKSPACE}", headers=_computer())

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_a_computer_token_cannot_reach_a_workspace_on_another_computer(
    client, world, computer_token
):
    resp = await client.get(f"/w/{OTHER_WORKSPACE}", headers=_computer())

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_a_computer_token_passes_a_workspace_the_user_does_not_have(
    client, world, computer_token
):
    """The route answers its own 404; the guard must not turn it into a 403
    that tells the computer which ids exist elsewhere."""
    resp = await client.get(f"/w/{UNKNOWN}", headers=_computer())

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_a_computer_token_passes_an_id_that_is_not_a_uuid_to_the_route(
    client, world, computer_token
):
    resp = await client.get("/w/not-a-uuid", headers=_computer())

    assert resp.status_code == 200
    assert world.lookups == 0


@pytest.mark.asyncio
async def test_a_computer_token_cannot_reach_a_workspace_that_sits_on_no_computer(
    client, world, computer_token
):
    world.computers[WORKSPACE] = None

    resp = await client.get(f"/w/{WORKSPACE}", headers=_computer())

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_a_computer_token_reaches_its_threads_and_not_another_computers(
    client, world, computer_token
):
    own = await client.get(f"/t/{THREAD}", headers=_computer())
    other = await client.get(f"/t/{OTHER_THREAD}", headers=_computer())

    assert (own.status_code, other.status_code) == (200, 403)


@pytest.mark.asyncio
async def test_a_computer_token_reaches_its_automations_and_not_another_computers(
    client, world, computer_token
):
    own = await client.get(f"/a/{AUTOMATION}", headers=_computer())
    other = await client.get(f"/a/{OTHER_AUTOMATION}", headers=_computer())

    assert (own.status_code, other.status_code) == (200, 403)


# -- a computer's token: never Home ----------------------------------------------


def _never_home(resp) -> None:
    assert resp.status_code == 403
    assert resp.json()["detail"]["message"] == "A computer's token never reaches Home"


@pytest.mark.asyncio
async def test_a_computer_token_cannot_reach_home_though_home_sits_on_its_computer(
    client, world, computer_token
):
    """A turn in Home runs the Chief of Staff, whose tools reach every computer."""
    world.computers[HOME] = COMPUTER

    _never_home(await client.get(f"/w/{HOME}", headers=_computer()))
    assert world.lookups == 0


@pytest.mark.asyncio
async def test_a_computer_token_cannot_reach_home_before_its_row_exists(
    client, world, computer_token
):
    """The lookup would answer MISSING, which passes to the route's 404; a
    route naming Home's id would create it instead."""
    _never_home(await client.get(f"/w/{HOME}", headers=_computer()))


@pytest.mark.asyncio
async def test_a_computer_token_cannot_filter_a_list_to_home(client, world, computer_token):
    _never_home(await client.get(f"/threads?workspace_id={HOME}", headers=_computer()))


@pytest.mark.asyncio
async def test_a_computer_token_cannot_reach_a_thread_or_automation_in_home(
    client, world, computer_token
):
    world.computers[THREAD] = reach.HOME
    world.computers[AUTOMATION] = reach.HOME

    _never_home(await client.get(f"/t/{THREAD}", headers=_computer()))
    _never_home(await client.get(f"/a/{AUTOMATION}", headers=_computer()))


@pytest.mark.asyncio
async def test_a_session_and_a_pat_reach_home(client, world, pat):
    session = await client.get(f"/w/{HOME}")
    token = await client.get(f"/w/{HOME}", headers=_pat())

    assert (session.status_code, token.status_code) == (200, 200)


# -- a computer's token: adds no computer -----------------------------------------


@pytest.mark.asyncio
async def test_a_computer_token_cannot_add_a_computer(client, world, computer_token):
    """A machine it added would hold a token of its own, outliving this one's
    revocation."""
    resp = await client.post("/c", headers=_computer())

    assert resp.status_code == 403
    assert "add or pair a computer" in resp.json()["detail"]["message"]


@pytest.mark.asyncio
async def test_a_session_and_a_pat_may_add_a_computer(client, world, pat):
    pat["scopes"] = ("computer:write",)

    session = await client.post("/c")
    token = await client.post("/c", headers=_pat())

    assert (session.status_code, token.status_code) == (200, 200)


@pytest.mark.asyncio
async def test_the_lookup_is_made_for_the_token_owner(client, world, computer_token):
    await client.get(f"/w/{WORKSPACE}", headers=_computer())

    assert world.asked("workspace") == [(USER, WORKSPACE)]


# -- a computer's token: not into the thread its call came from -----------------


@pytest.mark.asyncio
async def test_a_computer_token_cannot_run_into_the_thread_its_call_came_from(
    client, world, computer_token, filed_call
):
    filed_call["thread_id"] = THREAD

    resp = await client.post(
        f"/t/{THREAD}/run", headers=_computer({CALL_HEADER: CALL_ID})
    )

    assert resp.status_code == 403
    assert "conversation it runs in" in resp.json()["detail"]["message"]
    outcomes.call_context.assert_awaited_once_with(COMPUTER, CALL_ID)


@pytest.mark.asyncio
async def test_a_computer_token_runs_into_another_thread_of_its_computer(
    client, world, computer_token, filed_call
):
    world.computers[OTHER_THREAD] = COMPUTER
    filed_call["thread_id"] = THREAD

    resp = await client.post(
        f"/t/{OTHER_THREAD}/run", headers=_computer({CALL_HEADER: CALL_ID})
    )

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_a_call_whose_filing_is_gone_may_not_start_a_run(
    client, world, computer_token, filed_call
):
    """The command named its call, so it runs in some conversation; with
    what it filed gone (Redis down, or past its TTL) the run it would start
    could be in that one."""
    resp = await client.post(
        f"/t/{THREAD}/run", headers=_computer({CALL_HEADER: CALL_ID})
    )

    assert resp.status_code == 403
    assert "Cannot tell which conversation" in resp.json()["detail"]["message"]
    assert (await client.get(f"/t/{THREAD}", headers=_computer({CALL_HEADER: CALL_ID}))).status_code == 200


@pytest.mark.asyncio
async def test_without_the_call_header_the_own_thread_is_not_refused(
    client, world, computer_token, filed_call
):
    filed_call["thread_id"] = THREAD

    resp = await client.post(f"/t/{THREAD}/run", headers=_computer())

    assert resp.status_code == 200
    outcomes.call_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_malformed_call_id_is_not_looked_up(
    client, world, computer_token, filed_call
):
    filed_call["thread_id"] = THREAD

    resp = await client.post(f"/t/{THREAD}/run", headers=_computer({CALL_HEADER: "short"}))

    assert resp.status_code == 200
    outcomes.call_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_reading_the_own_thread_is_allowed_for_the_call_that_runs_in_it(
    client, world, computer_token, filed_call
):
    """The guard is on what a route marks ``own_thread=False``: sending, not
    reading."""
    filed_call["thread_id"] = THREAD

    resp = await client.get(f"/t/{THREAD}", headers=_computer({CALL_HEADER: CALL_ID}))

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_a_pat_may_run_into_a_thread_whatever_the_call_header_says(
    client, world, pat, filed_call
):
    filed_call["thread_id"] = THREAD

    resp = await client.post(f"/t/{THREAD}/run", headers={**_pat(), CALL_HEADER: CALL_ID})

    assert resp.status_code == 200
    outcomes.call_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_computer_token_cannot_trigger_an_automation_into_its_own_thread(
    client, world, computer_token, filed_call
):
    """The run would start a turn in the thread the call runs in."""
    filed_call["thread_id"] = THREAD
    world.automation_threads[AUTOMATION] = THREAD

    resp = await client.post(
        f"/a/{AUTOMATION}/trigger", headers=_computer({CALL_HEADER: CALL_ID})
    )

    assert resp.status_code == 403
    assert "conversation it runs in" in resp.json()["detail"]["message"]
    world.thread_of.assert_awaited_once_with(USER, AUTOMATION)


@pytest.mark.asyncio
async def test_a_computer_token_triggers_an_automation_into_a_sibling_thread(
    client, world, computer_token, filed_call
):
    filed_call["thread_id"] = THREAD
    world.automation_threads[AUTOMATION] = OTHER_THREAD

    resp = await client.post(
        f"/a/{AUTOMATION}/trigger", headers=_computer({CALL_HEADER: CALL_ID})
    )

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_a_trigger_from_no_thread_never_looks_up_the_automations(
    client, world, computer_token, filed_call
):
    resp = await client.post(f"/a/{AUTOMATION}/trigger", headers=_computer())

    assert resp.status_code == 200
    world.thread_of.assert_not_awaited()


# -- a computer's token: what a query names ------------------------------------


@pytest.mark.asyncio
async def test_a_list_filtered_to_a_workspace_of_its_computer_is_allowed(
    client, world, computer_token
):
    resp = await client.get(f"/threads?workspace_id={WORKSPACE}", headers=_computer())

    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_a_list_filtered_to_a_workspace_of_another_computer_is_refused(
    client, world, computer_token
):
    resp = await client.get(f"/threads?workspace_id={OTHER_WORKSPACE}", headers=_computer())

    assert resp.status_code == 403
    assert resp.json()["detail"]["type"] == "forbidden_for_token"


# -- a refused or throttled computer token --------------------------------------


@pytest.mark.asyncio
async def test_a_refused_computer_token_is_a_401_and_never_a_session(client, monkeypatch):
    """The server runs in oss mode here, where a request with no credential
    is the local user: a refused token must not fall through to that."""
    monkeypatch.setattr(
        cli_tokens, "authenticate", AsyncMock(side_effect=computer_token_core.Refused)
    )

    resp = await client.get("/open", headers=_computer())

    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"] == 'Bearer error="invalid_token"'


@pytest.mark.asyncio
async def test_a_throttled_computer_token_is_a_429_with_retry_after(client, monkeypatch):
    monkeypatch.setattr(
        cli_tokens, "authenticate", AsyncMock(side_effect=cli_tokens.Throttled(COMPUTER, None, 7000))
    )

    resp = await client.get("/open", headers=_computer())

    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "7"
    assert resp.json()["detail"]["type"] == "rate_limited"


@pytest.mark.asyncio
async def test_an_authenticated_computer_token_carries_its_computer_and_scopes(
    client, computer_token
):
    resp = await client.get("/open", headers=_computer())

    assert resp.json() == {
        "user_id": USER,
        "auth": "computer",
        "client_id": None,
        "scopes": list(cli_tokens.SCOPES),
        "computer_id": COMPUTER,
    }
