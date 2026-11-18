"""A computer's token reaches nothing a shared workspace holds.

``require_scopes`` runs for real against ``turn_targets.in_shared_workspace``
for real; only its database read is replaced, by rows held here, so the test
also sees how many reads a request costs. On a hit the person-level checks
(``shared_workspaces.access``) run for real too, over a :class:`SharedWorld`
standing in for their reads, so the refusal is pinned against the same rule
that answers a person in the app.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI, HTTPException, Request
from httpx import ASGITransport, AsyncClient

from src.server.database import turn_targets
from src.server.database.shared_workspaces import SharedRow
from src.server.dependencies import principal as principal_module
from src.server.dependencies.principal import (
    SHARED_WORKSPACE_REFUSAL,
    Principal,
    get_principal,
    require_scopes,
)
from src.server.services import membership
from src.server.services.shared_workspaces import access as reach
from tests.unit.server.dependencies.reach_world import (
    COMPUTER,
    OTHER_COMPUTER,
    OTHER_THREAD,
    OTHER_WORKSPACE,
    THREAD,
    UNKNOWN,
    USER,
    WORKSPACE,
    act_as_computer,
)

ORG = "org-reach-1"
OTHER_ORG = "org-reach-2"

# The caller's organization: a workspace they are listed on, with a thread
# shared with it, one they started and keep private, another member's
# private one, and its holder's computer.
SHARED_WORKSPACE = "a1100000-0000-4000-8000-0000000000aa"
SHARED_THREAD = "b2200000-0000-4000-8000-0000000000aa"
MY_PRIVATE_THREAD = "b2200000-0000-4000-8000-0000000000ab"
PRIVATE_THREAD = "b2200000-0000-4000-8000-0000000000ac"
HOLDER_COMPUTER = "c0ffee00-0000-4000-8000-0000000000aa"
HOLDER = "shared-6a3c1f3e-0000-4000-8000-000000000001"
# The caller's organization again: one they are not on, open to no one.
UNREACHED_WORKSPACE = "a1100000-0000-4000-8000-0000000000bb"
UNREACHED_THREAD = "b2200000-0000-4000-8000-0000000000bb"
UNREACHED_COMPUTER = "c0ffee00-0000-4000-8000-0000000000bb"
UNREACHED_HOLDER = "shared-6a3c1f3e-0000-4000-8000-000000000002"
# Another organization's, open to everyone there.
FOREIGN_WORKSPACE = "a1100000-0000-4000-8000-0000000000cc"
FOREIGN_THREAD = "b2200000-0000-4000-8000-0000000000cc"
FOREIGN_COMPUTER = "c0ffee00-0000-4000-8000-0000000000cc"
FOREIGN_HOLDER = "shared-6a3c1f3e-0000-4000-8000-000000000003"


class SharedWorld:
    """The shared workspaces of two organizations, ``user`` a member of the
    first, as the reads behind ``in_shared_workspace`` and the access checks
    find them. ``door`` is the ``teams`` door; ``seat`` None makes ``user``
    a member of no organization. ``access_reads`` names each person-level
    read, so a test sees when they are asked at all."""

    def __init__(self, user: str) -> None:
        self.user = user
        self.door = True
        self.seat: reach.Seat | None = reach.Seat(ORG)
        self.rows = {
            SHARED_WORKSPACE: SharedRow(
                SHARED_WORKSPACE, HOLDER, ORG, False, "someone", "members"
            ),
            UNREACHED_WORKSPACE: SharedRow(
                UNREACHED_WORKSPACE, UNREACHED_HOLDER, ORG, False, "someone", "members"
            ),
            FOREIGN_WORKSPACE: SharedRow(
                FOREIGN_WORKSPACE, FOREIGN_HOLDER, OTHER_ORG, True, "someone", "members"
            ),
        }
        self.listed = {(SHARED_WORKSPACE, user)}
        self.thread_workspace = {
            SHARED_THREAD: SHARED_WORKSPACE,
            MY_PRIVATE_THREAD: SHARED_WORKSPACE,
            PRIVATE_THREAD: SHARED_WORKSPACE,
            UNREACHED_THREAD: UNREACHED_WORKSPACE,
            FOREIGN_THREAD: FOREIGN_WORKSPACE,
        }
        # Threads ``user`` may not see in a workspace they reach.
        self.hidden = {PRIVATE_THREAD}
        self.computer_owner = {
            HOLDER_COMPUTER: HOLDER,
            UNREACHED_COMPUTER: UNREACHED_HOLDER,
            FOREIGN_COMPUTER: FOREIGN_HOLDER,
        }
        self.access_reads: list[str] = []

    def is_shared(self, *, workspace_id=None, thread_id=None, computer_id=None) -> bool:
        """What ``in_shared_workspace``'s one read answers: any shared
        workspace on the deployment, whoever reaches it."""
        return (
            workspace_id in self.rows
            or self.thread_workspace.get(thread_id) in self.rows
            or computer_id in self.computer_owner
        )

    def install(self, monkeypatch) -> None:
        world = self
        monkeypatch.setattr(membership, "teams_door_open", lambda: world.door)

        async def seat(caller):
            world.access_reads.append("seat")
            return world.seat if caller == world.user else None

        async def shared_row(workspace_id):
            world.access_reads.append("shared_row")
            return world.rows.get(workspace_id)

        async def role(workspace_id, caller):
            return "member" if (workspace_id, caller) in world.listed else None

        async def of_holder(holder):
            return next((r for r in world.rows.values() if r.holder_id == holder), None)

        async def thread_meta(thread_id, *, conn=None):
            world.access_reads.append("thread")
            workspace = world.thread_workspace.get(thread_id)
            if workspace is None:
                return None
            return {"workspace_id": workspace, "user_id": world.rows[workspace].holder_id}

        async def hidden(thread_ids, caller, *, conn=None):
            return {t for t in thread_ids if t in world.hidden}

        async def workspace_row(workspace_id, conn=None):
            world.access_reads.append("workspace")
            row = world.rows.get(workspace_id)
            return {"workspace_id": workspace_id, "user_id": row.holder_id} if row else None

        async def computer_row(computer_id, **kw):
            world.access_reads.append("computer")
            owner = world.computer_owner.get(computer_id)
            return {"computer_id": computer_id, "user_id": owner} if owner else None

        monkeypatch.setattr(reach, "caller_seat", seat)
        monkeypatch.setattr(reach.db, "get_shared_workspace", shared_row)
        monkeypatch.setattr(reach.db, "get_member_role", role)
        monkeypatch.setattr(reach.db, "shared_of_holder", of_holder)
        monkeypatch.setattr(reach.conversation_db, "get_thread_auth_meta", thread_meta)
        monkeypatch.setattr(reach.thread_reach, "hidden", hidden)
        monkeypatch.setattr(principal_module, "get_workspace", workspace_row)
        monkeypatch.setattr(principal_module, "get_computer", computer_row)


class Rows:
    """What the one read in ``in_shared_workspace`` finds."""

    def __init__(self, world: SharedWorld) -> None:
        self.world = world
        self.computer_owner = {COMPUTER: USER, OTHER_COMPUTER: USER}
        self.reads: list[dict] = []

    @asynccontextmanager
    async def connection(self):
        rows = self

        class Cursor:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, sql, params):
                rows.reads.append(params)
                self.params = params

            async def fetchone(self):
                p = self.params
                world = rows.world
                shared = world.is_shared(workspace_id=p["workspace"], thread_id=p["thread"])
                owner = rows.computer_owner.get(p["computer"]) or world.computer_owner.get(
                    p["computer"]
                )
                return (shared, owner)

        class Conn:
            def cursor(self):
                return Cursor()

        yield Conn()


@pytest.fixture
def world(monkeypatch) -> SharedWorld:
    found = SharedWorld(USER)
    found.install(monkeypatch)
    return found


@pytest.fixture
def rows(monkeypatch, world) -> Rows:
    found = Rows(world)
    monkeypatch.setattr(turn_targets, "get_db_connection", found.connection)
    return found


# What the stand-in routes answer as their own: the user's personal rows,
# and for anything else the 404 a route gives an id it cannot open.
_PERSONAL = {WORKSPACE, OTHER_WORKSPACE, THREAD, OTHER_THREAD, COMPUTER, OTHER_COMPUTER}


def _route(*ids: str | None) -> dict:
    if any(i is not None and i not in _PERSONAL for i in ids):
        raise HTTPException(status_code=404, detail="Not found")
    return {"ok": True}


def _build_app() -> FastAPI:
    app = FastAPI()
    read = require_scopes("agent:read")

    @app.get("/w/{workspace_id}")
    async def workspace(workspace_id: str, principal: Principal = Depends(read)):
        return _route(workspace_id)

    @app.get("/t/{thread_id}")
    async def thread(thread_id: str, principal: Principal = Depends(read)):
        return _route(thread_id)

    @app.post("/c/{computer_id}/start")
    async def computer(
        computer_id: str, principal: Principal = Depends(require_scopes("computer:write"))
    ):
        return _route(computer_id)

    @app.get("/threads")
    async def threads(
        workspace_id: str | None = None,
        thread_id: str | None = None,
        principal: Principal = Depends(read),
    ):
        return _route(workspace_id, thread_id)

    @app.get("/plain")
    async def plain(principal: Principal = Depends(read)):
        return _route()

    return app


@pytest_asyncio.fixture
async def app() -> FastAPI:
    return _build_app()


@pytest_asyncio.fixture
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
def as_computer(app):
    return act_as_computer(app)


@pytest.fixture
def as_session(app):
    async def session(request: Request) -> Principal:
        return Principal(USER, "session")

    app.dependency_overrides[get_principal] = session


def _refused(resp) -> None:
    assert resp.status_code == 403
    assert resp.json()["detail"] == {
        "message": SHARED_WORKSPACE_REFUSAL,
        "type": "forbidden_for_token",
    }


def _answer(resp) -> tuple[int, object]:
    return resp.status_code, resp.json()


async def _call(client, route: str, target: str):
    url = route.format(target)
    return await (client.post(url) if url.startswith("/c/") else client.get(url))


# (where the id is named, what the member reaches, what they do not reach in
# their own organization, another organization's, another member's private
# thread)
_KINDS = [
    ("/w/{}", [SHARED_WORKSPACE], [UNREACHED_WORKSPACE, FOREIGN_WORKSPACE]),
    ("/threads?workspace_id={}", [SHARED_WORKSPACE], [UNREACHED_WORKSPACE, FOREIGN_WORKSPACE]),
    (
        "/t/{}",
        [SHARED_THREAD, MY_PRIVATE_THREAD],
        [UNREACHED_THREAD, FOREIGN_THREAD, PRIVATE_THREAD],
    ),
    (
        "/threads?thread_id={}",
        [SHARED_THREAD, MY_PRIVATE_THREAD],
        [UNREACHED_THREAD, FOREIGN_THREAD, PRIVATE_THREAD],
    ),
    ("/c/{}/start", [HOLDER_COMPUTER], [UNREACHED_COMPUTER, FOREIGN_COMPUTER]),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("route", "reached", "unreached"), _KINDS)
async def test_a_member_is_refused_what_they_reach_and_told_nothing_of_the_rest(
    client, rows, world, as_computer, route, reached, unreached
):
    for target in reached:
        _refused(await _call(client, route, target))

    # Anything the user does not reach answers exactly as an id that does
    # not exist, so the refusal never says an id is someone else's shared one.
    made_up = _answer(await _call(client, route, UNKNOWN))
    assert made_up == (404, {"detail": "Not found"})
    for target in unreached:
        assert _answer(await _call(client, route, target)) == made_up


@pytest.mark.asyncio
@pytest.mark.parametrize(("route", "reached", "unreached"), _KINDS)
async def test_someone_in_no_organization_is_told_nothing(
    client, rows, world, as_computer, route, reached, unreached
):
    world.seat = None
    made_up = _answer(await _call(client, route, UNKNOWN))
    for target in reached + unreached:
        assert _answer(await _call(client, route, target)) == made_up


@pytest.mark.asyncio
@pytest.mark.parametrize(("route", "reached", "unreached"), _KINDS)
async def test_with_the_teams_door_shut_nothing_is_read(
    client, rows, world, as_computer, route, reached, unreached
):
    # No one reaches a shared workspace then, so its routes 404 on their own.
    world.door = False
    made_up = _answer(await _call(client, route, UNKNOWN))
    for target in reached + unreached:
        assert _answer(await _call(client, route, target)) == made_up
    assert rows.reads == []
    assert world.access_reads == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "personal"),
    [
        ("/w/{}", WORKSPACE),
        ("/threads?workspace_id={}", OTHER_WORKSPACE),
        ("/t/{}", THREAD),
        ("/threads?thread_id={}", OTHER_THREAD),
        ("/c/{}/start", OTHER_COMPUTER),
    ],
)
async def test_a_personal_id_costs_one_small_read(client, rows, world, as_computer, route, personal):
    assert (await _call(client, route, personal)).status_code == 200
    assert len(rows.reads) == 1
    # The person-level checks run only on a hit.
    assert world.access_reads == []


@pytest.mark.asyncio
async def test_its_own_computer_costs_no_read(client, rows, as_computer):
    assert (await client.post(f"/c/{COMPUTER}/start")).status_code == 200
    assert rows.reads == []


@pytest.mark.asyncio
async def test_a_request_naming_nothing_costs_no_read(client, rows, as_computer):
    assert (await client.get("/plain")).status_code == 200
    assert (await client.get("/threads")).status_code == 200
    assert rows.reads == []


@pytest.mark.asyncio
async def test_a_path_id_wins_over_the_querys(client, rows, world, as_computer):
    # The route acts on the path's thread, so that is the one judged.
    _refused(await client.get(f"/t/{SHARED_THREAD}?thread_id={THREAD}"))
    assert (await client.get(f"/t/{THREAD}?thread_id={SHARED_THREAD}")).status_code == 200


@pytest.mark.asyncio
async def test_a_path_workspace_and_a_query_thread_are_judged_together(
    client, rows, world, as_computer
):
    _refused(await client.get(f"/w/{WORKSPACE}?thread_id={SHARED_THREAD}"))
    assert len(rows.reads) == 1


@pytest.mark.asyncio
async def test_a_malformed_id_is_never_read(client, rows, world, as_computer):
    for route in ("/w/{}", "/t/{}", "/threads?workspace_id={}", "/c/{}/start"):
        assert (await _call(client, route, "not-a-uuid")).status_code == 404
    assert rows.reads == []
    assert world.access_reads == []


@pytest.mark.asyncio
async def test_a_session_reaches_a_shared_workspace_with_no_lookup(
    client, rows, world, as_session
):
    for route in (
        f"/w/{SHARED_WORKSPACE}",
        f"/t/{SHARED_THREAD}",
        f"/threads?workspace_id={SHARED_WORKSPACE}",
        f"/threads?thread_id={SHARED_THREAD}",
        f"/c/{HOLDER_COMPUTER}/start",
    ):
        # The stand-in route's own answer, never the token's refusal.
        assert (await _call(client, route, "")).status_code == 404
    assert rows.reads == []
    assert world.access_reads == []
