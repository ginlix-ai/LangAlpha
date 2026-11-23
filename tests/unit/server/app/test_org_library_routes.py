"""The organization library's routes: who reaches them and who may publish.

The library is on for an active member of an organization granted
``org_library``; for anyone else every route answers as an off route (404),
and while the account cannot be read, 503. Publishing follows the
``org_library.publish`` rule the auth service sends per account, and keeps
the link as its owner set it, so a link that is off is refused. A member's
own skill is never replaced by an install. The console's routes take a
service caller and act only on the organization they name.

The rows are the database's business (``tests/integration/test_org_library_db.py``);
here the module is stood in for.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config.models import OrgLibraryPolicy, PolicyLayer
from src.server.app import org_library as route
from src.server.services import org_library as library
from src.server.services.account_contract import Account, Org
from src.server.services.account import Standing
from src.server.utils.api import get_current_user_id

USER = "member-1"
ORG = "org-a"
TOKEN = "service-secret"
THREAD = "11111111-2222-4333-8444-555555555555"
ITEM = "99999999-2222-4333-8444-555555555555"
_AT = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _row(**overrides) -> dict:
    row = {
        "item_id": ITEM,
        "org_id": ORG,
        "kind": "thread",
        "title": "Q3 notes",
        "summary": "",
        "publisher_id": USER,
        "publisher_name": "Mia",
        "thread_id": THREAD,
        "artifact_id": None,
        "published_at": _AT,
        "updated_at": _AT,
        "share_token": "tok123",
        "code": None,
        "path": None,
        "skill_name": None,
        "content_hash": None,
        "installed_hash": None,
        "audience": "organization",
        "listed": True,
    }
    row.update(overrides)
    return row


@pytest.fixture
def state(monkeypatch):
    """The caller's account, the library flag and the publish rule, each set
    to the allowed case unless a test changes it."""
    s = SimpleNamespace(
        account=Account(org=Org(ORG, "Acme")),
        flag=True,
        publish=True,
        public_links=True,
    )

    async def read_standing(user_id, **_):
        if s.account is None:
            return Standing(None, None)
        org = PolicyLayer(
            org_library=OrgLibraryPolicy(publish=s.publish), public_links=s.public_links
        )
        account = replace(s.account, policy=org)
        return Standing(account, account)

    async def feature(user_id, key, account):
        if key == "shared_workspaces":
            return False
        assert key == "org_library"
        return s.flag

    monkeypatch.setattr("src.server.services.membership.teams_door_open", lambda: True)
    monkeypatch.setattr("src.server.services.account.read_standing", read_standing)
    monkeypatch.setattr("src.server.services.account.deployment_policy", lambda: None)
    monkeypatch.setattr("src.server.services.features.feature_enabled_for_account", feature)
    monkeypatch.setattr(route, "feature_enabled_for_account", feature)
    monkeypatch.setattr(library.activity, "record", MagicMock())
    return s


@pytest.fixture
def db(monkeypatch):
    mock = MagicMock()
    mock.PAGE_SIZE = 50
    mock.list_items = AsyncMock(return_value=([_row()], None))
    mock.get_item = AsyncMock(return_value=_row())
    mock.publish = AsyncMock(return_value=(ITEM, True))
    mock.remove = AsyncMock(return_value=_row(archive_key=None))
    mock.own_skill_origin = AsyncMock(return_value=(False, None))
    monkeypatch.setattr(route, "library_db", mock)
    monkeypatch.setattr(library, "library_db", mock)
    return mock


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(route.router)
    app.dependency_overrides[get_current_user_id] = lambda: USER
    with patch("src.server.utils.api._SERVICE_TOKEN", TOKEN):
        yield TestClient(app)


@pytest.fixture
def owns_thread(monkeypatch):
    from src.server.services.shared_workspaces.access import Access

    monkeypatch.setattr(
        "src.server.app.org_library.thread_access",
        AsyncMock(return_value=Access(USER)),
    )

    async def thread(thread_id):
        return {"conversation_thread_id": thread_id, "is_shared": True, "workspace_id": "ws-1"}

    monkeypatch.setattr(library, "get_thread_by_id", AsyncMock(side_effect=thread))


# --- who reaches it ------------------------------------------------------------


@pytest.mark.parametrize(
    ("account", "flag", "status"),
    [
        (Account(org=Org(ORG, "Acme")), True, 200),
        (Account(), True, 404),
        (Account(org=Org(ORG, "Acme", suspended=True)), True, 404),
        (Account(org=Org(ORG, "Acme")), False, 404),
        (None, True, 503),
    ],
    ids=["member", "personal", "suspended", "not-granted", "unreadable"],
)
def test_the_library_is_on_for_an_active_member_granted_it(client, state, db, account, flag, status):
    state.account, state.flag = account, flag
    response = client.get("/api/v1/org-library")
    assert response.status_code == status
    if status == 200:
        body = response.json()
        assert body["can_publish"] is True
        assert [i["url"] for i in body["items"]] == ["/s/tok123"]
        db.list_items.assert_awaited_once()
        assert db.list_items.await_args.args == (ORG,)
    else:
        db.list_items.assert_not_awaited()


@pytest.mark.parametrize("allowed", [True, False])
def test_the_list_leaves_out_public_links_the_organization_stopped(client, state, db, allowed):
    state.public_links = allowed
    assert client.get("/api/v1/org-library").status_code == 200
    assert db.list_items.await_args.kwargs["public_links"] is allowed


def test_a_bad_cursor_is_400(client, state, db):
    db.list_items.side_effect = ValueError("cursor")
    assert client.get("/api/v1/org-library?cursor=zzz").status_code == 400


# --- publishing ------------------------------------------------------------------


def test_a_member_publishes_their_shared_thread(client, state, db, owns_thread):
    response = client.post("/api/v1/org-library", json={"kind": "thread", "target_id": THREAD})
    assert response.status_code == 200
    assert db.publish.await_args.kwargs["org_id"] == ORG
    library.activity.record.assert_called_once()


def test_publishing_is_refused_where_the_rule_says_no(client, state, db, owns_thread):
    state.publish = False
    response = client.post("/api/v1/org-library", json={"kind": "thread", "target_id": THREAD})
    assert response.status_code == 403
    assert response.json()["detail"]["set_by"] == "organization"
    db.publish.assert_not_awaited()
    assert client.get("/api/v1/org-library").json()["can_publish"] is False


def test_a_thread_whose_link_is_off_is_refused_and_its_link_left_alone(client, state, db, owns_thread, monkeypatch):
    monkeypatch.setattr(
        library, "get_thread_by_id",
        AsyncMock(return_value={"conversation_thread_id": THREAD, "is_shared": False}),
    )
    response = client.post("/api/v1/org-library", json={"kind": "thread", "target_id": THREAD})
    assert response.status_code == 409
    assert response.json()["detail"]["type"] == "link_off"
    db.publish.assert_not_awaited()


def _in_shared(monkeypatch, value: str, role: str = "member") -> str:
    from src.server.database.shared_workspaces import SharedRow
    from src.server.services.shared_workspaces.access import Access, SharedAccess

    holder = "shared-00000000-0000-4000-8000-0000000000aa"
    shared = SharedRow("ws-1", holder, ORG, everyone=False, created_by=USER, share_out=value)
    monkeypatch.setattr(
        "src.server.app.org_library.thread_access",
        AsyncMock(return_value=Access(holder, SharedAccess(shared, USER, role))),
    )
    monkeypatch.setattr(
        "src.server.services.shared_workspaces.access.has_active_manager", AsyncMock(return_value=True)
    )
    return holder


@pytest.mark.parametrize(
    ("value", "role", "status", "kind"),
    [
        ("managers", "member", 403, "share_out_managers_only"),
        ("none", "manager", 403, "share_out_none"),
        ("managers", "manager", 200, None),
        ("members", "member", 200, None),
    ],
)
def test_a_shared_workspaces_managers_say_who_publishes_from_it(
    client, state, db, owns_thread, monkeypatch, value, role, status, kind
):
    holder = _in_shared(monkeypatch, value, role)
    response = client.post("/api/v1/org-library", json={"kind": "thread", "target_id": THREAD})
    assert response.status_code == status
    if kind:
        assert response.json()["detail"]["type"] == kind
        db.publish.assert_not_awaited()
    else:
        # The thread is in the holder's workspace, not the publisher's.
        assert db.publish.await_args.kwargs["owner_id"] == holder
        assert db.publish.await_args.kwargs["publisher_id"] == USER


def test_the_organizations_rule_is_asked_before_the_workspaces(client, state, db, owns_thread, monkeypatch):
    _in_shared(monkeypatch, "none", "manager")
    state.publish = False
    response = client.post("/api/v1/org-library", json={"kind": "thread", "target_id": THREAD})
    assert response.json()["detail"]["set_by"] == "organization"


def test_the_lookup_names_the_workspaces_rule_only_when_it_is_why(client, state, db, owns_thread, monkeypatch):
    db.find_item = AsyncMock(return_value=None)
    personal = client.get(f"/api/v1/org-library/published?thread_id={THREAD}").json()
    assert personal == {"item": None, "can_publish": True}
    _in_shared(monkeypatch, "managers", "member")
    held = client.get(f"/api/v1/org-library/published?thread_id={THREAD}").json()
    assert held == {"item": None, "can_publish": False, "workspace_rule": "managers"}


def test_another_members_thread_is_refused_by_the_owner_check(client, state, db, monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setattr(
        "src.server.app.org_library.thread_access",
        AsyncMock(side_effect=HTTPException(status_code=403, detail="Not your thread")),
    )
    response = client.post("/api/v1/org-library", json={"kind": "thread", "target_id": THREAD})
    assert response.status_code == 403
    db.publish.assert_not_awaited()


@pytest.mark.parametrize(
    "body",
    [
        {"kind": "thread"},
        {"kind": "skill", "target_id": THREAD},
        {"kind": "thread", "target_id": THREAD, "skill_name": "dcf"},
    ],
)
def test_a_request_names_the_target_its_kind_takes(client, state, db, body):
    assert client.post("/api/v1/org-library", json=body).status_code == 422


# --- removing and installing ---------------------------------------------------------


def test_a_member_removes_only_their_own_item(client, state, db):
    db.remove.return_value = None
    assert client.delete(f"/api/v1/org-library/{ITEM}").status_code == 404
    assert db.remove.await_args.kwargs == {"publisher_id": USER, "org_id": ORG}


def test_installing_never_replaces_a_skill_of_the_members_own(client, state, db):
    db.get_item.return_value = _row(
        kind="skill", thread_id=None, share_token=None, skill_name="dcf", content_hash="sha256:2"
    )
    db.own_skill_origin.return_value = (True, None)
    response = client.post(f"/api/v1/org-library/{ITEM}/install")
    assert response.status_code == 409
    assert response.json()["detail"] == {
        "type": "name_taken",
        "message": "You already have a skill named dcf.",
        "name": "dcf",
    }


def test_an_item_of_another_organization_is_not_installed(client, state, db):
    db.get_item.return_value = _row(org_id="org-b", kind="skill", skill_name="dcf", content_hash="h")
    assert client.post(f"/api/v1/org-library/{ITEM}/install").status_code == 404


# --- the console -------------------------------------------------------------------


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer a-session"}, {"X-Service-Token": TOKEN, "X-User-Id": "u1"}],
    ids=["nobody", "a-user", "service-for-a-user"],
)
def test_only_a_service_caller_reaches_the_console_routes(headers, db):
    app = FastAPI()
    app.include_router(route.router)
    with patch("src.server.utils.api._SERVICE_TOKEN", TOKEN):
        client = TestClient(app)
        assert client.get(f"/api/v1/org-library/service?org_id={ORG}", headers=headers).status_code == 401
        assert (
            client.delete(f"/api/v1/org-library/service/{ITEM}?org_id={ORG}&actor_id=a", headers=headers).status_code
            == 401
        )
    db.list_items.assert_not_awaited()
    db.remove.assert_not_awaited()


def test_the_console_removes_within_the_organization_it_names_as_its_actor(state, db):
    app = FastAPI()
    app.include_router(route.router)
    service = {"X-Service-Token": TOKEN}
    with patch("src.server.utils.api._SERVICE_TOKEN", TOKEN):
        client = TestClient(app)
        listed = client.get(f"/api/v1/org-library/service?org_id={ORG}", headers=service)
        assert listed.status_code == 200
        assert listed.json()["items"][0]["publisher_id"] == USER
        assert db.list_items.await_args.kwargs["listed_only"] is False

        db.remove.return_value = None
        gone = client.delete(f"/api/v1/org-library/service/{ITEM}?org_id=org-b&actor_id=admin-1", headers=service)
        assert gone.status_code == 404
        assert db.remove.await_args.kwargs == {"publisher_id": None, "org_id": "org-b"}

        db.remove.return_value = _row(archive_key=None)
        done = client.delete(f"/api/v1/org-library/service/{ITEM}?org_id={ORG}&actor_id=admin-1", headers=service)
        assert done.status_code == 204
    actor, action = library.activity.record.call_args.args
    assert (actor, action) == ("admin-1", library.Action.LIBRARY_REMOVED)
    assert library.activity.record.call_args.kwargs["detail"]["publisher_id"] == USER
