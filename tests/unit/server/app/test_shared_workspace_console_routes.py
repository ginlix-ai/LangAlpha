"""The console's routes over an organization's shared workspaces.

Only a service caller with no user reaches them; each acts on the
organization the caller names and answers 404 for another's workspace. A
manager set from the console is either promoted or, when not listed, added
once they are an active member of that organization; the activity log
records the admin as the actor and marks the act as the console's.

The rows are the database's business; here the module is stood in for.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.server.app import shared_workspaces as route
from src.server.database.shared_workspaces import SharedRow
from src.server.services.account_contract import Org
from src.server.services.shared_workspaces import service

ORG = "org-a"
TOKEN = "service-secret"
WS = "11111111-2222-4333-8444-555555555555"
_AT = datetime(2026, 11, 1, tzinfo=timezone.utc)
SERVICE = {"X-Service-Token": TOKEN}


def _row(**overrides) -> dict:
    row = {
        "workspace_id": WS,
        "holder_id": "shared-x",
        "org_id": ORG,
        "everyone": False,
        "created_by": "ana",
        "share_out": "managers",
        "created_at": _AT,
        "name": "Q4 earnings",
        "description": None,
        "created_by_name": "Ana",
        "created_by_email": "ana@example.com",
        "computer_status": "stopped",
        "turn_running": True,
        "last_activity_at": _AT,
        "members": [
            {"user_id": "ana", "role": "manager", "added_at": _AT, "name": "Ana", "email": "ana@example.com"},
            {"user_id": "sam", "role": "member", "added_at": _AT, "name": "Sam", "email": "sam@example.com"},
        ],
    }
    row.update(overrides)
    return row


@pytest.fixture
def db(monkeypatch):
    mock = MagicMock()
    mock.list_for_org = AsyncMock(return_value=([_row()], "next"))
    mock.get_live_in_org = AsyncMock(return_value=_row())
    mock.get_member_role = AsyncMock(return_value="member")
    mock.make_manager = AsyncMock(return_value="member")
    mock.names_of = AsyncMock(return_value={"lee": "Lee"})
    monkeypatch.setattr(service, "db", mock)
    monkeypatch.setattr(service, "users_db", mock)
    monkeypatch.setattr("src.server.database.shared_workspaces_console.list_for_org", mock.list_for_org)
    return mock


@pytest.fixture
def recorded(monkeypatch):
    record = MagicMock()
    monkeypatch.setattr("src.server.services.activity.record", record)
    return record


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(route.router)
    with patch("src.server.utils.api._SERVICE_TOKEN", TOKEN):
        yield TestClient(app)


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer a-session"}, {"X-Service-Token": TOKEN, "X-User-Id": "u1"}],
    ids=["nobody", "a-user", "service-for-a-user"],
)
def test_only_a_service_caller_reaches_the_console_routes(client, db, headers):
    assert client.get(f"/api/v1/shared-workspaces/service?org_id={ORG}", headers=headers).status_code == 401
    assert client.get(f"/api/v1/shared-workspaces/service/{WS}?org_id={ORG}", headers=headers).status_code == 401
    put = f"/api/v1/shared-workspaces/service/{WS}/managers/sam?org_id={ORG}&actor_id=admin"
    assert client.put(put, headers=headers).status_code == 401
    delete = f"/api/v1/shared-workspaces/service/{WS}?org_id={ORG}&actor_id=admin"
    assert client.delete(delete, headers=headers).status_code == 401
    db.list_for_org.assert_not_awaited()
    db.make_manager.assert_not_awaited()


def test_the_list_names_members_managers_and_state(client, db):
    got = client.get(f"/api/v1/shared-workspaces/service?org_id={ORG}&limit=10&cursor=c1", headers=SERVICE)
    assert got.status_code == 200
    body = got.json()
    assert body["next_cursor"] == "next"
    (ws,) = body["workspaces"]
    assert ws["workspace_id"] == WS and ws["name"] == "Q4 earnings"
    assert ws["created_by"] == "ana" and ws["created_by_name"] == "Ana"
    assert ws["managers"] == ["ana"]
    assert [m["user_id"] for m in ws["members"]] == ["ana", "sam"]
    assert ws["computer_status"] == "stopped" and ws["everyone"] is False
    assert ws["turn_running"] is True
    assert db.list_for_org.await_args.args == (ORG,)
    assert db.list_for_org.await_args.kwargs == {"cursor": "c1", "limit": 10}


def test_one_workspace_reads_as_its_list_entry(client, db):
    listed = client.get(f"/api/v1/shared-workspaces/service?org_id={ORG}", headers=SERVICE).json()
    got = client.get(f"/api/v1/shared-workspaces/service/{WS}?org_id={ORG}", headers=SERVICE)
    assert got.status_code == 200
    assert got.json() == listed["workspaces"][0]
    assert db.list_for_org.await_args.args == (ORG,)
    assert db.list_for_org.await_args.kwargs == {"workspace_id": WS}


def test_one_workspace_not_in_the_organization_is_404(client, db):
    db.list_for_org.return_value = ([], None)
    got = client.get(f"/api/v1/shared-workspaces/service/{WS}?org_id=org-b", headers=SERVICE)
    assert got.status_code == 404
    assert db.list_for_org.await_args.args == ("org-b",)
    assert client.get(f"/api/v1/shared-workspaces/service/{WS}", headers=SERVICE).status_code == 422


def test_a_bad_cursor_is_400(client, db):
    db.list_for_org.side_effect = ValueError("malformed cursor")
    assert client.get(f"/api/v1/shared-workspaces/service?org_id={ORG}&cursor=x", headers=SERVICE).status_code == 400


def test_another_organizations_workspace_is_404(client, db, recorded):
    db.get_live_in_org.return_value = None
    put = f"/api/v1/shared-workspaces/service/{WS}/managers/sam?org_id=org-b&actor_id=admin"
    assert client.put(put, headers=SERVICE).status_code == 404
    delete = f"/api/v1/shared-workspaces/service/{WS}?org_id=org-b&actor_id=admin"
    with patch.object(service, "delete", AsyncMock()) as deleted:
        assert client.delete(delete, headers=SERVICE).status_code == 404
    deleted.assert_not_awaited()
    db.make_manager.assert_not_awaited()
    assert db.get_live_in_org.await_args.args == (WS, "org-b")
    recorded.assert_not_called()


def test_a_listed_member_is_promoted_as_the_admins_act(client, db, recorded):
    put = f"/api/v1/shared-workspaces/service/{WS}/managers/sam?org_id={ORG}&actor_id=admin"
    got = client.put(put, headers=SERVICE)
    assert got.status_code == 200 and got.json()["workspace_id"] == WS
    db.make_manager.assert_awaited_once_with(WS, "sam", "admin")
    actor, action = recorded.call_args.args
    assert (actor, action) == ("admin", service.Action.SHARED_WORKSPACE_ROLE_CHANGED)
    assert recorded.call_args.kwargs["detail"] == {
        "member_id": "sam", "role": "manager", "was": "member", "from_console": True,
    }


def test_a_manager_already_is_left_alone(client, db, recorded):
    db.get_member_role.return_value = "manager"
    put = f"/api/v1/shared-workspaces/service/{WS}/managers/ana?org_id={ORG}&actor_id=admin"
    assert client.put(put, headers=SERVICE).status_code == 200
    db.make_manager.assert_not_awaited()
    recorded.assert_not_called()


@pytest.mark.parametrize(
    ("org", "status"),
    [(Org(ORG, "Acme"), 200), (Org("org-b", "Other"), 400), (Org(ORG, "Acme", suspended=True), 400), (None, 400)],
    ids=["active-member", "other-org", "suspended", "no-org"],
)
def test_someone_not_listed_is_added_only_when_an_active_member(client, db, recorded, org, status):
    db.get_member_role.return_value = None
    db.make_manager.return_value = None
    with patch("src.server.services.account.member_of", AsyncMock(return_value=org)):
        put = f"/api/v1/shared-workspaces/service/{WS}/managers/lee?org_id={ORG}&actor_id=admin"
        got = client.put(put, headers=SERVICE)
    assert got.status_code == status
    if status == 200:
        assert recorded.call_args.args == ("admin", service.Action.SHARED_WORKSPACE_MEMBER_ADDED)
        assert recorded.call_args.kwargs["detail"]["from_console"] is True
    else:
        assert got.json()["detail"]["type"] == "member_not_found"
        db.make_manager.assert_not_awaited()


def test_a_holder_or_unknown_id_is_never_added(client, db):
    db.get_member_role.return_value = None
    db.names_of.return_value = {}
    with patch("src.server.services.account.member_of", AsyncMock()) as member_of:
        for who in ("shared-abc", "nobody"):
            put = f"/api/v1/shared-workspaces/service/{WS}/managers/{who}?org_id={ORG}&actor_id=admin"
            assert client.put(put, headers=SERVICE).status_code == 400
    member_of.assert_not_awaited()
    db.make_manager.assert_not_awaited()


def test_the_console_deletes_as_the_admin(client, db, recorded):
    manager = MagicMock()
    manager.remove_computer_with_workspaces = AsyncMock()
    workspace = {"workspace_id": WS, "computer_id": "c-1"}
    db.get_shared_workspace = AsyncMock(
        return_value=SharedRow(WS, "shared-1", ORG, everyone=False, created_by="ana", share_out="managers")
    )
    published = AsyncMock()
    with (
        patch("src.server.services.workspace_manager.WorkspaceManager.get_instance", return_value=manager),
        patch("src.server.database.workspace.get_workspace", AsyncMock(return_value=workspace)),
        patch.object(service, "publish_workspace_changed", published),
    ):
        got = client.delete(f"/api/v1/shared-workspaces/service/{WS}?org_id={ORG}&actor_id=admin", headers=SERVICE)
    assert got.status_code == 204
    # Its members are told it was deleted, on the organization's channel.
    published.assert_awaited_once_with(ORG, WS, "deleted")
    manager.remove_computer_with_workspaces.assert_awaited_once_with("c-1", [WS], delete_files=False)
    assert recorded.call_args.args == ("admin", service.Action.SHARED_WORKSPACE_DELETED)
    assert recorded.call_args.kwargs["detail"] == {"from_console": True}


@pytest.mark.parametrize(
    "moving, working, sentence",
    [
        (False, True, "This workspace has active work. Stop it or wait for it to finish before deleting."),
        (True, False, "This workspace is moving to another computer. Delete it once the move finishes."),
        (False, False, "This computer is busy. Try again in a moment."),
    ],
)
def test_a_delete_its_computer_refuses_is_409_and_deletes_nothing(client, db, recorded, moving, working, sentence):
    from src.server.services.computer_errors import ComputerRemovalRefused, Retirement

    manager = MagicMock()
    manager.remove_computer_with_workspaces = AsyncMock(side_effect=ComputerRemovalRefused(Retirement.BUSY))
    db.get_shared_workspace = AsyncMock(
        return_value=SharedRow(WS, "shared-1", ORG, everyone=False, created_by="ana", share_out="managers")
    )
    published = AsyncMock()
    with (
        patch("src.server.services.workspace_manager.WorkspaceManager.get_instance", return_value=manager),
        patch("src.server.database.workspace.get_workspace", AsyncMock(return_value={"workspace_id": WS, "computer_id": "c-1"})),
        patch("src.server.database.workspace.workspace_is_moving", AsyncMock(return_value=moving)),
        patch("src.server.database.workspace.workspace_has_active_work", AsyncMock(return_value=working)),
        patch.object(service, "publish_workspace_changed", published),
    ):
        got = client.delete(f"/api/v1/shared-workspaces/service/{WS}?org_id={ORG}&actor_id=admin", headers=SERVICE)
    assert got.status_code == 409
    assert got.json()["detail"] == sentence
    published.assert_not_awaited()
    recorded.assert_not_called()


def test_the_team_count_is_a_service_callers_and_scoped_to_the_organization(client, db):
    db.count_naming_team = AsyncMock(return_value=2)
    url = f"/api/v1/shared-workspaces/service/teams/team-1?org_id={ORG}"
    for headers in ({}, {"Authorization": "Bearer a-session"}, {"X-Service-Token": TOKEN, "X-User-Id": "u1"}):
        assert client.get(url, headers=headers).status_code == 401
    db.count_naming_team.assert_not_awaited()
    got = client.get(url, headers=SERVICE)
    assert got.status_code == 200 and got.json() == {"workspaces": 2}
    db.count_naming_team.assert_awaited_once_with(ORG, "team-1")
    assert client.get("/api/v1/shared-workspaces/service/teams/team-1", headers=SERVICE).status_code == 422


def test_a_console_entry_names_its_team(client, db):
    db.list_for_org.return_value = ([_row(team_id="team-1")], None)
    (ws,) = client.get(f"/api/v1/shared-workspaces/service?org_id={ORG}", headers=SERVICE).json()["workspaces"]
    assert ws["team_id"] == "team-1"
    db.list_for_org.return_value = ([_row()], None)
    (ws,) = client.get(f"/api/v1/shared-workspaces/service?org_id={ORG}", headers=SERVICE).json()["workspaces"]
    assert ws["team_id"] is None
