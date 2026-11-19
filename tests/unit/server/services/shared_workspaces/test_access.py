"""Who may use a workspace or a thread: the access matrix.

A personal workspace answers as the owner check always has, reading nothing
about the account. A shared one (owned by a holder) answers for an active
member of its organization with the feature on who is listed, in the team
it names, or anyone in it while it is open to everyone, read against the
caller's current account. One the caller does not reach answers as no
workspace at all.
"""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.shared_workspaces import SharedRow
from src.server.services.account_contract import Account, Org, Team
from src.server.services.account import Standing
from src.server.services.shared_workspaces import access as access_mod
from src.server.services.shared_workspaces.access import (
    MANAGER_ONLY,
    thread_access,
    workspace_access,
)

pytestmark = pytest.mark.asyncio

ORG = "org-n"
HOLDER = "shared-0000"
WS = "ws-1"
SHARED_ROW = SharedRow(WS, HOLDER, ORG, everyone=False, created_by="g", share_out="managers")
TEAM = "team-semis"
TEAM_ROW = replace(SHARED_ROW, team_id=TEAM)


def _account(
    org_id: str | None = ORG, *, suspended: bool = False, teams: tuple[Team, ...] = ()
) -> Account:
    if not org_id:
        return Account()
    return Account(org=Org(id=org_id, name="N", suspended=suspended), teams=teams)


def _in(team: str = TEAM, role: str = "member") -> Account:
    return _account(teams=(Team(id=team, name="Semis desk", role=role),))


class _World:
    """The account, the flags and the rows one case reads."""

    def __init__(
        self,
        *,
        account: Account | None = None,
        feature: bool = True,
        door: bool = True,
        shared: SharedRow | None | str = "default",
        roles: dict[str, str] | None = None,
        active: dict[str, bool] | None = None,
    ) -> None:
        self.account = _account() if account is None else account
        self.feature = feature
        self.door = door
        self.shared = SHARED_ROW if shared == "default" else shared
        self.roles = roles or {}
        self.active = active or {}
        self.standing = AsyncMock(side_effect=lambda uid, **_: Standing(self.account, self.account))

    def __enter__(self):
        self._stack = ExitStack()
        p = self._stack.enter_context
        p(patch("src.server.services.membership.teams_door_open", return_value=self.door))
        p(patch("src.server.services.account.read_standing", self.standing))
        p(patch(
            "src.server.services.features.feature_enabled_for_account",
            AsyncMock(return_value=self.feature),
        ))
        p(patch(
            "src.server.database.shared_workspaces.get_shared_workspace",
            AsyncMock(return_value=self.shared),
        ))
        p(patch(
            "src.server.database.shared_workspaces.get_member_role",
            AsyncMock(side_effect=lambda ws, uid: self.roles.get(uid)),
        ))
        p(patch(
            "src.server.database.shared_workspaces.manager_ids",
            AsyncMock(return_value=[u for u, r in self.roles.items() if r == "manager"]),
        ))
        p(patch(
            "src.server.services.account.active_in",
            AsyncMock(side_effect=lambda uid, org: self.active.get(uid, True)),
        ))
        return self

    def __exit__(self, *exc):
        self._stack.close()


def _shared_ws() -> dict:
    return {"workspace_id": WS, "user_id": HOLDER}


async def _status(coro) -> int | str:
    try:
        access = await coro
    except HTTPException as e:
        detail = e.detail
        if isinstance(detail, dict) and detail.get("type") == MANAGER_ONLY:
            return "manager_only"
        return e.status_code
    if access.shared is None:
        return "owner"
    shared = access.shared
    return (
        shared.role
        + ("" if shared.listed else "+unlisted")
        + ("+team_admin" if shared.team_admin else "")
        + ("+acting" if shared.acting_manager else "")
    )


async def test_a_personal_workspace_is_its_owners_and_reads_no_account():
    with _World() as w:
        access = await workspace_access({"workspace_id": WS, "user_id": "alice"}, "alice")
        assert (access.owner_id, access.shared) == ("alice", None)
        assert await _status(workspace_access({"workspace_id": WS, "user_id": "alice"}, "bob")) == 403
    w.standing.assert_not_awaited()


async def test_no_row_is_404():
    with _World():
        assert await _status(workspace_access(None, "alice")) == 404


@pytest.mark.parametrize(
    ("world", "caller", "manage", "expected"),
    [
        pytest.param({"roles": {"m": "member"}}, "m", False, "member", id="listed-member"),
        pytest.param({"roles": {"g": "manager"}}, "g", True, "manager", id="manager-manages"),
        pytest.param({"roles": {"m": "member", "g": "manager"}}, "m", True, "manager_only", id="member-may-not-manage"),
        pytest.param(
            {"roles": {"m": "member", "g": "manager"}, "active": {"g": False}},
            "m", True, "member+acting", id="no-active-manager",
        ),
        pytest.param({"roles": {}}, "s", False, 404, id="same-org-not-listed"),
        pytest.param(
            {"roles": {}, "shared": replace(SHARED_ROW, everyone=True)},
            "s", False, "member+unlisted", id="open-to-everyone",
        ),
        pytest.param({"shared": TEAM_ROW, "account": _in()}, "t", False, "member+unlisted", id="in-its-team"),
        pytest.param({"shared": TEAM_ROW, "account": _in("team-macro")}, "t", False, 404, id="in-another-team"),
        pytest.param({"shared": SHARED_ROW, "account": _in()}, "t", False, 404, id="names-no-team"),
        pytest.param(
            {"shared": TEAM_ROW, "account": _in(), "roles": {"t": "member"}},
            "t", False, "member", id="listed-and-in-its-team",
        ),
        pytest.param(
            {"shared": TEAM_ROW, "account": _in(), "roles": {"g": "manager"}},
            "t", True, "manager_only", id="team-member-may-not-manage",
        ),
        pytest.param(
            {"shared": TEAM_ROW, "account": _in(role="admin"), "roles": {"g": "manager"}},
            "a", True, "member+unlisted+team_admin", id="team-admin-manages-unlisted",
        ),
        pytest.param(
            {"shared": TEAM_ROW, "account": _in(role="admin"), "roles": {"a": "member", "g": "manager"}},
            "a", True, "member+team_admin", id="team-admin-listed-as-member-manages",
        ),
        pytest.param(
            {"shared": replace(TEAM_ROW, org_id="org-h"), "account": _in()},
            "t", False, 404, id="its-team-in-another-org",
        ),
        pytest.param({"roles": {"m": "member"}, "account": _account("org-h")}, "m", False, 404, id="other-org"),
        pytest.param({"roles": {"m": "member"}, "account": Account()}, "m", False, 404, id="no-org"),
        pytest.param({"roles": {"m": "member"}, "account": _account(suspended=True)}, "m", False, 403, id="suspended"),
        pytest.param({"roles": {"m": "member"}, "feature": False}, "m", False, 404, id="feature-off"),
        pytest.param({"roles": {"m": "member"}, "door": False}, "m", False, 404, id="teams-off"),
        pytest.param(
            {"roles": {"m": "member"}, "shared": replace(SHARED_ROW, holder_id="shared-other")},
            "m", False, 404, id="row-not-this-holders",
        ),
        pytest.param({"roles": {"m": "member"}, "shared": None}, "m", False, 404, id="not-shared-row"),
    ],
)
async def test_the_shared_access_matrix(world, caller, manage, expected):
    with _World(**world):
        result = await _status(workspace_access(_shared_ws(), caller, manage=manage))
    assert result == expected


async def test_an_unknown_account_is_503_never_a_guess():
    with _World(roles={"m": "member"}) as w:
        w.account = None
        w.standing.side_effect = lambda uid, **_: Standing(None, None)
        assert await _status(workspace_access(_shared_ws(), "m")) == 503


async def test_the_teams_door_shut_reads_no_account():
    with _World(roles={"m": "member"}, door=False) as w:
        assert await _status(workspace_access(_shared_ws(), "m")) == 404
    w.standing.assert_not_awaited()


async def test_a_shared_access_names_the_holder_as_owner():
    with _World(roles={"m": "member"}):
        access = await workspace_access(_shared_ws(), "m")
    assert access.owner_id == HOLDER and access.is_shared


def _reach(*reachers: str):
    """The thread's half of the rule: who reaches the thread itself."""
    return patch(
        "src.server.database.conversation.thread_reach.hidden",
        AsyncMock(side_effect=lambda ids, user_id, **_: set() if user_id in reachers else set(ids)),
    )


async def test_thread_access_follows_its_workspace():
    meta = {"user_id": HOLDER, "workspace_id": WS}
    with _World(roles={"m": "member"}), _reach("m", "x"):
        assert (await thread_access("t", "m", meta=meta)).owner_id == HOLDER
        # Not in the workspace: answered as no thread, whatever the thread is.
        assert await _status(thread_access("t", "x", meta=meta)) == 404
    with patch(
        "src.server.database.conversation.get_thread_auth_meta", AsyncMock(return_value=None)
    ):
        assert await _status(thread_access("t", "m")) == 404
    personal = {"user_id": "alice", "workspace_id": WS}
    assert (await thread_access("t", "alice", meta=personal)).shared is None
    assert await _status(thread_access("t", "bob", meta=personal)) == 403


async def _refusal(coro) -> tuple[int, object]:
    with pytest.raises(HTTPException) as e:
        await coro
    return e.value.status_code, e.value.detail


async def test_a_thread_someone_else_keeps_private_answers_as_no_thread():
    """The same status and body as an id no thread has, so an id says nothing."""
    meta = {"user_id": HOLDER, "workspace_id": WS}
    with _World(roles={"m": "member"}), _reach():
        private = await _refusal(thread_access("t", "m", meta=meta))
    with patch(
        "src.server.database.conversation.get_thread_auth_meta", AsyncMock(return_value=None)
    ):
        missing = await _refusal(thread_access("t", "m"))
    assert private == missing == (404, "Thread not found")


@pytest.mark.parametrize("manage", [False, True])
async def test_a_manager_reaches_no_further_into_a_thread(manage):
    meta = {"user_id": HOLDER, "workspace_id": WS}
    with _World(roles={"boss": "manager"}), _reach():
        assert await _status(thread_access("t", "boss", meta=meta, manage=manage)) == 404
    with _World(roles={"boss": "manager"}), _reach("boss"):
        assert await _status(thread_access("t", "boss", meta=meta, manage=manage)) == "manager"


async def test_a_member_managing_a_thread_they_reach_is_still_refused_as_before():
    meta = {"user_id": HOLDER, "workspace_id": WS}
    with _World(roles={"m": "member", "boss": "manager"}), _reach("m"):
        assert await _status(thread_access("t", "m", meta=meta, manage=True)) == "manager_only"


async def test_a_personal_thread_never_asks_who_reaches_it():
    reaches = AsyncMock()
    with patch("src.server.database.conversation.thread_reach.hidden", reaches):
        personal = {"user_id": "alice", "workspace_id": WS}
        assert (await thread_access("t", "alice", meta=personal)).shared is None
        assert await _status(thread_access("t", "bob", meta=personal)) == 403
    reaches.assert_not_called()


@pytest.mark.parametrize(
    ("world", "status", "expected"),
    [
        pytest.param({"roles": {"m": "member"}}, "running", "active", id="listed-reaches-it"),
        pytest.param({"roles": {"m": "member"}}, "deleted", "deleted", id="listed-when-deleted"),
        pytest.param(
            {"roles": {}, "shared": replace(SHARED_ROW, everyone=True)},
            "deleted", "deleted", id="everyone-when-deleted",
        ),
        pytest.param({"roles": {}}, "deleted", "gone", id="removed-before-deleted"),
        pytest.param({"roles": {}}, "running", "gone", id="not-in-it"),
        pytest.param({"roles": {"m": "member"}, "account": _account("org-h")}, "deleted", "gone", id="other-org"),
        pytest.param({"roles": {"m": "member"}, "account": Account()}, "deleted", "gone", id="no-org"),
        pytest.param({"roles": {"m": "member"}, "door": False}, "deleted", "gone", id="teams-off"),
        pytest.param({"roles": {"m": "member"}, "shared": None}, "deleted", "gone", id="not-shared"),
        pytest.param({"roles": {"m": "member"}}, None, "gone", id="no-workspace-row"),
    ],
)
async def test_standing_says_deleted_only_to_someone_still_in_it(world, status, expected):
    """A page that lost a shared workspace while away learns it was deleted
    only when its user was still in it; anyone else is told what a workspace
    they never reached would say."""
    with _World(**world), patch(
        "src.server.database.workspace.workspace_status", AsyncMock(return_value=status)
    ):
        assert await access_mod.standing(WS, "m") == expected


async def test_standing_refuses_a_suspended_member_as_every_route_does():
    from src.server.services.account import AccountSuspended

    with _World(roles={"m": "member"}, account=_account(suspended=True)), patch(
        "src.server.database.workspace.workspace_status", AsyncMock(return_value="deleted")
    ):
        with pytest.raises(AccountSuspended):
            await access_mod.standing(WS, "m")
