"""A shared workspace's team: who it lets in, the roster read, and what
losing a team takes away.

A workspace names at most one team of its organization. Someone reaches it
when listed on it, when it is open to everyone, or when they are in the team
it names; the one rule exists twice, in Python for one workspace and in SQL
for lists, and the two must agree. Whoever stops reaching it loses what they
had running there through one path, however they lost it.
"""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from fastapi import HTTPException

from src.server.database.shared_workspaces import SharedRow, reach_sql, reaches
from src.server.services.account_contract import Team
from src.server.services.shared_workspaces import teams, turns
from src.server.services.shared_workspaces.access import Seat



ORG = "org-n"
TEAM = "team-semis"
ROW = SharedRow("ws-1", "shared-0000", ORG, everyone=False, created_by="g", share_out="managers")


# -- the rule ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("row", "role", "teams_in", "expected"),
    [
        (ROW, "member", (), True),
        (ROW, None, (), False),
        (replace(ROW, everyone=True), None, (), True),
        (replace(ROW, team_id=TEAM), None, (TEAM,), True),
        (replace(ROW, team_id=TEAM), None, ("team-macro",), False),
        (replace(ROW, team_id=TEAM), "manager", (), True),
        (ROW, None, (TEAM,), False),
    ],
    ids=["listed", "neither", "everyone", "in-its-team", "in-another-team", "listed-not-in-team", "names-none"],
)
def test_who_reaches_a_workspace(row, role, teams_in, expected):
    assert reaches(row, role, frozenset(teams_in)) is expected


def test_the_sql_rule_leaves_teams_out_for_someone_in_none():
    # A person in no team reads the clause shared workspaces had before teams.
    assert reach_sql("s", "m.user_id IS NOT NULL", None) == "(m.user_id IS NOT NULL OR s.everyone)"
    assert reach_sql("s", "listed", "%(teams)s") == (
        "(listed OR s.everyone OR s.team_id = ANY(%(teams)s::text[]))"
    )


def test_a_seat_reaches_only_its_own_organizations_workspaces():
    seat = Seat(ORG, (Team(TEAM, "Semis desk", "admin"),))
    named = replace(ROW, team_id=TEAM)
    assert seat.reaches(named, None)
    assert not seat.reaches(replace(named, org_id="org-h"), None)
    access = seat.access(named, "a", None)
    assert (access.role, access.listed, access.team_admin, access.may_manage) == ("member", False, True, True)
    member = Seat(ORG, (Team(TEAM, "Semis desk", "member"),)).access(named, "t", None)
    assert (member.team_admin, member.may_manage) == (False, False)
    assert seat.team_of(ROW) is None


# -- the roster -------------------------------------------------------------


def _answer(status: int, body: object = None) -> MagicMock:
    resp = MagicMock(status_code=status)
    resp.json.return_value = body
    return resp


def _roster_body(**team) -> dict:
    return {
        "team": {"id": TEAM, "name": "Semis desk", "organization_id": ORG, **team},
        "members": [
            {"user_id": "a", "name": "Adam", "email": "a@x.test", "role": "admin", "active": True},
            {"user_id": "t", "name": None, "email": None, "role": "member", "active": False},
        ],
    }


def _asked(resp_or_error):
    client = MagicMock()
    if isinstance(resp_or_error, Exception):
        client.get = AsyncMock(side_effect=resp_or_error)
    else:
        client.get = AsyncMock(return_value=resp_or_error)
    return (
        patch.object(teams, "get_http_client", AsyncMock(return_value=client)),
        patch.object(teams.platform_client, "platform_gating_active", return_value=True),
        patch.object(teams.platform_client, "service_token_missing", return_value=False),
        patch.object(teams.platform_client, "service_headers", return_value={"X-Service-Token": "s"}),
        client,
    )


async def _read(resp_or_error):
    *patches, client = _asked(resp_or_error)
    with patches[0], patches[1], patches[2], patches[3]:
        return await teams.read_roster(TEAM, ORG), client


@pytest.mark.asyncio
async def test_a_roster_reads_the_team_and_its_people():
    roster, client = await _read(_answer(200, _roster_body()))
    assert (roster.id, roster.name, roster.org_id) == (TEAM, "Semis desk", ORG)
    assert [(m.user_id, m.role, m.active) for m in roster.members] == [("a", "admin", True), ("t", "member", False)]
    assert client.get.await_args.kwargs["params"] == {"org_id": ORG}
    assert client.get.await_args.args[0].endswith(f"/api/auth/teams/{TEAM}/roster")


@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (_answer(404), teams.TeamNotFound),
        (_answer(200, _roster_body(id="team-macro")), teams.TeamNotFound),
        (_answer(200, _roster_body(organization_id="org-h")), teams.TeamNotFound),
        (_answer(500), teams.RosterUnavailable),
        (_answer(200, {"team": {}}), teams.RosterUnavailable),
        (_answer(200, {**_roster_body(), "members": [{"user_id": "a", "role": "owner", "active": True}]}), teams.RosterUnavailable),
        (OSError("refused"), teams.RosterUnavailable),
    ],
    ids=["404", "another-team", "another-org", "500", "unreadable", "unknown-role", "unreachable"],
)
@pytest.mark.asyncio
async def test_a_roster_that_is_not_the_one_asked_for_is_none(answer, error):
    with pytest.raises(error):
        await _read(answer)


@pytest.mark.asyncio
async def test_with_no_auth_service_there_is_no_roster():
    with patch.object(teams.platform_client, "platform_gating_active", return_value=False):
        with pytest.raises(teams.RosterUnavailable):
            await teams.read_roster(TEAM, ORG)


@pytest.mark.parametrize(
    ("mine", "read", "status", "kind"),
    [
        ((), None, 403, teams.TEAM_NOT_YOURS),
        ((Team(TEAM, "Semis desk", "member"),), teams.TeamNotFound(TEAM), 400, teams.TEAM_NOT_FOUND),
        ((Team(TEAM, "Semis desk", "member"),), teams.RosterUnavailable("x"), 503, None),
    ],
    ids=["not-in-it", "gone", "unanswered"],
)
@pytest.mark.asyncio
async def test_only_a_team_you_are_in_and_that_exists_may_be_named(mine, read, status, kind):
    reader = AsyncMock(side_effect=read)
    with patch.object(teams, "read_roster", reader), pytest.raises(HTTPException) as e:
        await teams.require_nameable(mine, TEAM, ORG)
    assert e.value.status_code == status
    if kind:
        assert e.value.detail["type"] == kind
    if not mine:
        reader.assert_not_awaited()


# -- losing it --------------------------------------------------------------


@pytest.mark.parametrize(
    ("reach", "row", "listed", "expected"),
    [
        ("not", replace(ROW, team_id=TEAM), None, True),
        ("member", replace(ROW, team_id=TEAM), None, False),
        ("unknown", replace(ROW, team_id=TEAM), None, True),
        ("unknown", replace(ROW, team_id=TEAM), "member", False),
        ("unknown", replace(ROW, everyone=True), None, False),
    ],
    ids=["not", "member", "unknown-team-only", "unknown-listed", "unknown-everyone"],
)
@pytest.mark.asyncio
async def test_whether_someone_lost_a_workspace(reach, row, listed, expected):
    with (
        patch.object(turns, "verdict", AsyncMock(return_value=reach)),
        patch.object(turns, "shared_access", MagicMock()),
        patch.object(turns.db, "get_member_role", AsyncMock(return_value=listed)),
    ):
        assert await turns.lost(row, "t") is expected


@pytest.mark.asyncio
async def test_a_failed_read_loses_no_one():
    with (
        patch.object(turns, "verdict", AsyncMock(return_value="unknown")),
        patch.object(turns, "shared_access", MagicMock()),
        patch.object(turns.db, "get_member_role", AsyncMock(side_effect=OSError)),
    ):
        assert await turns.lost(replace(ROW, team_id=TEAM), "t") is False


@pytest.mark.asyncio
async def test_closing_to_everyone_or_changing_the_team_ends_only_who_lost_it():
    end = AsyncMock()
    actors = AsyncMock(return_value=["t", "l"])
    with patch.object(turns, "end_access", end), patch.object(turns.db, "unlisted_actors", actors):
        await turns.end_access_of_unreached(replace(ROW, everyone=True))
        end.assert_not_awaited()
        actors.assert_not_awaited()

        # No team: only the listed reach it, as before teams.
        await turns.end_access_of_unreached(ROW)
        assert end.await_args_list == [call(ROW.workspace_id, ROW.holder_id, None)]
        actors.assert_not_awaited()

        end.reset_mock()
        named = replace(ROW, team_id=TEAM)
        with patch.object(turns, "lost", AsyncMock(side_effect=lambda row, user: user == "t")):
            await turns.end_access_of_unreached(named)
        assert end.await_args_list == [call(ROW.workspace_id, ROW.holder_id, "t")]


@pytest.mark.asyncio
async def test_end_access_is_one_path_for_turns_tokens_and_previews():
    with (
        patch.object(turns, "stop_turns_of_former_members", AsyncMock(side_effect=OSError)) as stop,
        patch.object(turns, "revoke_former_members_tokens", AsyncMock()) as tokens,
        patch.object(turns.workspace_db, "get_workspace", AsyncMock(return_value={"sandbox_id": "sb"})),
        patch.object(turns.preview_capabilities, "revoke", AsyncMock()) as previews,
    ):
        await turns.end_access("ws-1", "shared-0000", "t")
    # One step failing leaves the others to run.
    stop.assert_awaited_once_with("ws-1", "t")
    tokens.assert_awaited_once_with("ws-1", "shared-0000", "t")
    previews.assert_awaited_once_with("sb")


@pytest.mark.asyncio
async def test_someone_out_of_the_team_loses_what_they_run_there():
    named = replace(ROW, team_id=TEAM)
    standing = MagicMock(suspended=False)
    with (
        patch.object(turns, "running_work", AsyncMock(return_value=([{"workspace_id": "ws-1"}], []))),
        patch.object(turns, "shared_computers_minting_as", AsyncMock(return_value=[("ws-2", "shared-2", "c2")])),
        patch.object(turns.db, "workspaces_running_commands_of", AsyncMock(return_value=["ws-3"])),
        patch.object(turns.account, "read_standing", AsyncMock(return_value=standing)),
        patch.object(turns.db, "get_shared_workspace", AsyncMock(side_effect=lambda ws: replace(named, workspace_id=ws))),
        patch.object(turns, "lost", AsyncMock(side_effect=lambda row, user: row.workspace_id != "ws-2")),
        patch.object(turns, "end_access", AsyncMock()) as end,
    ):
        await turns.stop_shared_work_if_gone("t")
    assert end.await_args_list == [
        call("ws-1", ROW.holder_id, "t"),
        call("ws-3", ROW.holder_id, "t"),
    ]


@pytest.mark.asyncio
async def test_the_notice_tells_their_open_feeds():
    from src.server.services import account

    with (
        patch.object(account.membership, "forget_membership", AsyncMock(return_value=1)),
        patch.object(account, "is_feature_enabled_system", return_value=True),
        patch.object(account.mcp_config_version, "bump_user_workspaces_mcp_version", AsyncMock()),
        patch.object(account.membership, "teams_door_open", return_value=True),
        patch.object(account.account_suspension, "stop_if_suspended_soon", MagicMock()),
        patch("src.server.services.shared_workspaces.turns.stop_shared_work_if_gone_soon") as stop,
        patch(
            "src.server.services.shared_workspaces.feed.publish_user_event", AsyncMock()
        ) as published,
    ):
        await account.on_membership_changed("t", None)
    stop.assert_called_once_with("t")
    user, event = published.await_args.args
    assert user == "t" and event["type"] == "shared_reach"


@pytest.mark.parametrize(("lost", "ended"), [(False, False), (True, True)], ids=["still-reaches", "lost"])
@pytest.mark.asyncio
async def test_removing_a_listed_person_ends_only_access_they_lost(lost, ended):
    # Someone also in its team, or in a workspace open to everyone, keeps
    # their turns, commands, token and previews.
    from src.server.services.shared_workspaces import service
    from src.server.services.shared_workspaces.access import SharedAccess

    named = replace(ROW, team_id=TEAM)
    shared = SharedAccess(named, "g", "manager")
    with (
        patch.object(service.db, "get_member_role", AsyncMock(return_value="member")),
        patch.object(service.db, "remove_member", AsyncMock(return_value="member")) as removed,
        patch("src.server.services.activity.record", MagicMock()),
        patch.object(service, "publish_members_changed", AsyncMock()),
        patch.object(service, "lost", AsyncMock(return_value=lost)) as asked,
        patch.object(service, "end_access", AsyncMock()) as end,
    ):
        await service.remove_member(shared, "t")
    removed.assert_awaited_once_with(named.workspace_id, "t")
    asked.assert_awaited_once_with(named, "t")
    assert end.await_args_list == ([call(named.workspace_id, named.holder_id, "t")] if ended else [])
