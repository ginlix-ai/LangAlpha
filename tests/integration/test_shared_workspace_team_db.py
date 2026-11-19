"""A shared workspace's team against real PostgreSQL.

The rule for who reaches a workspace lives in Python for one workspace and
in SQL for lists; across every mix of listed, open to everyone and the team
named, the list returns exactly the workspaces the Python rule says. Naming
a team closes the workspace to everyone and opening it clears the team, and
the count of workspaces naming a team leaves out deleted ones and other
organizations.
"""

from __future__ import annotations

import itertools
import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ORG = "org-team-db"
CALLER = "sw-team-caller"
CREATOR = "sw-team-creator"
TEAM, OTHER = "team-semis", "team-macro"


async def _workspace(cur, *, listed: bool, everyone: bool, team: str | None, org: str = ORG) -> str:
    ws = uuid.uuid4()
    holder = f"shared-{ws}"
    await cur.execute("INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,))
    await cur.execute(
        "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, %s, 'running')",
        (ws, holder, str(ws)[:8]),
    )
    await cur.execute(
        "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, everyone, everyone_since,"
        " created_by, team_id, team_since)"
        " VALUES (%s, %s, %s, %s, CASE WHEN %s THEN now() END, %s, %s, CASE WHEN %s::text IS NOT NULL THEN now() END)",
        (ws, holder, org, everyone, everyone, CREATOR, team, team),
    )
    await cur.execute(
        "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by) VALUES (%s, %s, 'manager', %s)",
        (ws, CREATOR, CREATOR),
    )
    if listed:
        await cur.execute(
            "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by) VALUES (%s, %s, 'member', %s)",
            (ws, CALLER, CREATOR),
        )
    return str(ws)


async def _seed_users(cur) -> None:
    for uid in (CALLER, CREATOR):
        await cur.execute("INSERT INTO users (user_id, name) VALUES (%s, %s)", (uid, uid))


async def test_the_list_returns_exactly_what_the_rule_says(patched_get_db_connection, cleanup_tables):
    from src.server.database import shared_workspaces as db

    mixes = [
        (listed, everyone, team)
        for listed, everyone, team in itertools.product((False, True), (False, True), (None, TEAM, OTHER))
        if not (everyone and team)
    ]
    async with patched_get_db_connection() as conn, conn.cursor() as cur:
        await _seed_users(cur)
        made = {mix: await _workspace(cur, listed=mix[0], everyone=mix[1], team=mix[2]) for mix in mixes}
        # Another organization's workspace naming the same team reaches no one here.
        await _workspace(cur, listed=True, everyone=True, team=None, org="org-elsewhere")

    for teams_in in ((), (TEAM,), (TEAM, OTHER)):
        rows = await db.list_for_member(CALLER, ORG, frozenset(teams_in))
        got = {row.workspace_id: role for row, role in rows}
        want = {
            ws for (listed, _, _), ws in made.items()
            if db.reaches(await db.get_shared_workspace(ws), "member" if listed else None, frozenset(teams_in))
        }
        assert set(got) == want, teams_in
        assert {ws for ws, role in got.items() if role == "member"} == {
            ws for (listed, _, _), ws in made.items() if listed
        }
        with_unread = await db.list_with_unread(CALLER, ORG, frozenset(teams_in))
        assert {row.workspace_id for row, _, _ in with_unread} == want


async def test_naming_a_team_and_opening_to_everyone_exclude_each_other(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database import shared_workspaces as db

    async with patched_get_db_connection() as conn, conn.cursor() as cur:
        await _seed_users(cur)
        ws = await _workspace(cur, listed=False, everyone=True, team=None)

    A = db.Audience

    async def team_since():
        async with patched_get_db_connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT team_since FROM shared_workspaces WHERE workspace_id = %s", (ws,)
            )
            return (await cur.fetchone())["team_since"]

    async def name(team):
        return await db.set_audience(ws, everyone=None, team_id=team, names_team=True)

    async def open_(everyone):
        return await db.set_audience(ws, everyone=everyone, team_id=None, names_team=False)

    assert await name(TEAM) == (A(True, None), A(False, TEAM))
    row = await db.get_shared_workspace(ws)
    assert (row.team_id, row.everyone) == (TEAM, False)

    since = await team_since()
    # Naming the same team again keeps when it was named; closing an already
    # closed workspace keeps its team.
    assert await name(TEAM) == (A(False, TEAM), A(False, TEAM))
    assert await open_(False) == (A(False, TEAM), A(False, TEAM))
    assert await team_since() == since

    assert await open_(True) == (A(False, TEAM), A(True, None))
    row = await db.get_shared_workspace(ws)
    assert (row.team_id, row.everyone) == (None, True)

    assert await name(OTHER) == (A(True, None), A(False, OTHER))
    assert await name(None) == (A(False, OTHER), A(False, None))
    row = await db.get_shared_workspace(ws)
    assert (row.team_id, row.everyone) == (None, False)

    # Both at once, in one write: no team, and open.
    await name(TEAM)
    assert await db.set_audience(ws, everyone=True, team_id=None, names_team=True) == (
        A(False, TEAM), A(True, None)
    )
    # Naming no team keeps it open.
    assert await name(None) == (A(True, None), A(True, None))

    with pytest.raises(LookupError):
        await db.set_audience(str(uuid.uuid4()), everyone=None, team_id=TEAM, names_team=True)


async def test_the_count_of_workspaces_naming_a_team(patched_get_db_connection, cleanup_tables):
    from src.server.database import shared_workspaces as db

    async with patched_get_db_connection() as conn, conn.cursor() as cur:
        await _seed_users(cur)
        first = await _workspace(cur, listed=False, everyone=False, team=TEAM)
        await _workspace(cur, listed=False, everyone=False, team=TEAM)
        await _workspace(cur, listed=False, everyone=False, team=TEAM, org="org-elsewhere")
        await _workspace(cur, listed=False, everyone=False, team=OTHER)

    assert await db.count_naming_team(ORG, TEAM) == 2
    assert await db.count_naming_team("org-elsewhere", TEAM) == 1
    assert await db.count_naming_team(ORG, "team-none") == 0

    async with patched_get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s", (first,))
    assert await db.count_naming_team(ORG, TEAM) == 1
