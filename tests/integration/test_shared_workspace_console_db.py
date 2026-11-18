"""The console's reads and writes over an organization's shared workspaces,
real PostgreSQL.

A page holds only the named organization's live workspaces, newest first,
each with its members and its computer's state, and pages by its cursor
without repeating or skipping one. Making someone a manager promotes a listed
member, or adds someone not listed, and says which.
"""

from __future__ import annotations

import uuid

import pytest

from src.server.database import shared_workspaces as db
from src.server.database import shared_workspaces_console as console_db

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ORG = "org-console"
PEOPLE = {"ana": "Ana Ruiz", "sam": "Sam Ito", "lee": "Lee Chen"}


async def _seed(conn, name: str, *, org: str = ORG, status: str = "running", age: int = 0) -> str:
    ws, computer = uuid.uuid4(), uuid.uuid4()
    holder = f"shared-{uuid.uuid4()}"
    async with conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (holder,)
        )
        await cur.execute(
            "INSERT INTO computers (computer_id, user_id, kind, status, name)"
            " VALUES (%s, %s, 'daytona', 'stopped', %s)",
            (computer, holder, name),
        )
        await cur.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status, computer_id)"
            " VALUES (%s, %s, %s, %s, %s)",
            (ws, holder, name, status, computer),
        )
        await cur.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, created_by, created_at)"
            " VALUES (%s, %s, %s, 'ana', NOW() - make_interval(mins => %s))",
            (ws, holder, org, age),
        )
        for user, role in (("ana", "manager"), ("sam", "member")):
            await cur.execute(
                "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)"
                " VALUES (%s, %s, %s, 'ana')",
                (ws, user, role),
            )
    return str(ws)


async def _people(conn) -> None:
    async with conn.cursor() as cur:
        for uid, name in PEOPLE.items():
            await cur.execute(
                "INSERT INTO users (user_id, name, email) VALUES (%s, %s, %s)",
                (uid, name, f"{uid}@example.com"),
            )


async def test_pages_hold_only_the_organizations_live_workspaces(
    patched_get_db_connection, cleanup_tables
):
    async with patched_get_db_connection() as conn:
        await _people(conn)
        live = [await _seed(conn, f"Desk {i}", age=i) for i in range(5)]
        other = await _seed(conn, "Other org", org="org-other")
        gone = await _seed(conn, "Gone", status="deleted", age=99)

    seen: list[str] = []
    cursor = None
    while True:
        rows, cursor = await console_db.list_for_org(ORG, cursor=cursor, limit=2)
        seen += [r["workspace_id"] for r in rows]
        if cursor is None:
            break
    assert seen == live

    (first,), _ = await console_db.list_for_org(ORG, limit=1)
    assert first["name"] == "Desk 0" and first["created_by_name"] == "Ana Ruiz"
    assert first["computer_status"] == "stopped" and first["last_activity_at"] is not None
    assert [(m["user_id"], m["role"], m["name"]) for m in first["members"]] == [
        ("ana", "manager", "Ana Ruiz"),
        ("sam", "member", "Sam Ito"),
    ]
    only, more = await console_db.list_for_org(ORG, workspace_id=live[3])
    assert [r["workspace_id"] for r in only] == [live[3]] and more is None
    for absent in (other, gone, "not-a-uuid"):
        assert await console_db.list_for_org(ORG, workspace_id=absent) == ([], None)
    with pytest.raises(ValueError):
        await console_db.list_for_org(ORG, cursor="not-a-cursor")


async def test_the_console_reaches_only_its_own_organizations_workspace(
    patched_get_db_connection, cleanup_tables
):
    async with patched_get_db_connection() as conn:
        await _people(conn)
        ours = await _seed(conn, "Ours")
        gone = await _seed(conn, "Gone", status="deleted")
    assert (await db.get_live_in_org(ours, ORG)).workspace_id == ours
    assert await db.get_live_in_org(ours, "org-other") is None
    assert await db.get_live_in_org(gone, ORG) is None
    assert await db.get_live_in_org("not-a-uuid", ORG) is None


async def test_making_a_manager_promotes_or_adds_and_says_which(
    patched_get_db_connection, cleanup_tables
):
    async with patched_get_db_connection() as conn:
        await _people(conn)
        ws = await _seed(conn, "Desk")
    assert await db.make_manager(ws, "sam", "admin") == "member"
    assert await db.get_member_role(ws, "sam") == "manager"
    assert await db.make_manager(ws, "lee", "admin") is None
    assert await db.get_member_role(ws, "lee") == "manager"
    assert await db.make_manager(ws, "lee", "admin") == "manager"


async def test_turn_running_reads_the_run_ledger_of_every_thread(
    patched_get_db_connection, cleanup_tables
):
    """True while a root or subagent run is in progress in one of the
    workspace's threads; a turn paused on a question runs nothing."""
    async with patched_get_db_connection() as conn:
        await _people(conn)
        idle, running, paused, background = [
            await _seed(conn, f"Desk {i}", age=i) for i in range(4)
        ]
        async with conn.cursor() as cur:
            for ws, status in ((running, "in_progress"), (paused, "interrupted"), (background, "completed")):
                thread = uuid.uuid4()
                await cur.execute(
                    "INSERT INTO conversation_threads"
                    " (conversation_thread_id, workspace_id, current_status, thread_index)"
                    " VALUES (%s, %s, 'completed', 0)",
                    (thread, ws),
                )
                run = uuid.uuid4()
                await cur.execute(
                    "INSERT INTO conversation_responses"
                    " (conversation_response_id, conversation_thread_id, turn_index, status, created_at)"
                    " VALUES (%s, %s, 0, 'in_progress', NOW())",
                    (run, thread),
                )
                if status != "in_progress":
                    await cur.execute(
                        "UPDATE conversation_responses SET status = %s"
                        " WHERE conversation_response_id = %s",
                        (status, run),
                    )
                if ws == background:
                    await cur.execute(
                        "INSERT INTO subagent_tasks (thread_id, task_id) VALUES (%s, 'k1')",
                        (thread,),
                    )
                    await cur.execute(
                        "INSERT INTO subagent_runs (thread_id, task_id, status)"
                        " VALUES (%s, 'k1', 'in_progress')",
                        (thread,),
                    )

    rows, _ = await console_db.list_for_org(ORG)
    running_now = {r["workspace_id"]: r["turn_running"] for r in rows}
    assert running_now == {idle: False, running: True, paused: False, background: True}
    (one,), _ = await console_db.list_for_org(ORG, workspace_id=running)
    assert one["turn_running"] is True
