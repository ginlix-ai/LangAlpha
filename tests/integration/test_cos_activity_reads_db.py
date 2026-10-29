"""Integration tests for the cross-workspace activity reads the Chief of
Staff's Home view uses: recent threads, settled automation runs, and
workspaces sorted by recency, against real PostgreSQL."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _thread(
    db, workspace_id, index, *, title=None, archived=False, age_seconds=0
):
    """A thread row with a controlled updated_at, so ordering does not
    depend on how fast consecutive inserts actually run."""
    thread_id = str(uuid4())
    async with db() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO conversation_threads (
                conversation_thread_id, workspace_id, current_status,
                thread_index, title, archived_at, created_at, updated_at
            )
            VALUES (
                %s, %s, 'completed', %s, %s,
                CASE WHEN %s THEN NOW() ELSE NULL END,
                NOW() - make_interval(secs => %s),
                NOW() - make_interval(secs => %s)
            )
            """,
            (thread_id, workspace_id, index, title, archived, age_seconds, age_seconds),
        )
    return thread_id


async def _execution(
    db,
    automation_id,
    *,
    status="completed",
    completed_at=None,
    conversation_thread_id=None,
):
    """An execution row with the exact status and completed_at a settlement
    test needs, rather than one driven through the settle_execution lifecycle."""
    execution_id = str(uuid4())
    async with db() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO automation_executions (
                automation_execution_id, automation_id, status, scheduled_at,
                server_id, created_at, heartbeat_at, completed_at,
                conversation_thread_id
            )
            VALUES (%s, %s, %s, NOW(), 'server-1', NOW(), NOW(), %s, %s)
            """,
            (execution_id, automation_id, status, completed_at, conversation_thread_id),
        )
    return execution_id


async def _ws(
    db, user_id, name, *, status="running", last_activity_at=None, updated_at=None
):
    """A workspace row with an explicit updated_at: every ORM UPDATE
    re-stamps updated_at via trg_workspaces_updated_at, so only a direct
    INSERT can pin it to a chosen value."""
    workspace_id = str(uuid4())
    async with db() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO workspaces (
                workspace_id, user_id, name, status, created_at, updated_at,
                last_activity_at
            )
            VALUES (%s, %s, %s, %s, NOW(), %s, %s)
            """,
            (workspace_id, user_id, name, status, updated_at, last_activity_at),
        )
    return workspace_id


class TestRecentThreadsForUser:
    """get_recent_threads_for_user: the cross-workspace recency feed."""

    async def test_orders_newest_first_and_excludes_archived_and_given_thread(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation.threads_read import (
            get_recent_threads_for_user,
        )

        db = patched_get_db_connection
        ws_id = str(seed_workspace["workspace_id"])

        oldest = await _thread(db, ws_id, 0, age_seconds=300)
        middle = await _thread(db, ws_id, 1, age_seconds=60)
        archived = await _thread(db, ws_id, 2, age_seconds=30, archived=True)
        excluded = await _thread(db, ws_id, 3, age_seconds=10)
        newest = await _thread(db, ws_id, 4, age_seconds=0)

        rows = await get_recent_threads_for_user(
            seed_workspace["user_id"], exclude_thread_id=excluded
        )

        assert [str(r["conversation_thread_id"]) for r in rows] == [
            newest,
            middle,
            oldest,
        ]
        assert archived not in [str(r["conversation_thread_id"]) for r in rows]
        assert rows[0]["workspace_name"] == seed_workspace["name"]

    async def test_excludes_automation_origin_other_user_and_deleted_workspace(
        self, seed_user, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import create_thread
        from src.server.database.conversation.threads_read import (
            get_recent_threads_for_user,
        )
        from src.server.database.user import create_user
        from src.server.database.workspace import create_workspace, delete_workspace

        ws_id = str(seed_workspace["workspace_id"])

        kept = await create_thread(
            conversation_thread_id=str(uuid4()),
            workspace_id=ws_id,
            current_status="completed",
            msg_type="ptc",
            title="Kept thread",
        )
        await create_thread(
            conversation_thread_id=str(uuid4()),
            workspace_id=ws_id,
            current_status="completed",
            msg_type="ptc",
            title="Automation thread",
            metadata={"origin": {"type": "automation", "id": "auto-1"}},
        )

        other_user = await create_user(
            user_id="test-user-integration-002",
            email="other@example.com",
            name="Other User",
        )
        other_ws = await create_workspace(
            user_id=other_user["user_id"], name="Other User Workspace", status="running"
        )
        await create_thread(
            conversation_thread_id=str(uuid4()),
            workspace_id=str(other_ws["workspace_id"]),
            current_status="completed",
            msg_type="ptc",
            title="Other user's thread",
        )

        deleted_ws = await create_workspace(
            user_id=seed_user["user_id"], name="Soon Deleted", status="running"
        )
        await create_thread(
            conversation_thread_id=str(uuid4()),
            workspace_id=str(deleted_ws["workspace_id"]),
            current_status="completed",
            msg_type="ptc",
            title="Thread in a deleted workspace",
        )
        await delete_workspace(str(deleted_ws["workspace_id"]))

        rows = await get_recent_threads_for_user(seed_user["user_id"])

        assert [str(r["conversation_thread_id"]) for r in rows] == [
            str(kept["conversation_thread_id"])
        ]

    async def test_title_falls_back_to_first_query_when_empty_or_null(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.conversation import create_query, create_thread
        from src.server.database.conversation.threads_read import (
            get_recent_threads_for_user,
        )

        ws_id = str(seed_workspace["workspace_id"])
        long_query = (
            "What is the long-run outlook for semiconductor capex spending? " * 3
        )

        null_title_thread = await create_thread(
            conversation_thread_id=str(uuid4()),
            workspace_id=ws_id,
            current_status="completed",
            msg_type="ptc",
        )
        null_title_id = str(null_title_thread["conversation_thread_id"])
        await create_query(
            conversation_query_id=str(uuid4()),
            conversation_thread_id=null_title_id,
            turn_index=0,
            content=long_query,
            query_type="initial",
        )
        await create_query(
            conversation_query_id=str(uuid4()),
            conversation_thread_id=null_title_id,
            turn_index=1,
            content="follow up question",
            query_type="follow_up",
        )

        empty_title_thread = await create_thread(
            conversation_thread_id=str(uuid4()),
            workspace_id=ws_id,
            current_status="completed",
            msg_type="ptc",
            title="",
        )
        empty_title_id = str(empty_title_thread["conversation_thread_id"])
        await create_query(
            conversation_query_id=str(uuid4()),
            conversation_thread_id=empty_title_id,
            turn_index=0,
            content="Short query",
            query_type="initial",
        )

        rows = await get_recent_threads_for_user(seed_workspace["user_id"])
        by_id = {str(r["conversation_thread_id"]): r for r in rows}

        assert by_id[null_title_id]["title"] == long_query[:120]
        assert by_id[empty_title_id]["title"] == "Short query"

    async def test_status_is_the_latest_attempts_as_activity_words_it(
        self, seed_workspace, patched_get_db_connection, seed_response
    ):
        """The row carries the latest attempt (by run_seq), not the thread's
        legacy status column, and the renderer words it from there."""
        from src.server.database.conversation.threads_read import (
            get_recent_threads_for_user,
        )
        from src.tools.secretary.activity import render_activity

        db = patched_get_db_connection
        ws_id = str(seed_workspace["workspace_id"])

        # current_status says completed on every row; only the runs differ.
        failed = await _thread(db, ws_id, 0, title="Second turn failed")
        await seed_response(str(uuid4()), failed, 0, status="completed")
        await seed_response(str(uuid4()), failed, 1, status="error")
        stopping = await _thread(db, ws_id, 1, title="Stopping")
        await seed_response(str(uuid4()), stopping, 0, status="in_progress")
        async with db() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE conversation_responses SET cancel_requested_at = NOW()"
                " WHERE conversation_thread_id = %s",
                (stopping,),
            )
        never_ran = await _thread(db, ws_id, 2, title="Never ran")

        rows = await get_recent_threads_for_user(seed_workspace["user_id"])
        by_id = {str(r["conversation_thread_id"]): r for r in rows}

        assert by_id.keys() == {never_ran, stopping, failed}
        assert by_id[never_ran]["latest_run_status"] is None
        assert by_id[stopping]["latest_run_status"] == "in_progress"
        assert by_id[failed]["latest_run_status"] == "error"
        text = render_activity(
            workspaces=[],
            workspace_total=0,
            threads=rows,
            runs=[],
            holdings=[],
            home_id="",
            zone=ZoneInfo("UTC"),
            today=datetime.now(timezone.utc).date(),
        )
        assert '"Never ran" in Test Workspace: not started,' in text
        assert '"Stopping" in Test Workspace: stopping,' in text
        assert '"Second turn failed" in Test Workspace: failed,' in text


class TestListSettledSince:
    """list_settled_since: a user's finished automation runs since a watermark."""

    async def test_filters_by_status_and_cutoff_and_scopes_to_owner(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation
        from src.server.database.automation_executions import list_settled_since
        from src.server.database.user import create_user

        db = patched_get_db_connection
        since = datetime.now(timezone.utc) - timedelta(minutes=10)

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Daily Briefing",
            trigger_type="cron",
            instruction="brief me",
            cron_expression="0 9 * * *",
        )
        aid = str(auto["automation_id"])

        too_early = await _execution(
            db, aid, status="completed", completed_at=since - timedelta(minutes=1)
        )
        completed = await _execution(
            db, aid, status="completed", completed_at=since + timedelta(minutes=1)
        )
        failed = await _execution(
            db, aid, status="failed", completed_at=since + timedelta(minutes=2)
        )
        timed_out = await _execution(
            db, aid, status="timeout", completed_at=since + timedelta(minutes=3)
        )
        still_running = await _execution(db, aid, status="running")

        other_user = await create_user(
            user_id="test-user-integration-003",
            email="third@example.com",
            name="Third User",
        )
        other_auto = await create_automation(
            user_id=other_user["user_id"],
            name="Other User's Automation",
            trigger_type="cron",
            instruction="other",
            cron_expression="0 9 * * *",
        )
        other_execution = await _execution(
            db,
            str(other_auto["automation_id"]),
            status="completed",
            completed_at=since + timedelta(minutes=1),
        )

        rows = await list_settled_since(seed_user["user_id"], since)
        ids = [str(r["automation_execution_id"]) for r in rows]

        assert ids == [timed_out, failed, completed]
        assert too_early not in ids
        assert still_running not in ids
        assert other_execution not in ids
        assert all(r["automation_name"] == "Daily Briefing" for r in rows)


class TestWorkspacesSortByRecent:
    """get_workspaces_for_user(sort_by='recent'): COALESCE(last_activity_at, updated_at) DESC."""

    async def test_orders_by_coalesced_activity_and_updated_at(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.workspace import get_workspaces_for_user

        db = patched_get_db_connection
        now = datetime.now(timezone.utc)

        most_recent = await _ws(
            db,
            seed_user["user_id"],
            "Most Recent",
            last_activity_at=now,
            updated_at=now - timedelta(hours=1),
        )
        fallback_to_updated_at = await _ws(
            db,
            seed_user["user_id"],
            "Fallback",
            last_activity_at=None,
            updated_at=now - timedelta(minutes=5),
        )
        oldest = await _ws(
            db,
            seed_user["user_id"],
            "Oldest",
            last_activity_at=now - timedelta(minutes=30),
            updated_at=now - timedelta(hours=2),
        )

        workspaces, _ = await get_workspaces_for_user(
            seed_user["user_id"], sort_by="recent"
        )

        assert [str(w["workspace_id"]) for w in workspaces] == [
            most_recent,
            fallback_to_updated_at,
            oldest,
        ]

    async def test_ties_fall_back_to_the_workspace_id(
        self, seed_user, patched_get_db_connection
    ):
        """Equal timestamps must not come back in whatever order the plan
        happens to produce: the activity block would rewrite itself."""
        from src.server.database.workspace import get_workspaces_for_user

        db = patched_get_db_connection
        at = datetime.now(timezone.utc)

        tied = [
            await _ws(
                db,
                seed_user["user_id"],
                f"Tied {i}",
                last_activity_at=at,
                updated_at=at,
            )
            for i in range(4)
        ]

        for sort_by in ("recent", "activity"):
            workspaces, _ = await get_workspaces_for_user(
                seed_user["user_id"], sort_by=sort_by
            )
            assert [str(w["workspace_id"]) for w in workspaces] == sorted(
                tied, reverse=True
            )
