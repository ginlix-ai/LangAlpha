"""A shared workspace's private thread, real PostgreSQL.

Maya keeps one thread private and shares another; Lee is a member and Nate a
manager. Every list a member reads (the workspace's, recents, search, the
feed snapshot, the attention count, one thread by id) holds the private
thread for Maya alone, and so does the unread flag. A feed event is addressed
from the database when it is published. Making a thread private and a colleague's first
turn decide under the thread row's lock, in either order: the turn is refused
once the flip committed first, and the flip is refused once the turn did.
A removal or a change of who the workspace is open to meets a turn's START
under the member row and the workspace row: committed first, it refuses the
turn; waiting, it finds the run to stop.
"""

from __future__ import annotations

import asyncio
import uuid
from functools import partial

import pytest
from fastapi import HTTPException

from tests.integration.test_shared_thread_reads_db import _turn

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

MAYA, LEE, NATE = "pt-maya", "pt-lee", "pt-nate"
HOLDER = "shared-00000000-0000-4000-8000-0000000000cc"


@pytest.fixture(autouse=True)
def _tuple_rows(patched_get_db_connection, monkeypatch):
    from tests.integration.test_steer_outbox_db import _app_rows

    _app_rows(monkeypatch)


async def _seed(conn) -> dict[str, str]:
    ws = uuid.uuid4()
    private, shared = uuid.uuid4(), uuid.uuid4()
    async with conn.cursor() as cur:
        for uid in (MAYA, LEE, NATE):
            await cur.execute("INSERT INTO users (user_id) VALUES (%s)", (uid,))
        await cur.execute(
            "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (HOLDER,)
        )
        await cur.execute(
            "INSERT INTO workspaces (workspace_id, user_id, name, status) VALUES (%s, %s, 'Desk', 'running')",
            (ws, HOLDER),
        )
        await cur.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, created_by)"
            " VALUES (%s, %s, 'org', %s)",
            (ws, HOLDER, MAYA),
        )
        for uid, role in ((MAYA, "manager"), (LEE, "member"), (NATE, "manager")):
            await cur.execute(
                "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by, added_at)"
                " VALUES (%s, %s, %s, %s, NOW() - INTERVAL '2 hours')",
                (ws, uid, role, MAYA),
            )
        await cur.execute(
            "INSERT INTO conversation_threads"
            " (conversation_thread_id, workspace_id, current_status, thread_index, title, created_by)"
            " VALUES (%s, %s, 'completed', 0, 'Orchid takeover memo', %s)",
            (private, ws, MAYA),
        )
        await cur.execute(
            "INSERT INTO conversation_threads"
            " (conversation_thread_id, workspace_id, current_status, thread_index, title, created_by,"
            "  workspace_shared_at, workspace_shared_by)"
            " VALUES (%s, %s, 'completed', 1, 'Orchid supply chain', %s, NOW(), %s)",
            (shared, ws, MAYA, MAYA),
        )
    return {"ws": str(ws), "private": str(private), "shared": str(shared)}


async def _lists(who: str, s: dict[str, str]) -> dict[str, set[str]]:
    from src.server.database.conversation.viewer_threads import (
        get_thread_for_viewer,
        shared_lifecycle_rows,
        threads_with_shared,
        workspace_threads_for_member,
    )
    from src.server.database.search.read import search_threads

    ids = lambda rows: {str(r["conversation_thread_id"]) for r in rows}  # noqa: E731
    page, total = await workspace_threads_for_member(s["ws"], who)
    assert total == len(page)
    recents, _ = await threads_with_shared(who, [s["ws"]])
    snapshot, _ = await shared_lifecycle_rows(who, [s["ws"]])
    found = await search_threads(who, ["Orchid"], limit=20, shared=[s["ws"]])
    return {
        "workspace": ids(page),
        "recents": ids(recents),
        "snapshot": {str(r["conversation_thread_id"]) for r in snapshot},
        "search": {m.thread_id for m in found},
        "by_id": {t for t in (s["private"], s["shared"]) if await get_thread_for_viewer(t, who)},
    }


async def test_only_its_starter_lists_a_private_thread(patched_get_db_connection, cleanup_tables):
    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
        # A finished turn in each, so the snapshot names both as unread.
        await _turn(conn, s["private"], MAYA, "completed")
        await _turn(conn, s["shared"], MAYA, "completed")

    both = {s["private"], s["shared"]}
    for name, got in (await _lists(MAYA, s)).items():
        assert got == both, name
    for who in (LEE, NATE):
        for name, got in (await _lists(who, s)).items():
            assert got == {s["shared"]}, (who, name)


async def test_a_member_is_shown_no_thread_number(patched_get_db_connection, cleanup_tables):
    """A workspace numbers its threads in one sequence, private ones
    included, so the numbers would count what a member cannot see."""
    from src.server.database.conversation.viewer_threads import (
        get_thread_for_viewer,
        threads_with_shared,
        workspace_threads_for_member,
    )

    own_ws, own = uuid.uuid4(), uuid.uuid4()
    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO workspaces (workspace_id, user_id, name, status)"
                " VALUES (%s, %s, 'Own', 'running')",
                (own_ws, LEE),
            )
            await cur.execute(
                "INSERT INTO conversation_threads"
                " (conversation_thread_id, workspace_id, current_status, thread_index, title)"
                " VALUES (%s, %s, 'completed', 4, 'Lee alone')",
                (own, own_ws),
            )

    for who in (MAYA, LEE):
        page, _ = await workspace_threads_for_member(s["ws"], who)
        assert page and all(r["thread_index"] is None for r in page), who
        assert (await get_thread_for_viewer(s["shared"], who))["thread_index"] is None

    recents, _ = await threads_with_shared(LEE, [s["ws"]])
    numbers = {str(r["conversation_thread_id"]): r["thread_index"] for r in recents}
    assert numbers == {s["shared"]: None, str(own): 4}

    # A sort by number is the order the threads were started in.
    by_number, _ = await workspace_threads_for_member(
        s["ws"], MAYA, sort_by="thread_index", sort_order="asc"
    )
    by_start, _ = await workspace_threads_for_member(
        s["ws"], MAYA, sort_by="created_at", sort_order="asc"
    )
    assert [r["conversation_thread_id"] for r in by_number] == [
        r["conversation_thread_id"] for r in by_start
    ]


async def test_a_private_threads_folders_are_named_only_to_its_starter(
    patched_get_db_connection, cleanup_tables
):
    """A thread's folders in the workspace's folder carry its id's first
    eight characters; one no thread has is hidden from everyone."""
    from src.server.database.conversation.thread_reach import unreached_folders

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    private, shared, stray = s["private"][:8], s["shared"][:8], "0badf00d"
    names = [private, shared, stray]

    assert await unreached_folders(s["ws"], MAYA, names) == {stray}
    for who in (LEE, NATE):
        assert await unreached_folders(s["ws"], who, names) == {private, stray}


async def test_the_unread_flag_counts_only_threads_the_member_reaches(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation.shared_thread_reads import stamp_member_seen
    from src.server.database.conversation.viewer_threads import count_shared_attention
    from src.server.database.shared_workspaces import list_with_unread

    async def unread(who: str) -> bool:
        [(_, _, flag)] = await list_with_unread(who, "org")
        return flag

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
        run, _ = await _turn(conn, s["private"], MAYA, "completed")
    # Only the private thread holds an unread turn: Maya's flag, nobody else's.
    assert await unread(MAYA) is True
    assert await unread(LEE) is False and await unread(NATE) is False
    assert await count_shared_attention(LEE, [s["ws"]]) == 0
    await stamp_member_seen(s["private"], MAYA, run)
    assert await unread(MAYA) is False


async def test_a_feed_event_is_addressed_from_the_database_when_published(
    patched_get_db_connection, cleanup_tables, monkeypatch
):
    import json
    from types import SimpleNamespace

    from src.server.database.conversation import thread_reach
    from src.server.services import thread_lifecycle_feed
    from src.server.services.shared_workspaces import feed
    from src.server.services.shared_workspaces.feed import Reach, SharedFeed

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)

    async def reach_of(user_id, **_):
        return Reach("org", frozenset({s["ws"]}))

    sent: list[dict] = []

    async def publish(channel, message):
        sent.append(json.loads(message))

    monkeypatch.setattr(feed, "reach_of", reach_of)
    monkeypatch.setattr(
        thread_lifecycle_feed, "get_cache_client",
        lambda: SimpleNamespace(enabled=True, client=SimpleNamespace(publish=publish)),
    )

    async def told(who: str) -> int:
        sent.clear()
        await thread_lifecycle_feed.publish_thread_title(
            user_id=None, thread_id=s["private"], workspace_id=s["ws"], title="t", shared_org="org"
        )
        return len(await (await SharedFeed.open(who)).on_event(sent[0]))

    assert (await told(MAYA), await told(LEE)) == (1, 0)
    # Shared now: the next event published is everyone's.
    assert (await thread_reach.set_shared(s["private"], MAYA, True))[0] == "done"
    assert await told(LEE) == 1


async def _start(thread: str, user: str, hold: float = 0.0):
    from src.server.database.runs import lifecycle
    from src.server.services.runs import admission

    async def hook(conn):
        await asyncio.sleep(hold)
        await admission._refuse_private_thread(thread, user, conn)

    return await lifecycle.start_run(
        run_id=str(uuid.uuid4()), thread_id=thread, request_key=str(uuid.uuid4()),
        user_id=user, before_commit=hook,
    )


async def _start_reaching(thread: str, user: str, *, teams=(), hold: float = 0.0):
    """START with both of a shared turn's hooks, holding the transaction open
    for ``hold`` seconds once they have read."""
    from src.server.database.runs import lifecycle
    from src.server.services.account_contract import Team
    from src.server.services.runs import admission
    from src.server.services.shared_workspaces.access import Seat

    seat = Seat("org", tuple(Team(t, t, "member") for t in teams))

    async def held(conn):
        await asyncio.sleep(hold)

    async def hook(conn):
        await admission._refuse_private_thread(
            thread, user, conn,
            then=partial(admission._refuse_unreached_workspace, HOLDER, user, seat, then=held),
        )

    return await lifecycle.start_run(
        run_id=str(uuid.uuid4()), thread_id=thread, request_key=str(uuid.uuid4()),
        user_id=user, before_commit=hook,
    )


async def test_a_turn_holding_its_member_row_is_found_by_the_removal_that_waited(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database import shared_workspaces as db

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    start = asyncio.create_task(_start_reaching(s["shared"], LEE, hold=1.0))
    await asyncio.sleep(0.4)  # START now holds Lee's member row
    # The delete waits on START's lock, so the run has committed once it
    # returns, and the removal's stop finds it.
    assert await db.remove_member(s["ws"], LEE) == "member"
    turns, _ = await db.running_work(s["ws"], LEE)
    await start
    assert [str(t["conversation_thread_id"]) for t in turns] == [s["shared"]]


async def test_a_removal_that_committed_first_refuses_the_turn(
    patched_get_db_connection, cleanup_tables
):
    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    async with patched_get_db_connection() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM shared_workspace_members WHERE workspace_id = %s AND user_id = %s",
                (s["ws"], LEE),
            )
            start = asyncio.create_task(_start_reaching(s["shared"], LEE))
            await asyncio.sleep(0.4)  # START waits on the deleted row
            assert not start.done()
    with pytest.raises(HTTPException) as refused:
        await start
    assert (refused.value.status_code, refused.value.detail) == (404, "Thread not found")
    async with patched_get_db_connection() as conn:
        assert await _runs(conn, s["shared"], LEE) == 0


async def test_team_and_everyone_count_as_reach_at_start(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database import shared_workspaces as db

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
        await conn.execute(
            "UPDATE shared_workspaces SET team_id = 'team-1', team_since = NOW()"
            " WHERE workspace_id = %s",
            (s["ws"],),
        )
    await db.remove_member(s["ws"], LEE)
    with pytest.raises(HTTPException):
        await _start_reaching(s["shared"], LEE, teams=("team-2",))
    await _start_reaching(s["shared"], LEE, teams=("team-1",))
    await db.set_audience(s["ws"], everyone=True, team_id=None, names_team=False)
    async with patched_get_db_connection() as conn:
        await conn.execute(
            "UPDATE conversation_responses SET status = 'completed'"
            " WHERE conversation_thread_id = %s",
            (s["shared"],),
        )
    await _start_reaching(s["shared"], LEE)
    async with patched_get_db_connection() as conn:
        assert await _runs(conn, s["shared"], LEE) == 2


async def test_closing_to_everyone_waits_for_a_turn_holding_the_workspace_row(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database import shared_workspaces as db

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    await db.remove_member(s["ws"], LEE)
    await db.set_audience(s["ws"], everyone=True, team_id=None, names_team=False)
    start = asyncio.create_task(_start_reaching(s["shared"], LEE, hold=1.0))
    await asyncio.sleep(0.4)
    await db.set_audience(s["ws"], everyone=False, team_id=None, names_team=False)
    turns, _ = await db.running_work(s["ws"], None)
    await start
    assert [str(t["conversation_thread_id"]) for t in turns] == [s["shared"]]


async def _visibility(conn, thread_id: str):
    from psycopg.rows import dict_row

    from src.server.database.conversation import thread_reach

    async with conn.cursor(row_factory=dict_row) as cur:
        return await thread_reach._visibility(cur, thread_id)


async def _runs(conn, thread: str, user: str) -> int:
    row = await (
        await conn.execute(
            "SELECT count(*) FROM conversation_responses"
            " WHERE conversation_thread_id = %s AND user_id = %s",
            (thread, user),
        )
    ).fetchone()
    return int(row["count"] if isinstance(row, dict) else row[0])


async def test_a_colleagues_first_turn_holding_the_row_wins_and_the_flip_is_refused(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation import thread_reach

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    start = asyncio.create_task(_start(s["shared"], LEE, hold=1.0))
    await asyncio.sleep(0.4)  # START now holds the thread row, inside its hook
    outcome, _, _ = await thread_reach.set_shared(s["shared"], MAYA, False)
    await start
    assert outcome == "others_sent"
    async with patched_get_db_connection() as conn:
        assert await _runs(conn, s["shared"], LEE) == 1
        visibility = await _visibility(conn, s["shared"])
    assert visibility.shared and visibility.others_sent


async def test_a_flip_holding_the_row_wins_and_the_colleagues_turn_is_refused(
    patched_get_db_connection, cleanup_tables, monkeypatch
):
    from src.server.database.conversation import thread_reach

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    original = thread_reach._visibility

    async def slow(cur, thread_id):
        await asyncio.sleep(1.0)  # the flip has updated the row and not committed
        return await original(cur, thread_id)

    monkeypatch.setattr(thread_reach, "_visibility", slow)
    flip = asyncio.create_task(thread_reach.set_shared(s["shared"], MAYA, False))
    await asyncio.sleep(0.4)
    with pytest.raises(HTTPException) as refused:
        await _start(s["shared"], LEE)
    assert (refused.value.status_code, refused.value.detail) == (404, "Thread not found")
    assert (await flip)[0] == "done"
    async with patched_get_db_connection() as conn:
        assert await _runs(conn, s["shared"], LEE) == 0


async def test_the_starter_can_take_back_only_what_no_one_else_wrote_in(
    patched_get_db_connection, cleanup_tables
):
    from src.server.database.conversation import thread_reach

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
        await _turn(conn, s["shared"], MAYA, "completed")
    assert (await thread_reach.set_shared(s["shared"], LEE, False))[0] == "not_starter"
    assert (await thread_reach.set_shared(s["shared"], MAYA, False))[0] == "done"
    assert (await thread_reach.set_shared(s["shared"], MAYA, False))[0] == "unchanged"
    assert (await thread_reach.set_shared(s["shared"], MAYA, True))[0] == "done"
    async with patched_get_db_connection() as conn:
        await _turn(conn, s["shared"], LEE, "completed")
    assert (await thread_reach.set_shared(s["shared"], MAYA, False))[0] == "others_sent"
    assert (await thread_reach.set_shared(str(uuid.uuid4()), MAYA, False))[0] == "missing"


async def test_making_private_takes_its_link_and_library_item_down_for_good(
    patched_get_db_connection, cleanup_tables
):
    """Lee turned the link on for the organization and published it; once
    Maya makes the thread private, the token and the library answer as for a
    link never turned on, and sharing it again leaves the link off."""
    from src.server.database import org_library
    from src.server.database.conversation import thread_reach
    from src.server.database.conversation.threads_read import get_thread_by_share_token
    from src.server.database.conversation.threads_write import update_thread_sharing

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    on = await update_thread_sharing(
        s["shared"], True, share_permissions={"audience": "organization", "allow_files": True}
    )
    token = on["share_token"]
    assert await org_library.publish(
        org_id="org", publisher_id=LEE, kind="thread", target_id=s["shared"],
        title=None, summary=None, owner_id=HOLDER,
    )
    assert await get_thread_by_share_token(token)
    listed = lambda: org_library.list_items("org", viewer=LEE)  # noqa: E731
    assert [i["thread_id"] for i in (await listed())[0]] == [s["shared"]]

    outcome, _, link_off = await thread_reach.set_shared(s["shared"], MAYA, False)
    assert outcome == "done" and link_off["audience"] == "organization"
    assert await get_thread_by_share_token(token) is None
    assert (await listed())[0] == []

    assert (await thread_reach.set_shared(s["shared"], MAYA, True))[0] == "done"
    assert await get_thread_by_share_token(token) is None
    assert (await listed())[0] == []
    async with patched_get_db_connection() as conn:
        row = await (
            await conn.execute(
                "SELECT share_token FROM conversation_threads WHERE conversation_thread_id = %s",
                (s["shared"],),
            )
        ).fetchone()
    # The token is kept, so turning the link on again opens the same URL.
    assert (row["share_token"] if isinstance(row, dict) else row[0]) == token


async def test_a_colleagues_comment_words_keep_the_thread_shared(
    patched_get_db_connection, cleanup_tables
):
    """Lee queues a comment for the shared thread: Maya cannot take it back,
    nor once her own send carried his words into her turn. Words of Lee's
    cannot be left waiting for her private thread either."""
    from src.server.database.artifacts import comments, delivery, routing
    from src.server.database.conversation import thread_reach

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
        artifact = str(uuid.uuid4())
        await conn.execute(
            "INSERT INTO share_links (code, workspace_id, kind, path, artifact_id)"
            " VALUES (%s, %s, 'file', 'memo.md', %s)",
            (f"pt{uuid.uuid4().hex[:10]}", s["ws"], artifact),
        )
    anchor = {"kind": "text", "quote": "Orchid"}
    lees = await comments.create_comment(
        user_id=HOLDER, artifact_id=artifact, anchor=anchor, body="check the margin",
        delivery="queued", thread_id=s["shared"], caller=LEE,
    )
    assert (await thread_reach.set_shared(s["shared"], MAYA, False))[0] == "others_sent"
    assert await delivery.claim_comments(
        user_id=HOLDER, thread_id=s["shared"], comment_ids=[lees.comment_id],
        request_key=str(uuid.uuid4()), caller=MAYA,
    )
    assert (await thread_reach.set_shared(s["shared"], MAYA, False))[0] == "others_sent"
    async with patched_get_db_connection() as conn:
        assert (await _visibility(conn, s["shared"])).others_sent

    mayas = await comments.create_comment(
        user_id=HOLDER, artifact_id=artifact, anchor=anchor, body="for my draft",
        delivery="queued", thread_id=s["private"], caller=MAYA,
    )
    with pytest.raises(routing.DestinationUnavailable):
        await comments.add_owner_message(
            user_id=HOLDER, artifact_id=artifact, comment_id=mayas.comment_id,
            body="and the capex", delivery=None, caller=LEE,
        )
    async with patched_get_db_connection() as conn:
        assert not (await _visibility(conn, s["private"])).others_sent


async def test_a_colleagues_send_does_not_carry_a_comment_queued_for_a_private_thread(
    patched_get_db_connection, cleanup_tables
):
    """Maya's comment waits for her private thread. Lee's send to the shared
    thread, naming it as a Send all would, carries nothing and leaves it
    queued; her own send to that thread carries it."""
    from src.server.database.artifacts import comments, delivery

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
        artifact = str(uuid.uuid4())
        await conn.execute(
            "INSERT INTO share_links (code, workspace_id, kind, path, artifact_id)"
            " VALUES (%s, %s, 'file', 'memo.md', %s)",
            (f"pt{uuid.uuid4().hex[:10]}", s["ws"], artifact),
        )
    mayas = await comments.create_comment(
        user_id=HOLDER, artifact_id=artifact, anchor={"kind": "text", "quote": "Orchid"},
        body="for my draft", delivery="queued", thread_id=s["private"], caller=MAYA,
    )
    assert await delivery.claim_comments(
        user_id=HOLDER, thread_id=s["shared"], comment_ids=[mayas.comment_id],
        request_key=str(uuid.uuid4()), caller=LEE,
    ) == []
    async with patched_get_db_connection() as conn:
        row = await (
            await conn.execute(
                "SELECT delivery, thread_id::text FROM artifact_comments WHERE comment_id = %s",
                (mayas.comment_id,),
            )
        ).fetchone()
    assert tuple(row.values() if isinstance(row, dict) else row) == ("queued", s["private"])
    (claimed,) = await delivery.claim_comments(
        user_id=HOLDER, thread_id=s["private"], comment_ids=[mayas.comment_id],
        request_key=str(uuid.uuid4()), caller=MAYA,
    )
    assert claimed.delivered == ["for my draft"]
