"""A Drive file's comments against a real Postgres (migration 155): one
record per file in its owner's Home, keyed by the entry, which follows the
file through a move, the trash and a restore and goes with it on a purge;
it holds comments and nothing a file's record shares or versions, and its
comments go to any of the owner's own workspaces but Home's stand-in."""

# The ``space`` fixture comes from drive_helpers; each test takes it by name.
# ruff: noqa: F811

from __future__ import annotations

import uuid

import psycopg
import pytest

from src.server.database import artifacts
from src.server.database import workspace as workspaces
from src.server.database.artifacts import DestinationUnavailable
from src.server.database.drive import trash as trash_rows
from src.server.database.drive import writes
from src.server.database.drive.records import Actor
from src.server.database.home_workspace import get_home_workspace_id
from tests.integration.drive_helpers import (  # noqa: F401
    LIMITS,
    person_of,
    ref_of,
    save_text,
    space,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

ANCHOR = {"kind": "block", "lines": "1", "quote": "hello"}


async def _thread(workspace_id: str) -> str:
    from src.server.database.conversation.threads_write import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, workspace_id, "completed")
    return thread_id


async def _setup(space):
    user_id = space.user_id
    home = str((await workspaces.get_or_create_home_workspace(user_id))["workspace_id"])
    ws = str(
        (await workspaces.create_workspace(user_id=user_id, name="NVDA", status="running"))[
            "workspace_id"
        ]
    )
    entry = await save_text(space, "notes/a.md", "hello")
    record = await artifacts.ensure_record(home, "drive", entry.entry_id)
    return user_id, home, ws, entry, record


async def _comment(user_id, record, **kw):
    return await artifacts.create_comment(
        user_id=user_id, artifact_id=record.artifact_id, anchor=ANCHOR, body="louder",
        caller=user_id, **{"delivery": "note", **kw},
    )


async def _claim(user_id, thread_id, comment):
    return await artifacts.claim_comments(
        user_id=user_id, thread_id=thread_id, comment_ids=[comment.comment_id],
        request_key=str(uuid.uuid4()), caller=user_id, drive_on=True,
    )


async def test_a_drive_file_has_one_record_in_its_owner_s_home(space):
    user_id, home, _, entry, record = await _setup(space)
    again = await artifacts.ensure_record(home, "drive", entry.entry_id)
    assert (record.artifact_id, record.kind, record.path, record.workspace_id) == (
        again.artifact_id, "drive", entry.entry_id, home,
    )
    found = await artifacts.get_artifact(record.artifact_id, user_id=user_id)
    assert found.kind == "drive"
    assert (await artifacts.find_record(home, "drive", entry.entry_id)).artifact_id == (
        record.artifact_id
    )
    # A file's writes never take it.
    with pytest.raises(artifacts.ArtifactNotFound):
        await artifacts.set_keep_updated(
            user_id=user_id, artifact_id=record.artifact_id, keep_updated=True
        )


@pytest.mark.parametrize(
    "assignment",
    [
        "shared_at = NOW(), files = ARRAY[path]",
        "kept_at = NOW()",
        "keep_updated = TRUE",
        "audience = 'organization'",
        "title = 'a'",
        "path = 'notes/a.md'",
        "drive_entry_id = NULL",
    ],
)
async def test_a_drive_record_is_never_shared_kept_or_moved_off_its_entry(
    space, test_db_pool, assignment
):
    *_, record = await _setup(space)
    with pytest.raises(psycopg.errors.CheckViolation):
        async with test_db_pool.connection() as conn:
            await conn.execute(
                f"UPDATE share_links SET {assignment} WHERE artifact_id = %s",
                (record.artifact_id,),
            )


async def test_only_a_drive_record_names_an_entry(space, test_db_pool):
    _, _, ws, entry, _ = await _setup(space)
    with pytest.raises(psycopg.errors.CheckViolation):
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "INSERT INTO share_links (code, workspace_id, kind, path, drive_entry_id)"
                " VALUES ('drv-x', %s, 'file', 'a.md', %s)",
                (ws, entry.entry_id),
            )


async def test_a_drive_comment_goes_to_any_own_workspace_but_the_stand_in(
    space, test_db_pool
):
    user_id, home, ws, _, record = await _setup(space)
    stand_in = str(
        (await workspaces.create_workspace(user_id=user_id, name="stand-in", status="running"))[
            "workspace_id"
        ]
    )
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET config = '{\"home_stand_in\": true}' WHERE workspace_id = %s",
            (stand_in,),
        )
    in_ws, in_home, in_stand_in = (
        await _thread(ws), await _thread(home), await _thread(stand_in),
    )
    with pytest.raises(DestinationUnavailable):
        await _comment(user_id, record, delivery="queued", thread_id=in_stand_in)
    for thread_id in (in_ws, in_home):
        comment = await _comment(user_id, record, delivery="queued", thread_id=thread_id)
        assert (comment.kind, comment.path, comment.thread_id) == (
            "drive", "notes/a.md", thread_id,
        )

    picked = await artifacts.destination_threads(
        user_id=user_id, artifact_id=record.artifact_id, caller=user_id, query=None, limit=20
    )
    assert {t.thread_id for t in picked} == {in_ws, in_home}
    assert {t.thread_id: t.home for t in picked} == {in_ws: False, in_home: True}


async def test_the_record_follows_the_file_and_a_trashed_one_is_not_sent(space):
    user_id, home, ws, entry, record = await _setup(space)
    thread_id = await _thread(ws)
    comment = await _comment(user_id, record, delivery="queued", thread_id=thread_id)
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.move("notes/a.md", "kept/b.md")

    (queued,) = await artifacts.list_queued_comments(
        user_id=user_id, thread_id=thread_id, drive_on=True
    )
    assert (queued.kind, queued.place, queued.entry_id, queued.workspace_id) == (
        "drive", "kept/b.md", entry.entry_id, home,
    )
    # The comment keeps where it was made.
    (listed,) = await artifacts.list_comments(record.artifact_id)
    assert listed.path == "notes/a.md"
    counts = await artifacts.drive_comment_counts(user_id=user_id, space_id=space.space_id)
    assert (counts[entry.entry_id].open, counts[entry.entry_id].unread) == (1, 0)

    async with writes.locked(space.space_id, LIMITS) as tx:
        trash_id = await trash_rows.trash(tx, "kept/b.md", person_of(space), None, False)
    assert await artifacts.list_queued_comments(
        user_id=user_id, thread_id=thread_id, drive_on=True
    ) == []
    assert await _claim(user_id, thread_id, comment) == artifacts.Claim([], in_trash=True)
    assert await artifacts.drive_comment_counts(user_id=user_id, space_id=space.space_id) == {}

    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.restore(tx, trash_id)
    (claimed,) = (await _claim(user_id, thread_id, comment)).comments
    assert (claimed.kind, claimed.path, claimed.place, claimed.workspace_id) == (
        "drive", entry.entry_id, "kept/b.md", home,
    )
    reply = await artifacts.add_agent_reply(
        user_id=user_id, thread_id=thread_id, comment_id=comment.comment_id,
        body="Done.", resolve=False,
    )
    assert (reply.kind, reply.path, reply.place) == ("drive", entry.entry_id, "kept/b.md")
    counts = await artifacts.drive_comment_counts(user_id=user_id, space_id=space.space_id)
    assert (counts[entry.entry_id].open, counts[entry.entry_id].unread) == (1, 1)


async def test_a_purge_takes_the_record_and_its_comments(space, test_db_pool):
    user_id, _, _, _, record = await _setup(space)
    comment = await _comment(user_id, record)
    async with writes.locked(space.space_id, LIMITS) as tx:
        trash_id = await trash_rows.trash(tx, "notes/a.md", person_of(space), None, False)
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        assert await trash_rows.purge(tx, trash_id, None) >= 1
    async with test_db_pool.connection() as conn:
        left = await (
            await conn.execute(
                "SELECT (SELECT count(*) FROM share_links WHERE artifact_id = %s)"
                " + (SELECT count(*) FROM artifact_comments WHERE comment_id = %s) AS n",
                (record.artifact_id, comment.comment_id),
            )
        ).fetchone()
    assert left["n"] == 0


async def test_worked_is_the_latest_of_a_comment_s_thread_and_the_agent_s_save(
    space, test_db_pool
):
    user_id, _, ws, entry, record = await _setup(space)
    by_comment, by_save = await _thread(ws), await _thread(ws)
    await _comment(user_id, record, delivery="queued", thread_id=by_comment)

    found = await artifacts.destinations(
        user_id=user_id, artifact_id=record.artifact_id, beside_thread_id=None, caller=user_id
    )
    assert (found.worked.thread_id, found.default[1]) == (by_comment, "worked")

    agent = Actor(kind="agent", user_id=user_id, via="cli", thread_id=by_save)
    ref, data = ref_of("hello, louder")
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.save("notes/a.md", ref, agent, data=data)
    found = await artifacts.destinations(
        user_id=user_id, artifact_id=record.artifact_id, beside_thread_id=None, caller=user_id
    )
    assert found.worked.thread_id == by_save

    # Archived, it can take nothing, and the comment's thread is next.
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET archived_at = NOW()"
            " WHERE conversation_thread_id = %s",
            (by_save,),
        )
    found = await artifacts.destinations(
        user_id=user_id, artifact_id=record.artifact_id, beside_thread_id=None, caller=user_id
    )
    assert found.worked.thread_id == by_comment


async def test_a_former_home_s_drive_record_folds_into_home(space):
    user_id, home, _, entry, _ = await _setup(space)
    former = str(
        (
            await workspaces.create_workspace(
                user_id, "Home", workspace_id=get_home_workspace_id("old-account-drive"),
                status="unbound",
            )
        )["workspace_id"]
    )
    other = await save_text(space, "notes/other.md", "x")
    merged = await artifacts.ensure_record(former, "drive", entry.entry_id)
    alone = await artifacts.ensure_record(former, "drive", other.entry_id)
    await _comment(user_id, merged)

    await workspaces.get_or_create_home_workspace(user_id)

    assert await artifacts.get_artifact(merged.artifact_id, user_id=user_id) is None
    kept = await artifacts.find_record(home, "drive", entry.entry_id)
    assert [c.number for c in await artifacts.list_comments(kept.artifact_id)] == [1]
    assert (await artifacts.get_artifact(alone.artifact_id, user_id=user_id)).workspace_id == home


async def test_with_the_drive_off_its_comments_wait_and_others_go(space):
    """The agent could open none of its files, so a Drive file's comment
    stays queued, as one in the trash does; a workspace file's goes."""
    user_id, _, ws, _, record = await _setup(space)
    thread_id = await _thread(ws)
    drive_comment = await _comment(user_id, record, delivery="queued", thread_id=thread_id)
    file = await artifacts.ensure_record(ws, "file", "report.html")
    file_comment = await _comment(user_id, file, delivery="queued", thread_id=thread_id)

    queued = await artifacts.list_queued_comments(
        user_id=user_id, thread_id=thread_id, drive_on=False
    )
    assert [c.comment_id for c in queued] == [file_comment.comment_id]
    claimed = await artifacts.claim_comments(
        user_id=user_id, thread_id=thread_id,
        comment_ids=[drive_comment.comment_id, file_comment.comment_id],
        request_key=str(uuid.uuid4()), caller=user_id, drive_on=False,
    )
    assert [c.comment_id for c in claimed.comments] == [file_comment.comment_id]

    (still,) = await artifacts.list_queued_comments(
        user_id=user_id, thread_id=thread_id, drive_on=True
    )
    assert (still.comment_id, still.delivery) == (drive_comment.comment_id, "queued")


async def test_a_rename_s_tombstone_is_gone_not_in_the_trash(space):
    """A rename over a file ends its source as a tombstone nothing restores:
    its comments are neither sent nor held as trashed, so the owner reads
    that they are gone rather than to restore a file they cannot."""
    user_id, _, ws, entry, record = await _setup(space)
    await save_text(space, "notes/b.md", "other")
    thread_id = await _thread(ws)
    comment = await _comment(user_id, record, delivery="queued", thread_id=thread_id)
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.rename("notes/a.md", "notes/b.md", person_of(space), replace=True)

    assert await artifacts.list_queued_comments(
        user_id=user_id, thread_id=thread_id, drive_on=True
    ) == []
    assert await _claim(user_id, thread_id, comment) == artifacts.Claim([])
    assert await artifacts.drive_comment_counts(user_id=user_id, space_id=space.space_id) == {}
    # Nor is its record there to write another comment on.
    assert await artifacts.get_artifact(record.artifact_id, user_id=user_id) is None


async def test_the_agent_s_answer_is_refused_on_a_tombstone(space):
    """A comment the agent was sent, on a file it then renamed over another:
    the record is gone to its owner, so an answer is refused rather than
    written where only the sweep would find it."""
    user_id, _, ws, _, record = await _setup(space)
    await save_text(space, "notes/b.md", "other")
    thread_id = await _thread(ws)
    comment = await _comment(user_id, record, delivery="queued", thread_id=thread_id)
    assert (await _claim(user_id, thread_id, comment)).comments
    async with writes.locked(space.space_id, LIMITS) as tx:
        await tx.rename("notes/a.md", "notes/b.md", person_of(space), replace=True)

    with pytest.raises(artifacts.CommentNotFound):
        await artifacts.add_agent_reply(
            user_id=user_id, thread_id=thread_id, comment_id=comment.comment_id,
            body="Done.", resolve=True,
        )


async def test_a_trashed_file_keeps_its_record_for_its_restore(space):
    user_id, _, ws, _, record = await _setup(space)
    thread_id = await _thread(ws)
    comment = await _comment(user_id, record, delivery="queued", thread_id=thread_id)
    assert (await _claim(user_id, thread_id, comment)).comments
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "notes/a.md", person_of(space), None, False)
    found = await artifacts.get_artifact(record.artifact_id, user_id=user_id)
    assert found is not None and found.artifact_id == record.artifact_id
    # The agent's answer waits there with it.
    reply = await artifacts.add_agent_reply(
        user_id=user_id, thread_id=thread_id, comment_id=comment.comment_id,
        body="Done.", resolve=False,
    )
    assert reply.artifact_id == record.artifact_id


async def test_with_the_drive_off_a_send_of_only_drive_comments_says_so(space):
    user_id, _, ws, _, record = await _setup(space)
    thread_id = await _thread(ws)
    comment = await _comment(user_id, record, delivery="queued", thread_id=thread_id)
    claim = await artifacts.claim_comments(
        user_id=user_id, thread_id=thread_id, comment_ids=[comment.comment_id],
        request_key=str(uuid.uuid4()), caller=user_id, drive_on=False,
    )
    assert claim == artifacts.Claim([], drive_off=True)

    # In the trash as well, the Drive being off is still the reason.
    async with writes.locked(space.space_id, LIMITS) as tx:
        await trash_rows.trash(tx, "notes/a.md", person_of(space), None, False)
    claim = await artifacts.claim_comments(
        user_id=user_id, thread_id=thread_id, comment_ids=[comment.comment_id],
        request_key=str(uuid.uuid4()), caller=user_id, drive_on=False,
    )
    assert claim == artifacts.Claim([], drive_off=True)


async def test_a_record_for_an_entry_purged_since_its_check_is_not_found(space):
    user_id, home, _, _, _ = await _setup(space)
    other = await save_text(space, "notes/gone.md", "x")
    async with writes.locked(space.space_id, LIMITS) as tx:
        trash_id = await trash_rows.trash(tx, "notes/gone.md", person_of(space), None, False)
    async with writes.upkeep(space.space_id, LIMITS) as tx:
        await trash_rows.purge(tx, trash_id, None)

    with pytest.raises(artifacts.ArtifactNotFound):
        await artifacts.ensure_record(home, "drive", other.entry_id)


async def test_the_agent_s_last_save_is_one_descent_of_its_index(space, test_db_pool):
    _, _, _, entry, _ = await _setup(space)
    async with test_db_pool.connection() as conn:
        await conn.execute("SET enable_seqscan = off")
        plan = await (
            await conn.execute(
                "EXPLAIN SELECT v.thread_id FROM drive_versions v"
                " WHERE v.entry_id = %s::uuid AND v.actor_kind = 'agent'"
                " AND v.thread_id IS NOT NULL ORDER BY v.seq DESC LIMIT 1",
                (entry.entry_id,),
            )
        ).fetchall()
    assert "ix_drive_versions_agent_thread" in " ".join(str(r) for r in plan)
