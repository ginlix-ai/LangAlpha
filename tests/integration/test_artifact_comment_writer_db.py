"""Who wrote each message on a comment, against a real Postgres.

A shared workspace's comments are its holder's, so each person's message
names the member who wrote it and reads back with their name. A personal
workspace's messages name no one. A member whose account is removed leaves
their words, written by no one in particular.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from src.server.database.artifacts import comments

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

ANCHOR = {"kind": "text", "quote": "Revenue"}


@pytest_asyncio.fixture(loop_scope="session")
async def record(patched_get_db_connection, test_db_pool):
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    tag = uuid.uuid4().hex[:10]
    holder, maya, lee = f"shared-{uuid.uuid4()}", f"cw-maya-{tag}", f"cw-lee-{tag}"
    for user_id, name in ((holder, None), (maya, "Maya Okafor"), (lee, "Lee Chen")):
        await create_user(user_id=user_id, email=f"{tag}-{name or 'holder'}@example.com".replace(" ", ""), name=name)
    ws = str((await create_workspace(user_id=holder, name="Atlas", status="running"))["workspace_id"])
    artifact = str(uuid.uuid4())
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO share_links (code, workspace_id, kind, path, artifact_id)"
            " VALUES (%s, %s, 'file', 'report.html', %s)",
            (f"cw{tag}", ws, artifact),
        )
    try:
        yield {"holder": holder, "maya": maya, "lee": lee, "artifact": artifact}
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (ws,))
            await conn.execute("DELETE FROM users WHERE user_id IN (%s, %s, %s)", (holder, maya, lee))


def _writers(comment) -> list[tuple[str | None, str | None]]:
    return [(m.written_by, m.written_by_name) for m in comment.messages]


async def test_each_message_names_the_member_who_wrote_it(record, test_db_pool):
    r = record
    created = await comments.create_comment(
        user_id=r["holder"], artifact_id=r["artifact"], anchor=ANCHOR,
        body="check the revenue line", delivery="note", caller=r["maya"],
    )
    replied, _ = await comments.add_owner_message(
        user_id=r["holder"], artifact_id=r["artifact"], comment_id=created.comment_id,
        body="confirmed", delivery=None, caller=r["lee"],
    )
    assert _writers(replied) == [(r["maya"], "Maya Okafor"), (r["lee"], "Lee Chen")]

    # The workspace owner's own message, and any written before the column, name no one.
    plain, _ = await comments.add_owner_message(
        user_id=r["holder"], artifact_id=r["artifact"], comment_id=created.comment_id,
        body="owner's own", delivery=None, caller=r["holder"],
    )
    assert _writers(plain)[-1] == (None, None)

    async with test_db_pool.connection() as conn:
        await conn.execute("DELETE FROM users WHERE user_id = %s", (r["lee"],))
    (after,) = await comments.list_comments(r["artifact"])
    assert [m.body for m in after.messages] == ["check the revenue line", "confirmed", "owner's own"]
    assert _writers(after)[1] == (None, None)
